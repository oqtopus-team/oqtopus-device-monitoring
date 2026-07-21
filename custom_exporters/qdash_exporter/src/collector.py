from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from models import (
    Batch,
    CollectionOutcome,
    UpstreamRequestError,
    Window,
    format_utc,
)
from qdash_api import QDashGateway, normalize_records

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable

    from config import AppConfig
    from storage import SpoolBuffer, WindowStateStore

# Successful collection outcomes
_SUCCESS_OUTCOMES = frozenset({
    CollectionOutcome.SUCCESS_WITH_DATA,
    CollectionOutcome.SUCCESS_EMPTY,
})

logger = logging.getLogger(f"qdash_exporter.{__name__}")


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def compute_window_multiplier(empty_count: int, max_expand_windows: int) -> int:
    """Compute the window multiplier for a given empty_count and max_expand_windows.

    Returns:
        The window multiplier w, which is 1 for empty_count=0,
         and increases by 1 for each empty_count, up to max_expand_windows.

    """
    return min(empty_count + 1, max_expand_windows)


def next_empty_count(
    outcome: CollectionOutcome, empty_count: int, max_expand_windows: int
) -> int:
    """Compute the next empty_count based on the collection outcome.

    Args:
        outcome: The outcome of the collection attempt.
        empty_count: The current empty_count for the (chip_id, metric) combination.
        max_expand_windows: The maximum allowed window multiplier.

    Returns:
        The next empty_count, which is reset to 0 on success, or incremented on failure,
        capped at max_expand_windows - 1.

    """
    if outcome in _SUCCESS_OUTCOMES:
        return 0
    return min(empty_count + 1, max_expand_windows - 1)


class CollectionService:
    """Execute one collection cycle and manage window state updates."""

    def __init__(
        self,
        config: AppConfig,
        gateway: QDashGateway,
        spool: SpoolBuffer,
        state: WindowStateStore,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._config = config
        self._gateway = gateway
        self._spool = spool
        self._state = state
        self._clock = clock

    def run_cycle(self) -> None:
        """Run one collection cycle, updating the spool and window state."""
        cycle_now = self._clock()
        logger.info("Starting collection cycle.")

        # Check the metric catalog against configured metrics
        self._check_metric_catalog()

        # Discover chip IDs with retry logic
        try:
            chip_ids = self._discover_chip_ids_with_retry()
        except UpstreamRequestError:
            logger.exception(
                "Chip discovery failed after all retries; skipping this cycle."
            )
            return
        logger.info(
            "Discovered %d chip(s) using chip_discovery_mode=%r.",
            len(chip_ids),
            self._config.collection.chip_discovery_mode,
        )

        metrics = [
            *self._config.targets.qubit_metrics,
            *self._config.targets.coupling_metrics,
        ]

        # Run collection for each (chip_id x metric) combination
        combinations = 0
        batches_written = 0
        states_saved = 0
        failures = 0
        for chip_id in chip_ids:
            for metric in metrics:
                combinations += 1
                try:
                    if self._collect_one(chip_id, metric, cycle_now):
                        batches_written += 1
                    states_saved += 1
                except Exception:
                    # Catch any unexpected exception to avoid killing the cycle loop.
                    failures += 1
                    logger.exception(
                        "Unexpected error on chip_id=%r metric=%r; state unchanged.",
                        chip_id,
                        metric,
                    )

        logger.info(
            "Window state cache saved: %d entr(ies) updated this cycle, %d total.",
            states_saved,
            self._state.entry_count,
        )
        logger.info(
            "Cycle finished: %d combination(s), %d batch(es) written, %d failure(s).",
            combinations,
            batches_written,
            failures,
        )

    def _collect_one(self, chip_id: str, metric: str, now: datetime) -> bool:
        """Collect one (chip_id x metric) combination, updating spool and window state.

        Args:
            chip_id: The chip ID to collect.
            metric: The metric to collect.
            now: The current datetime (UTC).

        Returns:
            True if a batch was written to the spool, False otherwise.

        """
        max_expand_windows = self._config.collection.max_expand_windows
        interval_sec = self._config.collection.interval_sec

        # Compute the window for this combination based on the current empty_count
        current = self._state.get_empty_count(chip_id, metric)
        w = compute_window_multiplier(current, max_expand_windows)
        window = Window(
            from_at=now - timedelta(seconds=interval_sec * w),
            to_at=now,
        )
        logger.info(
            "Computed window for chip_id=%r metric=%r: w=%d from=%s to=%s.",
            chip_id,
            metric,
            w,
            format_utc(window.from_at),
            format_utc(window.to_at),
        )

        # Fetch the timeseries records with retry logic
        wrote_batch = False
        try:
            raw = self._fetch_with_retry(chip_id, metric, window)
        except UpstreamRequestError:
            logger.exception(
                "All fetch attempts failed for chip_id=%r metric=%r.",
                chip_id,
                metric,
            )
            outcome = CollectionOutcome.UPSTREAM_FAILURE
        else:
            kind = self._config.targets.kind_of(metric)
            records = normalize_records(raw, kind, window)
            outcome = (
                CollectionOutcome.SUCCESS_WITH_DATA
                if len(raw) > 0
                else CollectionOutcome.SUCCESS_EMPTY
            )
            logger.debug(
                "Fetched %d raw record(s) for chip_id=%r metric=%r.",
                len(raw),
                chip_id,
                metric,
            )
            # Write a batch to the spool if there are valid records
            if records:
                batch_id = self._spool.next_batch_id(now, chip_id, metric)
                batch = Batch(
                    batch_id=batch_id,
                    collected_at=now,
                    window=window,
                    chip_id=chip_id,
                    metric=metric,
                    records=tuple(records),
                )
                self._spool.write_batch(batch)
                wrote_batch = True

        # Update the window state based on the outcome
        self._state.save_entry(
            chip_id,
            metric,
            window,
            next_empty_count(outcome, current, max_expand_windows),
            now,
        )
        return wrote_batch

    def _fetch_with_retry(
        self, chip_id: str, metric: str, window: Window
    ) -> list[dict[str, Any]]:
        """Fetch timeseries records with retry logic.

        Args:
            chip_id: The chip ID to fetch.
            metric: The metric to fetch.
            window: The time window to fetch.

        Returns:
            A list of raw timeseries records.

        Raises:
            UpstreamRequestError: If all retry attempts fail.

        """
        max_attempts = 1 + self._config.collection.retry_max_attempts
        attempt = 0
        while True:
            attempt += 1
            try:
                records = self._gateway.fetch_timeseries_records(
                    chip_id, metric, window
                )
            except UpstreamRequestError:
                if attempt >= max_attempts:
                    raise
                logger.exception(
                    "Attempt %d/%d failed for chip_id=%r metric=%r; retrying.",
                    attempt,
                    max_attempts,
                    chip_id,
                    metric,
                )
            else:
                if attempt > 1:
                    logger.info(
                        "Fetch recovered on attempt %d/%d for chip_id=%r metric=%r.",
                        attempt,
                        max_attempts,
                        chip_id,
                        metric,
                    )
                return records

    def _discover_chip_ids_with_retry(self) -> list[str]:
        """Fetch the list of chip IDs with retry logic.

        Returns:
            A list of chip IDs.

        Raises:
            UpstreamRequestError: If all retry attempts fail.

        """
        max_attempts = 1 + self._config.collection.retry_max_attempts
        attempt = 0
        while True:
            attempt += 1
            try:
                chip_ids = self._gateway.discover_chip_ids(
                    self._config.collection.chip_discovery_mode
                )
            except UpstreamRequestError:
                if attempt >= max_attempts:
                    raise
                logger.exception(
                    "Chip discovery attempt %d/%d failed; retrying.",
                    attempt,
                    max_attempts,
                )
            else:
                if attempt > 1:
                    logger.info(
                        "Chip discovery recovered on attempt %d/%d.",
                        attempt,
                        max_attempts,
                    )
                return chip_ids

    def _check_metric_catalog(self) -> None:
        """Check the QDash metric catalog against configured metrics."""
        try:
            qubit_catalog, coupling_catalog = self._gateway.fetch_metric_catalog()
        except UpstreamRequestError:
            logger.exception("Failed to fetch metric catalog; skipping validation.")
            return

        missing = 0
        for metric in self._config.targets.qubit_metrics:
            if metric not in qubit_catalog:
                missing += 1
                logger.warning(
                    "Configured qubit metric %r missing from QDash catalog.", metric
                )
        for metric in self._config.targets.coupling_metrics:
            if metric not in coupling_catalog:
                missing += 1
                logger.warning(
                    "Configured coupling metric %r missing from QDash catalog.", metric
                )

        configured = len(self._config.targets.qubit_metrics) + len(
            self._config.targets.coupling_metrics
        )
        logger.info(
            "Metric catalog validation: %d configured metric(s), %d missing "
            "from catalog.",
            configured,
            missing,
        )


def run_collection_loop(
    service: CollectionService,
    interval_sec: int,
    stop_event: threading.Event,
) -> None:
    """Run the collection loop in a separate thread.

    Args:
        service: The CollectionService instance to run.
        interval_sec: The collection interval in seconds.
        stop_event: A threading.Event to signal when to stop the loop.

    """
    while not stop_event.is_set():
        started = time.monotonic()
        try:
            service.run_cycle()
        except Exception:
            # Catch any unexpected exception to avoid killing the thread.
            logger.exception("Collection cycle failed unexpectedly.")
        elapsed = time.monotonic() - started
        stop_event.wait(timeout=max(0.0, interval_sec - elapsed))
