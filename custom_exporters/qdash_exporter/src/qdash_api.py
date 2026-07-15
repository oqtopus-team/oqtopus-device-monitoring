from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final

from qdash.client import QDashApiError, QDashClient, QDashConfigError

from models import (
    MetricKind,
    NormalizedRecord,
    UpstreamRequestError,
    Window,
    format_utc,
    is_finite_number,
    to_epoch_ms,
)

if TYPE_CHECKING:
    from config import QDashClientConfig

__all__ = ["QDashConfigError", "QDashGateway", "normalize_records"]

NUMERIC_VALUE_TYPES: Final = frozenset({"float", "int"})

logger = logging.getLogger(f"qdash_exporter.{__name__}")


class QDashGateway:
    """A gateway to the QDash API, wrapping a QDashClient instance."""

    def __init__(self, client: QDashClient, tag: str) -> None:
        # tag == "" means the tag parameter is omitted from requests
        self._client = client
        self._tag = tag

    @classmethod
    def from_config(cls, cfg: QDashClientConfig, tag: str) -> QDashGateway:
        """Bootstrap a QDash client from a config file or environment variables.

        Args:
            cfg: The QDashClientConfig object containing configuration.
            tag: The tag to use for API requests.

        Returns:
            An instance of QDashGateway initialized with a QDashClient.

        """
        if cfg.config_file:
            client = QDashClient.from_profile(
                profile=cfg.config_profile, path=cfg.config_file
            )
            logger.info(
                "Created QDash client from config file %s (profile=%s).",
                cfg.config_file,
                cfg.config_profile,
            )
        else:
            try:
                # Attempt to create a client from the default config file
                client = QDashClient.from_profile(profile=cfg.config_profile)
                logger.info(
                    "Created QDash client from default config file (profile=%s).",
                    cfg.config_profile,
                )
            except QDashConfigError as exc:
                logger.warning(
                    "No usable default config.ini profile (%s); using from_env(): %s",
                    cfg.config_profile,
                    exc,
                    exc_info=exc,
                )
                # Fallback to creating a client from environment variables
                client = QDashClient.from_env()

        client.config.retry.max_attempts = 1
        return cls(client, tag)

    def fetch_metric_catalog(self) -> tuple[frozenset[str], frozenset[str]]:
        """Fetch the metric catalog from the QDash API.

        Returns:
            A tuple containing two frozensets: qubit metrics and coupling metrics.

        Raises:
            UpstreamRequestError: If the request to fetch the metric catalog fails.

        """
        try:
            raw = self._client.get_metrics_config()
        except QDashApiError as exc:
            msg = f"failed to fetch metric catalog: {exc}"
            raise UpstreamRequestError(msg) from exc

        if not isinstance(raw, dict):
            raw = {}
        return (
            frozenset(raw.get("qubit_metrics", {})),
            frozenset(raw.get("coupling_metrics", {})),
        )

    def discover_chip_ids(self, mode: str) -> list[str]:
        """Discover chip IDs from the QDash API based on the specified mode.

        Args:
            mode: The mode for filtering chips (e.g., "active" or "all").

        Returns:
            A list of chip IDs that match the specified mode.

        Raises:
            UpstreamRequestError: If the request to discover chips fails.

        """
        try:
            response = self._client.list_chips()
        except QDashApiError as exc:
            msg = f"failed to discover chips: {exc}"
            raise UpstreamRequestError(msg) from exc

        chips = response.chips
        if mode == "active":
            chips = [chip for chip in chips if str(chip.activity_status) == "active"]
        return [chip.chip_id for chip in chips]

    def fetch_timeseries_records(
        self, chip_id: str, metric: str, window: Window
    ) -> list[dict[str, Any]]:
        """Fetch timeseries records for a given chip and metric within a time window.

        Args:
            chip_id: The ID of the chip to fetch records for.
            metric: The metric to fetch records for.
            window: The time window to fetch records for.

        Returns:
            A list of raw timeseries records as dictionaries.

        Raises:
            UpstreamRequestError: If the request to fetch timeseries records fails.

        """
        try:
            response = self._client.get_task_results_timeseries(
                chip_id=chip_id,
                parameter=metric,
                start_at=format_utc(window.from_at),
                end_at=format_utc(window.to_at),
                tag=self._tag or None,
            )
        except QDashApiError as exc:
            msg = f"failed to fetch timeseries for {chip_id!r}/{metric!r}: {exc}"
            raise UpstreamRequestError(msg) from exc

        # Flatten the nested response into a list of dicts
        records: list[dict[str, Any]] = []
        for qid, points in response.data.items():
            for point in points:
                dumped = point.model_dump(mode="json")
                dumped["qid_role"] = qid
                records.append(dumped)
        return records


def normalize_records(  # noqa: C901
    raw_records: list[dict[str, Any]],
    kind: MetricKind,
    window: Window,
) -> list[NormalizedRecord]:
    """Normalize raw timeseries records into NormalizedRecord instances.

    Args:
        raw_records: A list of raw timeseries records as dictionaries.
        kind: The kind of metric (MetricKind.QUBIT or MetricKind.COUPLING).
        window: The time window for which the records were fetched.

    Returns:
        A list of NormalizedRecord instances that are valid and within specified window.

    """
    from_ms = to_epoch_ms(window.from_at)
    to_ms = to_epoch_ms(window.to_at)
    records: list[NormalizedRecord] = []

    for raw in raw_records:
        calibrated_at_raw = raw.get("calibrated_at")
        if not isinstance(calibrated_at_raw, str):
            logger.warning(
                "Skipping record with missing/invalid calibrated_at: %r", raw
            )
            continue
        try:
            calibrated_at = datetime.fromisoformat(calibrated_at_raw)
        except ValueError:
            logger.warning("Skipping record with unparseable calibrated_at: %r", raw)
            continue
        if calibrated_at.tzinfo is None:
            calibrated_at = calibrated_at.replace(tzinfo=UTC)
        timestamp_ms = to_epoch_ms(calibrated_at)

        if not from_ms <= timestamp_ms < to_ms:
            logger.warning(
                "Skipping record with timestamp outside the request window: %r", raw
            )
            continue

        value_raw = raw.get("value")
        if not is_finite_number(value_raw):
            logger.warning(
                "Skipping record with a null/non-numeric/NaN/Infinity value: %r", raw
            )
            continue

        value_type = raw.get("value_type")
        if value_type is not None and value_type not in NUMERIC_VALUE_TYPES:
            logger.warning(
                "Skipping record with a non-numeric-compatible value_type %r: %r",
                value_type,
                raw,
            )
            continue

        qid_role = raw.get("qid_role")
        if not isinstance(qid_role, str) or not qid_role:
            logger.warning("Skipping record with a missing or empty qid_role: %r", raw)
            continue

        unit_raw = raw.get("unit")
        unit = str(unit_raw) if unit_raw is not None else ""

        error: float | None = None
        if kind == MetricKind.QUBIT and "error" in raw and raw["error"] is not None:
            error_raw = raw["error"]
            if is_finite_number(error_raw):
                error_value = float(error_raw)
                if error_value:
                    error = error_value
            else:
                logger.warning(
                    "Discarding non-numeric error value, keeping base record: %r", raw
                )

        records.append(
            NormalizedRecord(
                timestamp_ms=timestamp_ms,
                value=float(value_raw),  # type: ignore[arg-type]
                unit=unit,
                qubit_id=qid_role if kind == MetricKind.QUBIT else None,
                coupling_id=qid_role if kind == MetricKind.COUPLING else None,
                error=error,
            )
        )

    return records
