from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING

from models import (
    Batch,
    BatchValidationError,
    LocalStateError,
    Window,
    WindowStateEntry,
    build_batch_id,
    format_utc,
    infer_metric_from_filename,
    is_valid_batch_filename,
    parse_utc,
    window_state_key,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime
    from pathlib import Path

logger = logging.getLogger(f"qdash_exporter.{__name__}")


class SpoolBuffer:
    """Manages the spool directory layout for pending batches and temporary files."""

    def __init__(self, dir_path: Path) -> None:
        self._dir_path = dir_path
        self._pending_dir = dir_path / "pending"
        self._tmp_dir = dir_path / "tmp"
        self._state_dir = dir_path / "state"

    @property
    def state_dir(self) -> Path:
        """Path to the state/ directory, where window_state.json is stored.

        Returns:
            The path to the state directory.

        """
        return self._state_dir

    def ensure_layout(self) -> None:
        """Create the spool directory layout if it doesn't exist.

        Raises:
            LocalStateError: If the directories cannot be created.

        """
        try:
            self._pending_dir.mkdir(parents=True, exist_ok=True)
            self._tmp_dir.mkdir(parents=True, exist_ok=True)
            self._state_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            msg = f"cannot create spool directories under {self._dir_path}: {exc}"
            raise LocalStateError(msg) from exc
        else:
            logger.info(
                "Ensured spool file buffer layout: pending=%s, tmp=%s, state=%s",
                self._pending_dir,
                self._tmp_dir,
                self._state_dir,
            )

    def next_batch_id(self, collected_at: datetime, chip_id: str, metric: str) -> str:
        """Generate the next batch_id for a given chip_id and metric.

        Args:
            collected_at: The datetime when the batch is collected.
            chip_id: The chip ID for which the batch is collected.
            metric: The metric for which the batch is collected.

        Returns:
            A unique batch_id string in the format:
            "{collected_at_utc}_{chip_id}_{metric}_{seq}",
            where seq is an incrementing integer for each unique combination of
            (collected_at, chip_id, metric).

        """
        seq = 1
        while True:
            batch_id = build_batch_id(collected_at, chip_id, metric, seq)
            pending_path = self._pending_dir / f"{batch_id}.json"
            tmp_path = self._tmp_dir / f"{batch_id}.json.tmp"
            if not pending_path.exists() and not tmp_path.exists():
                return batch_id
            seq += 1

    def write_batch(self, batch: Batch) -> Path:
        """Write a Batch to the spool's tmp/ directory and then move it to pending/.

        Args:
            batch: The Batch object to write.

        Returns:
            The Path to the pending batch file.

        """
        tmp_path = self._tmp_dir / f"{batch.batch_id}.json.tmp"
        pending_path = self._pending_dir / f"{batch.batch_id}.json"
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(batch.to_json_dict(), f)
            f.flush()
            os.fsync(f.fileno())
        tmp_path.rename(pending_path)
        logger.info("Wrote batch %s to pending/.", batch.batch_id)
        return pending_path

    def list_pending_filenames(self) -> list[str]:
        """List the filenames of all pending batch files in the spool.

        Returns:
            A sorted list of filenames in the pending/ directory.

        """
        return sorted(p.name for p in self._pending_dir.iterdir() if p.is_file())

    def read_batch(self, filename: str) -> Batch:
        """Read and validate a batch file from pending/ directory.

        Args:
            filename: The name of the batch file to read.

        Returns:
            A Batch object constructed from the JSON data in the file.

        Raises:
            BatchValidationError: If the file cannot be parsed as valid JSON,
              or does not conform to the expected schema.

        """
        path = self._pending_dir / filename
        try:
            with path.open("rt", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as exc:
            msg = f"cannot parse batch file {filename}: {exc}"
            raise BatchValidationError(msg) from exc
        return Batch.from_json_dict(data)

    def delete_pending(self, filenames: Sequence[str]) -> None:
        """Delete the specified pending batch files from the spool.

        Args:
            filenames: A sequence of filenames to delete from the pending/ directory.

        """
        for filename in filenames:
            path = self._pending_dir / filename
            try:
                path.unlink()
            except OSError:
                logger.exception("Failed to delete served batch file %s.", filename)
            else:
                logger.info("Deleted served batch file %s.", filename)

    def validate_pending_at_startup(self, enabled_metrics: frozenset[str]) -> None:
        """Validate all pending batch files at startup.

        Args:
            enabled_metrics: The set of metrics that are enabled in the configuration.

        Raises:
            LocalStateError: If any pending batch file is invalid or cannot be read.

        """
        validated = 0
        skipped = 0
        for filename in self.list_pending_filenames():
            if not is_valid_batch_filename(filename):
                msg = f"pending file has an invalid batch_id filename: {filename}"
                raise LocalStateError(msg)

            # Infer the metric from the filename and check if it's enabled
            metric = infer_metric_from_filename(filename, enabled_metrics)
            if metric is None:
                skipped += 1
                continue

            try:
                self.read_batch(filename)
            except (BatchValidationError, OSError) as exc:
                msg = f"pending file {filename} failed startup validation: {exc}"
                raise LocalStateError(msg) from exc
            validated += 1
        logger.info(
            "Startup validation of pending batches completed: validated=%d, skipped=%d",
            validated,
            skipped,
        )


class WindowStateStore:
    """Manages the window state file (window_state.json) under the state directory."""

    def __init__(self, state_dir: Path, max_expand_windows: int) -> None:
        self._state_path = state_dir / "window_state.json"
        self._tmp_path = state_dir / "window_state.json.tmp"
        self._max_expand_windows = max_expand_windows
        self._entries: dict[str, WindowStateEntry] = {}

    def load(self) -> None:
        """Load and validate the window state file (window_state.json).

        Raises:
            LocalStateError: On unreadable file, JSON parse error, or schema violation.

        """
        # If the state file does not exist, start with an empty state.
        if not self._state_path.exists():
            logger.info("No window state cache found; starting with empty state.")
            return

        try:
            with self._state_path.open("rt", encoding="utf-8") as f:
                raw = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            msg = f"cannot read window state file {self._state_path}: {exc}"
            raise LocalStateError(msg) from exc

        if not isinstance(raw, dict):
            msg = f"window state file root must be an object: {self._state_path}"
            raise LocalStateError(msg)

        entries = {key: self._parse_entry(key, value) for key, value in raw.items()}
        self._entries = entries
        logger.info(
            "Loaded window state cache with %d entries from %s.",
            len(entries),
            self._state_path,
        )

    def _parse_entry(self, key: str, value: object) -> WindowStateEntry:
        if not isinstance(value, dict):
            msg = f"window state entry {key!r} must be an object"
            raise LocalStateError(msg)

        empty_count = value.get("empty_count")
        if isinstance(empty_count, bool) or not isinstance(empty_count, int):
            msg = f"window state entry {key!r} has a missing or non-int 'empty_count'"
            raise LocalStateError(msg)

        valid_max = self._max_expand_windows - 1
        if not 0 <= empty_count <= valid_max:
            msg = f"entry {key!r}: empty_count {empty_count} not in [0, {valid_max}]"
            raise LocalStateError(msg)

        try:
            last_window_from = parse_utc(value.get("last_window_from"))  # type: ignore[arg-type]
            last_window_to = parse_utc(value.get("last_window_to"))  # type: ignore[arg-type]
            updated_at = parse_utc(value.get("updated_at"))  # type: ignore[arg-type]
        except (ValueError, TypeError) as exc:
            msg = f"window state entry {key!r} has an invalid timestamp: {exc}"
            raise LocalStateError(msg) from exc

        if not last_window_from < last_window_to:
            msg = f"window state entry {key!r}: 'last_window_from' must precede 'to'"
            raise LocalStateError(msg)

        return WindowStateEntry(
            empty_count=empty_count,
            last_window_from=last_window_from,
            last_window_to=last_window_to,
            updated_at=updated_at,
        )

    def get_empty_count(self, chip_id: str, metric: str) -> int:
        """Get the current empty_count for the (chip_id x metric) combination.

        Args:
            chip_id: The chip ID to look up.
            metric: The metric to look up.

        Returns:
            The current empty_count for the combination, or 0 if no entry exists.

        """
        entry = self._entries.get(window_state_key(chip_id, metric))
        return entry.empty_count if entry is not None else 0

    def save_entry(
        self,
        chip_id: str,
        metric: str,
        window: Window,
        empty_count: int,
        now: datetime,
    ) -> None:
        """Save or update window state entry for a given chip_id x metric combination.

        Args:
            chip_id: The chip ID for the entry.
            metric: The metric for the entry.
            window: The time window that was just processed.
            empty_count: The new empty_count to save.
            now: The current timestamp to record as updated_at.

        Raises:
            OSError: If the window state file cannot be written to disk.

        """
        key = window_state_key(chip_id, metric)
        previous = self._entries.get(key)
        self._entries[key] = WindowStateEntry(
            empty_count=empty_count,
            last_window_from=window.from_at,
            last_window_to=window.to_at,
            updated_at=now,
        )
        try:
            self._persist()
        except OSError:
            if previous is None:
                del self._entries[key]
            else:
                self._entries[key] = previous
            raise

    def _persist(self) -> None:
        """Persist the current window state to disk atomically."""
        serialized = {
            key: {
                "empty_count": entry.empty_count,
                "last_window_from": format_utc(entry.last_window_from),
                "last_window_to": format_utc(entry.last_window_to),
                "updated_at": format_utc(entry.updated_at),
            }
            for key, entry in self._entries.items()
        }
        with self._tmp_path.open("w", encoding="utf-8") as f:
            json.dump(serialized, f)
            f.flush()
            os.fsync(f.fileno())
        self._tmp_path.rename(self._state_path)
        logger.debug("Persisted window state with %d entries.", len(self._entries))
