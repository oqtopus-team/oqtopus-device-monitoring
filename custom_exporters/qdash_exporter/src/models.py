from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum, StrEnum, auto
from typing import Any

BATCH_FILENAME_PATTERN = re.compile(r"^\d{8}T\d{6}Z-.+-[A-Za-z0-9_]+_[1-9]\d*\.json$")
_TRAILING_SEQ_PATTERN = re.compile(r"_[1-9]\d*$")


class QdashExporterError(Exception):
    """Base class for all exporter-specific errors."""


class ConfigError(QdashExporterError):
    """Invalid or missing EXPORTER configuration (config.yaml / env overrides).

    Startup fails. qdash.client-side configuration failures are NOT mapped to
    this type; QDashConfigError propagates as-is (see qdash_api.py).
    """


class LocalStateError(QdashExporterError):
    """Corrupt window state or pending batch detected at startup. Startup fails."""


class BatchValidationError(QdashExporterError):
    """A batch JSON dict violates the batch schema."""


class UpstreamRequestError(QdashExporterError):
    """Any failure raised while calling QDash through qdash.client."""


class InternalServerError(QdashExporterError):
    """Exporter-local pull-path fault. Mapped to HTTP 500."""


class ServiceUnavailableError(QdashExporterError):
    """No deliverable buffered samples. Mapped to HTTP 503."""


class MetricKind(StrEnum):
    """Metric family kind."""

    QUBIT = "qubit"
    COUPLING = "coupling"


class CollectionOutcome(Enum):
    """Result classification of one chip_id x metric collection attempt."""

    SUCCESS_WITH_DATA = auto()  # success and at least one raw record returned
    SUCCESS_EMPTY = auto()  # success and zero raw records (data_count = 0)
    UPSTREAM_FAILURE = auto()  # upstream request failure after all retries


def format_utc(dt: datetime) -> str:
    """Format an aware UTC datetime as 'YYYY-MM-DDTHH:MM:SSZ'.

    Args:
        dt: An aware UTC datetime.

    Returns:
        A string representing the formatted UTC datetime.

    """
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(text: str) -> datetime:
    """Parse 'YYYY-MM-DDTHH:MM:SSZ' into an aware UTC datetime.

    Args:
        text: A string in 'YYYY-MM-DDTHH:MM:SSZ' format.

    Returns:
        An aware UTC datetime corresponding to the input string.

    Raises:
        ValueError: If the text is not a valid UTC ISO 8601 timestamp.

    """
    try:
        parsed = datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ")  # noqa: DTZ007
    except ValueError as exc:
        msg = f"invalid UTC timestamp: {text!r}"
        raise ValueError(msg) from exc
    return parsed.replace(tzinfo=UTC)


def to_epoch_ms(dt: datetime) -> int:
    """Convert an aware datetime to UTC UNIX epoch milliseconds.

    Args:
        dt: An aware datetime.

    Returns:
        UTC UNIX epoch milliseconds.

    """
    return round(dt.timestamp() * 1000)


def is_finite_number(value: object) -> bool:
    """Check if a value is a finite number, excluding bool, NaN, and Infinity.

    Args:
        value: The value to check.

    Returns:
        True if the value is a finite number (int or float), False otherwise.

    """
    if isinstance(value, bool):
        return False
    if not isinstance(value, int | float):
        return False
    return math.isfinite(value)


@dataclass(frozen=True)
class Window:
    """Half-open collection window [from_at, to_at)."""

    from_at: datetime  # inclusive, aware UTC
    to_at: datetime  # exclusive, aware UTC


@dataclass(frozen=True)
class NormalizedRecord:
    """One validated metric sample stored in a batch file."""

    timestamp_ms: int  # UTC epoch milliseconds (from calibrated_at)
    value: float
    unit: str  # "" when QDash does not provide a unit
    qubit_id: str | None  # exactly one of qubit_id / coupling_id is set
    coupling_id: str | None
    error: float | None = None  # optional; qubit metrics only (spec 3.5)


def _record_to_json_dict(record: NormalizedRecord) -> dict[str, Any]:
    data: dict[str, Any] = {
        "timestamp_ms": record.timestamp_ms,
        "value": record.value,
        "unit": record.unit,
    }
    if record.qubit_id is not None:
        data["qubit_id"] = record.qubit_id
    else:
        data["coupling_id"] = record.coupling_id
    if record.error is not None:
        data["error"] = record.error
    return data


def _parse_window(data: object) -> Window:
    if not isinstance(data, dict):
        msg = f"'window' is missing or not an object: {data!r}"
        raise BatchValidationError(msg)

    from_raw = data.get("from")
    to_raw = data.get("to")
    if not isinstance(from_raw, str) or not isinstance(to_raw, str):
        msg = f"'window.from'/'window.to' must be strings: {data!r}"
        raise BatchValidationError(msg)

    try:
        from_at = parse_utc(from_raw)
        to_at = parse_utc(to_raw)
    except ValueError as exc:
        msg = f"'window' has an invalid timestamp: {data!r}"
        raise BatchValidationError(msg) from exc

    if not from_at < to_at:
        msg = f"'window.from' must be strictly before 'window.to': {data!r}"
        raise BatchValidationError(msg)

    return Window(from_at=from_at, to_at=to_at)


def _parse_record(data: object, window: Window) -> NormalizedRecord:
    if not isinstance(data, dict):
        msg = f"record must be an object: {data!r}"
        raise BatchValidationError(msg)

    timestamp_ms = data.get("timestamp_ms")
    if isinstance(timestamp_ms, bool) or not isinstance(timestamp_ms, int):
        msg = f"'timestamp_ms' must be an int: {data!r}"
        raise BatchValidationError(msg)

    value = data.get("value")
    if not is_finite_number(value):
        msg = f"'value' must be a finite number: {data!r}"
        raise BatchValidationError(msg)

    qubit_id = data.get("qubit_id")
    coupling_id = data.get("coupling_id")
    has_qubit = isinstance(qubit_id, str)
    has_coupling = isinstance(coupling_id, str)
    if has_qubit == has_coupling:
        msg = f"record must have exactly one of 'qubit_id'/'coupling_id': {data!r}"
        raise BatchValidationError(msg)

    unit = data.get("unit")
    if not isinstance(unit, str):
        msg = f"'unit' is missing or not a string: {data!r}"
        raise BatchValidationError(msg)

    error: float | None = None
    if "error" in data:
        error_raw = data["error"]
        if not is_finite_number(error_raw):
            msg = f"'error' must be a finite number: {data!r}"
            raise BatchValidationError(msg)
        error = float(error_raw)  # type: ignore[arg-type]

    from_ms = to_epoch_ms(window.from_at)
    to_ms = to_epoch_ms(window.to_at)
    if not from_ms <= timestamp_ms < to_ms:
        window_bounds = f"[{from_ms}, {to_ms})"
        msg = f"'timestamp_ms' {timestamp_ms} outside window {window_bounds}: {data!r}"
        raise BatchValidationError(msg)

    return NormalizedRecord(
        timestamp_ms=timestamp_ms,
        value=float(value),  # type: ignore[arg-type]
        unit=unit,
        qubit_id=qubit_id if has_qubit else None,
        coupling_id=coupling_id if has_coupling else None,
        error=error,
    )


@dataclass(frozen=True)
class Batch:
    """Immutable batch: one chip_id x metric x window collection result."""

    batch_id: str
    collected_at: datetime
    window: Window
    chip_id: str
    metric: str
    records: tuple[NormalizedRecord, ...]  # always non-empty when written

    def to_json_dict(self) -> dict[str, Any]:
        """Serialize the Batch to a JSON-compatible dict.

        Returns:
            A dict suitable for JSON serialization, conforming to the batch schema.

        """
        return {
            "batch_id": self.batch_id,
            "collected_at": format_utc(self.collected_at),
            "window": {
                "from": format_utc(self.window.from_at),
                "to": format_utc(self.window.to_at),
            },
            "chip_id": self.chip_id,
            "metric": self.metric,
            "records": [_record_to_json_dict(record) for record in self.records],
        }

    @classmethod
    def from_json_dict(cls, data: dict[str, Any]) -> Batch:
        """Parse a JSON dict into a Batch object, validating the schema.

        Returns:
            A Batch instance if the dict is valid.

        Raises:
            BatchValidationError: If the dict does not conform to the batch schema.

        """
        if not isinstance(data, dict):
            msg = f"batch data must be a JSON object: {data!r}"
            raise BatchValidationError(msg)

        batch_id = data.get("batch_id")
        if not isinstance(batch_id, str):
            msg = f"'batch_id' is missing or not a string: {data!r}"
            raise BatchValidationError(msg)

        collected_at_raw = data.get("collected_at")
        if not isinstance(collected_at_raw, str):
            msg = f"'collected_at' is missing or not a string: {data!r}"
            raise BatchValidationError(msg)
        try:
            collected_at = parse_utc(collected_at_raw)
        except ValueError as exc:
            msg = f"'collected_at' is not a valid UTC timestamp: {collected_at_raw!r}"
            raise BatchValidationError(msg) from exc

        window = _parse_window(data.get("window"))

        chip_id = data.get("chip_id")
        if not isinstance(chip_id, str):
            msg = f"'chip_id' is missing or not a string: {data!r}"
            raise BatchValidationError(msg)

        metric = data.get("metric")
        if not isinstance(metric, str):
            msg = f"'metric' is missing or not a string: {data!r}"
            raise BatchValidationError(msg)

        records_raw = data.get("records")
        if not isinstance(records_raw, list) or not records_raw:
            msg = f"'records' must be a non-empty list: {data!r}"
            raise BatchValidationError(msg)

        records = tuple(_parse_record(r, window) for r in records_raw)

        return cls(
            batch_id=batch_id,
            collected_at=collected_at,
            window=window,
            chip_id=chip_id,
            metric=metric,
            records=records,
        )


@dataclass
class WindowStateEntry:
    """Per chip_id x metric window expansion state (spec 3.4.5 schema)."""

    empty_count: int
    last_window_from: datetime
    last_window_to: datetime
    updated_at: datetime


def window_state_key(chip_id: str, metric: str) -> str:
    """Get the state store key <chip_id>::<metric> for a chip_id x metric combination.

    Args:
        chip_id: The chip ID.
        metric: The metric name.

    Returns:
        The state store key for the combination.

    """
    return f"{chip_id}::{metric}"


def build_batch_id(collected_at: datetime, chip_id: str, metric: str, seq: int) -> str:
    """Build a batch_id string.

    Args:
        collected_at: The datetime when the batch was collected.
        chip_id: The chip ID.
        metric: The metric name.
        seq: The sequence number for the batch.

    Returns:
        A batch_id string in the format "<YYYYMMDDTHHMMSSZ>-<chip_id>-<metric>_<seq>".

    """
    timestamp = collected_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{chip_id}-{metric}_{seq}"


def is_valid_batch_filename(filename: str) -> bool:
    """Check that a pending filename matches the batch_id naming convention.

    Args:
        filename: The filename to check.

    Returns:
        True if the filename matches the batch_id naming convention, False otherwise.

    """
    return BATCH_FILENAME_PATTERN.match(filename) is not None


def infer_metric_from_filename(
    filename: str, enabled_metrics: frozenset[str]
) -> str | None:
    """Infer the metric name from a batch filename without opening the file.

    Args:
        filename: The batch filename (e.g., '20240101T120000Z-chip1-metric_1.json').
        enabled_metrics: The set of currently enabled metrics.

    Returns:
        The metric name if it can be inferred and is enabled, otherwise None.

    """
    stem = filename.removesuffix(".json")
    stem = _TRAILING_SEQ_PATTERN.sub("", stem)
    for metric in enabled_metrics:
        if stem.endswith(f"-{metric}"):
            return metric
    return None
