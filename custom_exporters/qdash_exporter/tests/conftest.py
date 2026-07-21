from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest

from config import AppConfig
from models import (
    Batch,
    NormalizedRecord,
    Window,
    build_batch_id,
    to_epoch_ms,
)
from storage import SpoolBuffer, WindowStateStore

if TYPE_CHECKING:
    from pathlib import Path

FIXED_NOW = datetime(2026, 6, 11, 1, 0, 0, tzinfo=UTC)
DEFAULT_WINDOW = Window(
    from_at=datetime(2026, 6, 11, 0, 0, 0, tzinfo=UTC),
    to_at=FIXED_NOW,
)


@pytest.fixture
def app_config(tmp_path: Path) -> AppConfig:
    return AppConfig.model_validate({
        "collection": {
            "interval_sec": 3600,
            "retry_max_attempts": 3,
            "max_expand_windows": 3,
        },
        "buffer": {"dir_path": str(tmp_path / "buffer")},
        "targets": {
            "qubit_metrics": ["t1", "t2_echo"],
            "coupling_metrics": ["zx90_gate_fidelity"],
        },
    })


@pytest.fixture
def spool(app_config: AppConfig) -> SpoolBuffer:
    buffer = SpoolBuffer(app_config.buffer.dir_path)
    buffer.ensure_layout()
    return buffer


@pytest.fixture
def state_store(app_config: AppConfig, spool: SpoolBuffer) -> WindowStateStore:
    return WindowStateStore(spool.state_dir, app_config.collection.max_expand_windows)


def make_record(
    *,
    qubit_id: str | None = "0",
    coupling_id: str | None = None,
    value: float = 45.2,
    unit: str = "us",
    error: float | None = None,
    timestamp_ms: int | None = None,
) -> NormalizedRecord:
    return NormalizedRecord(
        timestamp_ms=timestamp_ms
        if timestamp_ms is not None
        else to_epoch_ms(DEFAULT_WINDOW.from_at),
        value=value,
        unit=unit,
        qubit_id=qubit_id,
        coupling_id=coupling_id,
        error=error,
    )


def make_batch(
    *,
    chip_id: str = "chip_001",
    metric: str = "t1",
    seq: int = 1,
    collected_at: datetime = FIXED_NOW,
    window: Window = DEFAULT_WINDOW,
    records: tuple[NormalizedRecord, ...] | None = None,
) -> Batch:
    return Batch(
        batch_id=build_batch_id(collected_at, chip_id, metric, seq),
        collected_at=collected_at,
        window=window,
        chip_id=chip_id,
        metric=metric,
        records=records if records is not None else (make_record(),),
    )


class FakeGateway:
    """Scripted QDashGateway substitute for collector tests (duck typing)."""

    def __init__(
        self,
        *,
        chips: list[str] | Exception | None = None,
        chips_script: list[list[str] | Exception] | None = None,
        catalog: tuple[frozenset[str], frozenset[str]] | Exception | None = None,
        fetch_result: list[dict[str, Any]] | Exception | None = None,
        fetch_script: list[list[dict[str, Any]] | Exception] | None = None,
    ) -> None:
        self._chips = chips if chips is not None else []
        self._chips_script = chips_script
        self._catalog = catalog
        self._fetch_result = fetch_result if fetch_result is not None else []
        self._fetch_script = fetch_script
        self.fetch_calls: list[tuple[str, str, Window]] = []
        self.discover_calls = 0
        self.catalog_calls = 0

    def fetch_metric_catalog(self) -> tuple[frozenset[str], frozenset[str]]:
        self.catalog_calls += 1
        if isinstance(self._catalog, Exception):
            raise self._catalog
        if self._catalog is None:
            return frozenset(), frozenset()
        return self._catalog

    def discover_chip_ids(self, mode: str) -> list[str]:  # noqa: ARG002
        self.discover_calls += 1
        if self._chips_script:
            item = self._chips_script.pop(0)
            if isinstance(item, Exception):
                raise item
            return list(item)
        if isinstance(self._chips, Exception):
            raise self._chips
        return list(self._chips)

    def fetch_timeseries_records(
        self, chip_id: str, metric: str, window: Window
    ) -> list[dict[str, Any]]:
        self.fetch_calls.append((chip_id, metric, window))
        item = self._fetch_script.pop(0) if self._fetch_script else self._fetch_result
        if isinstance(item, Exception):
            raise item
        return item


def valid_raw_record(
    *,
    calibrated_at: str = "2026-06-11T00:30:00Z",
    qid_role: str = "0",
    value: float = 45.2,
    unit: str | None = "us",
    **extra: Any,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "calibrated_at": calibrated_at,
        "qid_role": qid_role,
        "value": value,
    }
    if unit is not None:
        record["unit"] = unit
    record.update(extra)
    return record


def window_of(multiplier: int, interval_sec: int = 3600) -> Window:
    return Window(
        from_at=FIXED_NOW - timedelta(seconds=interval_sec * multiplier),
        to_at=FIXED_NOW,
    )
