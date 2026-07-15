from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from models import BatchValidationError, LocalStateError
from storage import WindowStateStore
from tests.conftest import DEFAULT_WINDOW, FIXED_NOW, make_batch

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture

    from config import AppConfig
    from storage import SpoolBuffer

VALID_NAME = "20260611T010000Z-chip_001-t1_1.json"
DISABLED_NAME = "20260611T010000Z-chip_001-unknownmetric_1.json"
ENABLED = frozenset({"t1"})


def _pending(app_config: AppConfig) -> Path:
    return app_config.buffer.dir_path / "pending"


def _state_file(app_config: AppConfig) -> Path:
    return app_config.buffer.dir_path / "state" / "window_state.json"


class TestSpoolBuffer:
    """Test suite for SpoolBuffer."""

    def test_write_batch_leaves_tmp_empty_and_writes_pending(
        self, app_config: AppConfig, spool: SpoolBuffer
    ) -> None:
        batch = make_batch()

        pending_path = spool.write_batch(batch)

        assert list((app_config.buffer.dir_path / "tmp").iterdir()) == []
        assert pending_path.exists()
        assert spool.read_batch(pending_path.name) == batch

    def test_next_batch_id_increments_seq_within_same_second(
        self, spool: SpoolBuffer
    ) -> None:
        first = spool.next_batch_id(FIXED_NOW, "chip_001", "t1")
        spool.write_batch(make_batch(seq=1))

        second = spool.next_batch_id(FIXED_NOW, "chip_001", "t1")

        assert first.endswith("_1")
        assert second.endswith("_2")

    def test_list_pending_filenames_returns_sorted(self, spool: SpoolBuffer) -> None:
        for seq in (2, 1, 3):
            spool.write_batch(make_batch(seq=seq))

        filenames = spool.list_pending_filenames()

        assert filenames == sorted(filenames)

    def test_read_batch_invalid_json_raises(
        self, app_config: AppConfig, spool: SpoolBuffer
    ) -> None:
        (_pending(app_config) / VALID_NAME).write_text("not json", encoding="utf-8")

        with pytest.raises(BatchValidationError, match="cannot parse batch file"):
            spool.read_batch(VALID_NAME)

    def test_delete_pending_tolerates_missing_file(self, spool: SpoolBuffer) -> None:
        spool.delete_pending(["does-not-exist.json"])

    def test_validate_pending_invalid_filename_raises(
        self, app_config: AppConfig, spool: SpoolBuffer
    ) -> None:
        (_pending(app_config) / "bad.json").write_text("{}", encoding="utf-8")

        with pytest.raises(LocalStateError):
            spool.validate_pending_at_startup(ENABLED)

    def test_validate_pending_skips_disabled_metric_corrupt_batch(
        self, app_config: AppConfig, spool: SpoolBuffer
    ) -> None:
        (_pending(app_config) / DISABLED_NAME).write_text("corrupt", encoding="utf-8")

        spool.validate_pending_at_startup(ENABLED)

    def test_validate_pending_enabled_metric_corrupt_batch_raises(
        self, app_config: AppConfig, spool: SpoolBuffer
    ) -> None:
        (_pending(app_config) / VALID_NAME).write_text("corrupt", encoding="utf-8")

        with pytest.raises(LocalStateError):
            spool.validate_pending_at_startup(ENABLED)


class TestWindowStateStore:
    """Test suite for WindowStateStore."""

    def test_load_missing_state_file_starts_empty(
        self, state_store: WindowStateStore
    ) -> None:
        state_store.load()

        assert state_store.get_empty_count("chip_001", "t1") == 0

    def test_load_corrupt_json_raises(
        self, app_config: AppConfig, state_store: WindowStateStore
    ) -> None:
        _state_file(app_config).write_text("garbage", encoding="utf-8")

        with pytest.raises(LocalStateError):
            state_store.load()

    def test_load_empty_count_over_max_raises(
        self, app_config: AppConfig, state_store: WindowStateStore
    ) -> None:
        _state_file(app_config).write_text(
            '{"chip_001::t1": {"empty_count": 5, '
            '"last_window_from": "2026-06-11T00:00:00Z", '
            '"last_window_to": "2026-06-11T01:00:00Z", '
            '"updated_at": "2026-06-11T01:00:00Z"}}',
            encoding="utf-8",
        )

        with pytest.raises(LocalStateError):
            state_store.load()

    def test_get_empty_count_returns_zero_when_absent(
        self, state_store: WindowStateStore
    ) -> None:
        state_store.load()

        assert state_store.get_empty_count("chip_999", "unknown") == 0

    def test_save_entry_persists_given_empty_count(
        self, app_config: AppConfig, state_store: WindowStateStore
    ) -> None:
        state_store.load()
        state_store.save_entry("chip_001", "t1", DEFAULT_WINDOW, 2, FIXED_NOW)

        reloaded = WindowStateStore(
            app_config.buffer.dir_path / "state",
            app_config.collection.max_expand_windows,
        )
        reloaded.load()

        assert reloaded.get_empty_count("chip_001", "t1") == 2

    def test_save_entry_rolls_back_on_persist_failure(
        self, state_store: WindowStateStore, mocker: MockerFixture
    ) -> None:
        state_store.load()
        mocker.patch.object(state_store, "_persist", side_effect=OSError("disk full"))

        with pytest.raises(OSError, match="disk full"):
            state_store.save_entry("chip_001", "t1", DEFAULT_WINDOW, 1, FIXED_NOW)

        assert state_store.get_empty_count("chip_001", "t1") == 0
