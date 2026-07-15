from __future__ import annotations

import threading
from datetime import timedelta
from typing import TYPE_CHECKING, cast

import pytest

from collector import (
    CollectionService,
    compute_window_multiplier,
    next_empty_count,
    run_collection_loop,
)
from models import CollectionOutcome, UpstreamRequestError
from tests.conftest import FIXED_NOW, FakeGateway, valid_raw_record, window_of

if TYPE_CHECKING:
    from config import AppConfig
    from qdash_api import QDashGateway
    from storage import SpoolBuffer, WindowStateStore

INTERVAL = 3600


def _service(
    app_config: AppConfig,
    spool: SpoolBuffer,
    state_store: WindowStateStore,
    gateway: FakeGateway,
) -> CollectionService:
    state_store.load()
    return CollectionService(
        app_config,
        cast("QDashGateway", gateway),
        spool,
        state_store,
        clock=lambda: FIXED_NOW,
    )


class TestComputeWindowMultiplier:
    """Test suite for compute_window_multiplier function."""

    @pytest.mark.parametrize(
        ("empty_count", "max_windows", "expected"),
        [(0, 3, 1), (1, 3, 2), (2, 3, 3), (3, 3, 3), (5, 3, 3)],
    )
    def test_compute_window_multiplier(
        self, empty_count: int, max_windows: int, expected: int
    ) -> None:
        assert compute_window_multiplier(empty_count, max_windows) == expected


class TestNextEmptyCount:
    """Test suite for next_empty_count function."""

    @pytest.mark.parametrize(
        ("outcome", "empty_count", "expected"),
        [
            (CollectionOutcome.SUCCESS_WITH_DATA, 2, 0),
            (CollectionOutcome.SUCCESS_EMPTY, 2, 0),
            (CollectionOutcome.UPSTREAM_FAILURE, 0, 1),
            (CollectionOutcome.UPSTREAM_FAILURE, 1, 2),
            (CollectionOutcome.UPSTREAM_FAILURE, 2, 2),
            (CollectionOutcome.UPSTREAM_FAILURE, 5, 2),
        ],
    )
    def test_next_empty_count(
        self, outcome: CollectionOutcome, empty_count: int, expected: int
    ) -> None:
        assert next_empty_count(outcome, empty_count, 3) == expected


class TestCollectOne:
    """Test suite for CollectionService._collect_one method."""

    def test_collect_multiplier_uses_pre_request_empty_count(
        self, app_config: AppConfig, spool: SpoolBuffer, state_store: WindowStateStore
    ) -> None:
        gateway = FakeGateway(fetch_result=[])
        service = _service(app_config, spool, state_store, gateway)
        state_store.save_entry("chip_001", "t1", window_of(1), 1, FIXED_NOW)

        service._collect_one("chip_001", "t1", FIXED_NOW)

        requested_window = gateway.fetch_calls[-1][2]
        assert requested_window.from_at == FIXED_NOW - timedelta(seconds=INTERVAL * 2)

    def test_collect_success_with_data_writes_batch_and_resets(
        self, app_config: AppConfig, spool: SpoolBuffer, state_store: WindowStateStore
    ) -> None:
        gateway = FakeGateway(fetch_result=[valid_raw_record()])
        service = _service(app_config, spool, state_store, gateway)
        state_store.save_entry("chip_001", "t1", window_of(1), 1, FIXED_NOW)

        service._collect_one("chip_001", "t1", FIXED_NOW)

        assert len(spool.list_pending_filenames()) == 1
        assert state_store.get_empty_count("chip_001", "t1") == 0

    def test_collect_empty_success_writes_no_batch_and_resets(
        self, app_config: AppConfig, spool: SpoolBuffer, state_store: WindowStateStore
    ) -> None:
        gateway = FakeGateway(fetch_result=[])
        service = _service(app_config, spool, state_store, gateway)
        state_store.save_entry("chip_001", "t1", window_of(1), 1, FIXED_NOW)

        service._collect_one("chip_001", "t1", FIXED_NOW)

        assert spool.list_pending_filenames() == []
        assert state_store.get_empty_count("chip_001", "t1") == 0

    def test_collect_all_invalid_records_no_batch_but_success(
        self, app_config: AppConfig, spool: SpoolBuffer, state_store: WindowStateStore
    ) -> None:
        gateway = FakeGateway(fetch_result=[valid_raw_record(qid_role="")])
        service = _service(app_config, spool, state_store, gateway)
        state_store.save_entry("chip_001", "t1", window_of(1), 1, FIXED_NOW)

        service._collect_one("chip_001", "t1", FIXED_NOW)

        assert spool.list_pending_filenames() == []
        assert state_store.get_empty_count("chip_001", "t1") == 0

    def test_collect_upstream_failure_increments_empty_count(
        self, app_config: AppConfig, spool: SpoolBuffer, state_store: WindowStateStore
    ) -> None:
        gateway = FakeGateway(fetch_result=UpstreamRequestError("down"))
        service = _service(app_config, spool, state_store, gateway)

        service._collect_one("chip_001", "t1", FIXED_NOW)

        assert spool.list_pending_filenames() == []
        assert state_store.get_empty_count("chip_001", "t1") == 1
        assert len(gateway.fetch_calls) == 4


class TestFetchWithRetry:
    """Test suite for CollectionService._fetch_with_retry method."""

    def test_fetch_retries_configured_times_then_raises(
        self, app_config: AppConfig, spool: SpoolBuffer, state_store: WindowStateStore
    ) -> None:
        gateway = FakeGateway(fetch_result=UpstreamRequestError("down"))
        service = _service(app_config, spool, state_store, gateway)

        with pytest.raises(UpstreamRequestError):
            service._fetch_with_retry("chip_001", "t1", window_of(1))

        assert len(gateway.fetch_calls) == 4


class TestRunCycle:
    """Test suite for CollectionService.run_cycle method."""

    def test_run_cycle_continues_after_combination_error(
        self, app_config: AppConfig, spool: SpoolBuffer, state_store: WindowStateStore
    ) -> None:
        gateway = FakeGateway(
            chips=["chip_001"],
            fetch_script=[RuntimeError("boom"), [], []],
        )
        service = _service(app_config, spool, state_store, gateway)

        service.run_cycle()

        assert len(gateway.fetch_calls) == 3

    def test_run_cycle_chip_discovery_failure_skips_cycle(
        self, app_config: AppConfig, spool: SpoolBuffer, state_store: WindowStateStore
    ) -> None:
        gateway = FakeGateway(chips=UpstreamRequestError("down"))
        service = _service(app_config, spool, state_store, gateway)

        service.run_cycle()

        assert gateway.fetch_calls == []
        assert state_store.get_empty_count("chip_001", "t1") == 0

    def test_run_cycle_catalog_failure_is_warning_only(
        self, app_config: AppConfig, spool: SpoolBuffer, state_store: WindowStateStore
    ) -> None:
        gateway = FakeGateway(
            chips=["chip_001"],
            catalog=UpstreamRequestError("down"),
            fetch_result=[],
        )
        service = _service(app_config, spool, state_store, gateway)

        service.run_cycle()

        assert len(gateway.fetch_calls) == 3


class TestRunCollectionLoop:
    """Test suite for run_collection_loop function."""

    def test_run_collection_loop_stops_on_event(self) -> None:
        stop_event = threading.Event()
        calls: list[int] = []

        class OneShotService:
            def run_cycle(self) -> None:
                calls.append(1)
                stop_event.set()

        run_collection_loop(OneShotService(), 0, stop_event)  # type: ignore[arg-type]

        assert calls == [1]

    def test_run_collection_loop_survives_cycle_exception(self) -> None:
        stop_event = threading.Event()
        calls: list[int] = []

        class FailingService:
            def run_cycle(self) -> None:
                calls.append(1)
                stop_event.set()
                msg = "boom"
                raise RuntimeError(msg)

        run_collection_loop(FailingService(), 0, stop_event)  # type: ignore[arg-type]

        assert calls == [1]
