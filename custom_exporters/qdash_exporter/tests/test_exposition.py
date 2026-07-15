from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from exposition import PullService
from models import InternalServerError, ServiceUnavailableError
from tests.conftest import make_batch, make_record

if TYPE_CHECKING:
    from config import AppConfig
    from storage import SpoolBuffer

DISABLED_NAME = "20260611T010000Z-chip_001-unknownmetric_1.json"
CORRUPT_T1_NAME = "20260611T010000Z-chip_001-t1_9.json"


def _service(app_config: AppConfig, spool: SpoolBuffer) -> PullService:
    return PullService(spool, app_config.targets)


class TestBuildResponse:
    """Test suite for PullService.build_response method."""

    def test_build_response_empty_spool_raises_service_unavailable(
        self, app_config: AppConfig, spool: SpoolBuffer
    ) -> None:
        with pytest.raises(ServiceUnavailableError):
            _service(app_config, spool).build_response()

    def test_build_response_skips_disabled_metric_corrupt_batch(
        self, app_config: AppConfig, spool: SpoolBuffer
    ) -> None:
        spool.write_batch(make_batch(metric="t1"))
        (app_config.buffer.dir_path / "pending" / DISABLED_NAME).write_text(
            "corrupt", encoding="utf-8"
        )

        body, served, _count = _service(app_config, spool).build_response()

        assert b"qdash_qubit_t1" in body
        assert DISABLED_NAME not in served

    def test_build_response_corrupt_enabled_batch_raises_internal_error(
        self, app_config: AppConfig, spool: SpoolBuffer
    ) -> None:
        (app_config.buffer.dir_path / "pending" / CORRUPT_T1_NAME).write_text(
            "corrupt", encoding="utf-8"
        )

        with pytest.raises(InternalServerError):
            _service(app_config, spool).build_response()

    def test_build_response_renders_qubit_metric_with_error_pair(
        self, app_config: AppConfig, spool: SpoolBuffer
    ) -> None:
        record = make_record(qubit_id="0", value=45.2, unit="us", error=0.8)
        spool.write_batch(make_batch(metric="t1", records=(record,)))

        body, _served, _count = _service(app_config, spool).build_response()
        text = body.decode("utf-8")

        assert 'qdash_qubit_t1{chip_id="chip_001",qubit_id="0",unit="us"} 45.2' in text
        assert "qdash_qubit_t1_error{" in text
        assert str(record.timestamp_ms) in text

    def test_build_response_renders_coupling_metric(
        self, app_config: AppConfig, spool: SpoolBuffer
    ) -> None:
        record = make_record(qubit_id=None, coupling_id="0-1", unit="")
        spool.write_batch(make_batch(metric="zx90_gate_fidelity", records=(record,)))

        body, _served, _count = _service(app_config, spool).build_response()
        text = body.decode("utf-8")

        assert 'coupling_id="0-1"' in text
        assert "qdash_coupling_zx90_gate_fidelity_error" not in text

    def test_build_response_renders_multiple_samples(
        self, app_config: AppConfig, spool: SpoolBuffer
    ) -> None:
        spool.write_batch(
            make_batch(metric="t1", chip_id="chip_001", seq=1, records=(make_record(),))
        )
        spool.write_batch(
            make_batch(
                metric="t1",
                chip_id="chip_002",
                seq=2,
                records=(make_record(qubit_id="1"),),
            )
        )

        body, served, _count = _service(app_config, spool).build_response()
        text = body.decode("utf-8")

        assert 'chip_id="chip_001"' in text
        assert 'chip_id="chip_002"' in text
        assert len(served) == 2


class TestConfirmServed:
    """Test suite for PullService.confirm_served method."""

    def test_confirm_served_deletes_only_served_files(
        self, app_config: AppConfig, spool: SpoolBuffer
    ) -> None:
        kept = spool.write_batch(make_batch(metric="t1", seq=1)).name
        served = spool.write_batch(make_batch(metric="t1", seq=2)).name

        _service(app_config, spool).confirm_served([served])

        remaining = spool.list_pending_filenames()
        assert remaining == [kept]
