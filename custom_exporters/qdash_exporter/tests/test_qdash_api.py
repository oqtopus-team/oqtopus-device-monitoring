from __future__ import annotations

import math
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest
from qdash.client import QDashApiError, QDashConfigError

import qdash_api
from config import QDashClientConfig
from models import (
    MetricKind,
    UpstreamRequestError,
    format_utc,
)
from qdash_api import QDashGateway, normalize_records
from tests.conftest import DEFAULT_WINDOW, valid_raw_record

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


def _gateway(client: Any, tag: str = "") -> QDashGateway:
    return QDashGateway(client, tag)


def _timeseries_point(mocker: MockerFixture, dumped: dict[str, Any]) -> Any:
    point = mocker.MagicMock()
    point.model_dump.return_value = dumped
    return point


class TestFromConfig:
    """Test suite for QDashGateway.from_config method."""

    def test_from_config_explicit_path_propagates_config_error(
        self, mocker: MockerFixture
    ) -> None:
        mocker.patch.object(
            qdash_api.QDashClient, "from_profile", side_effect=QDashConfigError("bad")
        )
        cfg = QDashClientConfig(config_file="/etc/qdash/config.ini")

        with pytest.raises(QDashConfigError):
            QDashGateway.from_config(cfg, tag="calibration")

    def test_from_config_default_success_sets_retry_max_attempts_one(
        self, mocker: MockerFixture
    ) -> None:
        client = mocker.MagicMock()
        mocker.patch.object(qdash_api.QDashClient, "from_profile", return_value=client)

        QDashGateway.from_config(QDashClientConfig(), tag="calibration")

        assert client.config.retry.max_attempts == 1

    def test_from_config_default_failure_falls_back_to_from_env(
        self, mocker: MockerFixture
    ) -> None:
        client = mocker.MagicMock()
        mocker.patch.object(
            qdash_api.QDashClient,
            "from_profile",
            side_effect=QDashConfigError("no file"),
        )
        from_env = mocker.patch.object(
            qdash_api.QDashClient, "from_env", return_value=client
        )

        QDashGateway.from_config(QDashClientConfig(), tag="calibration")

        from_env.assert_called_once()
        assert client.config.retry.max_attempts == 1

    def test_from_config_from_env_failure_propagates(
        self, mocker: MockerFixture
    ) -> None:
        mocker.patch.object(
            qdash_api.QDashClient,
            "from_profile",
            side_effect=QDashConfigError("no file"),
        )
        mocker.patch.object(
            qdash_api.QDashClient, "from_env", side_effect=QDashConfigError("no env")
        )

        with pytest.raises(QDashConfigError):
            QDashGateway.from_config(QDashClientConfig(), tag="calibration")


class TestFetchMetricCatalog:
    """Test suite for QDashGateway.fetch_metric_catalog method."""

    def test_fetch_metric_catalog_returns_metric_name_sets(
        self, mocker: MockerFixture
    ) -> None:
        client = mocker.MagicMock()
        client.get_metrics_config.return_value = {
            "qubit_metrics": {"t1": {}, "t2_echo": {}},
            "coupling_metrics": {"zx90_gate_fidelity": {}},
        }

        qubit, coupling = _gateway(client).fetch_metric_catalog()

        assert qubit == frozenset({"t1", "t2_echo"})
        assert coupling == frozenset({"zx90_gate_fidelity"})

    def test_fetch_metric_catalog_non_dict_returns_empty(
        self, mocker: MockerFixture
    ) -> None:
        client = mocker.MagicMock()
        client.get_metrics_config.return_value = ["unexpected"]

        qubit, coupling = _gateway(client).fetch_metric_catalog()

        assert qubit == frozenset()
        assert coupling == frozenset()

    def test_fetch_metric_catalog_api_error_wrapped(
        self, mocker: MockerFixture
    ) -> None:
        client = mocker.MagicMock()
        client.get_metrics_config.side_effect = QDashApiError("boom")

        with pytest.raises(UpstreamRequestError):
            _gateway(client).fetch_metric_catalog()


class TestDiscoverChipIds:
    """Test suite for QDashGateway.discover_chip_ids method."""

    def test_discover_chip_ids_active_filters_inactive(
        self, mocker: MockerFixture
    ) -> None:
        client = mocker.MagicMock()
        client.list_chips.return_value = SimpleNamespace(
            chips=[
                SimpleNamespace(chip_id="c1", activity_status="active"),
                SimpleNamespace(chip_id="c2", activity_status="inactive"),
            ]
        )

        result = _gateway(client).discover_chip_ids("active")

        assert result == ["c1"]

    def test_discover_chip_ids_all_returns_all(self, mocker: MockerFixture) -> None:
        client = mocker.MagicMock()
        client.list_chips.return_value = SimpleNamespace(
            chips=[
                SimpleNamespace(chip_id="c1", activity_status="active"),
                SimpleNamespace(chip_id="c2", activity_status="inactive"),
            ]
        )

        result = _gateway(client).discover_chip_ids("all")

        assert result == ["c1", "c2"]

    def test_discover_chip_ids_api_error_wrapped(self, mocker: MockerFixture) -> None:
        client = mocker.MagicMock()
        client.list_chips.side_effect = QDashApiError("boom")

        with pytest.raises(UpstreamRequestError):
            _gateway(client).discover_chip_ids("active")


class TestFetchTimeseriesRecords:
    """Test suite for QDashGateway.fetch_timeseries_records method."""

    def test_fetch_timeseries_flattens_and_injects_qid(
        self, mocker: MockerFixture
    ) -> None:
        client = mocker.MagicMock()
        client.get_task_results_timeseries.return_value = SimpleNamespace(
            data={
                "0": [_timeseries_point(mocker, {"value": 1.0, "qid_role": "ignored"})],
                "1": [_timeseries_point(mocker, {"value": 2.0, "qid_role": "ignored"})],
            }
        )

        records = _gateway(client).fetch_timeseries_records("c1", "t1", DEFAULT_WINDOW)

        assert records == [
            {"value": 1.0, "qid_role": "0"},
            {"value": 2.0, "qid_role": "1"},
        ]

    def test_fetch_timeseries_formats_window_as_utc_string(
        self, mocker: MockerFixture
    ) -> None:
        client = mocker.MagicMock()
        client.get_task_results_timeseries.return_value = SimpleNamespace(data={})

        _gateway(client, tag="calibration").fetch_timeseries_records(
            "c1", "t1", DEFAULT_WINDOW
        )

        kwargs = client.get_task_results_timeseries.call_args.kwargs
        assert kwargs["start_at"] == format_utc(DEFAULT_WINDOW.from_at)
        assert kwargs["end_at"] == format_utc(DEFAULT_WINDOW.to_at)
        assert kwargs["tag"] == "calibration"

    def test_fetch_timeseries_omits_empty_tag(self, mocker: MockerFixture) -> None:
        client = mocker.MagicMock()
        client.get_task_results_timeseries.return_value = SimpleNamespace(data={})

        _gateway(client, tag="").fetch_timeseries_records("c1", "t1", DEFAULT_WINDOW)

        assert client.get_task_results_timeseries.call_args.kwargs["tag"] is None

    def test_fetch_timeseries_api_error_wrapped(self, mocker: MockerFixture) -> None:
        client = mocker.MagicMock()
        client.get_task_results_timeseries.side_effect = QDashApiError("boom")

        with pytest.raises(UpstreamRequestError):
            _gateway(client).fetch_timeseries_records("c1", "t1", DEFAULT_WINDOW)


class TestNormalizeRecords:
    """Test suite for normalize_records function."""

    def test_normalize_maps_qubit_record_fields(self) -> None:
        raw = [valid_raw_record(qid_role="3", value=45.2, unit="us")]

        records = normalize_records(raw, MetricKind.QUBIT, DEFAULT_WINDOW)

        assert len(records) == 1
        record = records[0]
        assert record.value == pytest.approx(45.2)
        assert record.unit == "us"
        assert record.qubit_id == "3"
        assert record.coupling_id is None

    def test_normalize_maps_coupling_record_with_error_none(self) -> None:
        raw = [valid_raw_record(qid_role="0-1", error=0.8)]

        records = normalize_records(raw, MetricKind.COUPLING, DEFAULT_WINDOW)

        assert records[0].coupling_id == "0-1"
        assert records[0].qubit_id is None
        assert records[0].error is None

    @pytest.mark.parametrize(
        "raw",
        [
            valid_raw_record() | {"calibrated_at": None},
            valid_raw_record() | {"calibrated_at": 123},
            valid_raw_record() | {"calibrated_at": "not-a-date"},
            valid_raw_record(calibrated_at="2026-06-10T23:59:00Z"),
            valid_raw_record() | {"value": None},
            valid_raw_record() | {"value": "x"},
            valid_raw_record() | {"value": True},
            valid_raw_record() | {"value": math.nan},
            valid_raw_record() | {"value": math.inf},
            valid_raw_record() | {"value_type": "str"},
            valid_raw_record() | {"qid_role": None},
            valid_raw_record() | {"qid_role": ""},
        ],
    )
    def test_normalize_skips_invalid_records(self, raw: dict[str, Any]) -> None:
        records = normalize_records([raw], MetricKind.QUBIT, DEFAULT_WINDOW)

        assert records == []

    def test_normalize_naive_calibrated_at_treated_as_utc(self) -> None:
        raw = [valid_raw_record(calibrated_at="2026-06-11T00:30:00")]

        records = normalize_records(raw, MetricKind.QUBIT, DEFAULT_WINDOW)

        assert len(records) == 1

    def test_normalize_error_zero_becomes_none(self) -> None:
        raw = [valid_raw_record(error=0)]

        records = normalize_records(raw, MetricKind.QUBIT, DEFAULT_WINDOW)

        assert records[0].error is None

    def test_normalize_error_non_numeric_keeps_base_record(self) -> None:
        raw = [valid_raw_record(error="oops")]

        records = normalize_records(raw, MetricKind.QUBIT, DEFAULT_WINDOW)

        assert len(records) == 1
        assert records[0].error is None

    def test_normalize_unit_missing_defaults_to_empty(self) -> None:
        raw = [valid_raw_record(unit=None)]

        records = normalize_records(raw, MetricKind.QUBIT, DEFAULT_WINDOW)

        assert records[0].unit == ""
