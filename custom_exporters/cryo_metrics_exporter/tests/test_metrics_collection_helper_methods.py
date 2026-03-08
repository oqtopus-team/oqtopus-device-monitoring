from datetime import datetime, timedelta
from typing import Any

import pytest
from cryo_metrics_exporter import (
    METRIC_CONFIGS,
    CustomCollector,
    InternalServerError,
    MetricFamilyType,
)
from prometheus_client.core import GaugeMetricFamily
from pytest_mock import MockerFixture


class TestComputeTimeRanges:
    """Test suite for compute_time_ranges method."""

    def test_compute_time_ranges_returns_correct_datetime_ranges(
        self,
        mocker: MockerFixture,
        sample_config: dict,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        now = datetime.now(collector._tz_exporter).replace(microsecond=0)
        mocker.patch("cryo_metrics_exporter.datetime").now.return_value = now
        expected = {
            "from_http": now - timedelta(seconds=60),
            "to_http": now,
            "from_smb": now - timedelta(seconds=60),
            "to_smb": now,
        }

        # Act
        result = collector.compute_time_ranges()

        # Assert
        assert result == expected

    def test_compute_time_ranges_respects_max_expand_windows(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        collector.empty_count_http = 5
        collector.empty_count_smb = 5

        now = datetime.now(collector._tz_exporter).replace(microsecond=0)
        mocker.patch("cryo_metrics_exporter.datetime").now.return_value = now

        expected = {
            "from_http": now - timedelta(seconds=60 * 5),
            "to_http": now,
            "from_smb": now - timedelta(seconds=60 * 5),
            "to_smb": now,
        }

        # Act
        result = collector.compute_time_ranges()

        # Assert
        assert result == expected


class TestSetupMetricFamilies:
    """Test suite for setup_metric_families static method."""

    def test_setup_metric_families_returns_all_metric_families(self):
        # Arrange & Act
        result = CustomCollector.setup_metric_families()

        # Assert
        for key in result:
            assert isinstance(key, MetricFamilyType)
            assert isinstance(result[key], GaugeMetricFamily)
        assert set(result.keys()) == set(METRIC_CONFIGS.keys())


class TestAddMetrics:
    """Test suite for _add_metrics static method."""

    def test_add_metrics_adds_single_metric(self):
        # Arrange
        metric_family = GaugeMetricFamily(
            "test_metric", "Test metric", labels=["device", "unit"]
        )
        data = {
            "labels": {"device": "test-device", "unit": "kelvin"},
            "values": [300.5],
            "timestamps": [1704787200],
        }

        # Act
        CustomCollector._add_metrics(data, metric_family)

        # Assert
        assert metric_family.samples[0].value == 300.5
        assert metric_family.samples[0].timestamp == 1704787200
        for label_name, label_value in data["labels"].items():
            assert metric_family.samples[0].labels[label_name] == label_value

    def test_add_metrics_adds_multiple_metrics(self):
        # Arrange
        metric_family = GaugeMetricFamily(
            "test_metric", "Test metric", labels=["device", "unit"]
        )
        data = {
            "labels": {"device": "test-device", "unit": "kelvin"},
            "values": [300.5, 301.2, 299.8],
            "timestamps": [1704787200, 1704787260, 1704787320],
        }

        # Act
        CustomCollector._add_metrics(data, metric_family)

        # Assert
        for i in range(3):
            assert metric_family.samples[i].value == data["values"][i]
            assert metric_family.samples[i].timestamp == data["timestamps"][i]
        for label_name, label_value in data["labels"].items():
            assert metric_family.samples[0].labels[label_name] == label_value


class TestProcessSMBData:
    """Test suite for _process_smb_data method."""

    def test_process_smb_data_adds_pressure_metrics_with_unit_conversions(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        metric_families = CustomCollector.setup_metric_families()
        mock_add_metrics = mocker.patch.object(CustomCollector, "_add_metrics")

        smb_results = [
            {
                "labels": {
                    "device_name": "test",
                    "unit": "millibar",
                    "location": "before_trap",
                },
                "values": [100.0],
                "timestamps": [1704787200],
                "metric_family": "pressure",
            }
        ]

        # Act
        collector._process_smb_data(smb_results, metric_families)

        # Assert
        assert mock_add_metrics.call_count == 2

        first_call_args = mock_add_metrics.call_args_list[0][0]
        second_call_args = mock_add_metrics.call_args_list[1][0]
        assert first_call_args[0]["labels"]["unit"] == "millibar"
        assert second_call_args[0]["labels"]["unit"] in {"kilopascal", "pascal"}

    def test_process_smb_data_adds_flow_metrics_with_unit_conversions(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        metric_families = CustomCollector.setup_metric_families()
        mock_add_metrics = mocker.patch.object(CustomCollector, "_add_metrics")

        smb_results = [
            {
                "labels": {
                    "device_name": "test",
                    "unit": "millimoles per second",
                },
                "values": [1.0],
                "timestamps": [1704787200],
                "metric_family": "helium_flow",
            }
        ]

        # Act
        collector._process_smb_data(smb_results, metric_families)

        # Assert
        assert mock_add_metrics.call_count == 2

        first_call_args = mock_add_metrics.call_args_list[0][0]
        second_call_args = mock_add_metrics.call_args_list[1][0]
        assert first_call_args[0]["labels"]["unit"] == "millimoles per second"
        assert second_call_args[0]["labels"]["unit"] == "micromoles per second"

    def test_process_smb_data_adds_compressor_pressure_with_unit_conversions(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        metric_families = CustomCollector.setup_metric_families()
        mock_add_metrics = mocker.patch.object(CustomCollector, "_add_metrics")

        smb_results = [
            {
                "labels": {
                    "device_name": "test",
                    "unit": "psig",
                    "side": "alp",
                },
                "values": [100.0],
                "timestamps": [1704787200],
                "metric_family": "compressor_pressure",
            }
        ]

        # Act
        collector._process_smb_data(smb_results, metric_families)

        # Assert
        assert mock_add_metrics.call_count == 2

        first_call_args = mock_add_metrics.call_args_list[0][0]
        second_call_args = mock_add_metrics.call_args_list[1][0]
        assert first_call_args[0]["labels"]["unit"] == "psig"
        assert second_call_args[0]["labels"]["unit"] == "megapascal"

    def test_process_smb_data_skips_empty_smb_results(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        metric_families = CustomCollector.setup_metric_families()
        mock_add_metrics = mocker.patch.object(CustomCollector, "_add_metrics")

        smb_results: list[dict[str, Any]] = []

        # Act
        collector._process_smb_data(smb_results, metric_families)

        # Assert
        mock_add_metrics.assert_not_called()


class TestProcessHTTPData:
    """Test suite for _process_http_data method."""

    def test_process_http_data_with_data_returns_retry_flags_and_count(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        metric_families = CustomCollector.setup_metric_families()
        mock_add_metrics = mocker.patch.object(CustomCollector, "_add_metrics")
        mock_logger_info = mocker.patch("cryo_metrics_exporter.logger.info")

        result_data = {
            "labels": {
                "device_name": "test",
                "unit": "kelvin",
                "stage": "mxc",
                "location": "flange",
                "raw": "true",
            },
            "values": [0.01, 0.02, 0.03],
            "timestamps": [1704787200, 1704787260, 1704787320],
            "metric_family": MetricFamilyType.TEMPERATURE,
        }
        mock_future = mocker.MagicMock()
        mock_future.result.return_value = (result_data, False)
        http_futures = [mock_future]

        # Act
        retry_flags, data_count = collector._process_http_data(
            http_futures, metric_families
        )

        # Assert
        assert retry_flags == [False]
        assert data_count == 3
        mock_add_metrics.assert_called_once_with(
            result_data, metric_families[MetricFamilyType.TEMPERATURE]
        )
        mock_logger_info.assert_called_once_with(
            "Successfully retrieved %d records from HTTP data sources.",
            3,
        )

    def test_process_http_data_partial_data_returns_correct_flags_and_count(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        metric_families = CustomCollector.setup_metric_families()
        mock_add_metrics = mocker.patch.object(CustomCollector, "_add_metrics")

        result_with_data = {
            "labels": {
                "device_name": "test",
                "unit": "kelvin",
                "stage": "plate_50k",
                "location": "flange",
                "raw": "true",
            },
            "values": [300.5, 301.2],
            "timestamps": [1704787200, 1704787260],
            "metric_family": MetricFamilyType.TEMPERATURE,
        }
        result_empty = {
            "labels": {
                "device_name": "test",
                "unit": "kelvin",
                "stage": "plate_4k",
                "location": "flange",
                "raw": "true",
            },
            "values": [],
            "timestamps": [],
            "metric_family": MetricFamilyType.TEMPERATURE,
        }
        mock_future_with_data = mocker.MagicMock()
        mock_future_with_data.result.return_value = (result_with_data, False)
        mock_future_empty = mocker.MagicMock()
        mock_future_empty.result.return_value = (result_empty, True)
        http_futures = [mock_future_with_data, mock_future_empty]

        # Act
        retry_flags, data_count = collector._process_http_data(
            http_futures, metric_families
        )

        # Assert
        assert retry_flags == [False, True]
        assert data_count == 2
        mock_add_metrics.assert_called_once_with(
            result_with_data, metric_families[MetricFamilyType.TEMPERATURE]
        )

    def test_process_http_data_multiple_futures_all_empty_no_retry_logs_error(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        metric_families = CustomCollector.setup_metric_families()
        mock_add_metrics = mocker.patch.object(CustomCollector, "_add_metrics")
        mock_logger_error = mocker.patch("cryo_metrics_exporter.logger.error")

        result_empty_1 = {
            "labels": {
                "device_name": "test",
                "unit": "kelvin",
                "stage": "plate_50k",
                "location": "flange",
                "raw": "true",
            },
            "values": [],
            "timestamps": [],
            "metric_family": MetricFamilyType.TEMPERATURE,
        }
        result_empty_2 = {
            "labels": {
                "device_name": "test",
                "unit": "kelvin",
                "stage": "plate_4k",
                "location": "flange",
                "raw": "true",
            },
            "values": [],
            "timestamps": [],
            "metric_family": MetricFamilyType.TEMPERATURE,
        }
        mock_future_1 = mocker.MagicMock()
        mock_future_1.result.return_value = (result_empty_1, False)
        mock_future_2 = mocker.MagicMock()
        mock_future_2.result.return_value = (result_empty_2, False)
        http_futures = [mock_future_1, mock_future_2]

        # Act
        retry_flags, data_count = collector._process_http_data(
            http_futures, metric_families
        )

        # Assert
        assert retry_flags == [False, False]
        assert data_count == 0
        mock_add_metrics.assert_not_called()
        mock_logger_error.assert_called_once_with(
            "No data retrieved from any HTTP data source."
        )

    def test_process_http_data_raises_internal_server_error_when_future_raises(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        metric_families = CustomCollector.setup_metric_families()

        mock_future = mocker.MagicMock()
        mock_future.result.side_effect = InternalServerError
        http_futures = [mock_future]

        # Act & Assert
        with pytest.raises(InternalServerError):
            collector._process_http_data(http_futures, metric_families)


class TestUpdateEmptyCounts:
    """Test suite for _update_empty_counts method."""

    def test_update_empty_counts_http_retry_flag_increments_empty_count_http(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        collector.empty_count_http = 0
        collector.empty_count_smb = 0

        # Act
        collector._update_empty_counts(
            [True, False],
            is_smb_retry_needed=False,
            is_http_internal_server_error=False,
            is_smb_internal_server_error=False,
            http_data_count=0,
        )

        # Assert
        assert collector.empty_count_http == 1
        assert collector.empty_count_smb == 0

    def test_update_empty_counts_http_retry_flag_caps_at_max_expand_windows_minus_1(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        collector.empty_count_http = 4
        collector.empty_count_smb = 0

        # Act
        collector._update_empty_counts(
            [True],
            is_smb_retry_needed=False,
            is_http_internal_server_error=False,
            is_smb_internal_server_error=False,
            http_data_count=0,
        )

        # Assert
        assert collector.empty_count_http == 4

    def test_update_empty_counts_http_data_received_resets_empty_count_http(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        collector.empty_count_http = 3
        collector.empty_count_smb = 0

        # Act
        collector._update_empty_counts(
            [False],
            is_smb_retry_needed=False,
            is_http_internal_server_error=False,
            is_smb_internal_server_error=False,
            http_data_count=5,
        )

        # Assert
        assert collector.empty_count_http == 0

    def test_update_empty_counts_http_internal_server_error_does_not_update(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        collector.empty_count_http = 2
        collector.empty_count_smb = 0

        # Act
        collector._update_empty_counts(
            [True],
            is_smb_retry_needed=False,
            is_http_internal_server_error=True,
            is_smb_internal_server_error=False,
            http_data_count=0,
        )

        # Assert
        assert collector.empty_count_http == 2

    def test_update_empty_counts_http_no_retry_no_data_does_not_change_count(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        collector.empty_count_http = 2
        collector.empty_count_smb = 0

        # Act
        collector._update_empty_counts(
            [False],
            is_smb_retry_needed=False,
            is_http_internal_server_error=False,
            is_smb_internal_server_error=False,
            http_data_count=0,
        )

        # Assert
        assert collector.empty_count_http == 2

    def test_update_empty_counts_smb_retry_needed_increments_empty_count_smb(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        collector.empty_count_http = 0
        collector.empty_count_smb = 0

        # Act
        collector._update_empty_counts(
            [],
            is_smb_retry_needed=True,
            is_http_internal_server_error=False,
            is_smb_internal_server_error=False,
            http_data_count=0,
        )

        # Assert
        assert collector.empty_count_smb == 1
        assert collector.empty_count_http == 0

    def test_update_empty_counts_smb_retry_caps_at_max_expand_windows_minus_1(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        collector.empty_count_http = 0
        collector.empty_count_smb = 4

        # Act
        collector._update_empty_counts(
            [],
            is_smb_retry_needed=True,
            is_http_internal_server_error=False,
            is_smb_internal_server_error=False,
            http_data_count=0,
        )

        # Assert
        assert collector.empty_count_smb == 4

    def test_update_empty_counts_smb_no_retry_resets_empty_count_smb(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        collector.empty_count_http = 0
        collector.empty_count_smb = 3

        # Act
        collector._update_empty_counts(
            [],
            is_smb_retry_needed=False,
            is_http_internal_server_error=False,
            is_smb_internal_server_error=False,
            http_data_count=0,
        )

        # Assert
        assert collector.empty_count_smb == 0

    def test_update_empty_counts_smb_internal_server_error_does_not_update(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        collector.empty_count_http = 0
        collector.empty_count_smb = 2

        # Act
        collector._update_empty_counts(
            [],
            is_smb_retry_needed=True,
            is_http_internal_server_error=False,
            is_smb_internal_server_error=True,
            http_data_count=0,
        )

        # Assert
        assert collector.empty_count_smb == 2
