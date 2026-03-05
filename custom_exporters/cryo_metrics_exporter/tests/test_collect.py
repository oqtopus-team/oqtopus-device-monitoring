from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from pytest_mock import MockerFixture

from cryo_metrics_exporter import (
    CustomCollector,
    InternalServerError,
    MetricFamilyType,
    ServiceUnavailableError,
)

DEFAULT_TIME_RANGES = {
    "from_http": datetime(2026, 1, 9, 11, 59, 0, tzinfo=ZoneInfo("UTC")),
    "to_http": datetime(2026, 1, 9, 12, 3, 0, tzinfo=ZoneInfo("UTC")),
    "from_ftp": datetime(2026, 1, 9, 11, 59, 0, tzinfo=ZoneInfo("UTC")),
    "to_ftp": datetime(2026, 1, 9, 12, 3, 0, tzinfo=ZoneInfo("UTC")),
}
EXPECTED_METRICS_NAMES = {
    "refrigerator_temperature",
    "refrigerator_pressure",
    "refrigerator_helium_flow",
    "refrigerator_device_status",
    "refrigerator_compressor",
    "refrigerator_compressor_pressure",
}
TEMPERATURE_DATA_SUCCEEDED_RESPONSE = [
    (
        {
            "labels": {
                "device_name": "test",
                "unit": "kelvin",
                "stage": "plate_50k",
                "location": "flange",
            },
            "values": [300.5, 301.2, 299.8],
            "timestamps": [1704787200, 1704787260, 1704787320],
            "metric_family": MetricFamilyType.TEMPERATURE,
        },
        False,
    ),
    (
        {
            "labels": {
                "device_name": "test",
                "unit": "kelvin",
                "stage": "plate_4k",
                "location": "flange",
            },
            "values": [4.2, 4.3, 4.1],
            "timestamps": [1704787200, 1704787260, 1704787320],
            "metric_family": MetricFamilyType.TEMPERATURE,
        },
        False,
    ),
    (
        {
            "labels": {
                "device_name": "test",
                "unit": "kelvin",
                "stage": "still",
                "location": "flange",
            },
            "values": [0.8, 0.85, 0.75],
            "timestamps": [1704787200, 1704787260, 1704787320],
            "metric_family": MetricFamilyType.TEMPERATURE,
        },
        False,
    ),
    (
        {
            "labels": {
                "device_name": "test",
                "unit": "kelvin",
                "stage": "mxc",
                "location": "flange",
            },
            "values": [1.1, 1.15, 1.05],
            "timestamps": [1704787200, 1704787260, 1704787320],
            "metric_family": MetricFamilyType.TEMPERATURE,
        },
        False,
    ),
]
TEMPERATURE_DATA_FAILED_RESPONSE = [
    (
        {
            "labels": {
                "device_name": "test",
                "unit": "kelvin",
                "stage": "plate_50k",
                "location": "flange",
            },
            "values": [],
            "timestamps": [],
            "metric_family": MetricFamilyType.TEMPERATURE,
        },
        True,
    ),
    (
        {
            "labels": {
                "device_name": "test",
                "unit": "kelvin",
                "stage": "plate_4k",
                "location": "flange",
            },
            "values": [],
            "timestamps": [],
            "metric_family": MetricFamilyType.TEMPERATURE,
        },
        True,
    ),
    (
        {
            "labels": {
                "device_name": "test",
                "unit": "kelvin",
                "stage": "still",
                "location": "flange",
            },
            "values": [],
            "timestamps": [],
            "metric_family": MetricFamilyType.TEMPERATURE,
        },
        True,
    ),
    (
        {
            "labels": {
                "device_name": "test",
                "unit": "kelvin",
                "stage": "mxc",
                "location": "flange",
            },
            "values": [],
            "timestamps": [],
            "metric_family": MetricFamilyType.TEMPERATURE,
        },
        True,
    ),
]
FTP_DATA_SUCCEEDED_RESPONSE = (
    [
        {
            "labels": {
                "device_name": "test",
                "unit": "millibar",
                "location": "before_trap",
            },
            "values": [100.0, 101.5, 99.2],
            "timestamps": [1704787200, 1704787260, 1704787320],
            "metric_family": MetricFamilyType.PRESSURE,
        },
        {
            "labels": {
                "device_name": "test",
                "unit": "millimoles per second",
            },
            "values": [1.2, 1.3, 1.1],
            "timestamps": [1704787200, 1704787260, 1704787320],
            "metric_family": MetricFamilyType.HELIUM_FLOW,
        },
        {
            "labels": {"device_name": "test", "unit": "None", "component": "scroll1"},
            "values": [0, 1, 0],
            "timestamps": [1704787200, 1704787260, 1704787320],
            "metric_family": MetricFamilyType.DEVICE_STATUS,
        },
        {
            "labels": {"device_name": "test", "unit": "Hz", "rotation": "actual_spd"},
            "values": [50.5, 51.2, 49.8],
            "timestamps": [1704787200, 1704787260, 1704787320],
            "metric_family": MetricFamilyType.COMPRESSOR,
        },
        {
            "labels": {
                "device_name": "test",
                "unit": "psig",
                "side": "alp",
            },
            "values": [50.0, 51.2, 49.8],
            "timestamps": [1704787200, 1704787260, 1704787320],
            "metric_family": MetricFamilyType.COMPRESSOR_PRESSURE,
        },
    ],
    False,
)


class TestCollect:
    """Test suite for collect method."""

    def test_collect_successful_retrieval_yields_metric_families(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)

        mocker.patch.object(
            CustomCollector,
            "compute_time_ranges",
            return_value=DEFAULT_TIME_RANGES,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_temperature_data",
            side_effect=TEMPERATURE_DATA_SUCCEEDED_RESPONSE,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_all_ftp_data",
            return_value=FTP_DATA_SUCCEEDED_RESPONSE,
        )
        expected_sample_counts = {
            "refrigerator_temperature": 12,
            "refrigerator_pressure": 6,
            "refrigerator_helium_flow": 6,
            "refrigerator_device_status": 3,
            "refrigerator_compressor": 3,
            "refrigerator_compressor_pressure": 6,
        }

        # Act
        result = list(collector.collect())

        # Assert
        for metric in result:
            assert metric.name in EXPECTED_METRICS_NAMES
            assert len(metric.samples) == expected_sample_counts[metric.name]
        assert collector.empty_count_http == 0
        assert collector.empty_count_ftp == 0

    def test_collect_when_http_data_retrieval_fails_increments_empty_count(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mocker.patch.object(
            collector, "empty_count_ftp", collector._max_expand_windows_ftp - 1
        )

        mock_logger_error = mocker.patch("cryo_metrics_exporter.logger.error")
        mocker.patch.object(
            CustomCollector,
            "compute_time_ranges",
            return_value=DEFAULT_TIME_RANGES,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_temperature_data",
            side_effect=TEMPERATURE_DATA_FAILED_RESPONSE,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_all_ftp_data",
            return_value=FTP_DATA_SUCCEEDED_RESPONSE,
        )
        expected_sample_counts = {
            "refrigerator_temperature": 0,
            "refrigerator_pressure": 6,
            "refrigerator_helium_flow": 6,
            "refrigerator_device_status": 3,
            "refrigerator_compressor": 3,
            "refrigerator_compressor_pressure": 6,
        }

        # Act
        result = list(collector.collect())

        # Assert
        for metric in result:
            assert metric.name in EXPECTED_METRICS_NAMES
            assert len(metric.samples) == expected_sample_counts[metric.name]
        mock_logger_error.assert_called_once_with(
            "No data retrieved from any HTTP data source."
        )
        assert collector.empty_count_http == 1
        assert collector.empty_count_ftp == 0

    def test_collect_when_http_data_retrieval_fails_remains_empty_count(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mocker.patch.object(
            collector, "empty_count_http", collector._max_expand_windows_http - 1
        )

        mock_logger_error = mocker.patch("cryo_metrics_exporter.logger.error")
        mocker.patch.object(
            CustomCollector,
            "compute_time_ranges",
            return_value=DEFAULT_TIME_RANGES,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_temperature_data",
            side_effect=TEMPERATURE_DATA_FAILED_RESPONSE,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_all_ftp_data",
            return_value=FTP_DATA_SUCCEEDED_RESPONSE,
        )
        expected_sample_counts = {
            "refrigerator_temperature": 0,
            "refrigerator_pressure": 6,
            "refrigerator_helium_flow": 6,
            "refrigerator_device_status": 3,
            "refrigerator_compressor": 3,
            "refrigerator_compressor_pressure": 6,
        }

        # Act
        result = list(collector.collect())

        # Assert
        for metric in result:
            assert metric.name in EXPECTED_METRICS_NAMES
            assert len(metric.samples) == expected_sample_counts[metric.name]
        mock_logger_error.assert_called_once_with(
            "No data retrieved from any HTTP data source."
        )
        assert collector.empty_count_http == (collector._max_expand_windows_http - 1)
        assert collector.empty_count_ftp == 0

    def test_collect_when_ftp_data_retrieval_fails_increments_empty_count(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mocker.patch.object(
            collector, "empty_count_http", collector._max_expand_windows_http - 1
        )

        mocker.patch.object(
            CustomCollector,
            "compute_time_ranges",
            return_value=DEFAULT_TIME_RANGES,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_temperature_data",
            side_effect=TEMPERATURE_DATA_SUCCEEDED_RESPONSE,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_all_ftp_data",
            return_value=([], True),
        )
        expected_sample_counts = {
            "refrigerator_temperature": 12,
            "refrigerator_pressure": 0,
            "refrigerator_helium_flow": 0,
            "refrigerator_device_status": 0,
            "refrigerator_compressor": 0,
            "refrigerator_compressor_pressure": 0,
        }

        # Act
        result = list(collector.collect())

        # Assert
        for metric in result:
            assert metric.name in EXPECTED_METRICS_NAMES
            assert len(metric.samples) == expected_sample_counts[metric.name]
        assert collector.empty_count_http == 0
        assert collector.empty_count_ftp == 1

    def test_collect_when_ftp_data_retrieval_fails_remains_empty_count(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mocker.patch.object(
            collector, "empty_count_ftp", collector._max_expand_windows_ftp - 1
        )

        mocker.patch.object(
            CustomCollector,
            "compute_time_ranges",
            return_value=DEFAULT_TIME_RANGES,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_temperature_data",
            side_effect=TEMPERATURE_DATA_SUCCEEDED_RESPONSE,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_all_ftp_data",
            return_value=([], True),
        )
        expected_sample_counts = {
            "refrigerator_temperature": 12,
            "refrigerator_pressure": 0,
            "refrigerator_helium_flow": 0,
            "refrigerator_device_status": 0,
            "refrigerator_compressor": 0,
            "refrigerator_compressor_pressure": 0,
        }

        # Act
        result = list(collector.collect())

        # Assert
        for metric in result:
            assert metric.name in EXPECTED_METRICS_NAMES
            assert len(metric.samples) == expected_sample_counts[metric.name]
        assert collector.empty_count_http == 0
        assert collector.empty_count_ftp == (collector._max_expand_windows_ftp - 1)

    def test_collect_internal_server_error_occured_in_http_raises_internal_server_error(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)

        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")
        mocker.patch.object(
            CustomCollector,
            "compute_time_ranges",
            return_value=DEFAULT_TIME_RANGES,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_temperature_data",
            side_effect=InternalServerError(),
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_all_ftp_data",
            return_value=([], True),
        )

        # Act & Assert
        with pytest.raises(InternalServerError):
            list(collector.collect())
        mock_logger_exception.assert_called_once_with(
            "Internal Server Error occurred during HTTP data retrieval."
        )
        assert collector.empty_count_http == 0
        assert collector.empty_count_ftp == 1

    def test_collect_internal_server_error_occured_in_ftp_raises_internal_server_error(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)

        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")
        mocker.patch.object(
            CustomCollector,
            "compute_time_ranges",
            return_value=DEFAULT_TIME_RANGES,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_temperature_data",
            side_effect=TEMPERATURE_DATA_FAILED_RESPONSE,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_all_ftp_data",
            side_effect=InternalServerError(),
        )

        # Act & Assert
        with pytest.raises(InternalServerError):
            list(collector.collect())
        mock_logger_exception.assert_called_once_with(
            "Internal Server Error occurred during FTP data retrieval."
        )
        assert collector.empty_count_http == 1
        assert collector.empty_count_ftp == 0

    def test_collect_all_data_sources_failed_raises_internal_server_error(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)

        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")
        mocker.patch.object(
            CustomCollector,
            "compute_time_ranges",
            return_value=DEFAULT_TIME_RANGES,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_temperature_data",
            side_effect=InternalServerError(),
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_all_ftp_data",
            side_effect=InternalServerError(),
        )

        # Act & Assert
        with pytest.raises(InternalServerError):
            list(collector.collect())
        mock_logger_exception.assert_any_call(
            "Internal Server Error occurred during HTTP data retrieval."
        )
        mock_logger_exception.assert_any_call(
            "Internal Server Error occurred during FTP data retrieval."
        )
        assert collector.empty_count_http == 0
        assert collector.empty_count_ftp == 0

    def test_collect_all_data_sources_failed_raises_service_unavailable_error(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)

        mock_logger_error = mocker.patch("cryo_metrics_exporter.logger.error")
        mocker.patch.object(
            CustomCollector,
            "compute_time_ranges",
            return_value=DEFAULT_TIME_RANGES,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_temperature_data",
            side_effect=TEMPERATURE_DATA_FAILED_RESPONSE,
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_all_ftp_data",
            return_value=([], True),
        )

        # Act & Assert
        with pytest.raises(ServiceUnavailableError):
            list(collector.collect())
        mock_logger_error.assert_any_call("All data sources failed to provide data.")
        assert collector.empty_count_http == 1
        assert collector.empty_count_ftp == 1
