from zoneinfo import ZoneInfo

import pytest
from pytest_mock import MockerFixture

from cryo_metrics_exporter import (
    MetricFamilyType,
    convert_flow_unit,
    convert_pressure_unit,
    convert_psig_unit,
    get_timezone,
)


class TestConvertPressureUnit:
    """Test suite for convert_pressure_unit function."""

    @pytest.mark.parametrize(
        ("location", "expected_multiplier", "expected_unit"),
        [
            ("before_trap", 0.1, "kilopascal"),
            ("after_trap", 0.1, "kilopascal"),
            ("tank", 0.1, "kilopascal"),
        ],
    )
    def test_convert_pressure_unit_converts_millibar_to_kilopascal(
        self, location: str, expected_multiplier: int, expected_unit: str
    ):
        # Arrange
        data = {
            "labels": {
                "device_name": "test",
                "unit": "millibar",
                "location": location,
                "raw": "true",
            },
            "values": [100.0, 200.0],
            "timestamps": [1620000000, 1620000600],
            "metric_family": MetricFamilyType.PRESSURE,
        }
        expected_data = {
            "labels": {
                "device_name": "test",
                "unit": expected_unit,
                "location": location,
                "raw": "false",
            },
            "values": [v * expected_multiplier for v in data["values"]],
            "timestamps": data["timestamps"],
            "metric_family": MetricFamilyType.PRESSURE,
        }

        # Act
        result = convert_pressure_unit(data)

        # Assert
        assert result == expected_data
        assert result is not data  # Deep copy

    @pytest.mark.parametrize(
        ("location", "expected_multiplier", "expected_unit"),
        [
            ("chamber_internal", 100, "pascal"),
            ("exhaust_pump", 100, "pascal"),
        ],
    )
    def test_convert_pressure_unit_converts_millibar_to_pascal(
        self, location: str, expected_multiplier: int, expected_unit: str
    ):
        # Arrange
        data = {
            "labels": {
                "device_name": "test",
                "unit": "millibar",
                "location": location,
                "raw": "true",
            },
            "values": [100.0, 200.0],
            "timestamps": [1620000000, 1620000600],
            "metric_family": MetricFamilyType.PRESSURE,
        }
        expected_data = {
            "labels": {
                "device_name": "test",
                "unit": expected_unit,
                "location": location,
                "raw": "false",
            },
            "values": [v * expected_multiplier for v in data["values"]],
            "timestamps": data["timestamps"],
            "metric_family": MetricFamilyType.PRESSURE,
        }

        # Act
        result = convert_pressure_unit(data)

        # Assert
        assert result == expected_data
        assert result is not data  # Deep copy


class TestConvertFlowUnit:
    """Test suite for convert_flow_unit function."""

    def test_convert_flow_unit_converts_millimoles_to_micromoles(self):
        # Arrange
        data = {
            "labels": {
                "device_name": "test",
                "unit": "millimoles per second",
                "raw": "true",
            },
            "values": [100.0, 200.0],
            "timestamps": [1620000000, 1620000600],
            "metric_family": MetricFamilyType.HELIUM_FLOW,
        }
        expected_multiplier = 1000
        expected_unit = "micromoles per second"
        expected_data = {
            "labels": {
                "device_name": "test",
                "unit": expected_unit,
                "raw": "false",
            },
            "values": [v * expected_multiplier for v in data["values"]],
            "timestamps": [1620000000, 1620000600],
            "metric_family": MetricFamilyType.HELIUM_FLOW,
        }

        # Act
        result = convert_flow_unit(data)

        # Assert
        assert result == expected_data
        assert result is not data


class TestConvertPsigUnit:
    """Test suite for convert_psig_unit function."""

    def test_convert_psig_unit_converts_to_megapascal(self):
        # Arrange
        data = {
            "labels": {
                "device_name": "test",
                "unit": "psig",
                "side": "alp",
                "raw": "true",
            },
            "values": [100.0, 200.0],
            "timestamps": [1620000000, 1620000600],
            "metric_family": MetricFamilyType.COMPRESSOR_PRESSURE,
        }
        expected_multiplier = 0.006894744825494
        expected_unit = "megapascal"
        expected_data = {
            "labels": {
                "device_name": "test",
                "unit": expected_unit,
                "side": "alp",
                "raw": "false",
            },
            "values": [v * expected_multiplier for v in data["values"]],
            "timestamps": [1620000000, 1620000600],
            "metric_family": MetricFamilyType.COMPRESSOR_PRESSURE,
        }

        # Act
        result = convert_psig_unit(data)

        # Assert
        assert result == expected_data
        assert result is not data


class TestGetTimezone:
    """Test suite for get_timezone function."""

    def test_get_timezone_valid_timezone_returns_zoneinfo(self):
        # Arrange
        tz_name = "Asia/Tokyo"

        # Act
        result = get_timezone(tz_name, "UTC")

        # Assert
        assert isinstance(result, ZoneInfo)
        assert result.key == tz_name

    def test_get_timezone_invalid_timezone_returns_default(self, mocker: MockerFixture):
        # Arrange
        tz_name = "invalid/timezone"
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = get_timezone(tz_name, "UTC")

        # Assert
        assert isinstance(result, ZoneInfo)
        assert result.key == "UTC"
        mock_logger_exception.assert_called_once_with(
            "Invalid timezone: %s. Using default timezone: %s.",
            tz_name,
            "UTC",
        )

    def test_get_timezone_empty_string_returns_default(self, mocker: MockerFixture):
        # Arrange
        tz_name = ""
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")
        # Act
        result = get_timezone(tz_name, "UTC")

        # Assert
        assert isinstance(result, ZoneInfo)
        assert result.key == "UTC"
        mock_logger_exception.assert_called_once_with(
            "Invalid timezone: %s. Using default timezone: %s.",
            tz_name,
            "UTC",
        )
