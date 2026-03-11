from datetime import datetime
from zoneinfo import ZoneInfo

from pytest_mock import MockerFixture

from cryo_metrics_exporter import CustomCollector


class TestParseTime:
    """Test suite for _parse_time method."""

    def test_parse_time_valid_datetime_returns_datetime_object(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-01-26", "12:00:00", "data"]
        line_number = 1
        file_path = "test_file.log"
        collector._tz_smb = ZoneInfo(
            sample_config["sources"]["smb"]["datasource_timezone"]
        )

        # Act
        result = collector._parse_time(columns, line_number, file_path)

        # Assert
        assert result == datetime(
            2026,
            1,
            9,
            12,
            0,
            0,
            tzinfo=collector._tz_smb,
        )

    def test_parse_time_insufficient_columns_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-01-26"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger = mocker.patch("cryo_metrics_exporter.logger.error")

        # Act
        result = collector._parse_time(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger.assert_called_once_with(
            "Insufficient columns for time parsing in %s at line %d",
            file_path,
            line_number,
        )

    def test_parse_time_invalid_datetime_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["32-13-26", "25:00:00", "data"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector._parse_time(columns, line_number, file_path)
        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "Invalid datetime format in %s at line %d",
            file_path,
            line_number,
        )

    def test_parse_time_missing_datetime_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-12", "00:00", "data"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector._parse_time(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "Invalid datetime format in %s at line %d",
            file_path,
            line_number,
        )

    def test_parse_time_invalid_format_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["DD-MM-YY", "HH:MM:SS", "data"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector._parse_time(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "Invalid datetime format in %s at line %d",
            file_path,
            line_number,
        )


class TestParsePressureLine:
    """Test suite for _parse_pressure_line method."""

    def test_parse_pressure_line_valid_line_returns_channel_values(
        self, sample_config: dict, pressure_line_valid: str
    ):
        """Return channel values for valid pressure line."""
        # Arrange
        collector = CustomCollector(sample_config)
        columns = pressure_line_valid.split(",")
        line_number = 1
        file_path = "test_file.log"
        expected = {
            "CH1": 2.00e-02,
            "CH2": 4.89e-01,
            "CH3": 2.18e01,
            "CH4": 1.37e02,
            "CH5": 6.82e02,
            "CH6": 1.01e03,
        }

        # Act
        result = collector._parse_pressure_line(columns, line_number, file_path)

        # Assert
        assert result == expected

    def test_parse_pressure_line_insufficient_columns_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-01-26", "12:00:00"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_error = mocker.patch("cryo_metrics_exporter.logger.error")

        # Act
        result = collector._parse_pressure_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_error.assert_called_once_with(
            "Insufficient columns for pressure parsing in %s at line %d",
            file_path,
            line_number,
        )

    def test_parse_pressure_line_negative_value_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = [
            "09-01-26",
            "12:00:00",
            "CH1",
            "0.0",
            "0.0",
            "-100.5",
            "0.0",
            "0.0",
        ]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_error = mocker.patch("cryo_metrics_exporter.logger.error")

        # Act
        result = collector._parse_pressure_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_error.assert_called_once_with(
            "Invalid pressure value in %s at line %d: %s",
            file_path,
            line_number,
            -100.5,
        )

    def test_parse_pressure_line_invalid_data_type_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = [
            "09-01-26",
            "12:00:00",
            "CH1",
            "0.0",
            "0.0",
            "invalid_data",
            "0.0",
            "0.0",
        ]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector._parse_pressure_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "Invalid pressure data format in %s at line %d",
            file_path,
            line_number,
        )

    def test_parse_pressure_line_invalid_data_format_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = [
            "09-01-26",
            "12:00:00",
            "CH1",
            "0.0",
            "0.0",
            "invalid_data",
            "0.0",
            "0.0",
            "CH2",
            "0.0",
            "0.0",
        ]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector._parse_pressure_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "Invalid pressure data format in %s at line %d",
            file_path,
            line_number,
        )

    def test_parse_pressure_line_device_name_not_found_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = [
            "09-01-26",
            "12:00:00",
            "UNKNOWN_CHANNEL",
            "0.0",
            "0.0",
            "100.5",
            "0.0",
            "0.0",
        ]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_error = mocker.patch("cryo_metrics_exporter.logger.error")

        # Act
        result = collector._parse_pressure_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_error.assert_called_once_with(
            "Devices %s have no data in %s at line %d",
            ["CH1", "CH2", "CH3", "CH4", "CH5", "CH6"],
            file_path,
            line_number,
        )


class TestParseGasflowLine:
    """Test suite for _parse_gasflow_line static method."""

    def test_parse_gasflow_line_valid_line_returns_float_value(
        self, gasflow_line_valid: str, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = gasflow_line_valid.split(",")
        line_number = 1
        file_path = "test_file.log"

        # Act
        result = collector._parse_gasflow_line(columns, line_number, file_path)

        # Assert
        assert isinstance(result, float)
        assert result == 50.5

    def test_parse_gasflow_line_insufficient_columns_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-01-26", "12:00:00"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_error = mocker.patch("cryo_metrics_exporter.logger.error")

        # Act
        result = collector._parse_gasflow_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_error.assert_called_once_with(
            "Insufficient columns for gas flow rate parsing in %s at line %d",
            file_path,
            line_number,
        )

    def test_parse_gasflow_line_negative_value_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-01-26", "12:00:00", "-50.5"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_error = mocker.patch("cryo_metrics_exporter.logger.error")

        # Act
        result = collector._parse_gasflow_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_error.assert_called_once_with(
            "Invalid gas flow rate value in %s at line %d: %s",
            file_path,
            line_number,
            -50.5,
        )

    def test_parse_gasflow_line_invalid_data_type_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-01-26", "12:00:00", "invalid_data"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector._parse_gasflow_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "Invalid gas flow rate data format in %s at line %d",
            file_path,
            line_number,
        )


class TestParseStatusLine:
    """Test suite for _parse_status_line method."""

    def test_parse_status_line_valid_line_returns_device_values(
        self, sample_config: dict, status_line_valid: str
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = status_line_valid.split(",")
        line_number = 1
        file_path = "test_file.log"
        expected = {
            "scroll1": 0,
            "scroll2": 1,
            "turbo1": 0,
            "turbo2": 1,
            "pulsetube": 0,
        }

        # Act
        result = collector._parse_status_line(columns, line_number, file_path)

        # Assert
        assert result == expected

    def test_parse_status_line_insufficient_columns_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-01-26", "12:00:00", "data"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_error = mocker.patch("cryo_metrics_exporter.logger.error")

        # Act
        result = collector._parse_status_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_error.assert_called_once_with(
            "Insufficient columns for machine state parsing in %s at line %d",
            file_path,
            line_number,
        )

    def test_parse_status_line_invalid_value_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-01-26", "12:00:00", "data", "scroll1", "2"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_error = mocker.patch("cryo_metrics_exporter.logger.error")

        # Act
        result = collector._parse_status_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_error.assert_called_once_with(
            "Invalid machine state value in %s at line %d: %s",
            file_path,
            line_number,
            2,
        )

    def test_parse_status_line_invalid_data_type_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-01-26", "12:00:00", "data", "scroll1", "invalid_data"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector._parse_status_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "Invalid machine state data format in %s at line %d",
            file_path,
            line_number,
        )

    def test_parse_status_line_invalid_data_format_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-01-26", "12:00:00", "data", "scroll1", "0", "scroll2"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector._parse_status_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "Invalid machine state data format in %s at line %d",
            file_path,
            line_number,
        )

    def test_parse_status_line_device_name_not_found_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-01-26", "12:00:00", "data", "dummy1", "0", "dummy2", "1"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_error = mocker.patch("cryo_metrics_exporter.logger.error")

        # Act
        result = collector._parse_status_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_error.assert_called_once_with(
            "Devices %s have no data in %s at line %d",
            ["scroll1", "scroll2", "turbo1", "turbo2", "pulsetube"],
            file_path,
            line_number,
        )


class TestParseCompressorLine:
    """Test suite for _parse_compressor_line method."""

    def test_parse_compressor_line_valid_line_returns_compressor_data(
        self, sample_config: dict, compressor_line_valid: str
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = compressor_line_valid.split(",")
        line_number = 1
        file_path = "test_file.log"
        expected_comp = {
            "tc400actualspd": 50.5,
            "tc400actualspd_2": 50.5,
            "tc400actualspd_3": 50.5,
        }
        expected_comp_press = {
            "cpalp": 10.2,
            "cpalp_2": 50.6,
        }

        # Act
        result_comp, result_comp_press = collector._parse_compressor_line(
            columns, line_number, file_path
        )

        # Assert
        assert result_comp == expected_comp
        assert result_comp_press == expected_comp_press

    def test_parse_compressor_line_insufficient_columns_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-01-26", "12:00:00"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_error = mocker.patch("cryo_metrics_exporter.logger.error")

        # Act
        result = collector._parse_compressor_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_error.assert_called_once_with(
            "Insufficient columns for compressor parsing in %s at line %d",
            file_path,
            line_number,
        )

    def test_parse_compressor_line_invalid_data_type_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-01-26", "12:00:00", "data", "scroll1", "invalid_data"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector._parse_compressor_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "Invalid compressor data format in %s at line %d",
            file_path,
            line_number,
        )

    def test_parse_compressor_line_invalid_data_format_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-01-26", "12:00:00", "data", "scroll1", "invalid_data", "scroll2"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector._parse_compressor_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "Invalid compressor data format in %s at line %d",
            file_path,
            line_number,
        )

    def test_parse_compressor_line_device_name_not_found_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        columns = ["09-01-26", "12:00:00", "dummy", "1.0", "dummy", "0"]
        line_number = 1
        file_path = "test_file.log"
        mock_logger_error = mocker.patch("cryo_metrics_exporter.logger.error")

        # Act
        result = collector._parse_compressor_line(columns, line_number, file_path)

        # Assert
        assert result is None
        mock_logger_error.assert_called_once_with(
            "Devices %s have no data in %s at line %d",
            [
                "tc400actualspd",
                "tc400actualspd_2",
                "tc400actualspd_3",
                "cpalp",
                "cpalp_2",
            ],
            file_path,
            line_number,
        )
