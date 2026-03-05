from collections.abc import Callable
from datetime import datetime
from ftplib import all_errors, error_perm, error_temp  # noqa: S402
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest
from pytest_mock import MockerFixture

from cryo_metrics_exporter import CustomCollector, FTPDataSource, InternalServerError


class TestFTPConnect:
    """Test suite for _ftp_connect method."""

    def test_ftp_connect_successful_connection_returns_ftp_object(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_ftp = mocker.MagicMock()
        mock_ftp_class = mocker.patch("cryo_metrics_exporter.FTP")
        mock_ftp_class.return_value = mock_ftp

        # Act
        result, is_retry_needed = collector._ftp_connect()

        # Assert
        assert result is mock_ftp
        assert is_retry_needed is False
        mock_ftp.connect.assert_called_once()
        mock_ftp.login.assert_called_once()
        mock_ftp.set_pasv.assert_called_once()
        mock_ftp.close.assert_not_called()

    def test_ftp_connect_timeout_error_returns_none(
        self,
        mocker: MockerFixture,
        sample_config: dict,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_ftp = mocker.MagicMock()
        mock_ftp.connect.side_effect = TimeoutError()
        mock_ftp_class = mocker.patch("cryo_metrics_exporter.FTP")
        mock_ftp_class.return_value = mock_ftp
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result, is_retry_needed = collector._ftp_connect()

        # Assert
        assert result is None
        assert is_retry_needed is True
        mock_logger_exception.assert_called_once_with(
            "FTP connection timed out to %s:%s as %s (timeout=%s sec)",
            sample_config["sources"]["ftp"]["host"],
            sample_config["sources"]["ftp"]["port"],
            sample_config["sources"]["ftp"]["user"],
            sample_config["sources"]["ftp"]["timeout_sec"],
        )

    def test_ftp_connect_file_not_found_error_returns_none(
        self,
        mocker: MockerFixture,
        sample_config: dict,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_ftp = mocker.MagicMock()
        mock_ftp.login.side_effect = error_perm("550 File not found")  # noqa: S321
        mock_ftp_class = mocker.patch("cryo_metrics_exporter.FTP")
        mock_ftp_class.return_value = mock_ftp
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result, is_retry_needed = collector._ftp_connect()

        # Assert
        assert result is None
        assert is_retry_needed is True
        mock_logger_exception.assert_called_once_with(
            "FTP connection failed to %s:%s as %s (file not found)",
            sample_config["sources"]["ftp"]["host"],
            sample_config["sources"]["ftp"]["port"],
            sample_config["sources"]["ftp"]["user"],
        )

    def test_ftp_connect_invalid_csv_format_error_returns_none(
        self,
        mocker: MockerFixture,
        sample_config: dict,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_ftp = mocker.MagicMock()
        mock_ftp.login.side_effect = error_perm("550 Invalid CSV format")  # noqa: S321
        mock_ftp_class = mocker.patch("cryo_metrics_exporter.FTP")
        mock_ftp_class.return_value = mock_ftp
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result, is_retry_needed = collector._ftp_connect()

        # Assert
        assert result is None
        assert is_retry_needed is False
        mock_logger_exception.assert_called_once_with(
            "FTP connection failed to %s:%s as %s (invalid CSV format)",
            sample_config["sources"]["ftp"]["host"],
            sample_config["sources"]["ftp"]["port"],
            sample_config["sources"]["ftp"]["user"],
        )

    def test_ftp_connect_permanent_error_raises_internal_server_error(
        self,
        mocker: MockerFixture,
        sample_config: dict,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_ftp = mocker.MagicMock()
        mock_ftp.login.side_effect = error_perm()  # noqa: S321
        mock_ftp_class = mocker.patch("cryo_metrics_exporter.FTP")
        mock_ftp_class.return_value = mock_ftp
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act & Assert
        with pytest.raises(InternalServerError):
            collector._ftp_connect()
        mock_logger_exception.assert_called_once_with(
            "A permanent error occurred during FTP connection to %s:%s as %s",
            sample_config["sources"]["ftp"]["host"],
            sample_config["sources"]["ftp"]["port"],
            sample_config["sources"]["ftp"]["user"],
        )

    def test_ftp_connect_general_error_returns_none(
        self,
        mocker: MockerFixture,
        sample_config: dict,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_ftp = mocker.MagicMock()
        mock_ftp.connect.side_effect = all_errors[0]()  # noqa: S321
        mock_ftp_class = mocker.patch("cryo_metrics_exporter.FTP")
        mock_ftp_class.return_value = mock_ftp
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result, is_retry_needed = collector._ftp_connect()

        # Assert
        assert result is None
        assert is_retry_needed is True
        mock_logger_exception.assert_called_once_with(
            "FTP connection failed to %s:%s as %s",
            sample_config["sources"]["ftp"]["host"],
            sample_config["sources"]["ftp"]["port"],
            sample_config["sources"]["ftp"]["user"],
        )


class TestFTPDisconnect:
    """Test suite for ftp_disconnect method."""

    def test_ftp_disconnect_successful_quit(self, mocker: MockerFixture):
        # Arrange
        mock_ftp = mocker.MagicMock()
        mock_ftp.quit.return_value = None

        # Act
        CustomCollector.ftp_disconnect(mock_ftp)

        # Assert
        mock_ftp.quit.assert_called_once()

    def test_ftp_disconnect_quit_fails_uses_close(self, mocker: MockerFixture):
        # Arrange
        mock_ftp = mocker.MagicMock()
        mock_ftp.quit.side_effect = all_errors[0]()  # noqa: S321
        mock_ftp.close.return_value = None
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        CustomCollector.ftp_disconnect(mock_ftp)

        # Assert
        mock_ftp.quit.assert_called_once()
        mock_ftp.close.assert_called_once()
        mock_logger_exception.assert_called_once_with(
            "FTP quit() failed. Using close() fallback."
        )

    def test_ftp_disconnect_both_quit_and_close_fail(self, mocker: MockerFixture):
        # Arrange
        mock_ftp = mocker.MagicMock()
        mock_ftp.quit.side_effect = all_errors[0]()  # noqa: S321
        mock_ftp.close.side_effect = all_errors[0]()  # noqa: S321
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        CustomCollector.ftp_disconnect(mock_ftp)

        # Assert
        mock_ftp.quit.assert_called_once()
        mock_ftp.close.assert_called_once()
        mock_logger_exception.assert_any_call("FTP close() also failed.")


class TestGenerateFilePath:
    """Test suite for generate_file_path method."""

    def test_generate_file_path_pressure_data_returns_one_path(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        tz = ZoneInfo("UTC")
        from_date = datetime(2026, 9, 30, 0, 0, 0, tzinfo=tz)
        to_date = datetime(2026, 9, 30, 23, 59, 59, tzinfo=tz)

        # Act
        result = collector.generate_file_path(
            from_date, to_date, FTPDataSource.PRESSURE
        )

        # Assert
        assert result == ["/log 26-09-30/maxigauge 26-09-30.log"]

    def test_generate_file_path_gass_flow_data_returns_multiple_paths(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        tz = ZoneInfo("UTC")
        from_date = datetime(2025, 12, 31, 0, 0, 0, tzinfo=tz)
        to_date = datetime(2026, 1, 1, 23, 59, 59, tzinfo=tz)

        # Act
        result = collector.generate_file_path(
            from_date, to_date, FTPDataSource.GAS_FLOW_RATE
        )

        # Assert
        assert result == [
            "/log 25-12-31/Flowmeter 25-12-31.log",
            "/log 26-01-01/Flowmeter 26-01-01.log",
        ]

    def test_generate_file_path_machine_state_data_returns_one_path(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        tz = ZoneInfo("UTC")
        from_date = datetime(2026, 1, 1, 0, 0, 0, tzinfo=tz)
        to_date = datetime(2026, 1, 1, 23, 59, 59, tzinfo=tz)

        # Act
        result = collector.generate_file_path(
            from_date, to_date, FTPDataSource.MACHINE_STATE
        )

        # Assert
        assert result == ["/log 26-01-01/Channels 26-01-01.log"]

    def test_generate_file_path_compressor_data_returns_one_path(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        tz = ZoneInfo("UTC")
        from_date = datetime(2026, 1, 1, 0, 0, 0, tzinfo=tz)
        to_date = datetime(2026, 1, 1, 23, 59, 59, tzinfo=tz)

        # Act
        result = collector.generate_file_path(
            from_date, to_date, FTPDataSource.COMPRESSOR
        )

        # Assert
        assert result == ["/log 26-01-01/Status_26-01-01.log"]


class TestFetchFTPFileData:
    """Test suite for fetch_ftp_file_data method."""

    def test_fetch_ftp_file_data_successful_retrieval_returns_lines(
        self, sample_config: dict, mock_ftp: MagicMock
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        lines = ["line1", "line2", "line3"]

        def mock_retrlines(_cmd: str, callback: Callable[[str], None]) -> None:
            for line in lines:
                callback(line)

        mock_ftp.retrlines.side_effect = mock_retrlines

        # Act
        result = collector.fetch_ftp_file_data(mock_ftp, "test.log")

        # Assert
        assert result == lines

    def test_fetch_ftp_file_data_file_not_found_error_returns_none(
        self, mocker: MockerFixture, sample_config: dict, mock_ftp: MagicMock
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        collector.empty_count_ftp = 0
        mock_ftp.retrlines.side_effect = error_perm("550 File not found")  # noqa: S321
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector.fetch_ftp_file_data(mock_ftp, "test.log")

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with("File not found: %s", "test.log")

    def test_fetch_ftp_file_data_permanent_error_raises_internal_server_error(
        self, mocker: MockerFixture, sample_config: dict, mock_ftp: MagicMock
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        collector.empty_count_ftp = 0
        mock_ftp.retrlines.side_effect = error_perm()  # noqa: S321
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act & Assert
        with pytest.raises(InternalServerError):
            collector.fetch_ftp_file_data(mock_ftp, "test.log")
        mock_logger_exception.assert_called_once_with(
            "A permanent FTP error occurred when accessing file: %s", "test.log"
        )

    def test_fetch_ftp_file_data_invalid_csv_format_error_raises_permanent_error(
        self, mocker: MockerFixture, sample_config: dict, mock_ftp: MagicMock
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        collector.empty_count_ftp = 0
        mock_ftp.retrlines.side_effect = error_perm("550 Invalid CSV format")  # noqa: S321
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act & Assert
        with pytest.raises(error_perm):  # noqa: S321
            collector.fetch_ftp_file_data(mock_ftp, "test.log")
        mock_logger_exception.assert_called_once_with(
            "Invalid CSV format in file: %s", "test.log"
        )

    def test_fetch_ftp_file_data_temporary_error_returns_none(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        mock_ftp: MagicMock,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_ftp.retrlines.side_effect = error_temp()  # noqa: S321
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector.fetch_ftp_file_data(mock_ftp, "test.log")

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "A temporary FTP error occurred when accessing file: %s", "test.log"
        )

    def test_fetch_ftp_file_data_timeout_returns_none(
        self, mocker: MockerFixture, sample_config: dict, mock_ftp: MagicMock
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_ftp.retrlines.side_effect = TimeoutError()
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector.fetch_ftp_file_data(mock_ftp, "test.log")

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "FTP connection timed out when accessing file: %s", "test.log"
        )

    def test_fetch_ftp_file_data_general_error_returns_none(
        self, mocker: MockerFixture, sample_config: dict, mock_ftp: MagicMock
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_ftp.retrlines.side_effect = all_errors[0]()  # noqa: S321
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector.fetch_ftp_file_data(mock_ftp, "test.log")

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "FTP connection failed when accessing file: %s", "test.log"
        )
