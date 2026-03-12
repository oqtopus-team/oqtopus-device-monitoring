from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from pytest_mock import MockerFixture
from smbprotocol.exceptions import SMBAuthenticationError, SMBException

from cryo_metrics_exporter import CustomCollector, InternalServerError, SMBDataSource


class TestSMBConnect:
    """Test suite for _smb_connect method."""

    def test_smb_connect_successful_connection_returns_true(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_register = mocker.patch("cryo_metrics_exporter.smbclient.register_session")

        # Act
        success, retry_needed = collector._smb_connect()

        # Assert
        assert success is True
        assert retry_needed is False
        mock_register.assert_called_once_with(
            collector._smb_server,
            username=collector._smb_user,
            password=collector._smb_password,
            port=collector._smb_port,
            connection_timeout=collector._smb_timeout,
        )

    def test_smb_connect_timeout_error_returns_false_with_retry(
        self,
        mocker: MockerFixture,
        sample_config: dict,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mocker.patch(
            "cryo_metrics_exporter.smbclient.register_session",
            side_effect=TimeoutError(),
        )
        mock_disconnect = mocker.patch.object(collector, "smb_disconnect")
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        success, retry_needed = collector._smb_connect()

        # Assert
        assert success is False
        assert retry_needed is True
        mock_logger_exception.assert_called_once_with(
            "SMB connection timed out to %s:%s as %s (timeout=%s sec)",
            collector._smb_server,
            collector._smb_port,
            collector._smb_user,
            collector._smb_timeout,
        )
        mock_disconnect.assert_called_once_with()

    def test_smb_connect_authentication_error_returns_false_with_retry(
        self,
        mocker: MockerFixture,
        sample_config: dict,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mocker.patch(
            "cryo_metrics_exporter.smbclient.register_session",
            side_effect=SMBAuthenticationError(),
        )
        mock_disconnect = mocker.patch.object(collector, "smb_disconnect")
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        success, retry_needed = collector._smb_connect()

        # Assert
        assert success is False
        assert retry_needed is True
        mock_logger_exception.assert_called_once_with(
            "SMB authentication failed to %s:%s as %s",
            collector._smb_server,
            collector._smb_port,
            collector._smb_user,
        )
        mock_disconnect.assert_called_once_with()

    def test_smb_connect_smb_exception_returns_false_with_retry(
        self,
        mocker: MockerFixture,
        sample_config: dict,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mocker.patch(
            "cryo_metrics_exporter.smbclient.register_session",
            side_effect=SMBException(),
        )
        mock_disconnect = mocker.patch.object(collector, "smb_disconnect")
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        success, retry_needed = collector._smb_connect()

        # Assert
        assert success is False
        assert retry_needed is True
        mock_logger_exception.assert_called_once_with(
            "SMB connection failed to %s:%s as %s",
            collector._smb_server,
            collector._smb_port,
            collector._smb_user,
        )
        mock_disconnect.assert_called_once_with()

    def test_smb_connect_os_error_returns_false_with_retry(
        self,
        mocker: MockerFixture,
        sample_config: dict,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mocker.patch(
            "cryo_metrics_exporter.smbclient.register_session",
            side_effect=OSError(),
        )
        mock_disconnect = mocker.patch.object(collector, "smb_disconnect")
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        success, retry_needed = collector._smb_connect()

        # Assert
        assert success is False
        assert retry_needed is True
        mock_logger_exception.assert_called_once_with(
            "SMB connection failed to %s:%s as %s",
            collector._smb_server,
            collector._smb_port,
            collector._smb_user,
        )
        mock_disconnect.assert_called_once_with()

    def test_smb_connect_unexpected_exception_raises_internal_server_error(
        self,
        mocker: MockerFixture,
        sample_config: dict,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mocker.patch(
            "cryo_metrics_exporter.smbclient.register_session",
            side_effect=Exception("unexpected error"),
        )
        mock_disconnect = mocker.patch.object(collector, "smb_disconnect")
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act & Assert
        with pytest.raises(InternalServerError):
            collector._smb_connect()
        mock_logger_exception.assert_called_once_with(
            "SMB connection failed to %s:%s as %s due to unexpected error",
            collector._smb_server,
            collector._smb_port,
            collector._smb_user,
        )
        mock_disconnect.assert_called_once_with()


class TestSMBDisconnect:
    """Test suite for smb_disconnect method."""

    def test_smb_disconnect_successful(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_delete = mocker.patch("cryo_metrics_exporter.smbclient.delete_session")
        mock_logger_info = mocker.patch("cryo_metrics_exporter.logger.info")

        # Act
        collector.smb_disconnect()

        # Assert
        mock_delete.assert_called_once_with(
            collector._smb_server,
            port=collector._smb_port,
        )
        mock_logger_info.assert_called_once_with("SMB session closed.")

    def test_smb_disconnect_failure(self, mocker: MockerFixture, sample_config: dict):
        # Arrange
        collector = CustomCollector(sample_config)
        mocker.patch(
            "cryo_metrics_exporter.smbclient.delete_session",
            side_effect=Exception("connection error"),
        )
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        collector.smb_disconnect()

        # Assert
        mock_logger_exception.assert_called_once_with("SMB session close failed.")


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
            from_date, to_date, SMBDataSource.PRESSURE
        )

        # Assert
        assert result == ["26-09-30/maxigauge 26-09-30.log"]

    def test_generate_file_path_gas_flow_data_returns_multiple_paths(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        tz = ZoneInfo("UTC")
        from_date = datetime(2025, 12, 31, 0, 0, 0, tzinfo=tz)
        to_date = datetime(2026, 1, 1, 23, 59, 59, tzinfo=tz)

        # Act
        result = collector.generate_file_path(
            from_date, to_date, SMBDataSource.GAS_FLOW_RATE
        )

        # Assert
        assert result == [
            "25-12-31/Flowmeter 25-12-31.log",
            "26-01-01/Flowmeter 26-01-01.log",
        ]

    def test_generate_file_path_machine_state_data_returns_one_path_with_base_path(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        collector._smb_base_path = "SMB Test/base"
        tz = ZoneInfo("UTC")
        from_date = datetime(2026, 1, 1, 0, 0, 0, tzinfo=tz)
        to_date = datetime(2026, 1, 1, 23, 59, 59, tzinfo=tz)

        # Act
        result = collector.generate_file_path(
            from_date, to_date, SMBDataSource.MACHINE_STATE
        )

        # Assert
        assert result == ["SMB Test/base/26-01-01/Channels 26-01-01.log"]

    def test_generate_file_path_compressor_data_returns_one_path_with_base_path(
        self, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        collector._smb_base_path = "SMB Test/base/"
        tz = ZoneInfo("UTC")
        from_date = datetime(2026, 1, 1, 0, 0, 0, tzinfo=tz)
        to_date = datetime(2026, 1, 1, 23, 59, 59, tzinfo=tz)

        # Act
        result = collector.generate_file_path(
            from_date, to_date, SMBDataSource.COMPRESSOR
        )

        # Assert
        assert result == ["SMB Test/base/26-01-01/Status_26-01-01.log"]


class TestFetchSMBFileData:
    """Test suite for fetch_smb_file_data method."""

    def test_fetch_smb_file_data_successful_retrieval_returns_lines(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        file_content = "line1\nline2\nline3"
        server = sample_config["sources"]["smb"]["server"]
        share = sample_config["sources"]["smb"]["share"]
        smb_path = f"\\\\{server}\\{share}\\test.log"
        mock_open_file = mocker.patch(
            "cryo_metrics_exporter.smbclient.open_file",
            return_value=mocker.mock_open(read_data=file_content)(),
        )

        # Act
        result = collector.fetch_smb_file_data("test.log")

        # Assert
        assert result == ["line1", "line2", "line3"]
        mock_open_file.assert_called_once_with(smb_path, mode="r", encoding="utf-8")

    def test_fetch_smb_file_data_file_not_found_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        smb_path = f"\\\\{collector._smb_server}\\{collector._smb_share}\\test.log"
        mocker.patch(
            "cryo_metrics_exporter.smbclient.open_file",
            side_effect=FileNotFoundError(),
        )
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector.fetch_smb_file_data("test.log")

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with("File not found: %s", smb_path)

    def test_fetch_smb_file_data_permission_error_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        smb_path = f"\\\\{collector._smb_server}\\{collector._smb_share}\\test.log"
        mocker.patch(
            "cryo_metrics_exporter.smbclient.open_file",
            side_effect=PermissionError(),
        )
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector.fetch_smb_file_data("test.log")

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "A permission error occurred when accessing file: %s", smb_path
        )

    def test_fetch_smb_file_data_timeout_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        smb_path = f"\\\\{collector._smb_server}\\{collector._smb_share}\\test.log"
        mocker.patch(
            "cryo_metrics_exporter.smbclient.open_file",
            side_effect=TimeoutError(),
        )
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector.fetch_smb_file_data("test.log")

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "SMB connection timed out when accessing file: %s", smb_path
        )

    def test_fetch_smb_file_data_smb_exception_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        smb_path = f"\\\\{collector._smb_server}\\{collector._smb_share}\\test.log"
        mocker.patch(
            "cryo_metrics_exporter.smbclient.open_file",
            side_effect=SMBException(),
        )
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector.fetch_smb_file_data("test.log")

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "SMB connection failed when accessing file: %s", smb_path
        )

    def test_fetch_smb_file_data_os_error_returns_none(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        smb_path = f"\\\\{collector._smb_server}\\{collector._smb_share}\\test.log"
        mocker.patch(
            "cryo_metrics_exporter.smbclient.open_file",
            side_effect=OSError(),
        )
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        result = collector.fetch_smb_file_data("test.log")

        # Assert
        assert result is None
        mock_logger_exception.assert_called_once_with(
            "SMB connection failed when accessing file: %s", smb_path
        )

    def test_fetch_smb_file_data_unexpected_exception_raises_internal_server_error(
        self, mocker: MockerFixture, sample_config: dict
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        smb_path = f"\\\\{collector._smb_server}\\{collector._smb_share}\\test.log"
        mocker.patch(
            "cryo_metrics_exporter.smbclient.open_file",
            side_effect=Exception("unexpected error"),
        )
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act & Assert
        with pytest.raises(InternalServerError):
            collector.fetch_smb_file_data("test.log")
        mock_logger_exception.assert_called_once_with(
            "Unexpected error occurred when accessing file: %s", smb_path
        )
