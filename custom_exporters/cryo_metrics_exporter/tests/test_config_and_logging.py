import copy
import logging
from zoneinfo import ZoneInfo

import pytest
from cryo_metrics_exporter import create_timezone_formatter, setup_config, setup_logging
from omegaconf import OmegaConf
from pytest_mock import MockerFixture


def get_base_config() -> dict:
    return {
        "exporter": {
            "device_name": "default-device",
            "port": 9101,
            "timezone": "UTC",
        },
        "retrieval": {
            "scrape_interval_sec": 60,
            "max_expand_windows": {
                "http": 5,
                "smb": 5,
            },
        },
        "sources": {
            "http": {
                "url": "http://localhost",
                "port": 80,
                "timeout_sec": 5,
                "datasource_timezone": "UTC",
            },
            "smb": {
                "server": "localhost",
                "share": "share_name",
                "port": 445,
                "username": "default-user",
                "base_path": "",
                "timeout_sec": 5,
                "datasource_timezone": "UTC",
            },
        },
    }


def apply_config_overrides(base: dict, overrides: dict) -> dict:
    result = copy.deepcopy(base)

    def update_nested(target: dict, updates: dict) -> None:
        for key, value in updates.items():
            if isinstance(value, dict) and key in target:
                update_nested(target[key], value)
            else:
                target[key] = value

    update_nested(result, overrides)
    return result


class TestSetupConfig:
    """Test suite for setup_config function."""

    def test_setup_config_loads_yaml_without_env_overrides(self, mocker: MockerFixture):
        # Arrange
        base = get_base_config()
        mock_load = mocker.patch("cryo_metrics_exporter.OmegaConf.load")
        mock_load.return_value = OmegaConf.create(base)
        mocker.patch.dict("os.environ", {}, clear=True)
        expected = apply_config_overrides(base, {})

        # Act
        result = setup_config()

        # Assert
        assert result == expected

    def test_setup_config_loads_yaml_and_applies_env_overrides(
        self, mocker: MockerFixture
    ):
        # Arrange
        base = get_base_config()
        mock_load = mocker.patch("cryo_metrics_exporter.OmegaConf.load")
        mock_load.return_value = OmegaConf.create(base)
        env_vars = {
            "EXPORTER_PORT": "9102",
            "EXPORTER_TIMEZONE": "Asia/Tokyo",
            "EXPORTER_DEVICE_NAME": "env-device",
            "RETRIEVAL_SCRAPE_INTERVAL_SEC": "120",
            "RETRIEVAL_MAX_EXPAND_WINDOWS_HTTP": "10",
            "RETRIEVAL_MAX_EXPAND_WINDOWS_SMB": "10",
            "SOURCES_HTTP_DATASOURCE_TIMEZONE": "Asia/Tokyo",
            "SOURCES_HTTP_URL": "http://env-host",
            "SOURCES_HTTP_PORT": "8080",
            "SOURCES_HTTP_TIMEOUT_SEC": "10",
            "SOURCES_SMB_DATASOURCE_TIMEZONE": "Asia/Tokyo",
            "SOURCES_SMB_SERVER": "env-host",
            "SOURCES_SMB_SHARE": "env-share",
            "SOURCES_SMB_PORT": "445",
            "SOURCES_SMB_USERNAME": "env-user",
            "SOURCES_SMB_BASE_PATH": "/env/path",
            "SOURCES_SMB_TIMEOUT_SEC": "10",
        }
        config_overrides = {
            "exporter": {
                "device_name": "env-device",
                "port": 9102,
                "timezone": "Asia/Tokyo",
            },
            "retrieval": {
                "scrape_interval_sec": 120,
                "max_expand_windows": {
                    "http": 10,
                    "smb": 10,
                },
            },
            "sources": {
                "http": {
                    "url": "http://env-host",
                    "port": 8080,
                    "timeout_sec": 10,
                    "datasource_timezone": "Asia/Tokyo",
                },
                "smb": {
                    "server": "env-host",
                    "share": "env-share",
                    "username": "env-user",
                    "port": 445,
                    "base_path": "/env/path",
                    "timeout_sec": 10,
                    "datasource_timezone": "Asia/Tokyo",
                },
            },
        }
        expected = apply_config_overrides(base, config_overrides)
        mocker.patch.dict("os.environ", env_vars, clear=True)

        # Act
        result = setup_config()

        # Assert
        assert result == expected

    def test_setup_config_loads_yaml_with_only_required_parameters(
        self, mocker: MockerFixture
    ):
        # Arrange
        base = get_base_config()
        minimal_config = {
            "exporter": {"device_name": "default-device"},
            "sources": {
                "http": {"url": "http://localhost"},
                "smb": {
                    "server": "localhost",
                    "share": "share_name",
                    "username": "default-user",
                },
            },
        }
        mock_load = mocker.patch("cryo_metrics_exporter.OmegaConf.load")
        mock_load.return_value = OmegaConf.create(minimal_config)
        mocker.patch.dict("os.environ", {}, clear=True)
        expected = apply_config_overrides(base, {})

        # Act
        result = setup_config()

        # Assert
        assert result == expected

    def test_setup_config_missing_required_parameter_raises_key_error(
        self, mocker: MockerFixture
    ):
        # Arrange
        invalid_config = OmegaConf.create({
            "exporter": {"device_name": None},
            "sources": {
                "http": {"url": None},
                "smb": {"server": None, "share": None, "username": None},
            },
        })
        mock_load = mocker.patch("cryo_metrics_exporter.OmegaConf.load")
        mock_load.return_value = invalid_config
        mocker.patch.dict("os.environ", {}, clear=True)

        # Act / Assert
        with pytest.raises(KeyError):
            setup_config()


class TestCreateTimezoneFormatter:
    """Test suite for create_timezone_formatter function."""

    def test_create_timezone_formatter_with_datefmt_formats_time_in_timezone(self):
        # Arrange
        tz = ZoneInfo("Asia/Tokyo")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        record.created = 0.0  # 1970-01-01 00:00:00 UTC

        # Act
        formatter_class = create_timezone_formatter(logging.Formatter, tz)
        formatter = formatter_class()
        result = formatter.formatTime(record, "%Y-%m-%d %H:%M:%S")

        # Assert
        assert result == "1970-01-01 09:00:00"

    def test_create_timezone_formatter_without_datefmt_formats_iso_in_timezone(self):
        # Arrange
        tz = ZoneInfo("Asia/Tokyo")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        record.created = 0.0  # 1970-01-01 00:00:00 UTC

        # Act
        formatter_class = create_timezone_formatter(logging.Formatter, tz)
        formatter = formatter_class()
        result = formatter.formatTime(record, None)

        # Assert
        assert result.startswith("1970-01-01T09:00:00")


class TestSetupLogging:
    """Test suite for setup_logging function."""

    def test_setup_logging_with_yaml_file(self, mocker: MockerFixture):
        # Arrange
        tz = ZoneInfo("Asia/Tokyo")
        config = {
            "version": 1,
            "formatters": {
                "fmt": {
                    "class": "logging.Formatter",
                    "format": "%(message)s",
                }
            },
            "handlers": {},
            "loggers": {},
        }
        mock_exists = mocker.patch("cryo_metrics_exporter.pathlib.Path.exists")
        mock_open = mocker.patch("cryo_metrics_exporter.pathlib.Path.open")
        mock_dictconfig = mocker.patch(
            "cryo_metrics_exporter.logging.config.dictConfig"
        )
        mock_safe_load = mocker.patch("cryo_metrics_exporter.yaml.safe_load")

        mock_file = mocker.MagicMock()
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value = mock_file
        mock_open.return_value.__exit__.return_value = None
        mock_safe_load.return_value = config

        # Act
        setup_logging(tz)

        # Assert
        mock_dictconfig.assert_called_once()
        applied_config = mock_dictconfig.call_args[0][0]
        formatter_config = applied_config["formatters"]["fmt"]
        assert "class" not in formatter_config
        assert "()" in formatter_config
        assert issubclass(formatter_config["()"], logging.Formatter)
        assert formatter_config["format"] == "%(message)s"

    def test_setup_logging_without_yaml_file(
        self,
        mocker: MockerFixture,
    ):
        # Arrange
        tz = ZoneInfo("Asia/Tokyo")
        mock_exists = mocker.patch("cryo_metrics_exporter.pathlib.Path.exists")
        mock_exists.return_value = False

        # Act/Assert
        with pytest.raises(FileNotFoundError):
            setup_logging(tz)

    def test_setup_logging_import_error_logs_exception(
        self,
        mocker: MockerFixture,
    ):
        # Arrange
        tz = ZoneInfo("Asia/Tokyo")
        config = {
            "version": 1,
            "formatters": {
                "bad_import": {
                    "class": "non.existent.Module",
                },
            },
            "handlers": {},
            "loggers": {},
        }
        mock_exists = mocker.patch("cryo_metrics_exporter.pathlib.Path.exists")
        mock_open = mocker.patch("cryo_metrics_exporter.pathlib.Path.open")
        mock_dictconfig = mocker.patch(
            "cryo_metrics_exporter.logging.config.dictConfig"
        )
        mock_safe_load = mocker.patch("cryo_metrics_exporter.yaml.safe_load")
        mock_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        mock_file = mocker.MagicMock()
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value = mock_file
        mock_open.return_value.__exit__.return_value = None
        mock_safe_load.return_value = config

        # Act
        setup_logging(tz)

        # Assert
        mock_exception.assert_called_once()
        assert (
            "Failed to create timezone-aware formatter"
            in mock_exception.call_args[0][0]
        )
        mock_dictconfig.assert_called_once_with(config)

    def test_setup_logging_attribute_error_logs_exception(
        self,
        mocker: MockerFixture,
    ):
        # Arrange
        tz = ZoneInfo("Asia/Tokyo")
        config = {
            "version": 1,
            "formatters": {
                "missing_class": {
                    "class": "logging.MissingClass",
                },
            },
            "handlers": {},
            "loggers": {},
        }
        mock_exists = mocker.patch("cryo_metrics_exporter.pathlib.Path.exists")
        mock_open = mocker.patch("cryo_metrics_exporter.pathlib.Path.open")
        mock_dictconfig = mocker.patch(
            "cryo_metrics_exporter.logging.config.dictConfig"
        )
        mock_safe_load = mocker.patch("cryo_metrics_exporter.yaml.safe_load")
        mock_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        mock_file = mocker.MagicMock()
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value = mock_file
        mock_open.return_value.__exit__.return_value = None
        mock_safe_load.return_value = config

        # Act
        setup_logging(tz)

        # Assert
        mock_exception.assert_called_once()
        assert (
            "Failed to create timezone-aware formatter"
            in mock_exception.call_args[0][0]
        )
        mock_dictconfig.assert_called_once_with(config)

    def test_setup_logging_value_error_logs_exception(
        self,
        mocker: MockerFixture,
    ):
        # Arrange
        tz = ZoneInfo("Asia/Tokyo")
        config = {
            "version": 1,
            "formatters": {
                "missing_class": {
                    "class": "",
                },
            },
            "handlers": {},
            "loggers": {},
        }
        mock_exists = mocker.patch("cryo_metrics_exporter.pathlib.Path.exists")
        mock_open = mocker.patch("cryo_metrics_exporter.pathlib.Path.open")
        mock_dictconfig = mocker.patch(
            "cryo_metrics_exporter.logging.config.dictConfig"
        )
        mock_safe_load = mocker.patch("cryo_metrics_exporter.yaml.safe_load")
        mock_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        mock_file = mocker.MagicMock()
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value = mock_file
        mock_open.return_value.__exit__.return_value = None
        mock_safe_load.return_value = config

        # Act
        setup_logging(tz)

        # Assert
        mock_exception.assert_called_once()
        assert (
            "Failed to create timezone-aware formatter"
            in mock_exception.call_args[0][0]
        )
        mock_dictconfig.assert_called_once_with(config)
