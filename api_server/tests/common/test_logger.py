import logging
import pathlib
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest

from common.logger import create_timezone_formatter, setup_logging
from common.types.config import LoggingConfig


class TestCreateTimezoneFormatter:
    """Tests for create_timezone_formatter function."""

    def test_create_timezone_formatter_with_datefmt_formats_time_in_timezone(self):
        """Test formatter with custom date format."""
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
        """Test formatter with ISO format."""
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
        record.created = 0.0

        # Act
        formatter_class = create_timezone_formatter(logging.Formatter, tz)
        formatter = formatter_class()
        result = formatter.formatTime(record, None)

        # Assert
        assert result.startswith("1970-01-01T09:00:00")


class TestSetupLogging:
    def test_setup_logging_when_formatter_has_class_rewrites_formatter(
        self,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Arrange
        config = {
            "formatters": {
                "fmt": {
                    "class": "logging.Formatter",
                    "format": "%(message)s",
                }
            },
            "handlers": {},
            "loggers": {},
        }
        logging_config = LoggingConfig(
            logging_config_path="dummy.yaml", logging_dir_path=str(tmp_path)
        )
        load_yaml_mock = Mock(return_value=config)
        dict_config_mock = Mock()
        import_module_mock = Mock(return_value=logging)
        monkeypatch.setattr("common.logger.load_yaml", load_yaml_mock)
        monkeypatch.setattr("common.logger.import_module", import_module_mock)
        monkeypatch.setattr("logging.config.dictConfig", dict_config_mock)

        # Act
        setup_logging(logging_config)

        # Assert
        dict_config_mock.assert_called_once()
        applied_config = dict_config_mock.call_args.args[0]
        formatter_config = applied_config["formatters"]["fmt"]
        assert "class" not in formatter_config
        assert "()" in formatter_config
        assert issubclass(formatter_config["()"], logging.Formatter)
        assert formatter_config["format"] == "%(message)s"

    def test_setup_logging_when_import_fails_logs_warning(
        self,
        tmp_path: pathlib.Path,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Arrange
        config = {
            "formatters": {
                "fmt": {
                    "class": "logging.MissingFormatter",
                    "format": "%(message)s",
                }
            },
            "handlers": {},
            "loggers": {},
        }
        logging_config = LoggingConfig(
            logging_config_path="dummy.yaml", logging_dir_path=str(tmp_path)
        )
        load_yaml_mock = Mock(return_value=config)
        dict_config_mock = Mock()
        import_module_mock = Mock(side_effect=ImportError())
        monkeypatch.setattr("common.logger.load_yaml", load_yaml_mock)
        monkeypatch.setattr("common.logger.import_module", import_module_mock)
        monkeypatch.setattr("logging.config.dictConfig", dict_config_mock)

        # Act
        with caplog.at_level(logging.WARNING, logger="api-server"):
            setup_logging(logging_config)

        # Assert
        dict_config_mock.assert_called_once()
        applied_config = dict_config_mock.call_args.args[0]
        formatter_config = applied_config["formatters"]["fmt"]
        assert formatter_config["class"] == "logging.MissingFormatter"
        warning_messages = [
            record.message for record in caplog.records if record.name == "api-server"
        ]
        assert any(
            "Could not create timezone-aware formatter for 'fmt'" in msg
            for msg in warning_messages
        )

    def test_setup_logging_without_formatter_class_skips_entry(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Arrange
        config = {
            "formatters": {"fmt": {"format": "%(message)s"}},
            "handlers": {},
            "loggers": {},
        }
        logging_config = LoggingConfig(
            logging_config_path="dummy.yaml", logging_dir_path=str(tmp_path)
        )
        load_yaml_mock = Mock(return_value=config)
        dict_config_mock = Mock()
        import_module_mock = Mock()
        monkeypatch.setattr("common.logger.load_yaml", load_yaml_mock)
        monkeypatch.setattr("common.logger.import_module", import_module_mock)
        monkeypatch.setattr("logging.config.dictConfig", dict_config_mock)

        # Act
        setup_logging(logging_config)

        # Assert
        dict_config_mock.assert_called_once_with(config)
        import_module_mock.assert_not_called()

    def test_setup_logging_without_formatters_block_skips_configuration(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Arrange
        config: dict = {"handlers": {}, "loggers": {}}
        logging_config = LoggingConfig(
            logging_config_path="dummy.yaml", logging_dir_path=str(tmp_path)
        )
        load_yaml_mock = Mock(return_value=config)
        dict_config_mock = Mock()
        import_module_mock = Mock()
        monkeypatch.setattr("common.logger.load_yaml", load_yaml_mock)
        monkeypatch.setattr("common.logger.import_module", import_module_mock)
        monkeypatch.setattr("logging.config.dictConfig", dict_config_mock)

        # Act
        setup_logging(logging_config)

        # Assert
        dict_config_mock.assert_called_once_with(config)
        import_module_mock.assert_not_called()

    def test_setup_logging_when_load_yaml_errors_falls_back(
        self,
        tmp_path: pathlib.Path,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Arrange
        logging_config = LoggingConfig(
            logging_config_path="dummy.yaml", logging_dir_path=str(tmp_path)
        )
        monkeypatch.setattr("common.logger.load_yaml", Mock(side_effect=KeyError()))
        root_logger = logging.getLogger()
        original_handlers = list(root_logger.handlers)
        original_level = root_logger.level

        # Act / Assert
        try:
            with caplog.at_level(logging.WARNING, logger="api-server"):
                setup_logging(logging_config)

            new_handlers = [
                h for h in root_logger.handlers if h not in original_handlers
            ]
            assert len(new_handlers) == 1
            fallback_handler = new_handlers[0]
            assert isinstance(fallback_handler, logging.StreamHandler)
            assert fallback_handler.formatter is not None
            assert root_logger.level == logging.INFO
            warning_messages = [
                record.message
                for record in caplog.records
                if record.name == "api-server"
            ]
            assert any(
                "Using basic logging with timezone" in msg for msg in warning_messages
            )
        finally:
            for handler in list(root_logger.handlers):
                if handler not in original_handlers:
                    root_logger.removeHandler(handler)
            root_logger.setLevel(original_level)
