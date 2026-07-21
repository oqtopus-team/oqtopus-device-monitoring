from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from pythonjsonlogger.jsonlogger import JsonFormatter

from main import _create_timezone_formatter, setup_logging  # noqa:PLC2701

if TYPE_CHECKING:
    from pathlib import Path

    import pytest
    from pytest_mock import MockerFixture

FIXED_EPOCH = datetime(2026, 6, 11, 0, 0, 0, tzinfo=UTC).timestamp()

LOGGING_YAML = """
version: 1
disable_existing_loggers: false
formatters:
  json:
    class: pythonjsonlogger.jsonlogger.JsonFormatter
    format: "%(message)s"
handlers:
  console:
    class: logging.StreamHandler
    level: INFO
    formatter: json
loggers:
  qdash_exporter:
    level: INFO
    handlers: [console]
"""

LOGGING_YAML_WITH_FILE = """
version: 1
disable_existing_loggers: false
formatters:
  json:
    class: pythonjsonlogger.jsonlogger.JsonFormatter
    format: "%(message)s"
handlers:
  file:
    class: logging.FileHandler
    level: INFO
    formatter: json
    filename: {filename}
loggers:
  qdash_exporter:
    level: INFO
    handlers: [file]
"""


def _record() -> logging.LogRecord:
    record = logging.LogRecord("mylogger", logging.INFO, "path", 1, "hello", None, None)
    record.created = FIXED_EPOCH
    return record


def _use_logging_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, content: str
) -> None:
    path = tmp_path / "logging.yaml"
    path.write_text(content, encoding="utf-8")
    monkeypatch.setenv("QDASH_EXPORTER_LOGGING_CONFIG_PATH", str(path))


class TestCreateTimezoneFormatter:
    """Test suite for _create_timezone_formatter function."""

    def test_format_time_applies_timezone_isoformat(self) -> None:
        tz = ZoneInfo("Asia/Tokyo")
        formatter_cls = _create_timezone_formatter(logging.Formatter, tz)
        formatter = formatter_cls()

        result = formatter.formatTime(_record())

        assert result == datetime.fromtimestamp(FIXED_EPOCH, tz).isoformat()

    def test_format_time_with_datefmt_uses_strftime(self) -> None:
        tz = ZoneInfo("Asia/Tokyo")
        formatter_cls = _create_timezone_formatter(logging.Formatter, tz)
        formatter = formatter_cls()

        result = formatter.formatTime(_record(), datefmt="%Y-%m-%d %H:%M:%S")

        expected = datetime.fromtimestamp(FIXED_EPOCH, tz).strftime("%Y-%m-%d %H:%M:%S")
        assert result == expected

    def test_add_fields_restricts_to_allowed_fields(self) -> None:
        formatter_cls = _create_timezone_formatter(JsonFormatter, ZoneInfo("UTC"))
        formatter = formatter_cls(
            "%(levelname)s %(name)s %(message)s", allowed_fields=["message"]
        )

        data = json.loads(formatter.format(_record()))

        assert data == {"message": "hello"}

    def test_add_fields_keeps_all_when_no_allowed_fields(self) -> None:
        formatter_cls = _create_timezone_formatter(JsonFormatter, ZoneInfo("UTC"))
        formatter = formatter_cls("%(levelname)s %(message)s")

        data = json.loads(formatter.format(_record()))

        assert data["levelname"] == "INFO"
        assert data["message"] == "hello"


class TestSetupLogging:
    """Test suite for setup_logging function."""

    def test_setup_logging_substitutes_formatter_class(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
    ) -> None:
        dict_config = mocker.patch("logging.config.dictConfig")
        _use_logging_config(tmp_path, monkeypatch, LOGGING_YAML)

        setup_logging("UTC")

        formatter = dict_config.call_args.args[0]["formatters"]["json"]
        assert "class" not in formatter
        assert callable(formatter["()"])

    def test_setup_logging_creates_handler_log_dir(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch("logging.config.dictConfig")
        log_dir = tmp_path / "nested" / "logs"
        content = LOGGING_YAML_WITH_FILE.format(filename=log_dir / "app.log")
        _use_logging_config(tmp_path, monkeypatch, content)

        setup_logging("UTC")

        assert log_dir.is_dir()

    def test_setup_logging_invalid_formatter_class_is_caught(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
    ) -> None:
        dict_config = mocker.patch("logging.config.dictConfig")
        bad_config = LOGGING_YAML.replace(
            "pythonjsonlogger.jsonlogger.JsonFormatter",
            "nonexistent.module.DoesNotExist",
        )
        _use_logging_config(tmp_path, monkeypatch, bad_config)

        setup_logging("UTC")

        formatter = dict_config.call_args.args[0]["formatters"]["json"]
        assert formatter["class"] == "nonexistent.module.DoesNotExist"
        assert "()" not in formatter
