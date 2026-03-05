import pathlib
import re
from types import SimpleNamespace

import pytest
from fastapi import Request
from omegaconf import OmegaConf

from common import config
from common.types.config import AppConfig, LoggingConfig, ServerConfig


def clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    keys = [
        config.ENV_API_SERVER_CONFIG_PATH,
        config.ENV_API_SERVER_LOGGING_CONFIG_PATH,
        config.ENV_API_SERVER_LOGGING_DIR_PATH,
        config.ENV_API_SERVER_OPERATION_HISTORY_PATH,
        config.ENV_API_SERVER_CONFIG_PATH_INSIDE,
        config.ENV_API_SERVER_LOGGING_CONFIG_PATH_INSIDE,
        config.ENV_API_SERVER_LOGGING_DIR_PATH_INSIDE,
        config.ENV_API_SERVER_OPERATION_HISTORY_PATH_INSIDE,
        config.ENV_API_SERVER_TIMEZONE,
        config.ENV_API_SERVER_LOCK_TIMEOUT_HOURS,
        config.ENV_API_SERVER_HOST,
        config.ENV_API_SERVER_PORT,
        config.ENV_VICTORIAMETRICS_URL,
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)


# ConfigError class Tests
def test_config_error_message_with_env_and_yaml_path_includes_both() -> None:
    # Arrange
    env_key = "TEST_ENV_KEY"
    yaml_path = "test.yaml.path"

    # Act
    error = config.ConfigError(env_key=env_key, yaml_path=yaml_path)

    # Assert
    assert env_key in error.message
    assert yaml_path in error.message


def test_config_error_message_without_yaml_path_includes_env() -> None:
    # Arrange
    env_key = "TEST_ENV_KEY"
    # Act
    error = config.ConfigError(env_key=env_key)
    # Assert
    assert env_key in error.message


# Config Functions Tests
def test_get_config_with_request_state_returns_config() -> None:
    # Arrange
    server_config = ServerConfig(timezone="UTC", host="127.0.0.1", port=8080)
    logging_config = LoggingConfig(
        logging_config_path="/config/logging.yaml",
        logging_dir_path="/logs",
    )
    app_config = AppConfig(
        server=server_config,
        log=logging_config,
        victoria_metrics_url="http://vm",
        lock_timeout_hours=1,
        operation_history_path="/data",
    )
    request = Request(
        scope={
            "type": "http",
            "app": SimpleNamespace(state=SimpleNamespace(config=app_config)),
        }
    )

    # Act
    result = config.get_config(request)

    # Assert
    assert result is app_config


def test_get_param_when_env_missing_uses_yaml_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    clear_env(monkeypatch)
    conf = OmegaConf.create({"a": {"b": "yaml"}})

    # Act
    value = config.get_param("KEY", conf, "a.b", "default")

    # Assert
    assert value == "yaml"


def test_get_param_when_missing_uses_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    clear_env(monkeypatch)
    conf = OmegaConf.create({})

    # Act
    value = config.get_param("KEY", conf, "a.b", "default")

    # Assert
    assert value == "default"


def test_get_param_with_int_value_returns_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    clear_env(monkeypatch)
    conf = OmegaConf.create({"a": {"b": 123}})

    # Act
    value = config.get_param("KEY", conf, "a.b", "default")

    # Assert
    assert value == "123"


def test_get_param_with_non_dict_path_uses_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    clear_env(monkeypatch)
    conf = OmegaConf.create({"a": ["not-dict"]})

    # Act
    value = config.get_param("KEY", conf, "a.b", "default")

    # Assert
    assert value == "default"


def test_get_param_with_none_path_uses_default(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    clear_env(monkeypatch)
    conf = OmegaConf.create({"a": "b"})

    # Act
    value = config.get_param("KEY", conf, None, "default")

    # Assert
    assert value == "default"


def test_get_param_with_empty_env_returns_empty_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    clear_env(monkeypatch)
    monkeypatch.setenv("KEY", "")
    conf = OmegaConf.create({"a": {"b": "yaml"}})

    # Act
    value = config.get_param("KEY", conf, "a.b", "default")

    # Assert
    assert value == ""


def test_init_config_with_missing_required_env_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    # VictoriaMetrics URL is required, so omitting it causes ConfigError
    clear_env(monkeypatch)

    # Act / Assert
    with pytest.raises(
        config.ConfigError,
        match=re.compile(
            rf"(?s)(?=.*{re.escape(config.ENV_VICTORIAMETRICS_URL)})(?=.*{re.escape(config.YAML_PATH_VICTORIAMETRICS_URL)}).+"
        ),
    ):
        config.init_config()


def test_init_config_with_yaml_file_succeeds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    # Arrange
    clear_env(monkeypatch)
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text(
        """
server:
  timezone: JST
  host: 127.0.0.1
  port: 9000
victoria_metrics:
  url: http://vm.local
operations:
    lock_timeout_hours: 2
		""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("API_SERVER_CONFIG_PATH_INSIDE", str(yaml_path))

    # Act
    app_config = config.init_config()

    # Assert
    assert app_config.server.timezone == "JST"
    assert app_config.server.host == "127.0.0.1"
    assert app_config.server.port == 9000
    assert app_config.victoria_metrics_url == "http://vm.local"
    assert app_config.lock_timeout_hours == 2


def test_init_config_when_env_present_overrides_yaml(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    # Arrange
    clear_env(monkeypatch)
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text(
        """
server:
  timezone: JST
  host: 0.0.0.0
  port: 9000
victoria_metrics:
  url: http://vm.local
		""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("API_SERVER_CONFIG_PATH", str(yaml_path))
    monkeypatch.setenv("API_SERVER_HOST", "1.1.1.1")
    monkeypatch.setenv("API_SERVER_PORT", "1234")
    monkeypatch.setenv("API_SERVER_TIMEZONE", "UTC")
    monkeypatch.setenv("API_SERVER_LOCK_TIMEOUT_HOURS", "5")
    monkeypatch.setenv("VICTORIAMETRICS_URL", "http://env")
    monkeypatch.setenv("API_SERVER_LOGGING_CONFIG_PATH", "/custom/logging.yaml")
    monkeypatch.setenv("API_SERVER_LOGGING_DIR_PATH", "/custom/logs")
    monkeypatch.setenv("API_SERVER_OPERATION_HISTORY_PATH", "/custom/data")

    # Act
    app_config = config.init_config()

    # Assert
    assert app_config.server.host == "1.1.1.1"
    assert app_config.server.port == 1234
    assert app_config.server.timezone == "UTC"
    assert app_config.victoria_metrics_url == "http://env"
    assert app_config.lock_timeout_hours == 5
    assert app_config.log.logging_config_path == "/config/logging.yaml"
    assert app_config.log.logging_dir_path == "/app/logs"
    assert app_config.operation_history_path == "/app/data"


def test_init_config_with_invalid_int_env_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    # Arrange
    clear_env(monkeypatch)
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("server:\n  host: 0.0.0.0\n", encoding="utf-8")
    monkeypatch.setenv("API_SERVER_CONFIG_PATH", str(yaml_path))
    monkeypatch.setenv("API_SERVER_PORT", "not-an-int")

    # Act / Assert
    with pytest.raises(ValueError, match="invalid literal for int"):
        config.init_config()
