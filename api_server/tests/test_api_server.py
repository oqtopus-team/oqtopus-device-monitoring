import logging
import runpy

import pytest
from fastapi import FastAPI

import api_server
from common.types.config import AppConfig


class DummyClient:
    """Stub VictoriaMetrics client for lifespan tests."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.closed_count = 0

    async def close(self) -> None:
        self.closed_count += 1


@pytest.mark.asyncio
async def test_lifespan_when_client_created_sets_state_and_closes_client(
    monkeypatch: pytest.MonkeyPatch, app_config: AppConfig
) -> None:
    # Arrange
    created_clients: list[DummyClient] = []

    monkeypatch.setattr(api_server, "init_config", lambda: app_config)

    def factory(url: str) -> DummyClient:
        client = DummyClient(url)
        created_clients.append(client)
        return client

    monkeypatch.setattr(api_server, "VictoriaMetricsClient", factory)

    # Act
    app = FastAPI(lifespan=api_server.lifespan)

    # Assert
    async with api_server.lifespan(app):
        assert app.state.config is app_config
        assert app.state.client is created_clients[0]

    assert created_clients[0].url == app_config.victoria_metrics_url
    assert created_clients[0].closed_count == 1


@pytest.mark.asyncio
async def test_lifespan_when_client_missing_sets_state_with_none_client(
    monkeypatch: pytest.MonkeyPatch,
    app_config: AppConfig,
) -> None:
    # Arrange
    monkeypatch.setattr(api_server, "init_config", lambda: app_config)

    def factory(_: str) -> None:
        return None

    monkeypatch.setattr(api_server, "VictoriaMetricsClient", factory)

    # Act
    app = FastAPI(lifespan=api_server.lifespan)

    # Assert
    async with api_server.lifespan(app):
        assert app.state.config is app_config
        assert app.state.client is None


@pytest.mark.asyncio
async def test_lifespan_when_init_config_fails_errors_and_logs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # Arrange
    def raise_error() -> AppConfig:
        raise RuntimeError

    monkeypatch.setattr(api_server, "init_config", raise_error)

    # Act
    app = FastAPI(lifespan=api_server.lifespan)

    # Assert
    with caplog.at_level(logging.CRITICAL), pytest.raises(RuntimeError):
        async with api_server.lifespan(app):
            pass

    assert any(
        "Failed to load application configuration" in message
        for message in caplog.messages
    )


@pytest.mark.asyncio
async def test_lifespan_when_context_errors_closes_client(
    monkeypatch: pytest.MonkeyPatch,
    app_config: AppConfig,
) -> None:
    # Arrange
    created_clients: list[DummyClient] = []

    monkeypatch.setattr(api_server, "init_config", lambda: app_config)

    def factory(url: str) -> DummyClient:
        client = DummyClient(url)
        created_clients.append(client)
        return client

    monkeypatch.setattr(api_server, "VictoriaMetricsClient", factory)

    # Act
    app = FastAPI(lifespan=api_server.lifespan)

    # Assert
    with pytest.raises(RuntimeError):
        async with api_server.lifespan(app):
            raise RuntimeError

    assert created_clients[0].closed_count == 1


def test_main_guard_with_valid_config_runs_uvicorn_succeeds(
    monkeypatch: pytest.MonkeyPatch, app_config: AppConfig
) -> None:
    # Arrange
    captured: dict[str, tuple] = {}

    monkeypatch.setattr("common.config.init_config", lambda: app_config)
    monkeypatch.setattr(
        "common.logger.setup_logging",
        lambda log_cfg, tz_str: captured.update(setup=(log_cfg, tz_str)),
    )
    monkeypatch.setattr(
        "uvicorn.run",
        lambda app, host, port: captured.update(run=(app, host, port)),
    )

    # Act
    runpy.run_module("api_server", run_name="__main__", alter_sys=True)

    # Assert
    assert captured.get("setup") == (app_config.log, app_config.server.timezone)
    run_args = captured.get("run")
    assert isinstance(run_args, tuple)
    assert isinstance(run_args[0], FastAPI)
    assert run_args[1] == app_config.server.host
    assert run_args[2] == app_config.server.port


def test_main_guard_with_config_error_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    def raise_error() -> AppConfig:
        raise RuntimeError

    monkeypatch.setattr("common.config.init_config", raise_error)

    # Act / Assert
    with pytest.raises(RuntimeError):
        runpy.run_module("api_server", run_name="__main__", alter_sys=True)
