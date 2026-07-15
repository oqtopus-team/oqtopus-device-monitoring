from __future__ import annotations

import logging
import logging.config
import os
import threading
from datetime import datetime
from http.server import HTTPServer
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import yaml

from collector import CollectionService, run_collection_loop
from config import load_config
from exposition import MetricsRequestHandler, PullService
from qdash_api import QDashGateway
from storage import SpoolBuffer, WindowStateStore

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger(f"qdash_exporter.{__name__}")


def _create_timezone_formatter(cls: type, tz: ZoneInfo) -> type:
    """Create a new formatter class that inherits from cls and overrides formatTime.

    Args:
        cls: The original formatter class to inherit from.
        tz: The timezone to use for formatting timestamps.

    Returns:
        A new formatter class with timezone-aware time formatting.

    """

    class TimezoneFormatter(cls):
        def __init__(
            self,
            *args: object,
            allowed_fields: Iterable[str] | None = None,
            **kwargs: object,
        ) -> None:
            super().__init__(*args, **kwargs)
            self._allowed = set(allowed_fields or [])

        def add_fields(
            self,
            log_record: dict[str, Any],
            record: logging.LogRecord,
            message_dict: dict[str, Any],
        ) -> None:
            # add fields as usual
            super().add_fields(log_record, record, message_dict)
            # restrict to allowed fields only
            if self._allowed:
                for k in list(log_record.keys()):
                    if k not in self._allowed:
                        log_record.pop(k, None)

        def formatTime(  # noqa: N802, PLR6301
            self, record: logging.LogRecord, datefmt: str | None = None
        ) -> str:
            dt = datetime.fromtimestamp(record.created, tz)
            if datefmt:
                return dt.strftime(datefmt)
            return dt.isoformat()

    return TimezoneFormatter


def setup_logging(exporter_timezone: str) -> None:
    """Configure logging from a YAML file and set up timezone-aware formatters.

    Args:
        exporter_timezone: The timezone to use for formatting timestamps in logs.

    """
    tz = ZoneInfo(exporter_timezone)

    config_path = Path(
        os.environ.get("QDASH_EXPORTER_LOGGING_CONFIG_PATH", "./config/logging.yaml")
    )
    with config_path.open("rt", encoding="utf-8") as f:
        log_config = yaml.safe_load(f)

    for handler_config in log_config.get("handlers", {}).values():
        filename = handler_config.get("filename")
        if filename:
            Path(filename).parent.mkdir(parents=True, exist_ok=True)

    # Dynamically create and substitute timezone-aware formatters
    if "formatters" in log_config:
        for name, formatter_config in log_config["formatters"].items():
            if "class" in formatter_config:
                try:
                    # Import the original formatter class
                    module_path, class_name = formatter_config["class"].rsplit(".", 1)
                    original_class = getattr(import_module(module_path), class_name)
                    # Create a new class with timezone support and replace it
                    formatter_config["()"] = _create_timezone_formatter(
                        original_class, tz
                    )
                    del formatter_config["class"]
                except (ImportError, AttributeError, ValueError):
                    logger.exception(
                        "Failed to create timezone-aware formatter for %s", name
                    )

    logging.config.dictConfig(log_config)
    logger.info(
        "Logging configured successfully from %s with timezone %s.", config_path, tz
    )


def main() -> None:
    """Serve as the entry point for the QDash Exporter application."""
    # Load configuration and set up logging
    config = load_config()
    setup_logging(config.exporter.timezone)
    logger.info("Validated configuration: %s", config.model_dump())

    # Initialize spool buffer and window state cache
    spool = SpoolBuffer(config.buffer.dir_path)
    state = WindowStateStore(spool.state_dir, config.collection.max_expand_windows)
    spool.ensure_layout()
    state.load()
    spool.validate_pending_at_startup(config.targets.enabled_metrics())

    # Bootstrap the QDash client with provided configurations
    gateway = QDashGateway.from_config(config.qdash_client, config.collection.tag)

    # Start the collection service in a separate thread
    service = CollectionService(config, gateway, spool, state)
    stop_event = threading.Event()
    collector_thread = threading.Thread(
        target=run_collection_loop,
        args=(service, config.collection.interval_sec, stop_event),
        daemon=True,
        name="collector",
    )
    collector_thread.start()

    # Set up the HTTP server to expose metrics
    MetricsRequestHandler.pull_service = PullService(spool, config.targets)
    server = HTTPServer(("0.0.0.0", config.exporter.port), MetricsRequestHandler)  # noqa: S104
    logger.info("Exporter started, listening on port %s.", config.exporter.port)
    server.serve_forever()


if __name__ == "__main__":
    main()
