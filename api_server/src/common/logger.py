import logging
import logging.config
from datetime import datetime
from importlib import import_module
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from common.config import DEFAULT_SERVER_TIMEZONE
from common.types.config import LoggingConfig
from common.util import load_yaml


def create_timezone_formatter(cls: type, tz: ZoneInfo) -> type:
    """Create a new formatter class that inherits from cls and overrides formatTime.

    Args:
        cls: The original formatter class to inherit from.
        tz (ZoneInfo): The timezone to apply.

    Returns:
        A new formatter class with timezone-aware time formatting.

    """

    class TimezoneFormatter(cls):
        def formatTime(  # noqa: N802, PLR6301
            self, record: logging.LogRecord, datefmt: str | None = None
        ) -> str:
            dt = datetime.fromtimestamp(record.created, tz)
            if datefmt:
                return dt.strftime(datefmt)
            return dt.isoformat()

    return TimezoneFormatter


def setup_logging(
    config_path: LoggingConfig, tz_str: str = DEFAULT_SERVER_TIMEZONE
) -> None:
    """Set up logging configuration from YAML file.

    Loads logging configuration from YAML file and applies it using
    logging.config.dictConfig. This enables structured JSON logging
    and centralized log configuration management.

    Args:
        config_path: Path to logging configuration YAML file
        tz_str: Timezone for log timestamps

    """
    try:
        log_config = load_yaml(config_path.logging_config_path)
        tz = ZoneInfo(tz_str)
        # Dynamically create and substitute timezone-aware formatters
        if "formatters" in log_config:
            for name, formatter_config in log_config["formatters"].items():
                if "class" in formatter_config:
                    try:
                        # Import the original formatter class
                        module_path, class_name = formatter_config["class"].rsplit(
                            ".", 1
                        )
                        module = import_module(module_path)
                        original_class = getattr(module, class_name)
                        # Create a new class with timezone support
                        tz_formatter_class = create_timezone_formatter(
                            original_class, tz
                        )
                        # Replace the class in the config
                        formatter_config["()"] = tz_formatter_class
                        del formatter_config["class"]
                    except (ImportError, AttributeError, ValueError) as e:
                        logging.getLogger("api-server").warning(
                            "Could not create timezone-aware formatter for '%s': %s",
                            name,
                            e,
                        )
        logging.config.dictConfig(log_config)
        logging.getLogger("api-server").info(
            "Logging configured successfully from %s with timezone %s.",
            config_path.logging_config_path,
            tz,
        )

    except (KeyError, TypeError, ZoneInfoNotFoundError):
        tz = ZoneInfo(DEFAULT_SERVER_TIMEZONE)
        # Basic config with timezone
        time_zone_formatter = create_timezone_formatter(logging.Formatter, tz)
        formatter = time_zone_formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        # Get root logger and add handler
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(handler)
        logging.getLogger("api-server").warning(
            "Error is occurred when setting up logging. "
            "Using basic logging with timezone %s.",
            tz,
        )
