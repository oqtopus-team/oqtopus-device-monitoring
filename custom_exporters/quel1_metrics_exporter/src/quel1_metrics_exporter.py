import logging
import logging.config
import os
import pathlib
import shutil
import subprocess  # noqa: S404
import time
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from importlib import import_module
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from prometheus_client import CollectorRegistry, GCCollector, start_http_server
from prometheus_client.core import GaugeMetricFamily

# setup logger
logger = logging.getLogger("qubit_controller")

DEFAULT_TIMEOUT_SEC = 5
DEFAULT_RETRY = 3
DEFAULT_EXPORTER_PORT = 9102
DEFAULT_TIMEZONE = "UTC"
PING_METRICS_NAME = "qubit_controller_ping_status_code"
MAX_WORKERS = os.getenv("MAX_WORKERS", "0")


def validate_targets(raw_targets: list) -> list[dict[str, str]]:
    """Validate that each target has name, ip, controller_type (all non-empty strings).

    Args:
        raw_targets (list): List of raw target configurations.

    Returns:
        list[dict[str, str]]: List of validated target dictionaries.

    """
    if not isinstance(raw_targets, list):
        logger.warning("ping.targets is not a list; skipping.")
        return []

    valid: list[dict[str, str]] = []
    for idx, t in enumerate(raw_targets):
        if not isinstance(t, dict):
            logger.warning("Target #%s is not a dict; skipping.", idx)
            continue

        name = t.get("name")
        ip = t.get("ip")
        controller_type = t.get("controller_type")

        missing = [
            k
            for k, v in {
                "name": name,
                "ip": ip,
                "controller_type": controller_type,
            }.items()
            if v is None
        ]
        if missing:
            logger.warning(
                "Target #%s missing keys: %s; skipping.", idx, ", ".join(missing)
            )
            continue

        if not isinstance(name, str) or not name.strip():
            logger.warning("Target #%s has empty/non-string name; skipping.", idx)
            continue
        if not isinstance(ip, str) or not ip.strip():
            logger.warning("Target #%s has empty/non-string ip; skipping.", idx)
            continue
        if not isinstance(controller_type, str) or not controller_type.strip():
            logger.warning(
                "Target #%s has empty/non-string controller_type; skipping.", idx
            )
            continue

        name_s: str = name.strip()
        ip_s: str = ip.strip()
        controller_type_s: str = controller_type.strip()

        valid.append({
            "name": name_s,
            "ip": ip_s,
            "controller_type": controller_type_s,
        })

    return valid


def create_timezone_formatter(cls: type, tz: ZoneInfo) -> type:
    """Create a new formatter class that inherits from cls and overrides formatTime.

    Args:
        cls: The original formatter class to inherit from.
        tz (ZoneInfo): The timezone to apply.

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


def ping_target(
    target: dict, count: int = DEFAULT_RETRY, timeout: int = DEFAULT_TIMEOUT_SEC
) -> int:
    """Execute ping command to check reachability of the target.

    Args:
        target (dict): A dictionary containing 'name' and 'ip' of the target.
        count (int): Number of ping attempts.
        timeout (int): Timeout for the ping command in seconds.

    Returns:
        int: 0 if reachable, 1 if unreachable or error occurs.

    """
    target_name = target["name"]
    ip = target["ip"]
    status_code = 1  # Default to unreachable

    ping_path = shutil.which("ping")
    if not ping_path:
        logger.error("ping command not found in PATH.")
        return 1

    try:
        subprocess.check_call(  # noqa: S603
            [ping_path, "-c", str(count), "-W", str(timeout), ip],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        status_code = 0  # Reachable
        status_text = "reachable"
        logger.debug("Ping to %s (%s) succeeded.", target_name, ip)
    except subprocess.CalledProcessError:
        status_code = 1  # Unreachable
        status_text = "unreachable"
        logger.debug("Ping to %s (%s) failed.", target_name, ip)
    except Exception:
        status_code = 1  # Other errors
        status_text = "error"
        logger.exception("An unexpected error occurred during ping for %s", target_name)
    logger.debug(
        "Ping check completed for target.",
        extra={"target_host": target_name, "target_ip": ip, "status": status_text},
    )
    return status_code


def get_cgroup_cpu_count() -> int:
    """Get the number of CPUs in the docker container.

    Returns:
        int: the number of CPUs in the docker container.

    """
    cpu_count = os.cpu_count() or 1
    try:
        quota = int(Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").read_text("utf-8"))
        period = int(Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us").read_text("utf-8"))
        if quota == -1:
            # cpu quota is not set
            return cpu_count
        # not to exceed the number of CPUs of the host
        return min(quota // period, cpu_count)
    except FileNotFoundError:
        return cpu_count


def get_allowed_threads() -> int:
    """Get the number of allowed threads.

    Returns:
        int: the number of allowed threads.

    """
    # maximum number of workers
    num_workers = 0
    max_allowed_threads = get_cgroup_cpu_count()
    try:
        num_workers = int(MAX_WORKERS)
        if num_workers > max_allowed_threads or num_workers < 1:
            num_workers = max_allowed_threads
    except ValueError:
        num_workers = max_allowed_threads
    logger.info("Using %d threads for pinging targets.", num_workers)
    return num_workers


class CustomCollector(GCCollector):
    """Collector that gathers custom metrics during Prometheus scrape."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self.targets: list[dict[str, str]] = config["ping"]["targets"]
        self.timeout = self._config.get("ping", {}).get("timeout", DEFAULT_TIMEOUT_SEC)
        self.count = self._config.get("ping", {}).get("count", DEFAULT_RETRY)
        self.num_workers = get_allowed_threads()

    def collect(self) -> Iterator[GaugeMetricFamily]:
        """Execute every time `/metrics` is called.

        Yields:
            GaugeMetricFamily: A metric family with ping status for each target.

        """
        logger.debug("Starting metrics collection for scrape request.")
        # Define metric family
        m_ping_status = GaugeMetricFamily(
            PING_METRICS_NAME,
            "QuEL machine reachability via ICMP ping (0=reachable, 1=unreachable)",
            labels=["target_host", "target_ip", "controller_type"],
        )
        # Execute ping checks in parallel

        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = {
                executor.submit(ping_target, target, self.count, self.timeout): target
                for target in self.targets
            }
            for future, target in futures.items():
                try:
                    status_code = future.result()
                except Exception:
                    logger.exception(
                        "Ping task raised unexpectedly for %s", target["name"]
                    )
                    status_code = 1
                m_ping_status.add_metric(
                    [target["name"], target["ip"], target["controller_type"]],
                    status_code,
                )
        yield m_ping_status


def setup_logging(config: dict) -> None:
    """Set up logging based on logging.yaml, applying the configured timezone."""
    path = "/config/logging.yaml"  # path in the container
    try:
        tz_string = config["exporter"]["timezone"]
        tz = ZoneInfo(tz_string)
    except (KeyError, TypeError, ZoneInfoNotFoundError):
        logger.warning(
            "Invalid or missing timezone in config. Falling back to default: %s.",
            DEFAULT_TIMEZONE,
        )
        tz = ZoneInfo(DEFAULT_TIMEZONE)

    with pathlib.Path(path).open("rt", encoding="utf-8") as f:
        log_config = yaml.safe_load(f)
    # Dynamically create and substitute timezone-aware formatters
    if "formatters" in log_config:
        for name, formatter_config in log_config["formatters"].items():
            if "class" in formatter_config:
                try:
                    # Import the original formatter class
                    module_path, class_name = formatter_config["class"].rsplit(".", 1)
                    module = import_module(module_path)
                    original_class = getattr(module, class_name)
                    # Create a new class with timezone support
                    tz_formatter_class = create_timezone_formatter(original_class, tz)
                    # Replace the class in the config
                    formatter_config["()"] = tz_formatter_class
                    del formatter_config["class"]
                except (ImportError, AttributeError, ValueError) as e:
                    logger.warning(
                        "Could not create timezone-aware formatter for '%s': %s",
                        name,
                        e,
                    )
    logging.config.dictConfig(log_config)
    logger.info("Logging configured successfully from %s with timezone %s.", path, tz)


def load_config() -> dict | None:
    """Load configuration file and override with environment variables.

    Returns:
        The loaded configuration dictionary, or None if loading failed.

    """
    config_path = "/config/config.yaml"  # path in the container
    with pathlib.Path(config_path).open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    if not isinstance(config, dict):
        logger.error(
            "Invalid configuration format in %s: root is not a dictionary.",
            config_path,
        )
        return None
    logger.info("Configuration loaded successfully from %s.", config_path)

    targets = validate_targets(config.get("ping", {}).get("targets", []))
    if not targets:
        logger.error("No valid targets found in configuration.")
        return None  # Abort if no valid targets
    config["ping"]["targets"] = targets

    # Override with environment variables
    config["exporter"]["port"] = int(
        os.getenv(
            "EXPORTER_PORT",
            config.get("exporter", {}).get("port", DEFAULT_EXPORTER_PORT),
        )
    )
    config["ping"]["timeout"] = int(
        os.getenv(
            "PING_TIMEOUT", config.get("ping", {}).get("timeout", DEFAULT_TIMEOUT_SEC)
        )
    )
    config["ping"]["count"] = int(
        os.getenv("PING_COUNT", config.get("ping", {}).get("count", DEFAULT_RETRY))
    )
    config["exporter"]["timezone"] = os.getenv(
        "SERVER_TIMEZONE",
        config.get("exporter", {}).get("timezone", DEFAULT_TIMEZONE),
    )
    logger.info("Configuration loaded as: %s", config)
    return config


if __name__ == "__main__":
    main_config = load_config()
    if main_config is None:
        logger.critical("Failed to load configuration. Exiting.")
        raise SystemExit(1)
    setup_logging(main_config)
    logger.info("config: %s", main_config)
    server_port = main_config["exporter"]["port"]

    # Unregister the default collector and register the custom collector
    registry = CollectorRegistry()
    registry.register(CustomCollector(main_config))
    start_http_server(server_port, registry=registry)
    logger.info("Exporter started, listening on port %s.", server_port)

    # Keep the process running
    while True:
        time.sleep(1)
