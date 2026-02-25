from __future__ import annotations

import concurrent.futures
import logging
import logging.config
import os
import pathlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import InitVar, dataclass, field
from datetime import datetime
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from prometheus_client import CollectorRegistry, start_http_server
from prometheus_client.core import GaugeMetricFamily
from quel_ic_config import Quel1Box, Quel1BoxType

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


# Constants
DEFAULT_EXPORTER_PORT = 9103
DEFAULT_TIMEZONE = "UTC"
DEFAULT_QUEL1SE_TIMEOUT = 5
MAX_WORKERS_ENV = os.getenv("MAX_WORKERS", "0")

# Environment variables
ENV_CONFIG_PATH = "QUEL1SE_EXPORTER_CONFIG_PATH"
ENV_LOGGING_CONFIG_PATH = "QUEL1SE_EXPORTER_LOGGING_CONFIG_PATH"
ENV_LOGGING_DIR_PATH = "QUEL1SE_EXPORTER_LOGGING_DIR_PATH"
ENV_EXPORTER_PORT = "QUBIT_CONTROLLER_EXPORTER_PORT"
ENV_EXPORTER_TIMEZONE = "EXPORTER_TIMEZONE"
ENV_QUEL1SE_TIMEOUT = "QUEL1SE_TIMEOUT"

# Default paths
DEFAULT_CONFIG_PATH = "./config/config.yaml"
DEFAULT_LOGGING_CONFIG_PATH = "./config/logging.yaml"
DEFAULT_LOGGING_DIR_PATH = "./logs"

# Metric names
TEMPERATURE_METRIC_NAME = "qubit_controller_temperature"
ACTUATOR_METRIC_NAME = "qubit_controller_actuator_usage"

# Setup logger
logger = logging.getLogger("quel1-se-exporter")


@dataclass
class Quel1seTarget:
    """Represents a QuEL-1 SE target configuration."""

    name: str
    wss_ip: str
    boxtype_str: InitVar[str]
    boxtype: Quel1BoxType = field(init=False)
    css_ip: str | None = None

    def __post_init__(self, boxtype_str: str) -> None:
        """Validate required fields.

        Raises:
            ValueError: If required fields are missing or invalid.

        """
        if not self.name or not self.name.strip():
            msg = "Target name cannot be empty"
            raise ValueError(msg)
        if not self.wss_ip or not self.wss_ip.strip():
            msg = "Target wss_ip cannot be empty"
            raise ValueError(msg)
        if not boxtype_str or not boxtype_str.strip():
            msg = "Target boxtype cannot be empty"
            raise ValueError(msg)
        try:
            self.boxtype = Quel1BoxType.fromstr(boxtype_str.strip())
        except Exception as e:
            msg = f"Invalid boxtype: {boxtype_str}"
            raise ValueError(msg) from e
        self.name = self.name.strip()
        self.wss_ip = self.wss_ip.strip()
        if self.css_ip:
            self.css_ip = self.css_ip.strip()


@dataclass
class CollectorResult:
    """Result from collecting metrics from a single target."""

    target: Quel1seTarget
    temperatures: dict[str, float] | None = None
    actuators: dict[str, dict[str, float]] | None = None
    success: bool = False
    error: str | None = None


def validate_quel1se_targets(raw_targets: list) -> list[Quel1seTarget]:
    """Validate QuEL-1 SE target configurations.

    Args:
        raw_targets: List of raw target configurations from YAML.

    Returns:
        List of validated Quel1seTarget objects.

    """
    if not isinstance(raw_targets, list):
        logger.warning("quel1se.targets is not a list; skipping.")
        return []

    valid: list[Quel1seTarget] = []
    for idx, t in enumerate(raw_targets):
        if not isinstance(t, dict):
            logger.warning("Target #%s is not a dict; skipping.", idx)
            continue

        name = t.get("name")
        wss_ip = t.get("wss_ip")
        css_ip = t.get("css_ip")
        boxtype = t.get("boxtype")

        # Check required fields
        missing = [
            k
            for k, v in {
                "name": name,
                "wss_ip": wss_ip,
                "boxtype": boxtype,
            }.items()
            if v is None
        ]
        if missing:
            logger.warning(
                "Target #%s missing required keys: %s; skipping.",
                idx,
                ", ".join(missing),
            )
            continue

        # Ensure types are strings for mypy
        if not isinstance(name, str) or not isinstance(wss_ip, str):
            logger.warning(
                "Target #%s has non-string 'name' or 'wss_ip'; skipping.", idx
            )
            continue

        # Validate css_ip if provided
        if css_ip is not None and not isinstance(css_ip, str):
            logger.warning("Target #%s has non-string 'css_ip'; skipping.", idx)
            continue

        if not isinstance(boxtype, str):
            logger.warning(
                "Target #%s has non-string or missing 'boxtype'; skipping.", idx
            )
            continue

        try:
            target = Quel1seTarget(
                name=name, wss_ip=wss_ip, css_ip=css_ip, boxtype_str=boxtype
            )
            valid.append(target)
        except ValueError as e:
            logger.warning("Target #%s validation failed: %s; skipping.", idx, e)
            continue

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


def get_cgroup_cpu_count() -> int:
    """Get the number of CPUs available in the container/cgroup.

    Returns:
        Number of CPUs available.

    """
    cpu_count = os.cpu_count() or 1
    try:
        quota = int(Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").read_text("utf-8"))
        period = int(Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us").read_text("utf-8"))
        if quota == -1:
            return cpu_count
        return min(quota // period, cpu_count)
    except FileNotFoundError:
        return cpu_count


def get_allowed_threads() -> int:
    """Get the number of allowed worker threads.

    Returns:
        Number of threads to use for parallel target collection.

    """
    max_allowed_threads = get_cgroup_cpu_count()
    try:
        num_workers = int(MAX_WORKERS_ENV)
        if num_workers > max_allowed_threads or num_workers < 1:
            num_workers = max_allowed_threads
    except ValueError:
        num_workers = max_allowed_threads
    logger.info("Using %d threads for collecting metrics.", num_workers)
    return num_workers


def collect_target_metrics(
    target: Quel1seTarget, timeout: float = DEFAULT_QUEL1SE_TIMEOUT
) -> CollectorResult:
    """Collect temperature and actuator metrics from a single QuEL-1 SE target.

    Args:
        target: The target configuration.
        timeout: Timeout in seconds for WSS operations.

    Returns:
        CollectorResult containing the collected metrics or error information.

    """
    result = CollectorResult(target=target)

    def _collect_metrics() -> tuple[dict[str, float], dict[str, dict[str, float]]]:
        box_local = None
        try:
            box_local = Quel1Box.create(
                ipaddr_wss=target.wss_ip,
                ipaddr_css=target.css_ip,
                boxtype=target.boxtype,
            )
            css = cast("Any", box_local.css)
            temperatures = css.get_tempctrl_temperature_now()
            actuators = css.get_tempctrl_actuator_output()
        finally:
            del box_local
        return temperatures, actuators

    try:
        # Use ThreadPoolExecutor to enforce timeout on _collect_metrics
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_collect_metrics)
            temperatures, actuators = future.result(timeout=timeout)
            result.temperatures = temperatures
            result.actuators = actuators

        result.success = True
        logger.debug(
            "Successfully collected metrics from target %s (wss:%s, css:%s).",
            target.name,
            target.wss_ip,
            target.css_ip,
        )

    except concurrent.futures.TimeoutError:
        result.error = f"Timeout after {timeout}s"
        logger.exception(
            "Timeout collecting metrics from %s (wss:%s, css:%s): %s",
            target.name,
            target.wss_ip,
            target.css_ip,
            result.error,
        )
    except ConnectionError as e:
        result.error = f"Connection error: {e}"
        logger.exception(
            "Connection error collecting metrics from %s (wss:%s, css:%s): %s",
            target.name,
            target.wss_ip,
            target.css_ip,
            result.error,
        )
    except Exception as e:
        result.error = f"Unexpected error: {e}"
        logger.exception(
            "Unexpected error collecting metrics from %s (wss:%s, css:%s): %s",
            target.name,
            target.wss_ip,
            target.css_ip,
            result.error,
        )

    return result


class Quel1seMetricsCollector:
    """Custom Prometheus collector for QuEL-1 SE metrics.

    Collects temperature and actuator metrics from configured QuEL-1 SE targets
    on each /metrics scrape request.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize the collector with configuration.

        Args:
            config: Configuration dictionary containing quel1se targets and settings.

        """
        self._config = config
        self.targets: list[Quel1seTarget] = config["quel1se"]["targets"]
        self.timeout = self._config.get("quel1se", {}).get(
            "timeout", DEFAULT_QUEL1SE_TIMEOUT
        )
        self.num_workers = get_allowed_threads()

    def collect(self) -> Iterator[GaugeMetricFamily]:
        """Collect metrics from all configured targets.

        This method is called by prometheus_client on each /metrics request.

        Yields:
            GaugeMetricFamily: Temperature and actuator metrics for all targets.

        """
        logger.debug("Starting metrics collection for scrape request.")

        # Define metric families
        m_temperature = GaugeMetricFamily(
            TEMPERATURE_METRIC_NAME,
            "Temperature readings from QuEL-1 SE sensors",
            labels=["target_name", "wss_ip", "location", "unit", "raw"],
        )

        m_actuator = GaugeMetricFamily(
            ACTUATOR_METRIC_NAME,
            "Actuator usage readings from QuEL-1 SE (duty ratio in [0,1])",
            labels=[
                "target_name",
                "wss_ip",
                "actuator_type",
                "location",
                "unit",
                "raw",
            ],
        )

        # Collect metrics from all targets in parallel
        results: list[CollectorResult] = []
        success_count = 0

        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = {
                executor.submit(collect_target_metrics, target, self.timeout): target
                for target in self.targets
            }

            for future in as_completed(futures):
                target = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    if result.success:
                        success_count += 1
                except Exception:
                    logger.exception(
                        "Metric collection task raised unexpectedly for %s",
                        target.name,
                    )
                    results.append(
                        CollectorResult(
                            target=target,
                            error="Task execution failed",
                        )
                    )

        # Process results and populate metrics
        for result in results:
            target = result.target
            target_labels_base = [target.name, target.wss_ip]

            # Add temperature metrics
            if result.temperatures:
                for location, value in result.temperatures.items():
                    m_temperature.add_metric(
                        [*target_labels_base, location, "celsius", "true"],
                        value,
                    )

            # Add actuator metrics
            if result.actuators:
                for actuator_type, locations in result.actuators.items():
                    for location, value in locations.items():
                        m_actuator.add_metric(
                            [
                                *target_labels_base,
                                actuator_type,
                                location,
                                "ratio",
                                "true",
                            ],
                            value,
                        )

        logger.debug(
            "Metrics collection completed. Success: %d/%d targets",
            success_count,
            len(self.targets),
        )

        yield m_temperature
        yield m_actuator


def setup_logging(config: dict) -> None:
    """Set up logging based on logging.yaml with configured timezone.

    Args:
        config: Configuration dictionary containing timezone setting.

    """
    logging_config_path = os.getenv(
        ENV_LOGGING_CONFIG_PATH, DEFAULT_LOGGING_CONFIG_PATH
    )

    # Determine timezone
    try:
        tz_string = config.get("exporter", {}).get("timezone", DEFAULT_TIMEZONE)
        tz = ZoneInfo(tz_string)
    except (KeyError, TypeError, ZoneInfoNotFoundError):
        logger.warning(
            "Invalid or missing timezone in config. Falling back to default: %s.",
            DEFAULT_TIMEZONE,
        )
        tz = ZoneInfo(DEFAULT_TIMEZONE)

    try:
        with pathlib.Path(logging_config_path).open("rt", encoding="utf-8") as f:
            log_config = yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning(
            "Logging config file not found at %s. Using basic configuration.",
            logging_config_path,
        )
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )
        return

    # Dynamically create and substitute timezone-aware formatters
    if "formatters" in log_config:
        for name, formatter_config in log_config["formatters"].items():
            if "class" in formatter_config:
                try:
                    module_path, class_name = formatter_config["class"].rsplit(".", 1)
                    module = import_module(module_path)
                    original_class = getattr(module, class_name)
                    tz_formatter_class = create_timezone_formatter(original_class, tz)
                    formatter_config["()"] = tz_formatter_class
                    del formatter_config["class"]
                except (ImportError, AttributeError, ValueError) as e:
                    logger.warning(
                        "Could not create timezone-aware formatter for '%s': %s",
                        name,
                        e,
                    )

    logging.config.dictConfig(log_config)
    logger.info(
        "Logging configured successfully from %s with timezone %s.",
        logging_config_path,
        tz,
    )


def load_config() -> dict | None:
    """Load configuration from YAML file with environment variable overrides.

    Returns:
        Configuration dictionary, or None if loading failed.

    """
    config_path = os.getenv(ENV_CONFIG_PATH, DEFAULT_CONFIG_PATH)

    with pathlib.Path(config_path).open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        logger.critical(
            "Invalid configuration format in %s: root is not a dictionary.",
            config_path,
        )
        return None

    logger.info("Configuration loaded successfully from %s.", config_path)

    # Ensure required sections exist
    if "exporter" not in config:
        config["exporter"] = {}
    if "quel1se" not in config:
        config["quel1se"] = {}

    # Validate targets
    targets = validate_quel1se_targets(config.get("quel1se", {}).get("targets", []))
    if not targets:
        logger.critical("No valid quel1se targets found in configuration.")
        return None
    config["quel1se"]["targets"] = targets

    # Apply environment variable overrides
    config["exporter"]["port"] = int(
        os.getenv(
            ENV_EXPORTER_PORT,
            config.get("exporter", {}).get("port", DEFAULT_EXPORTER_PORT),
        )
    )
    config["exporter"]["timezone"] = os.getenv(
        ENV_EXPORTER_TIMEZONE,
        config.get("exporter", {}).get("timezone", DEFAULT_TIMEZONE),
    )
    config["quel1se"]["timeout"] = int(
        os.getenv(
            ENV_QUEL1SE_TIMEOUT,
            config.get("quel1se", {}).get("timeout", DEFAULT_QUEL1SE_TIMEOUT),
        )
    )

    logger.info("Final configuration: %s", config)
    return config


if __name__ == "__main__":
    # Load configuration
    config = load_config()
    if config is None:
        logger.critical("Failed to load configuration. Exiting.")
        raise SystemExit(1)
    # Setup logging
    setup_logging(config)

    # Get server port
    server_port = config["exporter"]["port"]

    # Create and register custom collector
    registry = CollectorRegistry()
    registry.register(Quel1seMetricsCollector(config))

    # Start HTTP server
    start_http_server(server_port, registry=registry)
    logger.info("Exporter started, listening on port %s.", server_port)

    # Keep the process running
    while True:
        time.sleep(1)
