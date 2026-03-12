import copy
import logging
import logging.config
import os
import pathlib
import threading
import time
from collections.abc import Callable, Generator, Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from enum import StrEnum
from http.server import HTTPServer
from importlib import import_module
from typing import Any, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
import smbclient
import yaml
from omegaconf import OmegaConf
from prometheus_client import CollectorRegistry, GCCollector
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.exposition import MetricsHandler
from smbprotocol.exceptions import SMBAuthenticationError, SMBException


class MetricFamilyType(StrEnum):
    """Metric family types for the Cryo Metrics Exporter."""

    TEMPERATURE = "temperature"
    PRESSURE = "pressure"
    HELIUM_FLOW = "helium_flow"
    DEVICE_STATUS = "device_status"
    COMPRESSOR = "compressor"
    COMPRESSOR_PRESSURE = "compressor_pressure"


class SMBDataSource(StrEnum):
    """SMB Data source types for the Cryo Metrics Exporter."""

    PRESSURE = "pressure"
    GAS_FLOW_RATE = "gas_flow_rate"
    MACHINE_STATE = "machine_state"
    COMPRESSOR = "compressor"


# Setup logger
logger = logging.getLogger("cryo-metrics-exporter")

# Default configuration parameters
DEFAULT_EXPORTER_PORT = 9101
DEFAULT_EXPORTER_TIMEZONE = "UTC"
DEFAULT_SCRAPE_INTERVAL_SEC = 60
DEFAULT_MAX_EXPAND_WINDOWS_HTTP = 5
DEFAULT_MAX_EXPAND_WINDOWS_SMB = 5
DEFAULT_HTTP_DATASOURCE_TIMEZONE = "UTC"
DEFAULT_HTTP_PORT = 80
DEFAULT_HTTP_TIMEOUT_SEC = 5
DEFAULT_SMB_DATASOURCE_TIMEZONE = "UTC"
DEFAULT_SMB_PORT = 445
DEFAULT_SMB_BASE_PATH = ""
DEFAULT_SMB_TIMEOUT_SEC = 5

# Unit conversion factors
MILLIBAR_TO_KILOPASCAL = 0.1
MILLIBAR_TO_PASCAL = 100
MILLIMOLES_TO_MICROMOLES = 1000
PSIG_TO_MEGAPASCAL = 0.006894744825494

# Metric family configuration
METRIC_CONFIGS: dict[MetricFamilyType, dict[str, Any]] = {
    MetricFamilyType.TEMPERATURE: {
        "prometheus_name": "refrigerator_temperature",
        "description": "Temperature metrics of the refrigerator",
        "labels": ["device_name", "unit", "stage", "location", "raw"],
    },
    MetricFamilyType.PRESSURE: {
        "prometheus_name": "refrigerator_pressure",
        "description": "Pressure metrics of the refrigerator",
        "labels": ["device_name", "unit", "location", "raw"],
    },
    MetricFamilyType.HELIUM_FLOW: {
        "prometheus_name": "refrigerator_helium_flow",
        "description": "Helium flow metrics of the refrigerator",
        "labels": ["device_name", "unit", "raw"],
    },
    MetricFamilyType.DEVICE_STATUS: {
        "prometheus_name": "refrigerator_device_status",
        "description": "Device status metrics of the refrigerator",
        "labels": ["device_name", "unit", "component", "raw"],
    },
    MetricFamilyType.COMPRESSOR: {
        "prometheus_name": "refrigerator_compressor",
        "description": "Compressor metrics of the refrigerator",
        "labels": ["device_name", "unit", "rotation", "raw"],
    },
    MetricFamilyType.COMPRESSOR_PRESSURE: {
        "prometheus_name": "refrigerator_compressor_pressure",
        "description": "Compressor pressure metrics of the refrigerator",
        "labels": ["device_name", "unit", "side", "raw"],
    },
}

# SMB file naming templates
FILE_PATH_TEMPLATES = {
    SMBDataSource.PRESSURE: "<YY-MM-DD>/maxigauge <YY-MM-DD>.log",
    SMBDataSource.GAS_FLOW_RATE: "<YY-MM-DD>/Flowmeter <YY-MM-DD>.log",
    SMBDataSource.MACHINE_STATE: "<YY-MM-DD>/Channels <YY-MM-DD>.log",
    SMBDataSource.COMPRESSOR: "<YY-MM-DD>/Status_<YY-MM-DD>.log",
}

# Constants for parsing time from data lines
MIN_COLUMNS_TIME_PARSING = 2  # Minimum columns required for time parsing
DATE_INDEX = 0  # Index of the date column
TIME_INDEX = 1  # Index of the time column

# Constants for parsing pressure data lines
MIN_COLUMNS_PRESSURE_PARSING = 8  # Minimum columns required for pressure parsing
PRESSURE_START_INDEX = 2  # Index where pressure data starts
PRESSURE_COLUMN_STRIDE = 6  # Stride between channels in the data line
PRESSURE_VALUE_OFFSET = 3  # Offset to the pressure value within a channel's data

# Constants for parsing gas flow data lines
MIN_COLUMNS_GASFLOW_PARSING = 3  # Minimum columns required for gas flow parsing
GASFLOW_VALUE_OFFSET = 2  # Offset to the gas flow value within a channel's data

# Constants for parsing machine status data lines
MIN_COLUMNS_STATUS_PARSING = 5  # Minimum columns required for status parsing
STATUS_START_INDEX = 3  # Index where status data starts
STATUS_COLUMN_STRIDE = 2  # Stride between channels in the data line
STATUS_VALUE_OFFSET = 1  # Offset to the status value within a channel's data

# Constants for parsing pressure compressor lines
MIN_COLUMNS_COMPRESSOR_PARSING = 4  # Minimum columns required for compressor parsing
COMPRESSOR_START_INDEX = 2  # Index where pressure data starts
COMPRESSOR_COLUMN_STRIDE = 2  # Stride between channels in the data line
COMPRESSOR_VALUE_OFFSET = 1  # Offset to the pressure value within a channel's data

# Maximum number of worker threads for parallel data retrieval
MAX_WORKERS_COUNT = 2


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


def convert_pressure_unit(data: dict) -> dict:
    """Convert pressure data from millibar to pascal or kilopascal based on location.

    Args:
        data (dict): Original pressure data with labels and values.

    Returns:
        Converted pressure data with updated labels and values.

    """
    converted = copy.deepcopy(data)
    if converted["labels"]["location"] in {"before_trap", "after_trap", "tank"}:
        converted["labels"]["unit"] = "kilopascal"
        converted["labels"]["raw"] = "false"
        converted["values"] = [v * MILLIBAR_TO_KILOPASCAL for v in data["values"]]
    else:
        converted["labels"]["unit"] = "pascal"
        converted["labels"]["raw"] = "false"
        converted["values"] = [v * MILLIBAR_TO_PASCAL for v in data["values"]]
    return converted


def convert_flow_unit(data: dict) -> dict:
    """Convert gas flow data from millimols per second to mols per second.

    Args:
        data (dict): Original gas flow data with labels and values.

    Returns:
        Converted gas flow data with updated labels and values.

    """
    converted = copy.deepcopy(data)
    converted["labels"]["unit"] = "micromoles per second"
    converted["labels"]["raw"] = "false"
    converted["values"] = [v * MILLIMOLES_TO_MICROMOLES for v in data["values"]]
    return converted


def convert_psig_unit(data: dict) -> dict:
    """Convert compressor pressure data from psig to megapascals.

    Args:
        data (dict): Original gas flow data with labels and values.

    Returns:
        Converted gas flow data with updated labels and values.

    """
    converted = copy.deepcopy(data)
    converted["labels"]["unit"] = "megapascal"
    converted["labels"]["raw"] = "false"
    converted["values"] = [v * PSIG_TO_MEGAPASCAL for v in data["values"]]
    return converted


def get_timezone(tz_name: str, default_tz_name: str) -> ZoneInfo:
    """Get ZoneInfo object for the given timezone name.

    Args:
        tz_name (str): Timezone name.
        default_tz_name (str): Default timezone name.

    Returns:
        ZoneInfo object for the timezone, or default_tz_name if invalid.

    """
    try:
        tz = ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError):
        logger.exception(
            "Invalid timezone: %s. Using default timezone: %s.",
            tz_name,
            default_tz_name,
        )
        return ZoneInfo(default_tz_name)
    else:
        return tz


# Custom exceptions
class InternalServerError(Exception):
    """Internal Server Error exception."""


class ServiceUnavailableError(Exception):
    """Service Unavailable Error exception."""


class CustomCollector(GCCollector):
    """Collector that gathers custom metrics during Prometheus scrape."""

    def __init__(self, config: dict[str, Any]) -> None:
        # Store config reference
        self._config = config

        # Retrieval settings
        self._scrape_interval = config["retrieval"]["scrape_interval_sec"]
        self._max_expand_windows_http = config["retrieval"]["max_expand_windows"][
            "http"
        ]
        self._max_expand_windows_smb = config["retrieval"]["max_expand_windows"]["smb"]

        # HTTP settings
        self._http_url = config["sources"]["http"]["url"]
        self._http_port = config["sources"]["http"]["port"]
        self._http_timeout = config["sources"]["http"]["timeout_sec"]

        # SMB settings
        self._smb_server = config["sources"]["smb"]["server"]
        self._smb_share = config["sources"]["smb"]["share"]
        self._smb_port = config["sources"]["smb"]["port"]
        self._smb_user = config["sources"]["smb"]["username"]
        self._smb_timeout = config["sources"]["smb"]["timeout_sec"]
        self._smb_base_path = config["sources"]["smb"]["base_path"]

        # SMB password from environment variable
        self._smb_password = os.environ["SMB_PASSWORD"]

        # Timezone settings
        self._tz_exporter = get_timezone(
            config["exporter"]["timezone"], DEFAULT_EXPORTER_TIMEZONE
        )
        self._tz_http = get_timezone(
            config["sources"]["http"]["datasource_timezone"],
            DEFAULT_HTTP_DATASOURCE_TIMEZONE,
        )
        self._tz_smb = get_timezone(
            config["sources"]["smb"]["datasource_timezone"],
            DEFAULT_SMB_DATASOURCE_TIMEZONE,
        )

        # Counters for empty data retrievals
        self.empty_count_http = 0
        self.empty_count_smb = 0

        # Define channels configuration
        device_name = config["exporter"]["device_name"]
        self.temp_channels = {
            "1": {
                "device_name": device_name,
                "unit": "kelvin",
                "stage": "plate_50k",
                "location": "flange",
                "raw": "true",
            },
            "2": {
                "device_name": device_name,
                "unit": "kelvin",
                "stage": "plate_4k",
                "location": "flange",
                "raw": "true",
            },
            "5": {
                "device_name": device_name,
                "unit": "kelvin",
                "stage": "still",
                "location": "flange",
                "raw": "true",
            },
            "6": {
                "device_name": device_name,
                "unit": "kelvin",
                "stage": "mxc",
                "location": "flange",
                "raw": "true",
            },
        }
        self.http_targets = list(self.temp_channels.keys())

        self.smb_pressure_channels = {
            "CH1": {
                "device_name": device_name,
                "unit": "millibar",
                "location": "chamber_internal",
                "raw": "true",
            },
            "CH2": {
                "device_name": device_name,
                "unit": "millibar",
                "location": "still_tmp",
                "raw": "true",
            },
            "CH3": {
                "device_name": device_name,
                "unit": "millibar",
                "location": "after_trap",
                "raw": "true",
            },
            "CH4": {
                "device_name": device_name,
                "unit": "millibar",
                "location": "before_trap",
                "raw": "true",
            },
            "CH5": {
                "device_name": device_name,
                "unit": "millibar",
                "location": "tank",
                "raw": "true",
            },
            "CH6": {
                "device_name": device_name,
                "unit": "millibar",
                "location": "exhaust_pump",
                "raw": "true",
            },
        }

        self.smb_gasflow_channels = {
            "channel": {
                "device_name": device_name,
                "unit": "millimoles per second",
                "raw": "true",
            },
        }

        self.smb_stat_channels = {
            "scroll1": {
                "device_name": device_name,
                "unit": "None",
                "component": "scroll1",
                "raw": "true",
            },
            "scroll2": {
                "device_name": device_name,
                "unit": "None",
                "component": "scroll2",
                "raw": "true",
            },
            "turbo1": {
                "device_name": device_name,
                "unit": "None",
                "component": "turbo1",
                "raw": "true",
            },
            "turbo2": {
                "device_name": device_name,
                "unit": "None",
                "component": "turbo2",
                "raw": "true",
            },
            "pulsetube": {
                "device_name": device_name,
                "unit": "None",
                "component": "pulsetube",
                "raw": "true",
            },
        }

        self.smb_comp_channels = {
            "tc400actualspd": {
                "device_name": device_name,
                "unit": "Hz",
                "rotation": "actual_spd",
                "raw": "true",
            },
            "tc400actualspd_2": {
                "device_name": device_name,
                "unit": "Hz",
                "rotation": "actual_spd2",
                "raw": "true",
            },
            "tc400actualspd_3": {
                "device_name": device_name,
                "unit": "Hz",
                "rotation": "actual_spd3",
                "raw": "true",
            },
        }

        self.smb_comp_press_channels = {
            "cpalp": {
                "device_name": device_name,
                "unit": "psig",
                "side": "alp",
                "raw": "true",
            },
            "cpalp_2": {
                "device_name": device_name,
                "unit": "psig",
                "side": "alp2",
                "raw": "true",
            },
        }

    def _fetch_temperature_data(
        self, from_http: datetime, to_http: datetime, channel_nr: str
    ) -> tuple[dict[str, Any], bool]:
        """Retrieve temperature data from HTTP source.

        Args:
            from_http (datetime): Start time (with timezone).
            to_http (datetime): End time (with timezone).
            channel_nr (str): Channel number to retrieve.

        Returns:
            Retrieved data with labels, values and timestamps,
            and a boolean indicating if a retry is needed.

        Raises:
            InternalServerError: If there is an error during HTTP data retrieval.

        """
        # Convert times to ISO 8601 format with HTTP data source timezone
        start_time = from_http.astimezone(self._tz_http).isoformat()
        stop_time = to_http.astimezone(self._tz_http).isoformat()

        # Request body for HTTP POST request
        payload = {
            "channel_nr": int(channel_nr),
            "start_time": start_time,
            "stop_time": stop_time,
            "fields": [
                "timestamp",
                "resistance",
                "reactance",
                "temperature",
                "rez",
                "imz",
                "magnitude",
                "angle",
                "status_flags",
            ],
        }

        # Initialize results and retry flag
        is_retry_needed = False
        results = {
            "labels": copy.deepcopy(self.temp_channels[channel_nr]),
            "values": [],
            "timestamps": [],
            "metric_family": MetricFamilyType.TEMPERATURE,
        }

        # HTTP POST request to fetch data
        response = None
        try:
            response = requests.post(
                url=f"{self._http_url}:{self._http_port}/channel/historical-data",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self._http_timeout,
            )
            response.raise_for_status()
            raw_data = response.json()

            temp_list = raw_data.get("measurements", {}).get("temperature", [])
            time_list = raw_data.get("measurements", {}).get("timestamp", [])

        # Error handling for HTTP requests
        except requests.exceptions.HTTPError as err:
            not_found_error_status = {404}
            non_retryable_error_status = {400, 401, 403}
            status = getattr(err.response, "status_code", None)
            if status in non_retryable_error_status:
                logger.exception(
                    "HTTP request to channel %s failed with status code %s",
                    channel_nr,
                    status,
                )
                raise InternalServerError from err
            if status in not_found_error_status:
                logger.exception(
                    "HTTP request to channel %s failed with status code %s",
                    channel_nr,
                    status,
                )
            else:
                # Covers retryable status codes (5xx etc.), and general status codes
                is_retry_needed = True
                logger.exception(
                    "HTTP request to channel %s failed with status code %s",
                    channel_nr,
                    status,
                )
        except requests.exceptions.Timeout:
            is_retry_needed = True
            logger.exception(
                "HTTP connection to channel %s timed out (timeout=%s sec)",
                channel_nr,
                self._http_timeout,
            )
        except requests.exceptions.ConnectionError:
            is_retry_needed = True
            logger.exception(
                "HTTP request to channel %s failed due to network unreachable",
                channel_nr,
            )
        except requests.exceptions.RequestException:
            is_retry_needed = True
            logger.exception(
                "Unexpected error occurred during HTTP request to channel %s",
                channel_nr,
            )

        else:
            if len(temp_list) == 0:
                is_retry_needed = True
            logger.debug(
                "Retrieved %d temperature data from channel %s.",
                len(temp_list),
                channel_nr,
            )
            results["values"] = temp_list
            results["timestamps"] = time_list
        return (results, is_retry_needed)

    def _smb_connect(self) -> tuple[bool, bool]:
        """Register an SMB session with the configured parameters.

        Returns:
            Tuple of:
            - Boolean indicating if the connection was successful.
            - Boolean indicating if the retry is needed.

        Raises:
            InternalServerError: If an unexpected error occurs during SMB connection.

        """
        is_retry_needed = False
        is_registered = False
        try:
            smbclient.register_session(
                self._smb_server,
                username=self._smb_user,
                password=self._smb_password,
                port=self._smb_port,
                connection_timeout=self._smb_timeout,
            )
        except TimeoutError:
            is_retry_needed = True  # Retry is needed
            logger.exception(
                "SMB connection timed out to %s:%s as %s (timeout=%s sec)",
                self._smb_server,
                self._smb_port,
                self._smb_user,
                self._smb_timeout,
            )
            return (is_registered, is_retry_needed)
        except SMBAuthenticationError:
            is_retry_needed = True  # Retry is needed
            logger.exception(
                "SMB authentication failed to %s:%s as %s",
                self._smb_server,
                self._smb_port,
                self._smb_user,
            )
            return (is_registered, is_retry_needed)
        except (SMBException, OSError):
            is_retry_needed = True  # Retry is needed
            logger.exception(
                "SMB connection failed to %s:%s as %s",
                self._smb_server,
                self._smb_port,
                self._smb_user,
            )
            return (is_registered, is_retry_needed)
        except Exception as err:
            logger.exception(
                "SMB connection failed to %s:%s as %s due to unexpected error",
                self._smb_server,
                self._smb_port,
                self._smb_user,
            )
            raise InternalServerError from err
        else:
            is_registered = True
            logger.info(
                "SMB session registered to %s:%s as %s",
                self._smb_server,
                self._smb_port,
                self._smb_user,
            )
            return (is_registered, is_retry_needed)
        finally:
            # Ensure cleanup if connection failed
            if not is_registered:
                self.smb_disconnect()

    def smb_disconnect(self) -> None:
        """Close the SMB session gracefully."""
        try:
            smbclient.delete_session(self._smb_server, port=self._smb_port)
            logger.info("SMB session closed.")
        except Exception:
            logger.exception("SMB session close failed.")

    def fetch_smb_file_data(self, file_path: str) -> list[str] | None:
        """Fetch all lines from a file on the SMB share.

        Args:
            file_path (str): Path to the file on the SMB share.

        Returns:
            List of lines from the file, or None if retrieval fails.

        Raises:
            InternalServerError: If an unexpected error occurs during file access.

        """
        smb_path = f"\\\\{self._smb_server}\\{self._smb_share}\\{file_path}"
        try:
            with smbclient.open_file(smb_path, mode="r", encoding="utf-8") as f:
                all_lines = f.read().splitlines()
        except FileNotFoundError:
            logger.exception("File not found: %s", smb_path)
            return None
        except PermissionError:
            logger.exception(
                "A permission error occurred when accessing file: %s", smb_path
            )
            return None
        except TimeoutError:
            logger.exception(
                "SMB connection timed out when accessing file: %s", smb_path
            )
            return None
        except (SMBException, OSError):
            logger.exception("SMB connection failed when accessing file: %s", smb_path)
            return None
        except Exception as err:
            logger.exception(
                "Unexpected error occurred when accessing file: %s", smb_path
            )
            raise InternalServerError from err
        else:
            logger.info("Fetched %d lines from SMB file: %s", len(all_lines), smb_path)
            return all_lines

    def _parse_time(
        self, columns: list[str], line_number: int, file_path: str
    ) -> datetime | None:
        """Parse datetime from a SMB file line.

        Args:
            columns (list[str]): List of columns from a data line.
            line_number (int): Line number in the file (for logging).
            file_path (str): Path to the file (for logging).

        Returns:
            Parsed datetime if within time range for current scrape or None.
            Also None if parsing fails.

        """
        # Check minimum number of columns for time parsing
        if len(columns) < MIN_COLUMNS_TIME_PARSING:
            logger.error(
                "Insufficient columns for time parsing in %s at line %d",
                file_path,
                line_number,
            )
            return None

        # Convert date(DD-MM-YY) and time(HH:MM:SS) strings to datetime with timezone
        try:
            day, month, year = columns[DATE_INDEX].split("-")
            hour, minute, second = columns[TIME_INDEX].split(":")
            dt = datetime(
                year=int(f"20{year}"),  # Assuming 21st century for 2-digit year
                month=int(month),
                day=int(day),
                hour=int(hour),
                minute=int(minute),
                second=int(second),
                tzinfo=self._tz_smb,
            )
        except (ValueError, AttributeError, IndexError):
            logger.exception(
                "Invalid datetime format in %s at line %d",
                file_path,
                line_number,
            )
            return None
        else:
            return dt

    def _parse_pressure_line(
        self, columns: list[str], line_number: int, file_path: str
    ) -> dict[str, float] | None:
        """Parse pressure values from a SMB file line.

        Args:
            columns (list[str]): List of columns from a data line.
            line_number (int): Line number in the file (for logging).
            file_path (str): Path to the file (for logging).

        Returns:
            Dictionary mapping channel names to pressure values,
            or None if parsing fails.

        """
        # Check minimum number of columns for pressure parsing
        if len(columns) < MIN_COLUMNS_PRESSURE_PARSING:
            logger.error(
                "Insufficient columns for pressure parsing in %s at line %d",
                file_path,
                line_number,
            )
            return None

        # Initialize result dictionary with None values
        result: dict[str, float | None] = dict.fromkeys(self.smb_pressure_channels)

        # Extract pressure values for each channel from the data line
        for i in range(PRESSURE_START_INDEX, len(columns), PRESSURE_COLUMN_STRIDE):
            try:
                channel_name = columns[i]
                if channel_name not in result:
                    continue
                # Validate pressure value (must be positive)
                pressure_value = float(columns[i + PRESSURE_VALUE_OFFSET])
                if pressure_value <= 0:
                    logger.error(
                        "Invalid pressure value in %s at line %d: %s",
                        file_path,
                        line_number,
                        pressure_value,
                    )
                    return None
                result[channel_name] = pressure_value
            except (IndexError, ValueError):
                logger.exception(
                    "Invalid pressure data format in %s at line %d",
                    file_path,
                    line_number,
                )
                return None

        # Check for devices with missing data
        missing_devices = [key for key, value in result.items() if value is None]
        if missing_devices:
            logger.error(
                "Devices %s have no data in %s at line %d",
                missing_devices,
                file_path,
                line_number,
            )
            return None
        return cast("dict[str, float]", result)

    @staticmethod
    def _parse_gasflow_line(
        columns: list[str], line_number: int, file_path: str
    ) -> float | None:
        """Parse gas flow rate value from a SMB file line.

        Args:
            columns (list[str]): List of columns from a data line.
            line_number (int): Line number in the file (for logging).
            file_path (str): Path to the file (for logging).

        Returns:
            Dictionary mapping channel names to gas flow rate value,
            or None if parsing fails.

        """
        # Check minimum number of columns for gas flow rate parsing
        if len(columns) < MIN_COLUMNS_GASFLOW_PARSING:
            logger.error(
                "Insufficient columns for gas flow rate parsing in %s at line %d",
                file_path,
                line_number,
            )
            return None

        # Extract gas flow rate value from the data line
        try:
            # Validate gas flow rate value (must be non-negative)
            gasflow_value = float(columns[GASFLOW_VALUE_OFFSET])
            if gasflow_value < 0:
                logger.error(
                    "Invalid gas flow rate value in %s at line %d: %s",
                    file_path,
                    line_number,
                    gasflow_value,
                )
                return None
        except ValueError:
            logger.exception(
                "Invalid gas flow rate data format in %s at line %d",
                file_path,
                line_number,
            )
            return None
        return gasflow_value

    def _parse_status_line(
        self, columns: list[str], line_number: int, file_path: str
    ) -> dict[str, int] | None:
        """Parse machine state values from a SMB file line.

        Args:
            columns (list[str]): List of columns from a data line.
            line_number (int): Line number in the file (for logging).
            file_path (str): Path to the file (for logging).

        Returns:
            Dictionary mapping device names to status values (0 or 1),
            or None if parsing fails.

        """
        # Check minimum number of columns for Machine state parsing
        if len(columns) < MIN_COLUMNS_STATUS_PARSING:
            logger.error(
                "Insufficient columns for machine state parsing in %s at line %d",
                file_path,
                line_number,
            )
            return None

        # Initialize result dictionary with None values
        result: dict[str, int | None] = dict.fromkeys(self.smb_stat_channels)

        # Extract status values for each device from the data line
        for i in range(STATUS_START_INDEX, len(columns), STATUS_COLUMN_STRIDE):
            try:
                device_name = columns[i]
                if device_name not in result:
                    continue
                # Validate machine state value (must be 0 or 1)
                status_value = int(columns[i + STATUS_VALUE_OFFSET])
                if status_value not in {0, 1}:
                    logger.error(
                        "Invalid machine state value in %s at line %d: %s",
                        file_path,
                        line_number,
                        status_value,
                    )
                    return None
                result[device_name] = status_value
            except (IndexError, ValueError):
                logger.exception(
                    "Invalid machine state data format in %s at line %d",
                    file_path,
                    line_number,
                )
                return None

        # Check for devices with missing data
        missing_devices = [key for key, value in result.items() if value is None]
        if missing_devices:
            logger.error(
                "Devices %s have no data in %s at line %d",
                missing_devices,
                file_path,
                line_number,
            )
            return None
        return cast("dict[str, int]", result)

    def _parse_compressor_line(
        self, columns: list[str], line_number: int, file_path: str
    ) -> tuple[dict[str, float], dict[str, float]] | None:
        """Parse compressor values from a SMB file line.

        Args:
            columns (list[str]): List of columns from a data line.
            line_number (int): Line number in the file (for logging).
            file_path (str): Path to the file (for logging).

        Returns:
            Tuple of two dictionaries:
            - Compressor values mapping to parameter names.
            - Compressor pressure values mapping to parameter names.

        """
        # Check minimum number of columns for compressor data parsing
        if len(columns) < MIN_COLUMNS_COMPRESSOR_PARSING:
            logger.error(
                "Insufficient columns for compressor parsing in %s at line %d",
                file_path,
                line_number,
            )
            return None

        # Initialize result dictionaries with None values
        result_comp: dict[str, float | None] = dict.fromkeys(self.smb_comp_channels)
        result_comp_press: dict[str, float | None] = dict.fromkeys(
            self.smb_comp_press_channels
        )

        # Extract compressor data from the data line
        for i in range(COMPRESSOR_START_INDEX, len(columns), COMPRESSOR_COLUMN_STRIDE):
            try:
                param_name = columns[i]
                value = float(columns[i + COMPRESSOR_VALUE_OFFSET])
                # Add Compressor values
                if param_name in result_comp:
                    result_comp[param_name] = value
                    continue
                # Add Compressor pressure values
                if param_name in result_comp_press:
                    result_comp_press[param_name] = value
                    continue
            except (IndexError, ValueError):
                logger.exception(
                    "Invalid compressor data format in %s at line %d",
                    file_path,
                    line_number,
                )
                return None

        # Check for device names with missing data
        all_results = {**result_comp, **result_comp_press}
        missing_devices = [key for key, value in all_results.items() if value is None]
        if missing_devices:
            logger.error(
                "Devices %s have no data in %s at line %d",
                missing_devices,
                file_path,
                line_number,
            )
            return None
        return cast(
            "tuple[dict[str, float], dict[str, float]]",
            (result_comp, result_comp_press),
        )

    def generate_file_path(
        self, from_smb: datetime, to_smb: datetime, data_type: SMBDataSource
    ) -> list[str]:
        """Generate SMB file paths based on time range and SMB data source types.

        Args:
            from_smb (datetime): Start time (with timezone).
            to_smb (datetime): End time (with timezone).
            data_type (SMBDataSource): Type of SMB data source.

        Returns:
            List of generated file paths.

        """
        # Get date range
        from_date = from_smb.date()
        to_date = to_smb.date()

        # Generate list of directories for the target date range
        date_dirs = []
        current_date = from_date
        while current_date <= to_date:
            dir_name = current_date.strftime("%y-%m-%d")
            date_dirs.append(dir_name)
            current_date += timedelta(days=1)

        # Generate file paths based on the date directories
        base_path = self._smb_base_path
        if base_path and not base_path.endswith("/"):
            base_path = f"{base_path}/"
        path_template = FILE_PATH_TEMPLATES[data_type]
        return [
            f"{base_path}{path_template.replace('<YY-MM-DD>', date_dir)}"
            for date_dir in date_dirs
        ]

    def _fetch_smb_data_generic(
        self,
        from_smb: datetime,
        to_smb: datetime,
        file_paths: list[str],
        parser: Callable,
    ) -> Generator[tuple[datetime, dict | None]]:
        """Fetch parsed data from SMB files.

        Args:
            from_smb (datetime): Start time (with timezone).
            to_smb (datetime): End time (with timezone).
            file_paths (list[str]): List of file paths to fetch data from.
            parser (Callable): Function to parse a line into data dictionary.

        Yields:
            Tuples of (datetime, parsed data dictionary).

        """
        for file_path in file_paths:
            # Fetch all lines of each file on SMB share
            all_lines = self.fetch_smb_file_data(file_path)
            if not all_lines:
                continue
            # Process each line in the file
            for line_number, line in enumerate(all_lines, start=1):
                if not line:
                    continue
                columns = line.split(",")
                # Parse and validate datetime from the line
                parsed_time = self._parse_time(columns, line_number, file_path)
                if parsed_time is None:
                    continue
                if parsed_time < from_smb or parsed_time >= to_smb:
                    if parsed_time >= to_smb:
                        break
                    continue
                # Parse data from the line
                parsed_data = parser(columns, line_number, file_path)
                yield (parsed_time, parsed_data)

    def _fetch_smb_pressure_data(
        self, from_smb: datetime, to_smb: datetime
    ) -> tuple[list[dict[str, Any]], bool]:
        """Fetch pressure data with given time range.

        Args:
            from_smb (datetime): Start time (with timezone).
            to_smb (datetime): End time (with timezone).

        Returns:
            Tuple of:
            - List of dictionaries with pressure data per channel.
            - Boolean indicating if a retry is needed.

        """
        # Initialize result structure with all channels
        ret_val: list[dict[str, Any]] = [
            {
                "labels": copy.deepcopy(ch_dict),
                "values": [],
                "timestamps": [],
                "metric_family": MetricFamilyType.PRESSURE,
            }
            for ch_dict in self.smb_pressure_channels.values()
        ]

        # Create channel name to index mapping for efficient access
        ch_index_map = {
            ch_name: idx
            for idx, ch_name in enumerate(self.smb_pressure_channels.keys())
        }

        # Fetch and parse pressure data from SMB files
        file_paths = self.generate_file_path(from_smb, to_smb, SMBDataSource.PRESSURE)
        for dt, parsed_data in self._fetch_smb_data_generic(
            from_smb, to_smb, file_paths, self._parse_pressure_line
        ):
            if parsed_data is None:
                continue
            for channel_name, value in parsed_data.items():
                idx = ch_index_map[channel_name]
                ret_val[idx]["values"].append(value)
                ret_val[idx]["timestamps"].append(int(dt.timestamp()))
        # Determine if retry is needed (any channel has no data)
        is_retry_needed = any(len(item["values"]) == 0 for item in ret_val)
        return (ret_val, is_retry_needed)

    def _fetch_smb_gasflow_data(
        self, from_smb: datetime, to_smb: datetime
    ) -> tuple[list[dict[str, Any]], bool]:
        """Fetch gas flow rate data with given time range.

        Args:
            from_smb (datetime): Start time (with timezone).
            to_smb (datetime): End time (with timezone).

        Returns:
            Tuple of:
            - List of dictionaries with gas flow rate data.
            - Boolean indicating if a retry is needed.

        """
        # Initialize result structure with all channels
        ret_val: dict[str, Any] = {
            "labels": copy.deepcopy(self.smb_gasflow_channels["channel"]),
            "values": [],
            "timestamps": [],
            "metric_family": MetricFamilyType.HELIUM_FLOW,
        }

        # Fetch and parse gas flow rate data from SMB files
        file_paths = self.generate_file_path(
            from_smb, to_smb, SMBDataSource.GAS_FLOW_RATE
        )
        for dt, parsed_data in self._fetch_smb_data_generic(
            from_smb, to_smb, file_paths, self._parse_gasflow_line
        ):
            if parsed_data is not None:
                ret_val["values"].append(parsed_data)
                ret_val["timestamps"].append(int(dt.timestamp()))
        # Determine if retry is needed (no data retrieved)
        is_retry_needed = len(ret_val["values"]) == 0
        return ([ret_val], is_retry_needed)

    def _fetch_smb_status_data(
        self, from_smb: datetime, to_smb: datetime
    ) -> tuple[list[dict[str, Any]], bool]:
        """Fetch machine state data with given time range.

        Args:
            from_smb (datetime): Start time (with timezone).
            to_smb (datetime): End time (with timezone).

        Returns:
            Tuple of:
            - List of dictionaries with status data per device.
            - Boolean indicating if a retry is needed.

        """
        # Initialize result structure with all channels
        ret_val: list[dict[str, Any]] = [
            {
                "labels": copy.deepcopy(ch_dict),
                "values": [],
                "timestamps": [],
                "metric_family": MetricFamilyType.DEVICE_STATUS,
            }
            for ch_dict in self.smb_stat_channels.values()
        ]

        # Create channel name to index mapping for efficient access
        ch_index_map = {
            ch_name: idx for idx, ch_name in enumerate(self.smb_stat_channels.keys())
        }

        # Fetch and parse machine state data from SMB files
        file_paths = self.generate_file_path(
            from_smb, to_smb, SMBDataSource.MACHINE_STATE
        )
        for dt, parsed_data in self._fetch_smb_data_generic(
            from_smb, to_smb, file_paths, self._parse_status_line
        ):
            if parsed_data is None:
                continue
            for device_name, value in parsed_data.items():
                idx = ch_index_map[device_name]
                ret_val[idx]["values"].append(value)
                ret_val[idx]["timestamps"].append(int(dt.timestamp()))
        # Determine if retry is needed (any device has no data)
        is_retry_needed = any(len(item["values"]) == 0 for item in ret_val)
        return (ret_val, is_retry_needed)

    def _fetch_smb_compressor_data(
        self, from_smb: datetime, to_smb: datetime
    ) -> tuple[list[dict[str, Any]], bool]:
        """Fetch compressor data with given time range.

        Args:
            from_smb (datetime): Start time (with timezone).
            to_smb (datetime): End time (with timezone).

        Returns:
            Tuple of:
            - List of dictionaries with combined compressor and compressor pressure.
            - Boolean indicating if a retry is needed.

        """
        # Initialize result structure with all channels
        ret_val_comp: list[dict[str, Any]] = [
            {
                "labels": copy.deepcopy(ch_dict),
                "values": [],
                "timestamps": [],
                "metric_family": MetricFamilyType.COMPRESSOR,
            }
            for ch_dict in self.smb_comp_channels.values()
        ]
        ret_val_comp_press: list[dict[str, Any]] = [
            {
                "labels": copy.deepcopy(ch_dict),
                "values": [],
                "timestamps": [],
                "metric_family": MetricFamilyType.COMPRESSOR_PRESSURE,
            }
            for ch_dict in self.smb_comp_press_channels.values()
        ]

        # Create channel name to index mapping for efficient access
        ch_index_map = {
            ch_name: idx for idx, ch_name in enumerate(self.smb_comp_channels.keys())
        }
        ch_index_map_press = {
            ch_name: idx
            for idx, ch_name in enumerate(self.smb_comp_press_channels.keys())
        }

        # Fetch and parse compressor data from SMB files
        file_paths = self.generate_file_path(from_smb, to_smb, SMBDataSource.COMPRESSOR)
        for dt, parsed_data in self._fetch_smb_data_generic(
            from_smb, to_smb, file_paths, self._parse_compressor_line
        ):
            if parsed_data is None:
                continue
            comp_data, comp_press_data = parsed_data
            for param_name, value in comp_data.items():
                idx = ch_index_map[param_name]
                ret_val_comp[idx]["values"].append(value)
                ret_val_comp[idx]["timestamps"].append(int(dt.timestamp()))
            for param_name, value in comp_press_data.items():
                idx = ch_index_map_press[param_name]
                ret_val_comp_press[idx]["values"].append(value)
                ret_val_comp_press[idx]["timestamps"].append(int(dt.timestamp()))
        # Determine if retry is needed (any channel has no data)
        is_retry_needed_comp = any(len(item["values"]) == 0 for item in ret_val_comp)
        is_retry_needed_press = any(
            len(item["values"]) == 0 for item in ret_val_comp_press
        )

        # Combine results
        all_results = ret_val_comp + ret_val_comp_press
        return (all_results, is_retry_needed_comp or is_retry_needed_press)

    def _fetch_all_smb_data(
        self, from_smb: datetime, to_smb: datetime
    ) -> tuple[list[dict[str, Any]], bool]:
        """Fetch all SMB data sequentially.

        Args:
            from_smb (datetime): Start time (with timezone).
            to_smb (datetime): End time (with timezone).

        Returns:
            Tuple of:
            - Flattened list of all data dictionaries with metric_family field.
            - Boolean indicating if a retry is needed.

        """
        # Convert time ranges to SMB data source timezone
        from_smb_tz = from_smb.astimezone(self._tz_smb)
        to_smb_tz = to_smb.astimezone(self._tz_smb)

        data_sources = [
            (SMBDataSource.PRESSURE, self._fetch_smb_pressure_data),
            (SMBDataSource.GAS_FLOW_RATE, self._fetch_smb_gasflow_data),
            (SMBDataSource.MACHINE_STATE, self._fetch_smb_status_data),
            (SMBDataSource.COMPRESSOR, self._fetch_smb_compressor_data),
        ]

        all_smb_results: list[dict[str, Any]] = []
        retry_flags = []

        # Establish SMB connection
        is_connected, is_retry_needed = self._smb_connect()
        if not is_connected:
            return (all_smb_results, is_retry_needed)

        # Fetch data from all SMB data sources sequentially
        try:
            for source_name, fetch_method in data_sources:
                results, is_retry_needed = fetch_method(from_smb_tz, to_smb_tz)
                retry_flags.append(is_retry_needed)
                # Append only non-empty results
                added_count = 0
                for result in results:
                    if result["values"]:
                        all_smb_results.append(result)
                        added_count += len(result["values"])
                logger.debug(
                    "Retrieved %s records from SMB data source: %s",
                    added_count,
                    source_name,
                )
        finally:
            # close SMB connection
            self.smb_disconnect()

        # Log summary of SMB data retrieval
        if all_smb_results:
            logger.info(
                "Successfully retrieved %s records from SMB data sources.",
                sum(len(r["values"]) for r in all_smb_results),
            )
        else:
            logger.error("No data retrieved from any SMB data source.")
        return (all_smb_results, any(retry_flags))

    def _process_http_data(
        self,
        http_futures: list,
        metric_families: dict[MetricFamilyType, GaugeMetricFamily],
    ) -> tuple[list[bool], int]:
        """Process HTTP future results and add metrics to metric families.

        Args:
            http_futures (list): List of Future objects for HTTP data retrieval.
            metric_families (dict[MetricFamilyType, GaugeMetricFamily]):
                Dictionary of metric families to update.

        Returns:
            Tuple of:
            - List of retry flags for each HTTP channel.
            - Total count of data records retrieved.

        """
        http_retry_flgs: list[bool] = []
        http_data_count = 0
        for http_future in http_futures:
            result, is_retry_needed = http_future.result()
            http_retry_flgs.append(is_retry_needed)
            # Add metrics if data is available
            if result["values"]:
                self._add_metrics(result, metric_families[result["metric_family"]])
                http_data_count += len(result["values"])

        # Log summary of HTTP data retrieval
        if http_data_count > 0:
            logger.info(
                "Successfully retrieved %d records from HTTP data sources.",
                http_data_count,
            )
        else:
            logger.error("No data retrieved from any HTTP data source.")
        return (http_retry_flgs, http_data_count)

    def _process_smb_data(
        self,
        smb_results: list[dict[str, Any]],
        metric_families: dict[MetricFamilyType, GaugeMetricFamily],
    ) -> None:
        """Process all SMB data results and add metrics to the metric families.

        Args:
            smb_results (list[dict[str, Any]]):
                List of data dictionaries with metric_family field.
            metric_families (dict[MetricFamilyType, GaugeMetricFamily]):
                Dictionary of metric families to update.

        """
        for data in smb_results:
            # Add metrics
            metric_family_name = data["metric_family"]
            self._add_metrics(data, metric_families[metric_family_name])

            # Add unit converted metrics if applicable
            if metric_family_name == MetricFamilyType.PRESSURE:
                converted = convert_pressure_unit(data)
                self._add_metrics(converted, metric_families[metric_family_name])
            elif metric_family_name == MetricFamilyType.HELIUM_FLOW:
                converted = convert_flow_unit(data)
                self._add_metrics(converted, metric_families[metric_family_name])
            elif metric_family_name == MetricFamilyType.COMPRESSOR_PRESSURE:
                converted = convert_psig_unit(data)
                self._add_metrics(converted, metric_families[metric_family_name])

    @staticmethod
    def _add_metrics(data: dict, metric_family: GaugeMetricFamily) -> None:
        """Add metric data to metric family.

        Args:
            data (dict): Data dictionary with 'labels', 'values', 'timestamps'.
            metric_family (GaugeMetricFamily): Metric family to add metrics to.

        """
        for value, timestamp in zip(data["values"], data["timestamps"], strict=True):
            metric_family.add_metric(
                list(data["labels"].values()), value, timestamp=timestamp
            )

    def compute_time_ranges(self) -> dict[str, datetime]:
        """Compute time ranges for HTTP and SMB data retrieval.

        Returns:
            Dictionary with 'from_http', 'to_http', 'from_smb', 'to_smb' datetimes.

        """
        now = datetime.now(self._tz_exporter).replace(microsecond=0)

        # Compute expanded time windows based on empty counts
        w_http = min(self.empty_count_http + 1, self._max_expand_windows_http)
        w_smb = min(self.empty_count_smb + 1, self._max_expand_windows_smb)
        logger.info(
            "Time ranges for data retrieval: HTTP window=%d sec, SMB window=%d sec",
            w_http * self._scrape_interval,
            w_smb * self._scrape_interval,
        )

        return {
            "from_http": now - timedelta(seconds=self._scrape_interval * w_http),
            "to_http": now,
            "from_smb": now - timedelta(seconds=self._scrape_interval * w_smb),
            "to_smb": now,
        }

    @staticmethod
    def setup_metric_families() -> dict[MetricFamilyType, GaugeMetricFamily]:
        """Define Prometheus metric families for the exporter.

        Returns:
            Dictionary of Prometheus metric families by MetricFamilyType.

        """
        return {
            key: GaugeMetricFamily(
                config["prometheus_name"],
                config["description"],
                labels=config["labels"],
            )
            for key, config in METRIC_CONFIGS.items()
        }

    def _update_empty_counts(
        self,
        http_retry_flgs: list[bool],
        *,
        is_smb_retry_needed: bool,
        is_http_internal_server_error: bool,
        is_smb_internal_server_error: bool,
        http_data_count: int,
    ) -> None:
        """Update empty counts for HTTP and SMB data sources based on retrieval results.

        Args:
            http_retry_flgs (list[bool]) : List of retry flags for each HTTP channel.
            is_smb_retry_needed (bool) : Retry flag for SMB data retrieval.
            is_http_internal_server_error (bool) :
                Flag indicating if InternalServerError occurred during HTTP retrieval.
            is_smb_internal_server_error (bool) :
                Flag indicating if InternalServerError occurred during SMB retrieval.
            http_data_count (int) :
                Total count of data records retrieved from HTTP sources.

        """
        # Update empty counts based on HTTP retrieval results
        if not is_http_internal_server_error:
            if any(http_retry_flgs):
                self.empty_count_http = min(
                    self.empty_count_http + 1, self._max_expand_windows_http - 1
                )
            elif http_data_count > 0:
                self.empty_count_http = 0
        logger.info("Empty count for HTTP sources is %d.", self.empty_count_http)

        # Update empty counts based on SMB retrieval results
        if not is_smb_internal_server_error:
            if is_smb_retry_needed:
                self.empty_count_smb = min(
                    self.empty_count_smb + 1, self._max_expand_windows_smb - 1
                )
            else:
                self.empty_count_smb = 0
        logger.info("Empty count for SMB sources is %d.", self.empty_count_smb)

    def collect(self) -> Generator[GaugeMetricFamily]:
        """Collect metrics by fetching data from HTTP and SMB sources.

        Yields:
            Collected metrics for temperature and pressure.

        Raises:
            InternalServerError: If an internal error occurs during data retrieval.
            ServiceUnavailableError: If all data sources fail.

        """
        logger.info("Starting data retrieval for metric collection.")

        # Calculate time ranges for HTTP and SMB data retrieval
        time_ranges = self.compute_time_ranges()

        # Initialize metric families
        metric_families = self.setup_metric_families()

        http_retry_flgs: list[bool] = []
        smb_retry_flg = False

        # Execute data retrieval tasks in parallel:
        # - HTTP: multiple channels in parallel
        # - SMB: single thread (sequential)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_COUNT) as executor:
            # HTTP tasks
            http_futures = []
            for ch in self.http_targets:
                http_future = executor.submit(
                    self._fetch_temperature_data,
                    time_ranges["from_http"],
                    time_ranges["to_http"],
                    ch,
                )
                http_futures.append(http_future)
            # SMB tasks
            smb_future = executor.submit(
                self._fetch_all_smb_data,
                time_ranges["from_smb"],
                time_ranges["to_smb"],
            )

            # Process HTTP results
            http_data_count = 0
            http_internal_server_error = False
            try:
                http_retry_flgs, http_data_count = self._process_http_data(
                    http_futures, metric_families
                )
            except InternalServerError:
                http_internal_server_error = True
                logger.exception(
                    "Internal Server Error occurred during HTTP data retrieval."
                )

            # Process SMB results
            smb_internal_server_error = False
            smb_all_results: list[dict[str, Any]] = []
            try:
                smb_all_results, smb_retry_flg = smb_future.result()
                self._process_smb_data(smb_all_results, metric_families)
            except InternalServerError:
                smb_retry_flg = False
                smb_internal_server_error = True
                logger.exception(
                    "Internal Server Error occurred during SMB data retrieval."
                )

        # Update empty counts based on retrieval results
        self._update_empty_counts(
            http_retry_flgs,
            is_smb_retry_needed=smb_retry_flg,
            is_http_internal_server_error=http_internal_server_error,
            is_smb_internal_server_error=smb_internal_server_error,
            http_data_count=http_data_count,
        )

        # Raise InternalServerError if it occurred in both HTTP and SMB retrieval
        if http_internal_server_error and smb_internal_server_error:
            raise InternalServerError

        # Raise InternalServerError if one source has no data and the other has an error
        if http_data_count == 0 and smb_internal_server_error:
            raise InternalServerError
        if not smb_all_results and http_internal_server_error:
            raise InternalServerError

        # Raise ServiceUnavailableError if both HTTP and SMB data retrieval failed
        if http_data_count == 0 and not smb_all_results:
            logger.error("All data sources failed to provide data.")
            raise ServiceUnavailableError

        yield from metric_families.values()


def setup_config() -> dict[str, Any]:
    """Load configuration parameters and apply environment variable overrides.

    Returns:
        Configuration dictionary.

    Raises:
        KeyError: If a required configuration parameter is missing.

    """
    config_path = "/config/config.yaml"  # path in the container
    config = OmegaConf.load(config_path)

    # Define configuration parameters with environment variable names and defaults
    config_parameters = {
        "exporter.port": ("EXPORTER_PORT", int, DEFAULT_EXPORTER_PORT),
        "exporter.timezone": ("EXPORTER_TIMEZONE", str, DEFAULT_EXPORTER_TIMEZONE),
        "exporter.device_name": ("EXPORTER_DEVICE_NAME", str, None),
        "retrieval.scrape_interval_sec": (
            "RETRIEVAL_SCRAPE_INTERVAL_SEC",
            int,
            DEFAULT_SCRAPE_INTERVAL_SEC,
        ),
        "retrieval.max_expand_windows.http": (
            "RETRIEVAL_MAX_EXPAND_WINDOWS_HTTP",
            int,
            DEFAULT_MAX_EXPAND_WINDOWS_HTTP,
        ),
        "retrieval.max_expand_windows.smb": (
            "RETRIEVAL_MAX_EXPAND_WINDOWS_SMB",
            int,
            DEFAULT_MAX_EXPAND_WINDOWS_SMB,
        ),
        "sources.http.datasource_timezone": (
            "SOURCES_HTTP_DATASOURCE_TIMEZONE",
            str,
            DEFAULT_HTTP_DATASOURCE_TIMEZONE,
        ),
        "sources.http.url": ("SOURCES_HTTP_URL", str, None),
        "sources.http.port": ("SOURCES_HTTP_PORT", int, DEFAULT_HTTP_PORT),
        "sources.http.timeout_sec": (
            "SOURCES_HTTP_TIMEOUT_SEC",
            int,
            DEFAULT_HTTP_TIMEOUT_SEC,
        ),
        "sources.smb.datasource_timezone": (
            "SOURCES_SMB_DATASOURCE_TIMEZONE",
            str,
            DEFAULT_SMB_DATASOURCE_TIMEZONE,
        ),
        "sources.smb.server": ("SOURCES_SMB_SERVER", str, None),
        "sources.smb.share": ("SOURCES_SMB_SHARE", str, None),
        "sources.smb.port": ("SOURCES_SMB_PORT", int, DEFAULT_SMB_PORT),
        "sources.smb.username": ("SOURCES_SMB_USERNAME", str, None),
        "sources.smb.base_path": (
            "SOURCES_SMB_BASE_PATH",
            str,
            DEFAULT_SMB_BASE_PATH,
        ),
        "sources.smb.timeout_sec": (
            "SOURCES_SMB_TIMEOUT_SEC",
            int,
            DEFAULT_SMB_TIMEOUT_SEC,
        ),
    }

    for yaml_path, (env_var, cast_type, default) in config_parameters.items():
        try:
            # Set configuration parameters from environment variables
            param_value = cast_type(os.environ[env_var])
        except KeyError:
            yaml_param = OmegaConf.select(config, yaml_path)
            if yaml_param is not None:
                param_value = yaml_param  # Fallback to YAML configuration value
            else:
                if default is None:  # Required parameter missing case
                    raise
                param_value = default  # Fallback to default value
        OmegaConf.update(config, yaml_path, param_value, merge=False)

    return cast("dict[str, Any]", OmegaConf.to_container(config, resolve=True))


def setup_logging(tz: ZoneInfo) -> None:
    """Set up logging based on logging.yaml, applying the configured timezone.

    Args:
        tz (ZoneInfo): Timezone for log timestamps.

    """
    path = "/config/logging.yaml"  # path in the container

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
                except (ImportError, AttributeError, ValueError):
                    logger.exception(
                        "Failed to create timezone-aware formatter for %s", name
                    )
    logging.config.dictConfig(log_config)
    logger.info("Logging configured successfully from %s with timezone %s.", path, tz)


class CustomMetricHandler(MetricsHandler):
    """Custom HTTP request handler with error handling for metrics endpoint."""

    def do_GET(self) -> None:  # noqa: N802
        """Override the default do_GET to add error handling for data retrieval."""
        try:
            super().do_GET()
        except ServiceUnavailableError:
            logger.exception("Returning 503 Service Unavailable.")
            self._send_error_response(503, "Service Unavailable")
        except InternalServerError:
            logger.exception("Returning 500 Internal Server Error.")
            self._send_error_response(500, "Internal Server Error")
        except Exception:
            logger.exception(
                "Unhandled exception occurred. Returning 500 Internal Server Error.",
            )
            self._send_error_response(500, "Internal Server Error")
        else:
            logger.info("Returning 200 OK.")

    def _send_error_response(self, status_code: int, message: str) -> None:
        """Send error response with given status code and message.

        Args:
            status_code (int): HTTP status code (e.g., 500, 503).
            message (str): Error message to include in response body.

        """
        try:
            self.send_response(status_code)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(f"Error (HTTP {status_code}): {message}\n".encode())
        except OSError:
            logger.exception("Failed to send error response.")


if __name__ == "__main__":
    # Load configuration and set up logging
    main_config = setup_config()
    tz_logging = get_timezone(
        main_config["exporter"]["timezone"], DEFAULT_EXPORTER_TIMEZONE
    )
    setup_logging(tz_logging)
    logger.info("Configuration loaded: %s", main_config)

    # Unregister the default collector and then register the custom collector
    registry_obj = CollectorRegistry()
    registry_obj.register(CustomCollector(main_config))

    # Start HTTP server with custom handler
    CustomMetricHandler.registry = registry_obj
    server_port: int = main_config["exporter"]["port"]
    server = HTTPServer(("0.0.0.0", server_port), CustomMetricHandler)  # noqa: S104
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Exporter started, listening on port %s.", server_port)

    # Keep the process running
    while True:
        time.sleep(1)
