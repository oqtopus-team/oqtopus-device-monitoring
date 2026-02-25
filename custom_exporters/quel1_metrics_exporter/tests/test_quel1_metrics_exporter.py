import logging
import subprocess
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest
from quel1_metrics_exporter import (
    CustomCollector,
    create_timezone_formatter,
    get_allowed_threads,
    get_cgroup_cpu_count,
    load_config,
    ping_target,
    setup_logging,
    validate_targets,
)


class TestCreateTimezoneFormatter:
    """Tests for create_timezone_formatter function."""

    def test_create_timezone_formatter_with_datefmt(self):
        """Test formatter with custom date format."""
        tz = ZoneInfo("Asia/Tokyo")
        formatter_class = create_timezone_formatter(logging.Formatter, tz)
        formatter = formatter_class()

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        record.created = 1609459200.0  # 2021-01-01 00:00:00 UTC

        result = formatter.formatTime(record, "%Y-%m-%d %H:%M:%S")
        assert result == "2021-01-01 09:00:00"

    def test_create_timezone_formatter_without_datefmt(self):
        """Test formatter with ISO format."""
        tz = ZoneInfo("Asia/Tokyo")
        formatter_class = create_timezone_formatter(logging.Formatter, tz)
        formatter = formatter_class()

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        record.created = 1609459200.0

        result = formatter.formatTime(record, None)
        assert result.startswith("2021-01-01T09:00:00")


class TestPingTarget:
    """Tests for ping_target function."""

    @patch("quel1_metrics_exporter.shutil.which")
    @patch("quel1_metrics_exporter.subprocess.check_call")
    def test_ping_target_success(self, mock_check_call: Mock, mock_which: Mock):
        """Test successful ping."""
        mock_which.return_value = "/bin/ping"
        mock_check_call.return_value = 0

        target = {
            "name": "test-host",
            "ip": "192.168.1.1",
            "controller_type": "test-type",
        }
        result = ping_target(target, count=3, timeout=5)

        assert result == 0
        mock_check_call.assert_called_once()

    @patch("quel1_metrics_exporter.shutil.which")
    def test_ping_target_no_ping_command(self, mock_which: Mock):
        """Test when ping command is not found."""
        mock_which.return_value = None

        target = {
            "name": "test-host",
            "ip": "192.168.1.1",
            "controller_type": "test-type",
        }
        result = ping_target(target)

        assert result == 1

    @patch("quel1_metrics_exporter.shutil.which")
    def test_ping_target_invalid_ip(self, mock_which: Mock):
        """Test with invalid IP address."""
        mock_which.return_value = "/bin/ping"

        target = {
            "name": "test-host",
            "ip": "invalid-ip",
            "controller_type": "test-type",
        }
        result = ping_target(target)

        assert result == 1

    @patch("quel1_metrics_exporter.shutil.which")
    @patch("quel1_metrics_exporter.subprocess.check_call")
    def test_ping_target_unreachable(self, mock_check_call: Mock, mock_which: Mock):
        """Test unreachable target."""
        mock_which.return_value = "/bin/ping"
        mock_check_call.side_effect = subprocess.CalledProcessError(1, "ping")

        target = {
            "name": "test-host",
            "ip": "192.168.1.1",
            "controller_type": "test-type",
        }
        result = ping_target(target)

        assert result == 1

    @patch("quel1_metrics_exporter.shutil.which")
    @patch("quel1_metrics_exporter.subprocess.check_call")
    def test_ping_target_exception(self, mock_check_call: Mock, mock_which: Mock):
        """Test unexpected exception during ping."""
        mock_which.return_value = "/bin/ping"
        mock_check_call.side_effect = Exception("Unexpected error")

        target = {
            "name": "test-host",
            "ip": "192.168.1.1",
            "controller_type": "test-type",
        }
        result = ping_target(target)

        assert result == 1


class TestGetCPUCount:
    """Tests for get_cgroup_cpu_count function."""

    def test_get_cgroup_cpu_count_quota_set_positive_cpu_num_obtainable_1st(self):
        with (
            patch("pathlib.Path.read_text", side_effect=["200000", "100000"]),
            patch("os.cpu_count", return_value=4),
        ):
            assert get_cgroup_cpu_count() == 2

    def test_get_cgroup_cpu_count_quota_set_positive_cpu_num_obtainable_2nd(self):
        with (
            patch("pathlib.Path.read_text", side_effect=["400000", "100000"]),
            patch("os.cpu_count", return_value=4),
        ):
            assert get_cgroup_cpu_count() == 4

    def test_get_cgroup_cpu_count_quota_set_positive_cpu_num_obtainable_host_os(self):
        with (
            patch("pathlib.Path.read_text", side_effect=["500000", "100000"]),
            patch("os.cpu_count", return_value=4),
        ):
            assert get_cgroup_cpu_count() == 4

    def test_get_cgroup_cpu_count_quota_unlimited_positive_no_cpu_limit(self):
        with (
            patch("pathlib.Path.read_text", return_value="-1"),
            patch("os.cpu_count", return_value=7),
        ):
            assert get_cgroup_cpu_count() == 7

    @patch("quel1_metrics_exporter.MAX_WORKERS", 4)
    def test_get_allowed_threads_positive_set_max_threads(self):
        with patch("quel1_metrics_exporter.get_cgroup_cpu_count", return_value=5):
            assert get_allowed_threads() == 4

    @patch("quel1_metrics_exporter.MAX_WORKERS", "error")
    def test_get_allowed_threads_invalid_env(self):
        with patch("quel1_metrics_exporter.get_cgroup_cpu_count", return_value=8):
            assert get_allowed_threads() == 8

    def test_get_allowed_threads_positive_max_threads_unset(self):
        with patch("quel1_metrics_exporter.get_cgroup_cpu_count", return_value=7):
            assert get_allowed_threads() == 7


class TestCustomCollector:
    """Tests for CustomCollector class."""

    def test_init(self):
        """Test collector initialization."""
        config = {
            "ping": {
                "targets": [
                    {
                        "name": "host1",
                        "ip": "192.168.1.1",
                        "controller_type": "test-type",
                    },
                    {
                        "name": "host2",
                        "ip": "192.168.1.2",
                        "controller_type": "test-type",
                    },
                ]
            }
        }
        collector = CustomCollector(config)

        assert collector._config == config  # noqa: SLF001
        assert collector.targets == config["ping"]["targets"]
        assert collector.timeout == 5

    @patch("quel1_metrics_exporter.ping_target")
    def test_collect(self, mock_ping_target: Mock):
        """Test metrics collection."""
        mock_ping_target.return_value = 0

        config = {
            "ping": {
                "targets": [
                    {
                        "name": "host1",
                        "ip": "192.168.1.1",
                        "controller_type": "test-type",
                    },
                ],
                "timeout": 5,
            }
        }
        collector = CustomCollector(config)

        metrics = list(collector.collect())

        assert len(metrics) == 1
        assert metrics[0].name == "qubit_controller_ping_status_code"
        mock_ping_target.assert_called_once()

    @patch("quel1_metrics_exporter.ping_target", side_effect=Exception("boom"))
    @patch("quel1_metrics_exporter.logger.exception")
    def test_collect_future_exception(
        self, mock_logger_exception: Mock, _mock_ping: Mock
    ):
        """Ensure exception in future.result() is handled and status_code=1."""
        config = {
            "ping": {
                "targets": [
                    {
                        "name": "host1",
                        "ip": "192.168.1.1",
                        "controller_type": "test-type",
                    },
                ],
                "timeout": 5,
                "count": 1,
            }
        }
        collector = CustomCollector(config)
        metrics = list(collector.collect())

        assert len(metrics) == 1
        m = metrics[0]
        assert m.name == "qubit_controller_ping_status_code"
        assert len(m.samples) == 1
        s = m.samples[0]
        assert s.value == 1
        assert s.labels["target_host"] == "host1"
        assert s.labels["target_ip"] == "192.168.1.1"
        assert s.labels["controller_type"] == "test-type"

        mock_logger_exception.assert_called_once()
        # message format and target name are passed as args
        assert (
            "Ping task raised unexpectedly for %s"
            in mock_logger_exception.call_args[0][0]
        )
        assert mock_logger_exception.call_args[0][1] == "host1"


class TestSetupLogging:
    """Tests for setup_logging function."""

    @patch("quel1_metrics_exporter.pathlib.Path.exists")
    @patch("quel1_metrics_exporter.pathlib.Path.open")
    @patch("quel1_metrics_exporter.logging.config.dictConfig")
    @patch("quel1_metrics_exporter.yaml.safe_load")
    def test_setup_logging_with_yaml(
        self,
        mock_safe_load: Mock,
        mock_dictconfig: Mock,
        _mock_open: Mock,
        mock_exists: Mock,
    ):
        """Test logging setup with yaml file."""
        mock_exists.return_value = True
        mock_safe_load.return_value = {
            "version": 1,
            "formatters": {
                "json": {
                    "class": "logging.Formatter",
                    "format": "%(message)s",
                }
            },
        }

        config = {"exporter": {"timezone": "Asia/Tokyo"}}
        setup_logging(config)

        mock_dictconfig.assert_called_once()
        mock_safe_load.assert_called_once()

    @patch("quel1_metrics_exporter.pathlib.Path.exists")
    @patch("quel1_metrics_exporter.logging.getLogger")
    def test_setup_logging_without_yaml(self, mock_get_logger: Mock, mock_exists: Mock):
        """Test logging setup without yaml file."""
        mock_exists.return_value = False
        mock_root_logger = Mock()
        mock_get_logger.return_value = mock_root_logger

        config = {"exporter": {"timezone": "Asia/Tokyo"}}
        with pytest.raises(Exception):
            setup_logging(config)

    @patch("quel1_metrics_exporter.pathlib.Path.exists")
    @patch("quel1_metrics_exporter.pathlib.Path.open")
    @patch("quel1_metrics_exporter.logging.config.dictConfig")
    @patch("quel1_metrics_exporter.yaml.safe_load")
    @patch("quel1_metrics_exporter.logger.warning")
    def test_setup_logging_invalid_timezone(
        self,
        mock_warning: Mock,
        mock_safe_load: Mock,
        mock_dictconfig: Mock,
        _mock_open: Mock,
        mock_exists: Mock,
    ):
        """Test logging setup with invalid timezone falls back to UTC."""
        mock_exists.return_value = True
        mock_safe_load.return_value = {
            "version": 1,
            "formatters": {
                "standard": {
                    "class": "logging.Formatter",
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                }
            },
        }
        config = {"exporter": {"timezone": "Invalid/Timezone"}}
        setup_logging(config)
        # Verify dictConfig was called (logging was set up)
        mock_dictconfig.assert_called_once()
        # Verify the formatter was not modified (since timezone is invalid)
        # or that setup_logging handled the error gracefully
        mock_warning.assert_called()
        warning_call = mock_warning.call_args
        assert "Invalid or missing timezone in config" in warning_call[0][0]
        assert warning_call[0][1] == "UTC"  # DEFAULT_TIMEZONE


class TestSetupLoggingFormatterErrors:
    """Tests for formatter class import failures in setup_logging."""

    @patch("quel1_metrics_exporter.pathlib.Path.exists")
    @patch("quel1_metrics_exporter.logging.config.dictConfig")
    @patch("quel1_metrics_exporter.yaml.safe_load")
    @patch("quel1_metrics_exporter.logger.warning")
    def test_formatter_importerror(
        self,
        mock_warning: Mock,
        mock_safe_load: Mock,
        mock_dictconfig: Mock,
        mock_exists: Mock,
    ):
        """ImportError: module not found."""
        mock_exists.return_value = True
        log_config = {
            "version": 1,
            "formatters": {
                "bad_import": {"class": "non.existent.Module"},
            },
        }
        mock_safe_load.return_value = log_config
        with patch("quel1_metrics_exporter.pathlib.Path.open"):
            setup_logging({"exporter": {"timezone": "UTC"}})

        mock_warning.assert_called_once()
        assert (
            "Could not create timezone-aware formatter" in mock_warning.call_args[0][0]
        )
        mock_dictconfig.assert_called_once_with(log_config)

    @patch("quel1_metrics_exporter.pathlib.Path.exists")
    @patch("quel1_metrics_exporter.logging.config.dictConfig")
    @patch("quel1_metrics_exporter.yaml.safe_load")
    @patch("quel1_metrics_exporter.logger.warning")
    def test_formatter_attributeerror(
        self,
        mock_warning: Mock,
        mock_safe_load: Mock,
        mock_dictconfig: Mock,
        mock_exists: Mock,
    ):
        """AttributeError: class not found in existing module."""
        mock_exists.return_value = True
        log_config = {
            "version": 1,
            "formatters": {
                "missing_class": {"class": "logging.NoSuchClass"},
            },
        }
        mock_safe_load.return_value = log_config

        with patch("quel1_metrics_exporter.pathlib.Path.open"):
            setup_logging({"exporter": {"timezone": "UTC"}})

        mock_warning.assert_called_once()
        assert (
            "Could not create timezone-aware formatter" in mock_warning.call_args[0][0]
        )
        mock_dictconfig.assert_called_once_with(log_config)

    @patch("quel1_metrics_exporter.pathlib.Path.exists")
    @patch("quel1_metrics_exporter.logging.config.dictConfig")
    @patch("quel1_metrics_exporter.yaml.safe_load")
    @patch("quel1_metrics_exporter.logger.warning")
    def test_formatter_valueerror(
        self,
        mock_warning: Mock,
        mock_safe_load: Mock,
        mock_dictconfig: Mock,
        mock_exists: Mock,
    ):
        """ValueError: class path without dot causes rsplit failure."""
        mock_exists.return_value = True
        log_config = {
            "version": 1,
            "formatters": {
                "nodot": {"class": "BadClassPathWithoutDot"},
            },
        }
        mock_safe_load.return_value = log_config

        with patch("quel1_metrics_exporter.pathlib.Path.open"):
            setup_logging({"exporter": {"timezone": "UTC"}})

        mock_warning.assert_called_once()
        assert (
            "Could not create timezone-aware formatter" in mock_warning.call_args[0][0]
        )
        mock_dictconfig.assert_called_once_with(log_config)


class TestLoadConfig:
    """Tests for load_config function."""

    @patch.dict("os.environ", {"QUEL1_EXPORTER_CONFIG_PATH": "/test/config.yaml"})
    @patch("quel1_metrics_exporter.pathlib.Path.open")
    @patch("quel1_metrics_exporter.yaml.safe_load")
    def test_load_config_success(self, mock_safe_load: Mock, _mock_open: Mock):
        """Test successful config loading."""
        config_data = {
            "exporter": {"port": 9101, "timezone": "Asia/Tokyo"},
            "ping": {
                "timeout": 5,
                "targets": [
                    {
                        "name": "qube001",
                        "ip": "172.20.32.211",
                        "controller_type": "quel1",
                    }
                ],
            },
        }
        mock_safe_load.return_value = config_data

        result = load_config()

        assert result is not None
        assert result["exporter"]["port"] == 9101
        mock_safe_load.assert_called_once()

    @patch.dict("os.environ", {})
    @patch("quel1_metrics_exporter.pathlib.Path.open")
    @patch("quel1_metrics_exporter.yaml.safe_load")
    def test_load_config_minimal_config_uses_defaults(
        self, mock_safe_load: Mock, _mock_open: Mock
    ):
        """Test config loading with minimal configuration uses all defaults."""
        # Minimal config with only required targets
        config_data = {
            "exporter": {},
            "ping": {
                "targets": [
                    {
                        "name": "qube001",
                        "ip": "172.20.32.211",
                        "controller_type": "quel1",
                    }
                ],
            },
        }
        mock_safe_load.return_value = config_data

        result = load_config()

        assert result is not None
        # Verify defaults are applied
        assert result["exporter"]["port"] == 9102
        assert result["exporter"]["timezone"] == "UTC"
        assert result["ping"]["timeout"] == 5
        assert result["ping"]["count"] == 3

    @patch.dict("os.environ", {"QUEL1_EXPORTER_CONFIG_PATH": "/test/config.yaml"})
    @patch("quel1_metrics_exporter.pathlib.Path.open")
    @patch("quel1_metrics_exporter.yaml.safe_load")
    def test_load_config_invalid_format(self, mock_safe_load: Mock, _mock_open: Mock):
        """Test config loading with invalid format."""
        mock_safe_load.return_value = "not a dict"

        result = load_config()

        assert result is None

    @patch.dict(
        "os.environ",
        {
            "QUEL1_EXPORTER_CONFIG_PATH": "/test/config.yaml",
            "EXPORTER_PORT": "9999",
            "PING_TIMEOUT": "10",
            "SERVER_TIMEZONE": "UTC",
        },
    )
    @patch("quel1_metrics_exporter.pathlib.Path.open")
    @patch("quel1_metrics_exporter.yaml.safe_load")
    def test_load_config_with_env_overrides(
        self,
        mock_safe_load: Mock,
        _mock_open: Mock,
    ):
        """Test config loading with environment variable overrides."""
        config_data = {
            "exporter": {"port": 9101, "timezone": "Asia/Tokyo"},
            "ping": {
                "timeout": 2,
                "count": 2,
                "targets": [
                    {
                        "name": "qube001",
                        "ip": "172.20.32.211",
                        "controller_type": "quel1",
                    }
                ],
            },
        }
        mock_safe_load.return_value = config_data

        result = load_config()

        assert result["exporter"]["port"] == 9999
        assert result["ping"]["timeout"] == 10
        assert result["ping"]["count"] == 2
        assert result["exporter"]["timezone"] == "UTC"
        mock_safe_load.assert_called_once()

    @patch.dict(
        "os.environ",
        {
            "QUEL1_EXPORTER_CONFIG_PATH": "/test/config.yaml",
            "EXPORTER_PORT": "9999",
            "PING_TIMEOUT": "10",
            "SERVER_TIMEZONE": "UTC",
        },
    )
    @patch("quel1_metrics_exporter.pathlib.Path.open")
    @patch("quel1_metrics_exporter.yaml.safe_load")
    def test_load_config_no_target(
        self,
        mock_safe_load: Mock,
        _mock_open: Mock,
    ):
        """Test config loading with environment variable overrides."""
        config_data = {
            "exporter": {"port": 9101, "timezone": "Asia/Tokyo"},
            "ping": {
                "timeout": 5,
                "targets": [
                    {
                        "name": "qube001",
                        "controller_type": "quel1",
                    },
                    {
                        "ip": "172.20.32.211",
                        "controller_type": "quel1",
                    },
                    {
                        "name": "qube003",
                        "ip": "172.20.32.211",
                    },
                ],
            },
        }
        mock_safe_load.return_value = config_data

        result = load_config()
        assert result is None


class TestValidateTargets:
    """Tests for validate_targets function."""

    def test_validate_targets_success(self):
        """Test successful target validation."""
        raw_targets = [
            {"name": "host1", "ip": "192.168.1.1", "controller_type": "type1"},
            {"name": "host2", "ip": "192.168.1.2", "controller_type": "type2"},
        ]
        result = validate_targets(raw_targets)
        assert len(result) == 2
        assert result[0]["name"] == "host1"
        assert result[1]["ip"] == "192.168.1.2"

    def test_validate_targets_missing_fields(self):
        """Test targets with missing required fields."""
        raw_targets = [
            {"name": "host1", "ip": "192.168.1.1"},  # Missing controller_type
            {"ip": "192.168.1.2", "controller_type": "type2"},  # Missing name
            {"name": "host3", "controller_type": "type3"},  # Missing ip
        ]
        result = validate_targets(raw_targets)
        assert len(result) == 0

    def test_validate_targets_invalid_types(self):
        """Test targets with invalid field types."""
        raw_targets = [
            {"name": "host1", "ip": 12345, "controller_type": "type1"},  # ip not str
            {
                "name": None,
                "ip": "192.168.1.2",
                "controller_type": "type2",
            },  # name None
            {
                "name": 12,
                "ip": "192.168.1.2",
                "controller_type": "type2",
            },  # name not str
            {
                "name": "host3",
                "ip": "192.168.1.3",
                "controller_type": 123,
            },  # controller_type not str
        ]
        result = validate_targets(raw_targets)
        assert len(result) == 0

    def test_validate_targets_not_list(self):
        """Test targets with invalid field instance."""
        raw_targets = "fake_string_instead_of_list"  # Not a list
        result = validate_targets(raw_targets)
        assert len(result) == 0

    def test_validate_targets_not_dict(self):
        """Test targets with invalid field instance."""
        raw_targets = [
            "not_a_dict",
        ]
        result = validate_targets(raw_targets)
        assert len(result) == 0
