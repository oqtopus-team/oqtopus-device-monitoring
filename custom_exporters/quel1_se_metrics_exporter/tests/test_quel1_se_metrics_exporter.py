import logging
import math
import os
import pathlib
import tempfile
from unittest.mock import MagicMock, Mock, patch
from zoneinfo import ZoneInfo

import pytest
import yaml

from quel1_se_metrics_exporter import (
    DEFAULT_EXPORTER_PORT,
    DEFAULT_QUEL1SE_TIMEOUT,
    CollectorResult,
    GaugeMetricFamily,
    Quel1seMetricsCollector,
    Quel1seTarget,
    collect_target_metrics,
    create_timezone_formatter,
    get_allowed_threads,
    get_cgroup_cpu_count,
    load_config,
    setup_logging,
    validate_quel1se_targets,
)


class TestQuel1seTarget:
    """Tests for Quel1seTarget dataclass."""

    def test_valid_target(self):
        """Test creating a valid target."""
        target = Quel1seTarget(
            name="test-target",
            wss_ip="10.1.0.51",
            css_ip="10.1.0.50",
            boxtype_str="quel1se-riken8",
        )
        assert target.name == "test-target"
        assert target.wss_ip == "10.1.0.51"
        assert target.css_ip == "10.1.0.50"

    def test_target_without_optional_ip(self):
        """Test creating a target without optional ip field."""
        target = Quel1seTarget(
            name="test-target", wss_ip="10.1.0.51", boxtype_str="quel1se-riken8"
        )
        assert target.name == "test-target"
        assert target.wss_ip == "10.1.0.51"
        assert target.css_ip is None

    def test_target_strips_whitespace(self):
        """Test that target strips whitespace from fields."""
        target = Quel1seTarget(
            name="  test-target  ",
            wss_ip="  10.1.0.51  ",
            css_ip="  10.1.0.50  ",
            boxtype_str="quel1se-riken8",
        )
        assert target.name == "test-target"
        assert target.wss_ip == "10.1.0.51"
        assert target.css_ip == "10.1.0.50"

    def test_target_empty_name_raises(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="Target name cannot be empty"):
            Quel1seTarget(name="", wss_ip="10.1.0.51", boxtype_str="quel1se-riken8")

    def test_target_empty_wss_ip_raises(self):
        """Test that empty wss_ip raises ValueError."""
        with pytest.raises(ValueError, match="Target wss_ip cannot be empty"):
            Quel1seTarget(name="test-target", wss_ip="", boxtype_str="quel1se-riken8")

    def test_target_whitespace_only_name_raises(self):
        """Test that whitespace-only name raises ValueError."""
        with pytest.raises(ValueError, match="Target name cannot be empty"):
            Quel1seTarget(name="   ", wss_ip="10.1.0.51", boxtype_str="quel1se-riken8")

    def test_target_empty_wss_boxtype(self):
        """Test that whitespace-only name raises ValueError."""
        with pytest.raises(ValueError, match="Target boxtype cannot be empty"):
            Quel1seTarget(name="there_is_name", wss_ip="10.1.0.51", boxtype_str="")


class TestValidateQuel1seTargets:
    """Tests for validate_quel1se_targets function."""

    def test_valid_targets(self):
        """Test validation of valid targets."""
        raw_targets = [
            {
                "name": "target1",
                "wss_ip": "10.1.0.51",
                "css_ip": "10.1.0.50",
                "boxtype": "quel1se-riken8",
            },
            {"name": "target2", "wss_ip": "10.1.0.53", "boxtype": "quel1se-riken8"},
        ]
        targets = validate_quel1se_targets(raw_targets)
        assert len(targets) == 2
        assert targets[0].name == "target1"
        assert targets[1].name == "target2"

    def test_non_list_input(self):
        """Test that non-list input returns empty list."""
        targets = validate_quel1se_targets("not a list")
        assert targets == []

    def test_non_dict_target(self):
        """Test that non-dict targets are skipped."""
        raw_targets = [
            {"name": "target1", "wss_ip": "10.1.0.51", "boxtype": "quel1se-riken8"},
            "not a dict",
            {"name": "target2", "wss_ip": "10.1.0.53", "boxtype": "quel1se-riken8"},
        ]
        targets = validate_quel1se_targets(raw_targets)
        assert len(targets) == 2

    def test_missing_required_fields(self):
        """Test that targets with missing required fields are skipped."""
        raw_targets = [
            {"name": "target1", "boxtype": "quel1se-riken8"},  # missing wss_ip
            {"wss_ip": "10.1.0.51", "boxtype": "quel1se-riken8"},  # missing name
            {"wss_ip": "10.1.0.51"},  # missing boxtype
            {
                "name": "target3",
                "wss_ip": "10.1.0.53",
                "boxtype": "quel1se-riken8",
            },  # valid
        ]
        targets = validate_quel1se_targets(raw_targets)
        assert len(targets) == 1
        assert targets[0].name == "target3"

    def test_empty_name_skipped(self):
        """Test that targets with empty name are skipped."""
        raw_targets = [
            {"name": "", "wss_ip": "10.1.0.51", "boxtype": "quel1se-riken8"},
            {"name": "  ", "wss_ip": "10.1.0.52", "boxtype": "quel1se-riken8"},
            {"name": "valid", "wss_ip": "10.1.0.53", "boxtype": "quel1se-riken8"},
        ]
        targets = validate_quel1se_targets(raw_targets)
        assert len(targets) == 1

    def test_none_values(self):
        """Test that targets with None values are skipped."""
        raw_targets = [
            {"name": None, "wss_ip": "10.1.0.51", "boxtype": "quel1se-riken8"},
            {"name": "valid", "wss_ip": None, "boxtype": "quel1se-riken8"},
            {"name": "valid2", "wss_ip": "10.1.0.53", "boxtype": "quel1se-riken8"},
        ]
        targets = validate_quel1se_targets(raw_targets)
        assert len(targets) == 1
        assert targets[0].name == "valid2"


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

    def test_create_timezone_formatter_utc(self):
        """Test formatter with UTC timezone."""
        tz = ZoneInfo("UTC")
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
        assert result == "2021-01-01 00:00:00"


class TestGetCgroupCpuCount:
    """Tests for get_cgroup_cpu_count function."""

    @patch("quel1_se_metrics_exporter.os.cpu_count")
    @patch("quel1_se_metrics_exporter.Path")
    def test_no_cgroup_files(self, mock_path: Mock, mock_cpu_count: Mock):
        """Test when cgroup files don't exist."""
        mock_cpu_count.return_value = 4
        mock_path.return_value.read_text.side_effect = FileNotFoundError

        result = get_cgroup_cpu_count()
        assert result == 4

    @patch("quel1_se_metrics_exporter.os.cpu_count")
    def test_cpu_count_none(self, mock_cpu_count: Mock):
        """Test when os.cpu_count returns None."""
        mock_cpu_count.return_value = None

        with patch("quel1_se_metrics_exporter.Path") as mock_path:
            mock_path.return_value.read_text.side_effect = FileNotFoundError
            result = get_cgroup_cpu_count()
            assert result == 1

    def test_cgroup_quota_set(self):
        """Test when cgroup CPU quota is set."""
        with (
            patch("quel1_se_metrics_exporter.Path") as mock_path,
            patch("quel1_se_metrics_exporter.os.cpu_count", return_value=8),
        ):
            mock_path.return_value.read_text.side_effect = ["200000", "100000"]
            result = get_cgroup_cpu_count()
            assert result == 2

    def test_cgroup_quota_unlimited(self):
        """Test when cgroup CPU quota is unlimited (-1)."""
        with (
            patch("quel1_se_metrics_exporter.Path") as mock_path,
            patch("quel1_se_metrics_exporter.os.cpu_count", return_value=8),
        ):
            mock_path.return_value.read_text.return_value = "-1"
            result = get_cgroup_cpu_count()
            assert result == 8


class TestCollectorResult:
    """Tests for CollectorResult dataclass."""

    def test_default_values(self):
        """Test default values for CollectorResult."""
        target = Quel1seTarget(
            name="test", wss_ip="10.1.0.51", boxtype_str="quel1se-riken8"
        )
        result = CollectorResult(target=target)

        assert result.target == target
        assert result.temperatures is None
        assert result.actuators is None
        assert result.success is False
        assert result.error is None

    def test_with_values(self):
        """Test CollectorResult with provided values."""
        target = Quel1seTarget(
            name="test", wss_ip="10.1.0.51", boxtype_str="quel1se-riken8"
        )
        result = CollectorResult(
            target=target,
            temperatures={"sensor1": 25.0},
            actuators={"fan": {"fan1": 0.5}},
            success=True,
        )

        assert result.success is True
        assert result.temperatures == {"sensor1": 25.0}
        assert result.actuators == {"fan": {"fan1": 0.5}}

    def test_with_error(self):
        """Test CollectorResult with error."""
        target = Quel1seTarget(
            name="test", wss_ip="10.1.0.51", boxtype_str="quel1se-riken8"
        )
        result = CollectorResult(
            target=target,
            success=False,
            error="Connection failed",
        )

        assert result.success is False
        assert result.error == "Connection failed"


class TestCollectTargetMetrics:
    """Tests for collect_target_metrics function."""

    @patch("quel1_se_metrics_exporter.Quel1BoxType")
    @patch("quel1_se_metrics_exporter.Quel1Box.create")
    def test_successful_collection(self, mock_create: Mock, mock_boxtype: Mock):
        """Test successful metrics collection."""
        mock_box = MagicMock()
        mock_box.css.get_tempctrl_temperature_now.return_value = {
            "adda_lmx2594_0": 35.5,
            "adda_lmx2594_1": 36.0,
            "front_panel": 25.0,
        }
        mock_box.css.get_tempctrl_actuator_output.return_value = {
            "fan": {"adda_lmx2594_0": 0.5, "adda_lmx2594_1": 0.6},
            "heater": {"mx0_adrf6780_0": 0.1, "mx0_amp_1": 0.2},
        }
        mock_create.return_value = mock_box
        mock_boxtype.fromstr.return_value = "quel1se-riken8"

        target = Quel1seTarget(
            name="test", wss_ip="10.1.0.51", boxtype_str="quel1se-riken8"
        )
        result = collect_target_metrics(target)

        assert result.success is True
        assert result.temperatures == {
            "adda_lmx2594_0": 35.5,
            "adda_lmx2594_1": 36.0,
            "front_panel": 25.0,
        }
        assert result.actuators is not None
        assert "fan" in result.actuators
        assert "heater" in result.actuators
        assert result.actuators["fan"]["adda_lmx2594_0"] == 0.5

    @patch("quel1_se_metrics_exporter.collect_target_metrics")
    @patch("quel1_se_metrics_exporter.get_allowed_threads", return_value=1)
    def test_mapping_for_example_temperature_and_actuators_full(
        self, mock_threads: Mock, mock_collect: Mock
    ):
        """Verify labels and values when example data arrives."""
        target = Quel1seTarget(
            name="riken8-A", wss_ip="10.5.0.132", boxtype_str="quel1se-riken8"
        )

        # Example data from docs (subset used for assertions; all values passed through)
        temperatures = {
            "adda_lmx2594_1": 37.28664355165273,
            "adda_lmx2594_0": 36.81112528826384,
            "front_panel": 25.97690591138968,
            "rear_panel": 25.566415779240856,
            "mx0_adrf6780_0": 48.990019797397,
            "mx0_amp_1": 39.34623306511378,
            "mx0_adrf6780_2": 49.14784871091899,
            "mx0_amp_3": 39.34623306511378,
            "mx0_lmx2594_0": 44.25125831133681,
            "mx0_lmx2594_2": 42.511760452864564,
            "mx0_hmc8193_r": 43.46071073029901,
            "mx0_hmc8193_m0": 39.979633237981375,
            "mx0_hmc8193_m1": 35.38404919358061,
            "mx0_lmx2594_4": 39.34623306511378,
            "mx1_amp_0": 33.32131747271586,
            "mx1_amp_1": 38.71268328072796,
            "mx1_amp_2": 38.23742209475921,
            "mx1_amp_3": 35.542652057202815,
            "ps0_sw_monitorout": 31.57461465436984,
            "ps0_sw_monitorin": 31.733456186134504,
            "ps0_sw0_path_d": 32.20992013481674,
            "ps0_sw1_path_d": 31.256901179957993,
            "ps0_sw2_path_d": 31.256901179957993,
            "ps0_sw0_path_c": 29.985639691240635,
            "ps0_sw1_path_c": 31.57461465436984,
            "ps0_lna_readout": 29.826685916179997,
            "ps0_lna_path_b": 31.892287600363147,
            "ps0_lna_path_c": 32.52751237549228,
            "ps0_lna_path_d": 32.36872128448215,
            "ps0_sw_monitorloop": 31.09802920831089,
            "ps0_lna_readin": 26.486258836285344,
            "ps0_sw0_readin": 25.212495657437046,
            "ps0_sw1_readin": 25.531000230179473,
            "ps0_sw2_readin": 25.8494622169261,
            "ps0_sw_readloop": 27.282016876420073,
            "ps0_sw0_path_b": 29.03176242925565,
            "ps0_sw1_path_b": 28.395637278789422,
            "ps0_sw0_readout": 27.91843413377768,
            "ps0_sw1_readout": 27.282016876420073,
            "ps0_sw2_readout": 28.87274671278533,
            "ps0_sw2_path_b": 28.87274671278533,
            "ps0_sw2_path_c": 29.03176242925565,
            "ps1_sw_monitorout": 28.713720624615178,
            "ps1_sw_monitorin": 27.122886331424752,
            "ps1_sw0_path_d": 27.91843413377768,
            "ps1_sw1_path_d": 27.600246463272413,
            "ps1_sw2_path_d": 27.91843413377768,
            "ps1_sw0_path_c": 29.19076778768931,
            "ps1_sw1_path_c": 27.600246463272413,
            "ps1_lna_readout": 30.62135218275472,
            "ps1_lna_path_b": 30.939147061127642,
            "ps1_lna_path_c": 28.395637278789422,
            "ps1_lna_path_d": 27.759345531454528,
            "ps1_sw_monitorloop": 28.713720624615178,
            "ps1_lna_readin": 30.303516430586853,
            "ps1_sw0_readin": 29.03176242925565,
            "ps1_sw1_readin": 30.46243942300191,
            "ps1_sw2_readin": 28.87274671278533,
            "ps1_sw_readloop": 29.826685916179997,
            "ps1_sw0_path_b": 29.19076778768931,
            "ps1_sw1_path_b": 29.03176242925565,
            "ps1_sw0_readout": 28.713720624615178,
            "ps1_sw1_readout": 29.667721852197985,
            "ps1_sw2_readout": 27.91843413377768,
            "ps1_sw2_path_b": 28.55468415113546,
            "ps1_sw2_path_c": 28.87274671278533,
        }

        actuators = {
            "fan": {"adda_lmx2594_0": 0.908, "adda_lmx2594_1": 0.908},
            "heater": {
                "mx0_adrf6780_0": 0.563,
                "mx0_amp_1": 0.519,
                "mx0_adrf6780_2": 0.571,
                "mx0_amp_3": 0.556,
                "mx0_lmx2594_0": 0.49,
                "mx0_lmx2594_2": 0.545,
                "ps0_lna_readin": 0.391,
                "ps0_lna_readout": 0.465,
                "ps0_lna_path_b": 0.475,
                "ps0_lna_path_c": 0.419,
                "ps0_lna_path_d": 0.5,
                "ps0_sw_monitorloop": 0.469,
                "mx0_hmc8193_r": 0.532,
                "mx0_hmc8193_m0": 0.555,
                "mx0_hmc8193_m1": 0.511,
                "mx0_lmx2594_4": 0.509,
                "mx1_amp_0": 0.449,
                "mx1_amp_1": 0.489,
                "mx1_amp_2": 0.514,
                "mx1_amp_3": 0.494,
                "ps1_lna_readin": 0.487,
                "ps1_lna_readout": 0.468,
                "ps1_lna_path_b": 0.475,
                "ps1_lna_path_c": 0.506,
                "ps1_lna_path_d": 0.411,
                "ps1_sw_monitorloop": 0.462,
            },
        }

        mock_collect.return_value = CollectorResult(
            target=target, temperatures=temperatures, actuators=actuators, success=True
        )

        config = {
            "exporter": {"port": 9101},
            "quel1se": {"timeout": 5, "targets": [target]},
        }

        collector = Quel1seMetricsCollector(config)
        metric_families = list(collector.collect())

        def assert_has_sample(
            family: GaugeMetricFamily,
            expected_labels: dict[str, str],
            expected_value: float,
        ) -> None:
            for sample in getattr(family, "samples", []):
                # sample.labels is a dict
                if sample.labels == expected_labels and math.isclose(
                    sample.value, expected_value, rel_tol=1e-12, abs_tol=1e-12
                ):
                    return
            msg = (
                f"Sample not found in {family.name}; "
                f"labels={expected_labels}; "
                f"value={expected_value}"
            )
            raise AssertionError(msg)

        families = {m.name: m for m in metric_families}
        temp_family = families["qubit_controller_temperature"]
        act_family = families["qubit_controller_actuator_usage"]

        base = {"target_name": target.name, "wss_ip": target.wss_ip}

        # Check a few representative temperature samples
        assert_has_sample(
            temp_family,
            {**base, "location": "adda_lmx2594_0", "unit": "celsius", "raw": "true"},
            36.81112528826384,
        )
        assert_has_sample(
            temp_family,
            {**base, "location": "mx0_lmx2594_2", "unit": "celsius", "raw": "true"},
            42.511760452864564,
        )
        assert_has_sample(
            temp_family,
            {
                **base,
                "location": "ps1_sw_monitorloop",
                "unit": "celsius",
                "raw": "true",
            },
            28.713720624615178,
        )

        # Check a few representative actuator samples
        assert_has_sample(
            act_family,
            {
                **base,
                "actuator_type": "fan",
                "location": "adda_lmx2594_0",
                "unit": "ratio",
                "raw": "true",
            },
            0.908,
        )
        assert_has_sample(
            act_family,
            {
                **base,
                "actuator_type": "heater",
                "location": "mx0_adrf6780_0",
                "unit": "ratio",
                "raw": "true",
            },
            0.563,
        )
        assert_has_sample(
            act_family,
            {
                **base,
                "actuator_type": "heater",
                "location": "ps1_sw_monitorloop",
                "unit": "ratio",
                "raw": "true",
            },
            0.462,
        )

        # Sanity: total sample counts match dict sizes
        assert len(temp_family.samples) == len(temperatures)
        expected_act_samples = sum(len(v) for v in actuators.values())
        assert len(act_family.samples) == expected_act_samples

    @patch("quel1_se_metrics_exporter.Quel1Box.create")
    @patch("quel1_se_metrics_exporter.Quel1BoxType")
    def test_timeout_error(self, mock_boxtype: Mock, mock_wss_class: Mock):
        """Test handling of timeout error."""
        mock_wss_class.side_effect = TimeoutError("Connection timed out")

        target = Quel1seTarget(
            name="test", wss_ip="10.1.0.51", boxtype_str="quel1se-riken8"
        )
        result = collect_target_metrics(target, timeout=5)

        assert result.success is False
        assert "Timeout" in result.error

    @patch("quel1_se_metrics_exporter.Quel1Box.create")
    @patch("quel1_se_metrics_exporter.Quel1BoxType")
    def test_connection_error(self, mock_boxtype: Mock, mock_wss_class: Mock):
        """Test handling of connection error."""
        mock_wss_class.side_effect = ConnectionError("Connection refused")

        target = Quel1seTarget(
            name="test", wss_ip="10.1.0.51", boxtype_str="quel1se-riken8"
        )
        result = collect_target_metrics(target)

        assert result.success is False
        assert "Connection error" in result.error

    @patch("quel1_se_metrics_exporter.Quel1Box.create")
    @patch("quel1_se_metrics_exporter.Quel1BoxType")
    def test_unexpected_error(self, mock_boxtype: Mock, mock_wss_class: Mock):
        """Test handling of unexpected error."""
        mock_wss_class.side_effect = RuntimeError("Something went wrong")

        target = Quel1seTarget(
            name="test", wss_ip="10.1.0.51", boxtype_str="quel1se-riken8"
        )
        result = collect_target_metrics(target)

        assert result.success is False
        assert "Unexpected error" in result.error


class TestQuel1seMetricsCollector:
    """Tests for Quel1seMetricsCollector class."""

    def test_collector_initialization(self):
        """Test collector initialization with config."""
        config = {
            "exporter": {"port": 9101, "timezone": "UTC"},
            "quel1se": {
                "timeout": 5,
                "targets": [
                    Quel1seTarget(
                        name="target1", wss_ip="10.1.0.51", boxtype_str="quel1se-riken8"
                    ),
                    Quel1seTarget(
                        name="target2", wss_ip="10.1.0.53", boxtype_str="quel1se-riken8"
                    ),
                ],
            },
        }

        collector = Quel1seMetricsCollector(config)

        assert len(collector.targets) == 2
        assert collector.timeout == 5

    def test_collector_default_timeout(self):
        """Test collector uses default timeout."""
        config = {
            "exporter": {"port": 9101},
            "quel1se": {
                "targets": [
                    Quel1seTarget(
                        name="target1", wss_ip="10.1.0.51", boxtype_str="quel1se-riken8"
                    ),
                ],
            },
        }

        collector = Quel1seMetricsCollector(config)
        assert collector.timeout == DEFAULT_QUEL1SE_TIMEOUT

    @patch("quel1_se_metrics_exporter.collect_target_metrics")
    @patch("quel1_se_metrics_exporter.get_allowed_threads", return_value=2)
    def test_collector_collect(self, mock_threads: Mock, mock_collect: Mock):
        """Test collector collect method."""
        target1 = Quel1seTarget(
            name="target1", wss_ip="10.1.0.51", boxtype_str="quel1se-riken8"
        )
        target2 = Quel1seTarget(
            name="target2", wss_ip="10.1.0.53", boxtype_str="quel1se-riken8"
        )

        mock_collect.side_effect = [
            CollectorResult(
                target=target1,
                temperatures={"sensor1": 25.0, "sensor2": 26.0},
                actuators={"fan": {"fan1": 0.5}},
                success=True,
            ),
            CollectorResult(
                target=target2,
                temperatures={"sensor1": 27.0},
                actuators={"heater": {"heater1": 0.1}},
                success=True,
            ),
        ]

        config = {
            "exporter": {"port": 9101},
            "quel1se": {
                "timeout": 5,
                "targets": [target1, target2],
            },
        }

        collector = Quel1seMetricsCollector(config)
        metrics = list(collector.collect())

        assert len(metrics) == 2  # temperature and actuator families
        # Check metric names
        metric_names = [m.name for m in metrics]
        assert "qubit_controller_temperature" in metric_names
        assert "qubit_controller_actuator_usage" in metric_names

    @patch("quel1_se_metrics_exporter.collect_target_metrics")
    @patch("quel1_se_metrics_exporter.get_allowed_threads", return_value=2)
    def test_collector_partial_failure(self, mock_threads: Mock, mock_collect: Mock):
        """Test collector handles partial failures gracefully."""
        target1 = Quel1seTarget(
            name="target1", wss_ip="10.1.0.51", boxtype_str="quel1se-riken8"
        )
        target2 = Quel1seTarget(
            name="target2", wss_ip="10.1.0.53", boxtype_str="quel1se-riken8"
        )

        mock_collect.side_effect = [
            CollectorResult(
                target=target1,
                temperatures={"sensor1": 25.0},
                actuators={"fan": {"fan1": 0.5}},
                success=True,
            ),
            CollectorResult(
                target=target2,
                success=False,
                error="Connection failed",
            ),
        ]

        config = {
            "exporter": {"port": 9101},
            "quel1se": {
                "timeout": 5,
                "targets": [target1, target2],
            },
        }

        collector = Quel1seMetricsCollector(config)
        metrics = list(collector.collect())

        # Should still return metrics
        assert len(metrics) == 2

    @patch("quel1_se_metrics_exporter.collect_target_metrics")
    @patch("quel1_se_metrics_exporter.get_allowed_threads", return_value=1)
    def test_collector_future_exception(self, mock_threads: Mock, mock_collect: Mock):
        """Test collector handles exception in future.result()."""
        target1 = Quel1seTarget(
            name="target1", wss_ip="10.1.0.51", boxtype_str="quel1se-riken8"
        )

        mock_collect.side_effect = Exception("Unexpected failure")

        config = {
            "exporter": {"port": 9101},
            "quel1se": {
                "timeout": 5,
                "targets": [target1],
            },
        }

        collector = Quel1seMetricsCollector(config)
        # Should not raise
        metrics = list(collector.collect())
        assert len(metrics) == 2


class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_valid_config(self):
        """Test loading a valid configuration file."""
        config_content = """
exporter:
    port: 9101
    timezone: "Asia/Tokyo"

quel1se:
  timeout: 5
  targets:
    - name: "target1"
      wss_ip: "10.1.0.51"
      boxtype: "quel1se-riken8"
"""
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".yaml", delete=False
        ) as f:
            f.write(config_content)
            config_path = f.name

        try:
            with patch.dict(os.environ, {"QUEL1SE_EXPORTER_CONFIG_PATH": config_path}):
                config = load_config()
                assert config is not None
                assert config["exporter"]["port"] == 9101
                assert len(config["quel1se"]["targets"]) == 1
        finally:
            pathlib.Path(config_path).unlink()

    def test_config_file_not_found(self):
        """Test when config file doesn't exist."""
        with (
            patch.dict(
                os.environ, {"QUEL1SE_EXPORTER_CONFIG_PATH": "/nonexistent/config.yaml"}
            ),
            pytest.raises(FileNotFoundError),
        ):
            load_config()

    def test_invalid_yaml_raises(self):
        """Test invalid YAML causes yaml.YAMLError to be raised (current behavior)."""
        config_content = """
invalid: yaml: content:
  - missing closing bracket [
"""
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".yaml", delete=False
        ) as f:
            f.write(config_content)
            config_path = f.name

        try:
            with (
                patch.dict(os.environ, {"QUEL1SE_EXPORTER_CONFIG_PATH": config_path}),
                pytest.raises(yaml.YAMLError),
            ):
                load_config()
        finally:
            pathlib.Path(config_path).unlink()

    def test_env_override(self):
        """Test environment variable overrides."""
        config_content = """
exporter:
  port: 9101
  timezone: "UTC"

quel1se:
  timeout: 5
  targets:
    - name: "target1"
      wss_ip: "10.1.0.51"
      boxtype: "quel1se-riken8"
"""
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".yaml", delete=False
        ) as f:
            f.write(config_content)
            config_path = f.name

        try:
            with patch.dict(
                os.environ,
                {
                    "QUEL1SE_EXPORTER_CONFIG_PATH": config_path,
                    "QUBIT_CONTROLLER_EXPORTER_PORT": "9999",
                    "EXPORTER_TIMEZONE": "America/New_York",
                    "QUEL1SE_TIMEOUT": "10",
                },
            ):
                config = load_config()
                assert config is not None
                assert config["exporter"]["port"] == 9999
                assert config["exporter"]["timezone"] == "America/New_York"
                assert config["quel1se"]["timeout"] == 10
        finally:
            pathlib.Path(config_path).unlink()

    def test_no_valid_targets(self):
        """Test when no valid targets are found."""
        config_content = """
exporter:
  port: 9101

quel1se:
  timeout: 5
  targets: []
"""
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".yaml", delete=False
        ) as f:
            f.write(config_content)
            config_path = f.name

        try:
            with patch.dict(os.environ, {"QUEL1SE_EXPORTER_CONFIG_PATH": config_path}):
                config = load_config()
                assert config is None
        finally:
            pathlib.Path(config_path).unlink()

    def test_missing_exporter_section(self):
        """Test when exporter section is missing."""
        config_content = """
exporter1:
  port: 9101
quel1se:
  timeout: 5
  targets:
  - name: "target1"
    wss_ip: "10.1.0.51"
    boxtype: "quel1se-riken8"
"""
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".yaml", delete=False
        ) as f:
            f.write(config_content)
            config_path = f.name

        try:
            with patch.dict(os.environ, {"QUEL1SE_EXPORTER_CONFIG_PATH": config_path}):
                config = load_config()
                assert config is not None
                # Should use defaults

                assert config["exporter"]["port"] == DEFAULT_EXPORTER_PORT
        finally:
            pathlib.Path(config_path).unlink()

    def test_missing_quel1se_section(self):
        """Test when quel1se section is missing."""
        config_content = """
exporter1:
  port: 9101
"""
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".yaml", delete=False
        ) as f:
            f.write(config_content)
            config_path = f.name

        try:
            with patch.dict(os.environ, {"QUEL1SE_EXPORTER_CONFIG_PATH": config_path}):
                config = load_config()
                assert config is None
        finally:
            pathlib.Path(config_path).unlink()

    def test_config_not_dict(self):
        """Test when config root is not a dictionary."""
        config_content = """
- item1
- item2
"""
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".yaml", delete=False
        ) as f:
            f.write(config_content)
            config_path = f.name

        try:
            with patch.dict(os.environ, {"QUEL1SE_EXPORTER_CONFIG_PATH": config_path}):
                config = load_config()
                assert config is None
        finally:
            pathlib.Path(config_path).unlink()


class TestSetupLogging:
    """Tests for setup_logging function."""

    @patch("quel1_se_metrics_exporter.pathlib.Path.exists")
    @patch("quel1_se_metrics_exporter.logging.config.dictConfig")
    @patch("quel1_se_metrics_exporter.yaml.safe_load")
    @patch("quel1_se_metrics_exporter.logger.warning")
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
        with patch("quel1_se_metrics_exporter.pathlib.Path.open"):
            setup_logging({"server": {"timezone": "UTC"}})

        mock_warning.assert_called_once()
        assert (
            "Could not create timezone-aware formatter" in mock_warning.call_args[0][0]
        )
        mock_dictconfig.assert_called_once_with(log_config)

    def test_setup_logging_file_not_found(self):
        """Test setup_logging when config file not found."""
        config = {"exporter": {"timezone": "UTC"}}

        with patch.dict(
            os.environ,
            {"QUEL1SE_EXPORTER_LOGGING_CONFIG_PATH": "/nonexistent/logging.yaml"},
        ):
            # Should not raise, just use basic config
            setup_logging(config)

    def test_setup_logging_valid_config(self):
        """Test setup_logging with valid config file."""
        logging_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "simple": {"format": "%(levelname)s - %(message)s"},
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "DEBUG",
                    "formatter": "simple",
                    "stream": "ext://sys.stdout",
                },
            },
            "root": {"level": "INFO", "handlers": ["console"]},
        }

        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(logging_config, f)
            logging_path = f.name

        try:
            config = {"exporter": {"timezone": "Asia/Tokyo"}}
            with patch.dict(
                os.environ, {"QUEL1SE_EXPORTER_LOGGING_CONFIG_PATH": logging_path}
            ):
                setup_logging(config)
        finally:
            pathlib.Path(logging_path).unlink()

    def test_setup_logging_invalid_timezone(self):
        """Test setup_logging with invalid timezone falls back to UTC."""
        logging_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "simple": {"format": "%(levelname)s - %(message)s"},
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "DEBUG",
                    "formatter": "simple",
                    "stream": "ext://sys.stdout",
                },
            },
            "root": {"level": "INFO", "handlers": ["console"]},
        }

        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(logging_config, f)
            logging_path = f.name

        try:
            config = {"exporter": {"timezone": "Invalid/Timezone"}}
            with patch.dict(
                os.environ, {"QUEL1SE_EXPORTER_LOGGING_CONFIG_PATH": logging_path}
            ):
                # Should not raise, should use UTC
                setup_logging(config)
        finally:
            pathlib.Path(logging_path).unlink()

    def test_setup_logging_with_custom_formatter_class(self):
        """Test setup_logging with custom formatter class."""
        logging_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "json": {
                    "class": "logging.Formatter",
                    "format": "%(message)s",
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "DEBUG",
                    "formatter": "json",
                    "stream": "ext://sys.stdout",
                },
            },
            "root": {"level": "INFO", "handlers": ["console"]},
        }

        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(logging_config, f)
            logging_path = f.name

        try:
            config = {"exporter": {"timezone": "UTC"}}
            with patch.dict(
                os.environ, {"QUEL1SE_EXPORTER_LOGGING_CONFIG_PATH": logging_path}
            ):
                setup_logging(config)
        finally:
            pathlib.Path(logging_path).unlink()


class TestGetAllowedThreads:
    """Tests for get_allowed_threads function."""

    @patch("quel1_se_metrics_exporter.get_cgroup_cpu_count", return_value=4)
    @patch("quel1_se_metrics_exporter.MAX_WORKERS_ENV", "2")
    def test_max_workers_set(self, mock_cpu_count: Mock):
        """Test when MAX_WORKERS is set to valid value."""
        result = get_allowed_threads()
        assert result == 2

    @patch("quel1_se_metrics_exporter.get_cgroup_cpu_count", return_value=4)
    @patch("quel1_se_metrics_exporter.MAX_WORKERS_ENV", "0")
    def test_max_workers_zero(self, mock_cpu_count: Mock):
        """Test when MAX_WORKERS is set to 0."""
        result = get_allowed_threads()
        assert result == 4

    @patch("quel1_se_metrics_exporter.get_cgroup_cpu_count", return_value=4)
    @patch("quel1_se_metrics_exporter.MAX_WORKERS_ENV", "invalid")
    def test_max_workers_invalid(self, mock_cpu_count: Mock):
        """Test when MAX_WORKERS is not a valid integer."""
        result = get_allowed_threads()
        assert result == 4

    @patch("quel1_se_metrics_exporter.get_cgroup_cpu_count", return_value=4)
    @patch("quel1_se_metrics_exporter.MAX_WORKERS_ENV", "10")
    def test_max_workers_exceeds_limit(self, mock_cpu_count: Mock):
        """Test when MAX_WORKERS exceeds available CPUs."""
        result = get_allowed_threads()
        assert result == 4
