from zoneinfo import ZoneInfo

import pytest
from pytest_mock import MockerFixture

from cryo_metrics_exporter import CustomCollector


class TestCustomCollectorInit:
    """Test suite for CustomCollector initialization."""

    def test_custom_collector_init_sets_all_attributes_correctly(
        self, sample_config: dict
    ):
        # Arrange
        config = sample_config

        # Act
        collector = CustomCollector(config)

        # Assert
        assert collector._config == config
        assert collector._scrape_interval == 60
        assert collector._max_expand_windows_http == 5
        assert collector._max_expand_windows_smb == 5
        assert collector._http_url == "http://localhost"
        assert collector._http_port == 80
        assert collector._http_timeout == 5
        assert collector._smb_server == "localhost"
        assert collector._smb_share == "share_name"
        assert collector._smb_port == 445
        assert collector._smb_user == "testuser"
        assert collector._smb_timeout == 5
        assert collector._smb_base_path == ""
        assert collector.empty_count_http == 0
        assert collector.empty_count_smb == 0

    def test_custom_collector_init_reads_smb_password_from_environment(
        self, sample_config: dict, mocker: MockerFixture
    ):
        # Arrange
        config = sample_config
        mocker.patch.dict("os.environ", {"SMB_PASSWORD": "test_password"})

        # Act
        collector = CustomCollector(config)

        # Assert
        assert collector._smb_password == "test_password"  # noqa: S105

    def test_custom_collector_init_fails_when_smb_password_missing(
        self, sample_config: dict, monkeypatch: pytest.MonkeyPatch
    ):
        # Arrange
        config = sample_config
        monkeypatch.delenv("SMB_PASSWORD", raising=False)

        # Act & Assert
        with pytest.raises(KeyError):
            CustomCollector(config)

    def test_custom_collector_sets_timezones_correctly(
        self, sample_config: dict, mocker: MockerFixture
    ):
        # Arrange
        config = sample_config

        mock_tz_exporter = ZoneInfo("Asia/Tokyo")
        mock_tz_http = ZoneInfo("America/New_York")
        mock_tz_smb = ZoneInfo("Europe/London")
        side_effects = [mock_tz_exporter, mock_tz_http, mock_tz_smb]
        mocker.patch("cryo_metrics_exporter.get_timezone", side_effect=side_effects)

        # Act
        collector = CustomCollector(config)

        # Assert
        assert collector._tz_exporter == mock_tz_exporter
        assert collector._tz_http == mock_tz_http
        assert collector._tz_smb == mock_tz_smb

    def test_custom_collector_sets_temp_channels(self, sample_config: dict):
        # Arrange
        config = sample_config
        expected_device_name = config["exporter"]["device_name"]
        expected_channels = ["1", "2", "5", "6"]

        # Act
        collector = CustomCollector(config)

        # Assert
        assert collector.http_targets == expected_channels
        assert list(collector.temp_channels.keys()) == expected_channels
        for channel_id in collector.temp_channels:
            assert (
                collector.temp_channels[channel_id]["device_name"]
                == expected_device_name
            )

    def test_custom_collector_sets_smb_pressure_channels(self, sample_config: dict):
        # Arrange
        config = sample_config
        expected_device_name = config["exporter"]["device_name"]
        expected_channels = ["CH1", "CH2", "CH3", "CH4", "CH5", "CH6"]

        # Act
        collector = CustomCollector(config)

        # Assert
        assert list(collector.smb_pressure_channels.keys()) == expected_channels
        for channel_id in collector.temp_channels:
            assert (
                collector.temp_channels[channel_id]["device_name"]
                == expected_device_name
            )

    def test_custom_collector_sets_smb_gasflow_channels(self, sample_config: dict):
        # Arrange
        config = sample_config
        expected_device_name = config["exporter"]["device_name"]
        expected_channels = ["channel"]

        # Act
        collector = CustomCollector(config)

        # Assert
        assert list(collector.smb_gasflow_channels.keys()) == expected_channels
        assert (
            collector.smb_gasflow_channels["channel"]["device_name"]
            == expected_device_name
        )

    def test_custom_collector_sets_smb_stat_channels(self, sample_config: dict):
        # Arrange
        config = sample_config
        expected_device_name = config["exporter"]["device_name"]
        expected_channels = ["scroll1", "scroll2", "turbo1", "turbo2", "pulsetube"]

        # Act
        collector = CustomCollector(config)

        # Assert
        assert list(collector.smb_stat_channels.keys()) == expected_channels
        for channel_id in collector.smb_stat_channels:
            assert (
                collector.smb_stat_channels[channel_id]["device_name"]
                == expected_device_name
            )

    def test_custom_collector_sets_smb_comp_channels(self, sample_config: dict):
        # Arrange
        config = sample_config
        expected_device_name = config["exporter"]["device_name"]
        expected_channels = ["tc400actualspd", "tc400actualspd_2", "tc400actualspd_3"]

        # Act
        collector = CustomCollector(config)

        # Assert
        assert list(collector.smb_comp_channels.keys()) == expected_channels
        for channel_id in collector.smb_comp_channels:
            assert (
                collector.smb_comp_channels[channel_id]["device_name"]
                == expected_device_name
            )

    def test_custom_collector_sets_smb_comp_press_channels(self, sample_config: dict):
        # Arrange
        config = sample_config
        expected_device_name = config["exporter"]["device_name"]
        expected_channels = ["cpalp", "cpalp_2"]

        # Act
        collector = CustomCollector(config)

        # Assert
        assert list(collector.smb_comp_press_channels.keys()) == expected_channels
        for channel_id in collector.smb_comp_press_channels:
            assert (
                collector.smb_comp_press_channels[channel_id]["device_name"]
                == expected_device_name
            )
