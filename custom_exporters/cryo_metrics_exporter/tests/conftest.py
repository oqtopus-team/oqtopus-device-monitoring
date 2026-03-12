from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from omegaconf import OmegaConf
from pytest_mock import MockerFixture


@pytest.fixture(autouse=True)
def mock_smb_password_env_vars(mocker: MockerFixture):
    mocker.patch.dict("os.environ", {"SMB_PASSWORD": "test_password"})


@pytest.fixture
def sample_config():
    cfg = OmegaConf.create({
        "exporter": {
            "port": 9101,
            "timezone": "UTC",
            "device_name": "test-device",
        },
        "retrieval": {
            "scrape_interval_sec": 60,
            "max_expand_windows": {
                "http": 5,
                "smb": 5,
            },
        },
        "sources": {
            "http": {
                "url": "http://localhost",
                "port": 80,
                "timeout_sec": 5,
                "datasource_timezone": "UTC",
            },
            "smb": {
                "server": "localhost",
                "share": "share_name",
                "port": 445,
                "username": "testuser",
                "timeout_sec": 5,
                "base_path": "",
                "datasource_timezone": "UTC",
            },
        },
    })
    return OmegaConf.to_container(cfg, resolve=True)


@pytest.fixture
def sample_datetime_utc():
    tz = ZoneInfo("UTC")
    return datetime(2026, 1, 9, 12, 0, 0, tzinfo=tz)


@pytest.fixture
def pressure_line_valid():
    return (
        "09-01-26,12:00:00,"
        "CH1,,0,2.00e-02,4,1,"
        "CH2,,1,4.89e-01,0,1,"
        "CH3,,1,2.18e+01,0,1,"
        "CH4,,1,1.37e+02,0,1,"
        "CH5,,1,6.82e+02,0,1,"
        "CH6,,1,1.01e+03,0,1"
    )


@pytest.fixture
def gasflow_line_valid():
    return "09-01-26,12:00:00,50.5"


@pytest.fixture
def status_line_valid():
    return (
        "09-01-26,12:00:00,data,"
        "scroll1,0,scroll2,1,"
        "turbo1,0,turbo2,1,"
        "pulsetube,0,dummy,0"
    )


@pytest.fixture
def compressor_line_valid():
    return (
        "09-01-26,12:00:00,"
        "tc400actualspd,50.5,"
        "tc400actualspd_2,50.5,"
        "tc400actualspd_3,50.5,"
        "cpalp,10.2,cpalp_2,50.6"
    )


@pytest.fixture
def http_response_valid():
    return {
        "measurements": {
            "temperature": [300.5, 301.2, 299.8],
            "timestamp": [
                "2026-01-09T12:00:00",
                "2026-01-09T12:01:00",
                "2026-01-09T12:02:00",
            ],
        }
    }


@pytest.fixture
def http_response_empty():
    return {
        "measurements": {
            "temperature": [],
            "timestamp": [],
        }
    }
