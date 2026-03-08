from datetime import datetime, timedelta

from cryo_metrics_exporter import CustomCollector
from pytest_mock import MockerFixture


class TestFetchSMBDataGeneric:
    """Test suite for _fetch_smb_data_generic method."""

    def test_fetch_smb_data_generic_yields_datetime_and_parsed_data_from_single_file(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        sample_datetime_utc: datetime,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=1)
        file_paths = ["test.log"]

        mocker.patch.object(
            collector,
            "fetch_smb_file_data",
            return_value=[
                "09-01-26,12:00:00,data",
                "09-01-26,12:30:00,data",
            ],
        )

        mocker.patch.object(
            collector,
            "_parse_time",
            side_effect=[
                sample_datetime_utc,
                sample_datetime_utc + timedelta(minutes=30),
            ],
        )

        mocker.patch.object(
            collector, "_parse_pressure_line", return_value={"parsed": "data"}
        )

        # Act
        results = list(
            collector._fetch_smb_data_generic(
                from_time, to_time, file_paths, collector._parse_pressure_line
            )
        )

        # Assert
        assert len(results) == 2
        assert results[0] == (
            sample_datetime_utc,
            {"parsed": "data"},
        )
        assert results[1] == (
            sample_datetime_utc + timedelta(minutes=30),
            {"parsed": "data"},
        )

    def test_fetch_smb_data_generic_yields_datetime_and_parsed_data_from_multiple_files(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        sample_datetime_utc: datetime,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=2)
        file_paths = ["test1.log", "test2.log"]

        mocker.patch.object(
            collector,
            "fetch_smb_file_data",
            side_effect=[
                ["09-01-26,12:00:00,data"],
                ["09-01-26,12:30:00,data"],
            ],
        )

        mocker.patch.object(
            collector,
            "_parse_time",
            side_effect=[
                sample_datetime_utc,
                sample_datetime_utc + timedelta(minutes=30),
            ],
        )

        mocker.patch.object(
            collector,
            "_parse_pressure_line",
            side_effect=[
                {"parsed": "data1"},
                {"parsed": "data2"},
            ],
        )

        # Act
        results = list(
            collector._fetch_smb_data_generic(
                from_time, to_time, file_paths, collector._parse_pressure_line
            )
        )

        # Assert
        assert len(results) == 2
        assert results[0] == (
            sample_datetime_utc,
            {"parsed": "data1"},
        )
        assert results[1] == (
            sample_datetime_utc + timedelta(minutes=30),
            {"parsed": "data2"},
        )

    def test_fetch_smb_data_generic_filters_by_datetime_range(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        sample_datetime_utc: datetime,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(minutes=30)
        file_paths = ["test.log"]

        mocker.patch.object(
            collector,
            "fetch_smb_file_data",
            return_value=[
                "09-01-26,11:00:00,before",
                "09-01-26,12:10:00,within",
                "09-01-26,13:00:00,after",
            ],
        )

        mocker.patch.object(
            collector,
            "_parse_time",
            side_effect=[
                sample_datetime_utc - timedelta(hours=1),
                sample_datetime_utc + timedelta(minutes=10),
                sample_datetime_utc + timedelta(hours=1),
            ],
        )

        mocker.patch.object(
            collector,
            "_parse_pressure_line",
            side_effect=[
                {"value": "within"},
            ],
        )

        # Act
        results = list(
            collector._fetch_smb_data_generic(
                from_time, to_time, file_paths, collector._parse_pressure_line
            )
        )

        # Assert
        assert len(results) == 1
        assert results[0] == (
            sample_datetime_utc + timedelta(minutes=10),
            {"value": "within"},
        )

    def test_fetch_smb_data_generic_skips_empty_lines(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        sample_datetime_utc: datetime,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=1)

        mocker.patch.object(
            collector,
            "fetch_smb_file_data",
            return_value=[
                "09-01-26,12:00:00,data",
                "",
                "09-01-26,12:30:00,data",
            ],
        )

        mocker.patch.object(
            collector,
            "_parse_time",
            side_effect=[
                sample_datetime_utc,
                sample_datetime_utc + timedelta(minutes=30),
            ],
        )

        mocker.patch.object(
            collector, "_parse_pressure_line", return_value={"value": "data"}
        )

        # Act
        results = list(
            collector._fetch_smb_data_generic(
                from_time,
                to_time,
                ["test.log"],
                collector._parse_pressure_line,
            )
        )

        # Assert
        assert len(results) == 2
        assert results[0] == (
            sample_datetime_utc,
            {"value": "data"},
        )
        assert results[1] == (
            sample_datetime_utc + timedelta(minutes=30),
            {"value": "data"},
        )

    def test_fetch_smb_data_generic_skips_invalid_datetime_lines(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        sample_datetime_utc: datetime,
    ):
        """Skip lines with invalid datetime format."""
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=1)

        mocker.patch.object(
            collector,
            "fetch_smb_file_data",
            return_value=[
                "09-01-26,12:00:00,data1",
                "invalid-datetime,data2",
                "09-01-26,12:30:00,data3",
            ],
        )

        mocker.patch.object(
            collector,
            "_parse_time",
            side_effect=[
                sample_datetime_utc,
                None,
                sample_datetime_utc + timedelta(minutes=30),
            ],
        )

        mocker.patch.object(
            collector, "_parse_pressure_line", return_value={"value": "data"}
        )

        # Act
        results = list(
            collector._fetch_smb_data_generic(
                from_time,
                to_time,
                ["test.log"],
                collector._parse_pressure_line,
            )
        )

        # Assert
        assert len(results) == 2
        assert results[0] == (
            sample_datetime_utc,
            {"value": "data"},
        )
        assert results[1] == (
            sample_datetime_utc + timedelta(minutes=30),
            {"value": "data"},
        )

    def test_fetch_smb_data_generic_skips_file_with_no_data(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        sample_datetime_utc: datetime,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=2)
        file_paths = ["test1.log", "test2.log", "test3.log"]

        mocker.patch.object(
            collector,
            "fetch_smb_file_data",
            side_effect=[
                None,
                [],
                ["09-01-26,12:30:00,data"],
            ],
        )

        mocker.patch.object(
            collector,
            "_parse_time",
            side_effect=[
                sample_datetime_utc + timedelta(minutes=30),
            ],
        )

        mocker.patch.object(
            collector,
            "_parse_pressure_line",
            side_effect=[
                {"parsed": "data1"},
            ],
        )

        # Act
        results = list(
            collector._fetch_smb_data_generic(
                from_time, to_time, file_paths, collector._parse_pressure_line
            )
        )

        # Assert
        assert len(results) == 1
        assert results[0] == (
            sample_datetime_utc + timedelta(minutes=30),
            {"parsed": "data1"},
        )


class TestFetchSMBPressureData:
    """Test suite for _fetch_smb_pressure_data method."""

    def test_fetch_smb_pressure_data_successful_retrieval_returns_all_channels(
        self, mocker: MockerFixture, sample_config: dict, sample_datetime_utc: datetime
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=1)
        pressure_channels = {
            "CH1": 100.5,
            "CH2": 120.3,
            "CH3": 10.3,
            "CH4": 22.3,
            "CH5": 160.4,
            "CH6": 180.7,
        }
        expected_values = [100.5, 120.3, 10.3, 22.3, 160.4, 180.7]

        mocker.patch.object(
            CustomCollector, "generate_file_path", return_value=["test.log"]
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_data_generic",
            return_value=iter([(from_time, pressure_channels)]),
        )

        # Act
        result, is_retry = collector._fetch_smb_pressure_data(from_time, to_time)

        # Assert
        for i, expected_value in enumerate(expected_values):
            assert result[i]["values"] == [expected_value]
            assert result[i]["timestamps"] == [from_time.timestamp()]
        assert is_retry is False

    def test_fetch_smb_pressure_data_no_data_sets_retry_flag(
        self, mocker: MockerFixture, sample_config: dict, sample_datetime_utc: datetime
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=1)

        mocker.patch.object(
            CustomCollector, "generate_file_path", return_value=["test.log"]
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_data_generic",
            return_value=iter([(from_time, None)]),
        )

        # Act
        result, is_retry = collector._fetch_smb_pressure_data(from_time, to_time)

        # Assert
        assert len(result) == 6
        for i in range(6):
            assert result[i]["values"] == []
            assert result[i]["timestamps"] == []
        assert is_retry is True


class TestFetchSMBGasflowData:
    """Test suite for _fetch_smb_gasflow_data method."""

    def test_fetch_smb_gasflow_data_successful_retrieval_returns_data(
        self, mocker: MockerFixture, sample_config: dict, sample_datetime_utc: datetime
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=1)
        gasflow_data = 50.5

        mocker.patch.object(
            CustomCollector, "generate_file_path", return_value=["test.log"]
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_data_generic",
            return_value=iter([(from_time, gasflow_data)]),
        )

        # Act
        result, is_retry = collector._fetch_smb_gasflow_data(from_time, to_time)

        # Assert
        assert len(result) == 1
        assert result[0]["values"] == [50.5]
        assert result[0]["timestamps"] == [from_time.timestamp()]
        assert is_retry is False

    def test_fetch_smb_gasflow_data_no_data_sets_retry_flag(
        self, mocker: MockerFixture, sample_config: dict, sample_datetime_utc: datetime
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=1)

        mocker.patch.object(
            CustomCollector, "generate_file_path", return_value=["test.log"]
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_data_generic",
            return_value=iter([(from_time, None)]),
        )

        # Act
        result, is_retry = collector._fetch_smb_gasflow_data(from_time, to_time)

        # Assert
        assert len(result) == 1
        assert result[0]["values"] == []
        assert result[0]["timestamps"] == []
        assert is_retry is True


class TestFetchSMBStatusData:
    """Test suite for _fetch_smb_status_data method."""

    def test_fetch_smb_status_data_successful_retrieval_returns_devices(
        self, mocker: MockerFixture, sample_config: dict, sample_datetime_utc: datetime
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=1)
        status_data = {
            "scroll1": 1,
            "scroll2": 0,
            "turbo1": 1,
            "turbo2": 0,
            "pulsetube": 1,
        }
        excepted_values = [1, 0, 1, 0, 1]

        mocker.patch.object(
            CustomCollector, "generate_file_path", return_value=["test.log"]
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_data_generic",
            return_value=iter([(from_time, status_data)]),
        )
        # Act
        result, is_retry = collector._fetch_smb_status_data(from_time, to_time)

        # Assert
        assert len(result) == 5
        for i in range(5):
            assert result[i]["values"] == [excepted_values[i]]
            assert result[i]["timestamps"] == [from_time.timestamp()]
        assert is_retry is False

    def test_fetch_smb_status_data_no_data_sets_retry_flag(
        self, mocker: MockerFixture, sample_config: dict, sample_datetime_utc: datetime
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=1)

        mocker.patch.object(
            CustomCollector, "generate_file_path", return_value=["test.log"]
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_data_generic",
            return_value=iter([(from_time, None)]),
        )
        # Act
        result, is_retry = collector._fetch_smb_status_data(from_time, to_time)

        # Assert
        assert len(result) == 5
        for i in range(5):
            assert result[i]["values"] == []
            assert result[i]["timestamps"] == []
        assert is_retry is True


class TestFetchSMBCompressorData:
    """Test suite for _fetch_smb_compressor_data method."""

    def test_fetch_smb_compressor_data_successful_retrieval_returns_all_params(
        self, mocker: MockerFixture, sample_config: dict, sample_datetime_utc: datetime
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=1)
        comp_data = {
            "tc400actualspd": 50.0,
            "tc400actualspd_2": 60.0,
            "tc400actualspd_3": 70.0,
        }
        comp_press_data = {
            "cpalp": 1.5,
            "cpalp_2": 2.5,
        }
        compressor_data = (comp_data, comp_press_data)
        expected_comp_values = [50.0, 60.0, 70.0]
        expected_press_values = [1.5, 2.5]

        mocker.patch.object(
            CustomCollector, "generate_file_path", return_value=["test.log"]
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_data_generic",
            return_value=iter([(from_time, compressor_data)]),
        )

        # Act
        result, is_retry = collector._fetch_smb_compressor_data(from_time, to_time)

        # Assert
        assert len(result) == 5
        for i in range(3):
            assert result[i]["values"] == [expected_comp_values[i]]
            assert result[i]["timestamps"] == [from_time.timestamp()]
        for i in range(2):
            assert result[3 + i]["values"] == [expected_press_values[i]]
            assert result[3 + i]["timestamps"] == [from_time.timestamp()]
        assert is_retry is False

    def test_fetch_smb_compressor_data_no_data_sets_retry_flag(
        self, mocker: MockerFixture, sample_config: dict, sample_datetime_utc: datetime
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=1)

        mocker.patch.object(
            CustomCollector, "generate_file_path", return_value=["test.log"]
        )
        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_data_generic",
            return_value=iter([(from_time, None)]),
        )

        # Act
        result, is_retry = collector._fetch_smb_compressor_data(from_time, to_time)

        # Assert
        assert len(result) == 5
        for i in range(5):
            assert result[i]["values"] == []
            assert result[i]["timestamps"] == []
        assert is_retry is True


class TestFetchAllSMBData:
    """Test suite for _fetch_all_smb_data method."""

    def test_fetch_all_smb_data_successful_retrieval_returns_all_data(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        sample_datetime_utc: datetime,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=1)

        mock_connect = mocker.patch.object(
            collector, "_smb_connect", return_value=(True, False)
        )
        mock_disconnect = mocker.patch.object(
            collector, "smb_disconnect", return_value=None
        )

        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_pressure_data",
            return_value=(
                [{"labels": {}, "values": [100.0], "metric_family": "pressure"}],
                False,
            ),
        )

        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_gasflow_data",
            return_value=(
                [{"labels": {}, "values": [50.0], "metric_family": "helium_flow"}],
                False,
            ),
        )

        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_status_data",
            return_value=(
                [{"labels": {}, "values": [1], "metric_family": "device_status"}],
                False,
            ),
        )

        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_compressor_data",
            return_value=(
                [
                    {"labels": {}, "values": [50.0], "metric_family": "compressor"},
                    {
                        "labels": {},
                        "values": [50.0],
                        "metric_family": "compressor_pressure",
                    },
                ],
                False,
            ),
        )

        # Act
        result, is_retry = collector._fetch_all_smb_data(from_time, to_time)

        # Assert
        assert len(result) == 5
        assert is_retry is False
        mock_connect.assert_called_once()
        mock_disconnect.assert_called_once()

    def test_fetch_all_smb_data_connection_fails_returns_empty(
        self, mocker: MockerFixture, sample_config: dict, sample_datetime_utc: datetime
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=1)

        mock_connect = mocker.patch.object(
            collector, "_smb_connect", return_value=(False, True)
        )

        # Act
        result, is_retry = collector._fetch_all_smb_data(from_time, to_time)

        # Assert
        assert result == []
        assert is_retry is True
        mock_connect.assert_called_once()

    def test_fetch_all_smb_data_partial_failure_sets_retry_flag(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        sample_datetime_utc: datetime,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=1)

        mock_connect = mocker.patch.object(
            collector, "_smb_connect", return_value=(True, False)
        )
        mock_disconnect = mocker.patch.object(
            collector, "smb_disconnect", return_value=None
        )

        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_pressure_data",
            return_value=(
                [{"labels": {}, "values": [], "metric_family": "pressure"}],
                True,
            ),
        )

        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_gasflow_data",
            return_value=(
                [{"labels": {}, "values": [50.0], "metric_family": "helium_flow"}],
                False,
            ),
        )

        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_status_data",
            return_value=(
                [{"labels": {}, "values": [1], "metric_family": "device_status"}],
                False,
            ),
        )

        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_compressor_data",
            return_value=(
                [
                    {"labels": {}, "values": [50.0], "metric_family": "compressor"},
                    {
                        "labels": {},
                        "values": [50.0],
                        "metric_family": "compressor_pressure",
                    },
                ],
                False,
            ),
        )

        # Act
        result, is_retry = collector._fetch_all_smb_data(from_time, to_time)

        # Assert
        assert len(result) == 4
        assert is_retry is True
        mock_connect.assert_called_once()
        mock_disconnect.assert_called_once()

    def test_fetch_all_smb_data_all_sources_fail_sets_retry_flag(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        sample_datetime_utc: datetime,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        from_time = sample_datetime_utc
        to_time = sample_datetime_utc + timedelta(hours=1)

        mock_connect = mocker.patch.object(
            collector, "_smb_connect", return_value=(True, False)
        )
        mock_disconnect = mocker.patch.object(
            collector, "smb_disconnect", return_value=None
        )

        mock_logger_error = mocker.patch("cryo_metrics_exporter.logger.error")

        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_pressure_data",
            return_value=(
                [{"labels": {}, "values": [], "metric_family": "pressure"}],
                True,
            ),
        )

        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_gasflow_data",
            return_value=(
                [{"labels": {}, "values": [], "metric_family": "helium_flow"}],
                True,
            ),
        )

        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_status_data",
            return_value=(
                [{"labels": {}, "values": [], "metric_family": "device_status"}],
                True,
            ),
        )

        mocker.patch.object(
            CustomCollector,
            "_fetch_smb_compressor_data",
            return_value=(
                [
                    {"labels": {}, "values": [], "metric_family": "compressor"},
                    {
                        "labels": {},
                        "values": [],
                        "metric_family": "compressor_pressure",
                    },
                ],
                True,
            ),
        )

        # Act
        result, is_retry = collector._fetch_all_smb_data(from_time, to_time)

        # Assert
        assert result == []
        assert is_retry is True
        mock_connect.assert_called_once()
        mock_disconnect.assert_called_once()
        mock_logger_error.assert_called_once_with(
            "No data retrieved from any SMB data source."
        )
