from datetime import datetime

import pytest
import requests
from pytest_mock import MockerFixture

from cryo_metrics_exporter import CustomCollector, InternalServerError


class TestFetchTemperatureData:
    """Test suite for _fetch_temperature_data method."""

    def test_fetch_temperature_data_successful_retrieval_returns_data_and_no_retry(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        sample_datetime_utc: datetime,
        http_response_valid: dict,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_response = mocker.MagicMock()
        mock_response.json.return_value = http_response_valid
        mock_post = mocker.patch("cryo_metrics_exporter.requests.post")
        mock_post.return_value = mock_response

        from_time = sample_datetime_utc
        to_time = sample_datetime_utc

        # Act
        result, is_retry_needed = collector._fetch_temperature_data(
            from_time, to_time, "1"
        )

        # Assert
        assert result["values"] == [300.5, 301.2, 299.8]
        assert len(result["timestamps"]) == 3
        assert is_retry_needed is False

    def test_fetch_temperature_data_empty_data_sets_retry_flag(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        sample_datetime_utc: datetime,
        http_response_empty: dict,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_response = mocker.MagicMock()
        mock_response.json.return_value = http_response_empty
        mock_post = mocker.patch("cryo_metrics_exporter.requests.post")
        mock_post.return_value = mock_response

        from_time = sample_datetime_utc
        to_time = sample_datetime_utc

        # Act
        result, is_retry_needed = collector._fetch_temperature_data(
            from_time, to_time, "1"
        )

        # Assert
        assert result["values"] == []
        assert len(result["timestamps"]) == 0
        assert is_retry_needed is True

    @pytest.mark.parametrize("status_code", [400, 401, 403])
    def test_fetch_temperature_data_non_retryable_error_raises_internal_server_error(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        sample_datetime_utc: datetime,
        status_code: int,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_response = mocker.MagicMock()
        mock_response.status_code = status_code
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )
        mock_post = mocker.patch("cryo_metrics_exporter.requests.post")
        mock_post.return_value = mock_response

        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        from_time = sample_datetime_utc
        to_time = sample_datetime_utc

        # Act & Assert
        with pytest.raises(InternalServerError):
            collector._fetch_temperature_data(from_time, to_time, "1")
        mock_logger_exception.assert_called_once_with(
            "HTTP request to channel %s failed with status code %s", "1", status_code
        )

    @pytest.mark.parametrize("status_code", [404])
    def test_fetch_temperature_data_not_found_error_remains_retry_flag_false(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        sample_datetime_utc: datetime,
        status_code: int,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_response = mocker.MagicMock()
        mock_response.status_code = status_code
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )
        mock_post = mocker.patch("cryo_metrics_exporter.requests.post")
        mock_post.return_value = mock_response

        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        from_time = sample_datetime_utc
        to_time = sample_datetime_utc

        # Act
        result, is_retry_needed = collector._fetch_temperature_data(
            from_time, to_time, "1"
        )

        # Assert
        assert result["values"] == []
        assert len(result["timestamps"]) == 0
        assert is_retry_needed is False
        mock_logger_exception.assert_called_once_with(
            "HTTP request to channel %s failed with status code %s", "1", status_code
        )

    @pytest.mark.parametrize("status_code", [500, 503, None])
    def test_fetch_temperature_data_retryable_error_sets_retry_flag(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        sample_datetime_utc: datetime,
        status_code: int,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_response = mocker.MagicMock()
        mock_response.status_code = status_code
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )
        mock_post = mocker.patch("cryo_metrics_exporter.requests.post")
        mock_post.return_value = mock_response

        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        from_time = sample_datetime_utc
        to_time = sample_datetime_utc

        # Act
        result, is_retry_needed = collector._fetch_temperature_data(
            from_time, to_time, "1"
        )

        # Assert
        assert result["values"] == []
        assert len(result["timestamps"]) == 0
        assert is_retry_needed is True
        mock_logger_exception.assert_called_once_with(
            "HTTP request to channel %s failed with status code %s", "1", status_code
        )

    def test_fetch_temperature_data_other_status_code_sets_retry_flag(
        self,
        mocker: MockerFixture,
        sample_config: dict,
        sample_datetime_utc: datetime,
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_response = mocker.MagicMock()
        mock_response.status_code = 502
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )
        mock_post = mocker.patch("cryo_metrics_exporter.requests.post")
        mock_post.return_value = mock_response

        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        from_time = sample_datetime_utc
        to_time = sample_datetime_utc

        # Act
        result, is_retry_needed = collector._fetch_temperature_data(
            from_time, to_time, "1"
        )

        # Assert
        assert result["values"] == []
        assert len(result["timestamps"]) == 0
        assert is_retry_needed is True
        mock_logger_exception.assert_called_once_with(
            "HTTP request to channel %s failed with status code %s", "1", 502
        )

    def test_fetch_temperature_data_connection_timeout_sets_retry_flag(
        self, mocker: MockerFixture, sample_config: dict, sample_datetime_utc: datetime
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_response = mocker.MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.Timeout(
            response=mock_response
        )
        mock_post = mocker.patch("cryo_metrics_exporter.requests.post")
        mock_post.return_value = mock_response

        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        from_time = sample_datetime_utc
        to_time = sample_datetime_utc

        # Act
        result, is_retry_needed = collector._fetch_temperature_data(
            from_time, to_time, "1"
        )

        # Assert
        assert result["values"] == []
        assert len(result["timestamps"]) == 0
        assert is_retry_needed is True
        mock_logger_exception.assert_called_once_with(
            "HTTP connection to channel %s timed out (timeout=%s sec)",
            "1",
            collector._http_timeout,
        )

    def test_fetch_temperature_data_connection_error_sets_retry_flag(
        self, mocker: MockerFixture, sample_config: dict, sample_datetime_utc: datetime
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_response = mocker.MagicMock()
        mock_response.raise_for_status.side_effect = (
            requests.exceptions.ConnectionError(response=mock_response)
        )
        mock_post = mocker.patch("cryo_metrics_exporter.requests.post")
        mock_post.return_value = mock_response

        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        from_time = sample_datetime_utc
        to_time = sample_datetime_utc

        # Act
        result, is_retry_needed = collector._fetch_temperature_data(
            from_time, to_time, "1"
        )

        # Assert
        assert result["values"] == []
        assert len(result["timestamps"]) == 0
        assert is_retry_needed is True
        mock_logger_exception.assert_called_once_with(
            "HTTP request to channel %s failed due to network unreachable", "1"
        )

    def test_fetch_temperature_data_unexpected_error_sets_retry_flag(
        self, mocker: MockerFixture, sample_config: dict, sample_datetime_utc: datetime
    ):
        # Arrange
        collector = CustomCollector(sample_config)
        mock_response = mocker.MagicMock()
        mock_response.raise_for_status.side_effect = (
            requests.exceptions.RequestException(response=mock_response)
        )
        mock_post = mocker.patch("cryo_metrics_exporter.requests.post")
        mock_post.return_value = mock_response

        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        from_time = sample_datetime_utc
        to_time = sample_datetime_utc

        # Act
        result, is_retry_needed = collector._fetch_temperature_data(
            from_time, to_time, "1"
        )

        # Assert
        assert result["values"] == []
        assert len(result["timestamps"]) == 0
        assert is_retry_needed is True
        mock_logger_exception.assert_called_once_with(
            "Unexpected error occurred during HTTP request to channel %s", "1"
        )
