from pytest_mock import MockerFixture

from cryo_metrics_exporter import (
    CustomMetricHandler,
    InternalServerError,
    ServiceUnavailableError,
)


class TestSendErrorResponse:
    """Test suite for _send_error_response method."""

    def test_send_error_response_service_unavailable_error_sends_503(
        self, mocker: MockerFixture
    ):
        # Arrange
        mocker.patch.object(CustomMetricHandler, "handle")
        handler = CustomMetricHandler(
            mocker.MagicMock(), ("127.0.0.1", 8000), mocker.MagicMock()
        )
        mocker.patch.object(handler, "send_response")
        mocker.patch.object(handler, "send_header")
        mocker.patch.object(handler, "end_headers")
        mock_wfile = mocker.MagicMock()
        handler.wfile = mock_wfile

        # Act
        handler._send_error_response(503, "Service Unavailable")

        # Assert
        handler.send_response.assert_called_once_with(503)
        handler.send_header.assert_called_once_with(
            "Content-Type", "text/plain; charset=utf-8"
        )
        handler.end_headers.assert_called_once()
        written_data = mock_wfile.write.call_args[0][0].decode()
        assert "503" in written_data
        assert "Service Unavailable" in written_data

    def test_send_error_response_internal_server_error_sends_500(
        self, mocker: MockerFixture
    ):
        # Arrange
        mocker.patch.object(CustomMetricHandler, "handle")
        handler = CustomMetricHandler(
            mocker.MagicMock(), ("127.0.0.1", 8000), mocker.MagicMock()
        )
        mocker.patch.object(handler, "send_response")
        mocker.patch.object(handler, "send_header")
        mocker.patch.object(handler, "end_headers")
        mock_wfile = mocker.MagicMock()
        handler.wfile = mock_wfile

        # Act
        handler._send_error_response(500, "Internal Server Error")

        # Assert
        handler.send_response.assert_called_once_with(500)
        handler.send_header.assert_called_once_with(
            "Content-Type", "text/plain; charset=utf-8"
        )
        handler.end_headers.assert_called_once()
        written_data = mock_wfile.write.call_args[0][0].decode()
        assert "500" in written_data
        assert "Internal Server Error" in written_data

    def test_send_error_response_handles_write_failure(self, mocker: MockerFixture):
        # Arrange
        mocker.patch.object(CustomMetricHandler, "handle")
        handler = CustomMetricHandler(
            mocker.MagicMock(), ("127.0.0.1", 8000), mocker.MagicMock()
        )
        mocker.patch.object(handler, "send_response")
        mocker.patch.object(handler, "send_header")
        mocker.patch.object(handler, "end_headers")
        mock_wfile = mocker.MagicMock()
        handler.wfile = mock_wfile
        mock_wfile.write.side_effect = OSError
        mock_logger = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act & Assert
        handler._send_error_response(500, "Error")
        mock_logger.assert_called_once_with("Failed to send error response.")


class TestDoGet:
    """Test suite for do_GET method."""

    def test__do_get_calls_super(self, mocker: MockerFixture):
        # Arrange
        mocker.patch.object(CustomMetricHandler, "handle")
        handler = CustomMetricHandler(
            mocker.MagicMock(), ("127.0.0.1", 8000), mocker.MagicMock()
        )
        mock_super_do_get = mocker.patch("cryo_metrics_exporter.MetricsHandler.do_GET")

        # Act
        handler.do_GET()

        # Assert
        mock_super_do_get.assert_called_once()

    def test_do_get_handles_service_unavailable(self, mocker: MockerFixture):
        # Arrange
        mocker.patch.object(CustomMetricHandler, "handle")
        handler = CustomMetricHandler(
            mocker.MagicMock(), ("127.0.0.1", 8000), mocker.MagicMock()
        )
        mocker.patch(
            "cryo_metrics_exporter.MetricsHandler.do_GET",
            side_effect=ServiceUnavailableError,
        )
        mock_send_error = mocker.patch.object(handler, "_send_error_response")
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        handler.do_GET()

        # Assert
        mock_send_error.assert_called_once_with(503, "Service Unavailable")
        mock_logger_exception.assert_called_once_with(
            "Returning 503 Service Unavailable."
        )

    def test__do_get_handles_internal_server_error(self, mocker: MockerFixture):
        # Arrange
        mocker.patch.object(CustomMetricHandler, "handle")
        handler = CustomMetricHandler(
            mocker.MagicMock(), ("127.0.0.1", 8000), mocker.MagicMock()
        )
        mocker.patch(
            "cryo_metrics_exporter.MetricsHandler.do_GET",
            side_effect=InternalServerError,
        )
        mock_send_error = mocker.patch.object(handler, "_send_error_response")
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        handler.do_GET()

        # Assert
        mock_send_error.assert_called_once_with(500, "Internal Server Error")
        mock_logger_exception.assert_called_once_with(
            "Returning 500 Internal Server Error."
        )

    def test_do_get_handles_generic_exception(self, mocker: MockerFixture):
        # Arrange
        mocker.patch.object(CustomMetricHandler, "handle")
        handler = CustomMetricHandler(
            mocker.MagicMock(), ("127.0.0.1", 8000), mocker.MagicMock()
        )
        mocker.patch(
            "cryo_metrics_exporter.MetricsHandler.do_GET",
            side_effect=RuntimeError,
        )
        mock_send_error = mocker.patch.object(handler, "_send_error_response")
        mock_logger_exception = mocker.patch("cryo_metrics_exporter.logger.exception")

        # Act
        handler.do_GET()

        # Assert
        mock_send_error.assert_called_once_with(500, "Internal Server Error")
        mock_logger_exception.assert_called_once_with(
            "Unhandled exception occurred. Returning 500 Internal Server Error."
        )
