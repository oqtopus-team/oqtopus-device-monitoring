from unittest.mock import AsyncMock

import httpx
import pytest
from httpx import AsyncClient, HTTPError
from pytest_mock import MockerFixture

from common.request_validation import RequestValidationError
from common.types.config import AppConfig
from common.types.victoria_metrics import MetricDataResponse
from routers import metrics
from tests.conftest import (
    CURRENT_OPERATION_ID,
)
from tests.routers.conftest import (
    ExecutorStub,
    LockStub,
    OperationHistoryWriterStub,
    ValidatorStub,
)


@pytest.fixture(autouse=True)
def fixed_operation_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        metrics,
        "generate_operation_id",
        lambda *_args, **_kwargs: CURRENT_OPERATION_ID,
    )


def build_metric_data(
    metric: dict[str, str], values: list[float | int | None], timestamps: list[int]
) -> MetricDataResponse:
    return MetricDataResponse(
        metric=dict(metric), values=list(values), timestamps=list(timestamps)
    )


def test_get_client_dependency_with_app_config_returns_client(
    app_config: AppConfig,
) -> None:
    # Arrange / Act
    client = metrics.get_client(app_config)

    # Assert
    assert isinstance(client, metrics.VictoriaMetricsClient)


@pytest.mark.asyncio
async def test_get_metrics_names_with_valid_request_succeeds(
    async_client: AsyncClient, client: AsyncMock
) -> None:
    # Arrange
    client.get_metric_names.return_value = ["cpu", "mem"]

    # Act
    response = await async_client.get(
        "/metrics/names", params={"offset": 0, "limit": 2}
    )

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["metrics"] == ["cpu", "mem"]
    assert body["data"]["page"] == {"offset": 0, "limit": 2, "total": 2}
    client.get_metric_names.assert_awaited_once_with(offset=0, limit=2)


@pytest.mark.asyncio
async def test_get_metrics_names_when_http_error_occurs(
    async_client: AsyncClient, client: AsyncMock
) -> None:
    # Arrange
    client.get_metric_names.side_effect = HTTPError(
        "Error",
    )

    # Act
    response = await async_client.get("/metrics/names")

    # Assert
    assert response.status_code == 500
    assert "Error" in response.json()["message"]


@pytest.mark.asyncio
async def test_get_metric_label_keys_with_valid_request_succeeds(
    async_client: AsyncClient, client: AsyncMock
) -> None:
    # Arrange
    client.get_series_label_keys.return_value = ["env", "role"]

    # Act
    response = await async_client.get("/metrics/cpu/labels")

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert body["data"] == {"metric": "cpu", "label_keys": ["env", "role"]}
    client.get_series_label_keys.assert_awaited_once_with(metric_name="cpu")


@pytest.mark.asyncio
async def test_get_metric_label_keys_when_missing_returns_error(
    async_client: AsyncClient, client: AsyncMock
) -> None:
    # Arrange
    client.get_series_label_keys.return_value = []

    # Act
    response = await async_client.get("/metrics/cpu/labels")

    # Assert
    assert response.status_code == 404
    assert "Label is not found" in response.json()["message"]


@pytest.mark.asyncio
async def test_get_metric_label_keys_when_http_error_occurs(
    async_client: AsyncClient, client: AsyncMock
) -> None:
    # Arrange
    client.get_series_label_keys.side_effect = HTTPError(
        "Error",
    )

    # Act
    response = await async_client.get("/metrics/cpu/labels")

    # Assert
    assert response.status_code == 500
    assert "Internal server error" in response.json()["message"]


@pytest.mark.asyncio
async def test_get_metric_label_values_with_valid_request_succeeds(
    async_client: AsyncClient, client: AsyncMock
) -> None:
    # Arrange
    client.get_series_label_values.return_value = ["a", "b"]

    # Act
    response = await async_client.get(
        "/metrics/cpu/labels/env/values", params={"offset": 0, "limit": 2}
    )

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["values"] == ["a", "b"]
    assert body["data"]["page"] == {"offset": 0, "limit": 2, "total": 2}
    client.get_series_label_values.assert_awaited_once_with(
        metric_name="cpu", label_key="env", offset=0, limit=2
    )


@pytest.mark.asyncio
async def test_get_metric_label_values_when_missing_returns_error(
    async_client: AsyncClient, client: AsyncMock
) -> None:
    # Arrange
    client.get_series_label_values.return_value = []

    # Act
    response = await async_client.get("/metrics/cpu/labels/env/values")

    # Assert
    assert response.status_code == 404
    assert "Label values are not found" in response.json()["message"]


@pytest.mark.asyncio
async def test_get_metric_label_values_when_http_error_occurs(
    async_client: AsyncClient, client: AsyncMock
) -> None:
    # Arrange
    client.get_series_label_values.side_effect = HTTPError("fail")

    # Act
    response = await async_client.get("/metrics/cpu/labels/env/values")

    # Assert
    assert response.status_code == 500
    assert "Internal server error" in response.json()["message"]


@pytest.mark.asyncio
async def test_get_time_series_data_with_single_point_succeeds(
    async_client: AsyncClient,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    validator = ValidatorStub()
    monkeypatch.setattr(
        metrics, "RequestValidation", lambda *_args, **_kwargs: validator
    )
    client.read_timeseries.return_value = [
        build_metric_data({"__name__": "cpu"}, [1], [0])
    ]

    # Act
    response = await async_client.get(
        "/metrics/cpu/series/data",
        params={
            "start": "1970-01-01T00:00:00+00:00",
            "end": "1970-12-31T00:00:00+00:00",
        },
    )

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["values"] == [
        [
            "1970-01-01T00:00:00Z",  # timestamp 0
            "1",  # value 1
        ]
    ]
    assert body["data"]["step"] is None


@pytest.mark.asyncio
async def test_get_time_series_data_with_multiple_points_succeeds(
    async_client: AsyncClient,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    validator = ValidatorStub()
    monkeypatch.setattr(
        metrics, "RequestValidation", lambda *_args, **_kwargs: validator
    )
    client.read_timeseries.return_value = [
        build_metric_data(
            {"__name__": "cpu"},  # Metric
            [1, 2],  # values
            [0, 1],  # timestamps
        )
    ]

    # Act
    response = await async_client.get(
        "/metrics/cpu/series/data",
        params={
            "start": "1970-01-01T00:00:00+00:00",
            "end": "1970-12-31T00:00:00+00:00",
        },
    )

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["values"] == [
        [
            "1970-01-01T00:00:00Z",  # timestamp 0
            "1",  # value 1
        ],
        [
            "1970-01-01T00:00:01Z",  # timestamp 1
            "2",  # value 2
        ],
    ]
    assert body["data"]["step"] == 1  # step between timestamps is 1 second


@pytest.mark.asyncio
async def test_get_time_series_data_when_metrics_contains_none_values(
    async_client: AsyncClient,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    validator = ValidatorStub()
    monkeypatch.setattr(
        metrics, "RequestValidation", lambda *_args, **_kwargs: validator
    )
    client.read_timeseries.return_value = [
        build_metric_data({"__name__": "cpu"}, [1, None, 3], [0, 1, 2])
    ]

    # Act
    response = await async_client.get(
        "/metrics/cpu/series/data",
        params={
            "start": "1970-01-01T00:00:00+00:00",
            "end": "1970-12-31T00:00:00+00:00",
        },
    )

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["values"] == [
        [
            "1970-01-01T00:00:00Z",  # timestamp 0
            "1",  # value 1
        ],
        [
            "1970-01-01T00:00:02Z",  # timestamp 2
            "3",  # value 3
        ],
    ]
    assert body["data"]["step"] == 1  # step between timestamps is 1 second


@pytest.mark.asyncio
async def test_get_time_series_data_when_missing_timerange_returns_error(
    async_client: AsyncClient,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    validator = ValidatorStub()
    monkeypatch.setattr(
        metrics, "RequestValidation", lambda *_args, **_kwargs: validator
    )
    client.read_timeseries.return_value = [
        build_metric_data({"__name__": "cpu"}, [1], [0])
    ]

    # Act
    response = await async_client.get(
        "/metrics/cpu/series/data",
        params={},
    )

    # Assert
    assert response.status_code == 400
    body = response.json()
    assert "validation error" in body["message"]


@pytest.mark.asyncio
async def test_get_time_series_data_when_invalid_timestamp_returns_error(
    async_client: AsyncClient,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    validator = ValidatorStub()
    monkeypatch.setattr(
        metrics, "RequestValidation", lambda *_args, **_kwargs: validator
    )
    client.read_timeseries.return_value = [
        build_metric_data({"__name__": "cpu"}, [1], [0])
    ]

    # Act
    response = await async_client.get(
        "/metrics/cpu/series/data",
        params={
            "start": "1970-09-0100:00:00+00:00",  # invalid timestamp (missing T)
            "end": "1970-12-31T00:00:00+00:00",
        },
    )

    # Assert
    assert response.status_code == 400
    body = response.json()
    assert "validation error" in body["message"]


@pytest.mark.asyncio
async def test_get_time_series_data_when_missing_series_returns_error(
    async_client: AsyncClient,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange    validator = ValidatorStub()
    validator = ValidatorStub()
    monkeypatch.setattr(
        metrics, "RequestValidation", lambda *_args, **_kwargs: validator
    )
    client.read_timeseries.return_value = []

    # Act
    response = await async_client.get(
        "/metrics/cpu/series/data",
        params={
            "start": "1970-01-01T00:00:00+00:00",
            "end": "1970-12-31T00:00:00+00:00",
        },
    )

    # Assert
    assert response.status_code == 404
    assert "Expected exactly one time series" in response.json()["message"]


@pytest.mark.asyncio
async def test_get_time_series_data_when_multiple_series_returns_error(
    async_client: AsyncClient,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    validator = ValidatorStub()
    monkeypatch.setattr(
        metrics, "RequestValidation", lambda *_args, **_kwargs: validator
    )
    client.read_timeseries.return_value = [
        build_metric_data({"__name__": "cpu"}, [1], [0]),
        build_metric_data({"__name__": "cpu2"}, [2], [1]),
    ]

    # Act
    response = await async_client.get(
        "/metrics/cpu/series/data",
        params={
            "start": "1970-01-01T00:00:00+00:00",
            "end": "1970-12-31T00:00:00+00:00",
        },
    )

    # Assert
    assert response.status_code == 400
    assert "Expected exactly one time series" in response.json()["message"]


@pytest.mark.asyncio
async def test_get_time_series_data_when_sampling_guard_fails_errors(
    async_client: AsyncClient,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    validator = ValidatorStub(RequestValidationError("invalid"))

    monkeypatch.setattr(
        metrics, "RequestValidation", lambda *_args, **_kwargs: validator
    )

    # Act
    response = await async_client.get(
        "/metrics/cpu/series/data",
        params={
            "start": "1970-01-01T00:00:00+00:00",
            "end": "1970-12-31T00:00:00+00:00",
        },
    )

    # Assert
    assert response.status_code == 400
    assert "invalid" in response.json()["message"]
    client.read_timeseries.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_time_series_data_when_internal_server_error_in_validation_is_raised(
    async_client: AsyncClient,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    validator = ValidatorStub()

    def raise_http_error() -> None:
        raise HTTPError(message="Error")

    monkeypatch.setattr(
        metrics, "RequestValidation", lambda *_args, **_kwargs: validator
    )
    monkeypatch.setattr(
        validator,
        "validate_get_time_series_data",
        raise_http_error,
    )

    # Act
    response = await async_client.get(
        "/metrics/cpu/series/data",
        params={
            "start": "1970-01-01T00:00:00+00:00",
            "end": "1970-12-31T00:00:00+00:00",
        },
    )

    # Assert
    assert response.status_code == 500
    assert "Internal server error" in response.json()["message"]
    client.read_timeseries.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_time_series_data_when_request_errors_errors(
    async_client: AsyncClient,
    client: AsyncMock,
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    validator = ValidatorStub()

    monkeypatch.setattr(
        metrics, "RequestValidation", lambda *_args, **_kwargs: validator
    )
    client.count_over_time = mocker.AsyncMock(return_value=1)
    client.read_timeseries.side_effect = httpx.RequestError(
        "Error", request=httpx.Request("GET", "http://test")
    )

    # Act
    response = await async_client.get(
        "/metrics/cpu/series/data",
        params={
            "start": "1970-01-01T00:00:00+00:00",
            "end": "1970-12-31T00:00:00+00:00",
        },
    )

    # Assert
    assert response.status_code == 500
    assert "Error" in response.json()["message"]


@pytest.mark.asyncio
async def test_get_time_series_data_when_unexpected_error_errors(
    async_client: AsyncClient,
    client: AsyncMock,
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arranges
    validator = ValidatorStub()

    monkeypatch.setattr(
        metrics, "RequestValidation", lambda *_args, **_kwargs: validator
    )
    client.count_over_time = mocker.AsyncMock(return_value=1)
    client.read_timeseries.side_effect = HTTPError("fail")

    # Act
    response = await async_client.get(
        "/metrics/cpu/series/data",
        params={
            "start": "1970-01-01T00:00:00+00:00",
            "end": "1970-12-31T00:00:00+00:00",
        },
    )

    # Assert
    assert response.status_code == 500
    assert "Internal server error" in response.json()["message"]


@pytest.mark.asyncio
async def test_delete_time_series_data_with_valid_request_succeeds(
    async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    lock = LockStub()
    validator = ValidatorStub()
    executor = ExecutorStub()

    monkeypatch.setattr(metrics, "LockManager", lambda *_args, **_kwargs: lock)
    monkeypatch.setattr(
        metrics,
        "OperationHistoryWriter",
        lambda *_args, **_kwargs: OperationHistoryWriterStub(),
    )
    monkeypatch.setattr(
        metrics, "RequestValidation", lambda *_args, **_kwargs: validator
    )
    monkeypatch.setattr(
        metrics, "OperationExecutor", lambda *_args, **_kwargs: executor
    )

    # Act
    response = await async_client.request(
        "DELETE", "/metrics/cpu/series/data", json={"match": []}
    )

    # Assert
    assert response.status_code == 202
    body = response.json()
    assert body["data"]["operation_id"] == CURRENT_OPERATION_ID
    assert "scheduled successfully" in body["data"]["summary"]["message"]


@pytest.mark.parametrize(
    "lock_stub",
    [
        {"acquire_result": False, "holder": "other"},
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_delete_time_series_data_when_locked_returns_locked_error(
    async_client: AsyncClient, lock_stub: LockStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    monkeypatch.setattr(metrics, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        metrics,
        "OperationHistoryWriter",
        lambda *_args, **_kwargs: OperationHistoryWriterStub(),
    )
    monkeypatch.setattr(
        metrics, "OperationExecutor", lambda *_args, **_kwargs: ExecutorStub()
    )

    # Act
    response = await async_client.request(
        "DELETE", "/metrics/cpu/series/data", json={"match": []}
    )

    # Assert
    assert response.status_code == 423
    assert "other" in response.json()["message"]


@pytest.mark.parametrize(
    "lock_stub",
    [
        {"acquire_exc": FileNotFoundError("fail")},
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_delete_time_series_data_when_lock_raises_errors(
    async_client: AsyncClient, lock_stub: LockStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    monkeypatch.setattr(metrics, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        metrics,
        "OperationHistoryWriter",
        lambda *_args, **_kwargs: OperationHistoryWriterStub(),
    )
    monkeypatch.setattr(
        metrics, "OperationExecutor", lambda *_args, **_kwargs: ExecutorStub()
    )

    # Act
    response = await async_client.request(
        "DELETE", "/metrics/cpu/series/data", json={"match": []}
    )

    # Assert
    assert response.status_code == 500
    assert "Internal server error" in response.json()["message"]


@pytest.mark.parametrize(
    "validator_stub",
    [{"validate_exc": RequestValidationError("invalid")}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_delete_time_series_data_when_validation_fails_errors(
    async_client: AsyncClient,
    lock_stub: LockStub,
    validator_stub: ValidatorStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setattr(metrics, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        metrics,
        "OperationHistoryWriter",
        lambda *_args, **_kwargs: OperationHistoryWriterStub(),
    )
    monkeypatch.setattr(
        metrics, "RequestValidation", lambda *_args, **_kwargs: validator_stub
    )

    # Act
    response = await async_client.request(
        "DELETE", "/metrics/cpu/series/data", json={"match": []}
    )

    # Assert
    assert response.status_code == 400
    assert response.json()["message"] == "invalid"
    assert lock_stub.released is True


@pytest.mark.asyncio
async def test_delete_time_series_data_when_scheduling_fails_errors(
    async_client: AsyncClient,
    lock_stub: LockStub,
    validator_stub: ValidatorStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    executor = ExecutorStub()
    monkeypatch.setattr(metrics, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        metrics,
        "OperationHistoryWriter",
        lambda *_args, **_kwargs: OperationHistoryWriterStub(),
    )
    monkeypatch.setattr(
        metrics, "RequestValidation", lambda *_args, **_kwargs: validator_stub
    )
    monkeypatch.setattr(
        metrics, "OperationExecutor", lambda *_args, **_kwargs: executor
    )
    monkeypatch.setattr(
        "starlette.background.BackgroundTasks.add_task",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TypeError()),
    )

    # Act
    response = await async_client.request(
        "DELETE", "/metrics/cpu/series/data", json={"match": []}
    )

    # Assert
    assert response.status_code == 500
    assert "Internal server error" in response.json()["message"]
    assert lock_stub.released is True
