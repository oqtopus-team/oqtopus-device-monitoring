import pytest
from httpx import AsyncClient

from common.types.config import AppConfig
from routers import meta
from schemas.meta import (
    ProcessStatus,
)
from tests.conftest import (
    CURRENT_OPERATION_ID,
    build_error_history,
    build_finished_history,
    build_in_progress_history,
    build_started_history,
)
from tests.routers.conftest import (
    ExecutorStub,
    LockStub,
    OperationHistoryWriterStub,
    ValidatorStub,
)


def add_payload() -> dict:
    return {
        "metric_name": "cpu",
        "new_label_key": "instance",
        "default_value": "hostA",
        "selector": {"match": []},
    }


def modify_key_payload() -> dict:
    return {
        "metric_name": "cpu",
        "range": {"start": None, "end": None},
        "selector": {"match": []},
        "from_key": "old",
        "to_key": "new",
    }


def invalid_timestamp_modify_key_payload() -> dict:
    return {
        "metric_name": "cpu",
        "range": {"start": "invalid", "end": None},
        "selector": {"match": []},
        "from_key": "old",
        "to_key": "new",
    }


def modify_value_payload() -> dict:
    return {
        "metric_name": "cpu",
        "range": {"start": None, "end": None},
        "selector": {"match": []},
        "key": "instance",
        "from_value": "a",
        "to_value": "b",
    }


def invalid_timestamp_modify_value_payload() -> dict:
    return {
        "metric_name": "cpu",
        "range": {"start": "invalid", "end": None},
        "selector": {"match": []},
        "key": "instance",
        "from_value": "a",
        "to_value": "b",
    }


def delete_payload() -> dict:
    return {
        "metric_name": "cpu",
        "label_keys": ["env", "role"],
        "selector": {"match": []},
    }


@pytest.fixture(autouse=True)
def fixed_operation_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        meta,
        "generate_operation_id",
        lambda *_args, **_kwargs: CURRENT_OPERATION_ID,
    )


def test_get_client_dependency_with_app_config_returns_client(
    app_config: AppConfig,
) -> None:
    # Arrange / Act
    client = meta.get_client(app_config)

    # Assert
    assert isinstance(client, meta.VictoriaMetricsClient)


@pytest.mark.asyncio
async def test_get_operation_status_when_process_finished_returns_completed(
    async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    history = build_finished_history()
    monkeypatch.setattr(
        meta,
        "OperationHistoryWriter",
        lambda *_args, **_kwargs: OperationHistoryWriterStub(history=history),
    )

    # Act
    response = await async_client.get("/meta/status", params={"operation_id": "op"})

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["process_status"] == ProcessStatus.completed
    assert "Operation is currently finishing" in body["data"]["messages"]


@pytest.mark.asyncio
async def test_get_operation_status_when_process_in_progress_returns_completed_status(
    async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    history = build_in_progress_history()
    monkeypatch.setattr(
        meta,
        "OperationHistoryWriter",
        lambda *_args, **_kwargs: OperationHistoryWriterStub(history=history),
    )

    # Act
    response = await async_client.get("/meta/status", params={"operation_id": "op"})

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["process_status"] == ProcessStatus.in_progress
    assert "Operation is currently deleting" in body["data"]["messages"]


@pytest.mark.asyncio
async def test_get_operation_status_when_process_not_started_returns_none_status(
    async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    history = build_started_history()
    monkeypatch.setattr(
        meta,
        "OperationHistoryWriter",
        lambda *_args, **_kwargs: OperationHistoryWriterStub(history=history),
    )

    # Act
    response = await async_client.get("/meta/status", params={"operation_id": "op"})

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["process_status"] is None
    assert "extracting" in body["data"]["messages"]


@pytest.mark.asyncio
async def test_get_operation_status_when_process_failed_returns_failed_status(
    async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    history = build_error_history()
    monkeypatch.setattr(
        meta,
        "OperationHistoryWriter",
        lambda *_args, **_kwargs: OperationHistoryWriterStub(history=history),
    )

    # Act
    response = await async_client.get("/meta/status", params={"operation_id": "op"})

    # Assert
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["process_status"] == ProcessStatus.failed
    assert "Operation failed with error:" in body["data"]["messages"]


@pytest.mark.asyncio
async def test_get_operation_status_when_history_missing_errors(
    async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    monkeypatch.setattr(
        meta,
        "OperationHistoryWriter",
        lambda *_args, **_kwargs: OperationHistoryWriterStub(history=None),
    )

    # Act
    response = await async_client.get("/meta/status", params={"operation_id": "op"})

    # Assert
    assert response.status_code == 400
    assert "Invalid operation ID" in response.json()["message"]


@pytest.mark.asyncio
async def test_get_operation_status_when_history_file_is_nothing(
    async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    monkeypatch.setattr(
        meta,
        "OperationHistoryWriter",
        lambda *_args, **_kwargs: OperationHistoryWriterStub(
            exc=FileNotFoundError("unexpected error")
        ),
    )

    # Act
    response = await async_client.get("/meta/status", params={"operation_id": "op"})

    # Assert
    assert response.status_code == 400
    assert "Invalid operation ID" in response.json()["message"]


@pytest.mark.asyncio
async def test_get_operation_status_when_history_reader_errors(
    async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    monkeypatch.setattr(
        meta,
        "OperationHistoryWriter",
        lambda *_args, **_kwargs: OperationHistoryWriterStub(
            exc=IndexError("unexpected error")
        ),
    )

    # Act
    response = await async_client.get("/meta/status", params={"operation_id": "op"})

    # Assert
    assert response.status_code == 500
    assert "Internal server error" in response.json()["message"]


@pytest.mark.asyncio
async def test_add_metadata_with_valid_request_succeeds(
    async_client: AsyncClient,
    lock_stub: LockStub,
    validator_stub: ValidatorStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = ExecutorStub()

    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "RequestValidation", lambda *_args, **_kwargs: validator_stub
    )
    monkeypatch.setattr(meta, "OperationExecutor", lambda *_args, **_kwargs: executor)

    response = await async_client.patch("/meta/add", json=add_payload())

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
async def test_add_metadata_when_locked_returns_locked_error(
    async_client: AsyncClient,
    lock_stub: LockStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "OperationExecutor", lambda *_args, **_kwargs: ExecutorStub()
    )

    response = await async_client.patch("/meta/add", json=add_payload())

    assert response.status_code == 423
    assert "other" in response.json()["message"]


@pytest.mark.parametrize(
    "lock_stub",
    [
        {"acquire_exc": FileNotFoundError("invalid")},
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_add_metadata_when_lock_raises_errors(
    async_client: AsyncClient,
    lock_stub: LockStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "OperationExecutor", lambda *_args, **_kwargs: ExecutorStub()
    )

    response = await async_client.patch("/meta/add", json=add_payload())

    assert response.status_code == 500
    assert "Internal server error" in response.json()["message"]


@pytest.mark.parametrize(
    "validator_stub",
    [{"validate_exc": meta.RequestValidationError("invalid")}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_add_metadata_when_validation_fails_errors(
    async_client: AsyncClient,
    lock_stub: LockStub,
    validator_stub: ValidatorStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "RequestValidation", lambda *_args, **_kwargs: validator_stub
    )

    response = await async_client.patch("/meta/add", json=add_payload())

    assert response.status_code == 400
    assert response.json()["message"] == "invalid"


@pytest.mark.asyncio
async def test_add_metadata_when_scheduling_fails_errors(
    async_client: AsyncClient,
    lock_stub: LockStub,
    validator_stub: ValidatorStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = ExecutorStub()
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "RequestValidation", lambda *_args, **_kwargs: validator_stub
    )
    monkeypatch.setattr(meta, "OperationExecutor", lambda *_args, **_kwargs: executor)
    monkeypatch.setattr(
        "starlette.background.BackgroundTasks.add_task",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TypeError()),
    )

    response = await async_client.patch("/meta/add", json=add_payload())

    assert response.status_code == 500
    assert "Internal server error" in response.json()["message"]


@pytest.mark.asyncio
async def test_modify_key_with_valid_request_succeeds(
    async_client: AsyncClient,
    lock_stub: LockStub,
    validator_stub: ValidatorStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = ExecutorStub()

    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "RequestValidation", lambda *_args, **_kwargs: validator_stub
    )
    monkeypatch.setattr(meta, "OperationExecutor", lambda *_args, **_kwargs: executor)

    response = await async_client.patch("/meta/modify/key", json=modify_key_payload())

    assert response.status_code == 202
    assert response.json()["data"]["operation_id"] == CURRENT_OPERATION_ID


@pytest.mark.asyncio
async def test_modify_key_with_invalid_timestamp_returns_error(
    async_client: AsyncClient,
) -> None:

    response = await async_client.patch(
        "/meta/modify/key", json=invalid_timestamp_modify_key_payload()
    )

    assert response.status_code == 400
    assert "validation error" in response.json()["message"]


@pytest.mark.parametrize(
    "lock_stub",
    [
        {"acquire_result": False, "holder": "other"},
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_modify_key_when_locked_returns_locked_error(
    async_client: AsyncClient, lock_stub: LockStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "OperationExecutor", lambda *_args, **_kwargs: ExecutorStub()
    )

    response = await async_client.patch("/meta/modify/key", json=modify_key_payload())

    assert response.status_code == 423
    assert "other" in response.json()["message"]


@pytest.mark.parametrize(
    "lock_stub",
    [
        {"acquire_exc": FileNotFoundError("invalid")},
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_modify_key_when_lock_raises_errors(
    async_client: AsyncClient, lock_stub: LockStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "OperationExecutor", lambda *_args, **_kwargs: ExecutorStub()
    )

    response = await async_client.patch("/meta/modify/key", json=modify_key_payload())

    assert response.status_code == 500
    assert "Internal server error" in response.json()["message"]


@pytest.mark.parametrize(
    "validator_stub",
    [{"validate_exc": meta.RequestValidationError("invalid")}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_modify_key_when_validation_fails_errors(
    async_client: AsyncClient,
    lock_stub: LockStub,
    validator_stub: ValidatorStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "RequestValidation", lambda *_args, **_kwargs: validator_stub
    )

    response = await async_client.patch("/meta/modify/key", json=modify_key_payload())

    assert response.status_code == 400
    assert response.json()["message"] == "invalid"


@pytest.mark.asyncio
async def test_modify_key_when_scheduling_fails_errors(
    async_client: AsyncClient,
    lock_stub: LockStub,
    validator_stub: ValidatorStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = ExecutorStub()
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "RequestValidation", lambda *_args, **_kwargs: validator_stub
    )
    monkeypatch.setattr(meta, "OperationExecutor", lambda *_args, **_kwargs: executor)
    monkeypatch.setattr(
        "starlette.background.BackgroundTasks.add_task",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TypeError()),
    )

    response = await async_client.patch("/meta/modify/key", json=modify_key_payload())

    assert response.status_code == 500
    assert "Internal server error" in response.json()["message"]


@pytest.mark.asyncio
async def test_modify_value_with_valid_request_succeeds(
    async_client: AsyncClient,
    lock_stub: LockStub,
    validator_stub: ValidatorStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = ExecutorStub()

    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "RequestValidation", lambda *_args, **_kwargs: validator_stub
    )
    monkeypatch.setattr(meta, "OperationExecutor", lambda *_args, **_kwargs: executor)

    response = await async_client.patch(
        "/meta/modify/value", json=modify_value_payload()
    )

    assert response.status_code == 202
    assert response.json()["data"]["operation_id"] == CURRENT_OPERATION_ID


@pytest.mark.asyncio
async def test_modify_value_with_invalid_timestamp_returns_error(
    async_client: AsyncClient,
) -> None:

    response = await async_client.patch(
        "/meta/modify/value", json=invalid_timestamp_modify_value_payload()
    )

    assert response.status_code == 400
    assert "validation error" in response.json()["message"]


@pytest.mark.parametrize(
    "lock_stub",
    [
        {"acquire_result": False, "holder": "other"},
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_modify_value_when_locked_returns_locked_error(
    async_client: AsyncClient, lock_stub: LockStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "OperationExecutor", lambda *_args, **_kwargs: ExecutorStub()
    )

    response = await async_client.patch(
        "/meta/modify/value", json=modify_value_payload()
    )

    assert response.status_code == 423
    assert "other" in response.json()["message"]


@pytest.mark.parametrize(
    "lock_stub",
    [
        {"acquire_exc": FileNotFoundError("invalid")},
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_modify_value_when_lock_raises_errors(
    async_client: AsyncClient, lock_stub: LockStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "OperationExecutor", lambda *_args, **_kwargs: ExecutorStub()
    )

    response = await async_client.patch(
        "/meta/modify/value", json=modify_value_payload()
    )

    assert response.status_code == 500
    assert "Internal server error" in response.json()["message"]


@pytest.mark.parametrize(
    "validator_stub",
    [{"validate_exc": meta.RequestValidationError("invalid")}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_modify_value_when_validation_fails_errors(
    async_client: AsyncClient,
    lock_stub: LockStub,
    validator_stub: ValidatorStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "RequestValidation", lambda *_args, **_kwargs: validator_stub
    )

    response = await async_client.patch(
        "/meta/modify/value", json=modify_value_payload()
    )

    assert response.status_code == 400
    assert response.json()["message"] == "invalid"


@pytest.mark.asyncio
async def test_modify_value_when_scheduling_fails_errors(
    async_client: AsyncClient,
    lock_stub: LockStub,
    validator_stub: ValidatorStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = ExecutorStub()
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "RequestValidation", lambda *_args, **_kwargs: validator_stub
    )
    monkeypatch.setattr(meta, "OperationExecutor", lambda *_args, **_kwargs: executor)
    monkeypatch.setattr(
        "starlette.background.BackgroundTasks.add_task",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TypeError()),
    )

    response = await async_client.patch(
        "/meta/modify/value", json=modify_value_payload()
    )

    assert response.status_code == 500
    assert "Internal server error" in response.json()["message"]


@pytest.mark.asyncio
async def test_delete_metadata_with_valid_request_succeeds(
    async_client: AsyncClient,
    lock_stub: LockStub,
    validator_stub: ValidatorStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = ExecutorStub()

    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "RequestValidation", lambda *_args, **_kwargs: validator_stub
    )
    monkeypatch.setattr(meta, "OperationExecutor", lambda *_args, **_kwargs: executor)

    response = await async_client.patch("/meta/delete", json=delete_payload())

    assert response.status_code == 202
    assert response.json()["data"]["operation_id"] == CURRENT_OPERATION_ID


@pytest.mark.parametrize(
    "lock_stub",
    [
        {"acquire_result": False, "holder": "other"},
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_delete_metadata_when_locked_returns_locked_error(
    async_client: AsyncClient, lock_stub: LockStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "OperationExecutor", lambda *_args, **_kwargs: ExecutorStub()
    )

    response = await async_client.patch("/meta/delete", json=delete_payload())

    assert response.status_code == 423
    assert "other" in response.json()["message"]


@pytest.mark.parametrize(
    "lock_stub",
    [
        {"acquire_exc": FileNotFoundError("invalid")},
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_delete_metadata_when_lock_raises_errors(
    async_client: AsyncClient, lock_stub: LockStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)

    response = await async_client.patch("/meta/delete", json=delete_payload())

    assert response.status_code == 500
    assert "Internal server error" in response.json()["message"]


@pytest.mark.parametrize(
    "validator_stub",
    [{"validate_exc": meta.RequestValidationError("invalid")}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_delete_metadata_when_validation_fails_errors(
    async_client: AsyncClient,
    lock_stub: LockStub,
    validator_stub: ValidatorStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "RequestValidation", lambda *_args, **_kwargs: validator_stub
    )

    response = await async_client.patch("/meta/delete", json=delete_payload())

    assert response.status_code == 400
    assert response.json()["message"] == "invalid"


@pytest.mark.asyncio
async def test_delete_metadata_when_scheduling_fails_errors(
    async_client: AsyncClient,
    lock_stub: LockStub,
    validator_stub: ValidatorStub,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = ExecutorStub()
    monkeypatch.setattr(meta, "LockManager", lambda *_args, **_kwargs: lock_stub)
    monkeypatch.setattr(
        meta, "RequestValidation", lambda *_args, **_kwargs: validator_stub
    )
    monkeypatch.setattr(meta, "OperationExecutor", lambda *_args, **_kwargs: executor)
    monkeypatch.setattr(
        "starlette.background.BackgroundTasks.add_task",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TypeError()),
    )

    response = await async_client.patch("/meta/delete", json=delete_payload())

    assert response.status_code == 500
    assert "Internal server error" in response.json()["message"]
