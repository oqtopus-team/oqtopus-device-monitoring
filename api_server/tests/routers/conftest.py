from datetime import datetime
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient

from common.types.config import AppConfig
from common.types.operation import (
    OperationHistory,
)
from routers import meta, metrics
from schemas.errors import ErrorResponse
from schemas.meta import (
    AddLabelRequest,
    DeleteLabelRequest,
    ModifyLabelKeyRequest,
    ModifyLabelValueRequest,
    Selector,
)


class LockStub:
    def __init__(
        self,
        holder: str = "holder",
        acquire_exc: Exception | None = None,
        *,
        acquire_result: bool = True,
    ) -> None:
        self.acquire_result = acquire_result
        self.holder = holder
        self.acquire_exc = acquire_exc
        self.released = False

    def acquire_lock(self, _: str) -> bool:
        if self.acquire_exc is not None:
            raise self.acquire_exc
        return self.acquire_result

    def get_lock_holder(self) -> str:
        return self.holder

    def release_lock(self) -> None:
        self.released = True


class OperationHistoryWriterStub:
    def __init__(
        self,
        history: OperationHistory | None = None,
        exc: Exception | None = None,
    ) -> None:
        self.history = history
        self.exc = exc

    def read_history(self, _: str) -> OperationHistory:
        if self.exc is not None:
            raise self.exc
        if self.history is None:
            raise FileNotFoundError
        return self.history


class ValidatorStub:
    def __init__(self, validate_exc: Exception | None = None) -> None:
        self.validate_exc = validate_exc

    async def validate_get_time_series_data(
        self,
        metric_name: str,
        selector: Selector,
        start: datetime | None,
        end: datetime | None,
    ) -> None:
        _ = metric_name
        _ = selector
        _ = start
        _ = end
        if self.validate_exc is not None:
            raise self.validate_exc

    async def validate_add_label(self, request: AddLabelRequest) -> None:
        _ = request
        if self.validate_exc is not None:
            raise self.validate_exc

    async def validate_modify_label_key(self, request: ModifyLabelKeyRequest) -> None:
        _ = request
        if self.validate_exc is not None:
            raise self.validate_exc

    async def validate_modify_label_value(
        self, request: ModifyLabelValueRequest
    ) -> None:
        _ = request
        if self.validate_exc is not None:
            raise self.validate_exc

    async def validate_delete_label(self, request: DeleteLabelRequest) -> None:
        _ = request
        if self.validate_exc is not None:
            raise self.validate_exc

    async def validate_delete_time_series(
        self, metric_name: str, selector: Selector
    ) -> None:
        _ = metric_name
        _ = selector
        if self.validate_exc is not None:
            raise self.validate_exc


class ExecutorStub:
    def __init__(self) -> None:
        pass

    async def execute_add_label(self, operation_id: str, request: AddLabelRequest):
        _ = operation_id
        _ = request

    async def execute_modify_label_key(
        self, operation_id: str, request: ModifyLabelKeyRequest
    ):
        _ = operation_id
        _ = request

    async def execute_modify_label_value(
        self, operation_id: str, request: ModifyLabelValueRequest
    ):
        _ = operation_id
        _ = request

    async def execute_delete_label(
        self, operation_id: str, request: DeleteLabelRequest
    ):
        _ = operation_id
        _ = request

    async def execute_delete_time_series(
        self, operation_id: str, metric_name: str, selector: Selector
    ) -> None:
        _ = operation_id
        _ = metric_name
        _ = selector


@pytest.fixture
def lock_stub(request: pytest.FixtureRequest) -> LockStub:
    params = getattr(request, "param", {}) or {}
    return LockStub(**params)


@pytest.fixture
def validator_stub(request: pytest.FixtureRequest) -> ValidatorStub:
    params = getattr(request, "param", {}) or {}
    return ValidatorStub(**params)


@pytest.fixture
def app(app_config: AppConfig, client: AsyncMock) -> FastAPI:
    app = FastAPI()
    app.include_router(meta.router)
    app.include_router(metrics.router)

    # add handlers
    @app.exception_handler(RequestValidationError)
    def validation_exception_handler(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        """Handle request validation errors and return a structured JSON response.

        Args:
            _: The incoming HTTP request.
            exc: The validation error exception.

        Returns:
            JSONResponse: A JSON response with status code 400 and error message.

        """
        return ErrorResponse(
            status_code=400,
            content={"message": str(exc)},
        )

    app.state.config = app_config

    app.dependency_overrides[meta.get_config] = lambda: app_config
    app.dependency_overrides[meta.get_client] = lambda: client
    app.dependency_overrides[metrics.get_config] = lambda: app_config
    app.dependency_overrides[metrics.get_client] = lambda: client
    return app


@pytest_asyncio.fixture
async def async_client(app: FastAPI):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
