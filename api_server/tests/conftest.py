import datetime
import pathlib
from unittest.mock import AsyncMock

import pytest
from pytest_mock import MockerFixture

from common.types.config import AppConfig, LoggingConfig, ServerConfig
from common.types.operation import (
    OperationError,
    OperationHistory,
    OperationSteps,
    OperationType,
    StepInfo,
    Steps,
)
from schemas.meta import ProcessStatus

PREVIOUS_OPERATION_ID = "12345678T123456_1"
CURRENT_OPERATION_ID = "12345678T123456_2"


@pytest.fixture
def app_config(
    tmp_path: pathlib.Path,
) -> AppConfig:
    return AppConfig(
        server=ServerConfig(timezone="UTC", host="127.0.0.1", port=8080),
        log=LoggingConfig(logging_config_path="logging.yaml", logging_dir_path="logs"),
        victoria_metrics_url="http://localhost",
        lock_timeout_hours=1,
        operation_history_path=str(tmp_path),
    )


@pytest.fixture
def client(mocker: MockerFixture) -> AsyncMock:
    client = mocker.AsyncMock()
    client.get_metric_names = mocker.AsyncMock()
    client.read_timeseries = mocker.AsyncMock()
    client.write_timeseries = mocker.AsyncMock()
    client.delete_timeseries = mocker.AsyncMock()
    client.get_series_labels = mocker.AsyncMock()
    client.count_over_time = mocker.AsyncMock()
    return client


def build_history(
    start_at: datetime.datetime,
    progress: Steps | None,
    error: OperationError | None,
    steps: OperationSteps = OperationSteps.EXTRACTING,
    end_at: datetime.datetime | None = None,
) -> OperationHistory:
    return OperationHistory(
        operation_id=CURRENT_OPERATION_ID,
        operation_type=OperationType.ADD_LABEL,
        steps=steps,
        start_at=start_at,
        end_at=end_at,
        request={"action": "test"},
        progress=progress,
        error=error,
    )


def build_finished_history(
    start_at: datetime.datetime | None = None,
) -> OperationHistory:
    start_at = start_at or datetime.datetime.now(datetime.UTC) - datetime.timedelta(
        minutes=59
    )

    progress = Steps(
        steps=[
            StepInfo(
                name=OperationSteps.EXTRACTING,
                status=ProcessStatus.completed,
                start_time=start_at,
                completed_at=start_at + datetime.timedelta(minutes=1),
            ),
            StepInfo(
                name=OperationSteps.TRANSFORMING,
                status=ProcessStatus.completed,
                start_time=start_at + datetime.timedelta(minutes=2),
                completed_at=start_at + datetime.timedelta(minutes=3),
            ),
            StepInfo(
                name=OperationSteps.INGESTING,
                status=ProcessStatus.completed,
                start_time=start_at + datetime.timedelta(minutes=4),
                completed_at=start_at + datetime.timedelta(minutes=5),
            ),
            StepInfo(
                name=OperationSteps.DELETING,
                status=ProcessStatus.completed,
                start_time=start_at + datetime.timedelta(minutes=6),
                completed_at=start_at + datetime.timedelta(minutes=7),
            ),
            StepInfo(
                name=OperationSteps.FINISHING,
                status=ProcessStatus.completed,
                start_time=start_at + datetime.timedelta(minutes=8),
                completed_at=start_at + datetime.timedelta(minutes=9),
            ),
        ]
    )

    return build_history(
        start_at=start_at,
        progress=progress,
        error=None,
        end_at=start_at + datetime.timedelta(minutes=20),
    )


def build_timeout_history(lock_timeout_hours: int) -> OperationHistory:
    start_at = datetime.datetime.now(datetime.UTC) - datetime.timedelta(
        hours=lock_timeout_hours + 1
    )
    progress = Steps(
        steps=[
            StepInfo(
                name=OperationSteps.EXTRACTING,
                status=ProcessStatus.completed,
                start_time=start_at,
                completed_at=start_at + datetime.timedelta(minutes=1),
            ),
            StepInfo(
                name=OperationSteps.TRANSFORMING,
                status=ProcessStatus.in_progress,
                start_time=start_at + datetime.timedelta(minutes=2),
                completed_at=start_at + datetime.timedelta(minutes=3),
            ),
        ]
    )
    return build_history(
        start_at=start_at,
        progress=progress,
        error=None,
        end_at=start_at + datetime.timedelta(minutes=20),
    )


def build_in_progress_history() -> OperationHistory:
    start_at = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=59)

    progress = Steps(
        steps=[
            StepInfo(
                name=OperationSteps.EXTRACTING,
                status=ProcessStatus.completed,
                start_time=start_at,
                completed_at=start_at + datetime.timedelta(minutes=1),
            ),
            StepInfo(
                name=OperationSteps.TRANSFORMING,
                status=ProcessStatus.completed,
                start_time=start_at + datetime.timedelta(minutes=2),
                completed_at=start_at + datetime.timedelta(minutes=3),
            ),
            StepInfo(
                name=OperationSteps.INGESTING,
                status=ProcessStatus.completed,
                start_time=start_at + datetime.timedelta(minutes=4),
                completed_at=start_at + datetime.timedelta(minutes=5),
            ),
            StepInfo(
                name=OperationSteps.DELETING,
                status=ProcessStatus.in_progress,
                start_time=start_at + datetime.timedelta(minutes=6),
                completed_at=start_at + datetime.timedelta(minutes=7),
            ),
        ]
    )
    return build_history(
        start_at=start_at,
        progress=progress,
        error=None,
        end_at=start_at + datetime.timedelta(minutes=20),
    )


def build_started_history() -> OperationHistory:
    start_at = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=59)

    return build_history(
        start_at=start_at,
        progress=None,
        error=None,
    )


def build_error_history() -> OperationHistory:
    start_at = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=59)

    progress = Steps(
        steps=[
            StepInfo(
                name=OperationSteps.EXTRACTING,
                status=ProcessStatus.completed,
                start_time=start_at,
                completed_at=start_at + datetime.timedelta(minutes=1),
            ),
            StepInfo(
                name=OperationSteps.TRANSFORMING,
                status=ProcessStatus.completed,
                start_time=start_at + datetime.timedelta(minutes=2),
                completed_at=start_at + datetime.timedelta(minutes=3),
            ),
            StepInfo(
                name=OperationSteps.INGESTING,
                status=ProcessStatus.failed,
                start_time=start_at + datetime.timedelta(minutes=4),
                completed_at=start_at + datetime.timedelta(minutes=5),
            ),
        ]
    )

    error = OperationError(
        step=OperationSteps.INGESTING,
        message="Ingestion failed.",
        cleanup_instructions="Please delete ingested time-series data.",
    )

    return build_history(
        start_at=start_at,
        progress=progress,
        error=error,
        end_at=start_at + datetime.timedelta(minutes=20),
    )
