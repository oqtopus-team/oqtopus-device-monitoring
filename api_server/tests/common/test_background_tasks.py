from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest
from httpx import HTTPError
from pytest_mock import MockerFixture

from common.background_tasks import (
    OperationExecutor,
    OperationHistoryWriter,
)
from common.types.operation import OperationHistory, OperationSteps, OperationType
from common.types.victoria_metrics import MetricDataResponse
from schemas.meta import (
    AddLabelRequest,
    DeleteLabelRequest,
    ModifyLabelKeyRequest,
    ModifyLabelValueRequest,
    ProcessStatus,
    Selector,
    TimeRange,
)
from tests.conftest import CURRENT_OPERATION_ID


@pytest.fixture
def lock_manager(mocker: MockerFixture) -> Mock:
    return mocker.Mock(release_lock=mocker.Mock())


class StubHistoryWriter(OperationHistoryWriter):
    def __init__(self) -> None:
        self.records: list[tuple[str, OperationHistory]] = []

    def write_history(self, operation_id: str, history: OperationHistory) -> None:
        self.records.append((operation_id, history))


@pytest.fixture
def history_writer() -> OperationHistoryWriter:
    return StubHistoryWriter()


@pytest.fixture
def executor(
    client: AsyncMock, lock_manager: Mock, history_writer: OperationHistoryWriter
):
    return OperationExecutor(client, lock_manager, history_writer)


@pytest.fixture
def selector() -> Selector:
    return Selector(match=[])


def build_metrics_data(
    metric: dict[str, str], values: list[float | int | None], timestamps: list[int]
) -> MetricDataResponse:
    return MetricDataResponse(
        metric=dict(metric),
        values=list(values),
        timestamps=[t * 1000 for t in timestamps],
    )


@pytest.mark.asyncio
async def test_execute_add_label_with_valid_request_succeeds(
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    executor: OperationExecutor,
    selector: Selector,
) -> None:
    # Arrange
    request = AddLabelRequest(
        metric_name="cpu",
        new_label_key="env",
        default_value="prod",
        selector=selector,
    )
    exported = [build_metrics_data({"__name__": "cpu", "instance": "a"}, [1.0], [1])]
    client.read_timeseries.return_value = exported

    # Act
    await executor.execute_add_label(CURRENT_OPERATION_ID, request)

    # Assert
    client.read_timeseries.assert_awaited_once_with(
        metric_name="cpu", selector=selector
    )
    client.write_timeseries.assert_awaited_once()
    assert client.write_timeseries.await_args.kwargs["data"][0].metric["env"] == "prod"
    assert len(client.write_timeseries.await_args.kwargs["data"]) == 1
    client.delete_timeseries.assert_awaited_once_with(
        metric_name="cpu", selector=selector
    )
    assert lock_manager.release_lock.called
    assert len(history_writer.records) == 1
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.ADD_LABEL
    assert history.steps == OperationSteps.FINISHING
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
        OperationSteps.INGESTING,
        OperationSteps.DELETING,
        OperationSteps.FINISHING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
    ]


@pytest.mark.asyncio
async def test_execute_add_label_when_extracting_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = AddLabelRequest(
        metric_name="cpu",
        new_label_key="env",
        default_value="prod",
        selector=selector,
    )
    client.read_timeseries.side_effect = HTTPError("Exstracting failed")

    # Act
    await executor.execute_add_label(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.ADD_LABEL
    assert history.steps == OperationSteps.EXTRACTING
    assert history.error is not None
    assert history.error.step == OperationSteps.EXTRACTING
    assert history.error.cleanup_instructions in "Cleanup is not necessary."
    assert "Cleanup is not necessary." in history.error.cleanup_instructions
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [OperationSteps.EXTRACTING]
    assert [s.status for s in history.progress.steps] == [ProcessStatus.failed]


@pytest.mark.asyncio
async def test_execute_add_label_when_transforming_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = AddLabelRequest(
        metric_name="cpu",
        new_label_key="env",
        default_value="prod",
        selector=selector,
    )
    exported = [build_metrics_data({"__name__": "cpu", "env": "prod"}, [1.0], [1])]
    client.read_timeseries.return_value = exported

    # Act
    await executor.execute_add_label(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.ADD_LABEL
    assert history.steps == OperationSteps.TRANSFORMING
    assert history.error is not None
    assert history.error.step == OperationSteps.TRANSFORMING
    assert "Cleanup is not necessary." in history.error.cleanup_instructions
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.failed,
    ]


@pytest.mark.asyncio
async def test_execute_add_label_when_ingesting_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = AddLabelRequest(
        metric_name="cpu",
        new_label_key="env",
        default_value="prod",
        selector=selector,
    )
    # Arrange
    request = AddLabelRequest(
        metric_name="cpu",
        new_label_key="env",
        default_value="prod",
        selector=selector,
    )
    exported = [build_metrics_data({"__name__": "cpu"}, [1.0], [1])]
    client.read_timeseries.return_value = exported
    client.write_timeseries.side_effect = HTTPError("Ingesting failed")

    # Act
    await executor.execute_add_label(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.ADD_LABEL
    assert history.steps == OperationSteps.INGESTING
    assert history.error is not None
    assert history.error.step == OperationSteps.INGESTING
    assert (
        "Please delete ingested time-series data" in history.error.cleanup_instructions
    )
    assert "cpu" in history.error.cleanup_instructions
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
        OperationSteps.INGESTING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.failed,
    ]


@pytest.mark.asyncio
async def test_execute_add_label_when_deleting_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = AddLabelRequest(
        metric_name="cpu",
        new_label_key="env",
        default_value="prod",
        selector=selector,
    )
    exported = [build_metrics_data({"__name__": "cpu"}, [1.0], [1])]
    client.read_timeseries.return_value = exported
    client.delete_timeseries.side_effect = HTTPError("Delete failed")

    # Act
    await executor.execute_add_label(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.ADD_LABEL
    assert history.steps == OperationSteps.DELETING
    assert history.error is not None
    assert history.error.step == OperationSteps.DELETING
    assert (
        "Please delete original time-series data in VictoriaMetrics."
        in history.error.cleanup_instructions
    )
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
        OperationSteps.INGESTING,
        OperationSteps.DELETING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.failed,
    ]


@pytest.mark.asyncio
async def test_execute_modify_label_key_with_valid_request_succeeds(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelKeyRequest(
        metric_name="cpu",
        range=TimeRange(
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(9999, 12, 31, tzinfo=UTC),
        ),
        selector=selector,
        from_key="host",
        to_key="node",
    )
    exported = [
        build_metrics_data({"__name__": "cpu", "host": "a"}, [1.0, 2.0], [1, 2]),
    ]
    transformed = build_metrics_data(
        {"__name__": "cpu", "node": "a"}, [1.0, 2.0], [1, 2]
    )
    client.read_timeseries.return_value = exported

    # Act
    await executor.execute_modify_label_key(CURRENT_OPERATION_ID, request)

    # Assert
    client.read_timeseries.assert_awaited_once_with(
        metric_name="cpu", selector=selector
    )
    assert client.write_timeseries.await_args_list[0].kwargs["data"][0] == transformed
    assert client.write_timeseries.await_args_list[1].kwargs["data"] == []
    client.delete_timeseries.assert_awaited_once_with(
        metric_name="cpu", selector=selector
    )
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.MODIFY_KEY
    assert history.steps == OperationSteps.FINISHING
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
        OperationSteps.INGESTING,
        OperationSteps.DELETING,
        OperationSteps.FINISHING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
    ]


@pytest.mark.asyncio
async def test_execute_modify_label_key_when_extraction_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelKeyRequest(
        metric_name="cpu",
        range=TimeRange(
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        ),
        selector=selector,
        from_key="host",
        to_key="node",
    )
    client.read_timeseries.side_effect = HTTPError("Exstracting failed")

    # Act
    await executor.execute_modify_label_key(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.MODIFY_KEY
    assert history.steps == OperationSteps.EXTRACTING
    assert history.error is not None
    assert history.error.step == OperationSteps.EXTRACTING
    assert history.error.cleanup_instructions in "Cleanup is not necessary."
    assert "Cleanup is not necessary." in history.error.cleanup_instructions
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [OperationSteps.EXTRACTING]
    assert [s.status for s in history.progress.steps] == [ProcessStatus.failed]


@pytest.mark.asyncio
async def test_execute_modify_label_key_when_transforming_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelKeyRequest(
        metric_name="cpu",
        range=TimeRange(
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        ),
        selector=selector,
        from_key="host",
        to_key="node",
    )
    exported = [
        build_metrics_data({"__name__": "cpu", "node": "a"}, [1.0, 2.0], [1, 2]),
    ]
    client.read_timeseries.return_value = exported

    # Act
    await executor.execute_modify_label_key(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.MODIFY_KEY
    assert history.steps == OperationSteps.TRANSFORMING
    assert history.error is not None
    assert history.error.step == OperationSteps.TRANSFORMING
    assert "Cleanup is not necessary." in history.error.cleanup_instructions
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.failed,
    ]


@pytest.mark.asyncio
async def test_execute_modify_label_key_when_ingesting_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelKeyRequest(
        metric_name="cpu",
        range=TimeRange(
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        ),
        selector=selector,
        from_key="host",
        to_key="node",
    )
    exported = [
        build_metrics_data({"__name__": "cpu", "host": "a"}, [1.0, 2.0], [1, 2]),
    ]
    client.read_timeseries.return_value = exported
    client.write_timeseries.side_effect = HTTPError("Ingesting failed")

    # Act
    await executor.execute_modify_label_key(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.MODIFY_KEY
    assert history.steps == OperationSteps.INGESTING
    assert history.error is not None
    assert history.error.step == OperationSteps.INGESTING
    assert (
        "Please delete ingested time-series data" in history.error.cleanup_instructions
    )
    assert "cpu" in history.error.cleanup_instructions
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
        OperationSteps.INGESTING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.failed,
    ]


@pytest.mark.asyncio
async def test_execute_modify_label_key_when_deleting_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelKeyRequest(
        metric_name="cpu",
        range=TimeRange(
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        ),
        selector=selector,
        from_key="host",
        to_key="node",
    )
    exported = [
        build_metrics_data({"__name__": "cpu", "host": "a"}, [1.0, 2.0], [1, 2]),
    ]
    client.read_timeseries.return_value = exported
    client.delete_timeseries.side_effect = HTTPError("Delete failed")

    # Act
    await executor.execute_modify_label_key(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    client.write_timeseries.assert_awaited_once()
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.MODIFY_KEY
    assert history.steps == OperationSteps.DELETING
    assert history.error is not None
    assert history.error.step == OperationSteps.DELETING
    assert (
        "Please delete original time-series data in VictoriaMetrics."
        in history.error.cleanup_instructions
    )
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
        OperationSteps.INGESTING,
        OperationSteps.DELETING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.failed,
    ]


@pytest.mark.asyncio
async def test_execute_modify_label_value_with_valid_request_succeeds(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="cpu",
        range=TimeRange(
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        ),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    exported = [
        build_metrics_data({"__name__": "cpu", "env": "old"}, [1.0, 2.0], [1, 2]),
    ]
    transformed = build_metrics_data(
        {"__name__": "cpu", "env": "new"}, [1.0, 2.0], [1, 2]
    )
    client.read_timeseries.return_value = exported

    # Act
    await executor.execute_modify_label_value(CURRENT_OPERATION_ID, request)

    # Assert
    assert client.write_timeseries.await_args_list[0].kwargs["data"][0] == transformed
    assert client.write_timeseries.await_args_list[1].kwargs["data"] == []
    client.delete_timeseries.assert_awaited_once_with(
        metric_name="cpu", selector=selector
    )
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.MODIFY_VALUE
    assert history.steps == OperationSteps.FINISHING
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
        OperationSteps.INGESTING,
        OperationSteps.DELETING,
        OperationSteps.FINISHING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
    ]


@pytest.mark.asyncio
async def test_execute_modify_label_value_when_extracting_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="cpu",
        range=TimeRange(
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        ),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    client.read_timeseries.side_effect = HTTPError("Exstracting failed")

    # Act
    await executor.execute_modify_label_value(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.MODIFY_VALUE
    assert history.steps == OperationSteps.EXTRACTING
    assert history.error is not None
    assert history.error.step == OperationSteps.EXTRACTING
    assert history.error.cleanup_instructions in "Cleanup is not necessary."
    assert "Cleanup is not necessary." in history.error.cleanup_instructions
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [OperationSteps.EXTRACTING]
    assert [s.status for s in history.progress.steps] == [ProcessStatus.failed]


@pytest.mark.asyncio
async def test_execute_modify_label_value_when_transforming_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="cpu",
        range=TimeRange(
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        ),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    exported = [
        build_metrics_data({"__name__": "cpu", "env": "new"}, [1.0, 2.0], [1, 2]),
    ]
    client.read_timeseries.return_value = exported

    # Act
    await executor.execute_modify_label_value(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.MODIFY_VALUE
    assert history.steps == OperationSteps.TRANSFORMING
    assert history.error is not None
    assert history.error.step == OperationSteps.TRANSFORMING
    assert history.error is not None
    assert history.error.step == OperationSteps.TRANSFORMING
    assert "Cleanup is not necessary." in history.error.cleanup_instructions
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.failed,
    ]


@pytest.mark.asyncio
async def test_execute_modify_label_value_when_ingesting_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="cpu",
        range=TimeRange(
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        ),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    exported = [
        build_metrics_data({"__name__": "cpu", "env": "old"}, [1.0, 2.0], [1, 2]),
    ]
    client.read_timeseries.return_value = exported
    client.write_timeseries.side_effect = HTTPError("Ingesting failed")

    # Act
    await executor.execute_modify_label_value(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.MODIFY_VALUE
    assert history.steps == OperationSteps.INGESTING
    assert history.error is not None
    assert history.error.step == OperationSteps.INGESTING
    assert (
        "Please delete ingested time-series data" in history.error.cleanup_instructions
    )
    assert "cpu" in history.error.cleanup_instructions
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
        OperationSteps.INGESTING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.failed,
    ]


@pytest.mark.asyncio
async def test_execute_modify_label_value_when_deleting_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="cpu",
        range=TimeRange(
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        ),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    exported = [
        build_metrics_data(
            {"__name__": "cpu", "env": "old"},
            [1.0, 2.0, 3.0],
            [1, 2, 3],
        ),
    ]
    client.read_timeseries.return_value = exported
    client.delete_timeseries.side_effect = HTTPError("Delete failed")

    # Act
    await executor.execute_modify_label_value(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    client.write_timeseries.assert_awaited_once()
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.MODIFY_VALUE
    assert history.steps == OperationSteps.DELETING
    assert history.error is not None
    assert history.error.step == OperationSteps.DELETING
    assert (
        "Please delete original time-series data in VictoriaMetrics."
        in history.error.cleanup_instructions
    )
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
        OperationSteps.INGESTING,
        OperationSteps.DELETING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.failed,
    ]


@pytest.mark.asyncio
async def test_execute_delete_label_with_valid_request_succeeds(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = DeleteLabelRequest(
        metric_name="cpu", label_keys=["env"], selector=selector
    )
    exported = [build_metrics_data({"__name__": "cpu", "env": "prod"}, [1.0], [1])]
    client.read_timeseries.return_value = exported

    # Act
    await executor.execute_delete_label(CURRENT_OPERATION_ID, request)

    # Assert
    client.read_timeseries.assert_awaited_once_with(
        metric_name="cpu", selector=selector
    )
    client.write_timeseries.assert_awaited_once()
    assert client.write_timeseries.await_args.kwargs["data"][0].metric == {
        "__name__": "cpu"
    }
    assert len(client.write_timeseries.await_args.kwargs["data"]) == 1
    client.delete_timeseries.assert_awaited_once_with(
        metric_name="cpu", selector=selector
    )
    assert lock_manager.release_lock.called
    assert len(history_writer.records) == 1
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.DELETE_LABEL
    assert history.steps == OperationSteps.FINISHING
    assert history.error is None
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
        OperationSteps.INGESTING,
        OperationSteps.DELETING,
        OperationSteps.FINISHING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
    ]


@pytest.mark.asyncio
async def test_execute_delete_label_when_extracting_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = DeleteLabelRequest(
        metric_name="cpu", label_keys=["env"], selector=selector
    )
    client.read_timeseries.side_effect = HTTPError("Extracting failed")

    # Act
    await executor.execute_delete_label(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.DELETE_LABEL
    assert history.steps == OperationSteps.EXTRACTING
    assert history.error is not None
    assert history.error.step == OperationSteps.EXTRACTING
    assert history.error.cleanup_instructions in "Cleanup is not necessary."
    assert "Cleanup is not necessary." in history.error.cleanup_instructions
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [OperationSteps.EXTRACTING]
    assert [s.status for s in history.progress.steps] == [ProcessStatus.failed]


@pytest.mark.asyncio
async def test_execute_delete_label_when_transforming_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = DeleteLabelRequest(
        metric_name="cpu", label_keys=["env"], selector=selector
    )
    exported = [build_metrics_data({"__name__": "cpu"}, [1.0], [1])]
    client.read_timeseries.return_value = exported

    # Act
    await executor.execute_delete_label(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.DELETE_LABEL
    assert history.steps == OperationSteps.TRANSFORMING
    assert history.error is not None
    assert history.error.step == OperationSteps.TRANSFORMING
    assert "Cleanup is not necessary." in history.error.cleanup_instructions
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.failed,
    ]


@pytest.mark.asyncio
async def test_execute_delete_label_when_ingesting_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = DeleteLabelRequest(
        metric_name="cpu", label_keys=["env"], selector=selector
    )
    exported = [build_metrics_data({"__name__": "cpu", "env": "prod"}, [1.0], [1])]
    client.read_timeseries.return_value = exported
    client.write_timeseries.side_effect = HTTPError("Ingesting failed")

    # Act
    await executor.execute_delete_label(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.DELETE_LABEL
    assert history.steps == OperationSteps.INGESTING
    assert history.error is not None
    assert history.error.step == OperationSteps.INGESTING
    assert (
        "Please delete ingested time-series data" in history.error.cleanup_instructions
    )
    assert "cpu" in history.error.cleanup_instructions
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
        OperationSteps.INGESTING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.failed,
    ]


@pytest.mark.asyncio
async def test_execute_delete_label_when_deleting_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    request = DeleteLabelRequest(
        metric_name="cpu", label_keys=["env"], selector=selector
    )
    exported = [build_metrics_data({"__name__": "cpu", "env": "prod"}, [1.0], [1])]
    client.read_timeseries.return_value = exported
    client.delete_timeseries.side_effect = HTTPError("Delete failed")

    # Act
    await executor.execute_delete_label(CURRENT_OPERATION_ID, request)

    # Assert
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.steps == OperationSteps.DELETING
    assert history.error is not None
    assert history.error.step == OperationSteps.DELETING
    assert (
        "Please delete original time-series data in VictoriaMetrics."
        in history.error.cleanup_instructions
    )
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.EXTRACTING,
        OperationSteps.TRANSFORMING,
        OperationSteps.INGESTING,
        OperationSteps.DELETING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.completed,
        ProcessStatus.failed,
    ]


@pytest.mark.asyncio
async def test_execute_delete_time_series_with_valid_request_succeeds(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    # Act
    await executor.execute_delete_time_series(
        CURRENT_OPERATION_ID, metric_name="cpu", selector=selector
    )

    # Assert
    client.delete_timeseries.assert_awaited_once_with(
        metric_name="cpu", selector=selector
    )
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.DELETE_TIME_SERIES
    assert history.steps == OperationSteps.FINISHING
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.DELETING,
        OperationSteps.FINISHING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.completed,
        ProcessStatus.completed,
    ]


@pytest.mark.asyncio
async def test_execute_delete_time_series_when_deleting_fails_errors(
    executor: OperationExecutor,
    client: AsyncMock,
    history_writer: StubHistoryWriter,
    lock_manager: Mock,
    selector: Selector,
):
    # Arrange
    client.delete_timeseries.side_effect = HTTPError("Delete failed")

    # Act
    await executor.execute_delete_time_series(
        CURRENT_OPERATION_ID, metric_name="cpu", selector=selector
    )

    # Assert
    assert lock_manager.release_lock.called
    _, history = history_writer.records[0]
    assert history.operation_type == OperationType.DELETE_TIME_SERIES
    assert history.steps == OperationSteps.DELETING
    assert history.error is not None
    assert history.error.step == OperationSteps.DELETING
    assert (
        "Please delete original time-series data in VictoriaMetrics."
        in history.error.cleanup_instructions
    )
    assert history.progress is not None
    assert [s.name for s in history.progress.steps] == [
        OperationSteps.DELETING,
    ]
    assert [s.status for s in history.progress.steps] == [
        ProcessStatus.failed,
    ]


def test_split_exported_data_by_time_range_when_start_and_end_is_none_returns_original_data(
    executor: OperationExecutor,
):
    # Arrange
    data = [
        build_metrics_data(
            {"__name__": "cpu"},  # metric
            [1.0, 2.0, 3.0],  # values
            [1, 2, 3],  # timestamps
        )
    ]

    # Act
    in_range_data, out_of_range_data = executor._split_exported_data_by_time_range(
        data, None, None
    )

    # Assert
    assert in_range_data == data
    assert out_of_range_data == []


def test_split_exported_data_by_time_range_splits_data_correctly(
    executor: OperationExecutor,
):
    # Arrange
    data = [
        build_metrics_data(
            {"__name__": "cpu"},  # metric
            [1.0, 2.0, 3.0, 4.0, 5.0],  # values
            [1, 2, 3, 4, 5],  # timestamps
        )
    ]
    start = datetime.fromtimestamp(2, tz=UTC)
    end = datetime.fromtimestamp(4, tz=UTC)

    # Act
    in_range_data, out_of_range_data = executor._split_exported_data_by_time_range(
        data, start, end
    )

    # Assert
    assert in_range_data == [
        build_metrics_data(
            {"__name__": "cpu"},  # metric
            [2.0, 3.0, 4.0],  # values
            [2, 3, 4],  # timestamps
        )
    ]
    assert out_of_range_data == [
        build_metrics_data(
            {"__name__": "cpu"},  # metric
            [1.0, 5.0],  # values
            [1, 5],  # timestamps
        )
    ]
