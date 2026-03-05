import datetime
import json
import logging
import pathlib
from zoneinfo import ZoneInfo

import pytest

from common.operations import (
    LOCK_FILE_NAME,
    OPERATIONS_DIRECTORY_NAME,
    LockManager,
    OperationHistoryWriter,
)
from common.types.config import AppConfig
from common.types.operation import (
    OperationError,
    OperationSteps,
    OperationType,
    StepInfo,
)
from schemas.meta import ProcessStatus
from tests.conftest import (
    CURRENT_OPERATION_ID,
    PREVIOUS_OPERATION_ID,
    build_finished_history,
    build_history,
    build_in_progress_history,
    build_started_history,
    build_timeout_history,
)


class TestLockManager:
    def test_acquire_lock_when_unlocked_creates_lock_file(
        self, tmp_path: pathlib.Path, app_config: AppConfig
    ) -> None:
        # Arrange
        manager = LockManager(str(tmp_path), app_config)
        lock_file_path = tmp_path / LOCK_FILE_NAME

        # Act
        result = manager.acquire_lock(CURRENT_OPERATION_ID)

        # Assert
        assert result is True
        assert lock_file_path.read_text() == CURRENT_OPERATION_ID

    def test_acquire_lock_when_stale_lock_exists_succeeds(
        self,
        tmp_path: pathlib.Path,
        app_config: AppConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Arrange
        exiting_operation_id = PREVIOUS_OPERATION_ID
        manager = LockManager(str(tmp_path), app_config)
        lock_file_path = tmp_path / LOCK_FILE_NAME
        monkeypatch.setattr(manager, "_is_locked", lambda: True)
        monkeypatch.setattr(manager, "_is_stale_lock", lambda _: True)
        monkeypatch.setattr(manager, "get_lock_holder", lambda: exiting_operation_id)
        # Create existing lock file
        lock_file_path.write_text(exiting_operation_id)

        # Act
        result = manager.acquire_lock(CURRENT_OPERATION_ID)

        # Assert
        assert result is True
        assert lock_file_path.read_text() == CURRENT_OPERATION_ID

    def test_acquire_lock_when_active_lock_exists_fails(
        self,
        tmp_path: pathlib.Path,
        app_config: AppConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Arrange
        manager = LockManager(str(tmp_path), app_config)
        lock_file_path = tmp_path / LOCK_FILE_NAME
        monkeypatch.setattr(manager, "_is_locked", lambda: True)
        monkeypatch.setattr(manager, "_is_stale_lock", lambda _: False)
        monkeypatch.setattr(manager, "get_lock_holder", lambda: PREVIOUS_OPERATION_ID)
        # Create existing lock file
        lock_file_path.write_text(PREVIOUS_OPERATION_ID)

        # Act
        result = manager.acquire_lock(CURRENT_OPERATION_ID)

        # Assert
        assert result is False
        assert lock_file_path.read_text() == PREVIOUS_OPERATION_ID

    def test_acquire_lock_when_lock_holder_raises_errors(
        self,
        tmp_path: pathlib.Path,
        app_config: AppConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Arrange
        def raise_io_error() -> bool:
            raise FileNotFoundError

        manager = LockManager(str(tmp_path), app_config)
        lock_file_path = tmp_path / LOCK_FILE_NAME
        monkeypatch.setattr(manager, "_is_locked", lambda: True)
        monkeypatch.setattr(manager, "get_lock_holder", raise_io_error)
        # Create existing lock file
        lock_file_path.write_text(PREVIOUS_OPERATION_ID)

        # Act & Assert
        with pytest.raises(FileNotFoundError):
            manager.acquire_lock(CURRENT_OPERATION_ID)
        assert lock_file_path.read_text() == PREVIOUS_OPERATION_ID

    def test_get_lock_holder_with_existing_file_returns_holder(
        self, tmp_path: pathlib.Path, app_config: AppConfig
    ) -> None:
        # Arrange
        manager = LockManager(str(tmp_path), app_config)
        lock_file_path = tmp_path / LOCK_FILE_NAME
        lock_file_path.write_text(CURRENT_OPERATION_ID)

        # Act
        result = manager.get_lock_holder()

        # Assert
        assert result == CURRENT_OPERATION_ID

    def test_get_lock_holder_when_file_missing_errors(
        self, tmp_path: pathlib.Path, app_config: AppConfig
    ) -> None:
        # Arrange
        manager = LockManager(str(tmp_path), app_config)

        # Act & Assert
        with pytest.raises(FileNotFoundError):
            manager.get_lock_holder()

    def test_release_lock_with_existing_file_deletes_lock(
        self, tmp_path: pathlib.Path, app_config: AppConfig
    ) -> None:
        # Arrange
        manager = LockManager(str(tmp_path), app_config)
        lock_file_path = tmp_path / LOCK_FILE_NAME
        lock_file_path.write_text(CURRENT_OPERATION_ID)

        # Act
        manager.release_lock()

        # Assert
        assert not lock_file_path.exists()

    def test_release_lock_when_unlink_fails_errors(
        self,
        tmp_path: pathlib.Path,
        app_config: AppConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Arrange
        def raise_io_error(_: pathlib.Path) -> bool:
            raise FileNotFoundError

        manager = LockManager(str(tmp_path), app_config)
        monkeypatch.setattr(pathlib.Path, "unlink", raise_io_error)

        # Act & Assert
        with pytest.raises(FileNotFoundError):
            manager.release_lock()

    # These tests use acquire_lock to test _is_stale_lock logic
    def test_acquire_lock_when_previous_process_failed_returns_true(
        self,
        tmp_path: pathlib.Path,
        app_config: AppConfig,
    ) -> None:
        """Test _is_stale_lock method using aquire_lock method"""
        # Arrange
        manager = LockManager(str(tmp_path), app_config)
        start_at = datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.UTC)
        error = OperationError(
            step=OperationSteps.EXTRACTING,
            message="failed",
            cleanup_instructions="clean",
        )
        # Create previous operation history with error
        history = build_history(
            start_at=start_at,
            progress=None,
            error=error,
            end_at=start_at,
        )
        history_path = (
            tmp_path / OPERATIONS_DIRECTORY_NAME / f"{PREVIOUS_OPERATION_ID}.json"
        )
        history_path.parent.mkdir(parents=True, exist_ok=True)
        history_path.write_text(
            json.dumps(history.model_dump(mode="json")), encoding="utf-8"
        )
        # Create lock file for previous operation
        lock_file_path = tmp_path / LOCK_FILE_NAME
        lock_file_path.write_text(PREVIOUS_OPERATION_ID)

        # Act
        result = manager.acquire_lock(CURRENT_OPERATION_ID)

        # Assert
        assert result is True
        assert lock_file_path.read_text() == CURRENT_OPERATION_ID

    def test_acquire_lock_when_previous_not_started_returns_false(
        self, tmp_path: pathlib.Path, app_config: AppConfig
    ) -> None:
        """Test _is_stale_lock method using aquire_lock method"""
        # Arrange
        manager = LockManager(str(tmp_path), app_config)
        # Create previous operation history without start_at
        history = build_started_history()
        history_path = (
            tmp_path / OPERATIONS_DIRECTORY_NAME / f"{PREVIOUS_OPERATION_ID}.json"
        )
        history_path.parent.mkdir(parents=True, exist_ok=True)
        history_path.write_text(
            json.dumps(history.model_dump(mode="json")), encoding="utf-8"
        )
        # Create lock file for previous operation
        lock_file_path = tmp_path / LOCK_FILE_NAME
        lock_file_path.write_text(PREVIOUS_OPERATION_ID)

        # Act
        result = manager.acquire_lock(CURRENT_OPERATION_ID)

        # Assert
        assert result is False
        assert lock_file_path.read_text() == PREVIOUS_OPERATION_ID

    def test_acquire_lock_when_previous_timeout_returns_true(
        self, tmp_path: pathlib.Path, app_config: AppConfig
    ) -> None:
        """Test _is_stale_lock method using aquire_lock method"""
        # Arrange
        manager = LockManager(str(tmp_path), app_config)
        # Create previous operation history that has timed out
        history = build_timeout_history(
            lock_timeout_hours=app_config.lock_timeout_hours
        )
        history_path = (
            tmp_path / OPERATIONS_DIRECTORY_NAME / f"{PREVIOUS_OPERATION_ID}.json"
        )
        history_path.parent.mkdir(parents=True, exist_ok=True)
        history_path.write_text(
            json.dumps(history.model_dump(mode="json")), encoding="utf-8"
        )
        # Create lock file for previous operation
        lock_file_path = tmp_path / LOCK_FILE_NAME
        lock_file_path.write_text(PREVIOUS_OPERATION_ID)

        # Act
        result = manager.acquire_lock(CURRENT_OPERATION_ID)

        # Assert
        assert result is True
        assert lock_file_path.read_text() == CURRENT_OPERATION_ID

    def test_acquire_lock_when_previous_finished_returns_true(
        self, tmp_path: pathlib.Path, app_config: AppConfig
    ) -> None:
        """Test _is_stale_lock method using aquire_lock method"""
        # Arrange
        manager = LockManager(str(tmp_path), app_config)
        # Create previous operation history that has finished
        history = build_finished_history()
        history_path = (
            tmp_path / OPERATIONS_DIRECTORY_NAME / f"{PREVIOUS_OPERATION_ID}.json"
        )
        history_path.parent.mkdir(parents=True, exist_ok=True)
        history_path.write_text(
            json.dumps(history.model_dump(mode="json")), encoding="utf-8"
        )
        # Create lock file for previous operation
        lock_file_path = tmp_path / LOCK_FILE_NAME
        lock_file_path.write_text(PREVIOUS_OPERATION_ID)

        # Act
        result = manager.acquire_lock(CURRENT_OPERATION_ID)

        # Assert
        assert result is True
        assert lock_file_path.read_text() == CURRENT_OPERATION_ID

    def test_acquire_lock_when_previous_in_progress_returns_false(
        self, tmp_path: pathlib.Path, app_config: AppConfig
    ) -> None:
        """Test _is_stale_lock method using aquire_lock method"""
        # Arrange
        manager = LockManager(str(tmp_path), app_config)
        # Create previous operation history that is in progress
        history = build_in_progress_history()
        history_path = (
            tmp_path / OPERATIONS_DIRECTORY_NAME / f"{PREVIOUS_OPERATION_ID}.json"
        )
        history_path.parent.mkdir(parents=True, exist_ok=True)
        history_path.write_text(
            json.dumps(history.model_dump(mode="json")), encoding="utf-8"
        )
        # Create lock file for previous operation
        lock_file_path = tmp_path / LOCK_FILE_NAME
        lock_file_path.write_text(PREVIOUS_OPERATION_ID)

        # Act
        result = manager.acquire_lock(CURRENT_OPERATION_ID)

        # Assert
        assert result is False
        assert lock_file_path.read_text() == PREVIOUS_OPERATION_ID


class TestOperationHistoryWriter:
    def test_operation_history_writer_on_init_creates_directory(
        self,
        tmp_path: pathlib.Path,
    ) -> None:
        # Arrange
        operations_dir = tmp_path / OPERATIONS_DIRECTORY_NAME

        # Act
        OperationHistoryWriter(str(tmp_path), "UTC")

        # Assert
        assert operations_dir.exists()
        assert operations_dir.is_dir()

    def test_operation_history_writer_with_invalid_timezone_warns_and_uses_utc(
        self, tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Arrange
        operations_dir = tmp_path / OPERATIONS_DIRECTORY_NAME
        invalid_timezone = "Invalid/Timezone"

        # Act
        with caplog.at_level(logging.WARNING, logger="api-server.operations"):
            OperationHistoryWriter(str(tmp_path), invalid_timezone)

        # Assert
        assert operations_dir.exists()
        assert operations_dir.is_dir()
        # Check warning log is generated
        assert any(
            rec.levelno == logging.WARNING
            and "Timezone 'Invalid/Timezone' not found. Defaulting to UTC."
            in rec.message
            for rec in caplog.records
        )

    def test_write_history_with_valid_data_succeeds(
        self, tmp_path: pathlib.Path
    ) -> None:
        # Arrange
        writer = OperationHistoryWriter(str(tmp_path), "UTC")
        start_at = datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.UTC)
        history = build_finished_history(start_at=start_at)

        # Act
        writer.write_history(CURRENT_OPERATION_ID, history)

        # Assert
        history_file = (
            tmp_path / OPERATIONS_DIRECTORY_NAME / f"{CURRENT_OPERATION_ID}.json"
        )
        assert history_file.exists()
        with history_file.open("r", encoding="utf-8") as f:
            content = json.load(f)
        assert content["operation_id"] == CURRENT_OPERATION_ID
        assert content["operation_type"] == OperationType.ADD_LABEL
        assert content["start_at"] == start_at.strftime("%Y-%m-%dT%H:%M:%S%z")

    def test_write_history_when_write_fails_errors(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Arrange
        def raise_io_error(_data: str, _: str) -> None:
            raise PermissionError

        writer = OperationHistoryWriter(str(tmp_path), "UTC")
        start_at = datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.UTC)
        history = build_history(
            start_at=start_at,
            progress=None,
            error=None,
        )
        monkeypatch.setattr(pathlib.Path, "write_text", raise_io_error)

        # Act & Assert
        with pytest.raises(PermissionError):
            writer.write_history(CURRENT_OPERATION_ID, history)

    def test_stepinfo_serialize_datetime_with_context_applies_timezone(self) -> None:
        # Arrange
        start_time = datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
        completed_at = datetime.datetime(1970, 12, 31, 0, 0, 0, tzinfo=datetime.UTC)
        step = StepInfo(
            name=OperationSteps.EXTRACTING,
            status=ProcessStatus.completed,
            start_time=start_time,
            completed_at=completed_at,
        )

        # Act
        serialized = step.model_dump(context=ZoneInfo("Asia/Tokyo"), mode="json")

        # Assert
        assert serialized["start_time"] == "1970-01-01T09:00:00+0900"
        assert serialized["completed_at"] == "1970-12-31T09:00:00+0900"

    def test_stepinfo_serialize_datetime_with_none_completed_at_uses_empty_string(
        self,
    ) -> None:
        # Arrange
        start_time = datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.UTC)
        step = StepInfo(
            name=OperationSteps.EXTRACTING,
            status=ProcessStatus.in_progress,
            start_time=start_time,
            completed_at=None,
        )

        # Act
        serialized = step.model_dump(context=ZoneInfo("UTC"), mode="json")

        # Assert
        assert serialized["start_time"] == "1970-01-01T00:00:00+0000"
        assert serialized["completed_at"] == ""

    def test_stepinfo_serialize_datetime_with_invalid_context_falls_back_to_utc(
        self,
    ) -> None:
        # Arrange
        start_time = datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.UTC)
        completed_at = datetime.datetime(1970, 12, 31, 0, 0, tzinfo=datetime.UTC)
        step = StepInfo(
            name=OperationSteps.EXTRACTING,
            status=ProcessStatus.completed,
            start_time=start_time,
            completed_at=completed_at,
        )

        # Act
        serialized = step.model_dump(context="Invalid/Timezone", mode="json")

        # Assert
        assert serialized["start_time"] == "1970-01-01T00:00:00+0000"
        assert serialized["completed_at"] == "1970-12-31T00:00:00+0000"

    def test_operation_history_serialize_with_invalid_context_falls_back_to_utc(
        self,
    ) -> None:
        # Arrange
        start_at = datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.UTC)
        history = build_history(start_at=start_at, progress=None, error=None)

        # Act
        serialized = history.model_dump(context="Invalid/Timezone", mode="json")

        # Assert
        assert serialized["start_at"] == "1970-01-01T00:00:00+0000"
        assert serialized["end_at"] == ""

    def test_read_history_with_existing_file_succeeds(
        self, tmp_path: pathlib.Path
    ) -> None:
        # Arrange
        writer = OperationHistoryWriter(str(tmp_path), "UTC")
        start_at = datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.UTC)
        history_text = {
            "operation_id": CURRENT_OPERATION_ID,
            "operation_type": OperationType.ADD_LABEL.value,
            "steps": OperationSteps.EXTRACTING.value,
            "start_at": start_at.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "request": {"action": "test"},
        }
        history_file = (
            tmp_path / OPERATIONS_DIRECTORY_NAME / f"{CURRENT_OPERATION_ID}.json"
        )
        history_file.write_text(json.dumps(history_text), encoding="utf-8")

        # Act
        history_dict = writer.read_history(operation_id=CURRENT_OPERATION_ID)

        # Assert
        assert history_dict.operation_id == CURRENT_OPERATION_ID
        assert history_dict.operation_type == OperationType.ADD_LABEL
        assert history_dict.start_at == start_at
        assert history_dict.request == {"action": "test"}

    def test_read_history_when_file_missing_errors(
        self,
        tmp_path: pathlib.Path,
    ) -> None:
        # Arrange
        writer = OperationHistoryWriter(str(tmp_path), "UTC")

        # Act & Assert
        with pytest.raises(FileNotFoundError):
            writer.read_history(operation_id="non_existent")

    def test_read_history_when_read_fails_errors(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Arrange
        def raise_io_error(_: pathlib.Path) -> str:
            raise PermissionError

        writer = OperationHistoryWriter(str(tmp_path), "UTC")
        monkeypatch.setattr(pathlib.Path, "read_text", raise_io_error)

        history_file = (
            tmp_path / OPERATIONS_DIRECTORY_NAME / f"{CURRENT_OPERATION_ID}.json"
        )
        history_file.write_text(json.dumps({}), encoding="utf-8")

        # Act & Assert
        with pytest.raises(PermissionError):
            writer.read_history(operation_id=CURRENT_OPERATION_ID)
