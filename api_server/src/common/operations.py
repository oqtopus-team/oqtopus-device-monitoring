import datetime
import json
import logging
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from common.config import AppConfig
from common.types.operation import (
    OperationHistory,
    OperationSteps,
)
from schemas.meta import ProcessStatus

logger = logging.getLogger("api-server.operations")

LOCK_FILE_NAME: str = ".lock"
OPERATIONS_DIRECTORY_NAME: str = "operations"


class LockManager:
    """Manager for operation lock file.

    This class handles acquiring, releasing, and checking the global lock
    for metadata mutation operations.
    """

    def __init__(self, data_path: str, config: AppConfig) -> None:
        """Initialize the lock manager.

        Args:
            data_path: Path to the data directory where lock file is stored
            config: Application configuration

        """
        self._data_path = Path(data_path)
        self._lock_file = self._data_path / LOCK_FILE_NAME
        self._operations_dir = self._data_path / OPERATIONS_DIRECTORY_NAME
        self._tz_str = config.server.timezone
        self._lock_timeout_hours = config.lock_timeout_hours

        # Ensure directories exist
        self._data_path.mkdir(parents=True, exist_ok=True)
        self._operations_dir.mkdir(parents=True, exist_ok=True)

    def acquire_lock(self, operation_id: str) -> bool:
        """Acquire the lock for an operation.

        Args:
            operation_id: The operation ID requesting the lock

        Returns:
            True if lock acquired successfully, False if locked by another operation

        """
        can_take_lock = False

        try:
            if self._is_locked():
                # Check if it's a stale lock
                current_holder = self.get_lock_holder()

                if self._is_stale_lock(current_holder):
                    logger.warning(
                        "Stale lock detected for operation %s. Releasing lock.",
                        current_holder,
                    )
                    # Remove stale lock and acquire new one
                    self.release_lock()
                    can_take_lock = True
                else:
                    logger.info(
                        "Lock held by active operation %s, rejecting new operation %s",
                        current_holder,
                        operation_id,
                    )
                    can_take_lock = False
            else:
                can_take_lock = True

            if not can_take_lock:
                return False

            # Acquire lock by writing operation ID to lock file
            self._lock_file.write_text(operation_id)

        except Exception:
            logger.exception("Failed to acquire lock.")
            raise

        logger.info("Lock acquired for operation %s", operation_id)
        return True

    def get_lock_holder(self) -> str:
        """Get the operation ID currently holding the lock.

        Returns:
            The operation ID holding the lock

        """
        try:
            return self._lock_file.read_text().strip()
        except Exception:
            logger.exception("Failed to read lock file.")
            raise

    def release_lock(self) -> None:
        """Release the lock by deleting the lock file.

        This should be called when an operation completes (successfully or not).
        """
        try:
            self._lock_file.unlink()
            logger.info("Lock released")
        except Exception:
            logger.exception("Failed to release lock.")
            raise

    def _is_locked(self) -> bool:
        """Check if lock file exists.

        Returns:
            True if lock file exists, False otherwise

        """
        return self._lock_file.exists()

    def _is_stale_lock(self, operation_id: str) -> bool:
        """Check if a lock is stale (operation completed but lock not released).

        A lock is considered stale if the operation history file exists
        with a terminal status (completed or failed).

        Args:
            operation_id: The operation ID to check

        Returns:
            True if the lock is stale, False otherwise

        """
        try:
            operation_history_writer = OperationHistoryWriter(
                str(self._data_path), self._tz_str
            )
            history_dict = operation_history_writer.read_history(operation_id)

            # Check if operation is in terminal state
            if history_dict.error is not None:
                return True  # Failed operation

            # Processing not started
            if history_dict.progress is None:
                return False

            # Get dateinfo from start_at in history
            start_at = history_dict.start_at.astimezone(datetime.UTC)
            # Current time in UTC
            now = datetime.datetime.now(datetime.UTC)
            # Check if operation exceeded timeout
            if (now - start_at) > datetime.timedelta(hours=self._lock_timeout_hours):
                logger.warning(
                    "Operation %s exceeded timeout of %d hours.",
                    operation_id,
                    self._lock_timeout_hours,
                )
                return True  # Operation exceeded timeout

            if (
                history_dict.progress.steps[-1].status == ProcessStatus.completed
                and history_dict.progress.steps[-1].name == OperationSteps.FINISHING
            ):
                return True

        except Exception:
            logger.exception("Failed to read operation history file")
            return False

        return False  # Lock is held by an active operation


class OperationHistoryWriter:
    """Writer for operation history files.

    This class handles writing operation history files to track the status
    and details of metadata mutation operations.
    """

    def __init__(self, data_path: str, tz_str: str) -> None:
        """Initialize the operation history writer.

        Args:
            data_path: Path to the data directory where operation history files
            tz_str: Timezone string for timestamps

        """
        self._operations_dir = Path(data_path) / OPERATIONS_DIRECTORY_NAME
        self._operations_dir.mkdir(parents=True, exist_ok=True)

        # Set timezone
        try:
            self._tz = ZoneInfo(tz_str)
        except ZoneInfoNotFoundError:
            logger.warning("Timezone '%s' not found. Defaulting to UTC.", tz_str)
            self._tz = ZoneInfo("UTC")

    def write_history(self, operation_id: str, history: OperationHistory) -> None:
        """Write operation history to file.

        Args:
            operation_id: The operation ID
            history: Dictionary containing operation history data

        """
        history_file = self._operations_dir / f"{operation_id}.json"
        try:
            history_dict = history.model_dump(context=self._tz, mode="json")

            history_file.write_text(
                json.dumps(history_dict, indent=2, ensure_ascii=False)
            )
            logger.info("Operation history written for %s", operation_id)
            logger.info("History file path: %s", history_file.absolute())
        except Exception:
            logger.exception("Failed to write operation history")
            raise

    def read_history(self, operation_id: str) -> OperationHistory:
        """Read operation history from file.

        Args:
            operation_id: The operation ID to read

        Returns:
            Operation history object

        Raises:
            FileNotFoundError: If the history file does not exist

        """
        history_file = self._operations_dir / f"{operation_id}.json"
        if not history_file.exists():
            error_msg = f"History file for {operation_id} does not exist."
            raise FileNotFoundError(error_msg)

        try:
            return OperationHistory(**json.loads(history_file.read_text()))
        except Exception:
            logger.exception("Failed to read operation history")
            raise
