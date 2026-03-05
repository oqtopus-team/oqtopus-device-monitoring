import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime

from common.operations import (
    LockManager,
    OperationHistoryWriter,
)
from common.types.operation import (
    OperationError,
    OperationHistory,
    OperationSteps,
    OperationType,
    StepInfo,
    Steps,
)
from common.types.victoria_metrics import MetricDataResponse
from common.util import get_time
from common.victoria_metrics import VictoriaMetricsClient
from schemas.meta import (
    AddLabelRequest,
    DeleteLabelRequest,
    ModifyLabelKeyRequest,
    ModifyLabelValueRequest,
    ProcessStatus,
    Selector,
)

logger = logging.getLogger("api-server.metadata-executor")


MIN_DATE = datetime.fromisoformat("1970-01-01T00:00:00Z")
MAX_DATE = datetime.fromisoformat("9999-12-31T23:59:59Z")


class OperationExecutor:
    """Executor for operations.

    This class handles the background execution of metadata operations,
    including exporting, transforming, ingesting, and deleting time-series data.
    """

    def __init__(
        self,
        client: VictoriaMetricsClient,
        lock_manager: LockManager,
        history_writer: OperationHistoryWriter,
    ) -> None:
        """Initialize the operation executor.

        Args:
            client: VictoriaMetrics client for API operations
            lock_manager: Manager for operation locks
            history_writer: Writer for operation history files

        """
        self._client = client
        self._lock_manager = lock_manager
        self._history_writer = history_writer

    @staticmethod
    @asynccontextmanager
    async def _execute_step(
        step: OperationSteps,
        operation_id: str,
        steps: list[StepInfo],
    ) -> AsyncGenerator[None]:
        """Execute a single operation step with logging and status tracking.

        Args:
            step: The operation step to execute
            operation_id: Unique identifier for the operation
            steps: List to append step information to

        Yields:
            None: Control flow for the step execution

        """
        logger.info("Operation id: %s, step: %s", operation_id, step.value)

        step_start = get_time()
        steps.append(
            StepInfo(
                name=step,
                status=ProcessStatus.in_progress,
                start_time=step_start,
                completed_at=None,
            )
        )

        try:
            yield
            step_end = get_time()
            steps[-1].status = ProcessStatus.completed
            steps[-1].completed_at = step_end
        except Exception:
            step_end = get_time()
            steps[-1].status = ProcessStatus.failed
            steps[-1].completed_at = step_end
            raise

    @staticmethod
    def _create_cleanup_instruction(
        step: OperationSteps,
        original_metric_name: str,
        original_selector: Selector,
        ingested_metric_info: dict[str, str] | None = None,
    ) -> str:
        """Create cleanup instruction message.

        Args:
            step: The operation step where cleanup is needed
            original_metric_name: The name of the original metric
            original_selector: The selector used for the original time-series
            ingested_metric_info: Information about ingested metric, if applicable

        Returns:
            Instruction message

        """
        instruction: str

        if step in {OperationSteps.EXTRACTING, OperationSteps.TRANSFORMING}:
            instruction = "Cleanup is not necessary."
        elif step == OperationSteps.INGESTING:
            instruction = "Please delete ingested time-series data in VictoriaMetrics."
            if ingested_metric_info is not None:
                instruction += (
                    f" Metric information of ingested data: {ingested_metric_info}."
                )
        else:
            instruction = "Please delete original time-series data in VictoriaMetrics."
            instruction += (
                f" Metric name: {original_metric_name}, "
                f" Selector: {original_selector.model_dump()}."
            )
        return instruction

    @staticmethod
    def _create_error_info(
        step: OperationSteps,
        message: str,
        cleanup_instruction: str,
    ) -> OperationError:
        """Create an OperationError instance.

        Args:
            step: The operation step where the error occurred
            message: Error message describing the failure
            cleanup_instruction: Instructions for cleanup after the error

        Returns:
            OperationError instance with provided details

        """
        return OperationError(
            step=step,
            message=message,
            cleanup_instructions=cleanup_instruction,
        )

    def _write_history_file(self, operation_id: str, history: OperationHistory) -> None:
        """Write operation history using a prepared OperationHistory instance.

        Args:
            operation_id: Unique identifier for the operation
            history: Prepared OperationHistory model to write to disk

        """
        self._history_writer.write_history(operation_id=operation_id, history=history)

    async def execute_add_label(
        self, operation_id: str, request: AddLabelRequest
    ) -> None:
        """Execute add label operation in background.

        Args:
            operation_id: Unique identifier for this operation
            request: The add label request containing operation parameters

        """
        start_time = get_time()
        operation_type = OperationType.ADD_LABEL
        current_step = OperationSteps.EXTRACTING
        steps: list[StepInfo] = []
        error_info: OperationError | None = None
        transformed_data: list[MetricDataResponse] = []

        try:
            # Extracting
            async with self._execute_step(
                OperationSteps.EXTRACTING, operation_id, steps
            ):
                current_step = OperationSteps.EXTRACTING
                # export time-series data matching the selector from VictoriaMetrics
                exported_data = await self._client.read_timeseries(
                    metric_name=request.metric_name,
                    selector=request.selector,
                )

            # Transforming
            async with self._execute_step(
                OperationSteps.TRANSFORMING, operation_id, steps
            ):
                current_step = OperationSteps.TRANSFORMING
                # Transform the exported data to add the new label
                transformed_data = self._transform_add_label(
                    exported_data=exported_data,
                    new_label_key=request.new_label_key,
                    default_value=request.default_value,
                )

            # Ingesting
            async with self._execute_step(
                OperationSteps.INGESTING, operation_id, steps
            ):
                current_step = OperationSteps.INGESTING
                # Ingest the transformed data back to VictoriaMetrics
                await self._client.write_timeseries(data=transformed_data)

            # Deleting
            async with self._execute_step(OperationSteps.DELETING, operation_id, steps):
                current_step = OperationSteps.DELETING
                # Delete the original time-series without the new label
                await self._client.delete_timeseries(
                    metric_name=request.metric_name,
                    selector=request.selector,
                )

            # Finishing
            async with self._execute_step(
                OperationSteps.FINISHING, operation_id, steps
            ):
                current_step = OperationSteps.FINISHING

        except Exception as e:
            logger.exception(
                "Failed: operation id: %s, step: %s",
                operation_id,
                current_step.value,
            )
            # Create cleanup instruction
            instruction = self._create_cleanup_instruction(
                step=current_step,
                original_metric_name=request.metric_name,
                original_selector=request.selector,
                ingested_metric_info=transformed_data[0].metric
                if transformed_data
                else None,
            )
            # Create error info
            error_info = self._create_error_info(
                step=current_step,
                message=f"Failed to {current_step.value}: {e}",
                cleanup_instruction=instruction,
            )

        finally:
            self._write_history_file(
                operation_id=operation_id,
                history=OperationHistory(
                    operation_id=operation_id,
                    operation_type=operation_type,
                    steps=current_step,
                    start_at=start_time,
                    end_at=get_time(),
                    request=request.model_dump(),
                    progress=Steps.model_validate({"steps": steps}) if steps else None,
                    error=error_info,
                ),
            )
            self._lock_manager.release_lock()

    async def execute_modify_label_key(
        self, operation_id: str, request: ModifyLabelKeyRequest
    ) -> None:
        """Execute modify label key operation in background.

        Args:
            operation_id: Unique identifier for this operation
            request: The modify label key request containing operation parameters

        """
        start_time = get_time()
        operation_type = OperationType.MODIFY_KEY
        current_step = OperationSteps.EXTRACTING
        steps: list[StepInfo] = []
        error_info: OperationError | None = None
        transformed_data: list[MetricDataResponse] = []

        try:
            # Extracting
            async with self._execute_step(
                OperationSteps.EXTRACTING, operation_id, steps
            ):
                current_step = OperationSteps.EXTRACTING
                # export time-series data matching the selector from VictoriaMetrics
                exported_data = await self._client.read_timeseries(
                    metric_name=request.metric_name,
                    selector=request.selector,
                )

            # Transforming
            async with self._execute_step(
                OperationSteps.TRANSFORMING, operation_id, steps
            ):
                current_step = OperationSteps.TRANSFORMING

                # split exported data into in-range
                # and out-of-range based on request range
                exported_in_range_data, exported_out_of_range_data = (
                    self._split_exported_data_by_time_range(
                        exported_data=exported_data,
                        start=request.range.start,
                        end=request.range.end,
                    )
                )

                # Transform the exported data to modify the label key
                transformed_data = self._transform_modify_label_key(
                    exported_data=exported_in_range_data,
                    from_key=request.from_key,
                    to_key=request.to_key,
                )

            # Ingesting
            async with self._execute_step(
                OperationSteps.INGESTING, operation_id, steps
            ):
                current_step = OperationSteps.INGESTING
                # Ingest the transformed data back to VictoriaMetrics
                await self._client.write_timeseries(data=transformed_data)

            # Deleting
            async with self._execute_step(OperationSteps.DELETING, operation_id, steps):
                current_step = OperationSteps.DELETING
                # Delete the original time-series with the old label key
                await self._client.delete_timeseries(
                    metric_name=request.metric_name,
                    selector=request.selector,
                )

                # Re-ingest out-of-range data that should remain unchanged
                await self._client.write_timeseries(data=exported_out_of_range_data)

            # Finishing
            async with self._execute_step(
                OperationSteps.FINISHING, operation_id, steps
            ):
                current_step = OperationSteps.FINISHING

        except Exception as e:
            logger.exception(
                "Failed: operation id: %s, step: %s",
                operation_id,
                current_step.value,
            )
            # Create cleanup instruction
            instrauction = self._create_cleanup_instruction(
                step=current_step,
                original_metric_name=request.metric_name,
                original_selector=request.selector,
                ingested_metric_info=transformed_data[0].metric
                if transformed_data
                else None,
            )
            # Create error info
            error_info = self._create_error_info(
                step=current_step,
                message=f"Failed to {current_step.value}: {e}",
                cleanup_instruction=instrauction,
            )

        finally:
            self._write_history_file(
                operation_id=operation_id,
                history=OperationHistory(
                    operation_id=operation_id,
                    operation_type=operation_type,
                    steps=current_step,
                    start_at=start_time,
                    end_at=get_time(),
                    request=request.model_dump(),
                    progress=Steps.model_validate({"steps": steps}) if steps else None,
                    error=error_info,
                ),
            )
            self._lock_manager.release_lock()

    async def execute_modify_label_value(
        self, operation_id: str, request: ModifyLabelValueRequest
    ) -> None:
        """Execute modify label value operation in background.

        Args:
            operation_id: Unique identifier for this operation
            request: The modify label value request containing operation parameters

        """
        start_time = get_time()
        operation_type = OperationType.MODIFY_VALUE
        current_step = OperationSteps.EXTRACTING
        steps: list[StepInfo] = []
        error_info: OperationError | None = None
        transformed_data: list[MetricDataResponse] = []

        try:
            # Extracting
            async with self._execute_step(
                OperationSteps.EXTRACTING, operation_id, steps
            ):
                current_step = OperationSteps.EXTRACTING
                # export time-series data matching the selector from VictoriaMetrics
                exported_data = await self._client.read_timeseries(
                    metric_name=request.metric_name,
                    selector=request.selector,
                )

            # Transforming
            async with self._execute_step(
                OperationSteps.TRANSFORMING, operation_id, steps
            ):
                current_step = OperationSteps.TRANSFORMING

                # split exported data into in-range
                # and out-of-range based on request range
                exported_in_range_data, exported_out_of_range_data = (
                    self._split_exported_data_by_time_range(
                        exported_data=exported_data,
                        start=request.range.start,
                        end=request.range.end,
                    )
                )
                # Transform the exported data to modify the label value
                transformed_data = self._transform_modify_label_value(
                    exported_data=exported_in_range_data,
                    label_key=request.key,
                    from_value=request.from_value,
                    to_value=request.to_value,
                )

            # Ingesting
            async with self._execute_step(
                OperationSteps.INGESTING, operation_id, steps
            ):
                current_step = OperationSteps.INGESTING
                # Ingest the transformed data back to VictoriaMetrics
                await self._client.write_timeseries(data=transformed_data)

            # Deleting
            async with self._execute_step(OperationSteps.DELETING, operation_id, steps):
                current_step = OperationSteps.DELETING
                # Delete the original time-series with the old label value
                await self._client.delete_timeseries(
                    metric_name=request.metric_name,
                    selector=request.selector,
                )

                # Re-ingest out-of-range data that should remain unchanged
                await self._client.write_timeseries(data=exported_out_of_range_data)

            # Finishing
            async with self._execute_step(
                OperationSteps.FINISHING, operation_id, steps
            ):
                current_step = OperationSteps.FINISHING

        except Exception as e:
            logger.exception(
                "Failed: operation id: %s, step: %s",
                operation_id,
                current_step.value,
            )
            # Create cleanup instruction
            instrauction = self._create_cleanup_instruction(
                step=current_step,
                original_metric_name=request.metric_name,
                original_selector=request.selector,
                ingested_metric_info=transformed_data[0].metric
                if transformed_data
                else None,
            )
            # Create error info
            error_info = self._create_error_info(
                step=current_step,
                message=f"Failed to {current_step.value}: {e}",
                cleanup_instruction=instrauction,
            )

        finally:
            self._write_history_file(
                operation_id=operation_id,
                history=OperationHistory(
                    operation_id=operation_id,
                    operation_type=operation_type,
                    steps=current_step,
                    start_at=start_time,
                    end_at=get_time(),
                    request=request.model_dump(),
                    progress=Steps.model_validate({"steps": steps}) if steps else None,
                    error=error_info,
                ),
            )
            self._lock_manager.release_lock()

    async def execute_delete_label(
        self, operation_id: str, request: DeleteLabelRequest
    ) -> None:
        """Execute delete label operation in background.

        Args:
            operation_id: Unique identifier for this operation
            request: The delete label request containing operation parameters

        """
        start_time = get_time()
        operation_type = OperationType.DELETE_LABEL
        current_step = OperationSteps.EXTRACTING
        steps: list[StepInfo] = []
        error_info: OperationError | None = None
        transformed_data: list[MetricDataResponse] = []

        try:
            # Extracting
            async with self._execute_step(
                OperationSteps.EXTRACTING, operation_id, steps
            ):
                current_step = OperationSteps.EXTRACTING
                # export time-series data matching the selector from VictoriaMetrics
                exported_data = await self._client.read_timeseries(
                    metric_name=request.metric_name,
                    selector=request.selector,
                )

            # Transforming
            async with self._execute_step(
                OperationSteps.TRANSFORMING, operation_id, steps
            ):
                current_step = OperationSteps.TRANSFORMING
                # Transform the exported data to delete the specified labels
                transformed_data = self._transform_delete_label(
                    exported_data=exported_data,
                    label_keys=request.label_keys,
                )

            # Ingesting
            async with self._execute_step(
                OperationSteps.INGESTING, operation_id, steps
            ):
                current_step = OperationSteps.INGESTING
                # Ingest the transformed data back to VictoriaMetrics
                await self._client.write_timeseries(data=transformed_data)

            # Deleting
            async with self._execute_step(OperationSteps.DELETING, operation_id, steps):
                current_step = OperationSteps.DELETING
                # Delete the original time-series with the specified labels
                await self._client.delete_timeseries(
                    metric_name=request.metric_name,
                    selector=request.selector,
                )

            # Finishing
            async with self._execute_step(
                OperationSteps.FINISHING, operation_id, steps
            ):
                current_step = OperationSteps.FINISHING

        except Exception as e:
            logger.exception(
                "Failed: operation id: %s, step: %s",
                operation_id,
                current_step.value,
            )
            # Create cleanup instruction
            instrauction = self._create_cleanup_instruction(
                step=current_step,
                original_metric_name=request.metric_name,
                original_selector=request.selector,
                ingested_metric_info=transformed_data[0].metric
                if transformed_data
                else None,
            )
            # Create error info
            error_info = self._create_error_info(
                step=current_step,
                message=f"Failed to {current_step.value}: {e}",
                cleanup_instruction=instrauction,
            )

        finally:
            self._write_history_file(
                operation_id=operation_id,
                history=OperationHistory(
                    operation_id=operation_id,
                    operation_type=operation_type,
                    steps=current_step,
                    start_at=start_time,
                    end_at=get_time(),
                    request=request.model_dump(),
                    progress=Steps.model_validate({"steps": steps}) if steps else None,
                    error=error_info,
                ),
            )
            self._lock_manager.release_lock()

    async def execute_delete_time_series(
        self, operation_id: str, metric_name: str, selector: Selector
    ) -> None:
        """Execute delete time-series operation in background.

        Args:
            operation_id: Unique identifier for this operation
            metric_name: The name of the metric
            selector: The selector to match time-series

        """
        start_time = get_time()
        operation_type = OperationType.DELETE_TIME_SERIES
        current_step = OperationSteps.DELETING
        steps: list[StepInfo] = []
        error_info: OperationError | None = None

        try:
            # Deleting
            async with self._execute_step(OperationSteps.DELETING, operation_id, steps):
                current_step = OperationSteps.DELETING
                # Delete the time-series matching the selector
                await self._client.delete_timeseries(
                    metric_name=metric_name,
                    selector=selector,
                )

            # Finishing
            async with self._execute_step(
                OperationSteps.FINISHING, operation_id, steps
            ):
                current_step = OperationSteps.FINISHING

        except Exception as e:
            logger.exception(
                "Failed: operation id: %s, step: %s",
                operation_id,
                current_step.value,
            )
            # Create cleanup instruction
            instruction = self._create_cleanup_instruction(
                step=current_step,
                original_metric_name=metric_name,
                original_selector=selector,
            )
            # Create error info
            error_info = self._create_error_info(
                step=current_step,
                message=f"Failed to {current_step.value}: {e}",
                cleanup_instruction=instruction,
            )

        finally:
            self._write_history_file(
                operation_id=operation_id,
                history=OperationHistory(
                    operation_id=operation_id,
                    operation_type=operation_type,
                    steps=current_step,
                    start_at=start_time,
                    end_at=get_time(),
                    request={
                        "metric_name": metric_name,
                        "selector": selector.model_dump(),
                    },
                    progress=Steps.model_validate({"steps": steps}) if steps else None,
                    error=error_info,
                ),
            )
            self._lock_manager.release_lock()

    @staticmethod
    def _transform_add_label(
        exported_data: list[MetricDataResponse],
        new_label_key: str,
        default_value: str,
    ) -> list[MetricDataResponse]:
        """Transform a single time-series to add a new label.

        Args:
            exported_data: The exported_data as a string
            new_label_key: The new label key to add
            default_value: The default value for the new label

        Returns:
            Transformed exported_data with the new label added

        Raises:
            KeyError: If the new label key already exists in the metric metadata

        """
        for entry in exported_data:
            # Add new label to metric metadata
            if new_label_key not in entry.metric:
                entry.metric[new_label_key] = default_value
            else:
                error_message = f"Label key '{new_label_key}' already exists."
                logger.error(error_message)
                raise KeyError(error_message)

        return exported_data

    @staticmethod
    def _transform_modify_label_key(
        exported_data: list[MetricDataResponse],
        from_key: str,
        to_key: str,
    ) -> list[MetricDataResponse]:
        """Transform time-series data to modify a label key.

        Args:
            exported_data: The exported time-series data
            from_key: The current label key to be renamed
            to_key: The new label key name

        Returns:
            Transformed data with the label key modified

        Raises:
            KeyError: If the from_key does not exist in the metric metadata

        """
        for entry in exported_data:
            # Modify label key in metric metadata
            if from_key in entry.metric:
                label_value = entry.metric.pop(from_key)
                entry.metric[to_key] = label_value
            else:
                error_message = (
                    f"Label key '{from_key}' does not exist in the metric metadata."
                )
                logger.error(error_message)
                raise KeyError(error_message)
        return exported_data

    @staticmethod
    def _transform_modify_label_value(
        exported_data: list[MetricDataResponse],
        label_key: str,
        from_value: str,
        to_value: str,
    ) -> list[MetricDataResponse]:
        """Transform time-series data to modify a label value.

        Args:
            exported_data: The exported time-series data
            label_key: The label key whose value is to be modified
            from_value: The current label value to be changed
            to_value: The new label value

        Returns:
            Transformed data with the label value modified

        Raises:
            KeyError: If the label_key does not exist in the metric metadata

        """
        for entry in exported_data:
            # Modify label value in metric metadata
            if label_key in entry.metric and entry.metric[label_key] == from_value:
                entry.metric[label_key] = to_value
            else:
                error_message = (
                    f"Label key '{label_key}' with value '{from_value}' does not exist."
                )
                logger.error(error_message)
                raise KeyError(error_message)

        return exported_data

    @staticmethod
    def _transform_delete_label(
        exported_data: list[MetricDataResponse],
        label_keys: list[str],
    ) -> list[MetricDataResponse]:
        """Transform time-series data to delete specified labels.

        Args:
            exported_data: The exported time-series data
            label_keys: The list of label keys to be deleted

        Returns:
            Transformed data with the specified labels deleted

        Raises:
            KeyError: If none of the specified label keys exist in the metric metadata

        """
        for entry in exported_data:
            # Delete specified labels from metric metadata
            is_deleted = False
            for label_key in label_keys:
                if label_key in entry.metric:
                    entry.metric.pop(label_key)
                    is_deleted = True
            if not is_deleted:
                error_message = f"None of the specified label keys {label_keys} exist."
                logger.error(error_message)
                raise KeyError(error_message)

        return exported_data

    @staticmethod
    def _split_exported_data_by_time_range(
        exported_data: list[MetricDataResponse],
        start: datetime | None,
        end: datetime | None,
    ) -> tuple[list[MetricDataResponse], list[MetricDataResponse]]:
        """Split exported data based on the specified time range.

        Args:
            exported_data: The exported time-series data
            start: The start time of the range (ISO8601 string) or None
            end: The end time of the range (ISO8601 string) or None

        Returns:
            tuple containing two lists:
                - in_range_data: Data points within the specified time range
                - out_of_range_data: Data points outside the specified time range

        Raises:
            ValueError: If there is a mismatch between timestamps and values in the data

        """
        if start is None:
            start = MIN_DATE
        if end is None:
            end = MAX_DATE

        # Convert to milliseconds to match VictoriaMetrics timestamp format
        start_unix = int(start.timestamp() * 1000)
        end_unix = int(end.timestamp() * 1000)

        in_range_data: list[MetricDataResponse] = []
        out_of_range_data: list[MetricDataResponse] = []

        for entry in exported_data:
            in_range_values: list[float | None] = []
            in_range_timestamps: list[int] = []
            out_of_range_values: list[float | None] = []
            out_of_range_timestamps: list[int] = []

            if len(entry.timestamps) != len(entry.values):
                error_message = (
                    "Mismatch between number of timestamps and values in exported data."
                )
                logger.error(error_message)
                raise ValueError(error_message)

            for i in range(len(entry.timestamps)):
                timestamp = entry.timestamps[i]
                value = entry.values[i]

                if start_unix <= timestamp <= end_unix:
                    in_range_timestamps.append(timestamp)
                    in_range_values.append(value)
                else:
                    out_of_range_timestamps.append(timestamp)
                    out_of_range_values.append(value)

            if len(in_range_values) > 0 and len(in_range_timestamps) > 0:
                in_range_data.append(
                    MetricDataResponse(
                        metric=entry.metric,
                        values=in_range_values,
                        timestamps=in_range_timestamps,
                    )
                )
            if len(out_of_range_values) > 0 and len(out_of_range_timestamps) > 0:
                out_of_range_data.append(
                    MetricDataResponse(
                        metric=entry.metric,
                        values=out_of_range_values,
                        timestamps=out_of_range_timestamps,
                    )
                )

        return in_range_data, out_of_range_data
