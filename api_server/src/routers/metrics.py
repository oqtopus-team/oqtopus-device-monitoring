import logging
from datetime import datetime
from typing import Annotated

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Query, Request

from common import util
from common.background_tasks import (
    OperationExecutor,
)
from common.config import AppConfig, get_config
from common.operations import LockManager, OperationHistoryWriter
from common.request_validation import (
    RequestValidation,
    RequestValidationError,
)
from common.util import generate_operation_id
from common.victoria_metrics import (
    VictoriaMetricsClient,
)
from schemas.error import BadRequest, InternalServerError, LockedError, NotFoundError
from schemas.errors import (
    BadRequestResponse,
    ErrorResponse,
    InternalServerErrorResponse,
    LockedErrorResponse,
    NotFoundErrorResponse,
)
from schemas.meta import Selector
from schemas.metrics import (
    LabelKeysData,
    LabelKeysList,
    LabelValuesData,
    LabelValuesList,
    MetricsData,
    MetricsList,
    PageInfo,
    TimeSeriesData,
    TimeSeriesPayload,
)
from schemas.success import AcceptedResponse, Data

logger = logging.getLogger("api-server.api.metrics")

router = APIRouter(prefix="/metrics", tags=["metrics"])


def get_client(
    config: Annotated[AppConfig, Depends(get_config)],
) -> VictoriaMetricsClient:
    """Dependency injection for VictoriaMetrics client.

    Args:
        config: Application configuration containing VictoriaMetrics URL

    Returns:
        Initialized VictoriaMetrics client instance

    """
    return VictoriaMetricsClient(base_url=config.victoria_metrics_url)


@router.get(
    "/names",
    response_model=MetricsList,
    responses={
        200: {
            "model": MetricsList,
        },
        400: {
            "model": BadRequest,
        },
        500: {
            "model": InternalServerError,
        },
    },
)
async def get_metrics_names(
    client: Annotated[VictoriaMetricsClient, Depends(get_client)],
    offset: Annotated[int, Query(ge=0, description="Offset for pagination")] = 0,
    limit: Annotated[
        int, Query(ge=1, le=10000, description="Limit for pagination")
    ] = 100,
) -> MetricsList | ErrorResponse:
    """Get all available metric names with pagination.

    This endpoint queries VictoriaMetrics to retrieve all metric names
    and returns them in a paginated format.

    Args:
        offset: Starting position for pagination (default: 0)
        limit: Maximum number of items to return (default: 100, max: 10000)
        client: VictoriaMetrics client (injected dependency)

    Returns:
        MetricsList response containing metric names and pagination info
        BadRequestResponse if the request is invalid
        InternalServerErrorResponse if an internal error occurs

    """
    logger.info("invoked!")

    try:
        # Get metric names from VictoriaMetrics
        metrics = await client.get_metric_names(offset=offset, limit=limit)
        total = len(metrics)

        logger.info("Accepted: retrieved %d metric names.", len(metrics))
        # Construct response
        return MetricsList(
            status="success",
            data=MetricsData(
                metrics=metrics,
                page=PageInfo(offset=offset, limit=limit, total=total),
            ),
        )

    except Exception as e:
        # Handle unexpected errors
        logger.exception("Internal server error")
        return InternalServerErrorResponse(message=f"Internal server error: {e!s}")


@router.get(
    "/{metric_name}/labels",
    response_model=LabelKeysList,
    responses={
        200: {
            "model": LabelKeysList,
        },
        400: {
            "model": BadRequest,
        },
        404: {
            "model": NotFoundError,
        },
        500: {
            "model": InternalServerError,
        },
    },
)
async def get_metric_label_keys(
    metric_name: str,
    client: Annotated[VictoriaMetricsClient, Depends(get_client)],
) -> LabelKeysList | ErrorResponse:
    """Get label keys for a specific metric.

    Args:
        metric_name: Name of the metric to retrieve label keys for
        client: VictoriaMetrics client (injected dependency)

    Returns:
        LabelKeysList response containing label keys for the metric
        BadRequestResponse if the request is invalid
        InternalServerErrorResponse if an internal error occurs

    """
    logger.info("invoked for metric: %s", metric_name)

    try:
        # Get label keys from VictoriaMetrics
        target_series_label_keys = await client.get_series_label_keys(
            metric_name=metric_name
        )

        # If metric does not exist, return BadRequestResponse
        if target_series_label_keys is None or len(target_series_label_keys) == 0:
            logger.error("No label keys found for metric '%s'.", metric_name)
            return NotFoundErrorResponse(
                message=f"Label is not found in metric '{metric_name}'.",
            )

        # Construct response
        return LabelKeysList(
            status="success",
            data=LabelKeysData(
                metric=metric_name,
                label_keys=target_series_label_keys,
            ),
        )
    except Exception as e:
        # Handle unexpected errors
        logger.exception("Internal server error.")
        return InternalServerErrorResponse(message=f"Internal server error: {e!s}")


@router.get(
    "/{metric_name}/labels/{label_key}/values",
    response_model=LabelValuesList,
    responses={
        200: {
            "model": LabelValuesList,
        },
        400: {
            "model": BadRequest,
        },
        404: {
            "model": NotFoundError,
        },
        500: {
            "model": InternalServerError,
        },
    },
)
async def get_metric_label_values(
    metric_name: str,
    label_key: str,
    client: Annotated[VictoriaMetricsClient, Depends(get_client)],
    offset: Annotated[int, Query(ge=0, description="Offset for pagination")] = 0,
    limit: Annotated[
        int, Query(ge=1, le=10000, description="Limit for pagination")
    ] = 100,
) -> LabelValuesList | ErrorResponse:
    """Get label values for a specific metric and label key.

    This endpoint retrieves all label values associated with the specified metric
    name and label key.

    Args:
        metric_name: Name of the metric to retrieve label values for
        label_key: Label key to retrieve values for
        client: VictoriaMetrics client (injected dependency)
        offset: Starting position for pagination (default: 0)
        limit: Maximum number of items to return (default: 100, min: 1, max: 10000)

    Returns:
        LabelValuesList response containing label values for the metric and label key
        BadRequestResponse if the request is invalid
        InternalServerErrorResponse if an internal error occurs

    """
    logger.info("invoked!")

    try:
        # Get label values from VictoriaMetrics
        label_values = await client.get_series_label_values(
            metric_name=metric_name, label_key=label_key, offset=offset, limit=limit
        )

        # If no label values found, return BadRequestResponse
        if label_values is None or len(label_values) == 0:
            logger.error(
                "Label values are not found for metric '%s' and label key '%s'.",
                metric_name,
                label_key,
            )
            return NotFoundErrorResponse(
                message=f"Label values are not found for metric '{metric_name}'"
                f" and label key '{label_key}'.",
            )

        # Construct response
        return LabelValuesList(
            status="success",
            data=LabelValuesData(
                metric=metric_name,
                label_key=label_key,
                values=label_values,
                page=PageInfo(offset=offset, limit=limit, total=len(label_values)),
            ),
        )
    except Exception as e:
        # Handle unexpected errors
        logger.exception("Internal server error.")
        return InternalServerErrorResponse(message=f"Internal server error: {e!s}")


@router.get(
    "/{metric_name}/series/data",
    response_model=TimeSeriesData,
    responses={
        200: {
            "model": TimeSeriesData,
        },
        400: {
            "model": BadRequest,
        },
        404: {
            "model": NotFoundError,
        },
        500: {
            "model": InternalServerError,
        },
    },
)
async def get_time_series_data(
    metric_name: str,
    start: Annotated[str, Query(description="Start timestamp (ISO8601)")],
    end: Annotated[str, Query(description="End timestamp (ISO8601)")],
    client: Annotated[VictoriaMetricsClient, Depends(get_client)],
    request: Request,
) -> TimeSeriesData | ErrorResponse:
    """Get time series data .

    Args:
        metric_name: Name of the metric to retrieve time series data for
        start: Start timestamp in ISO8601 format
        end: End timestamp in ISO8601 format
        request: FastAPI Request object to access query parameters
        client: VictoriaMetrics client (injected dependency)

    Returns:
        TimeSeriesData response containing time series data for the metric
        BadRequestResponse if the request is invalid
        InternalServerErrorResponse if an internal error occurs

    """
    logger.info("invoked!")
    try:
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.fromisoformat(end)
    except ValueError as e:
        logger.exception("Invalid timestamp format.")
        return BadRequestResponse(
            message=f"Invalid timestamp format (validation error): {e}",
        )

    try:
        # Parse selector from query parameters
        selector = util.parse_deep_object_as_selector(dict(request.query_params))

        # Check within sampling limit
        validator = RequestValidation(client=client)
        try:
            await validator.validate_get_time_series_data(
                metric_name=metric_name,
                selector=selector,
                start=start_dt,
                end=end_dt,
            )
        except httpx.HTTPError:
            logger.exception("HTTP error during validation.")
        except RequestValidationError as e:
            logger.exception("Time-series query constraints are not satisfied.")
            return BadRequestResponse(
                message=f"Time-series query constraints are not satisfied: {e.message}",
            )

        # Get time series data from VictoriaMetrics
        time_series_data = await client.read_timeseries(
            metric_name=metric_name,
            selector=selector,
        )

        # Validate time series data
        if len(time_series_data) == 0:
            logger.error(
                "Expected exactly one time series, but got %d.",
                len(time_series_data),
            )

            return NotFoundErrorResponse(
                message=(
                    "Metric or label-value set not found. Expected exactly one "
                    f"time series, but got {len(time_series_data)}."
                ),
            )
        if len(time_series_data) > 1:
            logger.error(
                "Expected exactly one time series, but got %d.",
                len(time_series_data),
            )
            return BadRequestResponse(
                message=(
                    "Multiple time series found. Expected exactly one time series, "
                    f"but got {len(time_series_data)}."
                ),
            )

        time_series_values = time_series_data[0].values
        time_series_timestamps = time_series_data[0].timestamps

        # Change to QueryRangeResponse
        loop_size = min(len(time_series_timestamps), len(time_series_values))
        start_timestamps = int(start_dt.timestamp() * 1000)
        end_timestamps = int(end_dt.timestamp() * 1000)
        query_range_response = [
            [time_series_timestamps[i], str(time_series_values[i])]
            for i in range(loop_size)
            if start_timestamps <= time_series_timestamps[i] <= end_timestamps
            and time_series_values[i] is not None
        ]
        # Determine downsampling rate
        if len(time_series_timestamps) <= 1:
            downsampling_rate = None
        else:
            downsampling_rate = min(
                abs(time_series_timestamps[i] - time_series_timestamps[i - 1])
                for i in range(1, len(time_series_timestamps))
            )

        payload = TimeSeriesPayload(
            metric=metric_name,
            labels=time_series_data[0].metric,
            values=query_range_response,
            start=datetime.fromisoformat(start),
            end=datetime.fromisoformat(end),
            step=downsampling_rate,
        )

        # Construct response
        return TimeSeriesData(
            status="success",
            data=payload,
        )
    except Exception as e:
        # Handle unexpected errors
        logger.exception("Internal server error")
        return InternalServerErrorResponse(message=f"Internal server error: {e!s}")


@router.delete(
    "/{metric_name}/series/data",
    response_model=AcceptedResponse,
    status_code=202,
    responses={
        202: {
            "model": AcceptedResponse,
        },
        400: {
            "model": BadRequest,
        },
        423: {
            "model": LockedError,
        },
        500: {
            "model": InternalServerError,
        },
    },
)
async def delete_time_series_data(
    metric_name: str,
    request: Selector,
    client: Annotated[VictoriaMetricsClient, Depends(get_client)],
    config: Annotated[AppConfig, Depends(get_config)],
    background_tasks: BackgroundTasks,
) -> AcceptedResponse | ErrorResponse:
    """Delete time series data.

    Args:
        metric_name: Name of the metric to delete time series data for
        request: FastAPI Request object to access query parameters
        background_tasks: FastAPI background tasks manager
        client: VictoriaMetrics client (injected dependency)
        config: Application configuration (injected dependency)

    Returns:
        AcceptedResponse indicating the delete operation has been scheduled
        BadRequestResponse if the request is invalid
        LockedErrorResponse if the data is locked and cannot be deleted
        InternalServerErrorResponse if an internal error occurs

    """
    logger.info("invoked!")

    # Initialize operation managers
    lock_manager = LockManager(config.operation_history_path, config)
    history_writer = OperationHistoryWriter(
        config.operation_history_path, config.server.timezone
    )

    # Generate unique operation ID
    operation_id = generate_operation_id(data_path=config.operation_history_path)
    logger.info("Generated operation ID: %s", operation_id)

    # Try to acquire lock
    try:
        lock_acquired_result = lock_manager.acquire_lock(operation_id)
        if not lock_acquired_result:
            locked_by = lock_manager.get_lock_holder()
            logger.info(
                "Lock is held by %s, cannot proceed with operation %s",
                locked_by,
                operation_id,
            )
            return LockedErrorResponse(
                message=f"Another operation is in progress (locked by {locked_by})",
            )
        logger.info("Lock acquired for operation_id: %s", operation_id)
    except Exception as e:
        logger.exception(
            "Failed to acquire lock. (operation_id: %s)",
            operation_id,
        )
        return InternalServerErrorResponse(message=f"Internal server error: {e!s}")

    # Schedule background task for executing the operation
    try:
        validator = RequestValidation(client=client)

        try:
            await validator.validate_delete_time_series(
                metric_name=metric_name, selector=request
            )
        except RequestValidationError as e:
            lock_manager.release_lock()
            logger.exception(
                "Metadata validation error. (operation_id: %s)",
                operation_id,
            )
            return BadRequestResponse(message=str(e))

        executor = OperationExecutor(
            client=client,
            history_writer=history_writer,
            lock_manager=lock_manager,
        )

        background_tasks.add_task(
            executor.execute_delete_time_series, operation_id, metric_name, request
        )

    except Exception as e:
        lock_manager.release_lock()
        logger.exception(
            "Failed to schedule background task. (operation_id: %s)",
            operation_id,
        )
        return InternalServerErrorResponse(message=f"Internal server error: {e!s}")

    return AcceptedResponse(
        status="success",
        data=Data(
            operation_id=operation_id,
            summary={
                "message": "Delete time-series data operation scheduled successfully"
            },
        ),
    )
