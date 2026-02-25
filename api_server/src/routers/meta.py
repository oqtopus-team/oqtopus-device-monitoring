import logging
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends
from httpx import HTTPError

from common.background_tasks import (
    OperationExecutor,
)
from common.config import AppConfig, get_config
from common.operations import LockManager, OperationHistoryWriter
from common.request_validation import RequestValidation, RequestValidationError
from common.types.operation import OperationSteps
from common.util import generate_operation_id
from common.victoria_metrics import VictoriaMetricsClient
from schemas.error import BadRequest, InternalServerError, LockedError
from schemas.errors import (
    BadRequestResponse,
    ErrorResponse,
    InternalServerErrorResponse,
    LockedErrorResponse,
)
from schemas.meta import (
    AddLabelRequest,
    DeleteLabelRequest,
    ModifyLabelKeyRequest,
    ModifyLabelValueRequest,
    OperationStatusData,
    OperationStatusResponse,
    ProcessStatus,
)
from schemas.success import AcceptedResponse, Data

logger = logging.getLogger("api-server.api.meta")

router = APIRouter(prefix="/meta", tags=["metadata"])


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
    "/status",
    status_code=200,
    response_model=OperationStatusResponse,
    responses={
        400: {
            "model": BadRequest,
        },
        500: {
            "model": InternalServerError,
        },
    },
)
async def get_operation_status(
    operation_id: str,
    config: Annotated[AppConfig, Depends(get_config)],
) -> OperationStatusResponse | ErrorResponse:
    """Get the status of a metadata operation.

    Args:
        operation_id: The ID of the operation to query
        config: Application configuration

    Returns:
        OperationStatusResponse with operation status details
        BadRequestResponse if the request is invalid
        InternalServerErrorResponse if an internal error occurs

    """
    logger.info("invoked!")

    history_writer = OperationHistoryWriter(
        config.operation_history_path, config.server.timezone
    )

    lock_file_exists = False
    progress_status = None
    progress_step = OperationSteps.EXTRACTING
    messages = f"Operation is currently {progress_step}"

    # Check if lock file exists to determine if operation is in progress
    try:
        try:
            lock_manager = LockManager(config.operation_history_path, config)
            lock_holder = lock_manager.get_lock_holder()

            if lock_holder == operation_id:
                lock_file_exists = True
                progress_status = ProcessStatus.in_progress
                logger.info(
                    "Operation %s is in progress (lock file exists).",
                    operation_id,
                )
        except (FileNotFoundError, OSError):
            pass

        # Check operation history
        history_data = None
        try:
            history_data = history_writer.read_history(operation_id)
            logger.info(
                "Retrieved operation history for operation_id: %s",
                operation_id,
            )
        except FileNotFoundError:
            if not lock_file_exists:
                logger.exception(
                    "Failed to read operation history. (operation_id: %s)",
                    operation_id,
                )
                return BadRequestResponse(
                    message=f"Invalid operation ID: {operation_id}"
                )

        if history_data is not None:
            if history_data.progress:
                progress_status = history_data.progress.steps[-1].status
                progress_step = history_data.progress.steps[-1].name

            messages = f"Operation is currently {progress_step}"

            if history_data.error is not None:
                messages += (
                    f"\nOperation failed with error: {history_data.error.message}"
                )

        operation_status_data = OperationStatusData(
            process_status=progress_status, messages=messages
        )
        return OperationStatusResponse(status="success", data=operation_status_data)

    except Exception as e:
        logger.exception(
            "Failed to retrieve operation status. (operation_id: %s)",
            operation_id,
        )
        return InternalServerErrorResponse(message=f"Internal server error: {e!s}")


@router.patch(
    "/add",
    response_model=AcceptedResponse,
    status_code=202,
    responses={
        200: {
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
async def add_metadata(
    request: AddLabelRequest,
    background_tasks: BackgroundTasks,
    config: Annotated[AppConfig, Depends(get_config)],
    client: Annotated[VictoriaMetricsClient, Depends(get_client)],
) -> AcceptedResponse | ErrorResponse:
    """Add a new label to selected time-series.

    Args:
        request: The add label request containing operation parameters
        background_tasks: FastAPI background tasks manager
        config: Application configuration (injected dependency)
        client:  VictoriaMetrics client (injected dependency)

    Returns:
        AcceptedResponse with operation_id and summary
        BadRequest if the request is invalid
        LockedError if another operation is in progress, otherwise
        InternalServerError if an internal error occurs

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
            await validator.validate_add_label(request)

        except HTTPError as e:
            lock_manager.release_lock()
            logger.exception(
                "HTTP error during validation. (operation_id: %s)",
                operation_id,
            )
            return InternalServerErrorResponse(message=f"Internal server error: {e!s}")

        except RequestValidationError as e:
            lock_manager.release_lock()
            logger.exception(
                "Metadata validation error. (operation_id: %s)",
                operation_id,
            )
            return BadRequestResponse(message=str(e))

        executor = OperationExecutor(
            client=client,
            lock_manager=lock_manager,
            history_writer=history_writer,
        )
        background_tasks.add_task(executor.execute_add_label, operation_id, request)

    except Exception as e:
        lock_manager.release_lock()
        logger.exception(
            "Failed to schedule background task. (operation_id: %s)",
            operation_id,
        )
        return InternalServerErrorResponse(message=f"Internal server error: {e!s}")

    logger.info(
        "Accepted: add label operation scheduled successfully. (operation_id: %s)",
        operation_id,
    )
    return AcceptedResponse(
        status="success",
        data=Data(
            operation_id=operation_id,
            summary={"message": "Add label operation scheduled successfully"},
        ),
    )


@router.patch(
    "/modify/key",
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
async def modify_metadata_key(
    request: ModifyLabelKeyRequest,
    background_tasks: BackgroundTasks,
    config: Annotated[AppConfig, Depends(get_config)],
    client: Annotated[VictoriaMetricsClient, Depends(get_client)],
) -> AcceptedResponse | ErrorResponse:
    """Modify existing label-keys for selected time-series.

    Args:
        request: The modify label key request containing operation parameters
        background_tasks: FastAPI background tasks manager
        config: Application configuration (injected dependency)
        client:  VictoriaMetrics client (injected dependency)

    Returns:
        AcceptedResponse with operation_id and summary
        BadRequestResponse if the request is invalid
        LockedErrorResponse if another operation is in progress, otherwise
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
            await validator.validate_modify_label_key(request)

        except HTTPError as e:
            lock_manager.release_lock()
            logger.exception(
                "HTTP error during validation. (operation_id: %s)",
                operation_id,
            )
            return InternalServerErrorResponse(message=f"Internal server error: {e!s}")

        except RequestValidationError as e:
            lock_manager.release_lock()
            logger.exception(
                "Metadata validation error. (operation_id: %s)",
                operation_id,
            )
            return BadRequestResponse(message=str(e))

        executor = OperationExecutor(
            client=client,
            lock_manager=lock_manager,
            history_writer=history_writer,
        )
        background_tasks.add_task(
            executor.execute_modify_label_key, operation_id, request
        )

    except Exception as e:
        lock_manager.release_lock()
        logger.exception(
            "Failed to schedule background task. (operation_id: %s)",
            operation_id,
        )
        return InternalServerErrorResponse(message=f"Internal server error: {e!s}")

    logger.info(
        "Accepted: modify label key operation scheduled"
        " successfully. (operation_id: %s)",
        operation_id,
    )
    return AcceptedResponse(
        status="success",
        data=Data(
            operation_id=operation_id,
            summary={"message": "Modify label key operation scheduled successfully"},
        ),
    )


@router.patch(
    "/modify/value",
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
async def modify_metadata_value(
    request: ModifyLabelValueRequest,
    background_tasks: BackgroundTasks,
    config: Annotated[AppConfig, Depends(get_config)],
    client: Annotated[VictoriaMetricsClient, Depends(get_client)],
) -> AcceptedResponse | ErrorResponse:
    """Modify existing label-values for selected time-series.

    Args:
        request: The modify label value request containing operation parameters
        background_tasks: FastAPI background tasks manager
        config: Application configuration (injected dependency)
        client:  VictoriaMetrics client (injected dependency)

    Returns:
        AcceptedResponse with operation_id and summary
        BadRequestResponse if the request is invalid
        LockedErrorResponse if another operation is in progress, otherwise
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
            await validator.validate_modify_label_value(request)

        except HTTPError as e:
            lock_manager.release_lock()
            logger.exception(
                "HTTP error during validation. (operation_id: %s)",
                operation_id,
            )
            return InternalServerErrorResponse(message=f"Internal server error: {e!s}")

        except RequestValidationError as e:
            lock_manager.release_lock()
            logger.exception(
                "Metadata validation error. (operation_id: %s)",
                operation_id,
            )
            return BadRequestResponse(message=str(e))

        executor = OperationExecutor(
            client=client,
            lock_manager=lock_manager,
            history_writer=history_writer,
        )
        background_tasks.add_task(
            executor.execute_modify_label_value, operation_id, request
        )

    except Exception as e:
        lock_manager.release_lock()
        logger.exception(
            "Failed to schedule background task. (operation_id: %s)",
            operation_id,
        )
        return InternalServerErrorResponse(message=f"Internal server error: {e!s}")

    logger.info(
        "Accepted: modify label value operation scheduled "
        "successfully. (operation_id: %s)",
        operation_id,
    )
    return AcceptedResponse(
        status="success",
        data=Data(
            operation_id=operation_id,
            summary={"message": "Modify label value operation scheduled successfully"},
        ),
    )


@router.patch(
    "/delete",
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
async def delete_metadata(
    request: DeleteLabelRequest,
    background_tasks: BackgroundTasks,
    config: Annotated[AppConfig, Depends(get_config)],
    client: Annotated[VictoriaMetricsClient, Depends(get_client)],
) -> AcceptedResponse | ErrorResponse:
    """Delete labels from selected time-series.

    Args:
        request: The delete label request containing operation parameters
        background_tasks: FastAPI background tasks manager
        config: Application configuration (injected dependency)
        client:  VictoriaMetrics client (injected dependency)

    Returns:
        AcceptedResponse with operation_id and summary
        BadRequestResponse if the request is invalid
        LockedErrorResponse if another operation is in progress, otherwise
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
            await validator.validate_delete_label(request)

        except HTTPError as e:
            lock_manager.release_lock()
            logger.exception(
                "HTTP error during validation. (operation_id: %s)",
                operation_id,
            )
            return InternalServerErrorResponse(message=f"Internal server error: {e!s}")

        except RequestValidationError as e:
            lock_manager.release_lock()
            logger.exception(
                "Metadata validation error. operation_id: %s",
                operation_id,
            )
            return BadRequestResponse(message=str(e))

        executor = OperationExecutor(
            client=client,
            lock_manager=lock_manager,
            history_writer=history_writer,
        )

        background_tasks.add_task(executor.execute_delete_label, operation_id, request)

    except Exception as e:
        lock_manager.release_lock()
        logger.exception(
            "Failed to schedule background task. (operation_id: %s)",
            operation_id,
        )
        return InternalServerErrorResponse(message=f"Internal server error: {e!s}")

    logger.info(
        "Accepted: delete label operation scheduled successfully. (operation_id: %s)",
        operation_id,
    )
    return AcceptedResponse(
        status="success",
        data=Data(
            operation_id=operation_id,
            summary={"message": "Delete label operation scheduled successfully"},
        ),
    )
