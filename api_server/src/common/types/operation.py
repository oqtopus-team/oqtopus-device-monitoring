from datetime import datetime
from enum import StrEnum
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, FieldSerializationInfo, field_serializer

from schemas.meta import ProcessStatus


class OperationSteps(StrEnum):
    """Enumeration of operation steps for metadata operations."""

    EXTRACTING = "extracting"
    TRANSFORMING = "transforming"
    INGESTING = "ingesting"
    DELETING = "deleting"
    FINISHING = "finishing"


class OperationType(StrEnum):
    """Enumeration of operation types for metadata operations."""

    ADD_LABEL = "add"
    MODIFY_KEY = "modify/key"
    MODIFY_VALUE = "modify/value"
    DELETE_LABEL = "delete_label"
    DELETE_TIME_SERIES = "delete_time_series"


class OperationError(BaseModel):
    """Class for operation error details."""

    step: OperationSteps = Field(description="Step at which the error occurred")
    message: str = Field(description="Error message describing the failure")
    cleanup_instructions: str = Field(
        description="Instructions for cleaning up after the error"
    )


class OperationRequestRange(BaseModel):
    """Class for operation request time range."""

    start_time: str = Field(description="Start time for the operation range")
    end_time: str = Field(description="End time for the operation range")


class OperationRequestSelectorDetails(BaseModel):
    """Class for operation request selector details."""

    key: str = Field(description="Label key to filter time series")
    value: str | None = Field(
        default=None, description="Label value to filter time series"
    )
    regex: bool = Field(
        default=False, description="Indicates if the value is a regex pattern"
    )


class OperationRequestSelector(BaseModel):
    """Class for operation request selector details."""

    matchers: list[OperationRequestSelectorDetails] = Field(
        description="List of label matchers for selecting time series"
    )


class StepInfo(BaseModel):
    """Class for operation step details."""

    name: OperationSteps = Field(description="Name of the operation step")
    status: ProcessStatus = Field(description="Status of the operation step")
    start_time: datetime = Field(description="Start time of the operation step")
    completed_at: datetime | None = Field(
        default=None, description="End time of the operation step"
    )

    @field_serializer("start_time", "completed_at", mode="plain")
    def _serialize_datetime(self, v: datetime, info: FieldSerializationInfo) -> str:  # noqa: PLR6301  # Pydantic requires instance method
        if v is None:
            return ""
        try:
            tz = info.context
            v = v.astimezone(tz)
        except (KeyError, TypeError, ZoneInfoNotFoundError):
            v = v.astimezone(ZoneInfo("UTC"))
        return v.strftime("%Y-%m-%dT%H:%M:%S%z")


class Steps(BaseModel):
    """Class for operation steps progress."""

    steps: list[StepInfo] = Field(
        description="List of steps with their progress information"
    )


class OperationHistory(BaseModel):
    """Class for operation history records."""

    operation_id: str = Field(
        description="Operation ID", examples=["20251126T123456_1"]
    )

    operation_type: OperationType = Field(description="Type of the operation")

    steps: OperationSteps = Field(description="Current step of the operation")

    start_at: datetime = Field(description="Start time of the operation")

    end_at: datetime | None = Field(
        default=None, description="End time of the operation"
    )

    request: dict = Field(description="Details of the operation request")

    progress: Steps | None = Field(
        default=None, description="Progress details of the operation"
    )

    error: OperationError | None = Field(
        default=None, description="Error message if the operation failed."
    )

    @field_serializer("start_at", "end_at", mode="plain")
    def _serialize_datetime(self, v: datetime, info: FieldSerializationInfo) -> str:  # noqa: PLR6301  # Pydantic requires instance method
        if v is None:
            return ""
        try:
            tz = info.context
            v = v.astimezone(tz)
        except (KeyError, TypeError, ZoneInfoNotFoundError):
            v = v.astimezone(ZoneInfo("UTC"))
        return v.strftime("%Y-%m-%dT%H:%M:%S%z")
