from pydantic import BaseModel, Field


class MetricNameListResponse(BaseModel):
    """Class for metric names response from VictoriaMetrics."""

    status: str = Field(description="Response status")
    data: list[str] = Field(description="List of metric names")


class MetricDataResponse(BaseModel):
    """Class for exported data from VictoriaMetrics."""

    metric: dict[str, str] = Field(
        description="Metric metadata including labels and values"
    )
    values: list[float | int | None] = Field(description="List of metric values")
    timestamps: list[int] = Field(description="List of timestamps for the values")
