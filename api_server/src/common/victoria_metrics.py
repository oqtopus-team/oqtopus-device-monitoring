import json
import logging
import re
from datetime import datetime, timedelta

import httpx
from pydantic import ValidationError

from common.types.victoria_metrics import (
    MetricDataResponse,
    MetricNameListResponse,
)
from common.util import get_time
from schemas.meta import Selector

logger = logging.getLogger("api-server.victoria-metrics")

MIN_DATE = datetime.fromisoformat("1970-01-01T00:00:00Z")
RETENTION_DAYS = 365 * 100  # 100 years


class VictoriaMetricsError(Exception):
    """Custom exception for VictoriaMetrics client errors."""

    def __init__(self, message: str) -> None:
        """Initialize the VictoriaMetricsError.

        Args:
            message: Error message describing the exception

        """
        super().__init__(message)


class VictoriaMetricsClient:
    """Client for interacting with VictoriaMetrics API.

    This vm_client provides methods to query metrics, labels, and time-series data
    from a VictoriaMetrics instance.
    """

    def __init__(self, base_url: str) -> None:
        """Initialize the VictoriaMetrics vm_client.

        Args:
            base_url: Base URL of the VictoriaMetrics instance

        """
        self.base_url = base_url.rstrip("/")
        # Note: disable proxy by setting trust_env=False to ignore system proxy settings
        self.vm_client = httpx.AsyncClient(
            base_url=self.base_url, timeout=30.0, trust_env=False
        )

    async def get_metric_names(self, offset: int = 0, limit: int = 100) -> list[str]:
        """Retrieve all available metric names with pagination.

        This method queries the VictoriaMetrics API to get all metric names
        and applies pagination.

        Args:
            offset: Starting position for pagination (0-based index)
            limit: Maximum number of metric names to return

        Returns:
            list[str]: List of metric names for the current page

        """
        # Define time range for querying all metric names
        start = max(
            get_time()
            - timedelta(
                days=RETENTION_DAYS
            ),  # 100 years as retention period of `vmstorage` in docker file
            MIN_DATE + timedelta(milliseconds=1),  # avoid MIN_DATE edge case
        )
        end = get_time() + timedelta(
            days=RETENTION_DAYS
        )  # 100 years as retention period of `vmstorage` in docker file
        range_start = format(start.timestamp(), ".3f")  # Unix timestamp (milliseconds)
        range_end = format(end.timestamp(), ".3f")  # Unix timestamp (milliseconds)

        params = {
            "start": range_start,
            "end": range_end,
        }

        # Query VictoriaMetrics for all values of the __name__ label
        logger.info(
            "Endpoint: /select/0/prometheus/api/v1/label/__name__/values",
            extra={"params": params},
        )
        response = await self.vm_client.get(
            "/select/0/prometheus/api/v1/label/__name__/values",
            params=params,
        )
        response.raise_for_status()

        data = MetricNameListResponse.model_validate(response.json())
        all_metrics = data.data

        # Apply pagination
        if len(all_metrics) < offset:
            return []
        if len(all_metrics) < offset + limit:
            paginated_metrics = all_metrics[offset:]
        else:
            paginated_metrics = all_metrics[offset : offset + limit]

        return paginated_metrics

    async def get_series_labels(
        self,
        metric_name: str,
        selector: Selector,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict[str, str]]:
        """Get label sets for all series matching the selector.

        Args:
            metric_name: Name of the metric to query
            selector: label selector for filtering
            start: Optional start timestamp in ISO8601 format
            end: Optional end timestamp in ISO8601 format

        Returns:
            List of label dictionaries for matching series

        """
        # Build PromQL match expression
        match_expr = self._build_promql_from_selector(
            metric_name=metric_name, selector=selector
        )

        # Define time range for querying series labels
        if start is None:
            start = max(
                get_time()
                - timedelta(
                    days=RETENTION_DAYS
                ),  # 100 years as retention period of `vmstorage` in docker file
                MIN_DATE + timedelta(milliseconds=1),  # avoid MIN_DATE edge case
            )
        if end is None:
            end = get_time() + timedelta(
                days=RETENTION_DAYS
            )  # 100 years as retention period of `vmstorage` in docker file
        range_start = format(start.timestamp(), ".3f")  # Unix timestamp (milliseconds)
        range_end = format(end.timestamp(), ".3f")  # Unix timestamp (milliseconds)

        params = {
            "match[]": match_expr,
            "start": range_start,
            "end": range_end,
        }

        # Query VictoriaMetrics series API
        logger.info(
            "Endpoint: /select/0/prometheus/api/v1/series",
        )
        logger.info(
            "Query",
            extra={"params": params},
        )
        response = await self.vm_client.get(
            "/select/0/prometheus/api/v1/series",
            params=params,
        )
        response.raise_for_status()

        data = response.json()
        series = data.get("data", [])

        if isinstance(series, dict):
            series = [series]

        return series

    async def get_series_label_keys(
        self,
        metric_name: str,
    ) -> list[str]:
        """Get label keys for all series matching the selector.

        Args:
            metric_name: Name of the metric to query

        Returns:
            List of unique label keys for matching series

        """
        # Define time range for querying all metric names
        start = max(
            get_time()
            - timedelta(
                days=RETENTION_DAYS
            ),  # 100 years as retention period of `vmstorage` in docker file
            MIN_DATE + timedelta(milliseconds=1),  # avoid MIN_DATE edge case
        )
        end = get_time() + timedelta(
            days=RETENTION_DAYS
        )  # 100 years as retention period of `vmstorage` in docker file
        range_start = format(start.timestamp(), ".3f")  # Unix timestamp (milliseconds)
        range_end = format(end.timestamp(), ".3f")  # Unix timestamp (milliseconds)

        params = {
            "match[]": metric_name,
            "start": range_start,
            "end": range_end,
        }

        # Query VictoriaMetrics series API
        logger.info(
            "Endpoint: /select/0/prometheus/api/v1/labels",
        )
        logger.info("Query", extra={"params": params})
        response = await self.vm_client.get(
            "/select/0/prometheus/api/v1/labels", params=params
        )
        response.raise_for_status()

        data = response.json()

        return data.get("data", [])

    async def get_series_label_values(
        self,
        metric_name: str,
        label_key: str,
        offset: int = 0,
        limit: int = 100,
    ) -> list[str]:
        """Get label values for a specific label key.

        Args:
            metric_name: Name of the metric to query
            label_key: Label key to retrieve values
            offset: Starting position for pagination (0-based index)
            limit: Maximum number of label values to return

        Returns:
            List of label values for the specified label key

        """
        # Define time range for querying all metric names
        start = max(
            get_time()
            - timedelta(
                days=RETENTION_DAYS
            ),  # 100 years as retention period of `vmstorage` in docker file
            MIN_DATE + timedelta(milliseconds=1),  # avoid MIN_DATE edge case
        )
        end = get_time() + timedelta(
            days=RETENTION_DAYS
        )  # 100 years as retention period of `vmstorage` in docker file
        range_start = format(start.timestamp(), ".3f")  # Unix timestamp (milliseconds)
        range_end = format(end.timestamp(), ".3f")  # Unix timestamp (milliseconds)

        params = {
            "match[]": metric_name,
            "start": range_start,
            "end": range_end,
        }

        # Query VictoriaMetrics label values API
        logger.info(
            "Endpoint: /select/0/prometheus/api/v1/label/%s/values",
            label_key,
        )
        logger.info("Query", extra={"params": params})
        response = await self.vm_client.get(
            f"/select/0/prometheus/api/v1/label/{label_key}/values",
            params=params,
        )
        response.raise_for_status()

        data = response.json()
        label_values = data.get("data", [])

        # Apply pagination
        if len(label_values) < offset:
            return []
        if len(label_values) < offset + limit:
            label_values = label_values[offset:]
        else:
            label_values = label_values[offset : offset + limit]

        return label_values

    async def read_timeseries(
        self,
        metric_name: str,
        selector: Selector,
    ) -> list[MetricDataResponse]:
        """Export time-series data in Prometheus text format.

        This method exports time-series data from VictoriaMetrics in the
        Prometheus exposition format, suitable for re-importing after modification.

        Args:
            metric_name: Name of the metric to export
            selector: label selector for filtering

        Returns:
            Raw time-series data in Prometheus text format (JSONL)

        Raises:
            VictoriaMetricsError: If parsing the exported data fails

        """
        # Build PromQL match expression
        match_expr = self._build_promql_from_selector(
            metric_name=metric_name, selector=selector
        )

        # Prepare query parameters
        params = {"match[]": match_expr}

        # Query VictoriaMetrics export API
        logger.info("Endpoint: /select/0/prometheus/api/v1/export")
        logger.info("Query", extra={"params": params})
        response = await self.vm_client.get(
            "/select/0/prometheus/api/v1/export",
            params=params,
        )
        response.raise_for_status()

        try:
            parsed_data = self._parse_exported_data(response.text)
        except VictoriaMetricsError:
            logger.exception("Failed to parse exported data")
            raise

        return parsed_data

    async def write_timeseries(self, data: list[MetricDataResponse]) -> None:
        """Import time-series data.

        This method imports time-series data into VictoriaMetrics using
        the import API endpoint.

        Args:
            data: Time-series data (JSONL)

        """
        writen_data = []
        for entry in data:
            try:
                writen_data.append(json.dumps(entry.model_dump(), ensure_ascii=False))

            except Exception:
                logger.exception("Failed to serialize metric data")
                raise

        content = "\n".join(writen_data)

        logger.info("Endpoint: /insert/0/prometheus/api/v1/import")
        logger.info("Importing time-series data", extra={"data_count": len(data)})
        response = await self.vm_client.post(
            "/insert/0/prometheus/api/v1/import",
            headers={"Content-Type": "application/json"},
            content=content,
        )
        response.raise_for_status()

    async def delete_timeseries(
        self,
        metric_name: str,
        selector: Selector,
    ) -> None:
        """Delete time-series data matching the selector.

        This method schedules deletion of time-series data in VictoriaMetrics.
        The deletion is performed asynchronously in the background.

        Args:
            metric_name: Name of the metric to delete
            selector: Optional label selector for filtering (key-value pairs)

        """
        # Build PromQL match expression
        match_expr = self._build_promql_from_selector(
            metric_name=metric_name, selector=selector
        )

        # Prepare query parameters
        params = {"match[]": match_expr}

        # Send delete request to VictoriaMetrics
        logger.info("Endpoint: /delete/0/prometheus/api/v1/admin/tsdb/delete_series")
        logger.info("Query", extra={"params": params})
        response = await self.vm_client.delete(
            "/delete/0/prometheus/api/v1/admin/tsdb/delete_series",
            params=params,
        )
        response.raise_for_status()

    async def count_over_time(
        self,
        metric_name: str,
        selector: Selector,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> int:
        """Count the number of time-series matching the selector over a time range.

        Args:
            metric_name: Name of the metric to query
            selector: label selector for filtering
            start: Start timestamp in ISO8601 format
            end: End timestamp in ISO8601 format

        Returns:
            Count of matching time-series

        Raises:
            IndexError: If the response data is malformed
            ValueError: If the count value cannot be converted to int
            KeyError: If expected keys are missing in the response data
            VictoriaMetricsError: If count value is not found in the response

        """
        # Build PromQL match expression
        match_expr = self._build_promql_from_selector(
            metric_name=metric_name, selector=selector
        )

        if start is None:
            start = MIN_DATE
        if end is None:
            end = get_time() + timedelta(
                days=RETENTION_DAYS
            )  # 100 years as retention period of `vmstorage`

        # calculate the lookbehind window
        start_unix_ms = int(start.timestamp() * 1000)
        end_unix_ms = int(end.timestamp() * 1000)
        lookbehind_ms = end_unix_ms - start_unix_ms

        # Build PromQL query for counting series
        promql_query = f"count_over_time({match_expr}[{lookbehind_ms}ms])"

        # Execute the query
        response_data = await self._query(
            query=promql_query,
            time=end,
        )

        # Extract count from response
        count = None
        try:
            result_data = response_data["data"]["result"]

            if len(result_data) == 0:
                return 0

            if result_data:
                count = int(float(result_data[0].get("value", [0, "0"])[1]))
        except (IndexError, ValueError, KeyError):
            logger.exception("Failed to extract count from query response")
            raise

        if count is None:
            error_message = "Count value not found in query response"
            logger.error(error_message, extra={"response_data": response_data})
            raise VictoriaMetricsError(error_message)

        return count

    async def _query(
        self,
        query: str,
        time: datetime | None = None,
    ) -> dict:
        """Execute an instant query against VictoriaMetrics.

        Args:
            query: PromQL query string
            time: Optional evaluation timestamp in RFC3339 or Unix format

        Returns:
            VictoriaMetrics API response data

        """
        params = {"query": query}
        if time is not None:
            params["time"] = time.isoformat()

        logger.info("Endpoint: /select/0/prometheus/api/v1/query")
        logger.info("Query", extra={"params": params})

        response = await self.vm_client.get(
            "/select/0/prometheus/api/v1/query",
            params=params,
        )
        response.raise_for_status()

        return response.json()

    @staticmethod
    def _parse_exported_data(exported_data: str) -> list[MetricDataResponse]:
        """Parse a exported data into a dictionary of labels.

        Args:
            exported_data: The time-series data as a string of labels

        Returns:
            List of dictionaries where each dictionary represents a label

        Raises:
            VictoriaMetricsError: If parsing fails

        """
        if not exported_data.strip():
            return []

        lines = exported_data.strip().split("\n")

        parsed_data = []
        try:
            for line in lines:
                if not line.strip():
                    continue
                # Parse each line as a MetricDataResponse object
                parsed_data.append(MetricDataResponse.model_validate_json(line))
        except ValidationError as e:
            error_message = f"Failed to parse exported data: {e!s}"
            logger.exception(error_message)
            raise VictoriaMetricsError(error_message) from e

        return parsed_data

    async def close(self) -> None:
        """Close the HTTP vm_client and clean up resources."""
        await self.vm_client.aclose()

    @staticmethod
    def _build_promql_from_selector(metric_name: str, selector: Selector) -> str:
        """Build a PromQL selector string from a Selector object.

        Converts the Selector schema (with match items that can be regex)
        into a PromQL selector string.

        Args:
            metric_name: Name of the metric
            selector: Selector object with match items

        Returns:
            PromQL selector string

        Raises:
            VictoriaMetricsError: If label keys or values contain invalid characters

        """
        # Metric name must be non-empty
        if not metric_name:
            message = "Metric name cannot be empty"
            logger.error(message)
            raise VictoriaMetricsError(message)

        # If no match items, return just the metric name
        if not selector.match:
            return metric_name

        # Define regex pattern for valid Prometheus metric and label names
        # Ref: https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
        pattern = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

        # Check promql injection for metric name
        if not pattern.fullmatch(metric_name):
            message = f"Invalid metric name: {metric_name}"
            logger.error(message)
            raise VictoriaMetricsError(message)

        parts = []

        for match_item in selector.match:
            # Check promql injection for label keys
            if not pattern.fullmatch(match_item.key):
                message = f"Invalid label key: {match_item.key}"
                logger.error(message)
                raise VictoriaMetricsError(message)

            operator = "=~" if match_item.regex else "="

            parts.append(f'{match_item.key}{operator}"{match_item.value}"')

        return f"{metric_name}" + "{" + ",".join(parts) + "}"
