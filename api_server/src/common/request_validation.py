import logging
from datetime import datetime

from httpx import HTTPError

from common.victoria_metrics import VictoriaMetricsClient, VictoriaMetricsError
from schemas.meta import (
    AddLabelRequest,
    DeleteLabelRequest,
    MatchItem,
    ModifyLabelKeyRequest,
    ModifyLabelValueRequest,
    Selector,
)

MAX_TIME_SERIES_SAMPLINGS: int = 100000000  # 100 million data points

logger = logging.getLogger("api-server.request-validation")


class RequestValidationError(Exception):
    """Custom exception for request validation errors."""

    def __init__(self, message: str) -> None:
        """Initialize the exception with an error message.

        Args:
            message: The error message describing the validation failure

        """
        super().__init__(message)
        self.message = message


class RequestValidation:
    """Validates constraints of request."""

    def __init__(
        self,
        client: VictoriaMetricsClient,
    ) -> None:
        """Initialize the RequestValidation with a client.

        Args:
            client: client instance to execute queries

        """
        self._client = client

    async def validate_get_time_series_data(
        self,
        metric_name: str,
        selector: Selector,
        start: datetime | None,
        end: datetime | None,
    ) -> None:
        """Validate get time-series data request before execution.

        Args:
            metric_name: The name of the metric
            selector: The selector to match time-series
            start: Start datetime for the query range
            end: End datetime for the query range

        Raises:
            RequestValidationError: If validation fails
            HTTPError: If an error occurs during validation

        """
        logger.info("Validating get time-series data request.")

        # Check within sampling limit
        try:
            is_within_limit = await self._is_within_sampling_limit(
                client=self._client,
                metric_name=metric_name,
                selector=selector,
                start=start,
                end=end,
            )
        except HTTPError as e:
            logger.exception("Error occurred while checking sampling limit.")
            raise HTTPError(
                message="Failed to validate request due to internal error."
            ) from e
        except VictoriaMetricsError as e:
            logger.exception(
                "VictoriaMetricsError occurred while checking sampling limit."
            )
            message = f"Failed to validate request due to internal error: {e}"
            raise RequestValidationError(message=message) from e

        if not is_within_limit:
            raise RequestValidationError(message="Sampling limit exceeded.")

        logger.info("Get time-series data request validation passed.")

    async def validate_add_label(self, request: AddLabelRequest) -> None:
        """Validate add label request before background execution.

        Args:
            request: The add label request containing operation parameters

        Raises:
            RequestValidationError: If validation fails
            HTTPError: If an error occurs during validation

        """
        logger.info("Validating add label request.")

        # Check request label value is valid
        if not self._is_valid_request_label_value(request.default_value):
            raise RequestValidationError(
                message=f"Invalid \"default value\" '{request.default_value}'. "
                "Label value must not be empty."
            )

        try:
            series_labels = await self._client.get_series_labels(
                metric_name=request.metric_name,
                selector=request.selector,
            )
        except VictoriaMetricsError as e:
            logger.exception("Error occurred while fetching series labels.")
            message = f"Failed to validate request due to internal error: {e}"
            raise RequestValidationError(message=message) from e

        # Ensure selector matches exactly one time-series
        if not self._is_unique_request(series_labels):
            raise RequestValidationError(
                message="The selector must match exactly "
                "one time-series for add label operation. "
                f"{len(series_labels)} time-series matched.",
            )

        # Check if new label key already exists in the matched time-series labels
        if self._exists_label_key(series_labels[0], request.new_label_key):
            raise RequestValidationError(
                message=f"Label key '{request.new_label_key}' already exists "
                "in the matched time-series. "
            )

        # Check if adding the new label would cause overlap with existing time-series
        if not await self._can_add_label_without_timeseries_overlap(request):
            message = (
                "Adding the new label would cause overlap with existing time-series. "
            )
            raise RequestValidationError(message=message)

        # Check within sampling limit
        try:
            is_within_limit = await self._is_within_sampling_limit(
                client=self._client,
                metric_name=request.metric_name,
                selector=request.selector,
                start=None,
                end=None,
            )
        except HTTPError as e:
            logger.exception("Error occurred while checking sampling limit.")
            raise HTTPError(
                message="Failed to validate request due to internal error."
            ) from e
        except VictoriaMetricsError as e:
            logger.exception(
                "VictoriaMetricsError occurred while checking sampling limit."
            )
            message = f"Failed to validate request due to internal error: {e}"
            raise RequestValidationError(message=message) from e

        if not is_within_limit:
            raise RequestValidationError(message="Sampling limit exceeded.")

        logger.info("Add label request validation passed.")

    async def validate_modify_label_key(self, request: ModifyLabelKeyRequest) -> None:
        """Validate modify label key request before background execution.

        Args:
            request: The modify label key request containing operation parameters

        Raises:
            RequestValidationError: If validation fails
            HTTPError: If an error occurs during validation

        """
        logger.info("Validating modify label key request.")

        # Get labels for the target time-series
        try:
            target_series_labels = await self._client.get_series_labels(
                metric_name=request.metric_name,
                selector=request.selector,
                start=request.range.start,
                end=request.range.end,
            )
        except VictoriaMetricsError as e:
            logger.exception("Error occurred while fetching series labels.")
            message = f"Failed to validate request due to internal error: {e}"
            raise RequestValidationError(message=message) from e

        # Ensure selector matches exactly one time-series
        if not self._is_unique_request(target_series_labels):
            raise RequestValidationError(
                message="The selector must match exactly "
                "one time-series for modify label key operation. "
                f"{len(target_series_labels)} time-series matched.",
            )

        target_labels = target_series_labels[0]

        # Check if to_key already exists in the target time-series
        if self._exists_label_key(target_labels, request.to_key):
            raise RequestValidationError(
                message=f"Label key '{request.to_key}' already exists "
                "in the matched time-series."
            )

        # Verify that modifying the label key will not cause overlap
        # with existing time-series
        if not await self._can_modify_label_key_without_timeseries_overlap(
            request=request,
        ):
            raise RequestValidationError(
                message=f"The modified label {request.to_key} set would overlap "
                "with an existing time-series. "
                "A time-series with the same labels already exists."
            )

        # Check within sampling limit
        try:
            is_within_limit = await self._is_within_sampling_limit(
                client=self._client,
                metric_name=request.metric_name,
                selector=request.selector,
                start=request.range.start,
                end=request.range.end,
            )
        except HTTPError as e:
            logger.exception("Error occurred while checking sampling limit.")
            raise HTTPError(
                message="Failed to validate request due to internal error."
            ) from e
        except VictoriaMetricsError as e:
            logger.exception(
                "VictoriaMetricsError occurred while checking sampling limit."
            )
            message = f"Failed to validate request due to internal error: {e}"
            raise RequestValidationError(message=message) from e

        if not is_within_limit:
            raise RequestValidationError(
                message="Time-series query constraints are not satisfied."
            )

        logger.info("Modify label key request validation passed.")

    async def validate_modify_label_value(
        self, request: ModifyLabelValueRequest
    ) -> None:
        """Validate modify label value request before background execution.

        Args:
            request: The modify label value request containing operation parameters

        Raises:
            RequestValidationError: If validation fails
            HTTPError: If an error occurs during validation

        """
        logger.info("Validating modify label value request.")

        # Check request label value is valid
        if not self._is_valid_request_label_value(request.from_value):
            raise RequestValidationError(
                message=f"Invalid \"from value\" '{request.from_value}'. "
                "Label value must not be empty."
            )
        if not self._is_valid_request_label_value(request.to_value):
            raise RequestValidationError(
                message=f"Invalid \"to value\" '{request.to_value}'. "
                "Label value must not be empty."
            )

        # Get labels for the target time-series
        try:
            target_series_labels = await self._client.get_series_labels(
                metric_name=request.metric_name,
                selector=request.selector,
                start=request.range.start,
                end=request.range.end,
            )
        except VictoriaMetricsError as e:
            message = f"Failed to validate request due to internal error: {e}"
            logger.exception("Error occurred while fetching series labels.")
            raise RequestValidationError(message=message) from e

        # Ensure selector matches exactly one time-series
        if not self._is_unique_request(target_series_labels):
            raise RequestValidationError(
                message="The selector must match exactly one time-series "
                "for modify label value operation. "
                f"{len(target_series_labels)} time-series matched.",
            )

        # Check if modifying the label value would cause overlap
        if not await self._can_modify_label_value_without_timeseries_overlap(
            request=request,
        ):
            message = (
                "Modifying the label value would cause "
                "overlap with existing time-series."
            )
            raise RequestValidationError(message=message)

        # Check within sampling limit
        try:
            is_within_limit = await self._is_within_sampling_limit(
                client=self._client,
                metric_name=request.metric_name,
                selector=request.selector,
                start=request.range.start,
                end=request.range.end,
            )
        except HTTPError as e:
            logger.exception("Error occurred while checking sampling limit.")
            raise HTTPError(
                message="Failed to validate request due to internal error."
            ) from e
        except VictoriaMetricsError as e:
            logger.exception(
                "VictoriaMetricsError occurred while checking sampling limit."
            )
            message = f"Failed to validate request due to internal error: {e}"
            raise RequestValidationError(message=message) from e

        if not is_within_limit:
            raise RequestValidationError(
                message="Time-series query constraints are not satisfied."
            )

        logger.info("Modify label value request validation passed.")

    async def validate_delete_label(self, request: DeleteLabelRequest) -> None:
        """Validate delete label request before background execution.

        Args:
            request: The delete label request containing operation parameters

        Raises:
            RequestValidationError: If validation fails
            HTTPError: If an error occurs during validation

        """
        logger.info("Validating delete label request.")

        # Get labels for the target time-series
        try:
            target_series_labels = await self._client.get_series_labels(
                metric_name=request.metric_name,
                selector=request.selector,
            )
        except VictoriaMetricsError as e:
            logger.exception("Error occurred while fetching series labels.")
            message = f"Failed to validate request due to internal error: {e}"
            raise RequestValidationError(message=message) from e

        # Ensure selector matches exactly one time-series
        if not self._is_unique_request(target_series_labels):
            raise RequestValidationError(
                message="The selector must match exactly one time-series "
                "for delete label operation. "
                f"{len(target_series_labels)} time-series matched.",
            )

        # Check if delete label key exists in the target time-series
        exists_label_keys = [
            label_key
            for label_key in request.label_keys
            if not self._exists_label_key(target_series_labels[0], label_key)
        ]

        if len(exists_label_keys) > 0:
            raise RequestValidationError(
                message=f"Label keys '{', '.join(exists_label_keys)}' do not exist "
                "in the matched time-series."
            )

        # Verify that deleting the specified labels will not cause overlap
        # with existing time-series
        if not await self._can_delete_label_without_timeseries_overlap(request=request):
            raise RequestValidationError(
                message="Deleting the specified labels would cause overlap "
                "with existing time-series. "
                "A time-series with the same labels already exists."
            )

        # Check within sampling limit
        try:
            is_within_limit = await self._is_within_sampling_limit(
                client=self._client,
                metric_name=request.metric_name,
                selector=request.selector,
                start=None,
                end=None,
            )
        except HTTPError as e:
            logger.exception("Error occurred while checking sampling limit.")
            raise HTTPError(
                message="Failed to validate request due to internal error."
            ) from e
        except VictoriaMetricsError as e:
            logger.exception(
                "VictoriaMetricsError occurred while checking sampling limit."
            )
            message = f"Failed to validate request due to internal error: {e}"
            raise RequestValidationError(message=message) from e

        if not is_within_limit:
            raise RequestValidationError(
                message="Time-series query constraints are not satisfied."
            )

        logger.info("Delete label request validation passed.")

    async def validate_delete_time_series(
        self, metric_name: str, selector: Selector
    ) -> None:
        """Validate delete time-series request before execution.

        Args:
            metric_name: The name of the metric
            selector: The selector to match time-series

        Raises:
            RequestValidationError: If validation fails
            HTTPError: If an error occurs during validation

        """
        logger.info("Validating delete time-series request.")

        # Get labels for the target time-series
        try:
            target_series_labels = await self._client.get_series_labels(
                metric_name=metric_name,
                selector=selector,
            )
        except HTTPError as e:
            logger.exception("Error occurred while fetching series labels.")
            raise HTTPError(
                message="Failed to validate request due to internal error."
            ) from e

        # Ensure selector matches exactly one time-series
        if not self._is_unique_request(target_series_labels):
            raise RequestValidationError(
                message="The selector must match exactly one time-series"
                " for delete time-series operation. "
                f"{len(target_series_labels)} time-series matched.",
            )

        logger.info("Delete time-series request validation passed.")

    @staticmethod
    def _is_unique_request(series_labels: list[dict[str, str]]) -> bool:
        """Check if the request is unique.

        A request is considered unique if:
        - The selector matches exactly one time-series.

        Args:
            series_labels: List of label dictionaries for matched time-series

        Returns:
            True if the request is unique, False otherwise.

        """
        return len(series_labels) == 1

    @staticmethod
    def _is_not_overlap_request(new_series_labels: list[dict[str, str]]) -> bool:
        """Check if the request does not cause overlap.

        A request is considered not to cause overlap if:
        - The selector does not match any existing time-series.

        Args:
            new_series_labels: List of label dictionaries for matched time-series

        Returns:
            True if the request does not cause overlap, False otherwise.

        """
        return len(new_series_labels) == 0

    @staticmethod
    def _exists_label_key(series_labels: dict[str, str], label_key: str) -> bool:
        """Check if the label key exists in any of the matched time-series.

        Args:
            series_labels: List of label dictionaries for matched time-series
            label_key: The label key to check for existence

        Returns:
            True if the label key exists in any time-series, False otherwise.

        """
        return label_key in series_labels

    async def _can_add_label_without_timeseries_overlap(
        self,
        request: AddLabelRequest,
    ) -> bool:
        """Verify that adding a label will not cause time-series overlap.

        Verify that the provided set of labels, including the newly added label,
        does NOT overlap with any existing time-series.

        Args:
            request: The add label request containing operation parameters

        Returns:
            True if there is no overlap with existing time-series, False otherwise.

        """
        # Get existing selector match items
        existing_selector_match = list(request.selector.match or [])

        updated_selector_match = []
        has_empty_value_for_new_label_key = False
        for match_item in existing_selector_match:
            if match_item.key == request.new_label_key and not match_item.value:
                has_empty_value_for_new_label_key = True
                updated_selector_match.append(
                    MatchItem(key=match_item.key, value=request.default_value)
                )
            else:
                updated_selector_match.append(match_item)

        # Create a new selector match items with the new label
        if not has_empty_value_for_new_label_key:
            add_match_item = MatchItem(
                key=request.new_label_key, value=request.default_value
            )
            new_selector = Selector(match=[*updated_selector_match, add_match_item])
        else:
            new_selector = Selector(match=updated_selector_match)

        # Get series labels for the new selector
        new_series_labels = await self._client.get_series_labels(
            metric_name=request.metric_name,
            selector=new_selector,
        )

        # Check if the request does not cause overlap
        return self._is_not_overlap_request(new_series_labels)

    async def _can_modify_label_key_without_timeseries_overlap(
        self, request: ModifyLabelKeyRequest
    ) -> bool:
        """Verify that modifying a label key will not cause time-series overlap.

        Verify that the provided set of labels, including the newly added label,
        does NOT overlap with any existing time-series.

        Args:
            request: The modify label key request containing operation parameters

        Returns:
            True if there is no overlap with existing time-series, False otherwise.

        """
        # Get existing selector match items
        existing_selector_match = list(request.selector.match or [])

        # Create a new selector match items with the modified label key
        new_selector_match = []
        for match_item in existing_selector_match:
            if match_item.key == request.from_key:
                new_selector_match.append(
                    MatchItem(key=request.to_key, value=match_item.value)
                )
            else:
                new_selector_match.append(
                    MatchItem(key=match_item.key, value=match_item.value)
                )
        new_selector = Selector(match=new_selector_match)

        # Get series labels for the new selector
        new_series_labels = await self._client.get_series_labels(
            metric_name=request.metric_name,
            selector=new_selector,
            start=request.range.start,
            end=request.range.end,
        )

        return self._is_not_overlap_request(new_series_labels)

    async def _can_modify_label_value_without_timeseries_overlap(
        self, request: ModifyLabelValueRequest
    ) -> bool:
        """Verify that modifying a label value will not cause time-series overlap.

        Verify that the provided set of labels, including the modified label value,
        does NOT overlap with any existing time-series.

        Args:
            request: The modify label value request containing operation parameters

        Returns:
            True if there is no overlap with existing time-series, False otherwise.

        """
        # Get existing selector match items
        existing_selector_match = list(request.selector.match or [])

        # Replace the existing match item with the new one
        existing_match_item = MatchItem(key=request.key, value=request.from_value)
        new_match_item = MatchItem(key=request.key, value=request.to_value)

        # remove existing match item and add new match item
        if existing_match_item in existing_selector_match:
            existing_selector_match.remove(existing_match_item)
        existing_selector_match.append(new_match_item)

        # Create a new selector match items with the modified label value
        new_selector = Selector(match=existing_selector_match)

        # Get series labels for the new selector
        new_series_labels = await self._client.get_series_labels(
            metric_name=request.metric_name,
            selector=new_selector,
            start=request.range.start,
            end=request.range.end,
        )

        return self._is_not_overlap_request(new_series_labels)

    async def _can_delete_label_without_timeseries_overlap(
        self,
        request: DeleteLabelRequest,
    ) -> bool:
        """Verify that deleting labels will not cause time-series overlap.

        Verify that the provided set of labels, after deleting specified labels,
        does NOT overlap with any existing time-series.

        Args:
            request: The delete label request containing operation parameters

        Returns:
            True if there is no overlap with existing time-series, False otherwise.

        """
        # Get existing selector match items
        existing_selector_match = list(request.selector.match or [])

        # Create a new selector match items after deleting specified labels
        deleted_selector_match = [
            match_item
            for match_item in existing_selector_match
            if match_item.key not in request.label_keys
        ]
        new_selector = Selector(match=deleted_selector_match)

        # Get series labels for the new selector
        new_series_labels = await self._client.get_series_labels(
            metric_name=request.metric_name,
            selector=new_selector,
        )

        # Check if the request does not cause overlap
        # Note: After deleting labels, at least one time-series (the original one)
        return self._is_unique_request(new_series_labels)

    @staticmethod
    async def _is_within_sampling_limit(
        client: VictoriaMetricsClient,
        metric_name: str,
        selector: Selector,
        start: datetime | None,
        end: datetime | None,
    ) -> bool:
        """Check if the time-series query satisfies constraints.

        Args:
            client: VictoriaMetricsClient instance to execute queries
            metric_name: Name of the metric to query
            selector: Selector object with match items
            start: Start datetime for the query range
            end: End datetime for the query range

        Returns:
            True if the query satisfies constraints, False otherwise.

        Raises:
            HTTPError: If an error occurs during the query execution
            VictoriaMetricsError: If an error occurs in VictoriaMetrics client

        """
        try:
            num_samplings = await client.count_over_time(
                metric_name=metric_name, selector=selector, start=start, end=end
            )
        except HTTPError:
            logger.exception("Error occurred while counting over time.")
            raise
        except VictoriaMetricsError:
            logger.exception("VictoriaMetricsError occurred while counting over time.")
            raise
        return num_samplings <= MAX_TIME_SERIES_SAMPLINGS

    @staticmethod
    def _is_valid_request_label_value(value: str) -> bool:
        """Check if the label value is valid.

        A valid label value must satisfy the following conditions:
        - Must not be empty

        Args:
            value: The label value to validate

        Returns:
            True if the label value is valid, False otherwise.

        """
        return bool(value)
