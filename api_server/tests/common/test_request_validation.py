from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from httpx import HTTPError

from common.request_validation import (
    MAX_TIME_SERIES_SAMPLINGS,
    RequestValidation,
    RequestValidationError,
)
from common.victoria_metrics import VictoriaMetricsError
from schemas.meta import (
    AddLabelRequest,
    DeleteLabelRequest,
    MatchItem,
    ModifyLabelKeyRequest,
    ModifyLabelValueRequest,
    Selector,
    TimeRange,
)


@pytest.fixture
def validator(client: AsyncMock) -> RequestValidation:
    return RequestValidation(client=client)


@pytest.fixture
def selector() -> Selector:
    match_item1 = MatchItem(key="old", value="old_value", regex=False)
    match_item2 = MatchItem(key="test_key", value="test_value", regex=False)
    return Selector(match=[match_item1, match_item2])


@pytest.mark.asyncio
async def test_validate_get_time_series_data_within_sampling_limit_succeeds(
    validator: RequestValidation,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    monkeypatch.setattr(
        validator,
        "_is_within_sampling_limit",
        AsyncMock(return_value=True),
    )

    # Act / Assert
    await validator.validate_get_time_series_data(
        metric_name="cpu",
        selector=selector,
        start=None,
        end=None,
    )


@pytest.mark.asyncio
async def test_validate_get_time_series_data_when_sampling_exceeds_limit_fails(
    validator: RequestValidation,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    monkeypatch.setattr(
        validator,
        "_is_within_sampling_limit",
        AsyncMock(return_value=False),
    )

    # Act / Assert
    with pytest.raises(RequestValidationError, match=r"Sampling limit exceeded."):
        await validator.validate_get_time_series_data(
            metric_name="cpu",
            selector=selector,
            start=None,
            end=None,
        )


@pytest.mark.asyncio
async def test_validate_get_time_series_data_when_checking_sampling_limit_https_error_fails(
    validator: RequestValidation,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    monkeypatch.setattr(
        validator,
        "_is_within_sampling_limit",
        AsyncMock(side_effect=HTTPError("Connection error")),
    )

    # Act / Assert
    with pytest.raises(HTTPError):
        await validator.validate_get_time_series_data(
            metric_name="cpu",
            selector=selector,
            start=None,
            end=None,
        )


@pytest.mark.asyncio
async def test_validate_add_label_with_valid_request_succeeds(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = AddLabelRequest(
        metric_name="cpu", new_label_key="env", default_value="prod", selector=selector
    )
    client.get_series_labels.return_value = [{"__name__": "cpu"}]

    monkeypatch.setattr(
        validator,
        "_can_add_label_without_timeseries_overlap",
        AsyncMock(return_value=True),
    )

    monkeypatch.setattr(
        validator,
        "_is_within_sampling_limit",
        AsyncMock(return_value=True),
    )

    # Act / Assert
    await validator.validate_add_label(request)


@pytest.mark.asyncio
async def test_validate_add_label_when_get_time_series_data_fails(
    validator: RequestValidation, client: AsyncMock, selector: Selector
):
    # Arrange
    request = AddLabelRequest(
        metric_name="cpu", new_label_key="env", default_value="prod", selector=selector
    )
    client.get_series_labels.side_effect = HTTPError("Connection error")
    # Act / Assert
    with pytest.raises(HTTPError):
        await validator.validate_add_label(request)


@pytest.mark.asyncio
async def test_validate_add_label_when_multiple_series_labels_exist_fails(
    validator: RequestValidation, client: AsyncMock, selector: Selector
):
    # Arrange
    request = AddLabelRequest(
        metric_name="cpu", new_label_key="env", default_value="prod", selector=selector
    )
    client.get_series_labels.return_value = [
        {"label1": "value1"},
        {"label2": "value2"},
    ]

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_add_label(request)


@pytest.mark.asyncio
async def test_validate_add_label_when_label_key_exists_fails(
    validator: RequestValidation, client: AsyncMock, selector: Selector
):
    # Arrange
    request = AddLabelRequest(
        metric_name="cpu", new_label_key="env", default_value="prod", selector=selector
    )
    client.get_series_labels.return_value = [{"env": "prod"}]

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_add_label(request)


@pytest.mark.asyncio
async def test_validate_add_label_when_timeseries_overlap_fails(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = AddLabelRequest(
        metric_name="cpu", new_label_key="env", default_value="prod", selector=selector
    )
    client.get_series_labels.return_value = [{"__name__": "cpu"}]

    monkeypatch.setattr(
        validator,
        "_can_add_label_without_timeseries_overlap",
        AsyncMock(return_value=False),
    )

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_add_label(request)


@pytest.mark.asyncio
async def test_validate_add_label_when_checking_sampling_limit_https_error_fails(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = AddLabelRequest(
        metric_name="cpu", new_label_key="env", default_value="prod", selector=selector
    )
    client.get_series_labels.return_value = [{"__name__": "cpu"}]

    monkeypatch.setattr(
        validator,
        "_can_add_label_without_timeseries_overlap",
        AsyncMock(return_value=True),
    )

    monkeypatch.setattr(
        validator,
        "_is_within_sampling_limit",
        AsyncMock(side_effect=HTTPError("Connection error")),
    )

    # Act / Assert
    with pytest.raises(HTTPError):
        await validator.validate_add_label(request)


@pytest.mark.asyncio
async def test_validate_add_label_when_sampling_limit_exceeded_fails(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = AddLabelRequest(
        metric_name="cpu", new_label_key="env", default_value="prod", selector=selector
    )
    client.get_series_labels.return_value = [{"__name__": "cpu"}]

    monkeypatch.setattr(
        validator,
        "_can_add_label_without_timeseries_overlap",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        validator,
        "_is_within_sampling_limit",
        AsyncMock(return_value=False),
    )

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_add_label(request)


@pytest.mark.asyncio
async def test_validate_modify_label_key_with_valid_request_succeeds(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelKeyRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        from_key="from",
        to_key="to",
    )
    client.get_series_labels.return_value = [{"from": "value"}]

    monkeypatch.setattr(
        validator,
        "_can_modify_label_key_without_timeseries_overlap",
        AsyncMock(return_value=True),
    )

    monkeypatch.setattr(
        validator,
        "_is_within_sampling_limit",
        AsyncMock(return_value=True),
    )

    # Act / Assert
    await validator.validate_modify_label_key(request)


@pytest.mark.asyncio
async def test_validate_modify_label_key_when_get_time_series_data_fails(
    validator: RequestValidation, client: AsyncMock, selector: Selector
):
    # Arrange
    request = ModifyLabelKeyRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        from_key="from",
        to_key="to",
    )
    client.get_series_labels.side_effect = HTTPError("Connection error")
    # Act / Assert
    with pytest.raises(HTTPError):
        await validator.validate_modify_label_key(request)


@pytest.mark.asyncio
async def test_validate_modify_label_key_when_multiple_series_labels_exist_fails(
    validator: RequestValidation, client: AsyncMock, selector: Selector
):
    # Arrange
    request = ModifyLabelKeyRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        from_key="from",
        to_key="to",
    )
    client.get_series_labels.return_value = [
        {"label1": "value1"},
        {"label2": "value2"},
    ]

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_modify_label_key(request)


@pytest.mark.asyncio
async def test_validate_modify_label_key_when_from_key_missing_fails(
    validator: RequestValidation, client: AsyncMock, selector: Selector
):
    # Arrange
    request = ModifyLabelKeyRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        from_key="from",
        to_key="to",
    )
    client.get_series_labels.return_value = [{"other_key": "value"}]

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_modify_label_key(request)


@pytest.mark.asyncio
async def test_validate_modify_label_key_when_timeseries_overlap_fails(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelKeyRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        from_key="from",
        to_key="to",
    )
    client.get_series_labels.return_value = [{"from": "value"}]

    monkeypatch.setattr(
        validator,
        "_can_modify_label_key_without_timeseries_overlap",
        AsyncMock(return_value=False),
    )

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_modify_label_key(request)


@pytest.mark.asyncio
async def test_validate_modify_label_key_when_checking_sampling_limit_https_error_fails(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelKeyRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        from_key="from",
        to_key="to",
    )
    client.get_series_labels.return_value = [{"from": "value"}]

    monkeypatch.setattr(
        validator,
        "_can_modify_label_key_without_timeseries_overlap",
        AsyncMock(return_value=True),
    )

    monkeypatch.setattr(
        validator,
        "_is_within_sampling_limit",
        AsyncMock(side_effect=HTTPError("Connection error")),
    )

    # Act / Assert
    with pytest.raises(HTTPError):
        await validator.validate_modify_label_key(request)


@pytest.mark.asyncio
async def test_validate_modify_label_key_when_sampling_limit_exceeded_fails(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelKeyRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        from_key="from",
        to_key="to",
    )
    client.get_series_labels.return_value = [{"from": "value"}]

    monkeypatch.setattr(
        validator,
        "_can_modify_label_key_without_timeseries_overlap",
        AsyncMock(return_value=True),
    )

    monkeypatch.setattr(
        validator,
        "_is_within_sampling_limit",
        AsyncMock(return_value=False),
    )

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_modify_label_key(request)


@pytest.mark.asyncio
async def test_validate_modify_label_value_with_valid_request_succeeds(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    client.get_series_labels.return_value = [{"env": "old"}]

    monkeypatch.setattr(
        validator,
        "_can_modify_label_value_without_timeseries_overlap",
        AsyncMock(return_value=True),
    )

    monkeypatch.setattr(
        validator,
        "_is_within_sampling_limit",
        AsyncMock(return_value=True),
    )

    # Act / Assert
    await validator.validate_modify_label_value(request)


@pytest.mark.asyncio
async def test_validate_modify_label_value_when_get_time_series_data_fails(
    validator: RequestValidation, client: AsyncMock, selector: Selector
):
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    client.get_series_labels.side_effect = HTTPError("Connection error")
    # Act / Assert
    with pytest.raises(HTTPError):
        await validator.validate_modify_label_value(request)


@pytest.mark.asyncio
async def test_validate_modify_label_value_when_multiple_series_labels_exist_fails(
    validator: RequestValidation, client: AsyncMock, selector: Selector
):
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    client.get_series_labels.return_value = [
        {"label1": "value1"},
        {"label2": "value2"},
    ]

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_modify_label_value(request)


@pytest.mark.asyncio
async def test_validate_modify_label_key_when_key_exists_fails(
    validator: RequestValidation, client: AsyncMock, selector: Selector
):
    # Arrange
    request = ModifyLabelKeyRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        from_key="from",
        to_key="to",
    )
    client.get_series_labels.return_value = [{"to": "value"}]

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_modify_label_key(request)


@pytest.mark.asyncio
async def test_validate_modify_label_value_when_timeseries_overlap_fails(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    client.get_series_labels.return_value = [{"env": "old"}]

    monkeypatch.setattr(
        validator,
        "_can_modify_label_value_without_timeseries_overlap",
        AsyncMock(return_value=False),
    )

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_modify_label_value(request)


@pytest.mark.asyncio
async def test_validate_modify_label_value_when_checking_sampling_limit_https_error_fails(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    client.get_series_labels.return_value = [{"env": "old"}]

    monkeypatch.setattr(
        validator,
        "_can_modify_label_value_without_timeseries_overlap",
        AsyncMock(return_value=True),
    )

    monkeypatch.setattr(
        validator,
        "_is_within_sampling_limit",
        AsyncMock(side_effect=HTTPError("Connection error")),
    )

    # Act / Assert
    with pytest.raises(HTTPError):
        await validator.validate_modify_label_value(request)


@pytest.mark.asyncio
async def test_validate_modify_label_value_when_sampling_limit_exceeded_fails(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    client.get_series_labels.return_value = [{"env": "old"}]

    monkeypatch.setattr(
        validator,
        "_can_modify_label_value_without_timeseries_overlap",
        AsyncMock(return_value=True),
    )

    monkeypatch.setattr(
        validator,
        "_is_within_sampling_limit",
        AsyncMock(return_value=False),
    )

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_modify_label_value(request)


@pytest.mark.asyncio
async def test_validate_delete_label_with_valid_request_succeeds(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = DeleteLabelRequest(
        metric_name="cpu", label_keys=["env"], selector=selector
    )
    client.get_series_labels.return_value = [{"env": "prod"}]

    monkeypatch.setattr(
        validator,
        "_can_delete_label_without_timeseries_overlap",
        AsyncMock(return_value=True),
    )

    monkeypatch.setattr(
        validator,
        "_is_within_sampling_limit",
        AsyncMock(return_value=True),
    )

    # Act / Assert
    await validator.validate_delete_label(request)


@pytest.mark.asyncio
async def test_validate_delete_label_when_get_time_series_data_fails(
    validator: RequestValidation, client: AsyncMock, selector: Selector
):
    # Arrange
    request = DeleteLabelRequest(
        metric_name="cpu", label_keys=["env"], selector=selector
    )
    client.get_series_labels.side_effect = HTTPError("Connection error")
    # Act / Assert
    with pytest.raises(HTTPError):
        await validator.validate_delete_label(request)


@pytest.mark.asyncio
async def test_validate_delete_label_when_multiple_series_labels_exist_fails(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
):
    # Arrange
    request = DeleteLabelRequest(
        metric_name="cpu", label_keys=["env"], selector=selector
    )
    client.get_series_labels.return_value = [{"env": "prod"}, {"env": "dev"}]

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_delete_label(request)


@pytest.mark.asyncio
async def test_validate_delete_label_when_label_keys_missing_fails(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
):
    # Arrange
    request = DeleteLabelRequest(
        metric_name="cpu", label_keys=["env", "zone"], selector=selector
    )
    client.get_series_labels.return_value = [{"env": "prod"}]

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_delete_label(request)


@pytest.mark.asyncio
async def test_validate_delete_label_when_timeseries_overlap_fails(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = DeleteLabelRequest(
        metric_name="cpu", label_keys=["env"], selector=selector
    )
    client.get_series_labels.return_value = [{"env": "prod"}]

    monkeypatch.setattr(
        validator,
        "_can_delete_label_without_timeseries_overlap",
        AsyncMock(return_value=False),
    )

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_delete_label(request)


@pytest.mark.asyncio
async def test_validate_delete_label_when_checking_sampling_limit_https_error_fails(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = DeleteLabelRequest(
        metric_name="cpu", label_keys=["env"], selector=selector
    )
    client.get_series_labels.return_value = [{"env": "prod"}]

    monkeypatch.setattr(
        validator,
        "_can_delete_label_without_timeseries_overlap",
        AsyncMock(return_value=True),
    )

    monkeypatch.setattr(
        validator,
        "_is_within_sampling_limit",
        AsyncMock(side_effect=HTTPError("Connection error")),
    )

    # Act / Assert
    with pytest.raises(HTTPError):
        await validator.validate_delete_label(request)


@pytest.mark.asyncio
async def test_validate_delete_label_when_sampling_limit_exceeded_fails(
    validator: RequestValidation,
    client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    selector: Selector,
):
    # Arrange
    request = DeleteLabelRequest(
        metric_name="cpu", label_keys=["env"], selector=selector
    )
    client.get_series_labels.return_value = [{"env": "prod"}]

    monkeypatch.setattr(
        validator,
        "_can_delete_label_without_timeseries_overlap",
        AsyncMock(return_value=True),
    )

    monkeypatch.setattr(
        validator,
        "_is_within_sampling_limit",
        AsyncMock(return_value=False),
    )

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_delete_label(request)


@pytest.mark.asyncio
async def test_validate_delete_time_series_with_valid_request_succeeds(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
):
    # Arrange
    client.get_series_labels.return_value = [{"env": "prod"}]

    # Act / Assert
    await validator.validate_delete_time_series("cpu", selector)


@pytest.mark.asyncio
async def test_validate_delete_time_series_when_get_time_series_data_fails(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
):
    # Arrange
    client.get_series_labels.side_effect = HTTPError("Connection error")
    # Act / Assert
    with pytest.raises(HTTPError):
        await validator.validate_delete_time_series("cpu", selector)


@pytest.mark.asyncio
async def test_validate_delete_time_series_when_multiple_series_labels_exist_fails(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
):
    # Arrange
    client.get_series_labels.return_value = [{"env": "prod"}, {"env": "dev"}]

    # Act / Assert
    with pytest.raises(RequestValidationError):
        await validator.validate_delete_time_series("cpu", selector)


def test_is_unique_request_when_series_missing_returns_false(
    validator: RequestValidation,
) -> None:
    # Arrange
    series_labels: list[dict[str, str]] = []

    # Act
    result = validator._is_unique_request(series_labels)

    # Assert
    assert result is False


def test_is_unique_request_with_single_label_returns_true(
    validator: RequestValidation,
) -> None:
    # Arrange
    series_labels = [{"env": "prod"}]

    # Act
    result = validator._is_unique_request(series_labels)

    # Assert
    assert result is True


def test_is_unique_request_with_multiple_labels_returns_false(
    validator: RequestValidation,
) -> None:
    # Arrange
    series_labels = [{"label_key_1": "label_value_1"}, {"label_key_2": "label_value_2"}]

    # Act
    result = validator._is_unique_request(series_labels)

    # Assert
    assert result is False


def test_is_not_overlap_request_with_no_labels_returns_true(
    validator: RequestValidation,
) -> None:
    # Arrange
    series_labels: list[dict[str, str]] = []

    # Act
    result = validator._is_not_overlap_request(series_labels)

    # Assert
    assert result is True


def test_is_not_overlap_request_with_label_returns_false(
    validator: RequestValidation,
) -> None:
    # Arrange
    series_labels = [{"env": "prod"}]

    # Act
    result = validator._is_not_overlap_request(series_labels)

    # Assert
    assert result is False


def test_exists_label_key_when_label_present_returns_true(
    validator: RequestValidation,
) -> None:
    # Arrange
    series_labels = {"label_key": "label_value"}

    # Act
    result = validator._exists_label_key(series_labels, "label_key")

    # Assert
    assert result is True


def test_exists_label_key_when_label_missing_returns_false(
    validator: RequestValidation,
) -> None:
    # Arrange
    series_labels = {"other_key": "label_value"}

    # Act
    result = validator._exists_label_key(series_labels, "label_key")

    # Assert
    assert result is False


def test_is_valid_request_label_value_when_empty_returns_false(
    validator: RequestValidation,
) -> None:
    # Arrange
    value = ""

    # Act
    result = validator._is_valid_request_label_value(value)

    # Assert
    assert result is False


def test_is_valid_request_label_value_when_non_empty_returns_true(
    validator: RequestValidation,
) -> None:
    # Arrange
    value = "prod"

    # Act
    result = validator._is_valid_request_label_value(value)

    # Assert
    assert result is True


@pytest.mark.asyncio
async def test_validate_add_label_when_default_value_is_empty_fails_and_skips_series_lookup(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    request = AddLabelRequest(
        metric_name="cpu",
        new_label_key="env",
        default_value="",
        selector=selector,
    )

    # Act / Assert
    with pytest.raises(RequestValidationError, match='Invalid "default value"'):
        await validator.validate_add_label(request)

    client.get_series_labels.assert_not_called()


@pytest.mark.asyncio
async def test_can_add_label_without_overlap_returns_true(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    add_label_request = AddLabelRequest(
        metric_name="metric",
        new_label_key="new_key",
        default_value="default",
        selector=selector,
    )
    client.get_series_labels.return_value = []

    # Act
    result = await validator._can_add_label_without_timeseries_overlap(
        request=add_label_request,
    )

    # Assert
    assert result is True


@pytest.mark.asyncio
async def test_can_add_label_with_overlap_returns_false(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    add_label_request = AddLabelRequest(
        metric_name="metric",
        new_label_key="key",
        default_value="default",
        selector=selector,
    )
    client.get_series_labels.return_value = [{"__name__": "metric", "key": "value"}]

    # Act
    result = await validator._can_add_label_without_timeseries_overlap(
        request=add_label_request,
    )

    # Assert
    assert result is False


@pytest.mark.asyncio
async def test_can_add_label_with_empty_value_for_new_label_key_replaces_value(
    validator: RequestValidation,
    client: AsyncMock,
) -> None:
    # Arrange
    selector = Selector(match=[MatchItem(key="env", value="", regex=False)])
    add_label_request = AddLabelRequest(
        metric_name="metric",
        new_label_key="env",
        default_value="prod",
        selector=selector,
    )
    client.get_series_labels.return_value = []

    # Act
    result = await validator._can_add_label_without_timeseries_overlap(
        request=add_label_request,
    )

    # Assert
    assert result is True
    assert client.get_series_labels.call_count == 1
    called_selector = client.get_series_labels.call_args.kwargs["selector"]
    assert called_selector == Selector(
        match=[MatchItem(key="env", value="prod", regex=False)]
    )


@pytest.mark.asyncio
async def test_can_add_label_with_empty_value_for_new_label_key_and_overlap_returns_false(
    validator: RequestValidation,
    client: AsyncMock,
) -> None:
    # Arrange
    selector = Selector(
        match=[
            MatchItem(key="instance", value="dummy_instance", regex=False),
            MatchItem(key="env", value="", regex=False),
        ]
    )
    add_label_request = AddLabelRequest(
        metric_name="metric",
        new_label_key="env",
        default_value="prod",
        selector=selector,
    )
    client.get_series_labels.return_value = [{"__name__": "metric", "env": "prod"}]

    # Act
    result = await validator._can_add_label_without_timeseries_overlap(
        request=add_label_request,
    )

    # Assert
    assert result is False
    called_selector = client.get_series_labels.call_args.kwargs["selector"]
    assert called_selector == Selector(
        match=[
            MatchItem(key="instance", value="dummy_instance", regex=False),
            MatchItem(key="env", value="prod", regex=False),
        ]
    )


@pytest.mark.asyncio
async def test_can_add_label_when_selector_match_is_none_adds_new_match_item(
    validator: RequestValidation,
    client: AsyncMock,
) -> None:
    # Arrange
    selector = Selector(match=None)
    add_label_request = AddLabelRequest(
        metric_name="metric",
        new_label_key="new_key",
        default_value="default",
        selector=selector,
    )
    client.get_series_labels.return_value = []

    # Act
    result = await validator._can_add_label_without_timeseries_overlap(
        request=add_label_request,
    )

    # Assert
    assert result is True
    called_selector = client.get_series_labels.call_args.kwargs["selector"]
    assert called_selector == Selector(
        match=[MatchItem(key="new_key", value="default", regex=False)]
    )


@pytest.mark.asyncio
async def test_can_add_label_without_overlap_when_get_series_labels_raises_http_error(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    add_label_request = AddLabelRequest(
        metric_name="metric",
        new_label_key="new_key",
        default_value="default",
        selector=selector,
    )
    client.get_series_labels.side_effect = HTTPError("Connection error")

    # Act / Assert
    with pytest.raises(HTTPError):
        await validator._can_add_label_without_timeseries_overlap(
            request=add_label_request,
        )


@pytest.mark.asyncio
async def test_can_add_label_without_overlap_when_get_series_labels_raises_vm_error(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    add_label_request = AddLabelRequest(
        metric_name="metric",
        new_label_key="new_key",
        default_value="default",
        selector=selector,
    )
    client.get_series_labels.side_effect = VictoriaMetricsError("vm error")

    # Act / Assert
    with pytest.raises(VictoriaMetricsError):
        await validator._can_add_label_without_timeseries_overlap(
            request=add_label_request,
        )


@pytest.mark.asyncio
async def test_can_modify_label_key_without_overlap_returns_true(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    modify_label_key_request = ModifyLabelKeyRequest(
        metric_name="metric",
        range=TimeRange(),
        selector=selector,
        from_key="old",
        to_key="new",
    )
    client.get_series_labels.return_value = []

    # Act
    result = await validator._can_modify_label_key_without_timeseries_overlap(
        request=modify_label_key_request,
    )

    # Assert
    assert result is True


@pytest.mark.asyncio
async def test_can_modify_label_key_with_overlap_returns_false(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    modify_label_key_request = ModifyLabelKeyRequest(
        metric_name="metric",
        range=TimeRange(),
        selector=selector,
        from_key="old",
        to_key="new",
    )
    client.get_series_labels.return_value = [
        {"__name__": "metric", "old": "label_value", "new": "label_value"}
    ]

    # Act
    result = await validator._can_modify_label_key_without_timeseries_overlap(
        request=modify_label_key_request,
    )

    # Assert
    assert result is False


@pytest.mark.asyncio
async def test_can_modify_label_key_replaces_from_key_and_passes_range(
    validator: RequestValidation,
    client: AsyncMock,
) -> None:
    # Arrange
    start = datetime(1970, 1, 1, 1, 1, 1, tzinfo=UTC)
    end = datetime(1970, 1, 1, 1, 1, 2, tzinfo=UTC)
    selector = Selector(
        match=[
            MatchItem(key="from", value="from_value", regex=False),
            MatchItem(key="instance", value="dummy", regex=False),
        ]
    )
    modify_label_key_request = ModifyLabelKeyRequest(
        metric_name="metric",
        range=TimeRange(start=start, end=end),
        selector=selector,
        from_key="from",
        to_key="to",
    )
    client.get_series_labels.return_value = []

    # Act
    result = await validator._can_modify_label_key_without_timeseries_overlap(
        request=modify_label_key_request,
    )

    # Assert
    assert result is True
    call_kwargs = client.get_series_labels.call_args.kwargs
    assert call_kwargs["selector"] == Selector(
        match=[
            MatchItem(key="to", value="from_value", regex=False),
            MatchItem(key="instance", value="dummy", regex=False),
        ]
    )
    assert call_kwargs["start"] == start
    assert call_kwargs["end"] == end


@pytest.mark.asyncio
async def test_can_modify_label_key_without_overlap_when_get_series_labels_raises_http_error(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    request = ModifyLabelKeyRequest(
        metric_name="metric",
        range=TimeRange(),
        selector=selector,
        from_key="old",
        to_key="new",
    )
    client.get_series_labels.side_effect = HTTPError("Connection error")

    # Act / Assert
    with pytest.raises(HTTPError):
        await validator._can_modify_label_key_without_timeseries_overlap(
            request=request
        )


@pytest.mark.asyncio
async def test_can_modify_label_key_without_overlap_when_get_series_labels_raises_vm_error(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    request = ModifyLabelKeyRequest(
        metric_name="metric",
        range=TimeRange(),
        selector=selector,
        from_key="old",
        to_key="new",
    )
    client.get_series_labels.side_effect = VictoriaMetricsError("vm error")

    # Act / Assert
    with pytest.raises(VictoriaMetricsError):
        await validator._can_modify_label_key_without_timeseries_overlap(
            request=request
        )


@pytest.mark.asyncio
async def test_can_modify_label_value_without_overlap_returns_true(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    modify_label_value_request = ModifyLabelValueRequest(
        metric_name="metric",
        range=TimeRange(),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    client.get_series_labels.return_value = []

    # Act
    result = await validator._can_modify_label_value_without_timeseries_overlap(
        request=modify_label_value_request,
    )

    # Assert
    assert result is True


@pytest.mark.asyncio
async def test_can_modify_label_value_with_overlap_returns_false(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    modify_label_value_request = ModifyLabelValueRequest(
        metric_name="metric",
        range=TimeRange(),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    client.get_series_labels.return_value = [{"__name__": "metric", "env": "new"}]

    # Act
    result = await validator._can_modify_label_value_without_timeseries_overlap(
        request=modify_label_value_request,
    )

    # Assert
    assert result is False


@pytest.mark.asyncio
async def test_can_modify_label_value_replaces_from_value_and_passes_range(
    validator: RequestValidation,
    client: AsyncMock,
) -> None:
    # Arrange
    start = datetime(1970, 1, 1, 1, 1, 1, tzinfo=UTC)
    end = datetime(1970, 1, 1, 1, 1, 2, tzinfo=UTC)
    selector = Selector(
        match=[
            MatchItem(key="env", value="old", regex=False),
            MatchItem(key="instance", value="dummy", regex=False),
        ]
    )
    modify_label_value_request = ModifyLabelValueRequest(
        metric_name="metric",
        range=TimeRange(start=start, end=end),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    client.get_series_labels.return_value = []

    # Act
    result = await validator._can_modify_label_value_without_timeseries_overlap(
        request=modify_label_value_request,
    )

    # Assert
    assert result is True
    call_kwargs = client.get_series_labels.call_args.kwargs
    assert call_kwargs["selector"] == Selector(
        match=[
            MatchItem(key="instance", value="dummy", regex=False),
            MatchItem(key="env", value="new", regex=False),
        ]
    )
    assert call_kwargs["start"] == start
    assert call_kwargs["end"] == end


@pytest.mark.asyncio
async def test_can_modify_label_value_without_overlap_when_get_series_labels_raises_http_error(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="metric",
        range=TimeRange(),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    client.get_series_labels.side_effect = HTTPError("Connection error")

    # Act / Assert
    with pytest.raises(HTTPError):
        await validator._can_modify_label_value_without_timeseries_overlap(
            request=request,
        )


@pytest.mark.asyncio
async def test_can_modify_label_value_without_overlap_when_get_series_labels_raises_vm_error(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="metric",
        range=TimeRange(),
        selector=selector,
        key="env",
        from_value="old",
        to_value="new",
    )
    client.get_series_labels.side_effect = VictoriaMetricsError("vm error")

    # Act / Assert
    with pytest.raises(VictoriaMetricsError):
        await validator._can_modify_label_value_without_timeseries_overlap(
            request=request,
        )


@pytest.mark.asyncio
async def test_validate_modify_label_value_when_from_value_is_empty_fails_and_skips_series_lookup(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        key="env",
        from_value="",
        to_value="new",
    )

    # Act / Assert
    with pytest.raises(RequestValidationError, match='Invalid "from value"'):
        await validator.validate_modify_label_value(request)

    client.get_series_labels.assert_not_called()


@pytest.mark.asyncio
async def test_validate_modify_label_value_when_to_value_is_empty_fails_and_skips_series_lookup(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        key="env",
        from_value="old",
        to_value="",
    )

    # Act / Assert
    with pytest.raises(RequestValidationError, match='Invalid "to value"'):
        await validator.validate_modify_label_value(request)

    client.get_series_labels.assert_not_called()


@pytest.mark.asyncio
async def test_validate_modify_label_value_when_from_and_to_are_invalid_fails_from_value_first(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    request = ModifyLabelValueRequest(
        metric_name="cpu",
        range=TimeRange(),
        selector=selector,
        key="env",
        from_value="",
        to_value="",
    )

    # Act / Assert
    with pytest.raises(RequestValidationError, match='Invalid "from value"'):
        await validator.validate_modify_label_value(request)

    client.get_series_labels.assert_not_called()


@pytest.mark.asyncio
async def test_can_delete_label_with_single_overlap_returns_true(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    delete_label_request = DeleteLabelRequest(
        metric_name="metric",
        label_keys=["env"],
        selector=selector,
    )
    client.get_series_labels.return_value = [{"__name__": "metric", "env": "prod"}]

    # Act
    result = await validator._can_delete_label_without_timeseries_overlap(
        request=delete_label_request,
    )

    # Assert
    assert result is True


@pytest.mark.asyncio
async def test_can_delete_label_with_multiple_overlaps_returns_false(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    delete_label_request = DeleteLabelRequest(
        metric_name="metric",
        label_keys=["env"],
        selector=selector,
    )
    client.get_series_labels.return_value = [
        {"__name__": "metric", "env": "prod"},
        {"__name__": "metric", "env": "dev"},
    ]

    # Act
    result = await validator._can_delete_label_without_timeseries_overlap(
        request=delete_label_request,
    )

    # Assert
    assert result is False


@pytest.mark.asyncio
async def test_can_delete_label_without_overlap_returns_false(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    delete_label_request = DeleteLabelRequest(
        metric_name="metric",
        label_keys=["env"],
        selector=selector,
    )
    client.get_series_labels.return_value = []

    # Act
    result = await validator._can_delete_label_without_timeseries_overlap(
        request=delete_label_request,
    )

    # Assert
    assert result is False


@pytest.mark.asyncio
async def test_can_delete_label_removes_target_label_key_from_selector(
    validator: RequestValidation,
    client: AsyncMock,
) -> None:
    # Arrange
    selector = Selector(
        match=[
            MatchItem(key="instance", value="dummy", regex=False),
            MatchItem(key="env", value="prod", regex=False),
        ]
    )
    delete_label_request = DeleteLabelRequest(
        metric_name="metric",
        label_keys=["env"],
        selector=selector,
    )
    client.get_series_labels.return_value = [
        {"__name__": "metric", "instance": "dummy"}
    ]

    # Act
    result = await validator._can_delete_label_without_timeseries_overlap(
        request=delete_label_request,
    )

    # Assert
    assert result is True
    call_kwargs = client.get_series_labels.call_args.kwargs
    assert call_kwargs["selector"] == Selector(
        match=[MatchItem(key="instance", value="dummy", regex=False)]
    )
    assert "start" not in call_kwargs
    assert "end" not in call_kwargs


@pytest.mark.asyncio
async def test_can_delete_label_without_overlap_when_get_series_labels_raises_http_error(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    request = DeleteLabelRequest(
        metric_name="metric",
        label_keys=["env"],
        selector=selector,
    )
    client.get_series_labels.side_effect = HTTPError("Connection error")

    # Act / Assert
    with pytest.raises(HTTPError):
        await validator._can_delete_label_without_timeseries_overlap(request=request)


@pytest.mark.asyncio
async def test_can_delete_label_without_overlap_when_get_series_labels_raises_vm_error(
    validator: RequestValidation,
    client: AsyncMock,
    selector: Selector,
) -> None:
    # Arrange
    request = DeleteLabelRequest(
        metric_name="metric",
        label_keys=["env"],
        selector=selector,
    )
    client.get_series_labels.side_effect = VictoriaMetricsError("vm error")

    # Act / Assert
    with pytest.raises(VictoriaMetricsError):
        await validator._can_delete_label_without_timeseries_overlap(request=request)


@pytest.mark.asyncio
async def test_is_within_sampling_limit_below_limit_returns_true(
    validator: RequestValidation,
    client: AsyncMock,
) -> None:
    # Arrange
    client.count_over_time.return_value = MAX_TIME_SERIES_SAMPLINGS - 1
    match_item1 = MatchItem(key="k1", value="v1", regex=False)
    match_item2 = MatchItem(key="k2", value="v2", regex=False)
    selector = Selector(match=[match_item1, match_item2])

    # Act
    result = await validator._is_within_sampling_limit(
        client=client,
        metric_name="metric",
        selector=selector,
        start=None,
        end=None,
    )

    # Assert
    assert result is True


@pytest.mark.asyncio
async def test_is_within_sampling_limit_at_limit_returns_true(
    validator: RequestValidation,
    client: AsyncMock,
) -> None:
    # Arrange
    client.count_over_time.return_value = MAX_TIME_SERIES_SAMPLINGS
    match_item = MatchItem(key="k", value="v", regex=False)
    selector = Selector(match=[match_item])

    # Act
    result = await validator._is_within_sampling_limit(
        client=client,
        metric_name="metric",
        selector=selector,
        start=None,
        end=None,
    )

    # Assert
    assert result is True


@pytest.mark.asyncio
async def test_is_within_sampling_limit_above_limit_returns_false(
    validator: RequestValidation,
    client: AsyncMock,
) -> None:
    # Arrange
    client.count_over_time.return_value = MAX_TIME_SERIES_SAMPLINGS + 1
    match_item = MatchItem(key="k", value="v", regex=False)
    selector = Selector(match=[match_item])
    # Act
    result = await validator._is_within_sampling_limit(
        client=client,
        metric_name="metric",
        selector=selector,
        start=None,
        end=None,
    )

    # Assert
    assert result is False


@pytest.mark.asyncio
async def test_is_within_sampling_limit_when_count_over_time_raises_http_error_fails(
    validator: RequestValidation,
    client: AsyncMock,
) -> None:
    # Arrange
    client.count_over_time.side_effect = HTTPError("Connection error")
    match_item = MatchItem(key="k", value="v", regex=False)
    selector = Selector(match=[match_item])
    # Act / Assert
    with pytest.raises(HTTPError):
        await validator._is_within_sampling_limit(
            client=client,
            metric_name="metric",
            selector=selector,
            start=None,
            end=None,
        )
