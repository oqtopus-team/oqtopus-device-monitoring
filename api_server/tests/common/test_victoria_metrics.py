import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import httpx
import pytest
from pytest_mock import MockerFixture

from common.types.victoria_metrics import MetricDataResponse
from common.victoria_metrics import VictoriaMetricsClient, VictoriaMetricsError
from schemas.meta import MatchItem, Selector


class StubResponse:
    def __init__(self, json_data: dict | None = None, text_data: str = "") -> None:
        self._json_data = json_data
        self.text = text_data

    def json(self) -> dict | None:
        return self._json_data

    def raise_for_status(self) -> None:
        return None


class ErrorResponse(StubResponse):
    def __init__(self, status_code: int) -> None:
        self._request = httpx.Request("GET", "http://vm")
        self._response = httpx.Response(status_code, request=self._request)

    def raise_for_status(self) -> None:
        error_message = "Server Error"
        raise httpx.HTTPStatusError(
            error_message, request=self._request, response=self._response
        )


@pytest.fixture
def vm_client(mocker: MockerFixture) -> VictoriaMetricsClient:
    vm_client = VictoriaMetricsClient("http://vm")
    vm_client.vm_client = mocker.AsyncMock()
    return vm_client


@pytest.mark.asyncio
async def test_get_metric_names_with_offset_applies_offset(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    vm_client.vm_client.get = AsyncMock(
        return_value=StubResponse(
            json_data={"status": "success", "data": ["a", "b", "c"]}
        )
    )

    # Act
    result = await vm_client.get_metric_names(offset=1, limit=100)

    # Assert
    assert result == ["b", "c"]


@pytest.mark.asyncio
async def test_get_metric_names_with_limit_applies_pagination(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    vm_client.vm_client.get = AsyncMock(
        return_value=StubResponse(
            json_data={"status": "success", "data": ["a", "b", "c"]}
        )
    )

    # Act
    result = await vm_client.get_metric_names(offset=1, limit=1)

    # Assert
    assert result == ["b"]


@pytest.mark.asyncio
async def test_get_metric_names_when_partial_page_returns_tail(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    vm_client.vm_client.get = AsyncMock(
        return_value=StubResponse(
            json_data={"status": "success", "data": ["a", "b", "c"]}
        )
    )

    # Act
    result = await vm_client.get_metric_names(offset=2, limit=5)

    # Assert
    assert result == ["c"]


@pytest.mark.asyncio
async def test_get_metric_names_when_offset_too_large_returns_empty(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    vm_client.vm_client.get = AsyncMock(
        return_value=StubResponse(
            json_data={"status": "success", "data": ["a", "b", "c"]}
        )
    )

    # Act
    result = await vm_client.get_metric_names(offset=5, limit=2)

    # Assert
    assert result == []


@pytest.mark.parametrize(
    "status_code",
    [
        (100,),
        (300,),
        (400,),
        (500,),
    ],
)
@pytest.mark.asyncio
async def test_get_series_labels_with_response_raises_error(
    vm_client: VictoriaMetricsClient,
    status_code: int,
) -> None:
    # Arrange

    vm_client.vm_client.get = AsyncMock(return_value=ErrorResponse(status_code))

    # Act / Assert
    with pytest.raises(httpx.HTTPStatusError):
        await vm_client.get_series_labels(
            metric_name="metric", selector=Selector(match=[])
        )


@pytest.mark.asyncio
async def test_get_series_labels_with_empty_selector_builds_match_and_returns_labels(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    selector = Selector(match=[])
    vm_client.vm_client.get = AsyncMock(
        return_value=StubResponse(
            json_data={"data": [{"__name__": "metric", "job": "node"}]}
        )
    )

    # Act
    result = await vm_client.get_series_labels(
        metric_name="metric", selector=selector, start=None, end=None
    )

    # Assert
    assert result == [{"__name__": "metric", "job": "node"}]


@pytest.mark.asyncio
async def test_get_series_labels_with_selector_and_time_range_returns_labels(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    selector = Selector(
        match=[
            MatchItem(key="job", value="node", regex=False),
            MatchItem(key="env", value="prod", regex=True),
        ]
    )
    start = datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC)
    end = datetime(1970, 12, 31, 0, 0, 0, tzinfo=UTC)
    vm_client.vm_client.get = AsyncMock(
        return_value=StubResponse(json_data={"data": []})
    )

    # Act
    result = await vm_client.get_series_labels(
        metric_name="metric", selector=selector, start=start, end=end
    )

    # Assert
    assert result == []


@pytest.mark.asyncio
async def test_get_series_labels_when_response_is_dict_returns_list(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    selector = Selector(match=[])
    vm_client.vm_client.get = AsyncMock(
        return_value=StubResponse(
            json_data={"data": {"__name__": "metric", "job": "node"}}
        )
    )

    # Act
    result = await vm_client.get_series_labels(metric_name="metric", selector=selector)

    # Assert
    assert result == [{"__name__": "metric", "job": "node"}]


@pytest.mark.asyncio
async def test_get_series_label_keys_with_valid_response_returns_keys(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    vm_client.vm_client.get = AsyncMock(
        return_value=StubResponse(json_data={"data": ["__name__", "instance", "job"]})
    )

    # Act
    result = await vm_client.get_series_label_keys(metric_name="metric")

    # Assert
    assert result == ["__name__", "instance", "job"]


@pytest.mark.parametrize(
    "status_code",
    [
        (100,),
        (300,),
        (400,),
        (500,),
    ],
)
@pytest.mark.asyncio
async def test_get_series_label_keys_with_response_raises_error(
    vm_client: VictoriaMetricsClient,
    status_code: int,
) -> None:
    # Arrange
    vm_client.vm_client.get = AsyncMock(return_value=ErrorResponse(status_code))

    # Act / Assert
    with pytest.raises(httpx.HTTPStatusError):
        await vm_client.get_series_label_keys(
            metric_name="metric",
        )


@pytest.mark.asyncio
async def test_get_series_label_values_with_offset_applies_offset(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    vm_client.vm_client.get = AsyncMock(
        return_value=StubResponse(json_data={"data": ["host1", "host2", "host3"]})
    )

    # Act
    result = await vm_client.get_series_label_values(
        metric_name="metric", label_key="instance", offset=1, limit=100
    )

    # Assert
    assert result == ["host2", "host3"]


@pytest.mark.asyncio
async def test_get_series_label_values_with_limit_applies_pagination(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    vm_client.vm_client.get = AsyncMock(
        return_value=StubResponse(json_data={"data": ["host1", "host2", "host3"]})
    )

    # Act
    result = await vm_client.get_series_label_values(
        metric_name="metric", label_key="instance", offset=1, limit=1
    )

    # Assert
    assert result == ["host2"]


@pytest.mark.asyncio
async def test_get_series_label_values_when_partial_page_returns_tail(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    vm_client.vm_client.get = AsyncMock(
        return_value=StubResponse(json_data={"data": ["host1", "host2", "host3"]})
    )

    # Act
    result = await vm_client.get_series_label_values(
        metric_name="metric", label_key="instance", offset=2, limit=5
    )

    # Assert
    assert result == ["host3"]


@pytest.mark.asyncio
async def test_get_series_label_values_when_offset_too_large_returns_empty(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    vm_client.vm_client.get = AsyncMock(
        return_value=StubResponse(json_data={"data": ["host1", "host2", "host3"]})
    )

    # Act
    result = await vm_client.get_series_label_values(
        metric_name="metric", label_key="instance", offset=5, limit=2
    )

    # Assert
    assert result == []


@pytest.mark.parametrize(
    "status_code",
    [
        (100,),
        (300,),
        (400,),
        (500,),
    ],
)
@pytest.mark.asyncio
async def test_get_series_label_values_with_response_raises_error(
    vm_client: VictoriaMetricsClient,
    status_code: int,
) -> None:
    # Arrange
    vm_client.vm_client.get = AsyncMock(return_value=ErrorResponse(status_code))

    # Act / Assert
    with pytest.raises(httpx.HTTPStatusError):
        await vm_client.get_series_label_values(
            metric_name="metric",
            label_key="instance",
            offset=0,
            limit=10,
        )


@pytest.mark.asyncio
async def test_read_timeseries_when_export_blank_returns_empty(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    selector = Selector(match=[])
    vm_client.vm_client.get = AsyncMock(return_value=StubResponse(text_data=""))

    # Act
    result = await vm_client.read_timeseries(metric_name="metric", selector=selector)

    # Assert
    assert result == []


@pytest.mark.asyncio
async def test_read_timeseries_with_valid_export_returns_parsed_data(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    selector = Selector(match=[MatchItem(key="job", value="node", regex=False)])
    jsonl_data = json.dumps({
        "metric": {"__name__": "cpu", "job": "node"},
        "values": [1.0],
        "timestamps": [123],
    })
    vm_client.vm_client.get = AsyncMock(return_value=StubResponse(text_data=jsonl_data))

    # Act
    result = await vm_client.read_timeseries(metric_name="cpu", selector=selector)

    # Assert
    assert len(result) == 1
    assert result[0].metric["__name__"] == "cpu"
    assert result[0].metric["job"] == "node"
    assert result[0].values == [1.0]
    assert result[0].timestamps == [123]


@pytest.mark.asyncio
async def test_read_timeseries_when_parsing_fails_returns_empty(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    selector = Selector(match=[])
    invalid_jsonl = "test"
    vm_client.vm_client.get = AsyncMock(
        return_value=StubResponse(text_data=invalid_jsonl)
    )

    # Act / Assert
    with pytest.raises(VictoriaMetricsError):
        await vm_client.read_timeseries(metric_name="metric", selector=selector)


@pytest.mark.parametrize(
    "status_code",
    [
        (100,),
        (300,),
        (400,),
        (500,),
    ],
)
@pytest.mark.asyncio
async def test_read_timeseries_with_response_raises_error(
    vm_client: VictoriaMetricsClient,
    status_code: int,
) -> None:
    # Arrange
    selector = Selector(match=[])
    vm_client.vm_client.get = AsyncMock(return_value=ErrorResponse(status_code))

    # Act / Assert
    with pytest.raises(httpx.HTTPStatusError):
        await vm_client.read_timeseries(metric_name="metric", selector=selector)


@pytest.mark.asyncio
async def test_read_timeseries_when_parsing_fails_raises_error(
    vm_client: VictoriaMetricsClient,
    mocker: MockerFixture,
) -> None:
    # Arrange
    selector = Selector(match=[])
    invalid_jsonl = "test"
    vm_client.vm_client.get = AsyncMock(
        return_value=StubResponse(text_data=invalid_jsonl)
    )
    mocker.patch.object(
        vm_client,
        "_parse_exported_data",
        side_effect=VictoriaMetricsError("mock parsing error"),
    )

    # Act / Assert
    with pytest.raises(VictoriaMetricsError, match="mock parsing error"):
        await vm_client.read_timeseries(metric_name="metric", selector=selector)


@pytest.mark.asyncio
async def test_write_timeseries_with_valid_data_posts_jsonl(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    data = [
        MetricDataResponse(
            metric={"__name__": "cpu", "instance": "host1"},
            values=[1.0, 2.0],
            timestamps=[100, 200],
        ),
        MetricDataResponse(
            metric={"__name__": "mem", "instance": "host2"},
            values=[3.0],
            timestamps=[300],
        ),
    ]
    vm_client.vm_client.post = AsyncMock(return_value=StubResponse())

    # Act
    await vm_client.write_timeseries(data)

    # Assert
    vm_client.vm_client.post.assert_awaited_once()
    await_args = vm_client.vm_client.post.await_args
    assert await_args is not None
    args, kwargs = await_args
    assert args[0] == "/insert/0/prometheus/api/v1/import"
    assert kwargs["headers"]["Content-Type"] == "application/json"
    content = kwargs["content"]
    lines = content.split("\n")
    assert len(lines) == 2
    parsed_first = json.loads(lines[0])
    assert parsed_first["metric"]["__name__"] == "cpu"
    assert parsed_first["metric"]["instance"] == "host1"
    assert parsed_first["values"] == [1.0, 2.0]
    assert parsed_first["timestamps"] == [100, 200]
    parsed_second = json.loads(lines[1])
    assert parsed_second["metric"]["__name__"] == "mem"
    assert parsed_second["metric"]["instance"] == "host2"
    assert parsed_second["values"] == [3.0]
    assert parsed_second["timestamps"] == [300]


@pytest.mark.asyncio
async def test_write_timeseries_when_serialization_fails_errors(
    vm_client: VictoriaMetricsClient, mocker: MockerFixture
) -> None:
    # Arrange

    data = [
        MetricDataResponse(
            metric={"__name__": "cpu"},
            values=[1.0],
            timestamps=[100],
        )
    ]
    mocker.patch.object(
        MetricDataResponse, "model_dump", side_effect=TypeError("mock error")
    )

    # Act / Assert
    with pytest.raises(TypeError, match="mock error"):
        await vm_client.write_timeseries(data)


@pytest.mark.parametrize(
    "status_code",
    [
        (100,),
        (300,),
        (400,),
        (500,),
    ],
)
@pytest.mark.asyncio
async def test_write_timeseries_with_response_raises_error(
    vm_client: VictoriaMetricsClient,
    status_code: int,
) -> None:
    # Arrange
    data = [
        MetricDataResponse(
            metric={"__name__": "cpu"},
            values=[1.0],
            timestamps=[100],
        )
    ]
    vm_client.vm_client.post = AsyncMock(return_value=ErrorResponse(status_code))

    # Act / Assert
    with pytest.raises(httpx.HTTPStatusError):
        await vm_client.write_timeseries(data)


@pytest.mark.asyncio
async def test_delete_timeseries_with_selector_calls_delete_endpoint(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    selector = Selector(match=[MatchItem(key="instance", value="host1", regex=False)])
    vm_client.vm_client.delete = AsyncMock(return_value=StubResponse())

    # Act
    await vm_client.delete_timeseries(metric_name="metric", selector=selector)

    # Assert
    vm_client.vm_client.delete.assert_awaited_once_with(
        "/delete/0/prometheus/api/v1/admin/tsdb/delete_series",
        params={"match[]": 'metric{instance="host1"}'},
    )


@pytest.mark.parametrize(
    "status_code",
    [
        (100,),
        (300,),
        (400,),
        (500,),
    ],
)
@pytest.mark.asyncio
async def test_delete_timeseries_with_response_raises_error(
    vm_client: VictoriaMetricsClient,
    status_code: int,
) -> None:
    # Arrange
    selector = Selector(match=[MatchItem(key="instance", value="host1", regex=False)])
    vm_client.vm_client.delete = AsyncMock(return_value=ErrorResponse(status_code))

    # Act / Assert
    with pytest.raises(httpx.HTTPStatusError):
        await vm_client.delete_timeseries(metric_name="metric", selector=selector)


@pytest.mark.asyncio
async def test_count_over_time_with_valid_response_returns_count(
    vm_client: VictoriaMetricsClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    selector = Selector(match=[MatchItem(key="instance", value="host1", regex=False)])
    start = datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC)
    end = datetime(1970, 12, 31, 0, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(
        vm_client,
        "_query",
        AsyncMock(
            return_value={
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "cpu": "0",
                                "env": "local",
                                "instance": "test-instance:9100",
                                "job": "node",
                                "mode": "user",
                                "scrapedweek": "1970-01w1d0000",
                            },
                            "value": [0, "7"],  # _, Count value
                        },
                    ],
                },
                "stats": {"seriesFetched": "1", "executionTimeMsec": 4},
            }
        ),
    )

    # Act
    count = await vm_client.count_over_time(
        metric_name="metric", selector=selector, start=start, end=end
    )

    # Assert
    assert count == 7


@pytest.mark.asyncio
async def test_count_over_time_when_response_invalid_errors(
    vm_client: VictoriaMetricsClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    selector = Selector(match=[])
    monkeypatch.setattr(
        vm_client,
        "_query",
        AsyncMock(return_value={"data": {}}),
    )

    # Act / Assert
    with pytest.raises(KeyError):
        await vm_client.count_over_time(
            metric_name="metric", selector=selector, start=None, end=None
        )


@pytest.mark.asyncio
async def test_count_over_time_with_result_length_zero_returns_zero(
    vm_client: VictoriaMetricsClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    selector = Selector(match=[])
    monkeypatch.setattr(
        vm_client,
        "_query",
        AsyncMock(return_value={"data": {"result": []}}),
    )
    start = datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC)
    end = datetime(1970, 12, 31, 0, 0, 0, tzinfo=UTC)

    # Act
    count = await vm_client.count_over_time(
        metric_name="metric", selector=selector, start=start, end=end
    )

    # Assert
    assert count == 0


@pytest.mark.parametrize(
    "status_code",
    [
        (100,),
        (300,),
        (400,),
        (500,),
    ],
)
@pytest.mark.asyncio
async def test_count_over_time_with_response_raises_error(
    vm_client: VictoriaMetricsClient,
    status_code: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    selector = Selector(match=[])
    monkeypatch.setattr(
        vm_client,
        "_query",
        AsyncMock(
            side_effect=httpx.HTTPStatusError(
                "Error",
                request=httpx.Request("GET", "http://vm"),
                response=httpx.Response(status_code),
            )
        ),
    )

    # Act / Assert
    with pytest.raises(httpx.HTTPStatusError):
        await vm_client.count_over_time(
            metric_name="metric", selector=selector, start=None, end=None
        )


@pytest.mark.asyncio
async def test_query_with_valid_response_returns_payload(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    data = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {
                    "metric": {
                        "cpu": "0",
                        "env": "local",
                        "instance": "test-instance:9100",
                        "job": "node",
                        "mode": "user",
                        "scrapedweek": "1234-48w2d1600",
                    },
                    "value": [0, "1"],
                },
            ],
        },
        "stats": {"seriesFetched": "1", "executionTimeMsec": 4},
    }
    vm_client.vm_client.get = AsyncMock(return_value=StubResponse(json_data=data))
    time = datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC)

    # Act
    result = await vm_client._query(
        "count_over_time(node_cpu_seconds_total{"
        'job="node",instance="test-instance:9100",cpu="0", mode="user"'
        "}[2d])",
        time,
    )

    # Assert
    assert result == data


@pytest.mark.parametrize(
    "status_code",
    [
        (100,),
        (300,),
        (400,),
        (500,),
    ],
)
@pytest.mark.asyncio
async def test_query_with_response_raises_error(
    vm_client: VictoriaMetricsClient,
    status_code: int,
) -> None:
    # Arrange
    vm_client.vm_client.get = AsyncMock(return_value=ErrorResponse(status_code))
    time = datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC)

    # Act / Assert
    with pytest.raises(httpx.HTTPStatusError):
        await vm_client._query("some_query", time)


@pytest.mark.asyncio
async def test_close_vm_client_when_aclose_called_marks_closed(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    vm_client.vm_client.aclose = AsyncMock()

    # Act
    await vm_client.close()

    # Assert
    assert vm_client.vm_client.is_closed


def test_build_promql_from_selector_returns_correct_promql(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    metric_name = "cpu"
    selector = Selector(
        match=[
            MatchItem(key="job", value="node", regex=False),
            MatchItem(key="env", value=".*prod.*", regex=True),
            MatchItem(key="instance", value="local", regex=False),
        ]
    )

    # Act
    promql = vm_client._build_promql_from_selector(metric_name, selector)

    # Assert
    assert promql == 'cpu{job="node",env=~".*prod.*",instance="local"}'


def test_build_promql_from_selector_returns_null_promql(
    vm_client: VictoriaMetricsClient,
) -> None:
    # Arrange
    metric_name = "cpu"
    selector = Selector(match=[])

    # Act
    promql = vm_client._build_promql_from_selector(metric_name, selector)
    # Assert
    assert promql == metric_name


@pytest.mark.parametrize(
    "metrics_name",
    [":", "()", "?", "*", ",", "\\"],
)
def test_build_promql_from_selector_with_special_characters_in_metric_name(
    vm_client: VictoriaMetricsClient,
    metrics_name: str,
) -> None:
    # Arrange
    selector = Selector(
        match=[
            MatchItem(key="instance", value="instance:9100`", regex=False),
        ]
    )

    # Act / Assert
    with pytest.raises(VictoriaMetricsError):
        vm_client._build_promql_from_selector(metrics_name, selector)


@pytest.mark.parametrize(
    "label_key",
    [":", "()", "?", "*", ",", "\\"],
)
def test_build_promql_from_selector_with_special_characters_in_label_key(
    vm_client: VictoriaMetricsClient,
    label_key: str,
) -> None:
    # Arrange
    selector = Selector(
        match=[
            MatchItem(key=label_key, value="instance:9100", regex=False),
        ]
    )

    # Act / Assert
    with pytest.raises(VictoriaMetricsError):
        vm_client._build_promql_from_selector("metric_name", selector)
