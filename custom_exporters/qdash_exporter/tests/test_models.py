from __future__ import annotations

import copy
import math
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest

from models import (
    Batch,
    BatchValidationError,
    build_batch_id,
    format_utc,
    infer_metric_from_filename,
    is_finite_number,
    is_valid_batch_filename,
    parse_utc,
    to_epoch_ms,
    verify_filename_matches_batch,
    window_state_key,
)
from tests.conftest import make_batch


def _valid_batch_dict() -> dict[str, Any]:
    from_ms = to_epoch_ms(parse_utc("2026-06-11T00:00:00Z"))
    return {
        "batch_id": "20260611T010000Z-chip_001-t1_1",
        "collected_at": "2026-06-11T01:00:00Z",
        "window": {"from": "2026-06-11T00:00:00Z", "to": "2026-06-11T01:00:00Z"},
        "chip_id": "chip_001",
        "metric": "t1",
        "records": [
            {"timestamp_ms": from_ms, "qubit_id": "0", "value": 45.2, "unit": "us"}
        ],
    }


def _mutate(**changes: Any) -> dict[str, Any]:
    data = _valid_batch_dict()
    data.update(changes)
    return data


def _mutate_record(**changes: Any) -> dict[str, Any]:
    data = _valid_batch_dict()
    record = data["records"][0]
    for key in ("qubit_id", "coupling_id"):
        record.pop(key, None)
    record.update(changes)
    return data


def _without(field: str) -> dict[str, Any]:
    data = _valid_batch_dict()
    del data[field]
    return data


def _both_ids() -> dict[str, Any]:
    data = _valid_batch_dict()
    data["records"][0]["coupling_id"] = "0-1"
    return data


def _swapped_window() -> dict[str, Any]:
    data = _valid_batch_dict()
    data["window"] = {"from": "2026-06-11T01:00:00Z", "to": "2026-06-11T00:00:00Z"}
    return data


def _timestamp_outside_window() -> dict[str, Any]:
    data = _valid_batch_dict()
    data["records"][0]["timestamp_ms"] = to_epoch_ms(parse_utc("2026-06-11T01:00:00Z"))
    return data


class TestUtcConversion:
    """Test suite for format_utc and parse_utc functions."""

    def test_format_and_parse_utc_round_trip(self) -> None:
        original = datetime(2026, 6, 11, 1, 2, 3, tzinfo=UTC)

        result = parse_utc(format_utc(original))

        assert result == original

    def test_parse_utc_invalid_string_raises(self) -> None:
        with pytest.raises(ValueError, match="invalid UTC timestamp"):
            parse_utc("2026-06-11 01:00:00")


class TestIsFiniteNumber:
    """Test suite for is_finite_number function."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (True, False),
            (math.nan, False),
            (math.inf, False),
            (None, False),
            ("1", False),
            (1, True),
            (1.5, True),
        ],
    )
    def test_is_finite_number_rejects_bool_nan_inf(
        self, value: object, expected: bool
    ) -> None:
        assert is_finite_number(value) is expected


class TestBuildBatchId:
    """Test suite for build_batch_id function."""

    def test_build_batch_id_returns_expected_format(self) -> None:
        collected_at = datetime(2026, 6, 11, 1, 0, 0, tzinfo=UTC)

        batch_id = build_batch_id(collected_at, "chip_001", "t1", 2)

        assert batch_id == "20260611T010000Z-chip_001-t1_2"


class TestIsValidBatchFilename:
    """Test suite for is_valid_batch_filename function."""

    @pytest.mark.parametrize(
        ("filename", "expected"),
        [
            ("20260611T010000Z-chip_001-t1_1.json", True),
            ("20260611T010000Z-chip_001-t2_echo_3.json", True),
            ("chip_001-t1_1.json", False),
            ("20260611T010000Z-chip_001-t1.json", False),
            ("20260611T010000Z-chip_001-t1_0.json", False),
        ],
    )
    def test_is_valid_batch_filename(self, filename: str, expected: bool) -> None:
        assert is_valid_batch_filename(filename) is expected


class TestInferMetricFromFilename:
    """Test suite for infer_metric_from_filename function."""

    def test_infer_metric_distinguishes_suffix_metrics(self) -> None:
        enabled = frozenset({"t1", "t1_average"})

        metric = infer_metric_from_filename(
            "20260611T010000Z-chip_001-t1_1.json", enabled
        )

        assert metric == "t1"

    def test_infer_metric_returns_none_for_disabled_metric(self) -> None:
        enabled = frozenset({"t1"})

        metric = infer_metric_from_filename(
            "20260611T010000Z-chip_001-t2_echo_1.json", enabled
        )

        assert metric is None

    def test_infer_metric_handles_chip_id_with_underscore(self) -> None:
        enabled = frozenset({"t1"})

        metric = infer_metric_from_filename(
            "20260611T010000Z-chip_001-t1_1.json", enabled
        )

        assert metric == "t1"


class TestVerifyFilenameMatchesBatch:
    """Test suite for verify_filename_matches_batch function."""

    def test_verify_filename_matches_batch_metric_mismatch_raises(self) -> None:
        with pytest.raises(BatchValidationError, match="inconsistent with its"):
            verify_filename_matches_batch(
                "20260611T010000Z-chip_001-t1_1.json", make_batch(metric="t2")
            )

    def test_verify_filename_matches_batch_id_field_mismatch_raises(self) -> None:
        batch = replace(make_batch(), batch_id="20260611T010000Z-chip_001-t1_9")

        with pytest.raises(BatchValidationError, match="inconsistent with its"):
            verify_filename_matches_batch("20260611T010000Z-chip_001-t1_1.json", batch)

    def test_verify_filename_matches_batch_without_seq_suffix_raises(self) -> None:
        with pytest.raises(BatchValidationError, match="no valid sequence suffix"):
            verify_filename_matches_batch(
                "20260611T010000Z-chip_001-t1_x.json", make_batch()
            )


class TestWindowStateKey:
    """Test suite for window_state_key function."""

    def test_window_state_key_joins_chip_and_metric(self) -> None:
        assert window_state_key("chip_001", "t1") == "chip_001::t1"


class TestBatchSerialization:
    """Test suite for Batch JSON serialization."""

    def test_batch_to_and_from_json_dict_round_trip(self) -> None:
        source = Batch.from_json_dict(_valid_batch_dict())

        result = Batch.from_json_dict(source.to_json_dict())

        assert result == source

    def test_batch_to_json_dict_omits_none_error_and_other_id(self) -> None:
        record = Batch.from_json_dict(_valid_batch_dict()).to_json_dict()["records"][0]

        assert "error" not in record
        assert "coupling_id" not in record

    @pytest.mark.parametrize(
        "record",
        [
            {"qubit_id": "0", "value": 45.2, "unit": "us", "error": 0.8},
            {"coupling_id": "0-1", "value": 0.99, "unit": ""},
        ],
    )
    def test_batch_round_trip_serializes_optional_fields(
        self, record: dict[str, Any]
    ) -> None:
        from_ms = to_epoch_ms(parse_utc("2026-06-11T00:00:00Z"))
        data = _valid_batch_dict()
        data["records"] = [{"timestamp_ms": from_ms, **record}]

        source = Batch.from_json_dict(data)

        assert Batch.from_json_dict(source.to_json_dict()) == source

    @pytest.mark.parametrize(
        "data",
        [
            _without("batch_id"),
            _mutate(collected_at=123),
            _mutate(collected_at="2026-06-11 01:00:00"),
            _mutate(chip_id=123),
            _mutate(metric=123),
            _mutate(records="not-a-list"),
            _mutate(records=[]),
            _mutate(records=[123]),
            _mutate(window="not-a-dict"),
            _mutate(window={"from": 1, "to": 2}),
            _mutate(window={"from": "oops", "to": "2026-06-11T01:00:00Z"}),
            _swapped_window(),
            _both_ids(),
            _mutate_record(qubit_id=None),
            _mutate_record(qubit_id="0", unit=123),
            _timestamp_outside_window(),
            _mutate_record(qubit_id="0", timestamp_ms=True),
            _mutate_record(qubit_id="0", value=math.nan),
            _mutate_record(qubit_id="0", error="oops"),
        ],
    )
    def test_batch_from_json_dict_invalid_schema_raises(
        self, data: dict[str, Any]
    ) -> None:
        with pytest.raises(BatchValidationError):
            Batch.from_json_dict(copy.deepcopy(data))

    def test_batch_from_json_dict_non_object_raises(self) -> None:
        with pytest.raises(BatchValidationError, match="must be a JSON object"):
            Batch.from_json_dict(["not", "a", "dict"])  # type: ignore[arg-type]
