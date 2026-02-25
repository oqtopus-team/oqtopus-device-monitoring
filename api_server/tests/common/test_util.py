from datetime import UTC, datetime, tzinfo
from pathlib import Path

import pytest
import yaml

from common import util


def test_load_yaml_with_valid_file_returns_dict(tmp_path: Path) -> None:
    # Arrange
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("key: value\nnumber: 1", encoding="utf-8")

    # Act
    result = util.load_yaml(str(yaml_path))

    # Assert
    assert result == {"key": "value", "number": 1}


def test_load_yaml_with_empty_file_returns_none(tmp_path: Path) -> None:
    # Arrange
    yaml_path = tmp_path / "empty.yaml"
    yaml_path.write_text("", encoding="utf-8")

    # Act
    result = util.load_yaml(str(yaml_path))

    # Assert
    assert result is None


def test_load_yaml_when_file_missing_errors(tmp_path: Path) -> None:
    # Arrange
    missing_path = tmp_path / "absent.yaml"

    # Act / Assert
    with pytest.raises(FileNotFoundError):
        util.load_yaml(str(missing_path))


def test_load_yaml_with_invalid_yaml_errors(tmp_path: Path) -> None:
    # Arrange
    yaml_path = tmp_path / "invalid.yaml"
    yaml_path.write_text("key: [unclosed", encoding="utf-8")

    # Act / Assert
    with pytest.raises(yaml.YAMLError):
        util.load_yaml(str(yaml_path))


def test_parse_deep_object_as_selector_with_multiple_labels_returns_selector() -> None:
    # Arrange
    query_params = {
        "selector[instance]": "host1",
        "selector[job]": "node",
        "other": "ignore",
    }

    # Act
    selector = util.parse_deep_object_as_selector(query_params)

    # Assert
    assert selector.match is not None
    assert len(selector.match) == 2
    assert selector.match[0].key == "instance"
    assert selector.match[0].value == "host1"
    assert selector.match[0].regex is False
    assert selector.match[1].key == "job"
    assert selector.match[1].value == "node"
    assert selector.match[1].regex is False


def test_parse_deep_object_as_selector_with_encoded_values_decodes_values() -> None:
    # Arrange
    query_params = {
        "selector[label%20name]": "value%2F1",
    }

    # Act
    selector = util.parse_deep_object_as_selector(query_params)

    # Assert
    assert selector.match is not None
    assert selector.match[0].key == "label name"
    assert selector.match[0].value == "value/1"


def test_parse_deep_object_as_selector_without_selector_params_returns_empty_match() -> (
    None
):
    # Arrange
    query_params = {"foo": "bar"}

    # Act
    selector = util.parse_deep_object_as_selector(query_params)

    # Assert
    assert selector.match == []


def test_get_time_when_datetime_fixed_returns_utc_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    fixed_time = datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> "FixedDateTime":
            if tz is None:
                return cls(
                    fixed_time.year,
                    fixed_time.month,
                    fixed_time.day,
                    fixed_time.hour,
                    fixed_time.minute,
                    fixed_time.second,
                    fixed_time.microsecond,
                    fixed_time.tzinfo,
                )
            dt = fixed_time.astimezone(tz)
            return cls(
                dt.year,
                dt.month,
                dt.day,
                dt.hour,
                dt.minute,
                dt.second,
                dt.microsecond,
                dt.tzinfo,
            )

    monkeypatch.setattr(util, "datetime", FixedDateTime)

    # Act
    result = util.get_time()

    # Assert
    assert result == fixed_time
    assert result.tzinfo == util.UTC


def test_generate_operation_id_when_missing_directory_creates_and_returns_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    fixed_time = datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(util, "get_time", lambda: fixed_time)
    operations_dir = tmp_path / "test_ops"

    # Act
    operation_id = util.generate_operation_id(data_path=str(operations_dir))

    # Assert
    assert operations_dir.exists()
    assert operation_id == "19700101T000000_1"


def test_generate_operation_id_with_existing_file_increments_counter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    fixed_time = datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(util, "get_time", lambda: fixed_time)
    operations_dir = tmp_path / "test_ops"
    operations_dir.mkdir(parents=True, exist_ok=True)
    (operations_dir / "19700101T000000_1.json").write_text("{}", encoding="utf-8")

    # Act
    operation_id = util.generate_operation_id(data_path=str(operations_dir))

    # Assert
    assert operation_id == "19700101T000000_2"
