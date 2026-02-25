import re
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote

import yaml

from schemas.meta import Selector


def load_yaml(file_path: str) -> dict:
    """Load YAML file.

    Args:
        file_path: Path to the YAML file

    Returns:
        Dictionary of the loaded content

    """
    with Path(file_path).open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def parse_deep_object_as_selector(query_params: dict[str, str]) -> Selector:
    """Parse deepObject style query parameters into a flat dictionary.

    Args:
        query_params: Dictionary of query parameters from FastAPI Request.query_params

    Returns:
        Dictionary with extracted key-value pairs from selector[key]=value format

    """
    selector = {}
    pattern = re.compile(r"selector\[([^\]]+)\]")

    for param_key, param_value in query_params.items():
        match = pattern.match(param_key)
        if match:
            label_key = unquote(match.group(1))
            selector[label_key] = unquote(param_value)

    selector_list = []
    for key, value in selector.items():
        selector_list.append({"key": key, "value": value, "regex": False})

    return Selector(match=selector_list)


def get_time() -> datetime:
    """Get the current UTC time.

    Returns:
        Current UTC datetime object

    """
    return datetime.now(UTC)


def generate_operation_id(data_path: str = "data/operations") -> str:
    """Generate a unique operation ID.

    The operation ID format is: <timestamp>_<number>
    where timestamp is in UTC in ISO 8601 format (YYYYMMDDTHHmmss)
    and number is a counter that increments if the same timestamp already exists.

    Args:
        data_path: Path to the data directory where operation files are stored

    Returns:
        Unique operation ID string


    """
    now = get_time()
    timestamp = now.strftime("%Y%m%dT%H%M%S")

    operations_dir = Path(data_path)
    operations_dir.mkdir(parents=True, exist_ok=True)

    # Find the next available counter for this timestamp
    counter = 1
    while True:
        operation_id = f"{timestamp}_{counter}"
        history_file = operations_dir / f"{operation_id}.json"
        if not history_file.exists():
            break
        counter += 1

    return f"{timestamp}_{counter}"
