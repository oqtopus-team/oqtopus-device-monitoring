from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Final, Literal

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from models import ConfigError, MetricKind

_MODEL_CONFIG = ConfigDict(frozen=True, extra="forbid")
METRIC_NAME_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")

# Environment variable overrides for configuration values
ENV_OVERRIDES: Final[tuple[tuple[str, tuple[str, ...]], ...]] = (
    ("EXPORTER_PORT", ("exporter", "port")),
    ("EXPORTER_TIMEZONE", ("exporter", "timezone")),
    ("COLLECTION_INTERVAL_SEC", ("collection", "interval_sec")),
    ("COLLECTION_RETRY_MAX_ATTEMPTS", ("collection", "retry_max_attempts")),
    ("COLLECTION_MAX_EXPAND_WINDOWS", ("collection", "max_expand_windows")),
    ("COLLECTION_TAG", ("collection", "tag")),
    ("COLLECTION_CHIP_DISCOVERY_MODE", ("collection", "chip_discovery_mode")),
    ("BUFFER_DIR_PATH", ("buffer", "dir_path")),
    ("QDASH_CLIENT_CONFIG_FILE", ("qdash_client", "config_file")),
    ("QDASH_CLIENT_CONFIG_PROFILE", ("qdash_client", "config_profile")),
    ("TARGETS_QUBIT_METRICS", ("targets", "qubit_metrics")),
    ("TARGETS_COUPLING_METRICS", ("targets", "coupling_metrics")),
)


class ExporterConfig(BaseModel):
    """Settings under the 'exporter' key."""

    model_config = _MODEL_CONFIG

    port: int = Field(default=9104, ge=1, le=65535)
    timezone: str = "UTC"  # used only for log timestamps


class CollectionConfig(BaseModel):
    """Settings under the 'collection' key."""

    model_config = _MODEL_CONFIG

    interval_sec: int = Field(default=3600, gt=0)
    retry_max_attempts: int = Field(default=3, ge=0)
    max_expand_windows: int = Field(default=24, ge=1)
    tag: str = "calibration"  # "" means "do not pass tag"
    chip_discovery_mode: Literal["active", "all"] = "active"


class BufferConfig(BaseModel):
    """Settings under the 'buffer' key."""

    model_config = _MODEL_CONFIG

    dir_path: Path  # required, no default

    @field_validator("dir_path", mode="before")
    @classmethod
    def _reject_empty(cls, value: object) -> object:
        """Reject empty string or None for dir_path, which is required.

        Args:
            value: The raw value from the YAML file or environment variable.

        Returns:
            The validated value if it is not empty.

        Raises:
            ValueError: If the value is empty or None.

        """
        if not value:
            msg = "'buffer.dir_path' must not be empty"
            raise ValueError(msg)
        return value


class QDashClientConfig(BaseModel):
    """Settings under the 'qdash_client' key."""

    model_config = _MODEL_CONFIG

    config_file: str = ""
    config_profile: str = "default"


class TargetsConfig(BaseModel):
    """Settings under the 'targets' key."""

    model_config = _MODEL_CONFIG

    qubit_metrics: tuple[str, ...] = ()
    coupling_metrics: tuple[str, ...] = ()

    @field_validator("qubit_metrics", "coupling_metrics", mode="before")
    @classmethod
    def _coerce_metric_list(cls, value: object) -> object:
        """Coerce a comma-separated string into a tuple of strings.

        Args:
            value: The raw value from the YAML file or environment variable.

        Returns:
            A tuple of metric names, or an empty tuple if the input is None.

        """
        if value is None:
            return ()
        if isinstance(value, str):
            return tuple(item.strip() for item in value.split(",") if item.strip())
        return value

    @model_validator(mode="after")
    def _validate_targets(self) -> TargetsConfig:
        """Validate that at least one of qubit_metrics or coupling_metrics is non-empty.

        Returns:
            The validated TargetsConfig instance.

        Raises:
            ValueError: If both qubit_metrics and coupling_metrics are empty,
                or if any metric name is invalid or duplicated across the two lists.

        """
        if not self.qubit_metrics and not self.coupling_metrics:
            msg = "at least one of 'qubit_metrics'/'coupling_metrics' must be non-empty"
            raise ValueError(msg)

        # Validate metric names and check for duplicates across both lists
        all_metrics = [*self.qubit_metrics, *self.coupling_metrics]
        seen: set[str] = set()
        for metric in all_metrics:
            if not METRIC_NAME_PATTERN.match(metric):
                msg = f"invalid metric name: {metric!r}"
                raise ValueError(msg)
            if metric in seen:
                msg = f"duplicate metric name across targets lists: {metric!r}"
                raise ValueError(msg)
            seen.add(metric)
        return self

    def enabled_metrics(self) -> frozenset[str]:
        """Get a frozen set of all enabled metrics (qubit and coupling).

        Returns:
            A frozen set containing all enabled metric names.

        """
        return frozenset(self.qubit_metrics) | frozenset(self.coupling_metrics)

    def kind_of(self, metric: str) -> MetricKind:
        """Return the kind of an enabled metric.

        Args:
            metric: The name of the metric to check.

        Returns:
            The MetricKind corresponding to the metric.

        Raises:
            KeyError: If the metric is not enabled.

        """
        if metric in self.qubit_metrics:
            return MetricKind.QUBIT
        if metric in self.coupling_metrics:
            return MetricKind.COUPLING
        raise KeyError(metric)


class AppConfig(BaseModel):
    """Root of the validated configuration."""

    model_config = _MODEL_CONFIG

    exporter: ExporterConfig = Field(default_factory=ExporterConfig)
    collection: CollectionConfig = Field(default_factory=CollectionConfig)
    buffer: BufferConfig  # required section
    qdash_client: QDashClientConfig = Field(default_factory=QDashClientConfig)
    targets: TargetsConfig = Field(default_factory=TargetsConfig)


def _apply_env_override(
    raw: dict[str, Any],
    path: tuple[str, ...],
    value: str,
    env_name: str,
) -> None:
    """Apply an environment variable override to the raw config dict.

    Args:
        raw: The raw configuration dictionary loaded from YAML.
        path: The path to the configuration value within the nested dictionary.
        value: The raw string value from the environment variable.
        env_name: The name of the environment variable providing the override.

    Raises:
        ConfigError: If an intermediate key in the path exists but is not a mapping.

    """
    node = raw
    for key in path[:-1]:
        existing = node.get(key)
        if existing is None:
            existing = {}
            node[key] = existing
        if not isinstance(existing, dict):
            msg = f"config section {key!r} is not a mapping (env override {env_name})"
            raise ConfigError(msg)
        node = existing
    node[path[-1]] = value


def load_config() -> AppConfig:
    """Load and validate the application configuration.

    Returns:
        An instance of AppConfig containing the validated configuration.

    Raises:
        ConfigError: If the configuration file cannot be read, is invalid YAML,
            or fails validation against the AppConfig model.

    """
    config_path = Path(
        os.environ.get("QDASH_EXPORTER_CONFIG_PATH", "./config/config.yaml")
    )

    try:
        with config_path.open("rt", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except OSError as exc:
        msg = f"cannot read config file {config_path}: {exc}"
        raise ConfigError(msg) from exc
    except yaml.YAMLError as exc:
        msg = f"invalid YAML in config file {config_path}: {exc}"
        raise ConfigError(msg) from exc

    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        msg = f"config file root must be a mapping: {config_path}"
        raise ConfigError(msg)

    for env_name, path in ENV_OVERRIDES:
        if env_name not in os.environ:
            continue
        _apply_env_override(raw, path, os.environ[env_name], env_name)

    try:
        return AppConfig.model_validate(raw)
    except ValidationError as exc:
        msg = f"invalid configuration: {exc}"
        raise ConfigError(msg) from exc
