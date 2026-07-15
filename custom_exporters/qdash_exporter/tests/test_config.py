from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from config import ENV_OVERRIDES, AppConfig, load_config
from models import ConfigError

if TYPE_CHECKING:
    from pathlib import Path

BASE_YAML = """
buffer:
  dir_path: /tmp/buffer
targets:
  qubit_metrics: [t1]
"""


def _use_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, content: str) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(content, encoding="utf-8")
    monkeypatch.setenv("QDASH_EXPORTER_CONFIG_PATH", str(path))


class TestLoadConfig:
    """Test suite for load_config function."""

    def test_load_config_env_overrides_yaml_value(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(tmp_path, monkeypatch, BASE_YAML + "exporter:\n  port: 9000\n")
        monkeypatch.setenv("EXPORTER_PORT", "9001")

        config = load_config()

        assert config.exporter.port == 9001

    def test_load_config_default_applies_when_absent(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(tmp_path, monkeypatch, BASE_YAML)

        config = load_config()

        assert config.exporter.port == 9104

    def test_load_config_missing_buffer_dir_path_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(tmp_path, monkeypatch, "targets:\n  qubit_metrics: [t1]\n")

        with pytest.raises(ConfigError):
            load_config()

    def test_load_config_no_metrics_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(tmp_path, monkeypatch, "buffer:\n  dir_path: /tmp/buffer\n")

        with pytest.raises(ConfigError):
            load_config()

    def test_load_config_accepts_metric_list_form(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(tmp_path, monkeypatch, BASE_YAML)

        config = load_config()

        assert config.targets.qubit_metrics == ("t1",)

    def test_load_config_accepts_comma_separated_metric_string(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(tmp_path, monkeypatch, "buffer:\n  dir_path: /tmp/buffer\n")
        monkeypatch.setenv("TARGETS_QUBIT_METRICS", "t1, t2_echo")

        config = load_config()

        assert config.targets.qubit_metrics == ("t1", "t2_echo")

    def test_load_config_port_out_of_range_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(tmp_path, monkeypatch, BASE_YAML + "exporter:\n  port: 70000\n")

        with pytest.raises(ConfigError):
            load_config()

    def test_load_config_invalid_chip_discovery_mode_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(
            tmp_path,
            monkeypatch,
            BASE_YAML + "collection:\n  chip_discovery_mode: bogus\n",
        )

        with pytest.raises(ConfigError):
            load_config()

    def test_load_config_invalid_metric_name_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(
            tmp_path,
            monkeypatch,
            "buffer:\n  dir_path: /tmp/buffer\ntargets:\n  qubit_metrics: ['1bad']\n",
        )

        with pytest.raises(ConfigError):
            load_config()

    def test_load_config_duplicate_metric_across_lists_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(
            tmp_path,
            monkeypatch,
            "buffer:\n  dir_path: /tmp/buffer\n"
            "targets:\n  qubit_metrics: [t1]\n  coupling_metrics: [t1]\n",
        )

        with pytest.raises(ConfigError):
            load_config()

    def test_load_config_unknown_key_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(tmp_path, monkeypatch, BASE_YAML + "unexpected_key: 1\n")

        with pytest.raises(ConfigError):
            load_config()

    def test_load_config_retry_max_attempts_zero_allowed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(
            tmp_path, monkeypatch, BASE_YAML + "collection:\n  retry_max_attempts: 0\n"
        )

        config = load_config()

        assert config.collection.retry_max_attempts == 0

    def test_load_config_non_numeric_env_for_int_field_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(tmp_path, monkeypatch, BASE_YAML)
        monkeypatch.setenv("COLLECTION_INTERVAL_SEC", "not-a-number")

        with pytest.raises(ConfigError):
            load_config()

    def test_load_config_invalid_yaml_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(tmp_path, monkeypatch, "buffer: [unclosed\n")

        with pytest.raises(ConfigError):
            load_config()

    def test_load_config_non_mapping_root_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(tmp_path, monkeypatch, "- a\n- b\n")

        with pytest.raises(ConfigError):
            load_config()

    def test_load_config_empty_file_with_env_overrides_succeeds(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(tmp_path, monkeypatch, "")
        monkeypatch.setenv("BUFFER_DIR_PATH", str(tmp_path / "buffer"))
        monkeypatch.setenv("TARGETS_QUBIT_METRICS", "t1")

        config = load_config()

        assert config.targets.qubit_metrics == ("t1",)

    def test_load_config_env_override_on_non_mapping_section_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _use_config(tmp_path, monkeypatch, "buffer: not-a-mapping\n")
        monkeypatch.setenv("BUFFER_DIR_PATH", str(tmp_path / "buffer"))

        with pytest.raises(ConfigError):
            load_config()


class TestEnvOverrideDefinitions:
    """Test suite for ENV_OVERRIDES definitions."""

    def test_env_overrides_paths_point_to_real_fields(self) -> None:
        for _env_name, path in ENV_OVERRIDES:
            model: Any = AppConfig
            for key in path[:-1]:
                model = model.model_fields[key].annotation

            assert path[-1] in model.model_fields

    def test_env_override_names_are_unique(self) -> None:
        names = [env_name for env_name, _path in ENV_OVERRIDES]

        assert len(names) == len(set(names))
