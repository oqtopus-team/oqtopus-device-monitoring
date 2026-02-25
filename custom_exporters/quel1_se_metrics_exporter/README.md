# QuEL-1 SE Metrics Exporter

A Prometheus exporter that collects temperature and actuator metrics from QuEL-1 SE qubit controlling machines via the `quel_ic_config` WSS (Wave Generation Subsystem) APIs.

## Features

- Temperature readings from multiple sensors via WSS APIs
- Actuator (fan/heater) duty ratio monitoring
- Concurrent target collection using ThreadPoolExecutor
- Prometheus metrics endpoint with per-target status
- Timezone-aware logging (configurable)
- Dockerized deployment with compose
- Graceful error handling with partial success support

## Metrics

| Metric Name                       | Labels                                                              | Description                    |
| --------------------------------- | ------------------------------------------------------------------- | ------------------------------ |
| `qubit_controller_temperature`    | `target_name`, `wss_ip`, `location`, `unit`, `raw`                  | Per-sensor temperature reading |
| `qubit_controller_actuator_usage` | `target_name`, `wss_ip`, `actuator_type`, `location`, `unit`, `raw` | Actuator duty ratio [0,1]      |

Emitted by `Quel1seMetricsCollector.collect` in [src/quel1_se_metrics_exporter.py](src/quel1_se_metrics_exporter.py).

## Configuration

The exporter loads configuration from `QUEL1SE_EXPORTER_CONFIG_PATH` (default `./config/config.yaml`). See [config/config.yaml](config/config.yaml).

Supported keys:

- `exporter.port`: HTTP port for the metrics server (default 9103).
- `exporter.timezone`: IANA timezone for logs (default "UTC").
- `quel1se.timeout`: Per-target timeout in seconds (default 5).
- `quel1se.targets[]`: List of `{name, wss_ip, css_ip, boxtype}` targets to monitor.

You can override via environment variables:

- `QUBIT_CONTROLLER_EXPORTER_PORT`
- `EXPORTER_TIMEZONE `
- `QUEL1SE_TIMEOUT`
- `MAX_WORKERS`: Maximum number of concurrent collection workers.

Config loading logic: `load_config` function in [src/quel1_se_metrics_exporter.py](src/quel1_se_metrics_exporter.py).

## Logging

- If `/config/logging.yaml` exists, it is applied with timezone-aware formatter injection.
- Otherwise, a simple console logger is configured.

See:

- `setup_logging` function in [src/quel1_se_metrics_exporter.py](src/quel1_se_metrics_exporter.py)
- [config/logging.yaml](config/logging.yaml)

## Logs directory permissions

The exporter writes logs to `/logs` (bind-mounted from `./logs`). Ensure the host directory is writable by the container user:

- Directory permission: at least `775`.
- Owner/group: should match the container UID/GID used in the container.

Set up on Linux:

```bash
mkdir -p ./logs
sudo chmod 0775 ./logs
```

If a PermissionError for /logs/\*.log occurs, align container UID/GID via compose build args or temporarily run the service as root.

## Running Locally (Python)

Requirements are managed by [pyproject.toml](pyproject.toml). Use Python 3.12.

- Install dependencies (recommended via `uv`):

  ```bash
  uv sync
  ```

- Install quel_ic_config (required for actual device communication):

  ```bash
  pip install quel_ic_config
  ```

- Run tests in terminal:

  ```bash
  make test
  ```

- Start exporter:

  ```bash
  uv run python src/quel1_se_metrics_exporter.py
  ```

Metrics will be available at `http://localhost:9103/metrics` when `exporter.port` is 9103.

## Docker

Build and run with compose:

- Build image:

  ```bash
  make build
  ```

- Run service:

  ```bash
  make up
  ```

- Stop service:

  ```bash
  make down
  ```

- The container:
  - Exposes port `9103`.
  - Mounts config files into `/config`.
  - Writes logs to the bind-mounted `./logs` directory.

Docker image installs:

- Python deps from [pyproject.toml](pyproject.toml) using `uv`.
- Note: `quel_ic_config` must be installed separately or added to dependencies.

## Implementation Notes

Key functions and classes:

- Timezone-aware logging formatter creation: `create_timezone_formatter`
- Target metrics collection: `collect_target_metrics`
- Prometheus collector: `Quel1seMetricsCollector`
- Target validation: `validate_quel1se_targets`

All implemented in [src/quel1_se_metrics_exporter.py](src/quel1_se_metrics_exporter.py).

Main entrypoint (when run as a script) starts the HTTP server and registers the custom collector: see the source code [src/quel1_se_metrics_exporter.py](src/quel1_se_metrics_exporter.py).

## Architecture

```
vmagent --> HTTP GET /metrics --> quel1-se-exporter --> WSS API --> QuEL-1 SE devices
```

On each scrape:

1. vmagent sends HTTP GET request to `/metrics`
2. Exporter fans out to all configured targets using ThreadPoolExecutor
3. For each target, reads temperature and actuator data via WSS APIs
4. Aggregates results into Prometheus exposition format
5. Returns metrics to vmagent

## Formatting, Linting, and Type Checking

Run formatter, linter, and mypy together:

```bash
make fmt-lint
```

## Testing

Unit tests cover:

- Timezone formatter behavior.
- Target validation and dataclass behavior.
- Metrics collection with mock WSS clients.
- Error handling (timeout, connection errors, etc.).
- Logging setup with and without YAML.
- Config loading and environment overrides.

Run:

```bash
make test
```

Coverage settings are defined in [pyproject.toml](pyproject.toml).
