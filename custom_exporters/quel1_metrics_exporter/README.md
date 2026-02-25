# QuEL1 Metrics Exporter

A Prometheus exporter that pings configured QuEL machines and exposes reachability metrics.

## Features

- ICMP reachability checks via `ping` for configured targets.
- Prometheus metrics endpoint with per-target status codes.
- Timezone-aware logging (configurable).
- Dockerized deployment with compose.

## Metrics

- `qubit_controller_ping_status_code{target_host, target_ip, controller_type}`: 0 = reachable, 1 = unreachable/error.  
  Emitted by `CustomCollector.collect` in [`src/quel1_metrics_exporter.py`](src/quel1_metrics_exporter.py).

## Configuration

The exporter loads configuration from `QUEL1_EXPORTER_CONFIG_PATH` (default `/config/config.yaml`). See [`config/config.yaml`](config/config.yaml).

Supported keys:

- `exporter.port`: HTTP port for the metrics exporter (default 9102).
- `exporter.timezone`: IANA timezone (default "UTC").
- `ping.timeout`: Ping timeout in seconds (default 5).
- `ping.targets[]`: List of `{name, ip, controller_type}` targets to check.

You can override via environment variables:

- `EXPORTER_PORT`
- `PING_TIMEOUT`
- `SERVER_TIMEZONE`
- `MAX_WORKERS`: Maximum number of concurrent ping workers. Controls ping concurrency (parallelism) used by the exporter to reduce scrape latency. If unset, the exporter uses its internal default.
- `PING_COUNT`: Number of ping attempts per target. Controls how many times the exporter will attempt to ping each target before determining reachability. If unset, the exporter uses its internal default.

Config loading logic: `load_config` function in [`src/quel1_metrics_exporter.py`](src/quel1_metrics_exporter.py).

## Logging

- If `/config/logging.yaml` exists, it is applied with timezone-aware formatter injection.
- Otherwise, a simple console logger is configured.

See:

- `setup_logging` function in [`src/quel1_metrics_exporter.py`](src/quel1_metrics_exporter.py)
- [`config/logging.yaml`](config/logging.yaml)

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

Requirements are managed by [`pyproject.toml`](pyproject.toml). Use Python 3.13.

- Install dependencies (recommended via `uv`):

  ```bash
  uv sync
  ```

- Run tests in terminal:

  ```bash
  make test
  ```

- Start exporter:

  ```bash
  uv run python src/quel1_metrics_exporter.py
  ```

Metrics will be available at `http://localhost:9102/metrics` when `exporter.port` is 9102.

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
  - Exposes port `9102`.
  - Mounts config files into `/config`.
  - Writes logs to the bind-mounted `./logs` directory.

Docker image installs:

- `iputils-ping` for ICMP checks (see [`Dockerfile`](Dockerfile)).
- Python deps from [`pyproject.toml`](pyproject.toml) using `uv`.

## Implementation Notes

Key functions and classes:

- Timezone-aware logging formatter creation: `create_timezone_formatter`
- Ping execution and validation: `ping_target`
- Prometheus collector: `CustomCollector`

All implemented in [`src/quel1_metrics_exporter.py`](src/quel1_metrics_exporter.py).

Main entrypoint (when run as a script) starts the HTTP server and registers the custom collector: see the `__main__` block in [`src/quel1_metrics_exporter.py`](src/quel1_metrics_exporter.py).

## Formatting, Linting, and Type Checking

Run formatter, linter, and mypy together:

```bash
make fmt-lint
```

## Testing

Unit tests cover:

- Timezone formatter behavior.
- Ping path scenarios (success, missing `ping`, invalid IP, unreachable, exception).
- Target discovery and metric collection.
- Logging setup with and without YAML.
- Config loading and environment overrides.

Run:

```bash
make test
```

Coverage settings are defined in [`pyproject.toml`](pyproject.toml).
