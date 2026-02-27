# API Server

A FastAPI-based REST API server for managing metrics and metadata in VictoriaMetrics.

## Features

- Global lock mechanism to prevent conflicts during concurrent background tasks.
- Track and persist background tasks to disk.
- Timezone-aware logging (configurable).
- Dockerized deployment with compose.

## Configuration and Logging

For detailed configuration options, including server settings, VictoriaMetrics connection parameters, logging setup, and environment variable overrides, see the Configuration section in [oqtopus-device-monitoring/docs/api-server/api-server.md](../docs/api-server/api-server.md).

### Directory permissions

The api-server writes logs and operation history to `/app/logs` and `/app/data` (bind-mounted from `./logs` and `./data`). Ensure the host directory is writable by the container user:

- Directory permission: at least `775`.
- Owner/group: should match the container UID/GID used in the container.

Set up on Linux:

```bash
mkdir -p ./logs ./data
sudo chmod 0775 ./logs ./data
```

## Docker

Build and run with Dockerfile:

- Build image:

  ```bash
  make build-image
  ```

- Run service:

  ```bash
  make run
  ```

- Stop service:

  ```bash
  make stop
  ```

- The container:
  - Exposes port `8080`.
  - Mounts config files into `./config`.
  - Writes logs to the bind-mounted `./logs` directory.

**Initial Setup**:
Ensure VictoriaMetrics is accessible at the URL specified in `VICTORIAMETRICS_URL` environment variable or `config.yaml`. The server will validate the connection on startup and log any connectivity issues.

Docker image installs:

- Python deps from [pyproject.toml](../../pyproject.toml) using `uv`.
- `uvicorn` ASGI server for production-ready deployment (see [Dockerfile](Dockerfile)).

## Implementation Notes

Key modules and classes:

- **API Routers**:
  - [routers/metrics.py](routers/metrics.py): Metrics querying endpoints (`/metrics/query`, `/metrics/export`, etc.)
  - [routers/meta.py](routers/meta.py): Metadata management endpoints (`/meta/add-label`, `/meta/remove-label`, etc.)

- **Core Components**:
  - [common/config.py](common/config.py): Configuration loader with environment variable override support.
  - [common/logger.py](common/logger.py): Timezone-aware structured JSON logging setup.
  - [common/victoria_metrics.py](common/victoria_metrics.py): VictoriaMetrics HTTP client.
  - [common/operations.py](common/operations.py): Global lock and operation history management.
  - [common/background_tasks.py](common/background_tasks.py): Async task executor for metadata operations.

- **Data Models**:
  - [common/types/config.py](common/types/config.py): Configuration data types.
  - [common/types/operation.py](common/types/operation.py): Operation and step data types.
  - [common/types/victoria_metrics.py](common/types/victoria_metrics.py): VictoriaMetrics response types.

## Formatting, Linting, and Type Checking

Run formatter, linter, and mypy together:

```bash
make fmt-lint
```

This applies:

- **Ruff formatter**: Code style normalization.
- **Ruff linter**: Python code quality checks (all rules).
- **MyPy**: Strict type checking with unannotated functions disallowed.

## Testing

Run:

```bash
make test
```
