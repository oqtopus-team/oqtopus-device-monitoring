# Cryo Metrics Exporter

A Prometheus exporter that fetches metrics from HTTP and FTP data sources for cryogenic refrigerators.

## Features

- HTTP and FTP data retrievals are executed in parallel for efficient data collection.
- When no data is returned, the exporter retries on the next pull with an expanded time window (configurable limit).
- Timezone-aware logging with configurable settings.
- Dockerized deployment with compose.

## Metrics

The exporter exposes the following metrics. These metrics are emitted by `CustomCollector.collect` in [`src/cryo_metrics_exporter.py`](src/cryo_metrics_exporter.py).

### Temperature Metrics

- `refrigerator_temperature`: Temperature
  - Labels: `device_name`, `unit: kelvin`, `stage`, `location`, `raw`
  - Source: HTTP API

### Pressure Metrics

- `refrigerator_pressure`: Pressure
  - Labels: `device_name`, `unit: millibar`, `location`
  - Additional unit-converted variant: `unit: kilopascal` or `unit: pascal` depending on `location`.
  - Source: FTP server

### Helium Flow Metrics

- `refrigerator_helium_flow`: Helium flow rate
  - Labels: `device_name`, `unit: millimoles per second`, `raw`
  - Additional unit-converted variant: `unit: micromoles per second`
  - Source: FTP server

### Device Status Metrics

- `refrigerator_device_status`: Device status (0 or 1)
  - Labels: `device_name`, `unit: None`, `component`, `raw`
  - Source: FTP server

### Compressor Metrics

- `refrigerator_compressor`: Compressor actual speed
  - Labels: `device_name`, `unit: Hz`, `rotation`, `raw`
  - Source: FTP server

- `refrigerator_compressor_pressure`: Compressor pressure
  - Labels: `device_name`, `unit: psig`, `side`, `raw`
  - Additional unit-converted variant: `unit: megapascal`
  - Source: FTP server

## Configuration

The exporter loads configuration from `CRYO_EXPORTER_CONFIG_PATH` (default `./config/config.yaml`). See [`config/config.yaml`](config/config.yaml).

Supported sections and keys:

### Exporter Settings

- `exporter.port`: HTTP port for metrics endpoint (default 9101).
- `exporter.timezone`: IANA timezone for logging (default "UTC").
- `exporter.device_name`: Device name (required).

### Retrieval Settings

- `retrieval.scrape_interval_sec`: Prometheus scrape interval in seconds (default 60).
- `retrieval.max_expand_windows.http`: Max window expansion for HTTP retries (default 5).
- `retrieval.max_expand_windows.ftp`: Max window expansion for FTP retries (default 5).

### HTTP Data Source

- `sources.http.datasource_timezone`: Timezone of HTTP source timestamps (default "UTC").
- `sources.http.url`: Base URL for HTTP API (required).
- `sources.http.port`: HTTP API port (default 80).
- `sources.http.timeout_sec`: Request timeout in seconds (default 5).

### FTP Data Source

- `sources.ftp.datasource_timezone`: Timezone of FTP source timestamps (default "UTC").
- `sources.ftp.host`: FTP server hostname (required).
- `sources.ftp.port`: FTP server port (default 21).
- `sources.ftp.user`: FTP username (required).
- `sources.ftp.base_path`: FTP base directory (default "~/").
- `sources.ftp.timeout_sec`: Connection timeout in seconds (default 5).

You can override via environment variables:

- `EXPORTER_PORT`
- `EXPORTER_TIMEZONE`
- `EXPORTER_DEVICE_NAME`
- `RETRIEVAL_SCRAPE_INTERVAL_SEC`
- `RETRIEVAL_MAX_EXPAND_WINDOWS_HTTP`
- `RETRIEVAL_MAX_EXPAND_WINDOWS_FTP`
- `SOURCES_HTTP_DATASOURCE_TIMEZONE`
- `SOURCES_HTTP_URL`
- `SOURCES_HTTP_PORT`
- `SOURCES_HTTP_TIMEOUT_SEC`
- `SOURCES_FTP_DATASOURCE_TIMEZONE`
- `SOURCES_FTP_HOST`
- `SOURCES_FTP_PORT`
- `SOURCES_FTP_USER`
- `SOURCES_FTP_BASE_PATH`
- `SOURCES_FTP_TIMEOUT_SEC`
- `FTP_PASSWORD`: FTP password (injected via environment variable only).

Configuration loading logic: `setup_config` function in [`src/cryo_metrics_exporter.py`](src/cryo_metrics_exporter.py).

**_Note_**:
`FTP_PASSWORD` is required for this containerized exporter to connect to the FTP server. Set it in your environment or compose file.

## Logging

The exporter loads configuration for logging from `CRYO_EXPORTER_LOGGING_CONFIG_PATH` (default `./config/logging.yaml`).

- If the configuration file exists, it is applied with timezone-aware formatter injection.
- Otherwise, a simple console logger is configured.

See:

- `setup_logging` function in [`src/cryo_metrics_exporter.py`](src/cryo_metrics_exporter.py)
- [`config/logging.yaml`](config/logging.yaml)

## Logs Directory Permissions

The exporter writes logs to `/logs`, bind-mounted from `CRYO_EXPORTER_LOGGING_DIR_PATH` (default `./logs`).

Ensure the host directory is writable by the container user:

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
  uv run python src/cryo_metrics_exporter.py
  ```

Metrics will be available at `http://localhost:9101/metrics` when `exporter.port` is 9101.

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

The container:

- Exposes port `9101`.
- Mounts config files into `/config`.
- Writes logs to the bind-mounted `./logs` directory.

Docker image installs:

- Python dependencies from [`pyproject.toml`](pyproject.toml) using `uv`.

## Implementation Notes

Key functions and classes:

- Timezone-aware logging formatter creation: `create_timezone_formatter`
- Prometheus collector: `CustomCollector`

See complete implementation in [`src/cryo_metrics_exporter.py`](src/cryo_metrics_exporter.py).

Main entrypoint (when run as script) starts the HTTP server and registers the custom collector: see the `__main__` block in [`src/cryo_metrics_exporter.py`](src/cryo_metrics_exporter.py).

## Formatting, Linting, and Type Checking

Run formatter, linter, and type checker together:

```bash
make fmt-lint
```

## Testing

Unit tests cover:

- Timezone formatter behavior.
- Configuration loading with environment overrides.
- Logging setup with and without YAML file.
- Data retrieval from the HTTP API.
- Data retrieval and parsing CSV/Text files from FTP server.
- Metric collection in Prometheus format from HTTP/FTP servers.
- Retry logic and time window expansion.

Run:

```bash
make test
```

Coverage settings are defined in [`pyproject.toml`](pyproject.toml).
