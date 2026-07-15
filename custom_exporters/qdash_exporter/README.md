# QDash Exporter

A Prometheus exporter that collects per-qubit and per-coupling calibration metrics from QDash and serves them to `vmagent`.

## Features

- A scheduled background collector fetches time-series metrics from QDash through `qdash.client`; QDash is never accessed on the `/metrics` request path.
- Every collection cycle discovers chips according to `chip_discovery_mode`; the default targets only chips with `activity_status=active`.
- Collected records are written to immutable batch files in the Local Spool File Buffer, which is reused after container restart/recreate.
- On upstream request failure after retries, the next cycle expands the collection window backward per `chip_id × metric` (configurable limit).
- Window state is persisted per `chip_id × metric` combination and reused across restarts.
- Timezone-aware logging and Dockerized deployment with compose.

## Metrics

Metrics are gauges named from the configured metric set as `qdash_<qubit|coupling>_<metric>`. Each sample carries its QDash source timestamp, converted to UTC UNIX epoch milliseconds.

- Qubit metrics
  - Labels: `chip_id`, `qubit_id`, `unit`
  - When QDash returns an `error` for a sample, a paired `qdash_qubit_<metric>_error` is emitted with the same labels and timestamp.
- Coupling metrics
  - Labels: `chip_id`, `coupling_id`, `unit`

`unit` is `""` when QDash provides none. `null`, non-numeric, `NaN`, `Infinity`, and untimestamped values are skipped.

## Configuration

The exporter loads configuration from `QDASH_EXPORTER_CONFIG_PATH` (default `./config/config.yaml`). See [`config/config.yaml`](config/config.yaml). Any YAML key can be overridden by the corresponding environment variable.

### Exporter Settings

- `exporter.port`: HTTP port for the metrics endpoint (default 9104).
- `exporter.timezone`: IANA timezone for logging (default "UTC").

### Collection Settings

- `collection.interval_sec`: Background collector interval in seconds (default 3600).
- `collection.retry_max_attempts`: Max retry attempts per QDash request (default 3).
- `collection.max_expand_windows`: Max collection windows kept in backward expansion (default 24).
- `collection.tag`: Optional tag passed to `qdash.client` (default "calibration").
- `collection.chip_discovery_mode`: `active` (only `activity_status=active` chips) or `all` (default "active").

### Buffer Settings

- `buffer.dir_path`: Directory for immutable batch files and window state (**required**, must be persistent storage).

### QDash Client Settings

- `qdash_client.config_file`: Path to the `qdash.client` config file.
- `qdash_client.config_profile`: Profile name within that config file (default "default").

### Target Metrics

- `targets.qubit_metrics`: List of qubit metric names to collect.
- `targets.coupling_metrics`: List of coupling metric names to collect.

At least one metric must be configured across `targets.qubit_metrics` and `targets.coupling_metrics`; otherwise startup fails.

### Environment Variable Overrides

- `EXPORTER_PORT`, `EXPORTER_TIMEZONE`
- `COLLECTION_INTERVAL_SEC`, `COLLECTION_RETRY_MAX_ATTEMPTS`, `COLLECTION_MAX_EXPAND_WINDOWS`, `COLLECTION_TAG`, `COLLECTION_CHIP_DISCOVERY_MODE`
- `BUFFER_DIR_PATH`
- `QDASH_CLIENT_CONFIG_FILE`, `QDASH_CLIENT_CONFIG_PROFILE`
- `TARGETS_QUBIT_METRICS`, `TARGETS_COUPLING_METRICS`

QDash connection, authentication, TLS, proxy, and timeout settings are provided through `qdash.client` configuration.

Additional exporter environment variables:

- `QDASH_EXPORTER_CONFIG_PATH` (default `./config/config.yaml`)
- `QDASH_EXPORTER_LOGGING_CONFIG_PATH` (default `./config/logging.yaml`)
- `QDASH_EXPORTER_LOGGING_DIR_PATH` (default `./logs`)

## Local Spool File Buffer

Under `buffer.dir_path` the exporter maintains:

- `pending/<batch_id>.json`: immutable pending batches (one per `chip_id × metric × window`).
- `tmp/<batch_id>.json.tmp`: temporary writes, made visible via atomic rename.
- `state/window_state.json`: per-`chip_id × metric` window state cache.

Batch files are deleted only after a `/metrics` response that includes their samples completes successfully. The buffer must live on persistent storage so batches and window state are reused after restart/recreate. On startup the exporter validates the window state and pending batch files, and fails fast on corruption.

## `/metrics` Endpoint

`vmagent` scrapes `/metrics` (HTTP GET, no auth), which returns only pending buffered samples in Prometheus Text Exposition Format and never triggers a QDash request.

- `200 OK`: at least one buffered sample is deliverable.
- `503 Service Unavailable`: no deliverable buffered sample (treat repeated 503 as a data-freshness alert).
- `500 Internal Server Error`: unrecoverable exporter-local pull-path fault (e.g. buffer corruption).

## Logging

The exporter loads logging configuration from `QDASH_EXPORTER_LOGGING_CONFIG_PATH` (default `./config/logging.yaml`), applied with timezone-aware formatting. Logs are written to `/logs`, bind-mounted from `QDASH_EXPORTER_LOGGING_DIR_PATH` (default `./logs`).

Ensure the host log directory is writable by the container user:

```bash
mkdir -p ./logs
sudo chmod 0775 ./logs
```

## Running Locally (Python)

Requires Python 3.13. Dependencies are managed by [`pyproject.toml`](pyproject.toml).

```bash
uv sync                       # install dependencies
uv run python src/main.py     # start exporter
```

Metrics are available at `http://localhost:9104/metrics` when `exporter.port` is 9104.

## Docker

```bash
make build   # build image
make up      # run service
make down    # stop service
```

The container exposes port `9104`, mounts config files into `/config`, persists the spool buffer under `/data/qdash-buffer`, and writes logs to the bind-mounted `./logs` directory.

## Formatting, Linting, and Type Checking

```bash
make fmt-lint
```

## Testing

```bash
make test
```

Coverage settings are defined in [`pyproject.toml`](pyproject.toml).
