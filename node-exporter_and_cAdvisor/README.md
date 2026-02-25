# Node Exporter & cAdvisor (docker compose)

This compose project runs:

- Prometheus Node Exporter: host/node-level metrics on port `9100`
- Google cAdvisor: container-level metrics on port `8088`

Both are suitable scrape targets for Prometheus.

## Quick start

```bash
cd node-exporter_and_cAdvisor
docker compose up -d
```

Verify:

```bash
curl -fsS http://localhost:9100/metrics | head
curl -fsS http://localhost:8088/metrics | head
```

Stop and remove:

```bash
docker compose down -v
```

## Environment variables

- TIMEZONE (default: UTC)

  - Propagated to both containers as TZ

## Commands reference

```bash
# Start
docker compose up -d

# Follow logs
docker compose logs -f node-exporter
docker compose logs -f cadvisor

# Stop
docker compose down

# Recreate after edits
docker compose up -d --force-recreate
```
