# Getting Started

A step-by-step guide for setting up the qdash2 data collection platform for quantum computers.

This document covers everything from infrastructure setup and custom exporter configuration to data visualization and alerting in Grafana, and metadata management via the API server — designed so that first-time users can follow along from start to finish.

## Table of Contents

- [1. Prerequisites](#1-prerequisites)
- [2. System Architecture Overview](#2-system-architecture-overview)
- [3. Repository Structure](#3-repository-structure)
- [4. Deployment Order](#4-deployment-order)
- [5. Configuration Files Overview and How to Edit Them](#5-configuration-files-overview-and-how-to-edit-them)
- [6. Step 1 — Setting Up VictoriaMetrics Cluster](#6-step-1--setting-up-victoriametrics-cluster)
- [7. Step 2 — Deploying Node Exporter & cAdvisor](#7-step-2--deploying-node-exporter--cadvisor)
- [8. Step 3 — Deploying Custom Exporters](#8-step-3--deploying-custom-exporters)
  - [8.1 cryo-metrics-exporter (Dilution Refrigerator)](#81-cryo-metrics-exporter-dilution-refrigerator)
  - [8.2 quel1-metrics-exporter (QuEL Controller Ping Monitoring)](#82-quel1-metrics-exporter-quel-controller-ping-monitoring)
  - [8.3 quel1-se-metrics-exporter (QuEL-1 SE Temperature & Actuator)](#83-quel1-se-metrics-exporter-quel-1-se-temperature--actuator)
- [9. Step 4 — Deploying Grafana & Loki](#9-step-4--deploying-grafana--loki)
- [10. Step 5 — Deploying the API Server](#10-step-5--deploying-the-api-server)
- [11. Post-Deployment Verification](#11-post-deployment-verification)
- [12. How to Use Grafana](#12-how-to-use-grafana)
  - [12.1 Login and Home Screen](#121-login-and-home-screen)
  - [12.2 Preset Dashboards](#122-preset-dashboards)
  - [12.3 Navigating Dashboards](#123-navigating-dashboards)
  - [12.4 Running Ad Hoc Queries in Explore](#124-running-ad-hoc-queries-in-explore)
- [13. Creating Custom Alerts](#13-creating-custom-alerts)
  - [13.1 Preset Alerts](#131-preset-alerts)
  - [13.2 Creating Alerts from the Grafana UI](#132-creating-alerts-from-the-grafana-ui)
  - [13.3 Creating Alerts via Provisioning Files (Recommended)](#133-creating-alerts-via-provisioning-files-recommended)
  - [13.4 Configuring Contact Points](#134-configuring-contact-points)
  - [13.5 Configuring Notification Policies](#135-configuring-notification-policies)
- [14. How to Use the API Server](#14-how-to-use-the-api-server)
  - [14.1 API Endpoint Reference](#141-api-endpoint-reference)
  - [14.2 Retrieving Metric Names](#142-retrieving-metric-names)
  - [14.3 Retrieving Label Keys and Label Values](#143-retrieving-label-keys-and-label-values)
  - [14.4 Retrieving Time Series Data](#144-retrieving-time-series-data)
  - [14.5 Modifying Metadata (Adding Labels)](#145-modifying-metadata-adding-labels)
  - [14.6 Modifying Metadata (Renaming Label Keys)](#146-modifying-metadata-renaming-label-keys)
  - [14.7 Modifying Metadata (Changing Label Values)](#147-modifying-metadata-changing-label-values)
  - [14.8 Deleting Metadata (Removing Labels)](#148-deleting-metadata-removing-labels)
  - [14.9 Deleting Time Series Data](#149-deleting-time-series-data)
  - [14.10 Checking Operation Status](#1410-checking-operation-status)
- [15. Operational Procedures](#15-operational-procedures)
  - [15.1 Adding Scrape Targets](#151-adding-scrape-targets)
  - [15.2 Adding a New Custom Exporter](#152-adding-a-new-custom-exporter)
  - [15.3 Scaling vmagent](#153-scaling-vmagent)
  - [15.4 Timezone Configuration](#154-timezone-configuration)
  - [15.5 Backup and Data Persistence](#155-backup-and-data-persistence)
- [16. Developer Information](#16-developer-information)
- [17. Troubleshooting](#17-troubleshooting)
- [18. Port Reference](#18-port-reference)

---

## 1. Prerequisites

The following tools must be installed on the target machine(s).

| Tool                             | Version | Purpose                             |
| :------------------------------- | :------ | :---------------------------------- |
| Docker                           | 24+     | Container runtime                   |
| Docker Compose                   | v2+     | Multi-container orchestration       |
| Python                           | 3.13+   | Custom exporters / API server       |
| [uv](https://docs.astral.sh/uv/) | latest  | Python package & project management |
| Git                              | 2.x     | Source control                      |
| curl                             | any     | Verification                        |

> **Important**: All host machines must be synchronized with the same **NTP server**. Timestamps must be consistent across data sources, monitoring servers, and quantum computer controllers.

---

## 2. System Architecture Overview

```mermaid
flowchart LR
  subgraph Monitoring Targets
    quel1[QuEL Controller]
    quel1se[QuEL-1 SE Controller]
    cryo[Dilution Refrigerator<br>HTTP/SMB Server]
    host[Host OS]
    containers[Docker Containers]
  end

  subgraph Exporters
    ne[node-exporter :9100]
    ca[cAdvisor :8088]
    ce[cryo-metrics-exporter :9101]
    q1e[quel1-metrics-exporter :9102]
    q1se[quel1-se-metrics-exporter :9103]
  end

  subgraph VictoriaMetrics Cluster
    vmagent[vmagent :8429]
    vmauth_i[vmauth-insert :8427]
    vminsert1[vminsert-1]
    vminsert2[vminsert-2]
    vmstorage1[vmstorage-1]
    vmstorage2[vmstorage-2]
    vmselect1[vmselect-1]
    vmselect2[vmselect-2]
    vmauth_s[vmauth-select :8428]
  end

  subgraph Visualization
    grafana[Grafana :3000]
    loki[Loki :3100]
  end

  subgraph API
    apiserver[api-server :8080]
  end

  host --> ne
  containers --> ca
  cryo --> ce
  quel1 --> q1e
  quel1se --> q1se

  ne --> vmagent
  ca --> vmagent
  ce --> vmagent
  q1e --> vmagent
  q1se --> vmagent

  vmagent --> vmauth_i --> vminsert1 & vminsert2
  vminsert1 & vminsert2 --> vmstorage1 & vmstorage2
  vmstorage1 & vmstorage2 --> vmselect1 & vmselect2
  vmselect1 & vmselect2 --> vmauth_s

  vmauth_s --> grafana
  vmauth_s --> apiserver
```

**Data Flow**:

1. Each exporter collects metrics from its monitoring target and exposes them via the `/metrics` endpoint.
2. `vmagent` periodically scrapes (pulls) metrics from each exporter.
3. Scraped data is stored through the path: `vmauth-insert` → `vminsert` → `vmstorage`.
4. Grafana and the API server query data via `vmauth-select` → `vmselect`.

---

## 3. Repository Structure

```tree
qdash2/
├── docs/                               # Design documents & API specs
│   ├── system-design.md                #   System design document
│   ├── vmagent-scaling.md              #   vmagent scaling guide
│   ├── api-server/
│   │   ├── api-server.md               #   API server detailed design
│   │   └── oas/openapi.yaml            #   OpenAPI specification
│   └── custom-exporters/               #   Custom exporter detailed designs
├── victoriametrics-cluster/            # VictoriaMetrics Cluster + vmagent
│   ├── compose.yaml
│   ├── vmagent/prometheus.yml          #   Scrape target configuration
│   ├── vmauth-insert/auth.yml          #   Insert-side routing config
│   ├── vmauth-select/auth.yml          #   Select-side routing config
│   └── weekly-relabeling-cron/         #   Weekly relabeling cron job
├── node-exporter_and_cAdvisor/         # OS & container metrics
│   └── compose.yaml
├── grafana-and-loki/                   # Dashboards, alerts & log aggregation
│   ├── compose.yaml
│   ├── Makefile
│   ├── grafana/
│   │   ├── dashboards/                 #   Preset dashboard JSON files
│   │   ├── images/                     #   Custom images
│   │   └── provisioning/
│   │       ├── alerting/               #   Alert rules & notification configs
│   │       ├── dashboards/             #   Dashboard provider config
│   │       └── datasources/            #   Data source config
│   └── loki/
├── api_server/                         # RESTful API server
│   ├── compose.yaml
│   ├── Makefile
│   ├── config/
│   │   ├── config.yaml                 #   Application configuration
│   │   └── logging.yaml                #   Logging configuration
│   └── src/
└── custom_exporters/
    ├── cryo_metrics_exporter/          # Dilution refrigerator exporter
    ├── quel1_metrics_exporter/         # QuEL controller ping exporter
    └── quel1_se_metrics_exporter/      # QuEL-1 SE temperature exporter
```

---

## 4. Deployment Order

Deploy the components in the following order based on their dependencies.

| Order | Component                         | Dependencies                               |
| :---: | :-------------------------------- | :----------------------------------------- |
|   1   | VictoriaMetrics Cluster + vmagent | None                                       |
|   2   | Node Exporter & cAdvisor          | None                                       |
|   3   | Custom Exporters                  | Network connectivity to monitoring targets |
|   4   | Grafana & Loki                    | VictoriaMetrics Cluster                    |
|   5   | API Server                        | VictoriaMetrics Cluster                    |

> **Tip**: Steps 2 and 3 can be deployed in parallel. Steps 4 and 5 can also be deployed in parallel, but both require the VictoriaMetrics Cluster from Step 1 to be running.

---

## 5. Configuration Files Overview and How to Edit Them

This section lists the configuration files that need to be modified for your environment during initial setup. Details for each file are described in subsequent sections.

### 5.1 Files That Must Be Modified

| File                                                               | What to Change                                                |
| :----------------------------------------------------------------- | :------------------------------------------------------------ |
| `victoriametrics-cluster/vmagent/prometheus.yml`                   | Set scrape target IP addresses and ports for your environment |
| `grafana-and-loki/grafana/provisioning/datasources/datasource.yml` | Set the VictoriaMetrics host IP                               |
| `custom_exporters/cryo_metrics_exporter/.env`                      | Set the SMB password (`SMB_PASSWORD`)                         |
| `custom_exporters/cryo_metrics_exporter/config/config.yaml`        | Set HTTP/SMB data source connection details                   |
| `custom_exporters/quel1_metrics_exporter/config/config.yaml`       | Set ping target names and IP addresses                        |
| `custom_exporters/quel1_se_metrics_exporter/config/config.yaml`    | Set QuEL-1 SE target WSS/CSS IPs                              |
| `api_server/config/config.yaml`                                    | Set the VictoriaMetrics URL                                   |

### 5.2 Files to Consider Modifying Based on Your Environment

| File                                                                  | What to Change                            |
| :-------------------------------------------------------------------- | :---------------------------------------- |
| `victoriametrics-cluster/.env`                                        | Timezone (default: UTC)                   |
| `node-exporter_and_cAdvisor/.env`                                     | Timezone (default: UTC)                   |
| `grafana-and-loki/.env`                                               | Grafana admin password and timezone       |
| `grafana-and-loki/grafana/provisioning/alerting/alert_resources.yaml` | Slack channel ID and Bot User OAuth Token |
| `grafana-and-loki/grafana/provisioning/alerting/alert_rules.yaml`     | Alert thresholds                          |
| `custom_exporters/cryo_metrics_exporter/.env`                         | Timezone (default: UTC)                   |
| `custom_exporters/quel1_metrics_exporter/.env`                        | Timezone (default: UTC)                   |
| `custom_exporters/quel1_se_metrics_exporter/.env`                     | Timezone (default: UTC)                   |

### 5.3 Configuration Priority

All custom components (API server, custom exporters) determine their configuration using the following priority order (highest priority first):

1. **Environment variables** (e.g., `API_SERVER_PORT`, `EXPORTER_PORT`)
2. **YAML configuration file** values (e.g., `config/config.yaml`)
3. **Default values** (hard-coded in the application)

In other words, setting an environment variable overrides the value in the YAML file. Using the `environment` section in Docker Compose to set environment variables is the most flexible approach in containerized environments.

---

## 6. Step 1 — Setting Up VictoriaMetrics Cluster

VictoriaMetrics Cluster is the metrics storage backend. It consists of `vmstorage` (storage), `vminsert` (data ingestion), `vmselect` (query), `vmauth` (load balancer), `vmagent` (scraper), and a weekly relabeling cron job.

### 6.1 Configure Scrape Targets

Open `victoriametrics-cluster/vmagent/prometheus.yml` and set the IP addresses and ports for each exporter.

```yaml
global:
  scrape_interval: 10s
  external_labels:
    env: production

scrape_configs:
  - job_name: "node"
    scrape_interval: 60s
    static_configs:
      - targets: ["<NODE_EXPORTER_IP>:9100"]
      # Example: - targets: ["10.20.30.40:9100"]

  - job_name: "cadvisor"
    scrape_interval: 60s
    static_configs:
      - targets: ["<CADVISOR_IP>:8088"]

  - job_name: "virtual-machine"
    scrape_interval: 60s
    honor_timestamps: true
    static_configs:
      - targets: ["<CRYO_EXPORTER_IP>:9101"]

  - job_name: "quel1"
    scrape_interval: 60s
    static_configs:
      - targets: ["<QUEL1_EXPORTER_IP>:9102"]

  - job_name: "quel1-se"
    scrape_interval: 60s
    static_configs:
      - targets: ["<QUEL1SE_EXPORTER_IP>:9103"]
```

Replace each `<*_IP>` placeholder with the actual host IP address.

### 6.2 Set the Timezone (Optional)

Create `victoriametrics-cluster/.env` and specify the timezone.

```bash
# victoriametrics-cluster/.env
TIMEZONE_VM_CLUSTER=Asia/Tokyo
```

### 6.3 Start the Services

```bash
cd victoriametrics-cluster
docker compose up -d
```

### 6.4 Verify Operation

```bash
# Health check for vmselect
curl http://localhost:8428/select/0/prometheus/api/v1/status/buildinfo

# View vmagent targets in your browser
# Open http://localhost:8429/targets
```

---

## 7. Step 2 — Deploying Node Exporter & cAdvisor

These collect OS-level metrics (CPU usage, memory, disk, etc.) and container metrics. **Deploy on each host machine you want to monitor.**

### 7.1 Set the Timezone (Optional)

```bash
# node-exporter_and_cAdvisor/.env
TIMEZONE=Asia/Tokyo
```

### 7.2 Start the Services

```bash
cd node-exporter_and_cAdvisor
docker compose up -d
```

### 7.3 Verify Operation

```bash
# Check Node Exporter metrics
curl -fsS http://localhost:9100/metrics | head

# Check cAdvisor metrics
curl -fsS http://localhost:8088/metrics | head
```

---

## 8. Step 3 — Deploying Custom Exporters

### 8.1 cryo-metrics-exporter (Dilution Refrigerator)

An exporter that collects temperature, pressure, and gas flow data from dilution refrigerators via HTTP/SMB.

#### Edit the Configuration File

Edit `custom_exporters/cryo_metrics_exporter/config/config.yaml`.

```yaml
exporter:
  port: 9101 # Exporter listen port
  timezone: "Asia/Tokyo" # Timezone for logs
  device_name: "your.refrigerator.name" # Common label value for metrics

retrieval:
  scrape_interval_sec: 60 # Match with vmagent scrape interval

sources:
  http:
    url: "http://<HTTP_DATA_SOURCE_IP>" # HTTP data source URL
    port: 80
    datasource_timezone: "Asia/Tokyo" # Timezone of the data source
    timeout_sec: 5
  smb:
    server: "<SMB_SERVER_IP>" # SMB server IP
    share: "<SMB_SHARE>" # SMB share name
    port: 445
    username: "<SMB_USERNAME>" # SMB username
    datasource_timezone: "Asia/Tokyo"
    base_path: ""
    timeout_sec: 5
```

Set the SMB password in a `.env` file (do not put it in `config.yaml`).

```bash
# custom_exporters/cryo_metrics_exporter/.env
SMB_PASSWORD=your_smb_password
```

#### Create the Logs Directory

Create a `custom_exporters/cryo_metrics_exporter/logs/` directory and set directory permissions.

```bash
mkdir -p custom_exporters/cryo_metrics_exporter/logs
sudo chmod 755 custom_exporters/cryo_metrics_exporter/logs
```

#### Build and Start

```bash
cd custom_exporters/cryo_metrics_exporter
make build
make up
```

#### Verify Operation

```bash
curl http://localhost:9101/metrics
```

---

### 8.2 quel1-metrics-exporter (QuEL Controller Ping Monitoring)

An exporter that monitors the availability of QuEL quantum computer controllers using ICMP ping.

#### Edit the Configuration File

Edit `custom_exporters/quel1_metrics_exporter/config/config.yaml`.

```yaml
exporter:
  port: 9102
  timezone: "Asia/Tokyo"

ping:
  timeout: 5 # Ping timeout (seconds)
  count: 3 # Number of ICMP packets to send
  targets: # List of monitoring targets (at least one required)
    - name: "target_machine_1"
      controller_type: "quel1"
      ip: "<TARGET_IP_1>"
    - name: "target_machine_2"
      controller_type: "quel1"
      ip: "<TARGET_IP_2>"
```

#### Create the Logs Directory

Create a `custom_exporters/quel1_metrics_exporter/logs/` directory and set directory permissions.

```bash
mkdir -p custom_exporters/quel1_metrics_exporter/logs
sudo chmod 755 custom_exporters/quel1_metrics_exporter/logs
```

#### Build and Start

```bash
cd custom_exporters/quel1_metrics_exporter
make build
make up
```

#### Verify Operation

```bash
curl http://localhost:9102/metrics
```

---

### 8.3 quel1-se-metrics-exporter (QuEL-1 SE Temperature & Actuator)

An exporter that collects temperature and actuator information from QuEL-1 SE controllers via the WSS (Wave Generation Subsystem) API.

#### Edit the Configuration File

Edit `custom_exporters/quel1_se_metrics_exporter/config/config.yaml`.

```yaml
exporter:
  port: 9103
  timezone: "Asia/Tokyo"

quel1se:
  timeout: 5 # WSS query timeout (seconds)
  targets: # List of monitoring targets (at least one required)
    - name: "quel1se-riken8-1"
      wss_ip: "<WSS_IP>"
      css_ip: "<CSS_IP>" # CSS IP (optional)
      boxtype: "quel1se-riken8" # Device type
```

#### Create the Logs Directory

Create a `custom_exporters/quel1_se_metrics_exporter/logs/` directory and set directory permissions.

```bash
mkdir -p custom_exporters/quel1_se_metrics_exporter/logs
sudo chmod 755 custom_exporters/quel1_se_metrics_exporter/logs
```

#### Build and Start

```bash
cd custom_exporters/quel1_se_metrics_exporter
make build
make up
```

#### Verify Operation

```bash
curl http://localhost:9103/metrics
```

---

## 9. Step 4 — Deploying Grafana & Loki

Set up Grafana (visualization & alerting) and Loki (log aggregation).

### 9.1 Configure the VictoriaMetrics Data Source

Open `grafana-and-loki/grafana/provisioning/datasources/datasource.yml` and set the VictoriaMetrics host IP.

```yaml
apiVersion: 1
datasources:
  - name: VictoriaMetricsCluster
    uid: vmcluster
    type: prometheus
    access: proxy
    # ↓ Change this to your environment's IP address
    url: http://<VICTORIAMETRICS_HOST_IP>:8428/select/0/prometheus
    isDefault: true
    jsonData:
      httpMethod: POST
  - name: Loki
    type: loki
    uid: loki
    access: proxy
    url: http://loki:3100
    isDefault: false
```

> **Tip**: If Grafana and VictoriaMetrics are running on the same host, you can use `http://host.docker.internal:8428/select/0/prometheus`.

### 9.2 Set the Admin Password (Optional)

```bash
# grafana-and-loki/.env
GRAFANA_USERNAME=admin
GRAFANA_PASSWORD=your_secure_password
TIMEZONE_GRAFANA_AND_LOKI=Asia/Tokyo
```

### 9.3 Start the Services

```bash
cd grafana-and-loki
make up
```

### 9.4 Verify Access

- **Grafana UI**: `http://localhost:3000` (default: `admin` / `admin`)
- **Loki API**:

  ```bash
  curl http://localhost:3100/ready
  ```

---

## 10. Step 5 — Deploying the API Server

A RESTful API server for reading metrics and managing metadata.

### 10.1 Edit the Configuration File

Edit `api_server/config/config.yaml`.

```yaml
server:
  timezone: "Asia/Tokyo" # Timezone for operation IDs and logs
  port: 8080 # API server listen port

victoria_metrics:
  # ↓ Set your VictoriaMetrics host IP
  url: "http://<VICTORIAMETRICS_HOST_IP>:8428"

operations:
  lock_timeout_hours: 1 # Lock timeout (hours)
```

> **Tip**: If the API server and VictoriaMetrics are on the same Docker network, you can use `http://vmauth-select:8428`.

### 10.2 Create the Logs Directory

Create a `api_server/logs/` directory and set directory permissions.

```bash
mkdir -p api_server/logs
sudo chmod 755 api_server/logs
```

### 10.3 Build and Start

```bash
cd api_server
make build-image
make up
```

### 10.4 Verify Operation

```bash
curl http://localhost:8080/metrics/names
```

---

## 11. Post-Deployment Verification

After deploying all components, use the following checklist to verify end-to-end operation.

```bash
# 1. Verify all targets show "UP" on the vmagent targets page
#    Open http://localhost:8429/targets in your browser

# 2. Query VictoriaMetrics directly to confirm data is being ingested
curl 'http://localhost:8428/select/0/prometheus/api/v1/query?query=up'

# 3. Verify the scrapedweek label is applied (if configured)
curl 'http://localhost:8428/select/0/prometheus/api/v1/query?query={job="virtual-machine"}'

# 4. Log in to Grafana and verify data appears on dashboards
#    Open http://localhost:3000 → Explore → Example query: up

# 5. Verify the API server responds
curl http://localhost:8080/metrics/names
```

---

## 12. How to Use Grafana

### 12.1 Login and Home Screen

1. Open `http://localhost:3000` in your browser.
2. Enter your username and password (default: `admin` / `admin`).
3. You will be prompted to change the password on first login (this can be skipped).

After logging in, you can access all features from the left sidebar.

| Menu                               | Function                                      |
| :--------------------------------- | :-------------------------------------------- |
| **Dashboards**                     | Browse and search dashboards                  |
| **Explore**                        | Run ad hoc metric queries                     |
| **Alerting**                       | Manage alert rules, contact points & policies |
| **Connections** > **Data sources** | View and add data sources                     |

### 12.2 Preset Dashboards

The following dashboards are available out of the box.

| Dashboard                 | File                          | Description                                                |
| :------------------------ | :---------------------------- | :--------------------------------------------------------- |
| **Computer Metrics**      | `node-exporter-full.json`     | Detailed host OS metrics: CPU, memory, disk, network, etc. |
| **Docker Containers**     | `docker_containers.json`      | Docker container CPU and memory usage                      |
| **Dilution Refrigerator** | `cryo_metrics_exporter.json`  | Dilution refrigerator temperature, pressure, and gas flow  |
| **Qubit Controller**      | `quel1_metrics_exporter.json` | QuEL controller ping availability status                   |
| **Analysis Dashboard**    | `analysis_dashboard.json`     | Analysis dashboard                                         |

### 12.3 Navigating Dashboards

#### Opening a Dashboard

1. Click **Dashboards** in the left sidebar.
2. The dashboard list appears. Click on the desired dashboard.

#### Changing the Time Range

Click the **time range selector** at the top right of the dashboard (e.g., `Last 6 hours`) to change it.

- **Quick select**: `Last 5 minutes`, `Last 1 hour`, `Last 24 hours`, etc.
- **Custom range**: Enter any dates in `From` and `To` in ISO 8601 format (e.g., `2025-12-19 11:00:00`), and click **Apply time range**.
- **Auto-refresh**: Select a refresh interval from the dropdown at the top right (`5s`, `10s`, `30s`, `1m`, etc.) for real-time updates

#### Panel Operations

The following operations are available on each panel (graph).

| Operation            | How                                                                                                                              |
| :------------------- | :------------------------------------------------------------------------------------------------------------------------------- |
| Full-screen view     | Click the panel menu (three vertical dots on the top right of the panel) → **View**                                              |
| Zoom into time range | Drag to select a range on the graph                                                                                              |
| Data details         | Hover over a data point on the graph to display a tooltip                                                                        |
| Filter by legend     | Click a legend label (below the graph) to show only that series. Press `Ctrl` or `Command` key and click others to add them back |

#### Filtering with Variables

If **template variables** (dropdowns) appear at the top of a dashboard, you can use them to filter the displayed content by hostname, job name, etc.

### 12.4 Running Ad Hoc Queries in Explore

Explore lets you run queries freely without creating a dashboard.

1. Click **Explore** in the left sidebar.
2. Verify that the data source at the top of the screen is set to `VictoriaMetricsCluster`.
3. Enter a PromQL query in the query input field.

**Example Queries**:

```promql
# Availability status of all targets
up

# Dilution refrigerator temperature (50K stage)
refrigerator_temperature{stage="plate_50k"}

# QuEL controller ping status
qubit_controller_ping_status_code

# Host CPU usage (1-minute average)
100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)

# Memory usage
(1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100
```

1. Click the **Run query** button (or press `Shift+Enter`) to execute the query.
2. Switch between the **Graph** / **Table** tabs to change the display format.

---

## 13. Creating Custom Alerts

### 13.1 Preset Alerts

The following alerts are configured out of the box (in `grafana-and-loki/grafana/provisioning/alerting/alert_rules.yaml`).

| Alert Name                | Monitoring Target     | Threshold     | Description                                         |
| :------------------------ | :-------------------- | :------------ | :-------------------------------------------------- |
| `alert_flux_Helium_flow`  | Helium flow rate      | > 81.5 mmol/s | Main circulation helium flow rate exceeds threshold |
| `alert_temperature_50K`   | 50K stage temperature | > 41 K        | 50K stage temperature exceeds threshold             |
| `alert_temperature_4K`    | 4K stage temperature  | > 5 K         | 4K stage temperature exceeds threshold              |
| `alert_temperature_Still` | Still temperature     | > 2 K         | Still plate temperature exceeds threshold           |
| `alert_temperature_MXC`   | MXC temperature       | > 1 K         | MXC plate temperature exceeds threshold             |

### 13.2 Creating Alerts from the Grafana UI

> **Note**: Alerts created from the Grafana UI will be **lost** when the Grafana container is recreated. For persistent alerts, use [13.3](#133-creating-alerts-via-provisioning-files-recommended).

1. Log in to Grafana.
2. Click **Alerting** → **Alert rules** in the left sidebar.
3. Click **New alert rule**.
4. Configure the following:

   **1. Enter alert rule name**
   - Example: `alert_temperature_custom`

   **2. Define query and alert condition**
   - **Data source**: Select `VictoriaMetricsCluster`
   - **Query**: Enter a PromQL query

     ```promql
     refrigerator_temperature{stage="plate_50k"}
     ```

   - **Alert condition**: Select a condition type (e.g., `IS ABOVE`, `IS BELOW`, etc.) and set the threshold value
     - Example: `IS ABOVE 45` (fire alert when temperature exceeds 45K)

   **3. Add folder and labels**
   - **Folder**: Folder to store the alert rule (e.g., `alert_rules`)

   **4. Set evaluation behavior**
   - **Evaluation group**: Select an existing evaluation group or create a new one along with its evaluation interval
     > **Note**: Evaluation groups created via provisioning files are NOT selectable from the UI.
   - **Pending period**: Grace period before firing (e.g., `1m`)
   - **Keep firing for**: Grace period before resolving (e.g., `5m`)

   **5. Configure notifications**
   - **Contact point**: Select a contact point configured in [13.4](#134-configuring-contact-points)
   - **Summary**: Alert summary text

     ```text
     50K stage temperature has exceeded the custom threshold.
     ```

5. Click **Save**.

### 13.3 Creating Alerts via Provisioning Files (Recommended)

Alerts defined in provisioning files are **persistent** and survive container recreation.

**File**: `grafana-and-loki/grafana/provisioning/alerting/alert_rules.yaml`

Below is an example of adding a new alert rule. Append it after the existing rule definitions.

```yaml
# ↓ Append after existing rules
- uid: custom_alert_001
  title: alert_temperature_custom
  condition: C
  data:
    # Query A: Fetch the metric
    - refId: A
      relativeTimeRange:
        from: 600 # Look back 10 minutes
        to: 0
      datasourceUid: vmcluster
      model:
        editorMode: code
        expr: round(refrigerator_temperature{stage="plate_50k"}, 0.01) # PromQL query
        instant: true
        intervalMs: 1000
        legendFormat: __auto
        maxDataPoints: 43200
        range: false
        refId: A
    # Condition C: Threshold evaluation
    - refId: C
      datasourceUid: __expr__
      model:
        conditions:
          - evaluator:
              params:
                - 45 # ← Alert threshold (fires when this value is exceeded)
              type: gt # gt = greater than
            operator:
              type: and
            query:
              params:
                - A
            reducer:
              params: []
              type: last
            type: query
        datasource:
          type: __expr__
          uid: __expr__
        expression: A
        intervalMs: 1000
        maxDataPoints: 43200
        refId: C
        type: threshold
  dashboardUid: adv8kqf # Associated dashboard UID
  panelId: 2 # Associated panel ID
  noDataState: KeepLast # Behavior when no data
  execErrState: Error # Behavior on error
  annotations:
    dashboardUid: adv8kqf
    panelId: "2"
    summary: "50K stage temperature has exceeded the custom threshold." # Alert summary displayed in notifications
    currentValue: "{{ $values.A.Value }}[K]"
    resolvedMessage: "{{ $labels.alertname }} has been resolved."
  isPaused: false
```

After saving the file, restart Grafana to apply the changes.

```bash
cd grafana-and-loki
docker compose restart grafana
```

> **Tip**: Alerts defined via provisioning files appear as **read-only** in the Grafana UI. To modify them, edit the file directly.

### 13.4 Configuring Contact Points

Configure where alert notifications are sent when an alert fires.

**File**: `grafana-and-loki/grafana/provisioning/alerting/alert_resources.yaml`

#### Slack Notification Example

```yaml
contactPoints:
  - orgId: 1
    name: slack
    receivers:
      - uid: slack_uid
        type: slack
        settings:
          # Replace with your Bot User OAuth Token
          token: "xoxb-YOUR-BOT-TOKEN"
          # Notification channel (channel name or channel ID)
          recipient: "#your-alert-channel"
          title: "[{{ .Status | toUpper }}] {{ .GroupLabels.alertname }}"
          text: |
            {{ if gt (len .Alerts.Firing) 0 }}
            {{ range .Alerts.Firing }}
            Summary: {{ .Annotations.summary }}
            Last value: {{ .Annotations.currentValue }}
            Fired at: {{ .StartsAt.Format "2006-01-02 15:04:05" }}
            Panel URL: http://localhost:3000/d/{{ .Annotations.dashboardUid }}?viewPanel={{ .Annotations.panelId}}
            {{ end }}
            {{ end }}
            {{ if gt (len .Alerts.Resolved) 0 }}
            {{ range .Alerts.Resolved }}
            Summary: {{ .Annotations.resolvedMessage }}
            Resolved at: {{ .EndsAt.Format "2006-01-02 15:04:05" }}
            {{ end }}
            {{ end }}
        disableResolveMessage: false
```

> **Note**: If Grafana is accessed from outside the network, replace `localhost` in the Panel URL with the actual IP address of the Grafana host. Also update `GF_SERVER_DOMAIN` and `GF_SERVER_ROOT_URL` in `compose.yaml` accordingly.

### 13.5 Configuring Notification Policies

Notification policies define which alerts are sent to which contact points.

**File**: `grafana-and-loki/grafana/provisioning/alerting/alert_resources.yaml` (same file as the contact points)

```yaml
policies:
  - orgId: 1
    receiver: slack # ← Specify the contact point name defined above
    group_by:
      - grafana_folder
      - alertname
    group_wait: 0s # Wait time before grouping
    group_interval: 1s # Interval between notifications in the same group
    repeat_interval: 4h # Re-notification interval for the same alert
```

After saving the file, restart Grafana.

```bash
cd grafana-and-loki
docker compose restart grafana
```

---

## 14. How to Use the API Server

The API server provides a REST API for reading metrics stored in VictoriaMetrics and modifying metadata.

> **Important**: Metadata modification operations (PATCH / DELETE) are executed **asynchronously**. When a request is accepted, an `operation_id` is returned, which can be used to track the operation's progress. Only **one** operation can run at a time.

### 14.1 API Endpoint Reference

#### Read Operations (GET)

| Method | Path                                               | Description                                   |
| :----- | :------------------------------------------------- | :-------------------------------------------- |
| GET    | `/metrics/names`                                   | List metric names (with pagination)           |
| GET    | `/metrics/{metric_name}/labels`                    | List label keys for a metric                  |
| GET    | `/metrics/{metric_name}/labels/{label_key}/values` | List values for a label key (with pagination) |
| GET    | `/metrics/{metric_name}/series/data`               | Retrieve time series data                     |
| GET    | `/meta/status?operation_id=<ID>`                   | Check operation status                        |

#### Modification Operations (PATCH / DELETE) — Asynchronous, Returns HTTP 202

| Method | Path                                 | Description             |
| :----- | :----------------------------------- | :---------------------- |
| PATCH  | `/meta/add`                          | Add a label             |
| PATCH  | `/meta/modify/key`                   | Rename a label key      |
| PATCH  | `/meta/modify/value`                 | Change a label value    |
| PATCH  | `/meta/delete`                       | Delete a label          |
| DELETE | `/metrics/{metric_name}/series/data` | Delete time series data |

### 14.2 Retrieving Metric Names

```bash
# Get all metric names (default: offset=0, limit=100)
curl http://localhost:8080/metrics/names

# With pagination
curl 'http://localhost:8080/metrics/names?offset=0&limit=50'
```

**Example Response**:

```json
{
  "status": "success",
  "data": {
    "metrics": [
      "refrigerator_temperature",
      "refrigerator_pressure",
      "node_cpu_seconds_total"
    ],
    "page": { "offset": 0, "limit": 100, "total": 3 }
  }
}
```

### 14.3 Retrieving Label Keys and Label Values

```bash
# List label keys for a metric
curl http://localhost:8080/metrics/refrigerator_temperature/labels

# List values for a label key
curl 'http://localhost:8080/metrics/refrigerator_temperature/labels/stage/values'

# With pagination
curl 'http://localhost:8080/metrics/refrigerator_temperature/labels/stage/values?offset=0&limit=50'
```

### 14.4 Retrieving Time Series Data

```bash
# Retrieve time series data for a specific metric (specify time range in ISO 8601 format)
curl 'http://localhost:8080/metrics/refrigerator_temperature/series/data?start=2025-12-19T00:00:00Z&end=2025-12-19T23:59:59Z&selector[stage]=plate_50k'
```

- `start`, `end`: Datetime in ISO 8601 format (**required**)
- `selector[<label_key>]=<label_value>`: Filter by label (URL query parameter, exact match)

### 14.5 Modifying Metadata (Adding Labels)

Add a new label to existing time series.

```bash
curl -X PATCH http://localhost:8080/meta/add \
  -H "Content-Type: application/json" \
  -d '{
    "metric_name": "refrigerator_temperature",
    "new_label_key": "location",
    "default_value": "building_A",
    "selector": {
      "match": [
        { "key": "device_name", "value": "fridge_01", "regex": false }
      ]
    }
  }'
```

**Response (202 Accepted)**:

```json
{
  "status": "success",
  "data": {
    "operation_id": "20251219T150000_1",
    "summary": { "message": "Add label operation scheduled successfully" }
  }
}
```

### 14.6 Modifying Metadata (Renaming Label Keys)

Rename an existing label key.

```bash
curl -X PATCH http://localhost:8080/meta/modify/key \
  -H "Content-Type: application/json" \
  -d '{
    "metric_name": "refrigerator_temperature",
    "range": {
      "start": "2025-12-01T00:00:00+09:00",
      "end": "2025-12-31T23:59:59+09:00"
    },
    "selector": {
      "match": [
        { "key": "device_name", "value": "fridge_01", "regex": false }
      ]
    },
    "from_key": "old_label",
    "to_key": "new_label"
  }'
```

- `range.start` / `range.end`: Target time range (optional; defaults to all time if omitted)

### 14.7 Modifying Metadata (Changing Label Values)

Change the value of an existing label.

```bash
curl -X PATCH http://localhost:8080/meta/modify/value \
  -H "Content-Type: application/json" \
  -d '{
    "metric_name": "refrigerator_temperature",
    "range": {
      "start": "2025-12-01T00:00:00+09:00",
      "end": "2025-12-31T23:59:59+09:00"
    },
    "selector": {
      "match": [
        { "key": "device_name", "value": "fridge_01", "regex": false }
      ]
    },
    "key": "device_name",
    "from_value": "fridge_01",
    "to_value": "fridge_01_updated"
  }'
```

### 14.8 Deleting Metadata (Removing Labels)

Remove a label from specified time series.

```bash
curl -X PATCH http://localhost:8080/meta/delete \
  -H "Content-Type: application/json" \
  -d '{
    "metric_name": "refrigerator_temperature",
    "label_keys": ["unwanted_label"],
    "selector": {
      "match": [
        { "key": "device_name", "value": "fridge_01", "regex": false }
      ]
    }
  }'
```

### 14.9 Deleting Time Series Data

Delete time series data itself. The `selector` must **uniquely identify** the target series via labels.

```bash
curl -X DELETE http://localhost:8080/metrics/refrigerator_temperature/series/data \
  -H "Content-Type: application/json" \
  -d '{
    "match": [
      { "key": "device_name", "value": "fridge_01", "regex": false },
      { "key": "stage", "value": "plate_50k", "regex": false }
    ]
  }'
```

### 14.10 Checking Operation Status

Check the progress of an asynchronous operation (PATCH / DELETE).

```bash
curl 'http://localhost:8080/meta/status?operation_id=20251219T150000_1'
```

**Example Response**:

```json
{
  "status": "success",
  "data": {
    "process_status": "in_progress",
    "messages": "Operation is currently ingesting"
  }
}
```

`process_status` values:

| Value         | Meaning                                                                                          |
| :------------ | :----------------------------------------------------------------------------------------------- |
| `in_progress` | Running (progresses through steps: extracting → transforming → ingesting → deleting → finishing) |
| `completed`   | Completed successfully                                                                           |
| `failed`      | Failed (error message is included in `messages`)                                                 |

> **Note**: Metadata modification operations are executed as a pseudo-transaction. If a failure occurs during or after the `ingesting` step, partial metrics may remain and need to be manually deleted. See [`docs/api-server/api-server.md`](docs/api-server/api-server.md) for details.

#### Selector Specification

The `selector` used in API request bodies follows this format:

```json
{
  "match": [
    { "key": "label_name", "value": "exact_value", "regex": false },
    { "key": "label_name", "value": "regex_pattern.*", "regex": true }
  ]
}
```

- `key`: Label key name
- `value`: Value to match (`regex: true` uses a regular expression pattern)
- `regex`: `false` = exact match, `true` = regex match
- Multiple `match` entries are combined with AND logic

---

## 15. Operational Procedures

### 15.1 Adding Scrape Targets

To add a new exporter or service as a monitoring target:

1. Add a new job to `victoriametrics-cluster/vmagent/prometheus.yml`.

   ```yaml
   scrape_configs:
     # ... existing jobs ...
     - job_name: "new-exporter"
       scrape_interval: 60s
       static_configs:
         - targets: ["<EXPORTER_IP>:<PORT>"]
   ```

2. `vmagent` automatically reloads its configuration every 30 seconds (`-promscrape.configCheckInterval=30s`), so no restart is required.

3. Verify the new target shows as `UP` at `http://localhost:8429/targets`.

4. If you want to apply the `scrapedweek` label, update `TARGET_JOBS_REGEX` in `victoriametrics-cluster/weekly-relabeling-cron/relabel_weekly.sh`.

   ```bash
   export TARGET_JOBS_REGEX='virtual-machine|new-exporter'
   ```

   After updating, rebuild the cron container.

   ```bash
   cd victoriametrics-cluster
   docker compose up -d --build weekly-relabeling-cron
   ```

### 15.2 Adding a New Custom Exporter

1. Create a new directory under `custom_exporters/` with the same structure as existing exporters.

   ```tree
   custom_exporters/new_exporter/
   ├── compose.yaml
   ├── Dockerfile
   ├── Makefile
   ├── pyproject.toml
   ├── config/
   │   ├── config.yaml
   │   └── logging.yaml
   ├── src/
   └── tests/
   ```

2. Implement an exporter that exposes metrics via the `/metrics` endpoint using `prometheus_client`.

3. Build and deploy.

   ```bash
   cd custom_exporters/new_exporter
   make build
   make up
   ```

4. Add it as a scrape target following the steps in [15.1](#151-adding-scrape-targets).

### 15.3 Scaling vmagent

If you need to handle an increasing number of targets or require scraping redundancy, refer to `docs/vmagent-scaling.md`.

- **Sharded**: Distribute targets across multiple `vmagent` instances. No deduplication required.
- **HA (Replication)**: Scrape the same targets from multiple instances. Requires VictoriaMetrics-side deduplication (`-dedup.minScrapeInterval`).

### 15.4 Timezone Configuration

Below is a summary of timezone settings for each component. It is recommended to keep all components in the same timezone.

| Component                 | Configuration Method                                           | Default |
| :------------------------ | :------------------------------------------------------------- | :------ |
| VictoriaMetrics Cluster   | `.env` → `TIMEZONE_VM_CLUSTER`                                 | UTC     |
| Node Exporter & cAdvisor  | `.env` → `TIMEZONE`                                            | UTC     |
| Grafana & Loki            | `.env` → `TIMEZONE_GRAFANA_AND_LOKI`                           | UTC     |
| API Server                | `config.yaml` → `server.timezone` or env `API_SERVER_TIMEZONE` | UTC     |
| cryo-metrics-exporter     | `config.yaml` → `exporter.timezone` or env `EXPORTER_TIMEZONE` | UTC     |
| quel1-metrics-exporter    | `config.yaml` → `exporter.timezone` or env `SERVER_TIMEZONE`   | UTC     |
| quel1-se-metrics-exporter | `config.yaml` → `exporter.timezone` or env `EXPORTER_TIMEZONE` | UTC     |

> **Note**: VictoriaMetrics internally stores all data in UTC. Timezone settings primarily affect log timestamps and operation ID formatting.

### 15.5 Backup and Data Persistence

Data is persisted in Docker named volumes.

| Volume            | Component       | Contents                     |
| :---------------- | :-------------- | :--------------------------- |
| `vmstorage1-data` | VictoriaMetrics | Time series data (storage 1) |
| `vmstorage2-data` | VictoriaMetrics | Time series data (storage 2) |
| `vmagent-data`    | vmagent         | Remote write buffer          |
| `grafana-data`    | Grafana         | Dashboards, settings         |
| `loki-data`       | Loki            | Log data                     |

To completely remove data along with the services, use the `-v` option.

```bash
# Example: Completely remove VictoriaMetrics data
cd victoriametrics-cluster
docker compose down -v
```

> **Warning**: `docker compose down -v` **irreversibly deletes** all data in the volumes.

---

## 16. Developer Information

### 16.1 Local Development Environment

Each Python component manages its dependencies with [uv](https://docs.astral.sh/uv/).

```bash
# Example: Install API server dependencies
cd api_server
uv sync

# Example: Install cryo-metrics-exporter dependencies
cd custom_exporters/cryo_metrics_exporter
uv sync
```

### 16.2 Running Tests

Tests for each component are run with `pytest`.

```bash
# API Server
cd api_server
make test

# Custom Exporters
cd custom_exporters/cryo_metrics_exporter
make test

cd custom_exporters/quel1_metrics_exporter
make test

cd custom_exporters/quel1_se_metrics_exporter
make test
```

### 16.3 Linting and Formatting

All Python components use [Ruff](https://docs.astral.sh/ruff/) (formatter/linter) and [mypy](https://mypy-lang.org/) (type checker).

```bash
# Run from each component's directory
make fmt-lint
```

### 16.4 API Schema Generation

The API server schema is auto-generated from the OpenAPI specification.

```bash
cd api_server
make generate-api-server-schema
```

---

## 17. Troubleshooting

### Metrics Are Not Being Collected

1. Check target status at `http://localhost:8429/targets`.
2. Access the exporter directly to verify it is responding.

   ```bash
   curl http://<EXPORTER_IP>:<PORT>/metrics
   ```

3. Check `vmagent` logs.

   ```bash
   docker logs vmagent
   ```

4. Verify the IP addresses and ports in `prometheus.yml` are correct.

### Grafana Shows No Data

1. Verify the URL in `datasource.yml` is reachable from the Grafana container.

   ```bash
   docker exec grafana curl -s http://<VM_HOST>:8428/select/0/prometheus/api/v1/status/buildinfo
   ```

2. Restart Grafana: `docker compose restart grafana`
3. In the Explore view, query `up` to check whether the data source connection itself is working.

### scrapedweek Label Is Not Applied

1. Check the generated relabeling file.

   ```bash
   cat victoriametrics-cluster/weekly-relabeling-cron/relabel_weekly.yml
   ```

2. Verify that `TARGET_JOBS_REGEX` includes the job name.
3. Rebuild the cron container: `docker compose up -d --build weekly-relabeling-cron`

### API Server Operations Are Not Progressing

1. Check the operation status.

   ```bash
   curl 'http://localhost:8080/meta/status?operation_id=<OPERATION_ID>'
   ```

2. Check the `api_server/data/.lock` file. The lock timeout is configured via `operations.lock_timeout_hours` (default: 1 hour).
3. Check the logs in `api_server/logs/`.

### Custom Exporter Errors

- Check the logs in each exporter's `logs/` directory.
- cryo-metrics-exporter: Verify the `SMB_PASSWORD` environment variable is set.
- quel1-se-metrics-exporter: Verify network connectivity to the WSS endpoint.

### Grafana Alerts Are Not Firing

1. Check the status of the target rule (`OK`, `Pending`, `Firing`, `Error`) under **Alerting** → **Alert rules**.
2. If `Error`, verify the rule's query is correct and the data source is accessible.
3. If `Pending`, wait for the `Pending period` to elapse.
4. Test the contact point: Go to **Alerting** → **Contact points** → click the **Test** button to verify notifications are delivered.

---

## 18. Port Reference

| Service                   | Port | Description                                  |
| :------------------------ | :--- | :------------------------------------------- |
| Grafana                   | 3000 | Visualization & alerting UI                  |
| Loki                      | 3100 | Log aggregation API                          |
| vmauth-insert             | 8427 | VictoriaMetrics data ingestion load balancer |
| vmauth-select             | 8428 | VictoriaMetrics query load balancer          |
| vmagent                   | 8429 | Metrics scraper management UI                |
| vmselect-1                | 8481 | Direct access to vmselect instance 1         |
| API Server                | 8080 | Metrics & metadata management REST API       |
| cAdvisor                  | 8088 | Container metrics                            |
| Node Exporter             | 9100 | Host OS metrics                              |
| cryo-metrics-exporter     | 9101 | Dilution refrigerator metrics                |
| quel1-metrics-exporter    | 9102 | QuEL controller ping status metrics          |
| quel1-se-metrics-exporter | 9103 | QuEL-1 SE temperature & actuator metrics     |
