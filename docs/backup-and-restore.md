# Runbook — Backup and Restore: VictoriaMetrics / Grafana / Grafana Loki

This document describes how to back up and restore a monitoring platform deployed with the [oqtopus-device-monitoring](https://github.com/oqtopus-team/oqtopus-device-monitoring) configuration.

---

## 0. Prerequisites

### 0.1 Assumed Environment

The VictoriaMetrics Cluster, Grafana, and Grafana Loki containers run via Docker Compose.

### 0.2 Tools and Images

Backup and restore use `vmbackup`, `vmrestore`, and `LogCLI`, each run in a temporary Docker container from the images below.

| Tool                                                                     | Docker Image                         | Purpose                                  |
| :----------------------------------------------------------------------- | :----------------------------------- | :--------------------------------------- |
| [vmbackup](https://docs.victoriametrics.com/victoriametrics/vmbackup/)   | `victoriametrics/vmbackup:v1.145.0`  | Back up VictoriaMetrics data             |
| [vmrestore](https://docs.victoriametrics.com/victoriametrics/vmrestore/) | `victoriametrics/vmrestore:v1.145.0` | Restore VictoriaMetrics data             |
| [LogCLI](https://grafana.com/docs/loki/latest/query/logcli/)             | `grafana/logcli:3.5.0`               | Back up Grafana Loki alert state history |

### 0.3 Environment-Specific Values

The backup and restore scripts below reference values that differ per environment. Look up your environment's actual values using the table, then apply them to each script's configuration section ([§2](#2-backing-up-victoriametrics) onward).

| Item                             | Example Value                                                        | How to Verify                                                                                                           |
| :------------------------------- | :------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------- |
| Docker Compose network name      | `victoriametrics-cluster_monitoring` / `grafana-and-loki_monitoring` | `docker network ls`                                                                                                     |
| Docker Compose project directory | `/home/ubuntu/victoriametrics-cluster`                               | `docker inspect <container> --format '{{index .Config.Labels "com.docker.compose.project.working_dir"}}'`               |
| vmstorage container name         | `vmstorage-1`                                                        | `docker ps --format '{{.Names}}'`                                                                                       |
| vmstorage data volume name       | `victoriametrics-cluster_vmstorage1-data`                            | `docker inspect vmstorage-1 --format '{{json .Mounts}}'`                                                                |
| vmstorage Snapshot API port      | `8482`                                                               | the ports column of `docker ps` or [`compose.yaml`](../victoriametrics-cluster/compose.yaml) of VictoriaMetrics Cluster |
| Grafana URL / credentials        | `http://localhost:3000` / `admin:admin`                              | [`compose.yaml`](../grafana-and-loki/compose.yaml) of Grafana                                                           |
| Grafana Loki URL                 | `http://loki:3100`                                                   | [`compose.yaml`](../grafana-and-loki/compose.yaml) of Grafana Loki                                                      |

---

## 1. Backup Targets and Backup Policy

### 1.1 VictoriaMetrics Time-Series Data

#### Backup Policy

- The `vmstorage` startup option `-retentionPeriod=10y` makes `vmstorage` automatically delete time-series data older than 10 years.
- **A daily incremental backup** keeps **the past 7 generations** of snapshots. Older generations are rotated out automatically.
- **A weekly full backup** runs every Sunday and keeps **only the latest generation** snapshot. The previous weekly backup is deleted once the new one completes.

#### Backup Targets

`vmbackup` snapshots the entire `vmstorage` data directory. Each backed-up component and its role in the running `vmstorage` are listed below.

| Backup Target           | Path in Backup               | Description                                                                                                                       |
| :---------------------- | :--------------------------- | :-------------------------------------------------------------------------------------------------------------------------------- |
| Time-series data        | `<vmstorage_node>/data/`     | Metric values (`values.bin`) and timestamps (`timestamps.bin`), stored as parts (data blocks) in monthly partitions (`YYYY_MM/`). |
| Inverted index          | `<vmstorage_node>/indexdb/`  | Mapping from label sets to time-series IDs (`items.bin`), enabling search by metric name or label.                                |
| Metadata                | `<vmstorage_node>/metadata/` | Internal settings such as the composite index's minimum timestamp (`minTimestampForCompositeIndex`).                              |
| Backup management files | `<vmstorage_node>/*.ignore`  | Internal files used by `vmbackup`/`vmrestore` for generation management and consistency checks.                                   |

#### Backup Data Directory Structure

The directory structure after a backup is shown below. Each `vmstorage` node is backed up to **its own directory**, **split by month**.

```tree
<BACKUP_BASE_DIR>/
├── vmstorage-1/                            # Backup of vmstorage-1
│   ├── data/                               # Time-series data
│   │   └── small/
│   │       └── 2026_06/                    # Monthly partitioned backup data
│   │           ├── 18B7E3BF10B02B79/
│   │           │   ├── values.bin
│   │           │   ├── timestamps.bin
│   │           │   ├── index.bin
│   │           │   ├── metaindex.bin
│   │           │   └── metadata.json
│   │           ├── 18B7E3BF10B02B7A/
│   │           │   └── ...
│   │           └── parts.json
│   ├── indexdb/                            # Inverted index
│   │   ├── 18B7E3BCF6D56A60/
│   │   │   └── parts.json
│   │   └── 18B7E3BCF6D56A61/
│   │       ├── 18B7E3BCF7E23421/
│   │       │   ├── items.bin
│   │       │   ├── lens.bin
│   │       │   ├── index.bin
│   │       │   ├── metaindex.bin
│   │       │   └── metadata.json
│   │       ├── 18B7E3BCF7E23422/
│   │       │   └── ...
│   │       └── parts.json
│   ├── metadata/                           # Metadata
│   │   └── minTimestampForCompositeIndex
│   ├── backup_metadata.ignore              # vmbackup generation management / consistency information
│   └── backup_complete.ignore              # Backup completion marker
└── vmstorage-2/                            # Backup of vmstorage-2 (managed independently in a separate directory)
    └── ...
```

### 1.2 Grafana

#### Backup Policy

- Grafana configuration (dashboards, data sources, alerts, etc.) keeps **only the latest generation** snapshot.
- The previous backup is deleted automatically once a new one completes.

#### Backup Targets

Apart from the SQLite database, each backup target below is exported as a JSON file.

| Backup Target          | Path in Backup                        | Description                                                                                             |
| :--------------------- | :------------------------------------ | :------------------------------------------------------------------------------------------------------ |
| SQLite database        | `grafana.db`                          | Master database holding all of Grafana's internal state: users, permissions, settings, and preferences. |
| Folder definitions     | `folders.json`                        | List and hierarchy of folders that organize dashboards.                                                 |
| Dashboards             | `dashboards/<uid>.json`               | Full definition of each dashboard (panels, queries, variables, annotations), one JSON file per UID.     |
| Data sources           | `datasources/all.json`                | Connection settings for each data source (type, URL, credentials, default flag).                        |
| Alert rules            | `alerting/rules.json`                 | Alert evaluation conditions, intervals, target folders, and notification groups.                        |
| Contact points         | `alerting/contact-points.json`        | Destination channels for alert notifications (Email, Slack, Webhook, etc.).                             |
| Notification policies  | `alerting/notification-policies.json` | Alert routing tree, grouping keys, and repeat intervals.                                                |
| Mute timings           | `alerting/mute-timings.json`          | Schedules of days and time ranges that suppress notifications.                                          |
| Notification templates | `alerting/templates.json`             | Custom Go templates for alert notification messages.                                                    |

#### Backup Data Directory Structure

The directory structure after a backup is shown below. Each backup is stored in its own timestamped directory, and `latest` points to the most recent one.

```text
<EXPORT_DIR>/
├── 20260615_020000/                    # Timestamped backup directory
│   ├── grafana.db                      # SQLite database
│   ├── folders.json                    # Folder list
│   ├── dashboards/
│   │   ├── <uid-1>.json                # Dashboard
│   │   ├── <uid-2>.json
│   │   └── ...
│   ├── datasources/
│   │   └── all.json                    # All data sources
│   └── alerting/
│       ├── rules.json                  # Alert rules
│       ├── contact-points.json         # Contact points
│       ├── notification-policies.json  # Notification policies
│       ├── mute-timings.json           # Mute timings
│       └── templates.json              # Notification templates
└── latest -> 20260615_020000           # Symbolic link to the latest backup
```

### 1.3 Grafana Loki

#### Backup Policy

- A daily backup saves time-series data **going back 10 years from the present**.
- The backup keeps **only the latest generation** snapshot.

#### Backup Targets

The backup target is the alert state history exported from Grafana Loki, as listed below.

| Backup Target       | Path in Backup                   | Description                                                                       |
| :------------------ | :------------------------------- | :-------------------------------------------------------------------------------- |
| Alert state history | `alert_history_<YYYYMMDD>.jsonl` | Alert state-transition log from Grafana Unified Alerting, stored in Grafana Loki. |

#### Backup Data Directory Structure

The directory structure after a backup is shown below. The history is exported as a single JSONL file named with the run date.

```text
<OUTPUT_DIR>/
└── alert_history_20260615.jsonl   # JSONL backup dated with the run date
```

---

## 2. Backing Up VictoriaMetrics

`./backup_and_restore/backup-vmstorage.sh` backs up each `vmstorage` node to a separate directory and deletes generations beyond the retention period. The script runs `vmbackup` in a temporary Docker container. See [§1.1](#11-victoriametrics-time-series-data) for the backup targets.

### 2.1 Configure the Script (backup-vmstorage.sh)

Edit the following part of `backup-vmstorage.sh` to match your environment.

> **Warning**: Always back up each `vmstorage` node to a separate directory. Mixing data from different nodes leads to data corruption.

```bash
# --- Edit these variables for your environment ---
COMPOSE_NETWORK="victoriametrics-cluster_monitoring"                # Docker Compose network name
VMSTORAGE_1_VOLUME_NAME="victoriametrics-cluster_vmstorage1-data"   # Volume name of vmstorage-1
VMSTORAGE_1_PORT="8482"                                             # Port number of vmstorage-1
```

Place the edited script in a suitable directory (e.g., `/opt/scripts/`) and grant execute permission.

```bash
sudo cp backup-vmstorage.sh /opt/scripts/
sudo chown root:root /opt/scripts/backup-vmstorage.sh
sudo chmod 750 /opt/scripts/backup-vmstorage.sh
```

### 2.2 Create a Backup Manually

When running the script manually, specify the following variables to match your environment.

| Variable (Positional Argument)    | Default                 | Description                                              |
| :-------------------------------- | :---------------------- | :------------------------------------------------------- |
| `EXPORT_DIR` (`$1`, **required**) | —                       | Base directory under which the backup folder is created. |
| `RETENTION_COUNT` (`$2`)          | `7`                     | Number of backup generations to retain.                  |

For example, running the following command creates a backup.

```bash
# Example
sudo /opt/scripts/backup-vmstorage.sh /home/ubuntu/backups/vmstorage_1 7
```

---

## 3. Backing Up Grafana

`./backup_and_restore/backup-grafana.sh` copies `grafana.db` and exports JSON via the API into one timestamped directory, then deletes generations beyond the retention period. See [§1.2](#12-grafana) for the backup targets.

### 3.1 Configure the Script (backup-grafana.sh)

Place the edited script in a suitable directory (e.g., `/opt/scripts/`) and grant execute permission.

```bash
sudo cp backup-grafana.sh /opt/scripts/
sudo chown root:root /opt/scripts/backup-grafana.sh
sudo chmod 750 /opt/scripts/backup-grafana.sh
```

### 3.2 Create a Backup Manually

When running the script manually, specify the following variables to match your environment.

| Variable (Positional Argument)    | Default                 | Description                                              |
| :-------------------------------- | :---------------------- | :------------------------------------------------------- |
| `EXPORT_DIR` (`$1`, **required**) | —                       | Base directory under which the backup folder is created. |
| `GRAFANA_URL` (`$2`)              | `http://localhost:3000` | Grafana URL.                                             |
| `AUTH` (`$3`)                     | `admin:admin`           | Basic authentication credentials for the Grafana API.    |
| `RETENTION_COUNT` (`$4`)          | `1`                     | Number of backup generations to retain.                  |

For example, running the following command creates a backup.

```bash
# Example
sudo /opt/scripts/backup-grafana.sh /home/ubuntu/backups/grafana http://localhost:3000 admin:admin 1
```

---

## 4. Backing Up Grafana Loki

`./backup_and_restore/backup-grafana-loki.sh` runs `LogCLI` in a temporary Docker container and exports Grafana Loki's alert state history in JSONL format. See [§1.3](#13-grafana-loki) for the backup targets.

### 4.1 Configure the Script (backup-grafana-loki.sh)

Edit the following part of `backup-grafana-loki.sh` to match your environment.

```bash
COMPOSE_NETWORK="grafana-and-loki_monitoring"       # Docker Compose network name
LOKI_URL="http://loki:3100"                         # Grafana Loki URL in the Docker Compose network
OUTPUT_FILE="alert_history_$(date +%Y%m%d).jsonl"   # Output file name
```

Place the edited script in a suitable directory (e.g., `/opt/scripts/`) and grant execute permission.

```bash
sudo cp backup-grafana-loki.sh /opt/scripts/
sudo chown root:root /opt/scripts/backup-grafana-loki.sh
sudo chmod 750 /opt/scripts/backup-grafana-loki.sh
```

### 4.2 Create a Backup Manually

For example, running the following command creates a backup.

```bash
# Example
sudo /opt/scripts/backup-grafana-loki.sh
```

---

## 5. Scheduling with cron

Use cron to schedule backups of VictoriaMetrics, Grafana, and Grafana Loki per the policy in Section 1.

### 5.1 Create the cron File

Create the cron file `/etc/cron.d/odm-backup` as shown below. Adjust the script paths, run times, and other values for your environment.

```cron
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

# VictoriaMetrics daily backup - every day at 02:00 (keep 7 generations)
0 2 * * *  root  /opt/scripts/backup-vmstorage.sh /home/ubuntu/backups/vmstorage_1/daily 7  >> /var/log/vm-backup.log 2>&1

# VictoriaMetrics weekly backup - every Sunday at 03:00 (keep 1 generation)
0 3 * * 0  root  /opt/scripts/backup-vmstorage.sh /home/ubuntu/backups/vmstorage_1/weekly 1 >> /var/log/vm-backup.log 2>&1

# Grafana backup - daily at 02:30 (keep 1 generation)
30 2 * * * root  /opt/scripts/backup-grafana.sh /home/ubuntu/backups/grafana http://localhost:3000 admin:admin 1 >> /var/log/grafana-backup.log 2>&1

# Grafana Loki backup - daily at 02:30 (keep 1 generation)
30 2 * * * root  /opt/scripts/backup-grafana-loki.sh >> /var/log/grafana-backup.log 2>&1
```

After creating it, grant the appropriate permission.

```bash
sudo chmod 644 /etc/cron.d/odm-backup
```

### 5.2 Verify Operation

Run the following command and confirm the jobs configured above are listed.

```bash
crontab -l
```

---

## 6. Restoring VictoriaMetrics

`./backup_and_restore/restore-vmstorage.sh` runs `vmrestore` in a temporary Docker container to restore each `vmstorage` node's backup data.

> **Note**: Because `vmstorage` nodes use a shared-nothing architecture, each node can be restored independently without affecting the others. ([docs.victoriametrics.com](https://docs.victoriametrics.com/victoriametrics/cluster-victoriametrics/))

### 6.1 Configure the Script (restore-vmstorage.sh)

Edit the configuration in `restore-vmstorage.sh` to match your environment.

> **Warning**: Restore each `vmstorage` node from the directory that corresponds to the one configured in [§2.1](#21-configure-the-script-backup-vmstoragesh). Mixing data from different nodes leads to data corruption.

```bash
# --- Edit these variables for your environment ---
DAILY_BACKUP_DIR="/home/ubuntu/backups/vmstorage/daily"             # Directory for daily backup of vmstorage-1
WEEKLY_BACKUP_DIR="/home/ubuntu/backups/vmstorage/weekly"           # Directory for weekly backup of vmstorage-1
VMSTORAGE_1_VOLUME_NAME="victoriametrics-cluster_vmstorage1-data"   # Volume name of vmstorage-1
VMSTORAGE_1_PORT="8482"                                             # Port number of vmstorage-1
```

Place the edited script in a suitable directory (e.g., `/opt/scripts/`) and grant execute permission.

```bash
sudo cp restore-vmstorage.sh /opt/scripts/
sudo chown root:root /opt/scripts/restore-vmstorage.sh
sudo chmod 750 /opt/scripts/restore-vmstorage.sh
```

### 6.2 Run the Restore Manually

Running the following command restores the target backup files.

```bash
sudo /opt/scripts/restore-vmstorage.sh
```

---

## 7. Restoring Grafana

Use `./backup_and_restore/restore-grafana.sh` to restore a Grafana backup. It restores `grafana.db`, then overwrites each JSON backup file via the API in dependency order.

### 7.1 Configure the Script (restore-grafana.sh)

Edit the configuration in `restore-grafana.sh` to match your environment.

Place the edited script in a suitable directory (e.g., `/opt/scripts/`) and grant execute permission.

```bash
sudo cp restore-grafana.sh /opt/scripts/
sudo chown root:root /opt/scripts/restore-grafana.sh
sudo chmod 750 /opt/scripts/restore-grafana.sh
```

### 7.2 Stop the Grafana Container

```bash
cd grafana-and-loki
docker compose down grafana
```

### 7.3 Run the Restore Manually

`restore-grafana.sh` restores `grafana.db` and loads all resources via the API in one run. Specify the positional arguments for your environment.

| Variable (Positional Argument)    | Default                 | Description                                           |
| :-------------------------------- | :---------------------- | :---------------------------------------------------- |
| `BACKUP_DIR` (`$1`, **required**) | —                       | The timestamped backup directory to restore from.     |
| `GRAFANA_URL` (`$2`)              | `http://localhost:3000` | Grafana HTTP URL.                                     |
| `AUTH` (`$3`)                     | `admin:admin`           | Basic authentication credentials for the Grafana API. |

For example, running the following command restores the backup data.

```bash
sudo /opt/scripts/restore-grafana.sh /home/ubuntu/backups/grafana/20260615_020000 http://localhost:3000 admin:admin
```

### 7.4 Start the Grafana Container

After confirming the restore succeeded, restart the container.

```bash
cd grafana-and-loki
docker compose up -d grafana
```

---

## 8. Restoring Grafana Loki

`./backup_and_restore/restore-grafana-loki.sh` converts the JSONL backup into the Grafana Loki push API format and writes it back to the running Grafana Loki.

### 8.1 Configure the Script (restore-grafana-loki.sh)

Edit the following part of `restore-grafana-loki.sh` to match your environment.

```bash
LOKI_URL="http://localhost:3100"                   # Grafana Loki URL reachable from the host
INPUT_FILE="alert_history_$(date +%Y%m%d).jsonl"   # Input JSONL file
BATCH_SIZE=1000                                    # Max entries per push request
```

Place the edited script in a suitable directory (e.g., `/opt/scripts/`) and grant execute permission.

```bash
sudo cp restore-grafana-loki.sh /opt/scripts/
sudo chown root:root /opt/scripts/restore-grafana-loki.sh
sudo chmod 750 /opt/scripts/restore-grafana-loki.sh
```

### 8.2 Run the Restore

Run the restore with the following command, then verify Grafana's alert state history is restored.

```bash
sudo /opt/scripts/restore-grafana-loki.sh alert_history_20260615.jsonl
```
