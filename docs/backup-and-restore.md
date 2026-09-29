# Runbook — Backup and Restore: VictoriaMetrics / Grafana / Grafana Loki

This document describes how to back up and restore a monitoring platform deployed with the [oqtopus-device-monitoring](https://github.com/oqtopus-team/oqtopus-device-monitoring) configuration.

Run all commands from the repository root unless a step says otherwise.

---

## Table of Contents

- [0. Prerequisites](#0-prerequisites)
  - [0.1 Assumed Environment](#01-assumed-environment)
  - [0.2 Scripts](#02-scripts)
  - [0.3 Tools and Images](#03-tools-and-images)
  - [0.4 Environment-Specific Values](#04-environment-specific-values)
- [1. Backup Targets and Backup Policy](#1-backup-targets-and-backup-policy)
  - [1.1 Backup Directory Layout](#11-backup-directory-layout)
  - [1.2 VictoriaMetrics Time-Series Data](#12-victoriametrics-time-series-data)
  - [1.3 Grafana](#13-grafana)
  - [1.4 Grafana Loki](#14-grafana-loki)
- [2. Backing Up VictoriaMetrics](#2-backing-up-victoriametrics)
  - [2.1 Configure the Script (backup-vmstorage.sh)](#21-configure-the-script-backup-vmstoragesh)
  - [2.2 Create the Backup Manually](#22-create-the-backup-manually)
  - [2.3 Verify the Backup](#23-verify-the-backup)
- [3. Backing Up Grafana](#3-backing-up-grafana)
  - [3.1 Configure the Script (backup-grafana.sh)](#31-configure-the-script-backup-grafanash)
  - [3.2 Create the Backup Manually](#32-create-the-backup-manually)
  - [3.3 Verify the Backup](#33-verify-the-backup)
- [4. Backing Up Grafana Loki](#4-backing-up-grafana-loki)
  - [4.1 Configure the Script (backup-loki.sh)](#41-configure-the-script-backup-lokish)
  - [4.2 Create the Backup Manually](#42-create-the-backup-manually)
  - [4.3 Verify the Backup](#43-verify-the-backup)
- [5. Scheduling with cron](#5-scheduling-with-cron)
  - [5.1 Create the Log Directory](#51-create-the-log-directory)
  - [5.2 Create the cron File](#52-create-the-cron-file)
  - [5.3 Verify Operation](#53-verify-operation)
- [6. Restore Order](#6-restore-order)
- [7. Restoring VictoriaMetrics](#7-restoring-victoriametrics)
  - [7.1 Required Configuration Files](#71-required-configuration-files)
  - [7.2 Select the Backup Generation](#72-select-the-backup-generation)
  - [7.3 Write Back, One Node at a Time](#73-write-back-one-node-at-a-time)
  - [7.4 Verify the Restore](#74-verify-the-restore)
- [8. Restoring Grafana Loki](#8-restoring-grafana-loki)
  - [8.1 Required Configuration Files](#81-required-configuration-files)
  - [8.2 Select the Backup Generation](#82-select-the-backup-generation)
  - [8.3 Stop Grafana and Start Grafana Loki](#83-stop-grafana-and-start-grafana-loki)
  - [8.4 Push the Alert State History](#84-push-the-alert-state-history)
  - [8.5 Verify the Restore](#85-verify-the-restore)
- [9. Restoring Grafana](#9-restoring-grafana)
  - [9.1 Required Configuration Files](#91-required-configuration-files)
  - [9.2 Select the Backup Generation](#92-select-the-backup-generation)
  - [9.3 Restore the Backed-Up Provisioning Files](#93-restore-the-backed-up-provisioning-files)
  - [9.4 Start the Grafana Container](#94-start-the-grafana-container)
  - [9.5 (Optional) Restore Grafana from grafana.db as a Fallback](#95-optional-restore-grafana-from-grafanadb-as-a-fallback)
  - [9.6 Set Up the Injection Helper](#96-set-up-the-injection-helper)
  - [9.7 Inject the Resources](#97-inject-the-resources)
  - [9.8 Re-enter the Secrets by Hand](#98-re-enter-the-secrets-by-hand)
  - [9.9 Verify the Restore](#99-verify-the-restore)

---

## 0. Prerequisites

### 0.1 Assumed Environment

The VictoriaMetrics Cluster, Grafana, and Grafana Loki containers run via Docker Compose on an Ubuntu host.

| Stack | Directory | Containers relevant here |
| :--- | :--- | :--- |
| VictoriaMetrics Cluster | [`victoriametrics-cluster/`](../victoriametrics-cluster/) | `vmstorage-1`, `vmstorage-2` |
| Grafana and Grafana Loki | [`grafana-and-loki/`](../grafana-and-loki/) | `grafana`, `loki` |

### 0.2 Scripts

Backup uses the three scripts below.

| Script | Backup target | Section |
| :--- | :--- | :--- |
| [`./backup_and_restore/backup-vmstorage.sh`](../backup_and_restore/backup-vmstorage.sh) | VictoriaMetrics time-series data | [§2](#2-backing-up-victoriametrics) |
| [`./backup_and_restore/backup-grafana.sh`](../backup_and_restore/backup-grafana.sh) | Grafana configuration | [§3](#3-backing-up-grafana) |
| [`./backup_and_restore/backup-loki.sh`](../backup_and_restore/backup-loki.sh) | Grafana Loki alert state history | [§4](#4-backing-up-grafana-loki) |

Restore has no script. Run the commands in [§7](#7-restoring-victoriametrics)–[§9](#9-restoring-grafana) by hand.

### 0.3 Tools and Images

Backup and restore use `vmbackup`, `vmrestore`, and `LogCLI`, each run in a temporary Docker container from the images below.

| Tool | Docker image | Purpose |
| :--- | :--- | :--- |
| [vmbackup](https://docs.victoriametrics.com/victoriametrics/vmbackup/) | `victoriametrics/vmbackup:v1.127.0` | Back up VictoriaMetrics data |
| [vmrestore](https://docs.victoriametrics.com/victoriametrics/vmrestore/) | `victoriametrics/vmrestore:v1.127.0` | Restore VictoriaMetrics data |
| [LogCLI](https://grafana.com/docs/loki/latest/query/logcli/) | `grafana/logcli:3.5.8` | Back up Grafana Loki alert state history |

`jq` is also required by `backup-grafana.sh`, `backup-loki.sh`, and [§8.4](#84-push-the-alert-state-history).
Check that it is installed.

```bash
jq --version
```

### 0.4 Environment-Specific Values

The scripts reference values that differ per environment.
Look them up with the table below, then apply them to each script's configuration section ([§2](#2-backing-up-victoriametrics) onward).

| Item | Example value | How to verify |
| :--- | :--- | :--- |
| vmstorage / Grafana / Grafana Loki container names | `vmstorage-1` / `vmstorage-2` / `grafana` / `loki` | `docker ps` |
| Docker Compose network names | `victoriametrics-cluster_monitoring` / `grafana-and-loki_monitoring` | `docker network ls` |
| vmstorage data volume names | `victoriametrics-cluster_vmstorage1-data` / `victoriametrics-cluster_vmstorage2-data` | `docker inspect -f '{{.Name}} {{range .Mounts}}{{.Name}}{{end}}' vmstorage-1 vmstorage-2` |
| Grafana / Grafana Loki data volume names | `grafana-and-loki_grafana-data` / `grafana-and-loki_loki-data` | `docker volume ls` |
| vmstorage Snapshot API port | `8482` | `-httpListenAddr` in [`./victoriametrics-cluster/compose.yaml`](../victoriametrics-cluster/compose.yaml) |
| Query entry point (host) | `http://localhost:8428`, served by `vmauth-select` in front of `vmselect-1` and `vmselect-2` | `ports` in [`./victoriametrics-cluster/compose.yaml`](../victoriametrics-cluster/compose.yaml) |
| Grafana URL (host) | `http://localhost:3000` | `ports` in [`./grafana-and-loki/compose.yaml`](../grafana-and-loki/compose.yaml) |
| Grafana credentials | `GRAFANA_USERNAME` / `GRAFANA_PASSWORD` | `./grafana-and-loki/.env` |
| Grafana Loki URL (in-network / host) | `http://loki:3100` / `http://localhost:3100` | `ports` in [`./grafana-and-loki/compose.yaml`](../grafana-and-loki/compose.yaml) |

---

## 1. Backup Targets and Backup Policy

### 1.1 Backup Directory Layout

Each script creates its own subdirectory under `<EXPORT_DIR>`, and each run adds one timestamped generation directory (`<YYYYMMDD_HHMMSS>`) under it.
Each script keeps the number of generations set in its configuration section and rotates out older ones.
Passing the same `<EXPORT_DIR>` to all three produces the layout below.

```text
<EXPORT_DIR>/
├── vmstorage/              # backup-vmstorage.sh
│   ├── .lock               # flock target, prevents concurrent runs
│   ├── daily/
│   │   └── <YYYYMMDD_HHMMSS>/
│   └── weekly/
│       ├── <YYYYMMDD_HHMMSS>/
│       └── origin -> <YYYYMMDD_HHMMSS>
├── grafana/                # backup-grafana.sh
│   ├── .lock
│   └── <YYYYMMDD_HHMMSS>/
└── loki/                   # backup-loki.sh
    ├── .lock
    └── <YYYYMMDD_HHMMSS>/
```

Each script writes to `<YYYYMMDD_HHMMSS>.tmp` and renames it on success.
A `.tmp` directory remains only after SIGKILL, an OOM kill, or power loss; rotation ignores it and you can delete it.

> **Warning**: The backup holds credentials ([§1.3](#13-grafana)).
> Keep `<EXPORT_DIR>` out of version control and off world-readable shares.

### 1.2 VictoriaMetrics Time-Series Data

#### Backup Policy

- `vmstorage` deletes data older than `-retentionPeriod`.
- **A daily backup** keeps **the last `DAILY_KEEP` generations**.
- **A weekly backup** runs on `WEEKLY_DAY_OF_WEEK` and keeps **the last `WEEKLY_KEEP` generations**.
- All three are set in [§2.1](#21-configure-the-script-backup-vmstoragesh).
- If a generation from the same day exists, the backup is skipped.

Every generation, daily or weekly, is a complete backup that restores on its own.
The script passes the weekly backup as `-origin`, so `vmbackup` hard-links unchanged parts instead of copying them.
It does not build a diff chain, so rotating the weekly backup away leaves the older daily generations intact.

#### Backup Targets

`vmbackup` snapshots the entire `vmstorage` data directory.

| Backup target | Path in backup | Description |
| :--- | :--- | :--- |
| Time-series data | `<node>/data/{small,big}/<YYYY_MM>/` | Metric values (`values.bin`) and timestamps (`timestamps.bin`), stored as parts in monthly partitions. `big/` appears after merges produce large parts. |
| Inverted index | `<node>/indexdb/` | Mapping from label sets to time-series IDs, enabling search by metric name or label. |
| Metadata | `<node>/metadata/` | Internal settings such as `minTimestampForCompositeIndex`. |
| Backup management files | `<node>/*.ignore` | `backup_complete.ignore` and `backup_metadata.ignore`, used by `vmbackup`/`vmrestore` for generation management and consistency checks. |

#### Backup Directory Structure

Daily and weekly backups are stored under separate directories.
Within a generation, every `vmstorage` node has **its own directory** and the time-series data is **split by month**.

```text
<EXPORT_DIR>/vmstorage/
├── daily/                                    # Daily backups (DAILY_KEEP generations)
│   └── <YYYYMMDD_HHMMSS>/
│       ├── vmstorage-1/
│       │   ├── data/
│       │   │   └── small/
│       │   │       └── <YYYY_MM>/            # Monthly partition
│       │   │           └── <part_id>/        # values.bin, timestamps.bin, index.bin, ...
│       │   ├── indexdb/
│       │   ├── metadata/
│       │   │   └── minTimestampForCompositeIndex
│       │   ├── backup_complete.ignore
│       │   └── backup_metadata.ignore
│       └── vmstorage-2/                      # Managed independently
│           └── ...
└── weekly/                                   # Weekly backups (WEEKLY_KEEP generations)
    ├── <YYYYMMDD_HHMMSS>/
    │   ├── vmstorage-1/
    │   └── vmstorage-2/
    └── origin -> <YYYYMMDD_HHMMSS>           # Hard-link source for daily backups, not a diff chain
```

### 1.3 Grafana

#### Backup Policy

- Grafana configuration (dashboards, data sources, alerts, etc.) keeps **the last `DAILY_KEEP` generations** (set in [§3.1](#31-configure-the-script-backup-grafanash)).
- If a generation from the same day exists, the backup is skipped and Grafana keeps running.

#### Backup Targets

| Backup target | Path in backup |
| :--- | :--- |
| SQLite database | `db/grafana.db` |
| Folders | `api/folders.json` |
| Dashboards | `api/dashboards/<uid>.json` |
| Data sources | `api/datasources/<uid>.json` |
| Alert rules | `api/alert-rules.json` |
| Mute timings | `api/mute-timings.json` |
| Notification policy tree | `api/policies.json` |
| Notification templates | `api/templates.json` |
| Contact points | `api/contact-points.json` |
| Silences | `api/silences.json` |
| Provisioning sources | `files/provisioning/` |
| Provisioned dashboards | `files/dashboards/` |
| Custom images | `files/images/` |

`api/` holds only the resources created in the UI.
Resources that come from the provisioning files are restored as files in [§9.3](#93-restore-the-backed-up-provisioning-files) instead.

> **Warning**: `db/grafana.db` carries the hashed admin password, API keys, and the encrypted data source secrets, and `api/datasources/*.json` carries connection details.
> Protect the backup directory as described in [§1.1](#11-backup-directory-layout).

#### Backup Directory Structure

Each backup holds the API-exported resources under `api/` and the files copied from the container under `files/`.

```text
<EXPORT_DIR>/grafana/
└── <YYYYMMDD_HHMMSS>/
    ├── api/                             # UI-created resources exported via the Grafana API
    │   ├── dashboards/<uid>.json
    │   ├── datasources/<uid>.json
    │   ├── folders.json
    │   ├── alert-rules.json
    │   ├── mute-timings.json
    │   ├── templates.json
    │   ├── contact-points.json
    │   ├── silences.json
    │   └── policies.json                # absent when the policy tree is provisioned
    ├── files/                           # Raw files copied from the container
    │   ├── provisioning/
    │   │   ├── datasources/
    │   │   │   └── datasource.yml
    │   │   ├── dashboards/
    │   │   │   └── provider.yml
    │   │   └── alerting/
    │   │       ├── alert_resources.yaml
    │   │       └── alert_rules.yaml
    │   ├── dashboards/<dashboard>.json
    │   └── images/<image>
    └── db/
        └── grafana.db                   # SQLite database
```

### 1.4 Grafana Loki

#### Backup Policy

- The export covers the past **`RETENTION_YEARS` years** of alert state history.
- The backup keeps **the last `DAILY_KEEP` generations**.
- Both are set in [§4.1](#41-configure-the-script-backup-lokish).
- If a generation from the same day exists, the backup is skipped.
- The export is split by calendar month, **one file per month**, named `<YYYYMM>.json`. A month with no entries produces no file.

#### Backup Targets

| Backup target | Path in backup | Description |
| :--- | :--- | :--- |
| Alert state history | `<YYYYMM>.json` | Alert state-transition log from Grafana Unified Alerting, grouped into streams by label set. |

Each file preserves the original stream label sets (`from`, `orgID`, `group`, `folderUID`, ...), so [§8.4](#84-push-the-alert-state-history) can put every entry back into the stream it came from.

#### Backup Directory Structure

Each backup holds one file per calendar month.

```text
<EXPORT_DIR>/loki/
└── <YYYYMMDD_HHMMSS>/
    ├── 202508.json
    ├── 202509.json
    └── ...
```

---

## 2. Backing Up VictoriaMetrics

[`./backup_and_restore/backup-vmstorage.sh`](../backup_and_restore/backup-vmstorage.sh) runs `vmbackup` in a temporary Docker container, backs up each `vmstorage` node to a separate directory, and rotates out older generations.
See [§1.2](#12-victoriametrics-time-series-data) for the backup targets.

### 2.1 Configure the Script (backup-vmstorage.sh)

Look up the values with the table in [§0.4](#04-environment-specific-values), then edit the configuration section of `backup-vmstorage.sh`.

> **Warning**: Always back up each `vmstorage` node to a separate directory. Mixing data from different nodes leads to data corruption.

```bash
# ===== Configuration (edit this section) =====
# NODE_NAMES and NODE_VOLUMES are positional: the N-th node uses the N-th volume.
NODE_NAMES=(
  "vmstorage-1"
  "vmstorage-2"
)
NODE_VOLUMES=(
  "victoriametrics-cluster_vmstorage1-data"
  "victoriametrics-cluster_vmstorage2-data"
)
VMSTORAGE_NETWORK="victoriametrics-cluster_monitoring"
VMSTORAGE_PORT="8482"
DAILY_KEEP=7           # generations of daily backups to keep
WEEKLY_KEEP=1          # generations of weekly backups to keep
WEEKLY_DAY_OF_WEEK=7   # date +%u: 1=Mon ... 7=Sun
# =============================================
```

### 2.2 Create the Backup Manually

`<EXPORT_DIR>` must be an absolute path; the script rejects a relative one.

```bash
./backup_and_restore/backup-vmstorage.sh <EXPORT_DIR>
```

### 2.3 Verify the Backup

Confirm that the result matches the directory structure in [§1.2](#12-victoriametrics-time-series-data).

```bash
find <EXPORT_DIR>/vmstorage -maxdepth 4 | sort
```

Every node directory must contain `backup_complete.ignore`.

---

## 3. Backing Up Grafana

[`./backup_and_restore/backup-grafana.sh`](../backup_and_restore/backup-grafana.sh) copies `grafana.db` and exports JSON via the Grafana API into one timestamped directory, then rotates out older generations.
See [§1.3](#13-grafana) for the backup targets.

> **Note**: The script stops the `grafana` container while it copies `grafana.db`. Grafana is unavailable for a few seconds per run.

### 3.1 Configure the Script (backup-grafana.sh)

Look up the values with the table in [§0.4](#04-environment-specific-values), then edit the configuration section of `backup-grafana.sh`.

```bash
# ===== Configuration (edit this section) =====
GRAFANA_CONTAINER="grafana"           # Grafana container name
GRAFANA_URL="http://localhost:3000"   # Grafana URL reachable from the host
GRAFANA_ENV_FILE="/path/to/oqtopus-device-monitoring/grafana-and-loki/.env" # absolute path to Grafana credentials
DAILY_KEEP=1                          # generations of backups to keep
# =============================================
```

Point `GRAFANA_ENV_FILE` at the absolute path of `./grafana-and-loki/.env` on this host.
The script reads `GRAFANA_USERNAME` and `GRAFANA_PASSWORD` from that file on every run.

### 3.2 Create the Backup Manually

`<EXPORT_DIR>` must be an absolute path; the script rejects a relative one.

```bash
./backup_and_restore/backup-grafana.sh <EXPORT_DIR>
```

### 3.3 Verify the Backup

Confirm that the result matches the directory structure in [§1.3](#13-grafana).

```bash
find <EXPORT_DIR>/grafana -maxdepth 5 | sort
```

---

## 4. Backing Up Grafana Loki

[`./backup_and_restore/backup-loki.sh`](../backup_and_restore/backup-loki.sh) runs `LogCLI` in a temporary Docker container and exports the alert state history one calendar month at a time, then rotates out older generations.
See [§1.4](#14-grafana-loki) for the backup targets.

### 4.1 Configure the Script (backup-loki.sh)

Look up the values with the table in [§0.4](#04-environment-specific-values), then edit the configuration section of `backup-loki.sh`.

```bash
# ===== Configuration (edit this section) =====
LOKI_NETWORK="grafana-and-loki_monitoring" # Docker Compose network Grafana Loki is on
LOKI_INTERNAL_URL="http://loki:3100"       # Grafana Loki URL reachable from LOKI_NETWORK
RETENTION_YEARS=10                         # how far back to export
DAILY_KEEP=1                               # generations of backups to keep
# =============================================
```

### 4.2 Create the Backup Manually

`<EXPORT_DIR>` must be an absolute path; the script rejects a relative one.

```bash
./backup_and_restore/backup-loki.sh <EXPORT_DIR>
```

### 4.3 Verify the Backup

Confirm that the result matches the directory structure in [§1.4](#14-grafana-loki).

```bash
find <EXPORT_DIR>/loki -maxdepth 3 | sort
```

---

## 5. Scheduling with cron

Schedule the backups from [§2](#2-backing-up-victoriametrics)–[§4](#4-backing-up-grafana-loki) with cron, following the policy in [§1](#1-backup-targets-and-backup-policy).
The steps below use the `cron` package that ships with Ubuntu ([§0.1](#01-assumed-environment)).

### 5.1 Create the Log Directory

Create a log directory owned by `<USERNAME>`, the user the backup jobs run as.

```bash
sudo install -d -o <USERNAME> -g <USERNAME> -m 755 /var/log/odm-backup
```

### 5.2 Create the cron File

Create `/etc/cron.d/odm-backup` as below, replacing these placeholders:

- `<SCRIPT_DIR>`: absolute path of [`./backup_and_restore/`](../backup_and_restore/)
- `<EXPORT_DIR>`: absolute path of the backup destination ([§1.1](#11-backup-directory-layout))
- `<USERNAME>`: the user from [§5.1](#51-create-the-log-directory)

Credentials stay out of this file; `backup-grafana.sh` reads them from `GRAFANA_ENV_FILE` ([§3.1](#31-configure-the-script-backup-grafanash)).

```cron
# Template for /etc/cron.d/odm-backup.
# Replace <SCRIPT_DIR>, <EXPORT_DIR>, <USERNAME> (3rd field of each job line).

SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

SCRIPT_DIR=<SCRIPT_DIR>
EXPORT_DIR=<EXPORT_DIR>

# VictoriaMetrics - daily at 02:00 (the script takes the weekly full backup on WEEKLY_DAY_OF_WEEK)
0 2 * * * <USERNAME> $SCRIPT_DIR/backup-vmstorage.sh $EXPORT_DIR >> /var/log/odm-backup/vm-backup.log 2>&1

# Grafana - daily at 02:10 (stops Grafana briefly)
10 2 * * * <USERNAME> $SCRIPT_DIR/backup-grafana.sh $EXPORT_DIR >> /var/log/odm-backup/grafana-backup.log 2>&1

# Grafana Loki - daily at 02:20
20 2 * * * <USERNAME> $SCRIPT_DIR/backup-loki.sh $EXPORT_DIR >> /var/log/odm-backup/loki-backup.log 2>&1
```

Cron silently ignores the file unless it is owned by root with mode `644`.

```bash
sudo chown root:root /etc/cron.d/odm-backup
sudo chmod 644 /etc/cron.d/odm-backup
```

Each line appends both stdout and stderr to its own log, so errors and normal output share one file.

| Script | Log file |
| :--- | :--- |
| `backup-vmstorage.sh` | `/var/log/odm-backup/vm-backup.log` |
| `backup-grafana.sh` | `/var/log/odm-backup/grafana-backup.log` |
| `backup-loki.sh` | `/var/log/odm-backup/loki-backup.log` |

### 5.3 Verify Operation

Confirm that cron is running and the file is in place.

```bash
sudo systemctl status cron
sudo cat /etc/cron.d/odm-backup
```

After the backup jobs run, confirm that cron started all three, then that each one finished without an error or a warning.

```bash
journalctl -u cron --since today | grep -E 'backup-(vmstorage|grafana|loki)\.sh'
tail -n 10 -v /var/log/odm-backup/*.log
```

---

## 6. Restore Order

Restore the three stacks in the order below, or run a single section on its own.

| Step | Section | Target | Dependencies |
| :--- | :--- | :--- | :--- |
| 1 | [§7](#7-restoring-victoriametrics) | VictoriaMetrics time-series data | — |
| 2 | [§8](#8-restoring-grafana-loki) | Grafana Loki alert state history | `grafana` stopped until [§9.4](#94-start-the-grafana-container) |
| 3 | [§9](#9-restoring-grafana) | Grafana configuration | After [§8](#8-restoring-grafana-loki) |

A running Grafana writes new alert state history into the streams being restored, which is why it stays stopped through [§8](#8-restoring-grafana-loki).

---

## 7. Restoring VictoriaMetrics

`vmrestore` writes one node's backup back into that node's volume.
Nodes are [shared-nothing](https://docs.victoriametrics.com/victoriametrics/cluster-victoriametrics/) and can be restored independently, but **pick the same generation for every node**.

### 7.1 Required Configuration Files

Prepare the files needed to start the `vmstorage` containers.
On a fresh host, deploy them first as described in [`GETTING_STARTED.md`](../GETTING_STARTED.md).

- `./victoriametrics-cluster/compose.yaml`
- `./victoriametrics-cluster/.env`
- `./victoriametrics-cluster/vmagent/`
- `./victoriametrics-cluster/vmauth-select/`
- `./victoriametrics-cluster/vmauth-insert/`
- `./victoriametrics-cluster/weekly-relabeling-cron/`

Then confirm the node-to-volume mapping with the `docker inspect` command in [§0.4](#04-environment-specific-values), and use those volume names in [§7.3](#73-write-back-one-node-at-a-time).

### 7.2 Select the Backup Generation

Set the generation to restore as an absolute path.

```bash
cd victoriametrics-cluster
GEN_VM=<EXPORT_DIR>/vmstorage/daily/<YYYYMMDD_HHMMSS>
ls "$GEN_VM"    # expected: vmstorage-1  vmstorage-2
```

### 7.3 Write Back, One Node at a Time

`vmrestore` writes straight into the node's data volume, so the node has to be stopped first.
Stop **only** the node being restored; the others keep serving while it is down.
Run the commands in the shell where `GEN_VM` is set ([§7.2](#72-select-the-backup-generation)).

> **Warning**: `vmrestore` works like [`rsync --delete`](https://docs.victoriametrics.com/victoriametrics/vmrestore/), so every sample ingested after the chosen generation is deleted.
> Each node must also go into its own volume ([§7.1](#71-required-configuration-files)); mixing data from different nodes leads to data corruption.

`vmrestore` runs as root and leaves every file mode `700`/`600`.
Without the `chmod` to `755`/`644` below, the next backup fails with `permission denied`.

vmstorage-1:

```bash
docker stop -t 60 vmstorage-1
docker run --rm --network none \
  -v victoriametrics-cluster_vmstorage1-data:/storage \
  -v "$GEN_VM/vmstorage-1:/backup:ro" \
  victoriametrics/vmrestore:v1.127.0 -storageDataPath=/storage -src=fs:///backup
sudo chmod -R u+rwX,go+rX "$(docker volume inspect -f '{{.Mountpoint}}' victoriametrics-cluster_vmstorage1-data)"
docker start vmstorage-1
```

vmstorage-2:

```bash
docker stop -t 60 vmstorage-2
docker run --rm --network none \
  -v victoriametrics-cluster_vmstorage2-data:/storage \
  -v "$GEN_VM/vmstorage-2:/backup:ro" \
  victoriametrics/vmrestore:v1.127.0 -storageDataPath=/storage -src=fs:///backup
sudo chmod -R u+rwX,go+rX "$(docker volume inspect -f '{{.Mountpoint}}' victoriametrics-cluster_vmstorage2-data)"
docker start vmstorage-2
```

### 7.4 Verify the Restore

Confirm that every node is running again.

```bash
docker compose ps vmstorage-1 vmstorage-2
```

Then query a metric over a time range that predates the chosen generation, using the Grafana UI (**Explore**) or the query entry point ([§0.4](#04-environment-specific-values)), and confirm the samples are present.

---

## 8. Restoring Grafana Loki

Restores the alert state history.

> **Warning**: Restore Grafana Loki **before** Grafana ([§6](#6-restore-order)).

### 8.1 Required Configuration Files

Prepare the files needed to start the `loki` container.
On a fresh host, deploy them first as described in [`GETTING_STARTED.md`](../GETTING_STARTED.md).

- `./grafana-and-loki/compose.yaml`
- `./grafana-and-loki/.env`
- `./grafana-and-loki/loki/loki-config.yaml`

Confirm all three settings below in `loki-config.yaml` before starting `loki`.

| Setting | Required value | Grafana Loki default | Why it is required |
| :--- | :--- | :--- | :--- |
| `limits_config.reject_old_samples` | `false` | `true` | The entries being pushed are older than `reject_old_samples_max_age`. With the default, every push is rejected. |
| `ingester.max_chunk_age` | `175200h` | `2h` | An entry older than `max_chunk_age` is refused as too far behind. With the default, that is every entry in the history being restored. |
| `limits_config.max_query_length` | `876000h` | `30d1h` | Step 1 of the push loop queries one calendar month at a time. A 31-day month spans 744h and exceeds the default. |

### 8.2 Select the Backup Generation

Set the generation to restore as an absolute path.

```bash
cd grafana-and-loki
GEN_LOKI=<EXPORT_DIR>/loki/<YYYYMMDD_HHMMSS>
ls "$GEN_LOKI"    # expected: a list of <YYYYMM>.json
```

### 8.3 Stop Grafana and Start Grafana Loki

Stop `grafana` so that it cannot write into the streams being restored, then start `loki` and wait until it is ready.

```bash
docker compose stop grafana
docker compose up -d loki
until curl -sf http://localhost:3100/ready > /dev/null; do sleep 1; done
```

Leave `grafana` stopped until [§9.4](#94-start-the-grafana-container).

### 8.4 Push the Alert State History

Run the block below in the shell where `GEN_LOKI` is set ([§8.2](#82-select-the-backup-generation)).
It walks the `<YYYYMM>.json` files oldest month first and repeats four steps per month:

1. Query Grafana Loki for the entries it already holds in that month.
2. Subtract them from the backup file.
3. Skip the month if nothing is left.
4. Push the remainder in a single request.

`LOKI_URL` and the network name in `LOGCLI` come from [§0.4](#04-environment-specific-values).

> **Note**: One request per month is fine for a few hundred entries.
> Split `push.json` for tens of thousands, since Grafana Loki caps a push at 4 MB (`grpc_server_max_recv_msg_size`) and 6 MB (`ingestion_burst_size_mb`).

```bash
(                   # subshell: the settings below do not leak into your shell
set -o pipefail     # a failed key query must not look like an empty month
shopt -s nullglob   # an empty generation directory must not enter the loop

# Settings (see §0.4)
LOKI_URL="http://localhost:3100"
LOGCLI="docker run --rm --network grafana-and-loki_monitoring grafana/logcli:3.5.8 --addr=http://loki:3100"
WORK=$(mktemp -d)
trap 'rm -f "$WORK/existing.keys" "$WORK/push.json"' EXIT   # the responses stay
echo "response bodies: $WORK"

# RFC3339 timestamp -> nanoseconds, the form both Grafana Loki and the backup use as the key
TO_NS='def to_ns: capture("^(?<s>[^.Z]+)(\\.(?<f>[0-9]+))?Z$")
  | ((.s + "Z") | fromdateiso8601 | tostring) + (((.f // "") + "000000000")[0:9]);'

for f in "$GEN_LOKI"/*.json; do
  ym=$(basename "$f" .json)
  from="${ym:0:4}-${ym:4:2}-01T00:00:00Z"
  to=$(date -u -d "${ym:0:4}-${ym:4:2}-01 +1 month" +%Y-%m-01T00:00:00Z)

  # 1. Collect the keys Grafana Loki already holds for this month
  if ! $LOGCLI query '{from="state-history"}' --from="$from" --to="$to" \
       --limit=0 --batch=1000 --quiet -o jsonl \
       | jq -c "$TO_NS [(.timestamp | to_ns), .line]" > "$WORK/existing.keys"; then
    echo "$ym: key query failed, skipped" >&2
    continue
  fi

  # 2. Keep only the backup entries whose key is missing, stream by stream
  jq -c -n --slurpfile backup "$f" --slurpfile existing "$WORK/existing.keys" '
    ($existing | INDEX(tojson)) as $seen
    | {streams: [$backup[0].streams[]
        | .values |= map(select($seen[tojson] == null))
        | select(.values | length > 0)]}' > "$WORK/push.json"

  # 3. Nothing missing: this month is already restored
  if ! jq -e '[.streams[].values[]] | length > 0' "$WORK/push.json" > /dev/null; then
    echo "$ym: nothing to inject"
    continue
  fi

  # 4. Push the remainder in one request
  curl -sS -o "$WORK/response-$ym.json" -w "$ym: HTTP %{http_code}\n" \
    -H 'Content-Type: application/json' --data-binary @"$WORK/push.json" \
    "$LOKI_URL/loki/api/v1/push"
done
)
```

Each month prints `<YYYYMM>: HTTP 204` or `<YYYYMM>: nothing to inject`.
Anything else means that month failed to restore; the reason is in the response body, kept as `response-<YYYYMM>.json` in the directory the block prints when it starts.
Delete that directory once every month is restored.

> **Warning**: Do not re-run the loop right away.
> Step 1 does not necessarily see the entries just pushed, and a month pushed twice writes every entry twice.
> The duplicates then stay: the Delete API selects by stream and line content, so it cannot single out one of two byte-identical entries.
> Run [§8.5](#85-verify-the-restore) first, then re-run the loop only for a month that is still missing.

### 8.5 Verify the Restore

Query the alert state history in the Grafana UI (**Alerting → History**) or with `LogCLI`, and confirm that the expected entries are present.

---

## 9. Restoring Grafana

Grafana is restored from two sources:

- Provisioning files, copied back into the bind mount ([§9.3](#93-restore-the-backed-up-provisioning-files))
- UI-created resources, injected through the Grafana API ([§9.7](#97-inject-the-resources))

`grafana.db` is used only by the fallback in [§9.5](#95-optional-restore-grafana-from-grafanadb-as-a-fallback).

### 9.1 Required Configuration Files

Prepare the files needed to start the `grafana` container.
On a fresh host, deploy them first as described in [`GETTING_STARTED.md`](../GETTING_STARTED.md).

- `./grafana-and-loki/compose.yaml`
- `./grafana-and-loki/Makefile`
- `./grafana-and-loki/.env`

### 9.2 Select the Backup Generation

Set the generation to restore as an absolute path.

```bash
cd grafana-and-loki
GEN_G=<EXPORT_DIR>/grafana/<YYYYMMDD_HHMMSS>
ls "$GEN_G"    # expected: api  db  files
```

### 9.3 Restore the Backed-Up Provisioning Files

Copy `files/provisioning`, `files/dashboards`, and `files/images` into the matching subdirectories of `./grafana/`.
Run the command in the shell where `GEN_G` is set ([§9.2](#92-select-the-backup-generation)).

```bash
cp -r "$GEN_G/files/." ./grafana/
```

### 9.4 Start the Grafana Container

The injection in [§9.7](#97-inject-the-resources) requires the Grafana API to be reachable.

```bash
docker compose up -d grafana
until curl -sf http://localhost:3000/api/health > /dev/null; do sleep 1; done
```

Grafana starts with the admin credentials from `.env` (`GF_SECURITY_ADMIN_*`) and the provisioning from the bind mount.

### 9.5 (Optional) Restore Grafana from `grafana.db` as a Fallback

Use this only to recover what the API injection does not carry: UI-created users, teams, service accounts, secrets.
Skip this section otherwise.

> **Warning**: This replaces `grafana.db` in the volume outright. Everything the running Grafana holds and the backup does not is lost, unlike the API injection in [§9.7](#97-inject-the-resources).

`install` copies the file and sets the owner and mode Grafana expects (`472:0`, `640`) in one step.

```bash
docker compose stop grafana
sudo install -o 472 -g 0 -m 640 "$GEN_G/db/grafana.db" \
  "$(docker volume inspect -f '{{.Mountpoint}}' grafana-and-loki_grafana-data)/grafana.db"
docker compose start grafana
until curl -sf http://localhost:3000/api/health > /dev/null; do sleep 1; done
```

### 9.6 Set Up the Injection Helper

Run both blocks, and then [§9.7](#97-inject-the-resources), in the shell where `GEN_G` is set ([§9.2](#92-select-the-backup-generation)).

First, load the credentials and check them.
The command must print `200`; with the wrong credentials, every request in [§9.7](#97-inject-the-resources) returns 401 and nothing is injected.

```bash
shopt -s nullglob   # skip empty directories
G=http://localhost:3000
RESP=$(mktemp)      # response body of the last request, owner-readable only

. ./.env                              # keep the password out of the shell history
curl -s -o /dev/null -w '%{http_code}\n' \
  -u "${GRAFANA_USERNAME:?}:${GRAFANA_PASSWORD:?}" "$G/api/org"
```

Then define `gp`, the helper [§9.7](#97-inject-the-resources) calls for every request.
`X-Disable-Provenance` keeps the restored resources editable from the UI; endpoints that do not use it ignore the header.
Without it Grafana marks them `provenance: api` and the next backup skips them.
Every response body, success or failure, is written to `$RESP` and overwritten by the next request.

```bash
gp() {   # gp <METHOD> <API_PATH> <LABEL>, request body on stdin
  local code
  code=$(curl -sS -o "$RESP" -w '%{http_code}' \
    -u "$GRAFANA_USERNAME:$GRAFANA_PASSWORD" \
    -X "$1" -H 'Content-Type: application/json' -H 'X-Disable-Provenance: true' \
    --data-binary @- "$G$2")
  echo "$code  $1 $2  <- $3"
}
```

### 9.7 Inject the Resources

Keep the order below: every object has to exist before the ones that reference it.
Folders come before dashboards and alert rules, contact points before alert rules and the policy tree, and mute timings before the policy tree.

A file holding an array is replayed in order, so a subfolder is never created before its parent.
The template name lives in the JSON, and its URL form is percent-encoded because a name may contain `/`, `#`, or `?`.

```bash
jq -c '.[]' "$GEN_G/api/folders.json" | while IFS= read -r o; do
  gp POST /api/folders "$(jq -r '.title' <<< "$o")" <<< "$o"
done
for f in "$GEN_G"/api/datasources/*.json; do gp POST /api/datasources "$(basename "$f")" < "$f"; done
for f in "$GEN_G"/api/dashboards/*.json;  do gp POST /api/dashboards/db "$(basename "$f")" < "$f"; done
jq -c '.[]' "$GEN_G/api/templates.json" | while IFS= read -r o; do
  n=$(jq -r '.name' <<< "$o")
  jq -c '{template}' <<< "$o" | gp PUT "/api/v1/provisioning/templates/$(jq -rn --arg s "$n" '$s|@uri')" "$n"
done
jq -c '.[]' "$GEN_G/api/contact-points.json" | while IFS= read -r o; do
  gp POST /api/v1/provisioning/contact-points "$(jq -r '.name' <<< "$o")" <<< "$o"
done
jq -c '.[]' "$GEN_G/api/mute-timings.json" | while IFS= read -r o; do
  gp POST /api/v1/provisioning/mute-timings "$(jq -r '.name' <<< "$o")" <<< "$o"
done
if [ -f "$GEN_G/api/policies.json" ]; then gp PUT /api/v1/provisioning/policies policies.json < "$GEN_G/api/policies.json"; fi
jq -c '.[]' "$GEN_G/api/alert-rules.json" | while IFS= read -r o; do
  gp POST /api/v1/provisioning/alert-rules "$(jq -r '.title' <<< "$o")" <<< "$o"
done
# A silence has no uid and every push creates a new one, so expire the current set first.
SILENCE=/api/alertmanager/grafana/api/v2/silence
for i in $(curl -sS -u "$GRAFANA_USERNAME:$GRAFANA_PASSWORD" "$G${SILENCE}s" \
           | jq -r '(. // [])[] | select(.status.state != "expired") | .id'); do
  gp DELETE "$SILENCE/$i" 'expire' < /dev/null
done
jq -c '.[]' "$GEN_G/api/silences.json" | while IFS= read -r o; do
  gp POST "${SILENCE}s" "$(jq -r '.comment' <<< "$o")" <<< "$o"
done
```

Each line prints the HTTP code, the request, and the name of the resource it came from.
2xx is success; `409` and `412` mean that uid already exists and was left untouched.
For any other code, re-run that one line and read `$RESP`, which holds the last response only.

> **Note**: Two failures are expected.
> A mute timing that already exists returns `400 … Time interval with this name already exists` instead of `409`.
> A silence whose `endsAt` has passed returns `400 … end time can't be in the past` — move `endsAt` into the future first if you want to keep it.

### 9.8 Re-enter the Secrets by Hand

The Grafana API strips secret fields from its responses, so contact points and data sources restore with everything except the secret itself.
Resources that come from the provisioning files keep their secrets and need no action.

Contact points: list the `settings` keys present in each contact point.
oqtopus-device-monitoring ships only Slack, whose missing key is `url` or `token`.

```bash
jq -r '.[] | "\(.name)  (\(.type))  uid=\(.uid)  settings: \(.settings | keys | join(","))"' \
  "$GEN_G/api/contact-points.json"
```

Re-enter the missing value under **Alerting → Contact points**.

Data sources: list the ones that had a secret.
`secureJsonFields` names them (`basicAuthPassword`, `httpHeaderValue1`, ...), and after the restore that object is empty.

```bash
jq -r 'select((.secureJsonFields // {}) | length > 0)
  | "\(.name)  (\(.type))  uid=\(.uid)  secrets: \(.secureJsonFields | keys | join(","))"' \
  "$GEN_G"/api/datasources/*.json < /dev/null
```

Re-enter each value under **Connections → Data sources**.

### 9.9 Verify the Restore

Confirm in the Grafana UI or through the Grafana API that the restored folders, dashboards, data sources, and alert rules match the backup.

Then clear the shell: [§9.6](#96-set-up-the-injection-helper) left the helper, the response file, and the Grafana password in it.

```bash
rm -f "$RESP"
unset -f gp; unset G RESP GRAFANA_USERNAME GRAFANA_PASSWORD
shopt -u nullglob
```
