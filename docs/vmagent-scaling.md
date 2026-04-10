# Runbook — Scaling `vmagent`: **Sharded (load-spread)** vs **HA (replicated)**

This guide gives two production-ready patterns for scaling `vmagent`:

1. **Sharded**: split scrape targets across multiple `vmagent` instances.
2. **HA (replicated)**: scrape the _same_ targets from multiple `vmagent` instances and de-duplicate downstream.

References in brackets point to the official docs and deep dives.

---

## Summary

- **Sharded mode**: set `membersCount` + per-member `memberNum`. No VM dedup needed. Verify via `/service-discovery`. ([docs.victoriametrics.com][1])
- **HA mode**: set `replicationFactor>1` (or run two clusters with unique `cluster.name`) **and** enable VM dedup with `-dedup.minScrapeInterval` (≈ `scrape_interval`). Verify via `/targets` and `vm_deduplicated_samples_total`. ([docs.victoriametrics.com][1])

---

## 0. Prerequisites and assumptions

- The system is working along with VictoriaMetrics **Cluster** (via `vminsert/vmstorage/vmselect`).
- All `vmagent` instances will use the **same** `promscrape.config` (Prometheus-style `scrape_configs`), except for cluster flags shown below. ([docs.victoriametrics.com][1])
- For **HA (replicated)** setups, it is necessary to enable **deduplication** on VictoriaMetrics with `-dedup.minScrapeInterval`. See section 2B. ([docs.victoriametrics.com][1])

---

## 1. Common files

### 1.1 `prometheus.yml` (shared by all vmagent instances)

Keep one canonical scrape config and mount it read-only to each vmagent:

```yaml
global:
  scrape_interval: 15s
  scrape_timeout: 10s
  evaluation_interval: 15s

scrape_configs:
  - job_name: "node"
    static_configs:
      - targets:
          - node-exporter-1:9100
          - node-exporter-2:9100
          - node-exporter-3:9100
```

**Note:** vmagent can handle very large target sets; when that's not enough, use clustering flags below to shard. ([docs.victoriametrics.com][1])

### 1.2 Remote write URL(s)

For **cluster VictoriaMetrics**, the typical URL is:

```bash
http://<vminsert>:8480/insert/<tenant-id>/prometheus/api/v1/write
```

(Or via `vmauth` in front). ([docs.victoriametrics.com][1])

---

## 2. Pattern A — **Sharded (load-spread)** scraping

**Goal:** Distribute targets evenly so **each target is scraped by exactly one vmagent**.

### 2.1 Concept

- Set cluster size with `-promscrape.cluster.membersCount=N`.
- Give each vmagent a unique `-promscrape.cluster.memberNum` in `0..N-1`.
- All members share the **same** `prometheus.yml`. vmagent automatically splits targets across members. ([docs.victoriametrics.com][1])

### 2.2 Minimal Docker Compose (two members)

```yaml
services:
  vmagent-0:
    image: victoriametrics/vmagent:latest
    command:
      - -promscrape.config=/etc/prometheus/prometheus.yml
      - -promscrape.cluster.membersCount=2
      - -promscrape.cluster.memberNum=0
      - -promscrape.cluster.name=vmagent-shard # helps if you later add HA
      - -promscrape.cluster.memberLabel=vmagent_instance
      - -remoteWrite.url=http://vmauth-insert:8427/insert/0/prometheus/api/v1/write
      - -remoteWrite.tmpDataPath=/queues/remote
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - vmagent0-queue:/queues/remote
    ports: ["8429:8429"] # optional: expose UI (/targets, /service-discovery)

  vmagent-1:
    image: victoriametrics/vmagent:latest
    command:
      - -promscrape.config=/etc/prometheus/prometheus.yml
      - -promscrape.cluster.membersCount=2
      - -promscrape.cluster.memberNum=1
      - -promscrape.cluster.name=vmagent-shard
      - -promscrape.cluster.memberLabel=vmagent_instance
      - -remoteWrite.url=http://vmauth-insert:8427/insert/0/prometheus/api/v1/write
      - -remoteWrite.tmpDataPath=/queues/remote
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - vmagent1-queue:/queues/remote

volumes:
  vmagent0-queue: {}
  vmagent1-queue: {}
```

### 2.3 Verification checklist

1. **Target distribution UI**
   Open each vmagent’s **`/service-discovery`** and **`/targets`** pages to see which member actually scrapes a given target. If a target is _dropped_ on one member due to sharding, the page shows which `memberNum` is scraping it. Optionally set `-promscrape.cluster.memberURLTemplate` to make links to peer UIs clickable. ([docs.victoriametrics.com][1])
   Examples:

- `http://vmagent-0:8429/service-discovery`
- `http://vmagent-1:8429/targets`

2. **Scrape load**
   Dashboards/queries to watch:

- `vm_promscrape_discovered_targets` by member
- `vm_promscrape_active_samples` by member
  (Exported by vmagent itself.)

> That’s it. In sharded mode you **do not** need VM-side deduplication—each target is scraped only once. ([docs.victoriametrics.com][1])

---

## 3. Pattern B — **HA (replicated)** scraping

**Goal:** Scrape the **same** targets from _multiple_ vmagent instances for resilience; deduplicate downstream at VictoriaMetrics.

There are two equivalent ways:

### 3.1 **One** vmagent cluster with replication

Set:

- `-promscrape.cluster.membersCount=N`
- `-promscrape.cluster.replicationFactor=R` (e.g. `2`)
  Each target will be scraped by **R** members. ([docs.victoriametrics.com][1])

**Compose example (three members, RF=2):**

```yaml
services:
  vmagent-0:
    image: victoriametrics/vmagent:latest
    command:
      - -promscrape.config=/etc/prometheus/prometheus.yml
      - -promscrape.cluster.membersCount=3
      - -promscrape.cluster.replicationFactor=2
      - -promscrape.cluster.memberNum=0
      - -promscrape.cluster.name=vmagent-ha
      - -remoteWrite.url=http://vmauth-insert:8427/insert/0/prometheus/api/v1/write
      - -remoteWrite.tmpDataPath=/queues/remote
    volumes:
      [
        "./prometheus.yml:/etc/prometheus/prometheus.yml:ro",
        "q0:/queues/remote",
      ]

  vmagent-1:
    image: victoriametrics/vmagent:latest
    command:
      - -promscrape.config=/etc/prometheus/prometheus.yml
      - -promscrape.cluster.membersCount=3
      - -promscrape.cluster.replicationFactor=2
      - -promscrape.cluster.memberNum=1
      - -promscrape.cluster.name=vmagent-ha
      - -remoteWrite.url=http://vmauth-insert:8427/insert/0/prometheus/api/v1/write
      - -remoteWrite.tmpDataPath=/queues/remote
    volumes:
      [
        "./prometheus.yml:/etc/prometheus/prometheus.yml:ro",
        "q1:/queues/remote",
      ]

  vmagent-2:
    image: victoriametrics/vmagent:latest
    command:
      - -promscrape.config=/etc/prometheus/prometheus.yml
      - -promscrape.cluster.membersCount=3
      - -promscrape.cluster.replicationFactor=2
      - -promscrape.cluster.memberNum=2
      - -promscrape.cluster.name=vmagent-ha
      - -remoteWrite.url=http://vmauth-insert:8427/insert/0/prometheus/api/v1/write
      - -remoteWrite.tmpDataPath=/queues/remote
    volumes:
      [
        "./prometheus.yml:/etc/prometheus/prometheus.yml:ro",
        "q2:/queues/remote",
      ]

volumes: { q0: {}, q1: {}, q2: {} }
```

### 3.2 **Two (or more) independent vmagent clusters** as an HA pair

Run **two identically configured** clusters (or single vmagent instances) that scrape the same targets and push to the **same** VictoriaMetrics backend.
**Important:** give each HA cluster a unique name with `-promscrape.cluster.name` to make deduplication unambiguous. ([docs.victoriametrics.com][1])

---

### 3.3 **Mandatory** deduplication at VictoriaMetrics

When the same series is ingested from multiple agents, **enable dedup** on the backend:

- **Cluster VM:** set `-dedup.minScrapeInterval=<scrape_interval>` at **vmselect** (recommended to set the same value at **vmstorage** too for consistency). Examples:

  - `-dedup.minScrapeInterval=15s` if `scrape_interval` is `15s`.
  - For replicated storage (multiple `vmstorage` copies via `-replicationFactor`), a small value like `1ms` is typically used; keep it **≥** the true ingest duplication characteristics, or set to the global scrape interval if agents are identical. ([docs.victoriametrics.com][2])

- **Single-node VM:** same flag on the server. Dedup is applied during queries and background merges (ingested raw samples are still stored until merged). ([docs.victoriametrics.com][3])

> The vmagent docs explicitly state: **if each target is scraped by multiple vmagent instances, enable dedup at the remote storage and set `-dedup.minScrapeInterval` to the `scrape_interval`.** ([docs.victoriametrics.com][1])

---

### 3.4 Optional: pre-forwarding dedup/aggregation at vmagent

If you want to reduce ingest volume **before** sending to VM, you may enable **streaming dedup** or **stream aggregation** on vmagent, e.g.:

```yaml
-streamAggr.dedupInterval=60s
# or per-destination:
-remoteWrite.streamAggr.dedupInterval=60s
-remoteWrite.streamAggr.dropInputLabels=replica
```

This keeps only the last sample per interval and can merge replicas by dropping a `replica` label before dedup. Use carefully—this changes raw data granularity. ([docs.victoriametrics.com][1])

---

## 4. Operational checks & troubleshooting

### 4.1 Sharded vs HA visibility

- **Sharded**: Every target appears as **`DROPPED`** on non-responsible members (with the **responsible `memberNum`** shown) and **`UP`** on a single member. Use `/service-discovery` & `/targets`. ([docs.victoriametrics.com][1])
- **HA (replicated)**: Each target is **`UP`** on **R** members (R = replication factor). Use the same pages to confirm. ([docs.victoriametrics.com][1])

### 4.2 VictoriaMetrics dedup working?

- Ensure VM started with `-dedup.minScrapeInterval` as above. (Cluster: set on `vmselect`; recommended the same on `vmstorage`.) ([docs.victoriametrics.com][2])
- Watch backend metric **`vm_deduplicated_samples_total`** increasing when HA is on. ([VictoriaMetrics][4])

### 4.3 Sanity queries / dashboards

- In Grafana (VM datasource):

  - **Scrape volume per vmagent member:**
    `sum(rate(vm_promscrape_samples_added_total[5m])) by (vmagent_instance, job)`
  - **Discovered targets per vmagent member:**
    `sum(vm_promscrape_discovered_targets) by (vmagent_instance, state)`

- Use vmagent’s own UI to drill into targets: `/targets`, `/service-discovery`. ([docs.victoriametrics.com][1])

### 4.4 Failure simulation (HA)

- Stop one agent (e.g., `docker stop vmagent-1`) and confirm:

  - Remaining member(s) continue scraping the targets (R-1 redundancy).
  - Queries still return continuous series (thanks to VM dedup). ([docs.victoriametrics.com][1])

### 4.5 Common pitfalls

- **Forgetting VM-side dedup in HA** → apparent data inflation; sums/over_time results doubled. Fix by setting `-dedup.minScrapeInterval`. ([docs.victoriametrics.com][2])
- **Mismatched scrape intervals** across jobs with HA → pick the **largest** consistent value for `-dedup.minScrapeInterval`, or standardize your `scrape_interval`. (Analogous guidance exists for rule intervals.) ([docs.victoriametrics.com][5])
- **Changing HA topology** without updating `membersCount/memberNum` → targets may be under- or over-scraped. Confirm in `/service-discovery`. ([docs.victoriametrics.com][1])

---

## 5. Quick command-line examples (non-Docker)

### Sharded, 2 members

```bash
vmagent -promscrape.config=/etc/prometheus/prometheus.yml \
        -promscrape.cluster.membersCount=2 -promscrape.cluster.memberNum=0 \
        -remoteWrite.url=http://vmauth-insert:8427/insert/0/prometheus/api/v1/write

vmagent -promscrape.config=/etc/prometheus/prometheus.yml \
        -promscrape.cluster.membersCount=2 -promscrape.cluster.memberNum=1 \
        -remoteWrite.url=http://vmauth-insert:8427/insert/0/prometheus/api/v1/write
```

([docs.victoriametrics.com][1])

### HA, 3 members, RF=2

```bash
vmagent -promscrape.config=/etc/prometheus/prometheus.yml \
        -promscrape.cluster.membersCount=3 -promscrape.cluster.replicationFactor=2 \
        -promscrape.cluster.memberNum=0 -promscrape.cluster.name=vmagent-ha \
        -remoteWrite.url=http://vmauth-insert:8427/insert/0/prometheus/api/v1/write
# ...repeat for memberNum 1 and 2
```

([docs.victoriametrics.com][1])

[1]: https://docs.victoriametrics.com/victoriametrics/vmagent/ "VictoriaMetrics: vmagent"
[2]: https://docs.victoriametrics.com/victoriametrics/cluster-victoriametrics "VictoriaMetrics: Cluster version"
[3]: https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics "VictoriaMetrics: Single-node version"
[4]: https://victoriametrics.com/blog/vmstorage-retention-merging-deduplication/ "How vmstorage Processes Data: Retention, Merging, ..."
[5]: https://docs.victoriametrics.com/victoriametrics/vmalert/ "VictoriaMetrics: vmalert"
