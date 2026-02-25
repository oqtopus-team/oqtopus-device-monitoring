# Grafana & Loki Stack

This directory contains a lightweight Grafana + Loki setup with provisioning for:

- Prometheus-compatible VictoriaMetrics Cluster datasource
- Loki datasource
- Dashboards provider
- Alerting rules and contact points

## Prerequisites

- Docker and Docker Compose installed
- Access to a running VictoriaMetrics Cluster
- Linux environment (commands assume Linux)

## Files Overview

- `compose.yaml`: Defines Grafana and Loki services.
- `grafana/provisioning/datasources/datasource.yml`: Datasource definitions (Prometheus and Loki).
- `grafana/provisioning/dashboards/provider.yml`: Enables file-based dashboard provisioning.
- `grafana/dashboards/*`: Dashboard JSON files.
- `grafana/provisioning/alerting/*`: Alerting resources and rules.
- `loki/loki-config.yaml`: Loki configuration.
- `Makefile`: Helper targets to start/stop.

## Important: Set HOST_IP in datasource.yml

You must set the VictoriaMetrics Cluster host IP in the Prometheus URL inside `datasource.yml`.

Edit:

```text
grafana/provisioning/datasources/datasource.yml
```

Replace the `host.docker.internal` host IP with your VictoriaMetrics Cluster host IP:

```yaml
url: http://host.docker.internal:8428/select/0/prometheus
```

Example:

```yaml
url: http://10.20.108.113:8428/select/0/prometheus
```

This URL must be reachable from the Grafana container.

## Makefile Usage

The Makefile provides simple targets to bring the stack up and down.

- Show help:

  ```bash
  make help
  ```

- Start services (`docker compose up -d`):

  ```bash
  make up
  ```

- Stop services:

  ```bash
  make down
  ```

## Access

- Grafana UI: `http://localhost:3000`
  - Default admin credentials: `admin` / `admin`
- Loki API: `http://localhost:3100`

## Timezone

Containers are configured with `TZ=Asia/Tokyo`. Adjust in `compose.yaml` if needed.

## Alerting

Alerting is enabled and provisioned via:

- `grafana/provisioning/alerting/alert_rules.yaml`
- `grafana/provisioning/alerting/alert_resources.yaml`

### Alert rules

Alert rules are defined in `alert_rules.yaml`. The instructions for adding and modifying alert rules are provided in [Adding or modifying alert rules](#adding-or-modifying-alert-rules).

### Contact Points

Sample configurations for the Slack contact point is provided in `alert_resources.yaml`. Replace the placeholder Slack Webhook URL with your actual Slack Webhook URL:

```yaml
url: https://hooks.slack.com/services/XXXX/YYYY/ZZZZ
```

The example Slack message template provided in `alert_resources.yaml` sends notifications to a Slack channel with following details:

- Title displaying alert status and name, including a link to alerting rules in Grafana
- Summary
- Last value of the metrics
- Datetime when the alert was fired or resolved
- Panel URL for quick access to the dashboard in Grafana

Note:

- If you need to receive alerts from a network different from the one where Grafana is hosted, the following changes are required in `alert_resources.yaml` and `compose.yaml` to ensure that the links work correctly:
  - Replace `localhost` with the actual IP address of the Grafana host in the Panel URL field of the Slack message template in `alert_resources.yaml`.

  ```yaml
  # When your Grafana UI is accessible at http://grafana_host_ip:3000
  Panel URL: http://grafana_host_ip:3000/d/{{ .Annotations.dashboardUid }}?viewPanel={{ .Annotations.panelId}}
  ```

  - Update `GF_SERVER_DOMAIN` and `GF_SERVER_ROOT_URL` environment variables in `compose.yaml` to reflect the Grafana host IP. These variables allow Grafana to generate correct URLs within alert notifications.

  ```yaml
  # When your Grafana UI is accessible at http://grafana_host_ip:3000
  environment:
    - GF_SERVER_DOMAIN=grafana_host_ip
    - GF_SERVER_ROOT_URL=http://grafana_host_ip:3000
  ```

### Notification policies

Sample notification policies are defined in `alert_resources.yaml`. Replace placeholder receiver name `RECEIVER_NAME` with the one defined in the contact points section:

```yaml
policies:
  - orgId: 1
    receiver: RECEIVER_NAME
```

## Adding or modifying alert rules

Alert rules can be managed in two ways:

- Through the Grafana UI
- By editing the provisioning file `alert_rules.yaml` directly

**Important notes**:

- Alert rules created or modified via the Grafana UI are **NOT** persisted to the `alert_rules.yaml` file automatically, and will be lost if the Grafana container is recreated. To persist alert rules, you need to export them manually and replicate them in `alert_rules.yaml`.

- Alert rules configured by the provisioning file (`alert_rules.yaml`) are **NOT** editable via the Grafana UI. If you want to modify those rules, you need to edit the `alert_rules.yaml` file directly.

### Adding alert rules

#### Through Grafana UI

1. Access Grafana UI (default: `http://localhost:3000`) and log in.
2. Navigate to `Alerting` > `Alert rules` from the menu.
3. Click `New alert rule` to create a new alert rule.
4. Configure the alert rule (see also [Grafana documentation](https://grafana.com/docs/grafana/v12.3/alerting/alerting-rules/create-grafana-managed-rule/) for details):
   - Give the alert rule a name.
   - Select the datasource (e.g., VictoriaMetricsCluster) and metrics to configure the alert condition.
   - Set the alert condition and evaluation behavior.
   - Define notification settings (contact points, message templates, etc.).
5. Save the alert rule.

#### Editing provisioning file

1. Add new alert rule definitions to `alert_rules.yaml`. See also [Grafana documentation](https://grafana.com/docs/grafana/v12.3/alerting/set-up/provision-alerting-resources/file-provisioning/) for the format of alert rule definitions.
2. Save the file.
3. Restart Grafana: `docker compose restart grafana`

### Modifying alert rules

#### Through Grafana UI

1. Access Grafana UI (default: `http://localhost:3000`) and log in.
2. Navigate to `Alerting` > `Alert rules` from the menu.
3. Find the alert rule you want to modify and click `Edit`.
4. Make the necessary changes to the alert rule configuration. See also [Grafana documentation](https://grafana.com/docs/grafana/v12.3/alerting/alerting-rules/create-grafana-managed-rule/) for details
5. Save the alert rule.

#### Editing provisioning file

1. Modify the alert rule definitions in `alert_rules.yaml`. See also [Grafana documentation](https://grafana.com/docs/grafana/v12.3/alerting/set-up/provision-alerting-resources/file-provisioning/) for the format of alert rule definitions.
2. Save the file.
3. Restart Grafana: `docker compose restart grafana`

## Troubleshooting

- If Grafana cannot connect to VictoriaMetrics:
  - Verify the `url` in `datasource.yml`
  - Ensure the host IP is reachable from Docker network
  - Restart Grafana: `docker compose restart grafana`

- If dashboards do not appear:
  - Confirm dashboards are copied to `/var/lib/grafana/dashboards` inside the container
  - Check `provider.yml` path: `/var/lib/grafana/dashboards`
  - Wait up to `updateIntervalSeconds` (default: 10s), or restart Grafana
