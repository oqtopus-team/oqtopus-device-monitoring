#!/usr/bin/env bash

# ------------ configurations ------------
export TARGET_JOBS_REGEX='cryo-metrics|quel1-ping'
# ------------ configurations ------------

# ------------
set -Eeuo pipefail

YEAR=$(date +%G)
WEEK=$((10#$(date +%V)))
VALUE="${YEAR}${WEEK}w"

cat > /data/relabel_weekly.yml <<EOF
- if: '{job=~"${TARGET_JOBS_REGEX}"}'
  target_label: scrapedweek
  replacement: "${VALUE}"
EOF
# notify vmagent to reload config
curl --noproxy '*' -fsS -X POST http://vmagent:8429/-/reload
echo "updated scrapedweek=${VALUE} and reloaded"
