#!/bin/bash
# /opt/blockpools/crons/roi-refresh.sh
# Hits the backend endpoint to refresh the user_roi_snapshots table.
# Called by /etc/cron.d/blockpools-roi-refresh every 15 minutes.

set -euo pipefail

# Load env (same pattern as the rest of your backend setup)
set -a
source /etc/blockpools/backend.env
set +a

BACKEND_URL="${BACKEND_URL:-https://api.blockpools.io}"
CRON_SECRET="${CRON_SECRET:?CRON_SECRET not set in /etc/blockpools/backend.env}"

echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] Running ROI snapshot refresh..."

HTTP_STATUS=$(curl -s -o /tmp/roi-refresh-response.json -w "%{http_code}" \
  -X POST "${BACKEND_URL}/api/league-chat/refresh-roi" \
  -H "Authorization: Bearer ${CRON_SECRET}" \
  -H "Content-Type: application/json" \
  --max-time 30)

RESPONSE=$(cat /tmp/roi-refresh-response.json)

if [ "$HTTP_STATUS" -eq 200 ]; then
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] OK — ${RESPONSE}"
else
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] FAILED (HTTP ${HTTP_STATUS}) — ${RESPONSE}" >&2
  exit 1
fi