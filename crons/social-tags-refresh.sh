#!/bin/bash
# /opt/blockpools/crons/social-tags-refresh.sh
# Rebuilds the user_tags_by_league table (🔥 Hot / 🔮 Sharp bettor tags).
# Called by /etc/cron.d/blockpools-social-tags-refresh every 20 minutes.
#
# Hot/Sharp only change when a game RESOLVES (infrequent), so a 20-min cadence is
# plenty. The per-side counts shown on markets are computed live by the read
# endpoint (60s TTL + on-trade invalidation), independent of this cron.

set -euo pipefail

# Load env (same pattern as the rest of your backend setup)
set -a
source /etc/blockpools/backend.env
set +a

BACKEND_URL="${BACKEND_URL:-https://api.blockpools.io}"
CRON_SECRET="${CRON_SECRET:?CRON_SECRET not set in /etc/blockpools/backend.env}"

echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] Running social-tags refresh..."

HTTP_STATUS=$(curl -s -o /tmp/social-tags-refresh-response.json -w "%{http_code}" \
  -X POST "${BACKEND_URL}/api/social-tags/refresh" \
  -H "Authorization: Bearer ${CRON_SECRET}" \
  -H "Content-Type: application/json" \
  --max-time 60)

RESPONSE=$(cat /tmp/social-tags-refresh-response.json)

if [ "$HTTP_STATUS" -eq 200 ]; then
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] OK — ${RESPONSE}"
else
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] FAILED (HTTP ${HTTP_STATUS}) — ${RESPONSE}" >&2
  exit 1
fi
