#!/bin/bash
# scripts/setup-crons.sh
# Run once on the VPS after pulling the repo to wire up cron jobs.
# Usage: sudo bash scripts/setup-crons.sh

set -euo pipefail

REPO_DIR="/opt/blockpools/backend"
CRON_DIR="/opt/blockpools/crons"
LOG_DIR="/var/log/blockpools"

echo "==> Creating cron script directory..."
mkdir -p "$CRON_DIR"
mkdir -p "$LOG_DIR"

echo "==> Copying cron scripts from repo..."
cp "$REPO_DIR/crons/roi-refresh.sh" "$CRON_DIR/roi-refresh.sh"
chmod +x "$CRON_DIR/roi-refresh.sh"

echo "==> Installing cron job to /etc/cron.d/..."
cp "$REPO_DIR/crons/blockpools-roi-refresh" /etc/cron.d/blockpools-roi-refresh
chmod 644 /etc/cron.d/blockpools-roi-refresh

echo "==> Verifying cron daemon is running..."
systemctl is-active cron || systemctl start cron

echo ""
echo "✓ Cron job installed. Verify with:"
echo "  cat /etc/cron.d/blockpools-roi-refresh"
echo "  tail -f /var/log/blockpools/roi-refresh.log"
echo ""
echo "Don't forget: add CRON_SECRET to /etc/blockpools/backend.env if not already there."
echo "  echo 'CRON_SECRET=\$(openssl rand -hex 32)' | sudo tee -a /etc/blockpools/backend.env"