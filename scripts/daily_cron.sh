#!/usr/bin/env bash
# daily_cron.sh — run JobSignals ingestion + aggregations once.
# Installed by: make cron-install
# Schedule: daily at 06:00 local time (configurable via CRON_HOUR env var)

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="$REPO_DIR/.venv/bin/python"
LOG_FILE="$REPO_DIR/logs/cron.log"

mkdir -p "$REPO_DIR/logs"

echo "$(date '+%Y-%m-%d %H:%M:%S') — Starting daily ingestion" >> "$LOG_FILE"

cd "$REPO_DIR"
# shellcheck disable=SC1091
source .env 2>/dev/null || true

"$PYTHON" -m scripts.ingest_real >> "$LOG_FILE" 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') — Done" >> "$LOG_FILE"
