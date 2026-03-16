#!/usr/bin/env bash
# install_classify_cron.sh — weekly LLM title classification cron.
# Runs every Sunday at 07:00, classifies any NULL/Other title_family postings.
#
# Usage:  bash scripts/install_classify_cron.sh [install|uninstall]

set -euo pipefail

ACTION="${1:-install}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "$ACTION" == "uninstall" ]]; then
    ( crontab -l 2>/dev/null | grep -v "classify_titles" ) | crontab -
    echo "✓ Weekly classify cron removed"
    exit 0
fi

PYTHON="$REPO_DIR/.venv/bin/python"
LOG_FILE="$REPO_DIR/logs/classify.log"
CRON_LINE="0 7 * * 0 cd $REPO_DIR && $PYTHON -m scripts.classify_titles >> $LOG_FILE 2>&1"

mkdir -p "$REPO_DIR/logs"
( crontab -l 2>/dev/null | grep -v "classify_titles"; echo "$CRON_LINE" ) | crontab -

echo "✓ Weekly classify cron installed: every Sunday at 07:00"
echo "  Logs → $LOG_FILE"
echo "  View cron: crontab -l"
