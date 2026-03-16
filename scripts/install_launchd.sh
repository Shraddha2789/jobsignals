#!/usr/bin/env bash
# install_launchd.sh — install JobSignals API as a macOS LaunchAgent.
# Runs the FastAPI server at login, restarts on crash, survives reboots.
#
# Usage:  bash scripts/install_launchd.sh [install|uninstall]

set -euo pipefail

ACTION="${1:-install}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="$REPO_DIR/.venv/bin/python"
PLIST_LABEL="com.jobsignals.api"
PLIST_DEST="$HOME/Library/LaunchAgents/${PLIST_LABEL}.plist"
LOG_DIR="$REPO_DIR/logs"

if [[ "$ACTION" == "uninstall" ]]; then
    launchctl unload "$PLIST_DEST" 2>/dev/null || true
    rm -f "$PLIST_DEST"
    echo "✓ JobSignals API launchd service removed"
    exit 0
fi

mkdir -p "$LOG_DIR"

cat > "$PLIST_DEST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
    "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${PLIST_LABEL}</string>

    <key>ProgramArguments</key>
    <array>
        <string>${PYTHON}</string>
        <string>-m</string>
        <string>api.main</string>
    </array>

    <key>WorkingDirectory</key>
    <string>${REPO_DIR}</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin</string>
    </dict>

    <!-- Restart automatically if the process exits -->
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>

    <!-- Start immediately when loaded / at login -->
    <key>RunAtLoad</key>
    <true/>

    <!-- Wait 5 s before restarting on crash -->
    <key>ThrottleInterval</key>
    <integer>5</integer>

    <key>StandardOutPath</key>
    <string>${LOG_DIR}/api.log</string>

    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/api.error.log</string>
</dict>
</plist>
PLIST

# Unload first if already running (idempotent reinstall)
launchctl unload "$PLIST_DEST" 2>/dev/null || true
launchctl load -w "$PLIST_DEST"

echo "✓ JobSignals API installed as launchd service"
echo "  Auto-starts at login, restarts on crash"
echo "  Logs → $LOG_DIR/api.log"
echo "  Stop:    launchctl unload $PLIST_DEST"
echo "  Start:   launchctl load   $PLIST_DEST"
echo "  Status:  launchctl list | grep jobsignals"
