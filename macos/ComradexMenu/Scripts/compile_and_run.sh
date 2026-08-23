#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
APP_NAME=ComradexMenu
APP="$ROOT/${APP_NAME}.app"

pkill -f "${APP_NAME}.app/Contents/MacOS/${APP_NAME}" 2>/dev/null || true
if [[ "${1:-}" == "--test" ]]; then
  (cd "$ROOT" && swift test)
fi
SIGNING_MODE=adhoc MENU_BAR_APP=1 "$ROOT/Scripts/package_app.sh" release
open "$APP"

for _ in {1..10}; do
  if pgrep -f "${APP_NAME}.app/Contents/MacOS/${APP_NAME}" >/dev/null 2>&1; then
    echo "OK: $APP_NAME is running."
    exit 0
  fi
  sleep 0.4
done
echo "ERROR: $APP_NAME exited immediately." >&2
exit 1
