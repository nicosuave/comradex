#!/usr/bin/env bash
# Builds the iPhone app and widget and installs them on a connected device.
#
#   Scripts/install.sh                 # first paired iPhone
#   DEVICE=<udid> Scripts/install.sh       # from `xcrun devicectl list devices`
#
# Signing uses automatic provisioning for the team of the local Apple Development
# certificate (override with TEAM_ID). Provisioning authenticates with an App Store
# Connect API key when ASC_KEY_ID and ASC_ISSUER_ID are set (the key file defaults to
# ~/.appstoreconnect/private_keys/AuthKey_<id>.p8, where altool looks), otherwise
# with the account signed in to Xcode. Either needs permission to register the
# device and bundle IDs. COMRADEX_USAGE_SERVER overrides the Mac URL baked into the app,
# which defaults to this Mac's tailnet name on port 4363.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

if [[ -z "${TEAM_ID:-}" ]]; then
  IDENTITY=$(security find-identity -v -p codesigning | sed -n 's/.*"\(Apple Development: [^"]*\)".*/\1/p' | head -n 1)
  if [[ -z "$IDENTITY" ]]; then
    echo "ERROR: No Apple Development certificate found; set TEAM_ID" >&2
    exit 1
  fi
  TEAM_ID=$(security find-certificate -c "$IDENTITY" -p \
    | openssl x509 -noout -subject -nameopt multiline \
    | sed -n 's/^ *organizationalUnitName *= *//p' | head -n 1)
fi

if [[ -z "${DEVICE:-}" ]]; then
  DEVICES_JSON=$(mktemp)
  xcrun devicectl list devices --quiet --json-output "$DEVICES_JSON"
  DEVICE=$(jq -r '[.result.devices[] | select(.hardwareProperties.deviceType == "iPhone"
      and .hardwareProperties.reality == "physical"
      and .connectionProperties.tunnelState != "unavailable")][0].hardwareProperties.udid // empty' "$DEVICES_JSON")
  rm -f "$DEVICES_JSON"
  if [[ -z "$DEVICE" ]]; then
    echo "ERROR: No reachable iPhone. Unlock it on the same network as this Mac, or set DEVICE." >&2
    exit 1
  fi
fi

# The Mac this phone reads usage from: this Mac's tailnet name, unless overridden.
if [[ -z "${COMRADEX_USAGE_SERVER:-}" ]]; then
  MAC_NAME=$(tailscale status --json | jq -r '.Self.DNSName // empty' | sed 's/\.$//')
  if [[ -z "$MAC_NAME" ]]; then
    echo "ERROR: Could not read this Mac's Tailscale name; set COMRADEX_USAGE_SERVER" >&2
    exit 1
  fi
  COMRADEX_USAGE_SERVER="http://${MAC_NAME}:4363"
fi

AUTH_ARGS=()
if [[ -n "${ASC_KEY_ID:-}" ]]; then
  AUTH_ARGS=(
    -authenticationKeyID "$ASC_KEY_ID"
    -authenticationKeyIssuerID "${ASC_ISSUER_ID:?set ASC_ISSUER_ID with ASC_KEY_ID}"
    -authenticationKeyPath "${ASC_KEY_PATH:-$HOME/.appstoreconnect/private_keys/AuthKey_${ASC_KEY_ID}.p8}"
  )
fi

xcodegen generate --quiet
xcodebuild -project ComradexPhone.xcodeproj -scheme ComradexPhone -configuration Release \
  -destination "platform=iOS,id=$DEVICE" -derivedDataPath build \
  -allowProvisioningUpdates -allowProvisioningDeviceRegistration ${AUTH_ARGS[@]+"${AUTH_ARGS[@]}"} DEVELOPMENT_TEAM="$TEAM_ID" \
  COMRADEX_USAGE_SERVER="$COMRADEX_USAGE_SERVER" -quiet build
xcrun devicectl device install app --device "$DEVICE" build/Build/Products/Release-iphoneos/ComradexPhone.app
echo "Installed Comradex on $DEVICE (team $TEAM_ID), reading $COMRADEX_USAGE_SERVER"
