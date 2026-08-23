#!/usr/bin/env bash
set -euo pipefail

CONF=${1:-release}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

APP_NAME=ComradexMenu
DISPLAY_NAME="Comradex Menu"
BUNDLE_ID=${BUNDLE_ID:-com.nicosuave.comradex.menu}
MACOS_MIN_VERSION=14.0
MENU_BAR_APP=1
SIGNING_MODE=${SIGNING_MODE:-}
APP_IDENTITY=${APP_IDENTITY:-}
source "$ROOT/version.env"

ARCH_LIST=( ${ARCHES:-$(uname -m)} )
for ARCH in "${ARCH_LIST[@]}"; do
  swift build -c "$CONF" --arch "$ARCH"
done

APP="$ROOT/${APP_NAME}.app"
rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"

build_product_path() {
  echo ".build/$1-apple-macosx/$CONF/$APP_NAME"
}

BINARIES=()
for ARCH in "${ARCH_LIST[@]}"; do
  BINARY=$(build_product_path "$ARCH")
  if [[ ! -f "$BINARY" ]]; then
    echo "ERROR: Missing $ARCH binary at $BINARY" >&2
    exit 1
  fi
  BINARIES+=("$BINARY")
done
if [[ ${#BINARIES[@]} -gt 1 ]]; then
  lipo -create "${BINARIES[@]}" -output "$APP/Contents/MacOS/$APP_NAME"
else
  cp "${BINARIES[0]}" "$APP/Contents/MacOS/$APP_NAME"
fi
chmod +x "$APP/Contents/MacOS/$APP_NAME"

ACTUAL_ARCHES=$(lipo -archs "$APP/Contents/MacOS/$APP_NAME")
for ARCH in "${ARCH_LIST[@]}"; do
  if [[ " $ACTUAL_ARCHES " != *" $ARCH "* ]]; then
    echo "ERROR: packaged app is missing $ARCH (has: $ACTUAL_ARCHES)" >&2
    exit 1
  fi
done

cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>CFBundleName</key><string>${DISPLAY_NAME}</string>
  <key>CFBundleDisplayName</key><string>${DISPLAY_NAME}</string>
  <key>CFBundleIdentifier</key><string>${BUNDLE_ID}</string>
  <key>CFBundleExecutable</key><string>${APP_NAME}</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>NSPrincipalClass</key><string>NSApplication</string>
  <key>CFBundleShortVersionString</key><string>${MARKETING_VERSION}</string>
  <key>CFBundleVersion</key><string>${BUILD_NUMBER}</string>
  <key>LSMinimumSystemVersion</key><string>${MACOS_MIN_VERSION}</string>
  <key>LSUIElement</key><true/>
  <key>NSHighResolutionCapable</key><true/>
</dict></plist>
PLIST

if [[ "$SIGNING_MODE" == "adhoc" || -z "$APP_IDENTITY" ]]; then
  CODESIGN_ARGS=(--force --sign -)
else
  CODESIGN_ARGS=(--force --timestamp --options runtime --sign "$APP_IDENTITY")
fi
xattr -cr "$APP"
codesign "${CODESIGN_ARGS[@]}" "$APP"
echo "Created $APP (MENU_BAR_APP=$MENU_BAR_APP)"
