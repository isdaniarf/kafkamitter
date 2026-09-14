#!/bin/sh
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
APP_NAME="Kafkamitter"
BUNDLE_ID="dev.fithrantyo.kafkamitter"
VERSION="$(grep -m1 '^version' "$ROOT/Cargo.toml" | sed -E 's/.*"([^"]+)".*/\1/')"
DIST="$ROOT/dist"
APP="$DIST/$APP_NAME.app"

if [ "${KAFKAMITTER_PRECOMPILED_SHADERS:-0}" = "1" ]; then
    cargo build --release --no-default-features --manifest-path "$ROOT/Cargo.toml"
else
    cargo build --release --manifest-path "$ROOT/Cargo.toml"
fi

rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
cp "$ROOT/target/release/kafkamitter" "$APP/Contents/MacOS/kafkamitter"
cp "$ROOT/assets/Kafkamitter.icns" "$APP/Contents/Resources/Kafkamitter.icns"

cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>
    <string>$APP_NAME</string>
    <key>CFBundleDisplayName</key>
    <string>$APP_NAME</string>
    <key>CFBundleIdentifier</key>
    <string>$BUNDLE_ID</string>
    <key>CFBundleExecutable</key>
    <string>kafkamitter</string>
    <key>CFBundleIconFile</key>
    <string>Kafkamitter</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleVersion</key>
    <string>$VERSION</string>
    <key>CFBundleShortVersionString</key>
    <string>$VERSION</string>
    <key>LSMinimumSystemVersion</key>
    <string>13.0</string>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>NSSupportsAutomaticGraphicsSwitching</key>
    <true/>
</dict>
</plist>
PLIST

codesign --force --sign - "$APP"
du -sh "$APP" | awk '{print "bundle size: " $1}'
ls -l "$APP/Contents/MacOS/kafkamitter" | awk '{printf "binary size: %.1f MB\n", $5/1048576}'
echo "created $APP"
