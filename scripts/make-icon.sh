#!/bin/sh
# Builds the macOS app icon from the SVG source.
# Needs rsvg-convert: brew install librsvg
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SVG="$ROOT/assets/kafkamitter-icon.svg"
ICONSET="$(mktemp -d)/Kafkamitter.iconset"
ICNS="$ROOT/assets/Kafkamitter.icns"

mkdir -p "$ICONSET"
for spec in "16 icon_16x16" "32 icon_16x16@2x" "32 icon_32x32" "64 icon_32x32@2x" \
            "128 icon_128x128" "256 icon_128x128@2x" "256 icon_256x256" \
            "512 icon_256x256@2x" "512 icon_512x512" "1024 icon_512x512@2x"; do
    size="${spec% *}"
    name="${spec#* }"
    rsvg-convert -w "$size" -h "$size" "$SVG" -o "$ICONSET/$name.png"
done

iconutil -c icns "$ICONSET" -o "$ICNS"
rm -rf "$(dirname "$ICONSET")"
echo "wrote $ICNS ($(du -h "$ICNS" | cut -f1))"
