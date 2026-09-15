#!/bin/sh
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION="$(grep -m1 '^version' "$ROOT/Cargo.toml" | sed -E 's/.*"([^"]+)".*/\1/')"
ARCH="$(uname -m)"
DIST="$ROOT/dist.noindex"
APP="$DIST/Kafkamitter.app"
ZIP="$DIST/Kafkamitter-$VERSION-$ARCH.zip"

"$ROOT/scripts/bundle.sh" >/dev/null

rm -f "$ZIP"
ditto -c -k --sequesterRsrc --keepParent "$APP" "$ZIP"

echo "version: $VERSION"
echo "arch:    $ARCH"
echo "file:    $ZIP"
echo "size:    $(du -h "$ZIP" | cut -f1)"
echo "sha256:  $(shasum -a 256 "$ZIP" | cut -d' ' -f1)"
