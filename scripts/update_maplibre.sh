#!/usr/bin/env bash
# Download and unpack the MapLibre GL JS files vendored into web/vendor/maplibre-gl/
# (see web/app.js and web/index.html for where they're loaded from).
#
# Run from anywhere, optionally pinning a version:
#   bash scripts/update_maplibre.sh          # latest release
#   bash scripts/update_maplibre.sh v6.4.0    # specific tag

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENDOR_DIR="$REPO_ROOT/web/vendor/maplibre-gl"

VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
    VERSION="$(curl --fail --silent --show-error --location \
        https://api.github.com/repos/maplibre/maplibre-gl-js/releases/latest \
        | grep -m1 '"tag_name"' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')"
    echo "No version given, using latest: $VERSION"
fi

ZIP_URL="https://github.com/maplibre/maplibre-gl-js/releases/download/${VERSION}/dist.zip"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

echo "Downloading $ZIP_URL"
curl --fail --location --show-error --progress-bar -o "$TMP_DIR/dist.zip" "$ZIP_URL"

unzip -q -o "$TMP_DIR/dist.zip" -d "$TMP_DIR"

# Only the production ESM bundle, its shared chunk, the worker it fetches by
# relative URL at runtime, and the stylesheet -- not the -dev builds or .map
# files. See web/app.js's comment above the import for why each is needed.
FILES=(
    maplibre-gl.mjs
    maplibre-gl-shared.mjs
    maplibre-gl-worker.mjs
    maplibre-gl.css
)

mkdir -p "$VENDOR_DIR"
for f in "${FILES[@]}"; do
    cp "$TMP_DIR/dist/$f" "$VENDOR_DIR/$f"
    echo "Updated $VENDOR_DIR/$f"
done

echo ""
echo "=== MapLibre GL JS updated to $VERSION ==="
echo "Diff web/vendor/maplibre-gl/, then verify in a browser before committing."
