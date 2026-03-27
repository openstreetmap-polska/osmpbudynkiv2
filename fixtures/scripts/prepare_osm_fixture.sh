#!/usr/bin/env bash
# Prepare OSM fixture by extracting specific objects from the Poland PBF extract.
# Run from the repository root:
#   bash fixtures/scripts/prepare_osm_fixture.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

osmium getid \
    "$REPO_ROOT/example_data/OSM/poland-latest.osm.pbf" \
    -i "$SCRIPT_DIR/osm_object_ids.txt" \
    -o "$REPO_ROOT/fixtures/osm.pbf" \
    --overwrite
