#!/usr/bin/env bash
# Prepare all test fixtures from full-size data in example_data/.
# Run from the repository root:
#   bash fixtures/scripts/prepare_fixtures.sh
#
# Prerequisites:
#   - uv (https://docs.astral.sh/uv/)
#   - osmium-tool (for OSM fixture)
#   - Full-size data files in example_data/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "=== Preparing BDOT10k fixture ==="
uv run "$SCRIPT_DIR/prepare_geoparquet_fixture.py" \
    "$REPO_ROOT/example_data/BDOT10k/OT_BUBD_A.parquet" \
    "$REPO_ROOT/fixtures/bdot10k.parquet"

echo ""
echo "=== Preparing EGIB fixture ==="
uv run "$SCRIPT_DIR/prepare_geoparquet_fixture.py" \
    "$REPO_ROOT/example_data/EGiB/0_budynki.parquet" \
    "$REPO_ROOT/fixtures/egib.parquet"

echo ""
echo "=== Preparing OSM fixture ==="
bash "$SCRIPT_DIR/prepare_osm_fixture.sh"

echo ""
echo "=== Verifying BDOT10k fixture ==="
uv run "$SCRIPT_DIR/verify_geoparquet_fixture.py" \
    "$REPO_ROOT/example_data/BDOT10k/OT_BUBD_A.parquet" \
    "$REPO_ROOT/fixtures/bdot10k.parquet"

echo ""
echo "=== Verifying EGIB fixture ==="
uv run "$SCRIPT_DIR/verify_geoparquet_fixture.py" \
    "$REPO_ROOT/example_data/EGiB/0_budynki.parquet" \
    "$REPO_ROOT/fixtures/egib.parquet"

echo ""
echo "=== Preparing update (v2) fixtures ==="
bash "$SCRIPT_DIR/prepare_update_fixtures.sh"

echo ""
echo "=== All fixtures prepared and verified ==="
