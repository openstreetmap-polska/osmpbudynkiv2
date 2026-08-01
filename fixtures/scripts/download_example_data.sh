#!/usr/bin/env bash
# Download fresh full-size PRG/EGiB/BDOT10k/OSM snapshots into example_data/,
# suffixing each filename with today's date so repeated runs keep a dated
# history instead of silently overwriting the previous snapshot.
#
# Run from anywhere:
#   bash fixtures/scripts/download_example_data.sh
#
# Source URLs are kept in sync by hand with the [download_urls] section of
# example_config.toml -- update both places together if a source moves.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA_DIR="$REPO_ROOT/example_data"
DATE="$(date +%F)"

OSM_PBF_URL="https://download.openstreetmap.fr/extracts/europe/poland-latest.osm.pbf"
BDOT10K_URL="https://opendata.geoportal.gov.pl/bdot10k/schemat2021/GeoParquet/OT_BUBD_A.parquet"
EGIB_URL="https://opendata.geoportal.gov.pl/InneDane/latest_exports/eziudp_wfs/PARQUET/0_budynki.parquet"
PRG_URL="https://integracja.gugik.gov.pl/PRG/pobierz.php?adresy_zbiorcze_gml"

MAX_RETRIES=2

# download <url> <dest_path>
# Mirrors src/download.rs: skip if the destination already exists, otherwise
# retry with exponential backoff, cleaning up any partial file on failure.
download() {
    local url="$1"
    local dest="$2"

    if [[ -f "$dest" ]]; then
        echo "Already exists, skipping: $dest"
        return 0
    fi

    mkdir -p "$(dirname "$dest")"

    local attempt=1
    while (( attempt <= MAX_RETRIES )); do
        echo "Downloading (attempt $attempt/$MAX_RETRIES): $url"
        if curl --fail --location --show-error --progress-bar -o "$dest" "$url"; then
            return 0
        fi
        echo "Download failed: $url" >&2
        rm -f "$dest"
        if (( attempt < MAX_RETRIES )); then
            local delay=$(( 2 ** attempt ))
            echo "Retrying in ${delay}s..."
            sleep "$delay"
        fi
        (( attempt++ ))
    done

    echo "Failed to download $url after $MAX_RETRIES attempts" >&2
    return 1
}

download "$OSM_PBF_URL" "$DATA_DIR/OSM/poland-${DATE}.osm.pbf"
download "$BDOT10K_URL" "$DATA_DIR/BDOT10k/OT_BUBD_A_${DATE}.parquet"
download "$EGIB_URL" "$DATA_DIR/EGiB/0_budynki_${DATE}.parquet"
download "$PRG_URL" "$DATA_DIR/PRG/PRG-punkty_adresowe_${DATE}.zip"

echo ""
echo "=== All downloads complete (dated $DATE) ==="
