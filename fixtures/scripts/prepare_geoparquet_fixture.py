#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pyarrow>=19",
#     "shapely>=2",
#     "pyproj>=3",
# ]
# ///
"""Filter GeoParquet files to a bounding geometry, preserving all parquet metadata (CRS, encoding, etc.).

Usage:
    uv run fixtures/scripts/prepare_geoparquet_fixture.py <input.parquet> <output.parquet> [--wkt POLYGON|--geojson '{"type":"Polygon",...}']

If no geometry filter is given, reads WKT from bbox.txt next to this script.
The filter geometry is assumed to be in EPSG:4326 (WGS84) and is reprojected to match the file's CRS.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely import wkb
from shapely.ops import transform


def read_geo_metadata(schema: pa.Schema) -> dict:
    """Extract GeoParquet 'geo' metadata from the Arrow schema."""
    raw = schema.metadata.get(b"geo")
    if raw is None:
        sys.exit("ERROR: No 'geo' key in parquet metadata — not a GeoParquet file.")
    return json.loads(raw)


def get_crs_authority(geo_meta: dict, column: str) -> str | None:
    """Return 'EPSG:code' if available in the geo metadata for the given column."""
    col_meta = geo_meta.get("columns", {}).get(column, {})
    crs = col_meta.get("crs")
    if crs is None:
        return None
    # CRS stored as projjson — look for id.authority + id.code
    if isinstance(crs, str):
        crs = json.loads(crs)
    crs_id = crs.get("id", {})
    authority = crs_id.get("authority")
    code = crs_id.get("code")
    if authority and code:
        return f"{authority}:{code}"
    return None


def reproject_filter_geom(filter_geom: shapely.Geometry, target_crs: str) -> shapely.Geometry:
    """Reproject filter geometry from EPSG:4326 to the target CRS."""
    if target_crs.upper() == "EPSG:4326":
        return filter_geom
    transformer = pyproj.Transformer.from_crs("EPSG:4326", target_crs, always_xy=True)
    return transform(transformer.transform, filter_geom)


def parse_filter_geometry(args: argparse.Namespace) -> shapely.Geometry:
    """Parse the filter geometry from CLI args or fall back to bbox.txt."""
    if args.wkt:
        return shapely.from_wkt(args.wkt)
    if args.geojson:
        return shapely.from_geojson(args.geojson)
    # Fall back to bbox.txt
    bbox_path = Path(__file__).parent / "bbox.txt"
    if not bbox_path.exists():
        sys.exit("ERROR: No --wkt or --geojson given and bbox.txt not found.")
    for line in bbox_path.read_text().splitlines():
        line = line.strip()
        if line and line.startswith("POLYGON"):
            return shapely.from_wkt(line)
    sys.exit("ERROR: Could not find WKT in bbox.txt.")


def filter_geoparquet(input_path: str, output_path: str, filter_geom: shapely.Geometry) -> None:
    parquet_file = pq.ParquetFile(input_path)
    schema = parquet_file.schema_arrow
    geo_meta = read_geo_metadata(schema)

    geom_column = geo_meta["primary_column"]
    crs_authority = get_crs_authority(geo_meta, geom_column)
    print(f"Geometry column: {geom_column}")
    print(f"CRS: {crs_authority or 'unknown'}")

    if crs_authority:
        filter_geom_proj = reproject_filter_geom(filter_geom, crs_authority)
    else:
        filter_geom_proj = filter_geom

    shapely.prepare(filter_geom_proj)

    table = parquet_file.read()
    geom_col_index = schema.get_field_index(geom_column)

    # Parse WKB geometries and test intersection
    geom_arrays = table.column(geom_col_index)
    mask = []
    for wkb_val in geom_arrays:
        if wkb_val.as_py() is None:
            mask.append(False)
            continue
        geom = wkb.loads(wkb_val.as_py())
        mask.append(shapely.intersects(filter_geom_proj, geom))

    mask_array = pa.array(mask, type=pa.bool_())
    filtered = table.filter(mask_array)

    print(f"Rows: {table.num_rows} -> {filtered.num_rows}")

    if filtered.num_rows == 0:
        print("WARNING: No rows matched the filter geometry.")

    # Update bbox in geo metadata if present
    geo_meta_updated = update_geo_bbox(geo_meta, geom_column, filtered, geom_col_index)

    # Rebuild schema metadata — preserve all original keys, update 'geo'
    new_metadata = dict(schema.metadata)
    new_metadata[b"geo"] = json.dumps(geo_meta_updated).encode()
    new_schema = filtered.schema.with_metadata(new_metadata)
    filtered = filtered.cast(new_schema)

    # Write with the same row group configuration
    pq.write_table(
        filtered,
        output_path,
        compression="snappy",
        write_statistics=True,
    )
    print(f"Written: {output_path}")


def update_geo_bbox(geo_meta: dict, geom_column: str, table: pa.Table, geom_col_index: int) -> dict:
    """Recompute the bounding box in geo metadata to match the filtered data."""
    if table.num_rows == 0:
        return geo_meta

    geom_arrays = table.column(geom_col_index)
    all_bounds = []
    for wkb_val in geom_arrays:
        if wkb_val.as_py() is None:
            continue
        geom = wkb.loads(wkb_val.as_py())
        all_bounds.append(geom.bounds)  # (minx, miny, maxx, maxy)

    if all_bounds:
        minx = min(b[0] for b in all_bounds)
        miny = min(b[1] for b in all_bounds)
        maxx = max(b[2] for b in all_bounds)
        maxy = max(b[3] for b in all_bounds)

        col_meta = geo_meta.get("columns", {}).get(geom_column, {})
        col_meta["bbox"] = [minx, miny, maxx, maxy]
        geo_meta.setdefault("columns", {})[geom_column] = col_meta

    return geo_meta


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", help="Input GeoParquet file")
    parser.add_argument("output", help="Output GeoParquet file")
    parser.add_argument("--wkt", help="Filter geometry as WKT (EPSG:4326)")
    parser.add_argument("--geojson", help="Filter geometry as GeoJSON string (EPSG:4326)")
    args = parser.parse_args()

    filter_geom = parse_filter_geometry(args)
    print(f"Filter geometry: {filter_geom.geom_type} with {filter_geom.bounds}")

    filter_geoparquet(args.input, args.output, filter_geom)


if __name__ == "__main__":
    main()
