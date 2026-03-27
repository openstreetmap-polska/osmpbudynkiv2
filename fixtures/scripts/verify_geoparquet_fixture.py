#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pyarrow>=19",
# ]
# ///
"""Verify that a GeoParquet fixture preserves schema, encoding, CRS, and all metadata from the original.

Usage:
    uv run fixtures/scripts/verify_geoparquet_fixture.py <original.parquet> <fixture.parquet>
"""

from __future__ import annotations

import argparse
import json
import sys

import pyarrow.parquet as pq


def normalize_crs(crs):
    """Parse CRS value — may be a dict or a JSON-encoded string."""
    if isinstance(crs, str):
        return json.loads(crs)
    return crs


def verify(original_path: str, fixture_path: str) -> bool:
    ok = True

    def check(condition: bool, msg: str):
        nonlocal ok
        if condition:
            print(f"  OK: {msg}")
        else:
            print(f"  FAIL: {msg}")
            ok = False

    o = pq.ParquetFile(original_path)
    f = pq.ParquetFile(fixture_path)

    print(f"Original: {original_path} ({o.metadata.num_rows} rows)")
    print(f"Fixture:  {fixture_path} ({f.metadata.num_rows} rows)")
    print()

    # Column names
    check(
        o.schema_arrow.names == f.schema_arrow.names,
        f"Column names match ({len(o.schema_arrow.names)} columns)",
    )

    # Column types
    for i, (ot, ft) in enumerate(zip(o.schema_arrow.types, f.schema_arrow.types)):
        check(ot == ft, f"Column '{o.schema_arrow.names[i]}' type: {ot}")

    # geo metadata presence
    o_meta = o.schema_arrow.metadata
    f_meta = f.schema_arrow.metadata
    check(b"geo" in o_meta, "Original has 'geo' metadata")
    check(b"geo" in f_meta, "Fixture has 'geo' metadata")

    if b"geo" not in o_meta or b"geo" not in f_meta:
        return ok

    o_geo = json.loads(o_meta[b"geo"])
    f_geo = json.loads(f_meta[b"geo"])

    # Primary column
    check(
        o_geo.get("primary_column") == f_geo.get("primary_column"),
        f"Primary column: {f_geo.get('primary_column')}",
    )

    # Encoding
    geom_col = o_geo["primary_column"]
    o_enc = o_geo["columns"][geom_col].get("encoding")
    f_enc = f_geo["columns"][geom_col].get("encoding")
    check(o_enc == f_enc, f"Encoding: {f_enc}")

    # CRS (deep comparison)
    o_crs = normalize_crs(o_geo["columns"][geom_col].get("crs", {}))
    f_crs = normalize_crs(f_geo["columns"][geom_col].get("crs", {}))
    check(o_crs == f_crs, f"CRS projjson fully identical")

    crs_id = o_crs.get("id", {})
    if crs_id:
        print(f"       CRS authority: {crs_id.get('authority')}:{crs_id.get('code')}")

    # Row count sanity
    check(f.metadata.num_rows > 0, f"Fixture is not empty ({f.metadata.num_rows} rows)")
    check(
        f.metadata.num_rows <= o.metadata.num_rows,
        f"Fixture has fewer rows than original ({f.metadata.num_rows} <= {o.metadata.num_rows})",
    )

    return ok


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("original", help="Original GeoParquet file")
    parser.add_argument("fixture", help="Fixture GeoParquet file")
    args = parser.parse_args()

    print()
    ok = verify(args.original, args.fixture)
    print()

    if ok:
        print("ALL CHECKS PASSED")
    else:
        print("SOME CHECKS FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
