# Building Comparison Design

## Context

With OSM, BDOT10k, and EGIB building data imported into DuckDB (all in EPSG:4326 with RTREE spatial indexes), we need to compare government building datasets against OSM to identify buildings missing from OSM (import candidates) and visualize match status on a map.

Data volumes: ~17.9M OSM buildings, ~16.3M BDOT10k buildings, ~17.8M EGIB buildings.

## Decisions

- **Two independent comparisons:** BDOT10k vs OSM and EGIB vs OSM, not merged.
- **Matching logic:** Centroid containment — a gov building is "matched" if its centroid falls inside any OSM building polygon. Fast, handles most real-world cases where buildings roughly align.
- **Direction:** Gov building centroid checked against OSM polygons (not the reverse). Answers "is this gov building already represented in OSM?"
- **Deduplication:** Lateral join with LIMIT 1 picks one OSM match per gov building. Multiple overlapping OSM buildings covering the same centroid is rare.
- **Full recompute only:** No incremental updates for now. Cell-based partitioning (H3/QuadKey/XYZ) deferred to a future iteration.
- **Pure SQL approach:** The comparison runs entirely as DuckDB SQL queries, leveraging RTREE indexes. No Rust-side spatial logic.

## Result Tables

```sql
CREATE TABLE IF NOT EXISTS bdot10k_comparison (
    lokalnyid VARCHAR,          -- BDOT10k building identifier
    matched_osm_id BIGINT,      -- NULL if no match
    matched_osm_type VARCHAR,   -- NULL if no match
    matched BOOLEAN             -- convenience column for filtering/map styling
);

CREATE TABLE IF NOT EXISTS egib_comparison (
    id_budynku VARCHAR,         -- EGIB building identifier
    matched_osm_id BIGINT,
    matched_osm_type VARCHAR,
    matched BOOLEAN
);
```

Geometry is not duplicated — join back to the source table when needed for rendering. Keeps comparison tables small and fast to recompute.

## Comparison Query

```sql
CREATE TABLE bdot10k_comparison AS
SELECT
    b.LOKALNYID AS lokalnyid,
    match.osm_id AS matched_osm_id,
    match.osm_type AS matched_osm_type,
    match.osm_id IS NOT NULL AS matched
FROM bdot10k_buildings b
LEFT JOIN LATERAL (
    SELECT osm.osm_id, osm.osm_type
    FROM osm_buildings osm
    WHERE ST_Contains(osm.geom, ST_Centroid(b.geom))
    LIMIT 1
) match ON TRUE;
```

Same pattern for EGIB with `id_budynku` instead of `LOKALNYID`.

The lateral join stops at the first match per gov building — no need to find all matches and deduplicate.

## CLI

```bash
cargo run -- compare buildings          # compare both BDOT10k and EGIB vs OSM
cargo run -- compare buildings bdot10k  # compare only BDOT10k vs OSM
cargo run -- compare buildings egib     # compare only EGIB vs OSM
```

After comparison, log stats: total count, matched count, unmatched count, duration.

## Code Structure

- `src/compare/mod.rs` — dispatches compare subcommands
- `src/compare/buildings.rs` — comparison logic (SQL execution + logging)
- `src/cli.rs` — add `Compare` command variant with `BuildingsSource` subcommand
- `src/main.rs` — wire up the new command
- `src/db.rs` — add comparison tables to schema (with `IF NOT EXISTS`)

## Testing

Integration test using existing fixture files (`fixtures/osm.pbf`, `fixtures/bdot10k.parquet`, `fixtures/egib.parquet`) which all cover the same Warsaw neighborhood.

Test assertions:
- Total comparison row count equals source table row count
- Some buildings are matched, some are unmatched
- At least one known BDOT10k/EGIB building matches a known OSM building

## Future Considerations

- **Incremental updates:** Assign spatial cell IDs (H3/QuadKey/XYZ) to buildings, maintain a dirty-cell queue, re-compare only affected cells after source updates.
- **Overlap threshold:** If centroid containment proves too coarse, switch to area overlap ratio (`ST_Area(ST_Intersection) / ST_Area(gov_building) >= threshold`).
- **Performance:** If the lateral join is slow at scale, pre-compute centroids into a column with its own RTREE index, or batch the comparison spatially.
