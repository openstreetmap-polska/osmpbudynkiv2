# Feasibility and Architecture Design

## Purpose

Design for osmpbudynkiv2 — a rewrite of [gugik2osm](https://github.com/openstreetmap-polska/gugik2osm) that compares Polish government registry data (addresses, buildings) with OpenStreetMap data and generates importable data packages for JOSM. Single Rust binary with embedded DuckDB.

## Phasing

### Phase 1 — Import + Updates

All data ingestion and incremental update capabilities. CLI-only, no HTTP.

**Import commands:**
- `import osm` — download Poland PBF extract (from OSM France), read raw data using DuckDB's `ST_ReadOSM` function. `ST_ReadOSM` returns raw OSM elements (kind, id, tags, lat/lon for nodes, refs array for ways/relations) — it does NOT resolve geometry. Geometry construction (building polygons from way node refs, resolving relations/multipolygons) must be done as a post-processing step (see OSM Geometry Construction below). Buildings can be ways or relations (multipolygons, polygons with inner rings). Addresses can be nodes or ways — TBD whether to compute centroids for way-addresses and treat all as points. PBF metadata (replication sequence number) may need Rust-side PBF header parsing if `ST_ReadOSM` doesn't expose it.
- `import prg` — download PRG ZIP, parse with `prg_convert` library (git dependency from `github.com/ttomasz/prg_convert/`, needs to be usable as a library — may require adaptation if currently structured as a binary), insert addresses into DuckDB
- `import bdot10k` — download BDOT10k GeoParquet ZIP, extract relevant file. Known issue: the GeoParquet file has incorrect CRS declaration, which may prevent DuckDB from reading it correctly. Fallback: parse using georust crates (`geoparquet`, `arrow-rs`) and provide Arrow RecordBatches to DuckDB.
- `import egib` — download EGIB GeoParquet (2.4 GB, ~17.8M buildings), import via DuckDB
- `import full` — run all of the above

**Update commands:**
- `update osm` — apply OSM minutely replication diffs (see OSM Replication Consumer below)
- `update prg`, `update bdot10k`, `update egib` — download latest dataset, diff against currently loaded data, and apply changes incrementally (similar approach to OSM updates — identify added/modified/deleted records rather than full replace)

**Data comparison:**

After import/update, run spatial comparison to classify records:
- Records existing in gov data but not in OSM (candidates for import)
- Records existing in both with attribute differences (candidates for update)
- Records existing in OSM but not in gov data (potential deletions or data gaps)

Matching algorithm details TBD — will be designed separately once data is loaded and can be explored. Match results will be materialized into tables for fast serving in Phase 2.

### Phase 2 — HTTP Service + Tile Server

- `run` command starts HTTP service with background update threads
- GeoJSON data package downloads (bbox via GET, polygon via POST)
- Vector tile serving in MVT format (chosen over MLT as MLT is not yet widely supported by client libraries). DuckDB's `ST_AsMVT` function generates tiles directly from SQL queries. Fallback options if needed: pre-generated PMTiles via freestiler, or Rust-side MVT generation with `geozero`.
- Tile caching for lower zoom levels (5-14), refreshed periodically in background
- Aggregation at low zoom levels using H3 cells (DuckDB has H3 extension) or DBSCAN

### Phase 3 — Web UI

- Map showing status of addresses and buildings (exists in OSM, exists in gov data, exists in both)
- Interface to select area and download data package
- Deferred to Phase 3 or later: random location button, exclusion reporting endpoint (POST to mark records that should be ignored due to source data errors)

## Architecture

### Strategy: DuckDB-heavy

DuckDB handles storage, spatial queries, data comparison, and (eventually) tile generation. Rust handles CLI, HTTP serving, data download/parsing, and orchestration. Fall back to Rust-side spatial operations (`geo`/`geozero` crates) only if DuckDB performance proves insufficient for specific queries.

### DuckDB Schema (conceptual)

Core data tables:
- `osm_nodes` — all OSM nodes referenced by imported ways/relations: `(node_id INT64, lon DOUBLE, lat DOUBLE)`. Required to construct way/relation geometries from `ST_ReadOSM` raw data and to update geometries during replication. Storage estimate: tens of millions of rows for Poland.
- `osm_addresses` — address points from OSM with tags (addr:housenumber, addr:street, etc.) and geometry. Addresses can be nodes or ways — TBD whether way-addresses are stored as centroids or full geometry.
- `osm_buildings` — building polygons from OSM with tags and geometry. Buildings can be ways (simple polygons) or relations (multipolygons, polygons with inner rings/holes).
- `prg_addresses` — PRG address points with attributes and geometry
- `bdot10k_buildings` — BDOT10k building polygons with attributes and geometry
- `egib_buildings` — EGIB building polygons with attributes and geometry

Comparison/matching results stored in materialized tables (algorithm TBD).

Metadata table for replication state (OSM sequence number, last update timestamps per source).

### OSM Geometry Construction

`ST_ReadOSM` reads PBF files but returns raw OSM data — nodes with lat/lon, ways with arrays of node ID refs, relations with arrays of member refs and roles. It does NOT construct geometry.

`ST_ReadOSM` columns: `kind` (node/way/relation/changeset), `id`, `tags` (map), `refs` (int64[]), `lat`, `lon`, `ref_roles` (varchar[]), `ref_types` (node/way/relation[]).

Geometry must be constructed as a post-processing step:
1. Import all nodes from PBF into `osm_nodes` (at minimum, nodes referenced by address/building ways)
2. For ways: join `refs` array against `osm_nodes` to build linestrings/polygons using `ST_MakeLine` / `ST_MakePolygon`
3. For relations (multipolygons): resolve member ways, determine outer/inner rings from `ref_roles`, construct multipolygon geometry
4. Filter by relevant tags (building=*, addr:*) and insert into `osm_addresses` / `osm_buildings`

This is the same node-reference resolution problem as during replication updates — the `osm_nodes` table is a core schema element used by both import and update paths.

### DuckDB Extensions

Required at runtime. The binary auto-installs them on startup via `INSTALL <ext>; LOAD <ext>;` if not already present. Requires network on first run; subsequent runs load from disk. Bundled DuckDB pins the extension version, so no separate version management needed.

- `spatial` — geometry types, spatial functions, GeoParquet reading
- `h3` — H3 cell indexing for tile aggregation (Phase 2)

If the binary starts without network and extensions aren't cached, it exits with a clear error message explaining how to resolve this.

### Concurrency Model

Single DuckDB database file. Single writer, multiple readers.

- Background update threads acquire write access in short transactions (batch inserts/updates), then release
- HTTP request handlers read concurrently without blocking
- DuckDB's WAL mode enables this pattern
- Rust-side coordination via a shared connection pool or mutex-guarded write connection

### CLI Structure

Using `clap` for argument parsing:

```
osmpbudynkiv2 [--config <path>] import [osm|prg|bdot10k|egib|full]
osmpbudynkiv2 [--config <path>] update [osm|prg|bdot10k|egib]
osmpbudynkiv2 [--config <path>] run
```

The `--config` flag is global (applies to all subcommands) since all commands need to know the database path. Config file (optional) specifies: database path, update intervals per source, HTTP bind address/port.

### OSM Replication Consumer

1. Fetch the current state file from OSM France replication feed to get the latest available sequence number
2. Check our current sequence number in DuckDB metadata table
3. Download all replication files between our number and the latest — download in parallel for faster catch-up
4. For each diff (OsmChange XML, gzipped), parse and extract created/modified/deleted nodes, ways, relations
5. Filter to relevant data (addresses as nodes or ways, buildings as ways or relations within Poland)
6. Apply changes to `osm_addresses` and `osm_buildings` tables
7. Update stored sequence number
8. When caught up, sleep until next minute and repeat

Key concerns:
- Parallel download of replication files significantly speeds up catch-up after downtime
- Buildings can be ways (simple polygons) or relations (multipolygons with inner rings) — both must be handled
- Addresses can be nodes or ways — same treatment as during import
- Deletions must cascade correctly
- Must handle gaps in sequence numbers gracefully

### Data Download and Parsing

| Source | Format | Download | Parsing |
|--------|--------|----------|---------|
| OSM | PBF | HTTP (OSM France) | DuckDB `ST_ReadOSM` (raw data) + geometry construction |
| OSM updates | OsmChange XML (gzipped) | HTTP (OSM France replication) | `quick-xml` crate |
| PRG | ZIP containing GML/XML | HTTP (gov registry) | `prg_convert` (git dep) |
| BDOT10k | ZIP containing GeoParquet | HTTP (geoportal.gov.pl) | georust crates → Arrow RecordBatches → DuckDB (CRS issue) |
| EGIB | GeoParquet (2.4 GB) | HTTP (geoportal.gov.pl) | DuckDB Parquet reader |

### HTTP API (Phase 2)

- `GET /api/download?dataset=prg&bbox=minlon,minlat,maxlon,maxlat` — GeoJSON data package
- `POST /api/download` — GeoJSON body with polygon geometry, returns data package
- `GET /tiles/{dataset}/{z}/{x}/{y}.mvt` — vector tiles
- `GET /api/status` — last update timestamps per source, record counts

### Output Format

GeoJSON as default. If JOSM's GeoJSON support proves insufficient, add OSM XML writer as a separate output formatter behind the same API — the format selection is a presentation concern, isolated from the data layer.

### Error Handling

- HTTP downloads (data sources, replication diffs): retry with exponential backoff (3 attempts). If a single data source update fails, log the error and continue — the service should not crash because one source is temporarily unavailable.
- DuckDB query errors: log and propagate to caller. For the HTTP service, return 500 with a generic error message.
- Extension installation failure: exit with a clear error message on startup.
- Use `tracing` crate for structured logging.

## Key Dependencies

| Crate | Purpose |
|-------|---------|
| `duckdb` (bundled) | Embedded database, PBF reading via `ST_ReadOSM` |
| `clap` | CLI argument parsing |
| `quick-xml` | OsmChange XML parsing for replication |
| `prg_convert` (git dep from `github.com/ttomasz/prg_convert/`) | PRG address data parsing |
| `reqwest` | HTTP client for data downloads |
| georust crates (`geoparquet`, `arrow-rs`, `geo`) | BDOT10k GeoParquet parsing (CRS workaround), possible spatial fallback |
| `axum` or `actix-web` | HTTP server (Phase 2) |
| `tracing` | Structured logging |
| `serde` / `serde_json` | JSON/GeoJSON serialization |
| `osmpbf` (maybe) | Only if needed for PBF metadata extraction |

## Data Volume Estimates

| Source | Records | Source file size |
|--------|---------|-----------------|
| OSM addresses | ~8.7M (includes duplicates and some non-Poland) | Poland PBF ~2 GB |
| OSM buildings | ~17.9M | (included in PBF) |
| PRG | ~8.1M addresses | — |
| BDOT10k | ~16.3M buildings | ~2 GB GeoParquet |
| EGIB | ~17.8M buildings | ~2.4 GB GeoParquet |

## Key Technical Risks

1. **OSM geometry construction** — `ST_ReadOSM` provides raw data only. Building polygon geometry from node refs (ways) and resolving multipolygon relations (outer/inner rings) is non-trivial. This logic is shared between import and replication update paths. The most complex component in Phase 1.

2. **BDOT10k CRS issue** — the GeoParquet file has an incorrect CRS declaration. DuckDB may not read it correctly, requiring a Rust-side workaround (georust crates → Arrow RecordBatches → DuckDB). Needs early validation.

3. **DuckDB spatial extension stability** — the extension is actively developed and API may change between versions. Mitigation: pin DuckDB version, test spatial queries in CI.

4. **Spatial matching accuracy** — the address/building comparison logic is the core value proposition but requires empirical tuning against real data. Algorithm design deferred until data is loaded and explorable.

5. **Gov data incremental updates** — diffing current DB state against newly downloaded datasets to apply changes incrementally (rather than full replace) adds complexity but avoids expensive full re-imports. Need to determine good diff keys per source.

## Non-Goals (for now)

- Mobile support
- User authentication
- Multi-country support
- Real-time (sub-minute) updates
- Custom map styling
