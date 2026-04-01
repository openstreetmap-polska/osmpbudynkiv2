# RocksDB KV Cache for OSM Geometry Construction

## Problem

The current OSM import pipeline stores all raw data (238M nodes, 33M ways, 288K relations) in DuckDB tables and builds geometries via SQL joins. The `osm_nodes` table alone is ~5.7GB of raw data, and the UNNEST + JOIN operations during geometry construction exceed the 4GB memory target despite temp table materialization and spill-to-disk.

## Decision

Replace in-DuckDB raw OSM storage with a RocksDB key-value store for node coordinates and structural mappings. DuckDB retains only the final processed tables (`osm_buildings`, `osm_addresses`, `metadata`). Geometry construction moves to a Rust-orchestrated batch pipeline: Rust resolves coordinates from RocksDB, builds Arrow record batches, and feeds them to DuckDB for spatial function execution (`ST_MakePolygon`, `ST_MakeLine`, `ST_MakeValid`).

## Architecture

### Storage Split

**RocksDB** (intermediate/structural data):
- Node coordinates
- Way-to-node mappings
- Relation member mappings
- Reverse indexes for update cascading

**DuckDB** (final processed data):
- `osm_buildings` — polygons with building tags
- `osm_addresses` — points with address tags
- `metadata` — replication state, etc.
- BDOT10k, EGIB, PRG tables (unchanged)

### RocksDB Schema

Five column families, each with independent tuning:

| Column Family | Key | Value | Approx Entries |
|---|---|---|---|
| `nodes` | `i64` big-endian (node_id) | `(f64, f64)` — lon, lat | 238M |
| `ways` | `i64` big-endian (way_id) | `Vec<i64>` — ordered node_ids | 33M |
| `relations` | `i64` big-endian (relation_id) | `Vec<(i64, u8, u8)>` — ref, type, role | 288K |
| `node_to_ways` | `i64` big-endian (node_id) | `Vec<i64>` — way_ids referencing this node | 238M |
| `way_to_relations` | `i64` big-endian (way_id) | `Vec<i64>` — relation_ids referencing this way | ~few million |

Key encoding: fixed 8-byte big-endian `i64` (simple, sortable, no serialization overhead).

Value encoding: compact binary. Node coordinates: two `f64` packed as 16 bytes. Arrays: length-prefixed `i64` sequences. Relation members: packed structs with `u8` for type/role enums.

### RocksDB Configuration

- Compression: zstd
- Block cache: configurable, default 512MB (leaves room for DuckDB within 4GB budget)
- Write buffer: large during bulk import (64MB+), smaller during updates
- File location: configurable via project config (same config system as DuckDB file path)
- All RocksDB tuning parameters exposed in config

## Bulk Import Data Flow

### Phase 1 — PBF Streaming into RocksDB (3 passes)

PBF parsing stays in DuckDB via `ST_ReadOSM()`. Results are streamed as Arrow record batches — each batch is consumed and released before the next is fetched, so memory usage is bounded to one batch at a time.

**Pass 1 (nodes):**
```
ST_ReadOSM(pbf) WHERE kind='node' → Arrow batches
→ For each batch: write node:{id} → (lon, lat) to RocksDB
```

**Pass 2 (ways):**
```
ST_ReadOSM(pbf) WHERE kind='way' → Arrow batches
→ For each batch:
  - write way:{id} → [node_ids] to RocksDB
  - for each node_id in the way: append way_id to node_to_ways:{node_id}
```

**Pass 3 (relations):**
```
ST_ReadOSM(pbf) WHERE kind='relation' → Arrow batches
→ For each batch:
  - write relation:{id} → [(ref, type, role), ...] to RocksDB
  - for each way member: append relation_id to way_to_relations:{way_id}
```

### Phase 2 — Geometry Construction (batched)

Re-reads PBF to get tagged elements. For each batch, Rust resolves coordinates from RocksDB, builds Arrow record batches, and DuckDB constructs geometries.

**Building ways:**
```
ST_ReadOSM(pbf) WHERE kind='way' AND has building/address tags → Arrow batches
→ For each batch:
  1. For each way, look up node coordinates from RocksDB
  2. Build Arrow RecordBatch: (way_id, tag_values, lon_list, lat_list)
  3. DuckDB reads Arrow batch directly, executes:
     - UNNEST lon/lat lists with position
     - ST_MakePolygon(ST_MakeLine(...)) for buildings
     - ST_Point(AVG(lon), AVG(lat)) for addresses
     - ST_MakeValid() for invalid polygons
  4. Insert into osm_buildings / osm_addresses
```

**Building relations:**
```
ST_ReadOSM(pbf) WHERE kind='relation' AND has building/address tags → Arrow batches
→ For each batch:
  1. Look up member ways from RocksDB, then node coordinates for each way
  2. Build Arrow RecordBatch: (relation_id, way_id, member_role, lon_list, lat_list)
  3. DuckDB handles ST_MakeLine per way, ST_MakePolygon, ST_Union_Agg for outers,
     ST_Difference for inner holes
  4. Insert into osm_buildings / osm_addresses
```

## Incremental Update Data Flow

OsmChange XML parsing (`src/osm/replication.rs`) stays unchanged.

### Step 1 — Apply Changes to RocksDB

```
For each node change:
  create/modify: update node:{id} → (lon, lat)
  delete: remove node:{id}, remove from node_to_ways reverse index

For each way change:
  create/modify:
    - update way:{id} → [node_ids]
    - update node_to_ways reverse index (remove old entries, add new)
    - update way_to_relations reverse index if needed
  delete: remove way:{id}, clean up reverse indexes

For each relation change:
  create/modify: update relation:{id} → [(ref, type, role), ...]
  delete: remove relation:{id}, clean up way_to_relations
```

### Step 2 — Identify Affected Geometries

```
Collect affected way IDs:
  - ways directly created/modified/deleted
  - ways referenced by modified nodes (via node_to_ways:{node_id})

Collect affected relation IDs:
  - relations directly created/modified/deleted
  - relations referencing affected ways (via way_to_relations:{way_id})
```

### Step 3 — Rebuild Geometries

```
For affected ways:
  - Delete old rows from osm_buildings/osm_addresses (osm_id, osm_type='way')
  - For directly modified: tags available from OsmChange XML
  - For indirectly affected: check existence in osm_buildings/osm_addresses
  - If has relevant tags: look up coordinates from RocksDB, build Arrow batch, insert via DuckDB

For affected relations:
  - Same pattern: delete old, check tags, rebuild from RocksDB, insert new
```

## Error Handling and Failure Recovery

**Bulk import failure:**
- Delete both databases and re-run import (no partial resume, same as current behavior)
- RocksDB writes use `WriteBatch` for atomicity within each Arrow batch processing step

**Incremental update failure:**
- Each replication sequence is applied as a unit
- RocksDB changes use `WriteBatch` (atomic per sequence)
- DuckDB geometry rebuilds happen in a transaction
- If either fails, the sequence number is not advanced — next run retries
- Re-applying the same OsmChange is idempotent (creates/modifies overwrite, deletes are no-ops)

**Invalid geometries:**
- `ST_MakeValid()` applied after `ST_MakePolygon`
- Ways/relations producing NULL geometry after validation are logged and skipped

## Implementation Risk

The Arrow RecordBatch → DuckDB table source API (reading an Arrow batch directly as a query source) needs early verification. The `duckdb` Rust crate has `Appender` for bulk inserts, but the Arrow-as-table-source path may require a specific API that needs a spike. Fallback: use `Appender` to write coordinate data to a temp table.

## What Changes and What Stays

**Stays the same:**
- CLI structure (`import`, `update`, `run` commands)
- Config system (extended with RocksDB settings)
- DuckDB for final storage and spatial functions
- DuckDB for BDOT10k, EGIB, PRG imports
- OsmChange XML parsing
- Spatial indexes (RTREE on final tables)
- `ST_ReadOSM()` for PBF parsing

**Removed:**
- `osm_nodes`, `osm_ways`, `osm_relations` DuckDB tables
- Schema creation for those tables in `src/db.rs`
- All geometry SQL in `src/osm/geometry.rs` (replaced by Rust batch logic)
- Temp table materialization approach

**New:**
- `rust-rocksdb` dependency with `zstd` and static linking features
- RocksDB initialization and configuration (configurable via project config)
- KV store module: read/write for all 5 column families
- Batch geometry construction: Rust reads RocksDB, builds Arrow batches, feeds to DuckDB
- Updated `src/import/osm.rs`: 3 passes for RocksDB population + batched geometry phase
- Updated `src/update/osm.rs`: KV store updates, reverse index lookups, targeted geometry rebuilds
- Updated tests
