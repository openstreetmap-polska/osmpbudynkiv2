# UDF Geometry Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the arrow-vtab-based geometry construction (which panics on >2048 rows) with DuckDB scalar UDFs that resolve node coordinates directly from RocksDB, and optimize the encoding path to avoid redundant byte conversions.

**Architecture:** Register scalar UDFs (`resolve_node_coords`, `resolve_way_coords`) with a shared `Arc<RocksDB>` state at DB init time. Import and update code calls these UDFs in SQL instead of building Arrow RecordBatches in Rust. The raw-bytes optimization avoids decoding RocksDB LE floats to `f64` only to re-encode them as LE floats in WKB — instead copying the 16-byte node blobs directly into the WKB buffer.

**Tech Stack:** Rust, duckdb-rs (`VScalar` trait, `vscalar` feature), RocksDB, WKB encoding

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/osm/encoding.rs` | Modify | Add `NODE_BYTE_LEN` constant |
| `src/osm/kvstore.rs` | Modify | Add `get_node_raw` that returns raw 16 bytes without decoding |
| `src/osm/udf.rs` | Modify | Add raw-bytes WKB path, add `ResolveWayCoords` UDF, register both |
| `src/db.rs` | Modify | Accept `Option<Arc<RocksDB>>` and register UDFs when provided |
| `src/main.rs` | Modify | Wrap `kv` in `Arc`, pass to `init_db` |
| `src/import/osm.rs` | Modify | Replace `batch_geometry::build_way_geometries` call with UDF-based SQL, replace `batch_geometry::build_relation_geometries` with UDF-based SQL |
| `src/import/mod.rs` | No change | Already passes `&RocksDB` |
| `src/update/osm.rs` | Modify | Replace `rebuild_way_geometry` coord resolution + arrow vtab with UDF SQL, replace `rebuild_relation_geometry` arrow vtab with UDF SQL |
| `src/osm/batch_geometry.rs` | Delete | Fully replaced by UDF approach |
| `src/osm/mod.rs` | Modify | Remove `batch_geometry` module |

---

### Task 1: Raw-bytes optimization in encoding and kvstore

Add a way to get the raw 16-byte LE blob from RocksDB without decoding to `(f64, f64)`.

**Files:**
- Modify: `src/osm/encoding.rs`
- Modify: `src/osm/kvstore.rs`

- [ ] **Step 1: Add NODE_BYTE_LEN constant and document the layout**

In `src/osm/encoding.rs`, add after line 9:

```rust
/// Byte length of an encoded node value (lon: 8 bytes LE f64 + lat: 8 bytes LE f64).
pub const NODE_BYTE_LEN: usize = 16;
```

- [ ] **Step 2: Add `get_node_raw` to kvstore**

In `src/osm/kvstore.rs`, add after the existing `get_node` function (after line 83):

```rust
/// Get raw encoded node bytes (16 bytes: lon LE f64 || lat LE f64).
/// Returns the raw bytes without decoding — useful for building WKB directly.
pub fn get_node_raw(db: &RocksDB, node_id: i64) -> Result<Option<Vec<u8>>> {
    match db.get_cf(&cf(db, CF_NODES), encoding::encode_key(node_id))? {
        Some(value) => Ok(Some(value)),
        None => Ok(None),
    }
}
```

- [ ] **Step 3: Add test for get_node_raw**

In `src/osm/kvstore.rs` tests module, add:

```rust
#[test]
fn test_node_raw_roundtrip() {
    let (_tmp, db) = open_tmp_db();
    put_node(&db, 1, 20.0, 50.0).unwrap();
    let raw = get_node_raw(&db, 1).unwrap().unwrap();
    assert_eq!(raw.len(), encoding::NODE_BYTE_LEN);
    // Raw bytes should be LE-encoded lon then lat
    let lon = f64::from_le_bytes(raw[..8].try_into().unwrap());
    let lat = f64::from_le_bytes(raw[8..16].try_into().unwrap());
    assert!((lon - 20.0).abs() < 1e-15);
    assert!((lat - 50.0).abs() < 1e-15);
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test osm::kvstore::tests`
Expected: All pass including `test_node_raw_roundtrip`.

- [ ] **Step 5: Commit**

```bash
git add src/osm/encoding.rs src/osm/kvstore.rs
git commit -m "feat: add get_node_raw for zero-copy WKB encoding"
```

---

### Task 2: Add raw-bytes WKB builder and ResolveWayCoords UDF

Optimize `ResolveNodeCoords` to use raw bytes. Add `ResolveWayCoords` UDF that takes a way ID, looks up its node refs, then resolves all coordinates — needed for relation geometry building.

**Files:**
- Modify: `src/osm/udf.rs`

- [ ] **Step 1: Replace encode_wkb_linestring with raw-bytes version and update ResolveNodeCoords**

Replace the `encode_wkb_linestring` function and modify `ResolveNodeCoords::invoke` to use raw bytes:

```rust
use super::kvstore::{self, RocksDB};
use super::encoding;
```

Replace the `invoke` body's inner loop and WKB call. The new approach collects raw 16-byte blobs instead of `(f64, f64)` tuples:

```rust
    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let num_rows = input.len();
        let input_nulls = input.flat_vector(0);
        let list_vec = input.list_vector(0);
        let mut out = output.flat_vector();

        for i in 0..num_rows {
            if input_nulls.row_is_null(i as u64) {
                out.set_null(i);
                continue;
            }

            let (offset, length) = list_vec.get_entry(i);

            if length < 2 {
                out.set_null(i);
                continue;
            }

            let child = list_vec.child(offset + length);
            let refs = child.as_slice::<i64>();

            let mut raw_coords: Vec<u8> =
                Vec::with_capacity(length * encoding::NODE_BYTE_LEN);
            let mut all_found = true;

            for j in 0..length {
                match kvstore::get_node_raw(&state.kv, refs[offset + j]) {
                    Ok(Some(bytes)) => raw_coords.extend_from_slice(&bytes),
                    Ok(None) => {
                        all_found = false;
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            if !all_found {
                out.set_null(i);
                continue;
            }

            let wkb = encode_wkb_linestring_raw(length, &raw_coords);
            out.insert(i, &wkb);
        }

        Ok(())
    }
```

Replace `encode_wkb_linestring` with:

```rust
/// Encode raw node coordinate bytes as a WKB LineString (little-endian, 2D).
/// `raw_coords` is a flat buffer of N * 16 bytes (each 16 bytes = lon LE f64 || lat LE f64).
/// WKB LE LineString layout is identical: header + sequence of (f64 x, f64 y) pairs.
/// So we can copy the raw bytes directly without any float conversion.
fn encode_wkb_linestring_raw(num_points: usize, raw_coords: &[u8]) -> Vec<u8> {
    debug_assert_eq!(raw_coords.len(), num_points * encoding::NODE_BYTE_LEN);
    // byte order (1) + type (4) + num_points (4) + coords
    let mut buf = Vec::with_capacity(9 + raw_coords.len());
    buf.push(0x01); // little-endian
    buf.extend_from_slice(&2u32.to_le_bytes()); // wkbLineString = 2
    buf.extend_from_slice(&(num_points as u32).to_le_bytes());
    buf.extend_from_slice(raw_coords);
    buf
}
```

- [ ] **Step 2: Add ResolveWayCoords UDF**

This UDF takes a single `BIGINT` (way_id), looks up its node refs in RocksDB, then resolves all node coordinates and returns WKB. This is needed for relation geometry building where the SQL has way IDs, not node ref lists.

Add after `ResolveNodeCoords`:

```rust
/// Scalar UDF: `resolve_way_coords(way_id BIGINT) -> BLOB`
///
/// Takes an OSM way ID, looks up its node refs in RocksDB, resolves each
/// node's coordinates, and returns a WKB LineString.
/// Returns NULL if the way is not found, has <2 nodes, or any node is missing.
pub struct ResolveWayCoords;

impl VScalar for ResolveWayCoords {
    type State = KvState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let num_rows = input.len();
        let way_ids_vec = input.flat_vector(0);
        let way_ids = way_ids_vec.as_slice::<i64>();
        let mut out = output.flat_vector();

        for i in 0..num_rows {
            if way_ids_vec.row_is_null(i as u64) {
                out.set_null(i);
                continue;
            }

            let node_ids = match kvstore::get_way(&state.kv, way_ids[i]) {
                Ok(Some(ids)) => ids,
                Ok(None) => {
                    out.set_null(i);
                    continue;
                }
                Err(e) => return Err(e.into()),
            };

            if node_ids.len() < 2 {
                out.set_null(i);
                continue;
            }

            let mut raw_coords: Vec<u8> =
                Vec::with_capacity(node_ids.len() * encoding::NODE_BYTE_LEN);
            let mut all_found = true;

            for &nid in &node_ids {
                match kvstore::get_node_raw(&state.kv, nid) {
                    Ok(Some(bytes)) => raw_coords.extend_from_slice(&bytes),
                    Ok(None) => {
                        all_found = false;
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            if !all_found {
                out.set_null(i);
                continue;
            }

            let wkb = encode_wkb_linestring_raw(node_ids.len(), &raw_coords);
            out.insert(i, &wkb);
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Bigint)],
            LogicalTypeHandle::from(LogicalTypeId::Blob),
        )]
    }
}
```

- [ ] **Step 3: Register both UDFs in register_udfs**

```rust
pub fn register_udfs(conn: &Connection, kv: Arc<RocksDB>) -> Result<()> {
    let state = KvState { kv };
    conn.register_scalar_function_with_state::<ResolveNodeCoords>(
        "resolve_node_coords",
        &state,
    )
    .context("Failed to register resolve_node_coords UDF")?;
    conn.register_scalar_function_with_state::<ResolveWayCoords>(
        "resolve_way_coords",
        &state,
    )
    .context("Failed to register resolve_way_coords UDF")?;
    Ok(())
}
```

- [ ] **Step 4: Add tests for ResolveWayCoords**

Add to the `tests` module:

```rust
#[test]
fn test_resolve_way_coords() {
    let (_tmp, conn, kv) = setup();

    // Store a way with refs [1, 2, 3, 4, 5]
    kvstore::put_way(&kv, 100, &[1, 2, 3, 4, 5]).unwrap();

    let wkt: String = conn
        .query_row(
            "SELECT ST_AsText(ST_GeomFromWKB(resolve_way_coords(100::BIGINT)))",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(wkt, "LINESTRING (20 50, 21 50, 21 51, 20 51, 20 50)");
}

#[test]
fn test_resolve_way_coords_missing_way_returns_null() {
    let (_tmp, conn, _kv) = setup();

    let result: Option<Vec<u8>> = conn
        .query_row(
            "SELECT resolve_way_coords(999::BIGINT)",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert!(result.is_none());
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test osm::udf`
Expected: All pass (existing tests still pass with raw-bytes optimization, new tests pass).

- [ ] **Step 6: Commit**

```bash
git add src/osm/udf.rs
git commit -m "feat: add resolve_way_coords UDF, optimize WKB encoding with raw bytes"
```

---

### Task 3: Wire UDF registration into db init

Currently `db::init_db` doesn't know about RocksDB. Change it to accept an optional `Arc<RocksDB>` so UDFs are registered at startup. This makes them available everywhere — import, update, and future `run` command.

**Files:**
- Modify: `src/db.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Modify init_db to accept optional kv and register UDFs**

```rust
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use duckdb::Connection;
use duckdb::vtab::arrow::ArrowVTab;

use crate::osm::kvstore::RocksDB;
use crate::osm::udf;

pub fn init_db(
    path: &Path,
    init_commands: &[String],
    kv: Option<Arc<RocksDB>>,
) -> Result<Connection> {
    let conn =
        Connection::open(path).with_context(|| format!("Failed to open database at {path:?}"))?;

    conn.register_table_function::<ArrowVTab>("arrow")
        .context("Failed to register arrow vtab")?;

    if let Some(kv) = kv {
        udf::register_udfs(&conn, kv)?;
    }

    for cmd in init_commands {
        conn.execute_batch(cmd)
            .with_context(|| format!("Failed to execute DuckDB init command: {cmd}"))?;
    }

    create_schema(&conn)?;

    Ok(conn)
}
```

- [ ] **Step 2: Update main.rs to wrap kv in Arc and pass to init_db**

```rust
use std::sync::Arc;
// ... existing imports ...

fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = load_config(cli.config.as_deref())?;

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.log_level));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    info!(db_path = %config.db_path, rocksdb_path = %config.rocksdb_path, "Initializing databases");
    let kv = Arc::new(osm::kvstore::open(
        Path::new(&config.rocksdb_path),
        config.rocksdb_block_cache_mb,
        config.rocksdb_write_buffer_mb,
    )?);
    let conn = db::init_db(
        Path::new(&config.db_path),
        &config.duckdb_init_commands,
        Some(kv.clone()),
    )?;

    match cli.command {
        Command::Import { source } => {
            import::run(&conn, &kv, source, &config, &config.download_urls)?
        }
        Command::Update { source } => {
            update::run(&conn, &kv, source, &config, &config.download_urls)?
        }
        Command::Run => {
            anyhow::bail!("Run command is not yet implemented");
        }
    }

    Ok(())
}
```

- [ ] **Step 3: Fix test callers of init_db**

Every test that calls `init_db` needs to pass `None` as the third argument. Search for all callers:

In `src/db.rs` tests:
```rust
let conn = init_db(Path::new(":memory:"), &init_commands, None)?;
```

In `src/import/osm.rs` tests:
```rust
let conn = init_db(Path::new(":memory:"), &init_commands, None)?;
```

In `src/update/osm.rs` tests:
```rust
let conn = init_db(Path::new(":memory:"), &init_commands, None)?;
```

In any integration test files under `tests/`:

Run: `cargo build 2>&1 | grep "init_db"` to find all callers that need updating.

- [ ] **Step 4: Run full test suite**

Run: `cargo test`
Expected: All existing tests pass (tests use `None` for kv, so UDFs not registered in tests that don't need them).

- [ ] **Step 5: Commit**

```bash
git add src/db.rs src/main.rs src/import/osm.rs src/update/osm.rs tests/
git commit -m "feat: register RocksDB UDFs at db init"
```

---

### Task 4: Replace batch_geometry way import with UDF SQL

Replace `batch_geometry::build_way_geometries` with pure SQL using `resolve_node_coords`.

**Files:**
- Modify: `src/import/osm.rs`

- [ ] **Step 1: Replace the build_way_geometries call**

In `src/import/osm.rs`, replace line 74:
```rust
batch_geometry::build_way_geometries(conn, kv, pbf_str)?;
```

With a new function call:
```rust
import_way_buildings_and_addresses(conn, pbf_str)?;
```

- [ ] **Step 2: Implement import_way_buildings_and_addresses**

Add the function (after `stream_ways_to_rocksdb`):

```rust
fn import_way_buildings_and_addresses(conn: &Connection, pbf_path: &str) -> Result<()> {
    info!("Importing way buildings");
    conn.execute_batch(&format!(
        "INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
         SELECT id, 'way', element_at(tags, 'building')[1],
                ST_MakePolygon(ST_GeomFromWKB(resolve_node_coords(refs)))
         FROM ST_ReadOSM('{pbf_path}')
         WHERE kind = 'way'
           AND refs IS NOT NULL
           AND len(refs) >= 4
           AND element_at(tags, 'building')[1] IS NOT NULL
           AND resolve_node_coords(refs) IS NOT NULL"
    ))
    .context("Failed to import way buildings")?;

    let building_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_buildings WHERE osm_type = 'way'",
        [],
        |row| row.get(0),
    )?;
    info!(count = building_count, "Way buildings imported");

    info!("Importing way addresses");
    conn.execute_batch(&format!(
        "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
         SELECT id, 'way',
                element_at(tags, 'addr:housenumber')[1],
                element_at(tags, 'addr:street')[1],
                COALESCE(element_at(tags, 'addr:city')[1], element_at(tags, 'addr:place')[1]),
                element_at(tags, 'addr:postcode')[1],
                ST_Centroid(ST_GeomFromWKB(resolve_node_coords(refs)))
         FROM ST_ReadOSM('{pbf_path}')
         WHERE kind = 'way'
           AND refs IS NOT NULL
           AND len(refs) > 0
           AND element_at(tags, 'addr:housenumber')[1] IS NOT NULL
           AND resolve_node_coords(refs) IS NOT NULL"
    ))
    .context("Failed to import way addresses")?;

    let addr_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_addresses WHERE osm_type = 'way'",
        [],
        |row| row.get(0),
    )?;
    info!(count = addr_count, "Way addresses imported");

    Ok(())
}
```

Note: `ST_Centroid` on a linestring gives the average point, which matches the old `AVG(lon), AVG(lat)` behavior.

- [ ] **Step 3: Remove batch_geometry import**

In `src/import/osm.rs`, remove:
```rust
use crate::osm::{batch_geometry, kvstore};
```
Replace with:
```rust
use crate::osm::kvstore;
```

- [ ] **Step 4: Run import tests**

Run: `cargo test import::osm`

These tests use the fixture PBF and validate counts + geometry. They will need the UDFs registered. Update `run_import_with_fixture` and test setup to register UDFs:

```rust
fn run_import_with_fixture(conn: &Connection, pbf_path: &Path) -> Result<()> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let kv = Arc::new(kvstore::open(tmp_dir.path(), 512, 64)?);
    udf::register_udfs(conn, kv.clone())?;
    let config = Config::default();
    import(conn, &kv, &config, Some(pbf_path), "")?;
    Ok(())
}
```

Add the necessary imports at the top of the tests module:
```rust
use std::sync::Arc;
use crate::osm::udf;
```

Expected: All import fixture tests pass with same counts (2 buildings, 3 addresses).

- [ ] **Step 5: Commit**

```bash
git add src/import/osm.rs
git commit -m "feat: replace way geometry batch builder with UDF SQL in import"
```

---

### Task 5: Replace batch_geometry relation import with UDF SQL

Replace `batch_geometry::build_relation_geometries` with SQL using `resolve_way_coords`.

**Files:**
- Modify: `src/import/osm.rs`

- [ ] **Step 1: Replace the build_relation_geometries call**

In `src/import/osm.rs`, replace line 76:
```rust
batch_geometry::build_relation_geometries(conn, kv, pbf_str)?;
```

With:
```rust
import_relation_buildings_and_addresses(conn, pbf_str)?;
```

- [ ] **Step 2: Implement import_relation_buildings_and_addresses**

This is more complex because relations have multiple member ways with roles (outer/inner). We need to build multipolygons. The approach: use a CTE that calls `resolve_way_coords` per member way, then group by role.

```rust
fn import_relation_buildings_and_addresses(conn: &Connection, pbf_path: &str) -> Result<()> {
    info!("Importing relation buildings");

    // Relations with building or address tags, unnested to one row per way member
    // Each row gets its way geometry resolved via UDF
    conn.execute_batch(&format!(
        "INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
         WITH rel_members AS (
             SELECT
                 id AS relation_id,
                 element_at(tags, 'building')[1] AS building,
                 unnest(refs) AS ref_id,
                 unnest(ref_types) AS ref_type,
                 unnest(ref_roles) AS ref_role
             FROM ST_ReadOSM('{pbf_path}')
             WHERE kind = 'relation'
               AND refs IS NOT NULL
               AND len(refs) > 0
               AND element_at(tags, 'building')[1] IS NOT NULL
         ),
         way_geoms AS (
             SELECT
                 relation_id, building, ref_role,
                 ST_GeomFromWKB(resolve_way_coords(ref_id)) AS line_geom
             FROM rel_members
             WHERE ref_type = 'way'
               AND resolve_way_coords(ref_id) IS NOT NULL
         ),
         outer_polys AS (
             SELECT relation_id, building,
                    ST_Union_Agg(ST_MakePolygon(line_geom)) AS outer_geom
             FROM way_geoms
             WHERE (ref_role = 'outer' OR ref_role = '')
               AND ST_NPoints(line_geom) >= 4
             GROUP BY relation_id, building
         ),
         inner_polys AS (
             SELECT relation_id,
                    ST_Union_Agg(ST_MakePolygon(line_geom)) AS inner_geom
             FROM way_geoms
             WHERE ref_role = 'inner'
               AND ST_NPoints(line_geom) >= 4
             GROUP BY relation_id
         )
         SELECT
             o.relation_id, 'relation', o.building,
             CASE
                 WHEN i.inner_geom IS NOT NULL THEN ST_Difference(o.outer_geom, i.inner_geom)
                 ELSE o.outer_geom
             END AS geom
         FROM outer_polys o
         LEFT JOIN inner_polys i ON o.relation_id = i.relation_id
         WHERE o.outer_geom IS NOT NULL"
    ))
    .context("Failed to import relation buildings")?;

    let building_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_buildings WHERE osm_type = 'relation'",
        [],
        |row| row.get(0),
    )?;
    info!(count = building_count, "Relation buildings imported");

    info!("Importing relation addresses");
    conn.execute_batch(&format!(
        "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
         WITH rel_members AS (
             SELECT
                 id AS relation_id,
                 element_at(tags, 'addr:housenumber')[1] AS housenumber,
                 element_at(tags, 'addr:street')[1] AS street,
                 COALESCE(element_at(tags, 'addr:city')[1], element_at(tags, 'addr:place')[1]) AS city,
                 element_at(tags, 'addr:postcode')[1] AS postcode,
                 unnest(refs) AS ref_id,
                 unnest(ref_types) AS ref_type
             FROM ST_ReadOSM('{pbf_path}')
             WHERE kind = 'relation'
               AND refs IS NOT NULL
               AND len(refs) > 0
               AND element_at(tags, 'addr:housenumber')[1] IS NOT NULL
         ),
         way_geoms AS (
             SELECT
                 relation_id, housenumber, street, city, postcode,
                 ST_GeomFromWKB(resolve_way_coords(ref_id)) AS line_geom
             FROM rel_members
             WHERE ref_type = 'way'
               AND resolve_way_coords(ref_id) IS NOT NULL
         )
         SELECT
             relation_id, 'relation', housenumber, street, city, postcode,
             ST_Centroid(ST_Collect(list(line_geom)))
         FROM way_geoms
         GROUP BY relation_id, housenumber, street, city, postcode"
    ))
    .context("Failed to import relation addresses")?;

    let addr_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_addresses WHERE osm_type = 'relation'",
        [],
        |row| row.get(0),
    )?;
    info!(count = addr_count, "Relation addresses imported");

    Ok(())
}
```

- [ ] **Step 3: Run import tests**

Run: `cargo test import::osm`
Expected: All pass with same counts (2 buildings incl 1 relation, 3 addresses incl 1 relation).

- [ ] **Step 4: Commit**

```bash
git add src/import/osm.rs
git commit -m "feat: replace relation geometry batch builder with UDF SQL in import"
```

---

### Task 6: Simplify update/osm.rs rebuild_way_geometry with UDF

Replace the manual coord resolution + `ST_Point` list building in `rebuild_way_geometry` with UDF calls.

**Files:**
- Modify: `src/update/osm.rs`

- [ ] **Step 1: Rewrite rebuild_way_geometry**

Replace the function body from line 270. The new version uses `resolve_way_coords` instead of manually looking up each node and building SQL point lists:

```rust
fn rebuild_way_geometry(
    conn: &Connection,
    kv: &RocksDB,
    way_id: i64,
    way_changes: &[WayChange],
) -> Result<()> {
    if kvstore::get_way(kv, way_id)?.is_none() {
        return Ok(());
    }

    let way_change = way_changes.iter().find(|w| w.id == way_id);
    let (building_tag, housenumber, street, city, postcode) = match way_change {
        Some(wc) => (
            tag_value(&wc.tags, "building"),
            tag_value(&wc.tags, "addr:housenumber"),
            tag_value(&wc.tags, "addr:street"),
            tag_value(&wc.tags, "addr:city").or_else(|| tag_value(&wc.tags, "addr:place")),
            tag_value(&wc.tags, "addr:postcode"),
        ),
        None => {
            let has_building: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way')",
                    [way_id],
                    |row| row.get(0),
                )
                .unwrap_or(false);
            let has_address: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM osm_addresses WHERE osm_id = ? AND osm_type = 'way')",
                    [way_id],
                    |row| row.get(0),
                )
                .unwrap_or(false);

            if !has_building && !has_address {
                return Ok(());
            }
            (
                if has_building { Some("yes".to_string()) } else { None },
                if has_address { Some(String::new()) } else { None },
                None,
                None,
                None,
            )
        }
    };

    if building_tag.is_none() && housenumber.is_none() {
        return Ok(());
    }

    conn.execute(
        "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way'",
        [way_id],
    )?;
    conn.execute(
        "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'way'",
        [way_id],
    )?;

    if building_tag.is_some() {
        let building = building_tag.as_deref().unwrap_or("yes");
        let building_sql = building.replace('\'', "''");
        conn.execute_batch(&format!(
            "INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
             SELECT {way_id}, 'way', '{building_sql}',
                    ST_MakePolygon(ST_GeomFromWKB(resolve_way_coords({way_id})))
             WHERE resolve_way_coords({way_id}) IS NOT NULL
               AND ST_NPoints(ST_GeomFromWKB(resolve_way_coords({way_id}))) >= 4"
        ))?;
    }

    if housenumber.is_some() {
        conn.execute(
            "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
             SELECT ?, 'way', ?, ?, ?, ?,
                    ST_Centroid(ST_GeomFromWKB(resolve_way_coords(?)))
             WHERE resolve_way_coords(?) IS NOT NULL",
            duckdb::params![way_id, housenumber, street, city, postcode, way_id, way_id],
        )?;
    }

    Ok(())
}
```

- [ ] **Step 2: Run update tests**

Run: `cargo test update::osm`
Expected: All pass. The update tests seed RocksDB directly and test apply_changes.

Note: Update tests call `init_db` which now needs UDFs. Update `setup_test_db_and_kv` to register UDFs:

```rust
fn setup_test_db_and_kv() -> Result<(Connection, RocksDB, tempfile::TempDir)> {
    let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
    let tmpdir = tempfile::tempdir()?;
    let kv = kvstore::open(tmpdir.path(), 8, 4)?;

    let kv_arc = Arc::new(kvstore::open(tmpdir.path(), 8, 4)?);
    // Actually we can't open same path twice. Use init_db with kv_arc instead:
```

Actually, this is a problem — we can't open the same RocksDB path twice. The test creates `kv` as owned, then needs `Arc<RocksDB>` for UDF registration. The simplest fix: open RocksDB once, wrap in `Arc`, pass `Arc` clone to `init_db`, deref `Arc` as `&RocksDB` for kvstore calls.

This means `import::run` and `update::run` should also accept `&Arc<RocksDB>` or the tests need restructuring. The cleanest approach: keep `&RocksDB` in the signatures (since `Arc<RocksDB>` derefs to `&RocksDB`), and have tests create the `Arc` first, pass clone to init_db, then use `&*arc` for the rest.

Update test setup:
```rust
fn setup_test_db_and_kv() -> Result<(Connection, Arc<RocksDB>, tempfile::TempDir)> {
    let tmpdir = tempfile::tempdir()?;
    let kv = Arc::new(kvstore::open(tmpdir.path(), 8, 4)?);
    let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
    let conn = crate::db::init_db(Path::new(":memory:"), &init_commands, Some(kv.clone()))?;

    kvstore::put_node(&kv, 1, 20.0, 50.0)?;
    kvstore::put_node(&kv, 2, 20.001, 50.0)?;
    kvstore::put_node(&kv, 3, 20.001, 50.001)?;
    kvstore::put_node(&kv, 4, 20.0, 50.001)?;

    kvstore::put_way(&kv, 100, &[1, 2, 3, 4, 1])?;
    for &nid in &[1i64, 2, 3, 4] {
        kvstore::add_node_to_ways(&kv, nid, 100)?;
    }

    conn.execute_batch(
        "INSERT INTO osm_buildings VALUES (100, 'way', 'yes', ST_MakePolygon(ST_MakeLine(
            list_value(ST_Point(20.0, 50.0), ST_Point(20.001, 50.0),
                       ST_Point(20.001, 50.001), ST_Point(20.0, 50.001),
                       ST_Point(20.0, 50.0))
        )));
        INSERT INTO metadata VALUES ('osm_replication_sequence', '1000');",
    )?;

    Ok((conn, kv, tmpdir))
}
```

Update all test functions to use `Arc<RocksDB>` — since `Arc<RocksDB>` derefs to `RocksDB`, all `&kv` usage continues to work.

- [ ] **Step 3: Commit**

```bash
git add src/update/osm.rs
git commit -m "feat: simplify rebuild_way_geometry with UDF"
```

---

### Task 7: Simplify update/osm.rs rebuild_relation_geometry with UDF

Replace the flatten/arrow-vtab approach with UDF calls.

**Files:**
- Modify: `src/update/osm.rs`

- [ ] **Step 1: Rewrite rebuild_relation_geometry**

Replace the entire function. The new version uses `resolve_way_coords` via SQL instead of manually building Arrow RecordBatches:

```rust
fn rebuild_relation_geometry(
    conn: &Connection,
    kv: &RocksDB,
    relation_id: i64,
    relation_changes: &[RelationChange],
) -> Result<()> {
    let members = match kvstore::get_relation(kv, relation_id)? {
        Some(m) => m,
        None => return Ok(()),
    };

    let rel_change = relation_changes.iter().find(|r| r.id == relation_id);
    let (building_tag, housenumber, street, city, postcode) = match rel_change {
        Some(rc) => (
            tag_value(&rc.tags, "building"),
            tag_value(&rc.tags, "addr:housenumber"),
            tag_value(&rc.tags, "addr:street"),
            tag_value(&rc.tags, "addr:city").or_else(|| tag_value(&rc.tags, "addr:place")),
            tag_value(&rc.tags, "addr:postcode"),
        ),
        None => {
            let has_building: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM osm_buildings WHERE osm_id = ? AND osm_type = 'relation')",
                    [relation_id],
                    |row| row.get(0),
                )
                .unwrap_or(false);
            let has_address: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM osm_addresses WHERE osm_id = ? AND osm_type = 'relation')",
                    [relation_id],
                    |row| row.get(0),
                )
                .unwrap_or(false);

            if !has_building && !has_address {
                return Ok(());
            }
            (
                if has_building { Some("yes".to_string()) } else { None },
                if has_address { Some(String::new()) } else { None },
                None,
                None,
                None,
            )
        }
    };

    if building_tag.is_none() && housenumber.is_none() {
        return Ok(());
    }

    conn.execute(
        "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'relation'",
        [relation_id],
    )?;
    conn.execute(
        "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'relation'",
        [relation_id],
    )?;

    // Build a VALUES list of way members: (way_id, role)
    let way_members: Vec<(i64, &str)> = members
        .iter()
        .filter(|(_, member_type, _)| *member_type == encoding::encode_member_type("way"))
        .map(|(ref_id, _, role)| (*ref_id, encoding::decode_member_role(*role)))
        .collect();

    if way_members.is_empty() {
        return Ok(());
    }

    let values_sql: String = way_members
        .iter()
        .map(|(wid, role)| format!("({wid}, '{role}')"))
        .collect::<Vec<_>>()
        .join(", ");

    if building_tag.is_some() {
        let building = building_tag.as_deref().unwrap_or("yes");
        let building_sql = building.replace('\'', "''");
        conn.execute_batch(&format!(
            "INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
             WITH way_members(way_id, member_role) AS (VALUES {values_sql}),
             way_geoms AS (
                 SELECT way_id, member_role,
                        ST_GeomFromWKB(resolve_way_coords(way_id)) AS line_geom
                 FROM way_members
                 WHERE resolve_way_coords(way_id) IS NOT NULL
             ),
             outer_polys AS (
                 SELECT ST_Union_Agg(ST_MakePolygon(line_geom)) AS outer_geom
                 FROM way_geoms
                 WHERE (member_role = 'outer' OR member_role = '')
                   AND ST_NPoints(line_geom) >= 4
             ),
             inner_polys AS (
                 SELECT ST_Union_Agg(ST_MakePolygon(line_geom)) AS inner_geom
                 FROM way_geoms
                 WHERE member_role = 'inner'
                   AND ST_NPoints(line_geom) >= 4
             )
             SELECT
                 {relation_id}, 'relation', '{building_sql}',
                 CASE
                     WHEN i.inner_geom IS NOT NULL THEN ST_Difference(o.outer_geom, i.inner_geom)
                     ELSE o.outer_geom
                 END
             FROM outer_polys o
             LEFT JOIN inner_polys i ON true
             WHERE o.outer_geom IS NOT NULL"
        ))?;
    }

    if housenumber.is_some() {
        let hn_sql = housenumber.as_deref().map(|v| format!("'{}'", v.replace('\'', "''"))).unwrap_or_else(|| "NULL".to_string());
        let street_sql = street.as_deref().map(|v| format!("'{}'", v.replace('\'', "''"))).unwrap_or_else(|| "NULL".to_string());
        let city_sql = city.as_deref().map(|v| format!("'{}'", v.replace('\'', "''"))).unwrap_or_else(|| "NULL".to_string());
        let postcode_sql = postcode.as_deref().map(|v| format!("'{}'", v.replace('\'', "''"))).unwrap_or_else(|| "NULL".to_string());

        conn.execute_batch(&format!(
            "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
             WITH way_members(way_id, member_role) AS (VALUES {values_sql}),
             way_geoms AS (
                 SELECT ST_GeomFromWKB(resolve_way_coords(way_id)) AS line_geom
                 FROM way_members
                 WHERE resolve_way_coords(way_id) IS NOT NULL
             )
             SELECT {relation_id}, 'relation', {hn_sql}, {street_sql}, {city_sql}, {postcode_sql},
                    ST_Centroid(ST_Collect(list(line_geom)))
             FROM way_geoms"
        ))?;
    }

    Ok(())
}
```

- [ ] **Step 2: Remove now-unused imports from update/osm.rs**

Remove the arrow-related imports that were used by the old `rebuild_relation_geometry`:

```rust
// Remove these (were inside the function body):
use duckdb::arrow::array::{Float64Array, Int64Array, StringBuilder};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::vtab::arrow::arrow_recordbatch_to_query_params;
use std::sync::Arc;
```

These were `use` statements inside the function body, so they'll be gone when the function is replaced.

- [ ] **Step 3: Run update tests**

Run: `cargo test update::osm`
Expected: All pass.

- [ ] **Step 4: Commit**

```bash
git add src/update/osm.rs
git commit -m "feat: simplify rebuild_relation_geometry with UDF, remove arrow vtab usage"
```

---

### Task 8: Delete batch_geometry.rs and clean up

**Files:**
- Delete: `src/osm/batch_geometry.rs`
- Modify: `src/osm/mod.rs`

- [ ] **Step 1: Remove batch_geometry module**

In `src/osm/mod.rs`, remove:
```rust
pub mod batch_geometry;
```

- [ ] **Step 2: Delete the file**

```bash
rm src/osm/batch_geometry.rs
```

- [ ] **Step 3: Check for any remaining references**

Run: `cargo build 2>&1`
Expected: Clean build (no references to batch_geometry remain).

- [ ] **Step 4: Run full test suite**

Run: `cargo test`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: remove batch_geometry module, fully replaced by UDF approach"
```

---

### Task 9: Clean up Cargo.toml features

With the arrow vtab no longer used for data insertion (only for `ArrowVTab` registration which is still used by some queries), check if `vtab-arrow` is still needed. Also check if `vscalar-arrow` can be removed since we use `VScalar` not `VArrowScalar`.

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Remove vscalar-arrow feature**

We use `VScalar` (from `vscalar` feature), not `VArrowScalar` (from `vscalar-arrow`). Remove the unused feature:

```toml
duckdb = { version = "1.10501.0", features = ["bundled", "vtab", "vtab-arrow", "vscalar"] }
```

- [ ] **Step 2: Verify build**

Run: `cargo build`
Expected: Clean build.

Run: `cargo test`
Expected: All tests pass.

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "chore: remove unused vscalar-arrow feature"
```
