# Dataset Incremental Updates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `update prg`, `update bdot10k` and `update egib` — currently three `bail!("not yet implemented")` arms — so a fresh government snapshot is diffed against the live database and applied as a delta, without disrupting HTTP readers, while recording an aggregated per-area changeset.

**Architecture:** Five phases per refresh: download → stage (unindexed CTAS) → diff (whole-row hash join by ID) → apply (`DELETE` + `INSERT` in one transaction) → cleanup. The live table is never dropped or renamed, so its RTREE index stays live and correct and no reader-facing SQL changes. Design rationale and the DuckDB experiments behind it are in `docs/superpowers/specs/2026-07-22-dataset-incremental-updates-design.md`.

**Tech Stack:** Rust, DuckDB (bundled, via the `duckdb` crate) with the `spatial` extension, `clap` for CLI, `axum` + `tokio` for the server, `assert_cmd` + `tempfile` for integration tests.

## Global Constraints

- **Rust edition and toolchain:** as configured in `Cargo.toml` — do not change either.
- **Change cell zoom is z14**, matching the highest zoom `/tiles` serves. Defined once as a constant; never hard-code `14` at a call site.
- **`hash()` returns `UBIGINT`.** All hash columns and variables are `UBIGINT` in SQL, `u64` in Rust.
- **The row-hash expression must come from one shared function** used by both the import and the update path. If they disagree, every row compares as modified on every refresh, forever. This is the single most important invariant in the feature.
- **The apply phase must be exactly one transaction** covering the data delta, the `dataset_refreshes` row and the `dataset_change_areas` rows.
- **Within that transaction, the `dataset_change_areas` rows must be written before the delta.** The changeset reads the old geometry of removed and modified objects out of the live table, and the `DELETE` destroys it. Getting this backwards yields a silently wrong changeset, not an error.
- **Never `DROP` or `ALTER` a live source table** (`prg_addresses`, `bdot10k_buildings`, `egib_buildings`) outside the `import` path. DuckDB refuses to rename an indexed table, and a view degrades `RTREE_INDEX_SCAN` to `SEQ_SCAN` — both were verified and both are why this design exists.
- **Run `cargo fmt` and `cargo clippy` before every commit.** The repo is clean on both today; keep it that way.
- No new external dependencies. No migration machinery — the dev database is dropped and recreated when the schema changes.

---

### Task 1: Extract shared tile math

The XYZ↔lon/lat math currently lives as a private `tile_to_bbox` inside `src/server/tiles.rs`. The changeset needs the inverse. Move both into one crate-level module so the round-trip can be tested as a unit and there is no second copy of the projection formula.

**Files:**
- Create: `src/tile_math.rs`
- Modify: `src/main.rs` (add `mod tile_math;`)
- Modify: `src/server/tiles.rs` (delete local `tile_to_bbox`, import the shared one)

**Interfaces:**
- Consumes: nothing.
- Produces: `crate::tile_math::CHANGE_CELL_ZOOM: u32`, `crate::tile_math::tile_to_bbox(z: u32, x: u32, y: u32) -> (f64, f64, f64, f64)` returning `(min_lon, min_lat, max_lon, max_lat)`, and `crate::tile_math::lonlat_to_tile(lon: f64, lat: f64, z: u32) -> (u32, u32)` returning `(x, y)`.

- [ ] **Step 1: Write the failing test**

Create `src/tile_math.rs` containing only the tests plus stub signatures:

```rust
use std::f64::consts::PI;

/// Zoom level at which dataset change areas are aggregated. Matches the
/// highest zoom `/tiles` serves, so a change cell maps 1:1 onto a served
/// tile for cache invalidation.
pub const CHANGE_CELL_ZOOM: u32 = 14;

pub fn tile_to_bbox(_z: u32, _x: u32, _y: u32) -> (f64, f64, f64, f64) {
    unimplemented!()
}

pub fn lonlat_to_tile(_lon: f64, _lat: f64, _z: u32) -> (u32, u32) {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verified by hand against the SQL form used in the changeset builder:
    /// lon=21.0, lat=52.0 at z14 lands in tile (9147, 5411).
    #[test]
    fn lonlat_to_tile_known_point() {
        assert_eq!(lonlat_to_tile(21.0, 52.0, 14), (9147, 5411));
    }

    /// The tile a point maps to must be the tile whose bbox contains it.
    /// This is the property that keeps the Rust and SQL forms honest.
    #[test]
    fn tile_contains_the_point_that_produced_it() {
        for (lon, lat) in [
            (21.0, 52.0),
            (14.5, 49.35),
            (23.88, 54.54),
            (19.94, 50.06),
        ] {
            let (x, y) = lonlat_to_tile(lon, lat, CHANGE_CELL_ZOOM);
            let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(CHANGE_CELL_ZOOM, x, y);
            assert!(
                min_lon <= lon && lon <= max_lon && min_lat <= lat && lat <= max_lat,
                "tile ({x},{y}) bbox ({min_lon},{min_lat},{max_lon},{max_lat}) \
                 does not contain ({lon},{lat})"
            );
        }
    }

    /// Known bbox for the tile above, to catch a silently changed formula.
    #[test]
    fn tile_to_bbox_known_tile() {
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 9147, 5411);
        assert!((min_lon - 20.983887).abs() < 1e-5, "min_lon was {min_lon}");
        assert!((min_lat - 51.998410).abs() < 1e-5, "min_lat was {min_lat}");
        assert!((max_lon - 21.005859).abs() < 1e-5, "max_lon was {max_lon}");
        assert!((max_lat - 52.011937).abs() < 1e-5, "max_lat was {max_lat}");
    }
}
```

Add `mod tile_math;` to `src/main.rs` alongside the other `mod` declarations (after `mod shutdown;`).

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test tile_math -- --nocapture`
Expected: FAIL — all three tests panic with `not implemented`.

- [ ] **Step 3: Write minimal implementation**

Replace the two stubs in `src/tile_math.rs`:

```rust
/// Bounding box of an XYZ tile as (min_lon, min_lat, max_lon, max_lat).
pub fn tile_to_bbox(z: u32, x: u32, y: u32) -> (f64, f64, f64, f64) {
    let n = 2f64.powi(z as i32);
    let min_lon = x as f64 / n * 360.0 - 180.0;
    let max_lon = (x + 1) as f64 / n * 360.0 - 180.0;
    let max_lat = (PI * (1.0 - 2.0 * y as f64 / n)).sinh().atan() * 180.0 / PI;
    let min_lat = (PI * (1.0 - 2.0 * (y + 1) as f64 / n)).sinh().atan() * 180.0 / PI;
    (min_lon, min_lat, max_lon, max_lat)
}

/// XYZ tile containing a lon/lat point. Inverse of [`tile_to_bbox`].
pub fn lonlat_to_tile(lon: f64, lat: f64, z: u32) -> (u32, u32) {
    let n = 2f64.powi(z as i32);
    let x = ((lon + 180.0) / 360.0 * n).floor();
    let lat_rad = lat.to_radians();
    let y = ((1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / PI) / 2.0 * n).floor();
    (x as u32, y as u32)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test tile_math`
Expected: PASS — 3 passed.

- [ ] **Step 5: Point `tiles.rs` at the shared module**

In `src/server/tiles.rs`: delete the local `fn tile_to_bbox(...)` (around line 113) and its now-unused `use std::f64::consts::PI;` at line 1. Add near the other `use` lines:

```rust
use crate::tile_math::tile_to_bbox;
```

- [ ] **Step 6: Verify nothing regressed**

Run: `cargo test && cargo clippy -- -D warnings && cargo fmt -- --check`
Expected: all existing tests pass, no clippy warnings, formatting clean.

- [ ] **Step 7: Commit**

```bash
git add src/tile_math.rs src/main.rs src/server/tiles.rs
git commit -m "refactor: extract shared tile math with lonlat_to_tile inverse"
```

---

### Task 2: Changeset schema

Add the two changeset tables. Both are idempotent `CREATE TABLE IF NOT EXISTS` in `create_schema`, consistent with the rest of the schema.

**Files:**
- Modify: `src/db.rs` (`create_schema`, plus its `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: nothing.
- Produces: tables `dataset_refreshes(snapshot_id BIGINT, source VARCHAR, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, source_etag VARCHAR, added INTEGER, modified INTEGER, removed INTEGER)` and `dataset_change_areas(snapshot_id BIGINT, source VARCHAR, cell_z INTEGER, cell_x INTEGER, cell_y INTEGER, added INTEGER, modified INTEGER, removed INTEGER, detected_at TIMESTAMPTZ)`.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/db.rs`:

```rust
#[test]
fn test_init_db_creates_changeset_tables() -> Result<()> {
    let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
    let conn = init_db(Path::new(":memory:"), &init_commands, None)?;

    for table in ["dataset_refreshes", "dataset_change_areas"] {
        let count: i64 =
            conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row.get(0))?;
        assert_eq!(count, 0, "Table {table} should be empty initially");
    }
    Ok(())
}

#[test]
fn test_changeset_tables_round_trip() -> Result<()> {
    let init_commands = vec![
        "INSTALL spatial".to_string(),
        "LOAD spatial".to_string(),
        "INSTALL icu".to_string(),
        "LOAD icu".to_string(),
    ];
    let conn = init_db(Path::new(":memory:"), &init_commands, None)?;

    conn.execute_batch(
        "INSERT INTO dataset_refreshes
             VALUES (1, 'bdot10k', now(), now(), 'etag-abc', 10, 20, 5);
         INSERT INTO dataset_change_areas
             VALUES (1, 'bdot10k', 14, 9147, 5411, 10, 20, 5, now());",
    )?;

    let (source, added, modified, removed): (String, i32, i32, i32) = conn.query_row(
        "SELECT source, added, modified, removed FROM dataset_refreshes",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    assert_eq!((source.as_str(), added, modified, removed), ("bdot10k", 10, 20, 5));

    let (z, x, y): (i32, i32, i32) = conn.query_row(
        "SELECT cell_z, cell_x, cell_y FROM dataset_change_areas",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!((z, x, y), (14, 9147, 5411));

    Ok(())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_init_db_creates_changeset_tables test_changeset_tables_round_trip`
Expected: FAIL — `Catalog Error: Table with name dataset_refreshes does not exist!`

- [ ] **Step 3: Write minimal implementation**

Append to the `execute_batch` string in `src/db.rs::create_schema`, after the `package_exports` block:

```sql
-- One row per dataset refresh attempt, including no-ops. Owns snapshot_id,
-- which is assigned inside the apply transaction as MAX(snapshot_id) + 1.
CREATE TABLE IF NOT EXISTS dataset_refreshes (
    snapshot_id BIGINT PRIMARY KEY,
    source VARCHAR,
    started_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE,
    source_etag VARCHAR,
    added INTEGER,
    modified INTEGER,
    removed INTEGER
);

-- Aggregated change counts per XYZ tile (z = tile_math::CHANGE_CELL_ZOOM).
-- Both the old and the new geometry of a changed object contribute, so an
-- object that moves marks the cell it left and the cell it entered.
CREATE TABLE IF NOT EXISTS dataset_change_areas (
    snapshot_id BIGINT,
    source VARCHAR,
    cell_z INTEGER,
    cell_x INTEGER,
    cell_y INTEGER,
    added INTEGER,
    modified INTEGER,
    removed INTEGER,
    detected_at TIMESTAMP WITH TIME ZONE
);
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test db::tests`
Expected: PASS — all `db` tests including the two new ones.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings
git add src/db.rs
git commit -m "feat: add dataset_refreshes and dataset_change_areas tables"
```

---

### Task 3: Dataset spec and shared row-hash SQL

The single most important invariant: import and update must generate byte-identical hash SQL. Put both the per-source metadata and the hash-SQL generator in one crate-level module that both paths depend on.

**Files:**
- Create: `src/dataset.rs`
- Modify: `src/main.rs` (add `mod dataset;`)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `crate::dataset::GeomKind` — enum with variants `Point` and `Polygon`.
  - `crate::dataset::DatasetSpec` — struct with public fields `name: &'static str`, `table: &'static str`, `id_column: &'static str`, `geom_kind: GeomKind`.
  - `crate::dataset::DatasetSpec::representative_point_sql(&self, geom_expr: &str) -> String`.
  - `crate::dataset::BDOT10K: DatasetSpec`, `EGIB: DatasetSpec`, `PRG: DatasetSpec` (consts).
  - `crate::dataset::hashed_select(inner_select: &str) -> String` — wraps a SELECT so the result gains a `_row_hash` column.
  - `crate::dataset::ROW_HASH_VERSION: i64` and `crate::dataset::ROW_HASH_VERSION_KEY: &str`.

- [ ] **Step 1: Write the failing test**

Create `src/dataset.rs`:

```rust
//! Per-source metadata and the shared row-hash SQL used by both the import
//! and the update paths.
//!
//! The row hash is computed by hashing a whole-row reference over a subquery
//! alias rather than an explicit column list:
//!
//! ```sql
//! SELECT *, hash(s) AS _row_hash FROM (<inner select>) s
//! ```
//!
//! `hash(s)` hashes every column of `s` including `GEOMETRY`, and `s`
//! deliberately does not contain `_row_hash`, so the hash is never
//! self-referential. Because there is no column list to maintain, a source
//! gaining or losing a column cannot silently desynchronize the import and
//! update expressions.

/// Bumped whenever the row-hash expression changes in a way that alters its
/// output. A mismatch against `metadata.row_hash_version` means every row
/// will compare as modified; the refresh warns and proceeds.
pub const ROW_HASH_VERSION: i64 = 1;
pub const ROW_HASH_VERSION_KEY: &str = "row_hash_version";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeomKind {
    /// Geometry is already a point; use it directly.
    Point,
    /// Geometry is an area; use its centroid as the representative point.
    Polygon,
}

#[derive(Debug, Clone, Copy)]
pub struct DatasetSpec {
    /// Short source name, used in CLI output, job names and changeset rows.
    pub name: &'static str,
    /// The live table this source owns.
    pub table: &'static str,
    /// Stable per-object identifier. NOT unique — BDOT10k has duplicate IDs,
    /// so the diff compares an ID's whole row-set, never row to row.
    pub id_column: &'static str,
    pub geom_kind: GeomKind,
}

impl DatasetSpec {
    /// SQL for the point that represents this object when assigning it to a
    /// change cell.
    pub fn representative_point_sql(&self, geom_expr: &str) -> String {
        match self.geom_kind {
            GeomKind::Point => geom_expr.to_string(),
            GeomKind::Polygon => format!("ST_Centroid({geom_expr})"),
        }
    }

    /// Name of the transient staging table used during a refresh.
    pub fn staging_table(&self) -> String {
        format!("{}__staging", self.table)
    }
}

pub const BDOT10K: DatasetSpec = DatasetSpec {
    name: "bdot10k",
    table: "bdot10k_buildings",
    id_column: "LOKALNYID",
    geom_kind: GeomKind::Polygon,
};

pub const EGIB: DatasetSpec = DatasetSpec {
    name: "egib",
    table: "egib_buildings",
    id_column: "id_budynku",
    geom_kind: GeomKind::Polygon,
};

pub const PRG: DatasetSpec = DatasetSpec {
    name: "prg",
    table: "prg_addresses",
    id_column: "lokalny_id",
    geom_kind: GeomKind::Point,
};

/// Wrap `inner_select` so its result gains a `_row_hash UBIGINT` column.
///
/// This is the ONLY place the hash expression is written. Both the import
/// and the update path call it; if they ever diverge, every row compares as
/// modified on every refresh forever.
pub fn hashed_select(inner_select: &str) -> String {
    format!("SELECT *, hash(s) AS _row_hash FROM ({inner_select}) s")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashed_select_wraps_inner_query() {
        assert_eq!(
            hashed_select("SELECT 1 AS a"),
            "SELECT *, hash(s) AS _row_hash FROM (SELECT 1 AS a) s"
        );
    }

    #[test]
    fn representative_point_uses_centroid_for_polygons() {
        assert_eq!(BDOT10K.representative_point_sql("geom"), "ST_Centroid(geom)");
        assert_eq!(EGIB.representative_point_sql("geom"), "ST_Centroid(geom)");
    }

    #[test]
    fn representative_point_passes_through_for_points() {
        assert_eq!(PRG.representative_point_sql("geom"), "geom");
    }

    #[test]
    fn staging_table_is_derived_from_live_table() {
        assert_eq!(BDOT10K.staging_table(), "bdot10k_buildings__staging");
        assert_eq!(PRG.staging_table(), "prg_addresses__staging");
    }

    /// The hash must actually be computable over a GEOMETRY column and must
    /// agree between two independent evaluations of the same content. This is
    /// the invariant the whole feature rests on.
    #[test]
    fn hash_agrees_across_independent_evaluations() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE src (id VARCHAR, a VARCHAR, lon DOUBLE, lat DOUBLE);
             INSERT INTO src VALUES ('1','x',20.0,52.0), ('2','y',NULL,NULL);",
        )
        .unwrap();

        let inner = "SELECT id, a, ST_Point(lon, lat) AS geom FROM src";
        let sql = format!(
            "CREATE TABLE t1 AS {};
             CREATE TABLE t2 AS {};",
            hashed_select(inner),
            hashed_select(inner)
        );
        conn.execute_batch(&sql).unwrap();

        let disagreements: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM t1 JOIN t2 USING (id)
                 WHERE t1._row_hash IS DISTINCT FROM t2._row_hash",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(disagreements, 0, "same content must hash identically");

        let nulls: i64 = conn
            .query_row("SELECT COUNT(*) FROM t1 WHERE _row_hash IS NULL", [], |r| r.get(0))
            .unwrap();
        assert_eq!(nulls, 0, "NULL geometry must still produce a hash");
    }
}
```

Add `mod dataset;` to `src/main.rs`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test dataset::tests`
Expected: FAIL to compile — `src/dataset.rs` is not yet a module, or tests fail if `mod dataset;` was forgotten. Once it compiles, all five tests should pass since the implementation is written alongside. If any fail, fix before proceeding.

- [ ] **Step 3: Run test to verify it passes**

Run: `cargo test dataset::tests`
Expected: PASS — 5 passed.

- [ ] **Step 4: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings
git add src/dataset.rs src/main.rs
git commit -m "feat: add DatasetSpec and shared row-hash SQL generator"
```

---

### Task 4: BDOT10k — extract loader and write `_row_hash`

Split "load rows into table X" from "replace the live table" so the update path can reuse the loader, and route the load through `hashed_select`.

**Files:**
- Modify: `src/import/bdot10k.rs`
- Test: `tests/cli_import_bdot10k.rs`

**Interfaces:**
- Consumes: `crate::dataset::{hashed_select, BDOT10K}`.
- Produces: `crate::import::bdot10k::load_into(conn: &Connection, target_table: &str, parquet_path: &str) -> Result<()>` — creates `target_table` from the parquet with a `_row_hash` column and no index.

- [ ] **Step 1: Write the failing test**

Add to `tests/cli_import_bdot10k.rs`:

```rust
#[test]
fn test_import_bdot10k_writes_row_hash() {
    let db = tempfile::TempDir::new().unwrap();
    let rocksdb_dir = tempfile::TempDir::new().unwrap();
    let db_path = db.path().join("test.duckdb");
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    write!(
        tmp,
        "db_path = \"{}\"\nrocksdb_path = \"{}\"\n",
        db_path.display(),
        rocksdb_dir.path().display()
    )
    .unwrap();

    cmd()
        .args([
            "--config",
            tmp.path().to_str().unwrap(),
            "import",
            "bdot10k",
            "--file",
            "fixtures/bdot10k.parquet",
        ])
        .assert()
        .success();

    let conn = duckdb::Connection::open(&db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;").unwrap();
    let (total, null_hashes): (i64, i64) = conn
        .query_row(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE _row_hash IS NULL) FROM bdot10k_buildings",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(total, 74);
    assert_eq!(null_hashes, 0, "every row must carry a hash");
}
```

This test needs `duckdb::Connection`, which integration tests cannot reach: this is a binary-only crate, so `[dependencies]` are not in scope for `tests/`. Add to `[dev-dependencies]` in `Cargo.toml`, matching the `[dependencies]` version exactly so cargo reuses the already-compiled artifact rather than rebuilding the bundled C++:

```toml
duckdb = { version = "1.10502.0", features = ["bundled"] }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test cli_import_bdot10k test_import_bdot10k_writes_row_hash`
Expected: FAIL — `Binder Error: Referenced column "_row_hash" not found`.

- [ ] **Step 3: Write minimal implementation**

In `src/import/bdot10k.rs`, add the loader and call it from `import`. Replace the `execute_batch` that creates the table with:

```rust
/// Create `target_table` from a BDOT10k GeoParquet file, including the
/// `_row_hash` column. Does NOT create an index — callers that need one
/// create it themselves, and the update path deliberately does not.
///
/// Workaround: DuckDB's automatic GeoParquet conversion and ST_Read (GDAL)
/// both fail on BDOT10k files because their CRS (EPSG:2180) is stored as a
/// projjson string-in-string which DuckDB rejects as "invalid CRS". Instead
/// we disable the automatic conversion, read the file as plain parquet, and
/// manually convert the WKB geometry column. Geometry is transformed from
/// EPSG:2180 to EPSG:4326 for uniform spatial comparisons.
pub fn load_into(conn: &Connection, target_table: &str, parquet_path: &str) -> Result<()> {
    let inner = format!(
        "SELECT * EXCLUDE(GEOM), \
         ST_Transform(ST_GeomFromWKB(GEOM), 'EPSG:2180', 'EPSG:4326') AS geom \
         FROM '{parquet_path}'"
    );
    conn.execute_batch(&format!(
        "SET enable_geoparquet_conversion = false;
         DROP TABLE IF EXISTS {target_table};
         CREATE TABLE {target_table} AS {};",
        crate::dataset::hashed_select(&inner)
    ))
    .with_context(|| format!("Failed to load BDOT10k data into {target_table}"))
}
```

Then in `import`, replace the inline table-creation `execute_batch` with:

```rust
let t = std::time::Instant::now();
load_into(conn, crate::dataset::BDOT10K.table, parquet_str)?;
info!(
    elapsed = %format_duration(t.elapsed()),
    "Step done: load table"
);
```

Leave the index creation, count query, cleanup and final logging exactly as they are.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test cli_import_bdot10k`
Expected: PASS — the existing two tests plus the new one.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings
git add Cargo.toml Cargo.lock src/import/bdot10k.rs tests/cli_import_bdot10k.rs
git commit -m "refactor: extract bdot10k load_into and write _row_hash"
```

---

### Task 5: EGIB — extract loader and write `_row_hash`

Same split as Task 4. EGIB reads a normal GeoParquet, so it does not need the conversion workaround.

**Files:**
- Modify: `src/import/egib.rs`
- Test: `tests/cli_import_egib.rs`

**Interfaces:**
- Consumes: `crate::dataset::{hashed_select, EGIB}`.
- Produces: `crate::import::egib::load_into(conn: &Connection, target_table: &str, parquet_path: &str) -> Result<()>`.

- [ ] **Step 1: Write the failing test**

Add to `tests/cli_import_egib.rs`, mirroring Task 4's test but for EGIB. Read the existing file first to copy its `cmd()` helper and the expected fixture row count, then:

```rust
#[test]
fn test_import_egib_writes_row_hash() {
    let db = tempfile::TempDir::new().unwrap();
    let rocksdb_dir = tempfile::TempDir::new().unwrap();
    let db_path = db.path().join("test.duckdb");
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    write!(
        tmp,
        "db_path = \"{}\"\nrocksdb_path = \"{}\"\n",
        db_path.display(),
        rocksdb_dir.path().display()
    )
    .unwrap();

    cmd()
        .args([
            "--config",
            tmp.path().to_str().unwrap(),
            "import",
            "egib",
            "--file",
            "fixtures/egib.parquet",
        ])
        .assert()
        .success();

    let conn = duckdb::Connection::open(&db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;").unwrap();
    let null_hashes: i64 = conn
        .query_row(
            "SELECT COUNT(*) FILTER (WHERE _row_hash IS NULL) FROM egib_buildings",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(null_hashes, 0, "every row must carry a hash");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test cli_import_egib test_import_egib_writes_row_hash`
Expected: FAIL — `Binder Error: Referenced column "_row_hash" not found`.

- [ ] **Step 3: Write minimal implementation**

In `src/import/egib.rs`:

```rust
/// Create `target_table` from an EGIB GeoParquet file, including the
/// `_row_hash` column. Does NOT create an index.
///
/// Geometry is transformed from EPSG:2180 to EPSG:4326 for uniform spatial
/// comparisons.
pub fn load_into(conn: &Connection, target_table: &str, parquet_path: &str) -> Result<()> {
    let inner = format!(
        "SELECT * EXCLUDE(geometry, geometry_bbox), \
         ST_Transform(geometry, 'EPSG:2180', 'EPSG:4326') AS geom \
         FROM '{parquet_path}'"
    );
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {target_table};
         CREATE TABLE {target_table} AS {};",
        crate::dataset::hashed_select(&inner)
    ))
    .with_context(|| format!("Failed to load EGIB data into {target_table}"))
}
```

And in `import`, replace the inline table-creation `execute_batch` with:

```rust
let t = std::time::Instant::now();
load_into(conn, crate::dataset::EGIB.table, parquet_str)?;
info!(
    elapsed = %format_duration(t.elapsed()),
    "Step done: load table"
);
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test cli_import_egib`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings
git add src/import/egib.rs tests/cli_import_egib.rs
git commit -m "refactor: extract egib load_into and write _row_hash"
```

---

### Task 6: PRG — extract loader and write `_row_hash`

PRG differs: it streams arrow batches from GML inside a zip into a staging table, then materializes the final table with a geometry column. The hash goes on the *materialization* step, not the raw streaming step.

**Files:**
- Modify: `src/import/prg.rs`
- Test: `tests/cli_import_prg.rs`

**Interfaces:**
- Consumes: `crate::dataset::{hashed_select, PRG}`.
- Produces: `crate::import::prg::materialize_into(conn: &Connection, target_table: &str, raw_table: &str) -> Result<()>` — builds `target_table` from the already-streamed `raw_table`, adding `geom` and `_row_hash`, and drops `raw_table`.

- [ ] **Step 1: Write the failing test**

Add to `tests/cli_import_prg.rs`, copying the existing file's `cmd()` helper and fixture arguments (it needs both `--file fixtures/prg.zip` and `--terc-file fixtures/teryt.zip`):

```rust
#[test]
fn test_import_prg_writes_row_hash() {
    let db = tempfile::TempDir::new().unwrap();
    let rocksdb_dir = tempfile::TempDir::new().unwrap();
    let db_path = db.path().join("test.duckdb");
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    write!(
        tmp,
        "db_path = \"{}\"\nrocksdb_path = \"{}\"\n",
        db_path.display(),
        rocksdb_dir.path().display()
    )
    .unwrap();

    cmd()
        .args([
            "--config",
            tmp.path().to_str().unwrap(),
            "import",
            "prg",
            "--file",
            "fixtures/prg.zip",
            "--terc-file",
            "fixtures/teryt.zip",
        ])
        .assert()
        .success();

    let conn = duckdb::Connection::open(&db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;").unwrap();
    let null_hashes: i64 = conn
        .query_row(
            "SELECT COUNT(*) FILTER (WHERE _row_hash IS NULL) FROM prg_addresses",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(null_hashes, 0, "every row must carry a hash");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test cli_import_prg test_import_prg_writes_row_hash`
Expected: FAIL — `Binder Error: Referenced column "_row_hash" not found`.

- [ ] **Step 3: Write minimal implementation**

In `src/import/prg.rs`:

```rust
/// Build `target_table` from the streamed `raw_table`, adding a geometry
/// column built from EPSG:4326 lon/lat (the parser already reprojected from
/// EPSG:2180) and the `_row_hash` column. Drops `raw_table` afterwards.
/// Does NOT create an index.
pub fn materialize_into(conn: &Connection, target_table: &str, raw_table: &str) -> Result<()> {
    let inner = format!(
        "SELECT *, ST_Point(dlugosc_geograficzna, szerokosc_geograficzna) AS geom \
         FROM {raw_table} \
         WHERE dlugosc_geograficzna IS NOT NULL \
           AND szerokosc_geograficzna IS NOT NULL"
    );
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {target_table};
         CREATE TABLE {target_table} AS {};
         DROP TABLE {raw_table};",
        crate::dataset::hashed_select(&inner)
    ))
    .with_context(|| format!("Failed to materialize {target_table}"))
}
```

Replace the existing "materialize the final table" `execute_batch` in `import` with:

```rust
let t = std::time::Instant::now();
materialize_into(conn, crate::dataset::PRG.table, "prg_addresses_raw")?;
info!(
    elapsed = %format_duration(t.elapsed()),
    "Step done: build prg_addresses with geom column"
);
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test cli_import_prg`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings
git add src/import/prg.rs tests/cli_import_prg.rs
git commit -m "refactor: extract prg materialize_into and write _row_hash"
```

---

### Task 7: Diff engine

Classify every ID as added, removed or modified by comparing an ID's whole row-set hash. Verified behavior: duplicate IDs are replaced as a unit, and rows with NULL geometry hash consistently rather than always comparing as modified.

**Files:**
- Create: `src/update/diff.rs`
- Modify: `src/update/mod.rs` (add `mod diff;`)

**Interfaces:**
- Consumes: `crate::dataset::DatasetSpec`.
- Produces:
  - `crate::update::diff::DiffCounts { pub added: i64, pub modified: i64, pub removed: i64 }`.
  - `crate::update::diff::compute(conn: &Connection, spec: &DatasetSpec) -> Result<DiffCounts>` — reads `spec.table` and `spec.staging_table()`, creates temp tables `diff_added`, `diff_removed`, `diff_modified` (each a single `id VARCHAR` column) and returns the counts.

- [ ] **Step 1: Write the failing test**

Create `src/update/diff.rs`:

```rust
use anyhow::{Context, Result};
use duckdb::Connection;

use crate::dataset::DatasetSpec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiffCounts {
    pub added: i64,
    pub modified: i64,
    pub removed: i64,
}

pub fn compute(_conn: &Connection, _spec: &DatasetSpec) -> Result<DiffCounts> {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{DatasetSpec, GeomKind};
    use crate::db::init_db;
    use std::path::Path;

    const TEST_SPEC: DatasetSpec = DatasetSpec {
        name: "test",
        table: "live",
        id_column: "id",
        geom_kind: GeomKind::Point,
    };

    /// Live and staging tables covering every classification at once:
    ///   keep     - identical in both            -> unchanged
    ///   mod      - attribute changed            -> modified
    ///   del      - only in live                 -> removed
    ///   add      - only in staging              -> added
    ///   dup      - two rows, one changed        -> modified (whole ID)
    ///   nullgeom - NULL geometry, unchanged     -> unchanged
    fn setup() -> Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let inner_live = "SELECT * FROM (VALUES
             ('keep','v1',20.0,52.0), ('mod','v1',20.1,52.0), ('del','v1',20.2,52.0),
             ('dup','v1',20.3,52.0), ('dup','v2',20.4,52.0), ('nullgeom','v1',NULL,NULL)
           ) t(id, a, lon, lat)";
        let inner_stg = "SELECT * FROM (VALUES
             ('keep','v1',20.0,52.0), ('mod','CHANGED',20.1,52.0), ('add','v1',20.5,52.0),
             ('dup','v1',20.3,52.0), ('dup','CHANGED',20.4,52.0), ('nullgeom','v1',NULL,NULL)
           ) t(id, a, lon, lat)";
        let wrap = |inner: &str| {
            crate::dataset::hashed_select(&format!(
                "SELECT id, a, ST_Point(lon, lat) AS geom FROM ({inner})"
            ))
        };
        conn.execute_batch(&format!(
            "CREATE TABLE live AS {};
             CREATE TABLE live__staging AS {};",
            wrap(inner_live),
            wrap(inner_stg)
        ))
        .unwrap();
        conn
    }

    fn ids(conn: &Connection, table: &str) -> Vec<String> {
        let mut stmt = conn
            .prepare(&format!("SELECT id FROM {table} ORDER BY id"))
            .unwrap();
        let rows = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap();
        rows.map(|r| r.unwrap()).collect()
    }

    #[test]
    fn classifies_added_removed_and_modified() {
        let conn = setup();
        let counts = compute(&conn, &TEST_SPEC).unwrap();

        assert_eq!(ids(&conn, "diff_added"), vec!["add"]);
        assert_eq!(ids(&conn, "diff_removed"), vec!["del"]);
        assert_eq!(ids(&conn, "diff_modified"), vec!["dup", "mod"]);
        assert_eq!(
            counts,
            DiffCounts { added: 1, modified: 2, removed: 1 }
        );
    }

    /// An unchanged row must never appear in any bucket — in particular a row
    /// whose geometry is NULL, which would otherwise hash inconsistently.
    #[test]
    fn unchanged_rows_including_null_geometry_are_not_reported() {
        let conn = setup();
        compute(&conn, &TEST_SPEC).unwrap();
        for table in ["diff_added", "diff_removed", "diff_modified"] {
            let listed = ids(&conn, table);
            assert!(!listed.contains(&"keep".to_string()), "{table} listed 'keep'");
            assert!(
                !listed.contains(&"nullgeom".to_string()),
                "{table} listed 'nullgeom'"
            );
        }
    }

    /// Re-running the diff against identical content reports nothing.
    #[test]
    fn identical_snapshots_produce_no_changes() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let inner = "SELECT id, a, ST_Point(lon, lat) AS geom FROM (
             SELECT * FROM (VALUES ('a','v1',20.0,52.0), ('b','v2',21.0,53.0)) t(id,a,lon,lat))";
        conn.execute_batch(&format!(
            "CREATE TABLE live AS {};
             CREATE TABLE live__staging AS {};",
            crate::dataset::hashed_select(inner),
            crate::dataset::hashed_select(inner)
        ))
        .unwrap();

        let counts = compute(&conn, &TEST_SPEC).unwrap();
        assert_eq!(counts, DiffCounts { added: 0, modified: 0, removed: 0 });
    }
}
```

Add `mod diff;` to `src/update/mod.rs`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test update::diff`
Expected: FAIL — all three tests panic with `not implemented`.

- [ ] **Step 3: Write minimal implementation**

Replace the `compute` stub in `src/update/diff.rs`:

```rust
/// Classify every ID in `spec.table` vs `spec.staging_table()` into the
/// temp tables `diff_added`, `diff_removed` and `diff_modified`.
///
/// The comparison is per-ID, not per-row: an ID's rows are folded into a
/// single order-independent hash via `hash(list_sort(list(_row_hash)))`.
/// IDs are NOT unique in these datasets (BDOT10k ships duplicates), so an
/// ID's whole row-set is replaced as a unit and duplicates cannot drift.
pub fn compute(conn: &Connection, spec: &DatasetSpec) -> Result<DiffCounts> {
    let live = spec.table;
    let staging = spec.staging_table();
    let id = spec.id_column;

    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS diff_live_hashes;
         DROP TABLE IF EXISTS diff_new_hashes;
         DROP TABLE IF EXISTS diff_added;
         DROP TABLE IF EXISTS diff_removed;
         DROP TABLE IF EXISTS diff_modified;

         CREATE TEMP TABLE diff_live_hashes AS
             SELECT {id} AS id, hash(list_sort(list(_row_hash))) AS h
             FROM {live} GROUP BY {id};
         CREATE TEMP TABLE diff_new_hashes AS
             SELECT {id} AS id, hash(list_sort(list(_row_hash))) AS h
             FROM {staging} GROUP BY {id};

         CREATE TEMP TABLE diff_added AS
             SELECT id FROM diff_new_hashes ANTI JOIN diff_live_hashes USING (id);
         CREATE TEMP TABLE diff_removed AS
             SELECT id FROM diff_live_hashes ANTI JOIN diff_new_hashes USING (id);
         CREATE TEMP TABLE diff_modified AS
             SELECT n.id FROM diff_new_hashes n JOIN diff_live_hashes l USING (id)
             WHERE n.h IS DISTINCT FROM l.h;"
    ))
    .with_context(|| format!("Failed to compute diff for {}", spec.name))?;

    let count = |table: &str| -> Result<i64> {
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row.get(0))
            .with_context(|| format!("Failed to count {table}"))
    };

    Ok(DiffCounts {
        added: count("diff_added")?,
        modified: count("diff_modified")?,
        removed: count("diff_removed")?,
    })
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test update::diff`
Expected: PASS — 3 passed.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings
git add src/update/diff.rs src/update/mod.rs
git commit -m "feat: add per-ID row-set hash diff engine"
```

---

### Task 8: Changeset builder

Turn the diff tables into aggregated per-tile change counts. Both old and new geometry contribute, so an object that moves marks the cell it left and the cell it entered.

**Files:**
- Create: `src/update/changeset.rs`
- Modify: `src/update/mod.rs` (add `mod changeset;`)

**Interfaces:**
- Consumes: `crate::dataset::DatasetSpec`, `crate::tile_math::CHANGE_CELL_ZOOM`, and the `diff_added` / `diff_removed` / `diff_modified` temp tables from Task 7.
- Produces: `crate::update::changeset::insert_change_areas(conn: &Connection, spec: &DatasetSpec, snapshot_id: i64) -> Result<i64>` — inserts into `dataset_change_areas` and returns the number of cell rows written. Must be called inside the caller's transaction, and **before** the delta is applied: it reads the old geometry of removed and modified objects out of the live table, which the `DELETE` destroys.

- [ ] **Step 1: Write the failing test**

Create `src/update/changeset.rs`:

```rust
use anyhow::{Context, Result};
use duckdb::Connection;

use crate::dataset::DatasetSpec;
use crate::tile_math::CHANGE_CELL_ZOOM;

pub fn insert_change_areas(
    _conn: &Connection,
    _spec: &DatasetSpec,
    _snapshot_id: i64,
) -> Result<i64> {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{DatasetSpec, GeomKind};
    use crate::db::init_db;
    use crate::tile_math::lonlat_to_tile;
    use std::path::Path;

    const TEST_SPEC: DatasetSpec = DatasetSpec {
        name: "test",
        table: "live",
        id_column: "id",
        geom_kind: GeomKind::Point,
    };

    /// Build live/staging tables plus the three diff tables by hand, so this
    /// test does not depend on the diff engine's internals.
    fn setup() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE live AS
                 SELECT * FROM (VALUES
                     ('del', ST_Point(21.0, 52.0)),
                     ('mov', ST_Point(21.0, 52.0))
                 ) t(id, geom);
             CREATE TABLE live__staging AS
                 SELECT * FROM (VALUES
                     ('add', ST_Point(21.0, 52.0)),
                     ('mov', ST_Point(19.0, 50.0))
                 ) t(id, geom);
             CREATE TEMP TABLE diff_added    AS SELECT 'add' AS id;
             CREATE TEMP TABLE diff_removed  AS SELECT 'del' AS id;
             CREATE TEMP TABLE diff_modified AS SELECT 'mov' AS id;",
        )
        .unwrap();
        conn
    }

    #[test]
    fn aggregates_counts_per_cell() {
        let conn = setup();
        let rows = insert_change_areas(&conn, &TEST_SPEC, 7).unwrap();

        let (home_x, home_y) = lonlat_to_tile(21.0, 52.0, CHANGE_CELL_ZOOM);
        let (added, modified, removed): (i32, i32, i32) = conn
            .query_row(
                "SELECT added, modified, removed FROM dataset_change_areas
                 WHERE cell_x = ? AND cell_y = ?",
                duckdb::params![home_x, home_y],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        // 'add' added here, 'del' removed here, and 'mov' left from here.
        assert_eq!((added, modified, removed), (1, 1, 1));
        assert_eq!(rows, 2, "two distinct cells were touched");
    }

    /// An object that moves marks BOTH the cell it left and the one it entered.
    #[test]
    fn moved_object_marks_both_cells() {
        let conn = setup();
        insert_change_areas(&conn, &TEST_SPEC, 7).unwrap();

        let (dest_x, dest_y) = lonlat_to_tile(19.0, 50.0, CHANGE_CELL_ZOOM);
        let modified: i32 = conn
            .query_row(
                "SELECT modified FROM dataset_change_areas WHERE cell_x = ? AND cell_y = ?",
                duckdb::params![dest_x, dest_y],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(modified, 1, "destination cell must be marked too");
    }

    #[test]
    fn stamps_snapshot_id_source_and_zoom() {
        let conn = setup();
        insert_change_areas(&conn, &TEST_SPEC, 7).unwrap();

        let (snapshot_id, source, z): (i64, String, i32) = conn
            .query_row(
                "SELECT DISTINCT snapshot_id, source, cell_z FROM dataset_change_areas",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(snapshot_id, 7);
        assert_eq!(source, "test");
        assert_eq!(z, CHANGE_CELL_ZOOM as i32);
    }
}
```

Add `mod changeset;` to `src/update/mod.rs`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test update::changeset`
Expected: FAIL — all three tests panic with `not implemented`.

- [ ] **Step 3: Write minimal implementation**

Replace the stub:

```rust
/// Aggregate the diff tables into per-tile change counts and insert them
/// into `dataset_change_areas`. Returns the number of cell rows written.
///
/// Must be called inside the caller's transaction so the changeset commits
/// atomically with the data delta it describes.
///
/// Contributions:
///   - added    -> new geometry (from staging)
///   - removed  -> old geometry (from live)
///   - modified -> BOTH old and new geometry, so an object that moves marks
///                 the cell it left as well as the cell it entered.
///
/// Rows with NULL geometry contribute no cell (they have no location), but
/// are still counted in `dataset_refreshes`.
pub fn insert_change_areas(
    conn: &Connection,
    spec: &DatasetSpec,
    snapshot_id: i64,
) -> Result<i64> {
    let live = spec.table;
    let staging = spec.staging_table();
    let id = spec.id_column;
    let z = CHANGE_CELL_ZOOM;
    let n = format!("pow(2, {z})");

    // Web-Mercator XYZ tile of a point, matching tile_math::lonlat_to_tile.
    let cell_x = |p: &str| format!("floor((ST_X({p}) + 180) / 360 * {n})::INTEGER");
    let cell_y = |p: &str| {
        format!(
            "floor((1 - ln(tan(radians(ST_Y({p}))) + 1 / cos(radians(ST_Y({p})))) / pi()) \
             / 2 * {n})::INTEGER"
        )
    };

    let point_live = spec.representative_point_sql("l.geom");
    let point_stg = spec.representative_point_sql("s.geom");

    let sql = format!(
        "INSERT INTO dataset_change_areas
         SELECT {snapshot_id}, '{source}', {z}, cell_x, cell_y,
                COUNT(*) FILTER (WHERE kind = 'added')::INTEGER,
                COUNT(*) FILTER (WHERE kind = 'modified')::INTEGER,
                COUNT(*) FILTER (WHERE kind = 'removed')::INTEGER,
                now()
         FROM (
             SELECT 'added' AS kind, {sx} AS cell_x, {sy} AS cell_y
             FROM {staging} s JOIN diff_added d ON s.{id} = d.id
             WHERE s.geom IS NOT NULL
             UNION ALL
             SELECT 'removed', {lx}, {ly}
             FROM {live} l JOIN diff_removed d ON l.{id} = d.id
             WHERE l.geom IS NOT NULL
             UNION ALL
             SELECT 'modified', {sx}, {sy}
             FROM {staging} s JOIN diff_modified d ON s.{id} = d.id
             WHERE s.geom IS NOT NULL
             UNION ALL
             SELECT 'modified', {lx}, {ly}
             FROM {live} l JOIN diff_modified d ON l.{id} = d.id
             WHERE l.geom IS NOT NULL
         )
         GROUP BY cell_x, cell_y",
        source = spec.name,
        sx = cell_x(&point_stg),
        sy = cell_y(&point_stg),
        lx = cell_x(&point_live),
        ly = cell_y(&point_live),
    );

    conn.execute_batch(&sql)
        .with_context(|| format!("Failed to write change areas for {}", spec.name))?;

    conn.query_row(
        "SELECT COUNT(*) FROM dataset_change_areas WHERE snapshot_id = ?",
        duckdb::params![snapshot_id],
        |row| row.get(0),
    )
    .context("Failed to count inserted change areas")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test update::changeset`
Expected: PASS — 3 passed.

Note: a `modified` object that did **not** move contributes its cell twice (once from live, once from staging), so that cell's `modified` count is 2 for one object. The tests above use a moved object precisely to pin the both-cells behavior. If a future consumer needs exact object counts rather than "how much churn touched this tile", de-duplicate by `(id, cell)` — do not change it speculatively now.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings
git add src/update/changeset.rs src/update/mod.rs
git commit -m "feat: aggregate dataset changes into per-tile change areas"
```

---

### Task 9: Refresh orchestration

Wire staging, diff, apply and cleanup into one routine. This is where the transaction boundary, the staging `Drop` guard, and all the guard conditions live.

**Files:**
- Create: `src/update/dataset.rs`
- Modify: `src/update/mod.rs` (add `mod dataset;`)

**Interfaces:**
- Consumes: `crate::dataset::DatasetSpec`, `crate::update::diff::{compute, DiffCounts}`, `crate::update::changeset::insert_change_areas`.
- Produces: `crate::update::dataset::refresh(conn: &Connection, spec: &DatasetSpec, load: impl FnOnce(&Connection, &str) -> Result<()>, source_etag: Option<&str>) -> Result<DiffCounts>` — the `load` closure creates the staging table (callers pass a closure wrapping the relevant `load_into`).

- [ ] **Step 1: Write the failing test**

Create `src/update/dataset.rs` with the stub plus tests:

```rust
use anyhow::{Context, Result, bail};
use duckdb::Connection;
use tracing::{info, warn};

use crate::dataset::{DatasetSpec, ROW_HASH_VERSION, ROW_HASH_VERSION_KEY};
use crate::update::changeset::insert_change_areas;
use crate::update::diff::{self, DiffCounts};
use crate::utils::format_duration;

/// Fraction of the live table that may change before the refresh warns.
/// Measured normal churn for BDOT10k is ~2% over five weeks, so this only
/// fires on an upstream restructuring. It is a diagnostic, NOT a stop.
const IMPLAUSIBLE_CHURN_FRACTION: f64 = 0.5;

pub fn refresh(
    _conn: &Connection,
    _spec: &DatasetSpec,
    _load: impl FnOnce(&Connection, &str) -> Result<()>,
    _source_etag: Option<&str>,
) -> Result<DiffCounts> {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{DatasetSpec, GeomKind};
    use crate::db::init_db;
    use std::path::Path;

    const TEST_SPEC: DatasetSpec = DatasetSpec {
        name: "test",
        table: "live",
        id_column: "id",
        geom_kind: GeomKind::Point,
    };

    fn conn_with_live(rows: &str) -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let inner = format!("SELECT id, a, ST_Point(lon, lat) AS geom FROM ({rows})");
        conn.execute_batch(&format!(
            "CREATE TABLE live AS {};",
            crate::dataset::hashed_select(&inner)
        ))
        .unwrap();
        conn
    }

    /// Loader closure that fills staging from an inline VALUES list.
    fn loader(rows: &'static str) -> impl FnOnce(&Connection, &str) -> Result<()> {
        move |conn: &Connection, target: &str| {
            let inner = format!("SELECT id, a, ST_Point(lon, lat) AS geom FROM ({rows})");
            conn.execute_batch(&format!(
                "CREATE TABLE {target} AS {};",
                crate::dataset::hashed_select(&inner)
            ))?;
            Ok(())
        }
    }

    const LIVE_ROWS: &str = "SELECT * FROM (VALUES
        ('keep','v1',21.0,52.0), ('mod','v1',21.0,52.0), ('del','v1',21.0,52.0)
      ) t(id,a,lon,lat)";
    const NEW_ROWS: &str = "SELECT * FROM (VALUES
        ('keep','v1',21.0,52.0), ('mod','CHANGED',21.0,52.0), ('add','v1',21.0,52.0)
      ) t(id,a,lon,lat)";

    #[test]
    fn applies_delta_to_live_table() {
        let conn = conn_with_live(LIVE_ROWS);
        let counts = refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None).unwrap();
        assert_eq!(counts, DiffCounts { added: 1, modified: 1, removed: 1 });

        let mut stmt = conn.prepare("SELECT id, a FROM live ORDER BY id").unwrap();
        let rows: Vec<(String, String)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            rows,
            vec![
                ("add".to_string(), "v1".to_string()),
                ("keep".to_string(), "v1".to_string()),
                ("mod".to_string(), "CHANGED".to_string()),
            ]
        );
    }

    #[test]
    fn writes_refresh_row_and_change_areas() {
        let conn = conn_with_live(LIVE_ROWS);
        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), Some("etag-1")).unwrap();

        let (snapshot_id, source, etag, added, modified, removed): (
            i64, String, String, i32, i32, i32,
        ) = conn
            .query_row(
                "SELECT snapshot_id, source, source_etag, added, modified, removed
                 FROM dataset_refreshes",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
            )
            .unwrap();
        assert_eq!(snapshot_id, 1, "first refresh gets snapshot_id 1");
        assert_eq!((source.as_str(), etag.as_str()), ("test", "etag-1"));
        assert_eq!((added, modified, removed), (1, 1, 1));

        let cells: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM dataset_change_areas WHERE snapshot_id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(cells > 0, "expected at least one change area row");
    }

    /// snapshot_id is MAX + 1, so a second refresh does not collide.
    #[test]
    fn snapshot_ids_increment() {
        let conn = conn_with_live(LIVE_ROWS);
        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None).unwrap();
        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None).unwrap();

        let ids: Vec<i64> = {
            let mut stmt = conn
                .prepare("SELECT snapshot_id FROM dataset_refreshes ORDER BY snapshot_id")
                .unwrap();
            let r = stmt.query_map([], |r| r.get(0)).unwrap();
            r.map(|x| x.unwrap()).collect()
        };
        assert_eq!(ids, vec![1, 2]);
    }

    /// The load-bearing safety check: an empty staging table means a
    /// truncated or empty download, which would otherwise delete everything.
    #[test]
    fn empty_staging_aborts_and_leaves_live_untouched() {
        let conn = conn_with_live(LIVE_ROWS);
        let empty = "SELECT * FROM (VALUES ('x','y',1.0,1.0)) t(id,a,lon,lat) WHERE false";
        let err = refresh(&conn, &TEST_SPEC, loader(empty), None).unwrap_err();
        assert!(
            format!("{err:#}").contains("0 rows"),
            "error should name the empty staging table, got: {err:#}"
        );

        let live_rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM live", [], |r| r.get(0))
            .unwrap();
        assert_eq!(live_rows, 3, "live table must be untouched");
        let refreshes: i64 = conn
            .query_row("SELECT COUNT(*) FROM dataset_refreshes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(refreshes, 0, "aborted refresh must not be recorded");
    }

    /// Staging is dropped on both the success and the failure path.
    #[test]
    fn staging_table_is_always_cleaned_up() {
        let conn = conn_with_live(LIVE_ROWS);
        refresh(&conn, &TEST_SPEC, loader(NEW_ROWS), None).unwrap();
        assert!(!staging_exists(&conn), "staging left behind after success");

        let empty = "SELECT * FROM (VALUES ('x','y',1.0,1.0)) t(id,a,lon,lat) WHERE false";
        let _ = refresh(&conn, &TEST_SPEC, loader(empty), None);
        assert!(!staging_exists(&conn), "staging left behind after failure");
    }

    fn staging_exists(conn: &Connection) -> bool {
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                duckdb::params![TEST_SPEC.staging_table()],
                |r| r.get(0),
            )
            .unwrap();
        n > 0
    }

    /// Change areas must be written BEFORE the delta is applied, while the
    /// live table still holds the old snapshot. Written after, the DELETE has
    /// already destroyed the old rows: removed objects vanish from the
    /// changeset entirely, and a moved object marks its destination cell
    /// twice instead of marking both the cell it left and the one it entered.
    /// This is a silent wrong answer, not an error, so pin it with a test.
    #[test]
    fn change_areas_see_the_pre_apply_geometry() {
        use crate::tile_math::{CHANGE_CELL_ZOOM, lonlat_to_tile};

        const BEFORE: &str = "SELECT * FROM (VALUES
            ('mov','v1',21.0,52.0), ('del','v1',21.0,52.0)
          ) t(id,a,lon,lat)";
        // 'mov' moves to a different z14 cell; 'del' disappears.
        const AFTER: &str = "SELECT * FROM (VALUES
            ('mov','v1',19.0,50.0)
          ) t(id,a,lon,lat)";

        let conn = conn_with_live(BEFORE);
        refresh(&conn, &TEST_SPEC, loader(AFTER), None).unwrap();

        let cell = |lon: f64, lat: f64| -> (i32, i32, i32) {
            let (x, y) = lonlat_to_tile(lon, lat, CHANGE_CELL_ZOOM);
            conn.query_row(
                "SELECT COALESCE(SUM(added), 0)::INTEGER,
                        COALESCE(SUM(modified), 0)::INTEGER,
                        COALESCE(SUM(removed), 0)::INTEGER
                 FROM dataset_change_areas WHERE cell_x = ? AND cell_y = ?",
                duckdb::params![x, y],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap()
        };

        let (_, origin_modified, origin_removed) = cell(21.0, 52.0);
        assert_eq!(origin_removed, 1, "removed object must mark its cell");
        assert_eq!(origin_modified, 1, "moved object must mark the cell it left");

        let (_, dest_modified, _) = cell(19.0, 50.0);
        assert_eq!(dest_modified, 1, "moved object must mark the cell it entered");
    }

    /// An unchanged snapshot still records a refresh row, with zero counts
    /// and no change areas, so "ran and did nothing" is distinguishable from
    /// "never ran".
    #[test]
    fn unchanged_snapshot_records_a_noop_refresh() {
        let conn = conn_with_live(LIVE_ROWS);
        let counts = refresh(&conn, &TEST_SPEC, loader(LIVE_ROWS), None).unwrap();
        assert_eq!(counts, DiffCounts { added: 0, modified: 0, removed: 0 });

        let refreshes: i64 = conn
            .query_row("SELECT COUNT(*) FROM dataset_refreshes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(refreshes, 1);
        let cells: i64 = conn
            .query_row("SELECT COUNT(*) FROM dataset_change_areas", [], |r| r.get(0))
            .unwrap();
        assert_eq!(cells, 0);
    }

    /// Churn above IMPLAUSIBLE_CHURN_FRACTION warns but must NOT block —
    /// a genuinely restructured source should still land.
    #[test]
    fn implausible_churn_warns_but_still_applies() {
        let live = "SELECT * FROM (VALUES ('a','v1',21.0,52.0), ('b','v1',21.0,52.0))
                    t(id,a,lon,lat)";
        let conn = conn_with_live(live);
        const ALL_NEW: &str = "SELECT * FROM (VALUES ('a','X',21.0,52.0), ('b','X',21.0,52.0))
                               t(id,a,lon,lat)";

        let counts = refresh(&conn, &TEST_SPEC, loader(ALL_NEW), None).unwrap();
        assert_eq!(counts, DiffCounts { added: 0, modified: 2, removed: 0 });

        let changed: i64 = conn
            .query_row("SELECT COUNT(*) FROM live WHERE a = 'X'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(changed, 2, "100% churn must still be applied");
    }

    /// The whole point of the design: a concurrent reader must see the old
    /// snapshot or the new one, never a half-applied state. The delta below
    /// changes the row count (3 -> 4), so an intermediate would be visible
    /// as any count outside {3, 4}.
    #[test]
    fn readers_never_observe_a_partial_apply() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        const BEFORE: &str = "SELECT * FROM (VALUES
            ('a','v1',21.0,52.0), ('b','v1',21.0,52.0), ('c','v1',21.0,52.0)
          ) t(id,a,lon,lat)";
        // Same three rows, all modified, plus a fourth: a large delete+insert
        // with a net row-count change.
        const AFTER: &str = "SELECT * FROM (VALUES
            ('a','X',21.0,52.0), ('b','X',21.0,52.0), ('c','X',21.0,52.0),
            ('d','X',21.0,52.0)
          ) t(id,a,lon,lat)";

        let conn = conn_with_live(BEFORE);
        let reader = conn.try_clone().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_reader = stop.clone();

        let handle = std::thread::spawn(move || {
            let mut seen = Vec::new();
            while !stop_reader.load(Ordering::SeqCst) {
                if let Ok(n) =
                    reader.query_row("SELECT COUNT(*) FROM live", [], |r| r.get::<_, i64>(0))
                {
                    seen.push(n);
                }
            }
            seen
        });

        refresh(&conn, &TEST_SPEC, loader(AFTER), None).unwrap();
        stop.store(true, Ordering::SeqCst);
        let seen = handle.join().unwrap();

        assert!(!seen.is_empty(), "reader thread observed nothing");
        for n in &seen {
            assert!(
                *n == 3 || *n == 4,
                "reader saw a partially-applied state: {n} rows (expected 3 or 4). \
                 Observed sequence: {seen:?}"
            );
        }
    }
}
```

Add `mod dataset;` to `src/update/mod.rs`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test update::dataset`
Expected: FAIL — all nine tests panic with `not implemented`.

- [ ] **Step 3: Write minimal implementation**

Replace the `refresh` stub:

```rust
/// Drops the staging table on every exit path, including early returns and
/// errors. DuckDB has no temp-table-per-transaction semantics here, so this
/// is the only thing standing between a failed refresh and a stale staging
/// table blocking the next one.
struct StagingGuard<'a> {
    conn: &'a Connection,
    table: String,
}

impl Drop for StagingGuard<'_> {
    fn drop(&mut self) {
        if let Err(e) = self
            .conn
            .execute_batch(&format!("DROP TABLE IF EXISTS {}", self.table))
        {
            warn!(table = %self.table, error = %e, "failed to drop staging table");
        }
    }
}

/// Stage a new snapshot, diff it against the live table, and apply the delta
/// in a single transaction together with the changeset.
///
/// `load` must create the staging table named by `spec.staging_table()`,
/// including a `_row_hash` column (use `crate::dataset::hashed_select`).
pub fn refresh(
    conn: &Connection,
    spec: &DatasetSpec,
    load: impl FnOnce(&Connection, &str) -> Result<()>,
    source_etag: Option<&str>,
) -> Result<DiffCounts> {
    let total = std::time::Instant::now();
    let staging = spec.staging_table();

    conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging}"))
        .with_context(|| format!("Failed to clear stale staging table {staging}"))?;

    let _guard = StagingGuard {
        conn,
        table: staging.clone(),
    };

    // --- stage ---
    let t = std::time::Instant::now();
    load(conn, &staging).with_context(|| format!("Failed to stage {} snapshot", spec.name))?;
    let staged: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {staging}"), [], |row| row.get(0))
        .with_context(|| format!("Failed to count rows in {staging}"))?;
    info!(
        source = spec.name,
        rows = staged,
        elapsed = %format_duration(t.elapsed()),
        "Step done: stage snapshot"
    );

    // The load-bearing guard: an empty snapshot would delete the dataset.
    if staged == 0 {
        bail!(
            "Staged snapshot for {} has 0 rows — refusing to apply, \
             which would delete the entire live dataset. The download is \
             most likely empty or truncated.",
            spec.name
        );
    }

    check_row_hash_version(conn)?;

    // --- diff ---
    let t = std::time::Instant::now();
    let counts = diff::compute(conn, spec)?;
    info!(
        source = spec.name,
        added = counts.added,
        modified = counts.modified,
        removed = counts.removed,
        elapsed = %format_duration(t.elapsed()),
        "Step done: diff snapshot"
    );

    let live_rows: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {}", spec.table), [], |row| {
            row.get(0)
        })
        .with_context(|| format!("Failed to count rows in {}", spec.table))?;
    let churn = counts.added + counts.modified + counts.removed;
    if live_rows > 0 && (churn as f64) > (live_rows as f64) * IMPLAUSIBLE_CHURN_FRACTION {
        warn!(
            source = spec.name,
            churn,
            live_rows,
            "implausibly large change set (>{:.0}% of rows) — proceeding, but this \
             usually means the source was restructured rather than genuinely changed",
            IMPLAUSIBLE_CHURN_FRACTION * 100.0
        );
    }

    // --- apply ---
    let t = std::time::Instant::now();
    let id = spec.id_column;
    let live = spec.table;
    conn.execute_batch("BEGIN TRANSACTION")
        .context("Failed to begin apply transaction")?;

    let applied = (|| -> Result<i64> {
        // Allocated inside the transaction so the read and the write that
        // consumes it cannot be split by a concurrent refresh. The PRIMARY KEY
        // on snapshot_id is the backstop if two ever do overlap.
        let snapshot_id: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM dataset_refreshes",
                [],
                |row| row.get(0),
            )
            .context("Failed to allocate snapshot_id")?;

        // Change areas FIRST. insert_change_areas reads the OLD geometry of
        // removed and modified objects out of the live table — the cell each
        // object is leaving — and the DELETE below is about to destroy it.
        // Reordering these two produces a silently wrong changeset, not an
        // error: removed objects contribute nothing, and a moved object marks
        // its destination cell twice instead of both cells. Nothing in the
        // schema enforces this, so do not move it.
        insert_change_areas(conn, spec, snapshot_id)?;

        conn.execute_batch(&format!(
            "DELETE FROM {live} WHERE {id} IN (
                 SELECT id FROM diff_removed UNION ALL SELECT id FROM diff_modified);
             INSERT INTO {live} SELECT * FROM {staging} WHERE {id} IN (
                 SELECT id FROM diff_added UNION ALL SELECT id FROM diff_modified);"
        ))
        .with_context(|| format!("Failed to apply delta to {live}"))?;

        // source_etag comes from a remote HTTP server. Bind it; never
        // interpolate it into the statement text.
        conn.execute(
            "INSERT INTO dataset_refreshes VALUES (?, ?, now(), now(), ?, ?, ?, ?)",
            duckdb::params![
                snapshot_id,
                spec.name,
                source_etag,
                counts.added,
                counts.modified,
                counts.removed
            ],
        )
        .context("Failed to record refresh")?;

        Ok(snapshot_id)
    })();

    let snapshot_id = match applied {
        Ok(id) => {
            // A failed COMMIT leaves the transaction open, poisoning the
            // pooled connection for the next caller — roll back here too.
            if let Err(e) = conn.execute_batch("COMMIT") {
                if let Err(rb) = conn.execute_batch("ROLLBACK") {
                    warn!(error = %rb, "failed to roll back after failed commit");
                }
                return Err(e).context("Failed to commit apply transaction");
            }
            id
        }
        Err(e) => {
            if let Err(rb) = conn.execute_batch("ROLLBACK") {
                warn!(error = %rb, "failed to roll back apply transaction");
            }
            return Err(e);
        }
    };

    info!(
        source = spec.name,
        snapshot_id,
        elapsed = %format_duration(t.elapsed()),
        "Step done: apply delta"
    );
    info!(
        source = spec.name,
        added = counts.added,
        modified = counts.modified,
        removed = counts.removed,
        elapsed = %format_duration(total.elapsed()),
        "Dataset refresh complete"
    );

    Ok(counts)
}

/// A DuckDB upgrade can change `hash()` output, which makes every row compare
/// as modified. That is correct but slow and produces a misleadingly large
/// changeset, so warn loudly and explain the cause — but do not block.
fn check_row_hash_version(conn: &Connection) -> Result<()> {
    // Distinguish "no row yet" (first run) from a genuine query failure —
    // `.ok()` would conflate them and silently re-insert on a broken database.
    let stored: Option<String> = match conn.query_row(
        "SELECT value FROM metadata WHERE key = ?",
        duckdb::params![ROW_HASH_VERSION_KEY],
        |row| row.get(0),
    ) {
        Ok(v) => Some(v),
        Err(duckdb::Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e).context("Failed to read row hash version"),
    };

    match stored {
        Some(v) if v == ROW_HASH_VERSION.to_string() => {}
        Some(v) => warn!(
            stored = %v,
            expected = ROW_HASH_VERSION,
            "row hash version mismatch — every row will compare as modified. \
             This refresh is effectively a full rewrite. Re-run the full import \
             to resync."
        ),
        None => {
            conn.execute(
                "INSERT INTO metadata VALUES (?, ?)",
                duckdb::params![ROW_HASH_VERSION_KEY, ROW_HASH_VERSION.to_string()],
            )
            .context("Failed to record row hash version")?;
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test update::dataset`
Expected: PASS — 9 passed.

If `readers_never_observe_a_partial_apply` fails intermittently with counts other than 3 or 4, that is a genuine finding, not a flaky test: it means the apply is not actually atomic to concurrent readers. Do not weaken the assertion — investigate the transaction boundary.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings
git add src/update/dataset.rs src/update/mod.rs
git commit -m "feat: add dataset refresh orchestration with atomic delta apply"
```

---

### Task 10: Update fixtures

The integration tests need a second, changed snapshot per source. Generate them from the existing fixtures so the diff has known, asserted content.

**Files:**
- Create: `fixtures/scripts/prepare_update_fixtures.sh`
- Create: `fixtures/bdot10k_v2.parquet`, `fixtures/egib_v2.parquet` (generated)
- Modify: `fixtures/scripts/prepare_fixtures.sh` (call the new script)

**Interfaces:**
- Consumes: existing `fixtures/bdot10k.parquet` (74 rows), `fixtures/egib.parquet`.
- Produces: `fixtures/bdot10k_v2.parquet` — same as v1 but with exactly 1 row deleted, 1 row's `LICZBAKONDYGNACJI` changed, and 1 row added; same for `fixtures/egib_v2.parquet` via `kondygnacje_nadziemne`.

- [ ] **Step 1: Write the generator script**

Create `fixtures/scripts/prepare_update_fixtures.sh`:

```bash
#!/usr/bin/env bash
# Generate "v2" fixture snapshots for the update/diff integration tests.
#
# Each v2 file differs from its v1 counterpart by exactly:
#   - 1 row removed  (the row with the lexicographically smallest id)
#   - 1 row modified (storey count bumped on the largest id)
#   - 1 row added    (a copy of the largest id's row under a synthetic id)
#
# Those exact counts are asserted by tests/cli_update_*.rs — if you change
# this script, change those assertions too.
set -euo pipefail
cd "$(dirname "$0")/.."

duckdb -c "
SET enable_geoparquet_conversion = false;
COPY (
  WITH ranked AS (SELECT *, row_number() OVER (ORDER BY LOKALNYID) rn,
                         count(*) OVER () n FROM 'bdot10k.parquet')
  SELECT * EXCLUDE (rn, n) FROM ranked WHERE rn > 1 AND rn < n
  UNION ALL
  SELECT * EXCLUDE (rn, n) REPLACE (
      COALESCE(LICZBAKONDYGNACJI, 0) + 1 AS LICZBAKONDYGNACJI)
    FROM ranked WHERE rn = n
  UNION ALL
  SELECT * EXCLUDE (rn, n) REPLACE (LOKALNYID || '_ADDED' AS LOKALNYID)
    FROM ranked WHERE rn = n
) TO 'bdot10k_v2.parquet' (FORMAT PARQUET);
"

duckdb -c "
SET enable_geoparquet_conversion = false;
COPY (
  WITH ranked AS (SELECT *, row_number() OVER (ORDER BY id_budynku) rn,
                         count(*) OVER () n FROM 'egib.parquet')
  SELECT * EXCLUDE (rn, n) FROM ranked WHERE rn > 1 AND rn < n
  UNION ALL
  SELECT * EXCLUDE (rn, n) REPLACE (
      COALESCE(kondygnacje_nadziemne, 0) + 1 AS kondygnacje_nadziemne)
    FROM ranked WHERE rn = n
  UNION ALL
  SELECT * EXCLUDE (rn, n) REPLACE (id_budynku || '_ADDED' AS id_budynku)
    FROM ranked WHERE rn = n
) TO 'egib_v2.parquet' (FORMAT PARQUET);
"

echo "Wrote bdot10k_v2.parquet and egib_v2.parquet"
```

Make it executable: `chmod +x fixtures/scripts/prepare_update_fixtures.sh`

- [ ] **Step 2: Run the generator**

Run: `./fixtures/scripts/prepare_update_fixtures.sh`
Expected: `Wrote bdot10k_v2.parquet and egib_v2.parquet`

- [ ] **Step 3: Verify the fixtures differ as intended**

Run:

```bash
duckdb -c "
SET enable_geoparquet_conversion = false;
SELECT (SELECT count(*) FROM 'fixtures/bdot10k.parquet') AS v1,
       (SELECT count(*) FROM 'fixtures/bdot10k_v2.parquet') AS v2,
       (SELECT count(*) FROM (SELECT LOKALNYID FROM 'fixtures/bdot10k_v2.parquet'
          ANTI JOIN 'fixtures/bdot10k.parquet' USING (LOKALNYID))) AS added,
       (SELECT count(*) FROM (SELECT LOKALNYID FROM 'fixtures/bdot10k.parquet'
          ANTI JOIN 'fixtures/bdot10k_v2.parquet' USING (LOKALNYID))) AS removed;
"
```

Expected: `v1 = 74`, `v2 = 74`, `added = 1`, `removed = 1`.

- [ ] **Step 4: Hook into the main fixture script**

Add to the end of `fixtures/scripts/prepare_fixtures.sh`:

```bash
"$(dirname "$0")/prepare_update_fixtures.sh"
```

- [ ] **Step 5: Commit**

```bash
git add fixtures/scripts/prepare_update_fixtures.sh fixtures/scripts/prepare_fixtures.sh \
        fixtures/bdot10k_v2.parquet fixtures/egib_v2.parquet
git commit -m "test: add v2 fixture snapshots for update integration tests"
```

---

### Task 11: CLI wiring and integration tests

Replace the three `bail!`s and prove the whole pipeline end to end against a real file-backed database.

**Files:**
- Modify: `src/cli.rs` (add `--file` / `--terc-file` to the update subcommands)
- Modify: `src/update/mod.rs` (dispatch)
- Create: `tests/cli_update_bdot10k.rs`
- Create: `tests/cli_update_egib.rs`

**Interfaces:**
- Consumes: `crate::update::dataset::refresh`, `crate::import::{bdot10k, egib, prg}::load_into` / `materialize_into`.
- Produces: working `update bdot10k|egib|prg [--file <path>]` CLI subcommands.

- [ ] **Step 1: Write the failing test**

Create `tests/cli_update_bdot10k.rs`:

```rust
use assert_cmd::Command;

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("osmpbudynkiv2").unwrap();
    cmd.env("NO_COLOR", "1");
    cmd
}

/// Update needs a file-backed database: import and update are separate
/// process invocations, so ":memory:" would start each with an empty DB.
fn file_config() -> (tempfile::NamedTempFile, tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("test.duckdb");
    let rocksdb_path = dir.path().join("test.rocksdb");
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    write!(
        tmp,
        "db_path = \"{}\"\nrocksdb_path = \"{}\"\n",
        db_path.display(),
        rocksdb_path.display()
    )
    .unwrap();
    (tmp, dir, db_path)
}

#[test]
fn test_update_bdot10k_applies_delta_and_records_changeset() {
    let (cfg, _dir, db_path) = file_config();

    cmd()
        .args([
            "--config", cfg.path().to_str().unwrap(),
            "import", "bdot10k", "--file", "fixtures/bdot10k.parquet",
        ])
        .assert()
        .success();

    cmd()
        .args([
            "--config", cfg.path().to_str().unwrap(),
            "update", "bdot10k", "--file", "fixtures/bdot10k_v2.parquet",
        ])
        .assert()
        .success();

    let conn = duckdb::Connection::open(&db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;").unwrap();

    // v2 has 1 added, 1 removed, 1 modified relative to v1.
    let (added, modified, removed): (i32, i32, i32) = conn
        .query_row(
            "SELECT added, modified, removed FROM dataset_refreshes WHERE source = 'bdot10k'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!((added, modified, removed), (1, 1, 1));

    // Row count is unchanged (one in, one out) and the added row is present.
    let total: i64 = conn
        .query_row("SELECT COUNT(*) FROM bdot10k_buildings", [], |r| r.get(0))
        .unwrap();
    assert_eq!(total, 74);

    let added_present: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM bdot10k_buildings WHERE LOKALNYID LIKE '%_ADDED'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(added_present, 1);

    let cells: i64 = conn
        .query_row("SELECT COUNT(*) FROM dataset_change_areas", [], |r| r.get(0))
        .unwrap();
    assert!(cells > 0, "expected change areas to be recorded");

    // Staging must not survive the run.
    let staging: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables
             WHERE table_name = 'bdot10k_buildings__staging'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(staging, 0, "staging table leaked");
}

#[test]
fn test_update_bdot10k_unchanged_snapshot_is_a_noop() {
    let (cfg, _dir, db_path) = file_config();

    cmd()
        .args([
            "--config", cfg.path().to_str().unwrap(),
            "import", "bdot10k", "--file", "fixtures/bdot10k.parquet",
        ])
        .assert()
        .success();
    cmd()
        .args([
            "--config", cfg.path().to_str().unwrap(),
            "update", "bdot10k", "--file", "fixtures/bdot10k.parquet",
        ])
        .assert()
        .success();

    let conn = duckdb::Connection::open(&db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;").unwrap();
    let (added, modified, removed): (i32, i32, i32) = conn
        .query_row(
            "SELECT added, modified, removed FROM dataset_refreshes",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!((added, modified, removed), (0, 0, 0));

    let cells: i64 = conn
        .query_row("SELECT COUNT(*) FROM dataset_change_areas", [], |r| r.get(0))
        .unwrap();
    assert_eq!(cells, 0);
}

#[test]
fn test_update_bdot10k_missing_file_fails() {
    let (cfg, _dir, _db) = file_config();
    cmd()
        .args([
            "--config", cfg.path().to_str().unwrap(),
            "import", "bdot10k", "--file", "fixtures/bdot10k.parquet",
        ])
        .assert()
        .success();
    cmd()
        .args([
            "--config", cfg.path().to_str().unwrap(),
            "update", "bdot10k", "--file", "nonexistent.parquet",
        ])
        .assert()
        .failure();
}
```

Create `tests/cli_update_egib.rs` as the same three tests with `egib`, `egib_buildings`, `id_budynku`, `fixtures/egib.parquet` and `fixtures/egib_v2.parquet`. Do not assert `total == 74` — read the EGIB fixture's real row count from `tests/cli_import_egib.rs` and use that.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test cli_update_bdot10k`
Expected: FAIL — the CLI rejects `--file` for `update bdot10k`, or the command errors with "BDOT10k update is not yet implemented".

- [ ] **Step 3: Add the CLI flags**

In `src/cli.rs`, replace the `UpdateSource` enum:

```rust
#[derive(Subcommand)]
pub enum UpdateSource {
    /// Update OpenStreetMap data from replication feed
    Osm,
    /// Update BDOT10k building data from a fresh snapshot
    Bdot10k {
        /// Path to local file (skips download)
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// Update EGIB building data from a fresh snapshot
    Egib {
        /// Path to local file (skips download)
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// Update PRG address data from a fresh snapshot
    Prg {
        /// Path to local file (skips download)
        #[arg(long)]
        file: Option<PathBuf>,
        /// Path to a TERC (TERYT) dictionary file (.zip or .xml).
        #[arg(long)]
        terc_file: Option<PathBuf>,
    },
}
```

- [ ] **Step 4: Wire the dispatch**

Rewrite `src/update/mod.rs`'s `run` (keeping the existing `pub mod osm;` and adding the new modules):

```rust
pub mod changeset;
pub mod dataset;
pub mod diff;
pub mod osm;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;

use crate::cli::UpdateSource;
use crate::config::{Config, DownloadUrls};
use crate::dataset as spec;
use crate::download::{download_file, download_file_as};
use crate::osm::kvstore::RocksDB;

pub fn run(
    conn: &Connection,
    kv: &RocksDB,
    source: UpdateSource,
    config: &Config,
    urls: &DownloadUrls,
) -> Result<()> {
    match source {
        UpdateSource::Osm => osm::update(conn, kv, config, &urls.osm_replication),
        UpdateSource::Bdot10k { file } => {
            let path = resolve(file.as_deref(), config, &urls.bdot10k, None)?;
            let p = path_str(&path)?;
            dataset::refresh(
                conn,
                &spec::BDOT10K,
                |c, target| crate::import::bdot10k::load_into(c, target, &p),
                None,
            )
            .map(|_| ())
        }
        UpdateSource::Egib { file } => {
            let path = resolve(file.as_deref(), config, &urls.egib, None)?;
            let p = path_str(&path)?;
            dataset::refresh(
                conn,
                &spec::EGIB,
                |c, target| crate::import::egib::load_into(c, target, &p),
                None,
            )
            .map(|_| ())
        }
        UpdateSource::Prg { file, terc_file } => {
            crate::import::prg::update_prg(conn, config, file.as_deref(), terc_file.as_deref(), &urls.prg)
        }
    }
}

/// Resolve a local path or download the snapshot, then verify it is a
/// non-empty regular file BEFORE any staging work begins.
fn resolve(
    file: Option<&Path>,
    config: &Config,
    url: &str,
    download_as: Option<&str>,
) -> Result<PathBuf> {
    let path = match file {
        Some(p) => p.to_path_buf(),
        None => match download_as {
            Some(name) => download_file_as(url, &config.download_dir(), name)?,
            None => download_file(url, &config.download_dir())?,
        },
    };
    let meta = std::fs::metadata(&path)
        .with_context(|| format!("Source file {} is not readable", path.display()))?;
    if !meta.is_file() {
        anyhow::bail!("Source path {} is not a regular file", path.display());
    }
    if meta.len() == 0 {
        anyhow::bail!(
            "Source file {} is empty — refusing to proceed",
            path.display()
        );
    }
    Ok(path)
}

fn path_str(p: &Path) -> Result<String> {
    p.to_str()
        .map(str::to_string)
        .with_context(|| format!("Path {} is not valid UTF-8", p.display()))
}
```

PRG needs its own wrapper because staging requires streaming GML into a raw table first. Add to `src/import/prg.rs`:

```rust
/// Refresh `prg_addresses` from a fresh snapshot, reusing the import
/// streaming path to build the staging table.
pub fn update_prg(
    conn: &Connection,
    config: &Config,
    file: Option<&Path>,
    terc_file: Option<&Path>,
    url: &str,
) -> Result<()> {
    let (zip_path, terc) = prepare_source(conn, config, file, terc_file, url)?;
    crate::update::dataset::refresh(
        conn,
        &crate::dataset::PRG,
        |c, target| {
            let raw = format!("{target}_raw");
            stream_gml_into(c, &zip_path, &terc, &raw)?;
            materialize_into(c, target, &raw)
        },
        None,
    )
    .map(|_| ())
}
```

To make that compile, split the existing `import` function in `src/import/prg.rs` into three reusable pieces without changing its behavior:

- `prepare_source(...) -> Result<(PathBuf, Arc<TerytMapping>)>` — everything from resolving the zip path through building the TERC mapping (the current lines that download/resolve the zip and construct `terc`). Return the zip path and the `Arc`-wrapped mapping.
- `stream_gml_into(conn, zip_path, terc, raw_table) -> Result<()>` — the loop that enumerates GML entries and streams arrow batches into `raw_table` (currently hard-coded to `prg_addresses_raw`); parameterize the table name.
- `import(...)` — now calls `prepare_source`, `stream_gml_into(conn, &zip, &terc, "prg_addresses_raw")`, `materialize_into(conn, PRG.table, "prg_addresses_raw")`, then creates the index and logs, exactly as before.

Use the exact TERC type the existing code produces for the `Arc<...>` parameter — read the current `import` body and copy the type rather than guessing.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --test cli_update_bdot10k --test cli_update_egib`
Expected: PASS — 6 passed.

- [ ] **Step 6: Verify nothing regressed**

Run: `cargo test && cargo clippy -- -D warnings && cargo fmt -- --check`
Expected: all tests pass, including the existing import tests.

- [ ] **Step 7: Commit**

```bash
git add src/cli.rs src/update/mod.rs src/import/prg.rs \
        tests/cli_update_bdot10k.rs tests/cli_update_egib.rs
git commit -m "feat: implement update bdot10k/egib/prg CLI subcommands"
```

---

### Task 12: Skip-if-unchanged via conditional request

Make a daily schedule affordable: a `HEAD` request compares the source's `ETag` / `Last-Modified` against the last successful refresh and skips the download when nothing changed.

**Files:**
- Modify: `src/download.rs` (add `fetch_etag`)
- Modify: `src/update/mod.rs` (consult it before downloading)

**Interfaces:**
- Consumes: `dataset_refreshes.source_etag`.
- Produces: `crate::download::fetch_etag(url: &str) -> Result<Option<String>>` — returns the `ETag`, else `Last-Modified`, else `None`.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/download.rs`:

```rust
async fn serve_head(header_line: &'static str) -> Option<String> {
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: std::net::SocketAddr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let _ = stream.readable().await;
        let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;
        let resp = format!("HTTP/1.1 200 OK\r\n{header_line}Content-Length: 0\r\n\r\n");
        tokio::io::AsyncWriteExt::write_all(&mut stream, resp.as_bytes())
            .await
            .unwrap();
    });

    let client = reqwest::Client::new();
    do_fetch_etag(&client, &format!("http://{addr}/f.bin"))
        .await
        .unwrap()
}

#[tokio::test]
async fn etag_header_is_preferred() {
    let v = serve_head("ETag: \"abc123\"\r\nLast-Modified: Wed, 01 Jan 2025 00:00:00 GMT\r\n").await;
    assert_eq!(v.as_deref(), Some("\"abc123\""));
}

#[tokio::test]
async fn falls_back_to_last_modified() {
    let v = serve_head("Last-Modified: Wed, 01 Jan 2025 00:00:00 GMT\r\n").await;
    assert_eq!(v.as_deref(), Some("Wed, 01 Jan 2025 00:00:00 GMT"));
}

#[tokio::test]
async fn returns_none_when_server_offers_no_validator() {
    let v = serve_head("").await;
    assert_eq!(v, None);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib download::tests`
Expected: FAIL to compile — `cannot find function do_fetch_etag`.

- [ ] **Step 3: Write minimal implementation**

Add to `src/download.rs`:

```rust
/// Fetch a cheap change validator for `url` via HEAD: the `ETag` if the
/// server sends one, else `Last-Modified`, else `None`.
///
/// `None` means "cannot tell" and callers MUST treat it as changed — never
/// as unchanged — or a refresh could be skipped forever.
pub fn fetch_etag(url: &str) -> Result<Option<String>> {
    let rt = Runtime::new().context("Failed to create tokio runtime")?;
    let client = reqwest::Client::new();
    rt.block_on(do_fetch_etag(&client, url))
}

async fn do_fetch_etag(client: &reqwest::Client, url: &str) -> Result<Option<String>> {
    let response = client.head(url).send().await?.error_for_status()?;
    let headers = response.headers();
    let value = headers
        .get(reqwest::header::ETAG)
        .or_else(|| headers.get(reqwest::header::LAST_MODIFIED))
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    Ok(value)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib download::tests`
Expected: PASS — the three new tests plus the two existing download tests.

- [ ] **Step 5: Consult it before downloading**

In `src/update/mod.rs`, add a helper and call it from the three dataset arms before `resolve`:

```rust
/// True when the remote snapshot still carries the validator recorded by the
/// last successful refresh of `source`.
///
/// A missing validator on either side means "unknown", which is treated as
/// changed — skipping must only ever happen on a positive match.
fn source_unchanged(conn: &Connection, source: &str, url: &str) -> Result<(bool, Option<String>)> {
    let remote = match crate::download::fetch_etag(url) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(source, error = %e, "HEAD check failed; downloading anyway");
            return Ok((false, None));
        }
    };
    let Some(remote) = remote else {
        return Ok((false, None));
    };

    let stored: Option<String> = conn
        .query_row(
            "SELECT source_etag FROM dataset_refreshes
             WHERE source = ? AND source_etag IS NOT NULL
             ORDER BY snapshot_id DESC LIMIT 1",
            duckdb::params![source],
            |row| row.get(0),
        )
        .ok();

    Ok((stored.as_deref() == Some(remote.as_str()), Some(remote)))
}
```

Then in each dataset arm, when `file` is `None`, check first. For BDOT10k:

```rust
UpdateSource::Bdot10k { file } => {
    let mut etag = None;
    if file.is_none() {
        let (unchanged, remote) = source_unchanged(conn, spec::BDOT10K.name, &urls.bdot10k)?;
        etag = remote;
        if unchanged {
            tracing::info!(source = spec::BDOT10K.name, "source unchanged; skipping refresh");
            record_noop_refresh(conn, spec::BDOT10K.name, etag.as_deref())?;
            return Ok(());
        }
    }
    let path = resolve(file.as_deref(), config, &urls.bdot10k, None)?;
    let p = path_str(&path)?;
    dataset::refresh(
        conn,
        &spec::BDOT10K,
        |c, target| crate::import::bdot10k::load_into(c, target, &p),
        etag.as_deref(),
    )
    .map(|_| ())
}
```

Apply the same shape to the EGIB and PRG arms (PRG passes `Some(PRG_DOWNLOAD_FILENAME)` as `download_as`). Add the no-op recorder:

```rust
/// Record a refresh that ran but had nothing to do, so "ran and found no
/// changes" stays distinguishable from "never ran" in /status.
fn record_noop_refresh(conn: &Connection, source: &str, etag: Option<&str>) -> Result<()> {
    // Same rule as the apply path: allocate snapshot_id in the same
    // transaction that consumes it, so the MAX+1 read cannot be split from
    // the INSERT by a concurrent refresh.
    conn.execute_batch("BEGIN TRANSACTION")
        .context("Failed to begin no-op refresh transaction")?;

    let recorded = (|| -> Result<()> {
        let snapshot_id: i64 = conn.query_row(
            "SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM dataset_refreshes",
            [],
            |row| row.get(0),
        )?;
        conn.execute(
            "INSERT INTO dataset_refreshes VALUES (?, ?, now(), now(), ?, 0, 0, 0)",
            duckdb::params![snapshot_id, source, etag],
        )
        .context("Failed to record no-op refresh")?;
        Ok(())
    })();

    match recorded {
        Ok(()) => {
            if let Err(e) = conn.execute_batch("COMMIT") {
                if let Err(rb) = conn.execute_batch("ROLLBACK") {
                    tracing::warn!(error = %rb, "failed to roll back after failed commit");
                }
                return Err(e).context("Failed to commit no-op refresh");
            }
        }
        Err(e) => {
            if let Err(rb) = conn.execute_batch("ROLLBACK") {
                tracing::warn!(error = %rb, "failed to roll back no-op refresh");
            }
            return Err(e);
        }
    }
    Ok(())
}
```

- [ ] **Step 6: Verify nothing regressed**

Run: `cargo test && cargo clippy -- -D warnings && cargo fmt -- --check`
Expected: all pass. The `--file` integration tests from Task 11 must be unaffected, since the HEAD check only runs when `file` is `None`.

- [ ] **Step 7: Commit**

```bash
git add src/download.rs src/update/mod.rs
git commit -m "feat: skip dataset refresh when source ETag is unchanged"
```

---

### Task 13: Background jobs and config

Register the three refreshes as scheduled jobs, with a shared mutex so they cannot all stage ~16M rows at once against a 4GB memory limit.

**Files:**
- Modify: `src/config.rs` (three `JobConfig` fields + defaults + tests)
- Create: `src/server/jobs/dataset_update.rs`
- Modify: `src/server/jobs/mod.rs` (add `pub mod dataset_update;`)
- Modify: `src/server/mod.rs` (register the jobs)
- Modify: `example_config.toml` (document the new sections)

**Interfaces:**
- Consumes: `crate::update::run`, `crate::dataset::{BDOT10K, EGIB, PRG}`.
- Produces: `crate::server::jobs::dataset_update::DatasetUpdateJob` implementing `Job`, constructed as `DatasetUpdateJob::new(spec: &'static DatasetSpec, name: &'static str)`. `name` is the registry/config key (`"bdot10k_update"`), which is deliberately distinct from `spec.name` (`"bdot10k"`), the changeset source label.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/config.rs`:

```rust
#[test]
fn test_dataset_update_job_defaults() {
    let config = Config::default();
    assert!(config.jobs.bdot10k_update.enabled);
    assert_eq!(config.jobs.bdot10k_update.interval_seconds, 86400);
    assert_eq!(config.jobs.bdot10k_update.timeout_seconds, 3600);
    assert!(config.jobs.egib_update.enabled);
    assert_eq!(config.jobs.egib_update.interval_seconds, 86400);
    assert_eq!(config.jobs.egib_update.timeout_seconds, 3600);
    assert!(config.jobs.prg_update.enabled);
    assert_eq!(config.jobs.prg_update.interval_seconds, 86400);
    assert_eq!(config.jobs.prg_update.timeout_seconds, 7200);
}

#[test]
fn test_dataset_update_job_override() {
    let toml = r#"
[jobs.bdot10k_update]
enabled = false
interval_seconds = 3600
timeout_seconds = 300
"#;
    let config: Config = toml::from_str(toml).unwrap();
    assert!(!config.jobs.bdot10k_update.enabled);
    assert_eq!(config.jobs.bdot10k_update.interval_seconds, 3600);
    assert_eq!(config.jobs.bdot10k_update.timeout_seconds, 300);
    // Unrelated jobs keep their defaults.
    assert!(config.jobs.egib_update.enabled);
    assert_eq!(config.jobs.egib_update.interval_seconds, 86400);
}
```

Match the exact `toml::from_str` construction the neighbouring config tests use — read them first and copy the pattern.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test config::tests::test_dataset_update_job`
Expected: FAIL to compile — `no field bdot10k_update on type JobsConfig`.

- [ ] **Step 3: Add the config fields**

In `src/config.rs`, extend `JobsConfig` and give it an explicit `Default` (it currently derives one, which would use `JobConfig`'s 60s/600s defaults — wrong for these):

```rust
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct JobsConfig {
    pub osm_update: JobConfig,
    pub export_log_prune: ExportLogPruneConfig,
    pub bdot10k_update: JobConfig,
    pub egib_update: JobConfig,
    pub prg_update: JobConfig,
}

impl Default for JobsConfig {
    fn default() -> Self {
        // Government snapshots are republished irregularly, so a daily poll
        // is plenty; the ETag HEAD check makes a no-op poll nearly free.
        let daily = |timeout_seconds| JobConfig {
            enabled: true,
            interval_seconds: 86400,
            timeout_seconds,
        };
        Self {
            osm_update: JobConfig::default(),
            export_log_prune: ExportLogPruneConfig::default(),
            bdot10k_update: daily(3600),
            egib_update: daily(3600),
            // PRG streams ~16 GML files out of a ~1.7GB zip, so it needs longer.
            prg_update: daily(7200),
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test config::tests`
Expected: PASS.

- [ ] **Step 5: Add the job type**

Create `src/server/jobs/dataset_update.rs`:

```rust
use std::sync::{Mutex, OnceLock};

use anyhow::{Context, Result};

use crate::cli::UpdateSource;
use crate::dataset::DatasetSpec;
use crate::server::jobs::{Job, JobContext};

/// Serializes the three dataset refreshes against each other.
///
/// The scheduler's supervisor only guarantees no overlap *per job*, so
/// without this all three could stage ~16M rows simultaneously against the
/// configured `memory_limit`. They are not latency-sensitive, so running
/// them one at a time costs nothing that matters.
fn refresh_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Background job that refreshes one government dataset from a fresh snapshot.
pub struct DatasetUpdateJob {
    spec: &'static DatasetSpec,
    name: &'static str,
}

impl DatasetUpdateJob {
    pub fn new(spec: &'static DatasetSpec, name: &'static str) -> Self {
        Self { spec, name }
    }

    fn source(&self) -> UpdateSource {
        match self.spec.name {
            "bdot10k" => UpdateSource::Bdot10k { file: None },
            "egib" => UpdateSource::Egib { file: None },
            "prg" => UpdateSource::Prg { file: None, terc_file: None },
            other => unreachable!("unknown dataset spec {other}"),
        }
    }
}

impl Job for DatasetUpdateJob {
    fn name(&self) -> &'static str {
        self.name
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        // Poisoning only means a previous refresh panicked; the lock guards
        // memory headroom, not shared state, so recovering is correct.
        let _guard = refresh_lock().lock().unwrap_or_else(|e| e.into_inner());

        let conn = ctx.pool.get().context("failed to acquire pool connection")?;
        crate::update::run(
            &conn,
            &ctx.kv,
            self.source(),
            &ctx.config,
            &ctx.config.download_urls,
        )
    }
}
```

Add `pub mod dataset_update;` to `src/server/jobs/mod.rs`.

- [ ] **Step 6: Register the jobs**

In `src/server/mod.rs::run`, extend `job_list` after the existing two entries:

```rust
(
    Arc::new(jobs::dataset_update::DatasetUpdateJob::new(
        &crate::dataset::BDOT10K,
        "bdot10k_update",
    )) as Arc<dyn jobs::Job>,
    jobs::JobConfigResolved::from(&config.jobs.bdot10k_update),
),
(
    Arc::new(jobs::dataset_update::DatasetUpdateJob::new(
        &crate::dataset::EGIB,
        "egib_update",
    )) as Arc<dyn jobs::Job>,
    jobs::JobConfigResolved::from(&config.jobs.egib_update),
),
(
    Arc::new(jobs::dataset_update::DatasetUpdateJob::new(
        &crate::dataset::PRG,
        "prg_update",
    )) as Arc<dyn jobs::Job>,
    jobs::JobConfigResolved::from(&config.jobs.prg_update),
),
```

- [ ] **Step 7: Document the config**

Append to `example_config.toml` after the `[jobs.export_log_prune]` block:

```toml
# Government dataset refreshes. Each downloads the current snapshot, diffs it
# against the live table, and applies only the difference in a single
# transaction, so HTTP readers are never disrupted.
#
# A HEAD request compares the source's ETag/Last-Modified against the last
# successful refresh first, so a daily poll costs one round-trip when the
# publisher has not republished.
[jobs.bdot10k_update]
enabled = true
interval_seconds = 86400
timeout_seconds = 3600

[jobs.egib_update]
enabled = true
interval_seconds = 86400
timeout_seconds = 3600

# PRG streams ~16 GML files out of a ~1.7GB zip, so it gets a longer timeout.
[jobs.prg_update]
enabled = true
interval_seconds = 86400
timeout_seconds = 7200
```

- [ ] **Step 8: Verify the whole suite**

Run: `cargo test && cargo clippy -- -D warnings && cargo fmt -- --check`
Expected: everything passes.

- [ ] **Step 9: Verify the server still starts**

Run:

```bash
cargo build && ./target/debug/osmpbudynkiv2 --config ./example_config.toml run &
sleep 5 && curl -s localhost:3000/status | head -40 && kill %1
```

Expected: `/status` lists `bdot10k_update`, `egib_update` and `prg_update` alongside `osm_update` and `export_log_prune`. If the required source tables are absent the server will refuse to start — that is the pre-existing `check_startup_conditions` behavior, so run the imports first or point `db_path` at a database that already has them.

- [ ] **Step 10: Commit**

```bash
git add src/config.rs src/server/jobs/dataset_update.rs src/server/jobs/mod.rs \
        src/server/mod.rs example_config.toml
git commit -m "feat: schedule background refreshes for prg, bdot10k and egib"
```

---

## Final verification

- [ ] **Run the full suite**

Run: `cargo test && cargo clippy -- -D warnings && cargo fmt -- --check`
Expected: all green.

- [ ] **Confirm the spec's guard conditions are actually wired**

Each of these should be traceable to a test or an explicit code path:

| Spec condition | Where |
|---|---|
| Source file missing / zero-byte | `update::mod::resolve`, Task 12 Step 5; `test_update_bdot10k_missing_file_fails` |
| Staging yields 0 rows → abort | `empty_staging_aborts_and_leaves_live_untouched` |
| Apply failure → rollback | `refresh`'s ROLLBACK arm |
| Staging always dropped | `staging_table_is_always_cleaned_up` |
| Schema drift → abort | falls out of `INSERT INTO <live> SELECT * FROM <staging>` failing inside the transaction, which rolls back |
| Hash version mismatch → warn | `check_row_hash_version` |
| Implausible churn → warn, not block | `implausible_churn_warns_but_still_applies` |
| Readers never see a partial apply | `readers_never_observe_a_partial_apply` |
| Duplicate IDs replaced as a unit | `classifies_added_removed_and_modified` (the `dup` case) |
| Moved object marks both cells | `moved_object_marks_both_cells` |

- [ ] **Update the README**

Add `update bdot10k|egib|prg` to the CLI command list, noting that each takes an optional `--file` and that refreshes are also scheduled in the background by `run`.

- [ ] **Commit**

```bash
git add README.md
git commit -m "docs: document dataset update commands"
```
