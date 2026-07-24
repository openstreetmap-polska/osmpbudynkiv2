# Precomputed Unmatched Sets as the Serving Path — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `/tiles` and `/package` serve precomputed *unmatched* government objects, and keep those tables current as OSM and the government datasets change, via a dirty-cell queue drained by a background job.

**Architecture:** Three `*_unmatched` serving tables (replacing the unread `*_comparison` tables) hold only unmatched rows tagged with their z14 cell. A shared match rule defines "unmatched" in one place. The offline `compare` CLI does a full recompute; a new `match_refresh` background job does incremental per-cell recomputes fed by a `match_dirty_cells` queue that the OSM update and government refreshes enqueue into. Staleness surfaces on `/status` as queue depth.

**Tech Stack:** Rust, DuckDB (embedded, spatial + icu extensions), RocksDB (OSM KV), axum HTTP, `assert_cmd`/`tempfile` integration tests.

## Global Constraints

- **z14 everywhere.** All cell math uses `tile_math::CHANGE_CELL_ZOOM` (currently 14). Never hard-code 14.
- **One home per invariant.** The z14 cell→SQL projection and the match rule each live in exactly one place. Producers, `compare`, and `match_refresh` all call them; divergence is a correctness bug.
- **Read wide, write narrow.** Address recompute reads OSM from a buffered bbox but writes back only rows whose representative point is strictly inside the cell. Buildings need no buffer (any OSM polygon containing an in-cell point has a bbox intersecting the cell).
- **Serving tables store rows, not id-references.** BDOT10k `LOKALNYID` is not unique and DuckDB rowids are unstable across `DELETE`+`INSERT`.
- **Per-source dirty queue.** OSM *building* edits enqueue `bdot10k`+`egib`; OSM *address* edits enqueue `prg`. Government refreshes enqueue their own source.
- **OSM producers enqueue the 3×3 z14 neighbourhood** of each touched cell; government producers enqueue only the touched cell.
- **Ordering rules are load-bearing and unenforced by the schema:** (a) a drain reads and deletes queue rows under the same `enqueued_at <= batch_start` cutoff; (b) a cell's serving-table rewrite and its queue delete commit in one transaction.
- Match constants, carried over unchanged: address match = equal `UPPER(TRIM(housenumber))` within **50 m** (`ST_Distance_Sphere`), NULL housenumber never matches; building match = an `osm_buildings` polygon `ST_Contains` the government centroid. OSM read buffer = **0.001°** (`MATCH_BUFFER_DEG`, matches `/package` today).
- Follow existing patterns: inline `#[cfg(test)]` unit tests with in-memory DuckDB via `db::init_db`; integration tests in `tests/` via `assert_cmd` + file-backed DB.
- Run `cargo fmt` and `cargo clippy` clean before every commit.

---

# Phase 1 — Serve from precomputed tables (build order steps 1–3)

Independently shippable: after Phase 1 the endpoints serve unmatched sets that are correct at `compare` time but have no freshness guarantee (equivalent to today plus a repoint). No producers or job yet.

## Task 1: Shared z14 cell→SQL projection

Extract the cell-x/cell-y SQL currently hand-written in `update::changeset` into `tile_math`, so `changeset`, `compare`, and the future match code share one projection pinned against the Rust `lonlat_to_tile`.

**Files:**
- Modify: `src/tile_math.rs` (add `cell_x_sql` / `cell_y_sql`)
- Modify: `src/update/changeset.rs:33-45` (replace the local `cell_x`/`cell_y` closures with the shared functions)
- Test: inline in `src/tile_math.rs`

**Interfaces:**
- Produces:
  - `pub fn cell_x_sql(point_expr: &str) -> String` — SQL for the z14 tile X of a point expression.
  - `pub fn cell_y_sql(point_expr: &str) -> String` — SQL for the z14 tile Y.
  - Both use `CHANGE_CELL_ZOOM` and produce `INTEGER` results matching `lonlat_to_tile`.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/tile_math.rs`:

```rust
#[test]
fn cell_sql_matches_lonlat_to_tile() {
    use crate::db::init_db;
    use std::path::Path;
    let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
    let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
    for (lon, lat) in [(21.0, 52.0), (14.5, 49.35), (23.88, 54.54), (19.94, 50.06)] {
        let (rx, ry) = lonlat_to_tile(lon, lat, CHANGE_CELL_ZOOM);
        let sql = format!(
            "SELECT {}, {}",
            cell_x_sql(&format!("ST_Point({lon}, {lat})")),
            cell_y_sql(&format!("ST_Point({lon}, {lat})")),
        );
        let (sx, sy): (i32, i32) =
            conn.query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
        assert_eq!((sx as u32, sy as u32), (rx, ry), "mismatch at ({lon},{lat})");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib tile_math::tests::cell_sql_matches_lonlat_to_tile`
Expected: FAIL to compile — `cell_x_sql`/`cell_y_sql` not found.

- [ ] **Step 3: Add the shared functions**

Add to `src/tile_math.rs` (module level):

```rust
/// SQL for the Web-Mercator XYZ tile X of `point_expr` at [`CHANGE_CELL_ZOOM`].
/// The Rust inverse is [`lonlat_to_tile`]; `cell_sql_matches_lonlat_to_tile`
/// pins the two together. This is the ONLY home for the SQL projection.
pub fn cell_x_sql(point_expr: &str) -> String {
    let n = format!("pow(2, {})", CHANGE_CELL_ZOOM);
    format!("floor((ST_X({point_expr}) + 180) / 360 * {n})::INTEGER")
}

/// SQL for the Web-Mercator XYZ tile Y of `point_expr` at [`CHANGE_CELL_ZOOM`].
pub fn cell_y_sql(point_expr: &str) -> String {
    let n = format!("pow(2, {})", CHANGE_CELL_ZOOM);
    format!(
        "floor((1 - ln(tan(radians(ST_Y({point_expr}))) + 1 / cos(radians(ST_Y({point_expr})))) \
         / pi()) / 2 * {n})::INTEGER"
    )
}
```

- [ ] **Step 4: Repoint `changeset.rs` to the shared functions**

In `src/update/changeset.rs`, delete the local `cell_x`/`cell_y` closures (lines ~33-41) and their `let z`/`let n`, and replace their uses:

```rust
    let point_live = spec.representative_point_sql("l.geom");
    let point_stg = spec.representative_point_sql("s.geom");

    let sx = crate::tile_math::cell_x_sql(&point_stg);
    let sy = crate::tile_math::cell_y_sql(&point_stg);
    let lx = crate::tile_math::cell_x_sql(&point_live);
    let ly = crate::tile_math::cell_y_sql(&point_live);
    let z = crate::tile_math::CHANGE_CELL_ZOOM;
```

Leave the rest of `insert_change_areas` (the `sx = ...` interpolation in the `format!`) unchanged — it already references `sx/sy/lx/ly`.

- [ ] **Step 5: Run the tile_math and changeset tests**

Run: `cargo test --lib tile_math:: && cargo test --lib update::changeset::`
Expected: PASS (all existing changeset tests still green; new cell-SQL test green).

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy --all-targets 2>&1 | tail -5
git add src/tile_math.rs src/update/changeset.rs
git commit -m "refactor: extract shared z14 cell->SQL projection into tile_math"
```

## Task 2: Schema — `*_unmatched` serving tables and `match_dirty_cells`

Add the serving tables and the dirty queue to `create_schema`, and retire the runtime-created `*_comparison` tables. Serving tables are created here (idempotent) so the server can read them before the first `compare`, and so `match_refresh` can `DELETE`+`INSERT` per cell.

**Files:**
- Modify: `src/db.rs` (`create_schema`, add tables + a schema round-trip test)
- Test: inline in `src/db.rs`

**Interfaces:**
- Produces (DuckDB tables): `bdot10k_unmatched`, `egib_unmatched`, `prg_unmatched`, `match_dirty_cells` with the column layouts below.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/db.rs`:

```rust
#[test]
fn test_init_db_creates_serving_and_queue_tables() -> Result<()> {
    let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
    let conn = init_db(Path::new(":memory:"), &init, None)?;
    for table in [
        "bdot10k_unmatched",
        "egib_unmatched",
        "prg_unmatched",
        "match_dirty_cells",
    ] {
        let n: i64 =
            conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?;
        assert_eq!(n, 0, "table {table} should exist and be empty");
    }
    // prg_unmatched must carry the serving + cell columns.
    conn.execute_batch(
        "INSERT INTO prg_unmatched
         (geom, lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
          teryt_miejscowosc, cell_x, cell_y, computed_at)
         VALUES (ST_Point(21.0,52.0),'id1','5','Main','Town','00-001','0918123',
                 9147, 5411, now());",
    )?;
    let (hn, cx): (String, i32) = conn.query_row(
        "SELECT numer_porzadkowy, cell_x FROM prg_unmatched",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!((hn.as_str(), cx), ("5", 9147));
    Ok(())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib db::tests::test_init_db_creates_serving_and_queue_tables`
Expected: FAIL — `bdot10k_unmatched` does not exist.

- [ ] **Step 3: Add the tables to `create_schema`**

In `src/db.rs::create_schema`, inside the same `execute_batch` string that defines `dataset_change_areas`, append:

```sql
        -- Precomputed unmatched government objects served by /tiles and /package.
        -- Only unmatched rows are stored, tagged with the z14 cell of their
        -- representative point and the time that cell was last recomputed.
        CREATE TABLE IF NOT EXISTS bdot10k_unmatched (
            LOKALNYID VARCHAR,
            geom GEOMETRY,
            cell_x INTEGER,
            cell_y INTEGER,
            computed_at TIMESTAMP WITH TIME ZONE
        );
        CREATE TABLE IF NOT EXISTS egib_unmatched (
            id_budynku VARCHAR,
            geom GEOMETRY,
            cell_x INTEGER,
            cell_y INTEGER,
            computed_at TIMESTAMP WITH TIME ZONE
        );
        CREATE TABLE IF NOT EXISTS prg_unmatched (
            geom GEOMETRY,
            lokalny_id VARCHAR,
            numer_porzadkowy VARCHAR,
            ulica VARCHAR,
            miejscowosc VARCHAR,
            kod_pocztowy VARCHAR,
            teryt_miejscowosc VARCHAR,
            cell_x INTEGER,
            cell_y INTEGER,
            computed_at TIMESTAMP WITH TIME ZONE
        );

        -- Dirty-cell queue drained by the match_refresh job. Duplicates allowed
        -- (deduped on drain). source is 'bdot10k'|'egib'|'prg'; an OSM building
        -- edit enqueues bdot10k+egib, an OSM address edit enqueues prg.
        CREATE TABLE IF NOT EXISTS match_dirty_cells (
            source VARCHAR,
            cell_z INTEGER,
            cell_x INTEGER,
            cell_y INTEGER,
            enqueued_at TIMESTAMP WITH TIME ZONE
        );
```

- [ ] **Step 4: Run the schema tests**

Run: `cargo test --lib db::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets 2>&1 | tail -5
git add src/db.rs
git commit -m "feat: add *_unmatched serving tables and match_dirty_cells queue"
```

## Task 3: Shared match rule SQL

One home for "which government object is unmatched", parameterized by a spatial restriction. Buildings and addresses have different predicates; the buildings SQL is shared verbatim by both the full and incremental paths, and the address SQL is the per-cell form (the full address path keeps its grid-key optimization but is pinned to this rule by the equivalence test in Task 5).

**Files:**
- Create: `src/compare/rule.rs`
- Modify: `src/compare/mod.rs` (add `pub mod rule;`)
- Test: inline in `src/compare/rule.rs`

**Interfaces:**
- Produces:
  - `pub type Bounds = (f64, f64, f64, f64);` — `(min_lon, min_lat, max_lon, max_lat)`.
  - `pub const MATCH_DISTANCE_METERS: f64 = 50.0;`
  - `pub const OSM_MATCH_BUFFER_DEG: f64 = 0.001;`
  - `pub fn buffer(b: Bounds, deg: f64) -> Bounds`
  - `pub fn unmatched_buildings_sql(source_table: &str, select_list: &str, area: Bounds) -> String` — unmatched building rows whose centroid is within `area`, reading `osm_buildings` within `area`. `select_list` is the projection (rows aliased `b`).
  - `pub fn unmatched_addresses_in_cell_sql(source_table: &str, select_list: &str, write: Bounds, read: Bounds) -> String` — unmatched address rows whose point is within `write`, reading `osm_addresses` within `read` (rows aliased `a`).

- [ ] **Step 1: Write the failing test**

Create `src/compare/rule.rs` with a test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    fn conn() -> duckdb::Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch(
            "CREATE TABLE bsrc (LOKALNYID VARCHAR, geom GEOMETRY);
             CREATE TABLE asrc (lokalny_id VARCHAR, numer_porzadkowy VARCHAR, geom GEOMETRY);",
        )
        .unwrap();
        c
    }

    #[test]
    fn building_contained_by_osm_is_not_unmatched() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES
                 (1,'way',NULL, ST_MakeEnvelope(20.0,52.0,20.002,52.002));
             INSERT INTO bsrc VALUES ('in', ST_MakeEnvelope(20.0005,52.0005,20.0007,52.0007));
             INSERT INTO bsrc VALUES ('out', ST_MakeEnvelope(21.0,52.0,21.001,52.001));",
        )
        .unwrap();
        let sql = unmatched_buildings_sql("bsrc", "b.LOKALNYID", (14.0, 49.0, 25.0, 55.0));
        let ids: Vec<String> = {
            let mut s = c.prepare(&sql).unwrap();
            s.query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect()
        };
        assert_eq!(ids, vec!["out".to_string()], "only the uncontained building is unmatched");
    }

    #[test]
    fn address_within_50m_same_hn_is_not_unmatched() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_addresses VALUES (1,'node','12',NULL,NULL,NULL, ST_Point(21.01,52.2102));
             INSERT INTO asrc VALUES ('match','12', ST_Point(21.01,52.21));
             INSERT INTO asrc VALUES ('far','12', ST_Point(21.01,52.212));",
        )
        .unwrap();
        let area = (21.0, 52.2, 21.02, 52.22);
        let sql = unmatched_addresses_in_cell_sql(
            "asrc", "a.lokalny_id", area, buffer(area, OSM_MATCH_BUFFER_DEG),
        );
        let ids: Vec<String> = {
            let mut s = c.prepare(&sql).unwrap();
            s.query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect()
        };
        assert_eq!(ids, vec!["far".to_string()], "the ~22m match drops out, the ~220m one stays");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib compare::rule::`
Expected: FAIL to compile — module/functions not defined.

- [ ] **Step 3: Implement the rule module**

Write the module body in `src/compare/rule.rs` (above the test module), and add `pub mod rule;` to `src/compare/mod.rs`:

```rust
//! The single home for "which government object is unmatched against OSM".
//! Both `compare` (full recompute) and `match_refresh` (incremental per-cell)
//! resolve to this rule; the equivalence test in `compare` pins the address
//! grid-key fast path to it.

/// (min_lon, min_lat, max_lon, max_lat).
pub type Bounds = (f64, f64, f64, f64);

pub const MATCH_DISTANCE_METERS: f64 = 50.0;
/// OSM read buffer around a cell for address matching. Matches /package.
pub const OSM_MATCH_BUFFER_DEG: f64 = 0.001;

pub fn buffer(b: Bounds, deg: f64) -> Bounds {
    (b.0 - deg, b.1 - deg, b.2 + deg, b.3 + deg)
}

/// Unmatched building rows: government centroid within `area` and NOT contained
/// by any osm_buildings polygon (osm filtered to `area` for the R-tree scan —
/// no buffer needed: any polygon containing an in-`area` point has a bbox that
/// intersects `area`).
pub fn unmatched_buildings_sql(source_table: &str, select_list: &str, area: Bounds) -> String {
    let (x1, y1, x2, y2) = area;
    format!(
        "SELECT {select_list}
         FROM {source_table} b
         WHERE ST_Intersects(ST_Centroid(b.geom), ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
           AND NOT EXISTS (
               SELECT 1 FROM osm_buildings osm
               WHERE ST_Intersects(osm.geom, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
                 AND ST_Contains(osm.geom, ST_Centroid(b.geom))
           )"
    )
}

/// Unmatched address rows: government point within `write` and no osm_addresses
/// point (read from `read`) with equal normalized housenumber within 50 m.
/// NULL housenumber never matches (SQL `= NULL` is never true).
pub fn unmatched_addresses_in_cell_sql(
    source_table: &str,
    select_list: &str,
    write: Bounds,
    read: Bounds,
) -> String {
    let (wx1, wy1, wx2, wy2) = write;
    let (rx1, ry1, rx2, ry2) = read;
    let dist = MATCH_DISTANCE_METERS;
    format!(
        "SELECT {select_list}
         FROM {source_table} a
         WHERE ST_Intersects(a.geom, ST_MakeEnvelope({wx1}, {wy1}, {wx2}, {wy2}))
           AND NOT EXISTS (
               SELECT 1 FROM osm_addresses o
               WHERE ST_Intersects(o.geom, ST_MakeEnvelope({rx1}, {ry1}, {rx2}, {ry2}))
                 AND UPPER(TRIM(o.housenumber)) = UPPER(TRIM(a.numer_porzadkowy))
                 AND ST_Distance_Sphere(o.geom, a.geom) <= {dist}
           )"
    )
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib compare::rule::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets 2>&1 | tail -5
git add src/compare/rule.rs src/compare/mod.rs
git commit -m "feat: add shared unmatched match-rule SQL"
```

## Task 4: `compare` writes `*_unmatched`

Repoint the `compare` command to populate the serving tables with unmatched rows (tagged with z14 cell + `computed_at`) instead of the `*_comparison` tables. Buildings reuse the shared rule per 0.5° grid cell; addresses keep the single-pass grid-key strategy but emit only unmatched rows with cell tags. `compare` empties each serving table then inserts — offline, so no reader window.

**Files:**
- Modify: `src/compare/buildings.rs` (rewrite `compare_chunked` to anti-join + serving output)
- Modify: `src/compare/addresses.rs` (emit unmatched with cell tags)
- Test: rewrite inline tests in both; update `tests/cli_compare_buildings.rs`, `tests/cli_compare_addresses.rs`

**Interfaces:**
- Consumes: `rule::unmatched_buildings_sql`, `rule::unmatched_addresses_in_cell_sql` is *not* used here (addresses keep grid-key); `tile_math::cell_x_sql`/`cell_y_sql`.
- Produces: `bdot10k_unmatched`, `egib_unmatched`, `prg_unmatched` populated with unmatched rows; `compare_bdot10k`/`compare_egib`/`compare_prg` signatures unchanged (`fn(&Connection) -> Result<()>`).

- [ ] **Step 1: Rewrite building unit tests**

Replace the `compare_chunked`-based tests in `src/compare/buildings.rs` `tests` module with tests over the new writer. Keep `setup()` (seeds one `osm_buildings` polygon at 20.0..20.001,52.0..52.001). New tests:

```rust
    #[test]
    fn writes_only_unmatched_rows_with_cell_tags() {
        let conn = setup();
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES
                 ('inside', ST_MakeEnvelope(20.0002,52.0002,20.0008,52.0008)),
                 ('lonely', ST_MakeEnvelope(21.0,52.2,21.001,52.201));",
        )
        .unwrap();
        compare_bdot10k(&conn).unwrap();
        let ids: Vec<String> = {
            let mut s = conn.prepare("SELECT LOKALNYID FROM bdot10k_unmatched ORDER BY LOKALNYID").unwrap();
            s.query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect()
        };
        assert_eq!(ids, vec!["lonely".to_string()], "only the uncontained building is stored");
        let (cx, cy): (i32, i32) = conn.query_row(
            "SELECT cell_x, cell_y FROM bdot10k_unmatched", [], |r| Ok((r.get(0)?, r.get(1)?)),
        ).unwrap();
        let (ex, ey) = crate::tile_math::lonlat_to_tile(21.0005, 52.2005, crate::tile_math::CHANGE_CELL_ZOOM);
        assert_eq!((cx as u32, cy as u32), (ex, ey));
    }

    #[test]
    fn compare_is_idempotent() {
        let conn = setup();
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES ('lonely', ST_MakeEnvelope(21.0,52.2,21.001,52.201));",
        ).unwrap();
        compare_bdot10k(&conn).unwrap();
        compare_bdot10k(&conn).unwrap();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM bdot10k_unmatched", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 1, "re-running compare must not duplicate rows");
    }
```

Note: `setup()` currently also creates `test_source`; keep or drop as needed — these tests create their own `bdot10k_buildings`. Also add `bdot10k_unmatched`/`egib_unmatched` to the `setup()` connection by ensuring `init_db` created them (it does, via Task 2).

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib compare::buildings::`
Expected: FAIL — `compare_bdot10k` still writes `bdot10k_comparison`.

- [ ] **Step 3: Rewrite `buildings.rs` writer**

Replace `compare_chunked` and the two public fns so they fill the serving tables. New file body (replacing lines 11-151):

```rust
use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::compare::rule::unmatched_buildings_sql;
use crate::tile_math::{cell_x_sql, cell_y_sql};
use crate::utils::format_duration;

/// Grid cell size in degrees for chunked spatial comparison (memory bound).
const GRID_STEP: f64 = 0.5;

pub fn compare_bdot10k(conn: &Connection) -> Result<()> {
    compare_buildings(conn, "bdot10k", "bdot10k_buildings", "LOKALNYID", "bdot10k_unmatched")
}

pub fn compare_egib(conn: &Connection) -> Result<()> {
    compare_buildings(conn, "egib", "egib_buildings", "id_budynku", "egib_unmatched")
}

fn compare_buildings(
    conn: &Connection,
    label: &str,
    source_table: &str,
    id_col: &str,
    dest: &str,
) -> Result<()> {
    info!(source = label, "Comparing buildings against OSM");
    let t = std::time::Instant::now();

    conn.execute_batch(&format!("DELETE FROM {dest};"))
        .with_context(|| format!("Failed to clear {dest}"))?;

    let (min_x, min_y, max_x, max_y) = (14.0, 49.0, 25.0, 55.0);
    let cx = cell_x_sql("ST_Centroid(b.geom)");
    let cy = cell_y_sql("ST_Centroid(b.geom)");
    let select = format!("b.{id_col}, b.geom, {cx}, {cy}, now()");

    let mut y = min_y;
    while y < max_y {
        let mut x = min_x;
        while x < max_x {
            let area = (x, y, x + GRID_STEP, y + GRID_STEP);
            let inner = unmatched_buildings_sql(source_table, &select, area);
            conn.execute_batch(&format!(
                "INSERT INTO {dest} ({id_col}, geom, cell_x, cell_y, computed_at) {inner};"
            ))
            .with_context(|| format!("Failed comparing {label} in cell ({x},{y})"))?;
            x += GRID_STEP;
        }
        y += GRID_STEP;
    }

    let (total, unmatched): (i64, i64) = conn.query_row(
        &format!(
            "SELECT (SELECT COUNT(*) FROM {source_table}), (SELECT COUNT(*) FROM {dest})"
        ),
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    info!(
        source = label, total, unmatched, matched = total - unmatched,
        elapsed = %format_duration(t.elapsed()),
        "buildings comparison complete"
    );
    Ok(())
}
```

Keep the `#[cfg(test)]` module (now the tests from Step 1). Update `setup()` so it no longer depends on `test_source`/`compare_chunked`.

- [ ] **Step 4: Run building tests**

Run: `cargo test --lib compare::buildings::`
Expected: PASS.

- [ ] **Step 5: Rewrite address unit tests + writer**

In `src/compare/addresses.rs`, change `compare_prg` to write `prg_unmatched`. Keep the grid-key single-pass query but change the final projection: instead of `SELECT s.*` into a candidates table, select the serving columns plus cell tags for rows NOT matched. Replace `compare_addresses` body's final `SELECT`/target:

```rust
    conn.execute_batch("DELETE FROM prg_unmatched;")?;
    let cx = crate::tile_math::cell_x_sql("s.geom");
    let cy = crate::tile_math::cell_y_sql("s.geom");
    conn.execute_batch(&format!(
        "INSERT INTO prg_unmatched
         (geom, lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
          teryt_miejscowosc, cell_x, cell_y, computed_at)
         WITH
         neighbor_offsets(dx, dy) AS (
             VALUES (-1,-1),(-1,0),(-1,1),(0,-1),(0,0),(0,1),(1,-1),(1,0),(1,1)
         ),
         src_norm AS (
             SELECT lokalny_id,
                    UPPER(TRIM(numer_porzadkowy)) AS _hn,
                    FLOOR(ST_X(geom) / {GRID_KEY_DEG})::BIGINT AS _gx,
                    FLOOR(ST_Y(geom) / {GRID_KEY_DEG})::BIGINT AS _gy,
                    geom
             FROM prg_addresses
         ),
         osm_norm AS (
             SELECT UPPER(TRIM(housenumber)) AS _hn,
                    FLOOR(ST_X(geom) / {GRID_KEY_DEG})::BIGINT AS _gx,
                    FLOOR(ST_Y(geom) / {GRID_KEY_DEG})::BIGINT AS _gy,
                    geom
             FROM osm_addresses
         ),
         src_expanded AS (
             SELECT s.lokalny_id, s._hn, s.geom, s._gx + o.dx AS _sgx, s._gy + o.dy AS _sgy
             FROM src_norm s CROSS JOIN neighbor_offsets o
         ),
         matched_ids AS (
             SELECT DISTINCT s.lokalny_id
             FROM src_expanded s
             JOIN osm_norm o
               ON  s._hn = o._hn AND s._sgx = o._gx AND s._sgy = o._gy
               AND ST_Distance_Sphere(o.geom, s.geom) <= {MATCH_DISTANCE_METERS}
         )
         SELECT s.geom, s.lokalny_id, s.numer_porzadkowy, s.ulica, s.miejscowosc,
                s.kod_pocztowy, s.teryt_miejscowosc, {cx}, {cy}, now()
         FROM prg_addresses s
         WHERE NOT EXISTS (SELECT 1 FROM matched_ids m WHERE m.lokalny_id = s.lokalny_id);"
    ))
    .context("Failed to run address comparison query")?;
    Ok(())
```

Change `compare_prg` to call this refactored body directly (drop the `source_table`/`id_col`/`housenumber_col`/`candidates_table` parameters — they are now fixed to PRG columns) and to report `total`/`candidates` from `prg_addresses`/`prg_unmatched`. Update the inline tests to assert on `prg_unmatched` (count + that a matched address is absent), mirroring the existing cases (`matched_within_50m_excluded`, `same_number_but_too_far`, etc.) but querying `prg_unmatched` instead of `test_candidates`, and seeding a real `prg_addresses` table.

- [ ] **Step 6: Run address tests**

Run: `cargo test --lib compare::addresses::`
Expected: PASS.

- [ ] **Step 7: Update integration tests**

In `tests/cli_compare_buildings.rs` and `tests/cli_compare_addresses.rs`, replace references to `bdot10k_comparison`/`egib_comparison`/`prg_import_candidates` with `bdot10k_unmatched`/`egib_unmatched`/`prg_unmatched`, and change assertions from `(total, matched)` to unmatched-row counts. For the fixtures used today the building comparison found `matched=1` of 74, so `bdot10k_unmatched` should have 73 rows; assert `COUNT(*) = 73` and that the one matched id is absent. For addresses, all 3 fixture rows were candidates, so `prg_unmatched` has 3 rows.

- [ ] **Step 8: Run the compare integration tests**

Run: `cargo test --test cli_compare_buildings --test cli_compare_addresses`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
cargo fmt && cargo clippy --all-targets 2>&1 | tail -5
git add src/compare/ tests/cli_compare_buildings.rs tests/cli_compare_addresses.rs
git commit -m "feat: compare writes precomputed *_unmatched serving tables"
```

## Task 5: Address rule equivalence guard

Pin the address grid-key full path to the shared per-cell rule: on a fixture, the two must produce the identical unmatched id set. This is the invariant that lets the two implementations coexist.

**Files:**
- Test: inline in `src/compare/addresses.rs`

**Interfaces:**
- Consumes: `compare_prg` (full path), `rule::unmatched_addresses_in_cell_sql` (per-cell path).

- [ ] **Step 1: Write the equivalence test**

Add to the `tests` module in `src/compare/addresses.rs`:

```rust
/// The full grid-key path and the per-cell rule must agree on the unmatched
/// set. Seed a spread of addresses (some matched, some not, some near cell
/// edges) and compare the two id sets.
#[test]
fn full_and_per_cell_paths_agree() {
    use crate::compare::rule::{buffer, unmatched_addresses_in_cell_sql, OSM_MATCH_BUFFER_DEG};
    use crate::tile_math::{lonlat_to_tile, tile_to_bbox, CHANGE_CELL_ZOOM};
    use std::collections::BTreeSet;

    let conn = setup(); // creates prg_addresses + osm_addresses via init_db
    conn.execute_batch(
        "INSERT INTO prg_addresses (lokalny_id, numer_porzadkowy, geom) VALUES
            ('a','12', ST_Point(21.010, 52.210)),   -- matched (osm ~22m)
            ('b','12', ST_Point(21.010, 52.212)),   -- too far -> unmatched
            ('c','7',  ST_Point(21.050, 52.250)),   -- no osm -> unmatched
            ('d','9',  ST_Point(21.0001, 52.2001)); -- near a cell edge
         INSERT INTO osm_addresses VALUES
            (1,'node','12',NULL,NULL,NULL, ST_Point(21.010, 52.2102));",
    )
    .unwrap();

    // Full path.
    compare_prg(&conn).unwrap();
    let full: BTreeSet<String> = {
        let mut s = conn.prepare("SELECT lokalny_id FROM prg_unmatched").unwrap();
        s.query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect()
    };

    // Per-cell path over the distinct cells the addresses fall in.
    let mut cells = BTreeSet::new();
    for (lon, lat) in [(21.010, 52.210), (21.010, 52.212), (21.050, 52.250), (21.0001, 52.2001)] {
        cells.insert(lonlat_to_tile(lon, lat, CHANGE_CELL_ZOOM));
    }
    let mut per_cell = BTreeSet::new();
    for (cx, cy) in cells {
        let w = tile_to_bbox(CHANGE_CELL_ZOOM, cx, cy);
        let sql = unmatched_addresses_in_cell_sql(
            "prg_addresses", "a.lokalny_id", w, buffer(w, OSM_MATCH_BUFFER_DEG),
        );
        let mut s = conn.prepare(&sql).unwrap();
        for id in s.query_map([], |r| r.get::<_, String>(0)).unwrap() {
            per_cell.insert(id.unwrap());
        }
    }

    assert_eq!(full, per_cell, "full grid-key and per-cell rule disagree");
}
```

(Adjust `setup()` if needed so `prg_addresses` exists — add `CREATE TABLE prg_addresses (...)` there, or create it inline as above.)

- [ ] **Step 2: Run it**

Run: `cargo test --lib compare::addresses::full_and_per_cell_paths_agree`
Expected: PASS (if it fails, the two encodings genuinely disagree — fix the rule, not the test).

- [ ] **Step 3: Commit**

```bash
git add src/compare/addresses.rs
git commit -m "test: pin address full path to the shared per-cell rule"
```

## Task 6: Repoint `/tiles` and `/package` to serving tables

`/tiles` reads `*_unmatched` instead of the source tables; `/package` reads `*_unmatched` clipped to the request polygon, removing its own live anti-joins.

**Files:**
- Modify: `src/server/tiles.rs:20-51` (SQL), and its inline test seeds
- Modify: `src/server/package.rs` (replace `unmatched_addresses`/`unmatched_buildings` with serving reads; update `build_package`)
- Test: update inline tests in both files

**Interfaces:**
- Consumes: `prg_unmatched`, `bdot10k_unmatched`, `egib_unmatched`.
- Produces: `unmatched_addresses(conn, area) -> Result<Vec<AddressRow>>` and `unmatched_buildings(conn, dest_table, area) -> Result<Vec<String>>` now read the serving tables; `AddressRow` unchanged.

- [ ] **Step 1: Update tiles SQL**

In `src/server/tiles.rs`, change `ADDRESSES_MVT_SQL` `FROM prg_addresses a` → `FROM prg_unmatched a`, and in `BUILDINGS_MVT_SQL` change `bdot10k_buildings`→`bdot10k_unmatched` and `egib_buildings`→`egib_unmatched` (column names `LOKALNYID`/`id_budynku`/`geom`/`lokalny_id`/`numer_porzadkowy`/`miejscowosc` are all present in the serving tables). Update the inline test that seeds `prg_addresses`/`bdot10k_buildings`/`egib_buildings` (around lines 132-234) to seed the `*_unmatched` tables instead (add `cell_x, cell_y, computed_at` columns to the INSERTs, any valid values).

- [ ] **Step 2: Rewrite package readers**

In `src/server/package.rs`, replace the bodies of `unmatched_addresses` and `unmatched_buildings` so they read the serving tables clipped to the polygon. New `unmatched_addresses`:

```rust
pub fn unmatched_addresses(conn: &Connection, area: &RequestArea) -> Result<Vec<AddressRow>> {
    let (x1, y1, x2, y2) = (area.min_lon, area.min_lat, area.max_lon, area.max_lat);
    let sql = format!(
        "SELECT ST_AsGeoJSON(a.geom), a.numer_porzadkowy, a.ulica, a.miejscowosc,
                a.kod_pocztowy, a.teryt_miejscowosc
         FROM prg_unmatched a
         WHERE ST_Intersects(a.geom, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
           AND ST_Intersects(a.geom, ST_GeomFromGeoJSON(?))"
    );
    let mut stmt = conn.prepare(&sql).context("Failed to prepare package address query")?;
    let rows = stmt
        .query_map([area.polygon_geojson.as_str()], |row| {
            Ok(AddressRow {
                geometry_geojson: row.get(0)?,
                housenumber: row.get(1)?,
                street: row.get(2)?,
                city: row.get(3)?,
                postcode: row.get(4)?,
                simc: row.get(5)?,
            })
        })
        .context("Failed to run package address query")?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.context("Failed to read package address row")?);
    }
    Ok(out)
}
```

New `unmatched_buildings` (its `source_table` argument becomes the serving table name — update the two call sites in `build_package` from `"bdot10k_buildings"`/`"egib_buildings"` to `"bdot10k_unmatched"`/`"egib_unmatched"`):

```rust
pub fn unmatched_buildings(
    conn: &Connection,
    dest_table: &str,
    area: &RequestArea,
) -> Result<Vec<String>> {
    let (x1, y1, x2, y2) = (area.min_lon, area.min_lat, area.max_lon, area.max_lat);
    let sql = format!(
        "SELECT ST_AsGeoJSON(b.geom)
         FROM {dest_table} b
         WHERE ST_Intersects(b.geom, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
           AND ST_Intersects(ST_Centroid(b.geom), ST_GeomFromGeoJSON(?))"
    );
    let mut stmt = conn.prepare(&sql)
        .with_context(|| format!("Failed to prepare package building query for {dest_table}"))?;
    let rows = stmt
        .query_map([area.polygon_geojson.as_str()], |row| row.get(0))
        .with_context(|| format!("Failed to run package building query for {dest_table}"))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.with_context(|| format!("Failed to read package building row from {dest_table}"))?);
    }
    Ok(out)
}
```

Delete the now-unused `MATCH_DISTANCE_METERS`/`MATCH_BUFFER_DEG` consts in `package.rs` if nothing else references them.

- [ ] **Step 3: Update package inline tests**

The package tests (around lines 600-760) seed `prg_addresses`/`bdot10k_buildings`/`egib_buildings` and rely on the live anti-join. Reseed them into `prg_unmatched`/`bdot10k_unmatched`/`egib_unmatched` (with `cell_x, cell_y, computed_at`), and drop the OSM seed rows that existed only to drive the anti-join (matching now happens upstream). Assertions on returned geometry/tag content stay the same.

- [ ] **Step 4: Run server tests**

Run: `cargo test --lib server::tiles:: server::package::`
Expected: PASS.

- [ ] **Step 5: End-to-end smoke via CLI (manual)**

Build and run against fixtures to confirm the repoint holds end-to-end:

```bash
cargo build
# import osm + bdot10k + egib + prg into a scratch DB, then:
cargo run -- --config <scratch cfg> compare full
# confirm bdot10k_unmatched/egib_unmatched/prg_unmatched are populated
```

Expected: the three `*_unmatched` tables are non-empty; no reference to `*_comparison` remains (`grep -rn "_comparison\|import_candidates" src/` returns nothing).

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy --all-targets 2>&1 | tail -5
git add src/server/tiles.rs src/server/package.rs
git commit -m "feat: serve /tiles and /package from precomputed *_unmatched tables"
```

---

# Phase 2 — Keep the serving tables fresh (build order steps 4–8)

Adds the producers, the incremental drain job, staleness on `/status`, and the reconciliation sweep. After Phase 2 an OSM or government edit corrects the affected cells within one drain interval.

## Task 7: Per-cell incremental recompute

One function that rebuilds a single `(source, cell)`'s slice of its serving table from current live data, in one transaction. This is the unit `match_refresh` and the reconcile sweep both call.

**Files:**
- Create: `src/compare/incremental.rs`
- Modify: `src/compare/mod.rs` (add `pub mod incremental;`)
- Test: inline

**Interfaces:**
- Consumes: `rule::unmatched_buildings_sql`, `rule::unmatched_addresses_in_cell_sql`, `rule::{buffer, OSM_MATCH_BUFFER_DEG}`, `tile_math::{tile_to_bbox, cell_x_sql, cell_y_sql, CHANGE_CELL_ZOOM}`, `dataset::{BDOT10K, EGIB, PRG}`.
- Produces:
  - `pub fn recompute_cell(conn: &Connection, source: &str, cell_x: i32, cell_y: i32) -> Result<()>` — replaces that cell's rows in `<source>_unmatched` in one transaction (`DELETE WHERE cell_x=? AND cell_y=?` then `INSERT`). Panics/`bail!`s on unknown `source`.

- [ ] **Step 1: Write the failing test**

Create `src/compare/incremental.rs`:

```rust
use anyhow::{Context, Result, bail};
use duckdb::Connection;

use crate::compare::rule::{buffer, unmatched_addresses_in_cell_sql, unmatched_buildings_sql, OSM_MATCH_BUFFER_DEG};
use crate::tile_math::{cell_x_sql, cell_y_sql, tile_to_bbox, CHANGE_CELL_ZOOM};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use crate::tile_math::lonlat_to_tile;
    use std::path::Path;

    fn conn() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(), "LOAD spatial".to_string(),
            "INSTALL icu".to_string(), "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch("CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);").unwrap();
        c
    }

    #[test]
    fn recompute_replaces_only_that_cell() {
        let c = conn();
        // Two buildings in different z14 cells, neither matched.
        c.execute_batch(
            "INSERT INTO bdot10k_buildings VALUES
                 ('p', ST_MakeEnvelope(21.0,52.0,21.001,52.001)),
                 ('q', ST_MakeEnvelope(19.0,50.0,19.001,50.001));",
        ).unwrap();
        let (px, py) = lonlat_to_tile(21.0005, 52.0005, CHANGE_CELL_ZOOM);
        let (qx, qy) = lonlat_to_tile(19.0005, 50.0005, CHANGE_CELL_ZOOM);

        recompute_cell(&c, "bdot10k", px as i32, py as i32).unwrap();
        recompute_cell(&c, "bdot10k", qx as i32, qy as i32).unwrap();
        let n: i64 = c.query_row("SELECT COUNT(*) FROM bdot10k_unmatched", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 2);

        // Now 'p' becomes matched (add an osm building over it). Recompute only p's cell.
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES (1,'way',NULL, ST_MakeEnvelope(20.9,51.9,21.1,52.1));",
        ).unwrap();
        recompute_cell(&c, "bdot10k", px as i32, py as i32).unwrap();

        let ids: Vec<String> = {
            let mut s = c.prepare("SELECT LOKALNYID FROM bdot10k_unmatched ORDER BY LOKALNYID").unwrap();
            s.query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect()
        };
        assert_eq!(ids, vec!["q".to_string()], "p's cell rebuilt to matched; q's cell untouched");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib compare::incremental::`
Expected: FAIL to compile — `recompute_cell` not defined.

- [ ] **Step 3: Implement `recompute_cell`**

Add above the test module, and `pub mod incremental;` to `src/compare/mod.rs`:

```rust
/// Rebuild one z14 cell's slice of `<source>_unmatched` from current live data,
/// in a single transaction. Read wide (buffered OSM for addresses), write narrow
/// (only rows whose representative point is inside the cell).
pub fn recompute_cell(conn: &Connection, source: &str, cell_x: i32, cell_y: i32) -> Result<()> {
    let write = tile_to_bbox(CHANGE_CELL_ZOOM, cell_x as u32, cell_y as u32);
    let (dest, insert_cols, inner) = match source {
        "bdot10k" | "egib" => {
            let (src, id, dest) = if source == "bdot10k" {
                ("bdot10k_buildings", "LOKALNYID", "bdot10k_unmatched")
            } else {
                ("egib_buildings", "id_budynku", "egib_unmatched")
            };
            let cx = cell_x_sql("ST_Centroid(b.geom)");
            let cy = cell_y_sql("ST_Centroid(b.geom)");
            let select = format!("b.{id}, b.geom, {cx}, {cy}, now()");
            (
                dest,
                format!("{id}, geom, cell_x, cell_y, computed_at"),
                unmatched_buildings_sql(src, &select, write),
            )
        }
        "prg" => {
            let read = buffer(write, OSM_MATCH_BUFFER_DEG);
            let cx = cell_x_sql("a.geom");
            let cy = cell_y_sql("a.geom");
            let select = format!(
                "a.geom, a.lokalny_id, a.numer_porzadkowy, a.ulica, a.miejscowosc, \
                 a.kod_pocztowy, a.teryt_miejscowosc, {cx}, {cy}, now()"
            );
            (
                "prg_unmatched",
                "geom, lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy, \
                 teryt_miejscowosc, cell_x, cell_y, computed_at"
                    .to_string(),
                unmatched_addresses_in_cell_sql("prg_addresses", &select, write, read),
            )
        }
        other => bail!("recompute_cell: unknown source {other}"),
    };

    conn.execute_batch("BEGIN TRANSACTION")
        .context("recompute_cell: begin")?;
    let res = (|| -> Result<()> {
        conn.execute(
            &format!("DELETE FROM {dest} WHERE cell_x = ? AND cell_y = ?"),
            duckdb::params![cell_x, cell_y],
        )?;
        conn.execute_batch(&format!("INSERT INTO {dest} ({insert_cols}) {inner};"))?;
        Ok(())
    })();
    match res {
        Ok(()) => conn.execute_batch("COMMIT").context("recompute_cell: commit"),
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --lib compare::incremental::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets 2>&1 | tail -5
git add src/compare/incremental.rs src/compare/mod.rs
git commit -m "feat: add per-cell incremental unmatched recompute"
```

## Task 8: Government producer — enqueue touched cells on refresh

Enqueue the exact cells a dataset refresh touches, in the same transaction as the delta, reusing the distinct-cell set `insert_change_areas` already computes.

**Files:**
- Modify: `src/update/changeset.rs` (add `insert_dirty_cells`)
- Modify: `src/update/dataset.rs` (call it inside the apply transaction)
- Test: inline in `src/update/changeset.rs` and extend `src/update/dataset.rs` tests

**Interfaces:**
- Consumes: diff temp tables `diff_added`/`diff_removed`/`diff_modified`, `spec`, `tile_math::{cell_x_sql, cell_y_sql, CHANGE_CELL_ZOOM}`.
- Produces: `pub fn insert_dirty_cells(conn: &Connection, spec: &DatasetSpec) -> Result<()>` — inserts one `match_dirty_cells` row per distinct touched z14 cell for `spec.name`.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/update/changeset.rs` (the module already builds live/staging + diff tables in `setup()`):

```rust
#[test]
fn enqueues_distinct_touched_cells() {
    let conn = setup();
    insert_dirty_cells(&conn, &TEST_SPEC).unwrap();
    // 'del'/'mov' left the home cell; 'add'/'mov' arrive — 2 distinct cells.
    let (home_x, home_y) = lonlat_to_tile(21.0, 52.0, CHANGE_CELL_ZOOM);
    let (dest_x, dest_y) = lonlat_to_tile(19.0, 50.0, CHANGE_CELL_ZOOM);
    let cells: Vec<(String, i32, i32)> = {
        let mut s = conn.prepare(
            "SELECT source, cell_x, cell_y FROM match_dirty_cells ORDER BY cell_x, cell_y",
        ).unwrap();
        s.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap().map(|r| r.unwrap()).collect()
    };
    assert!(cells.iter().all(|(s, _, _)| s == "test"));
    assert!(cells.contains(&("test".to_string(), home_x as i32, home_y as i32)));
    assert!(cells.contains(&("test".to_string(), dest_x as i32, dest_y as i32)));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib update::changeset::enqueues_distinct_touched_cells`
Expected: FAIL — `insert_dirty_cells` not defined.

- [ ] **Step 3: Implement `insert_dirty_cells`**

Add to `src/update/changeset.rs`:

```rust
/// Enqueue one dirty-cell row per distinct z14 cell this refresh touches
/// (added from staging, removed/modified from both live and staging). Must run
/// inside the apply transaction so the queue commits atomically with the delta.
pub fn insert_dirty_cells(conn: &Connection, spec: &DatasetSpec) -> Result<()> {
    let live = spec.table;
    let staging = spec.staging_table();
    let id = spec.id_column;
    let z = crate::tile_math::CHANGE_CELL_ZOOM;
    let point_live = spec.representative_point_sql("l.geom");
    let point_stg = spec.representative_point_sql("s.geom");
    let sx = crate::tile_math::cell_x_sql(&point_stg);
    let sy = crate::tile_math::cell_y_sql(&point_stg);
    let lx = crate::tile_math::cell_x_sql(&point_live);
    let ly = crate::tile_math::cell_y_sql(&point_live);

    let sql = format!(
        "INSERT INTO match_dirty_cells
         SELECT DISTINCT '{source}', {z}, cell_x, cell_y, now()
         FROM (
             SELECT {sx} AS cell_x, {sy} AS cell_y
             FROM {staging} s JOIN diff_added d ON s.{id} = d.id WHERE s.geom IS NOT NULL
             UNION
             SELECT {lx}, {ly}
             FROM {live} l JOIN diff_removed d ON l.{id} = d.id WHERE l.geom IS NOT NULL
             UNION
             SELECT {sx}, {sy}
             FROM {staging} s JOIN diff_modified d ON s.{id} = d.id WHERE s.geom IS NOT NULL
             UNION
             SELECT {lx}, {ly}
             FROM {live} l JOIN diff_modified d ON l.{id} = d.id WHERE l.geom IS NOT NULL
         )",
        source = spec.name,
    );
    conn.execute_batch(&sql)
        .with_context(|| format!("Failed to enqueue dirty cells for {}", spec.name))
}
```

- [ ] **Step 4: Call it from the apply transaction**

In `src/update/dataset.rs`, inside the `applied` closure (right after `insert_change_areas(conn, spec, snapshot_id)?;` at line ~149):

```rust
        crate::update::changeset::insert_dirty_cells(conn, spec)?;
```

- [ ] **Step 5: Add a dataset-level assertion**

Extend `writes_refresh_row_and_change_areas` in `src/update/dataset.rs` (or add a sibling test) to assert `match_dirty_cells` is non-empty after a refresh with changes:

```rust
        let dirty: i64 = conn
            .query_row("SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'test'", [], |r| r.get(0))
            .unwrap();
        assert!(dirty > 0, "refresh must enqueue dirty cells");
```

- [ ] **Step 6: Run tests**

Run: `cargo test --lib update::changeset:: update::dataset::`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cargo fmt && cargo clippy --all-targets 2>&1 | tail -5
git add src/update/changeset.rs src/update/dataset.rs
git commit -m "feat: enqueue dirty cells on government refresh"
```

## Task 9: OSM producer — enqueue the 3×3 neighbourhood of touched cells

Record the z14 cell of each object the OSM diff touches (before a delete, after an insert) and enqueue its 3×3 neighbourhood, per layer, inside the diff transaction.

**Files:**
- Create: `src/update/dirty_cells.rs` (the `DirtyCells` collector)
- Modify: `src/update/mod.rs` (add `pub mod dirty_cells;`)
- Modify: `src/update/osm.rs` (thread the collector through `apply_changes` and the rebuild helpers; flush before commit)
- Test: inline in `src/update/dirty_cells.rs`; integration test in `tests/`

**Interfaces:**
- Consumes: `tile_math::{lonlat_to_tile, CHANGE_CELL_ZOOM}`.
- Produces:
  - `pub struct DirtyCells { buildings: HashSet<(i32,i32)>, addresses: HashSet<(i32,i32)> }`
  - `impl DirtyCells`: `new()`, `note_point(&mut self, layer: Layer, lon: f64, lat: f64)`, `note_existing(&mut self, conn: &Connection, layer: Layer, table: &str, osm_id: i64, osm_type: &str) -> Result<()>` (reads the row's geom cell from `table`), and `flush(&self, conn: &Connection) -> Result<()>` (expands 3×3, maps buildings→{bdot10k,egib}/addresses→{prg}, inserts into `match_dirty_cells`).
  - `pub enum Layer { Buildings, Addresses }`

- [ ] **Step 1: Write the failing test**

Create `src/update/dirty_cells.rs`:

```rust
use std::collections::HashSet;

use anyhow::{Context, Result};
use duckdb::Connection;

use crate::tile_math::{lonlat_to_tile, CHANGE_CELL_ZOOM};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Layer {
    Buildings,
    Addresses,
}

#[derive(Default)]
pub struct DirtyCells {
    buildings: HashSet<(i32, i32)>,
    addresses: HashSet<(i32, i32)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    fn conn() -> Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    #[test]
    fn flush_expands_3x3_and_fans_out_by_layer() {
        let c = conn();
        let mut d = DirtyCells::default();
        d.note_point(Layer::Buildings, 21.0, 52.0);
        d.flush(&c).unwrap();

        // Buildings fan out to bdot10k + egib; 3x3 => 9 cells each.
        let (bx, by) = lonlat_to_tile(21.0, 52.0, CHANGE_CELL_ZOOM);
        let bdot: i64 = c.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE source='bdot10k'", [], |r| r.get(0)).unwrap();
        let egib: i64 = c.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE source='egib'", [], |r| r.get(0)).unwrap();
        assert_eq!((bdot, egib), (9, 9));
        // Center cell present.
        let center: i64 = c.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE source='bdot10k' AND cell_x=? AND cell_y=?",
            duckdb::params![bx as i32, by as i32], |r| r.get(0)).unwrap();
        assert_eq!(center, 1);
        // Addresses untouched.
        let prg: i64 = c.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE source='prg'", [], |r| r.get(0)).unwrap();
        assert_eq!(prg, 0);
    }

    #[test]
    fn note_existing_reads_geom_cell_from_table() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES (5,'way',NULL, ST_MakeEnvelope(21.0,52.0,21.001,52.001));",
        ).unwrap();
        let mut d = DirtyCells::default();
        d.note_existing(&c, Layer::Buildings, "osm_buildings", 5, "way").unwrap();
        d.flush(&c).unwrap();
        let (bx, by) = lonlat_to_tile(21.0005, 52.0005, CHANGE_CELL_ZOOM);
        let center: i64 = c.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE source='bdot10k' AND cell_x=? AND cell_y=?",
            duckdb::params![bx as i32, by as i32], |r| r.get(0)).unwrap();
        assert_eq!(center, 1);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib update::dirty_cells::`
Expected: FAIL to compile — methods not defined.

- [ ] **Step 3: Implement the collector**

Add to `src/update/dirty_cells.rs` (above tests) and `pub mod dirty_cells;` to `src/update/mod.rs`:

```rust
impl DirtyCells {
    pub fn new() -> Self {
        Self::default()
    }

    fn set(&mut self, layer: Layer) -> &mut HashSet<(i32, i32)> {
        match layer {
            Layer::Buildings => &mut self.buildings,
            Layer::Addresses => &mut self.addresses,
        }
    }

    /// Record the cell of a known point (node fast path — no query).
    pub fn note_point(&mut self, layer: Layer, lon: f64, lat: f64) {
        if lon.is_finite() && lat.is_finite() {
            let (x, y) = lonlat_to_tile(lon, lat, CHANGE_CELL_ZOOM);
            self.set(layer).insert((x as i32, y as i32));
        }
    }

    /// Record the cell of a row currently in `table` for (osm_id, osm_type).
    /// A no-op when the row is absent (nothing to leave from).
    pub fn note_existing(
        &mut self,
        conn: &Connection,
        layer: Layer,
        table: &str,
        osm_id: i64,
        osm_type: &str,
    ) -> Result<()> {
        let cx = crate::tile_math::cell_x_sql("geom");
        let cy = crate::tile_math::cell_y_sql("geom");
        let sql = format!(
            "SELECT {cx}, {cy} FROM {table}
             WHERE osm_id = ? AND osm_type = ? AND geom IS NOT NULL"
        );
        let mut stmt = conn.prepare(&sql).with_context(|| format!("note_existing prepare {table}"))?;
        let rows = stmt.query_map(duckdb::params![osm_id, osm_type], |r| {
            Ok((r.get::<_, i32>(0)?, r.get::<_, i32>(1)?))
        })?;
        for row in rows {
            let (x, y) = row?;
            self.set(layer).insert((x, y));
        }
        Ok(())
    }

    /// Insert the 3×3 neighbourhood of every recorded cell into
    /// match_dirty_cells: buildings → bdot10k+egib, addresses → prg.
    pub fn flush(&self, conn: &Connection) -> Result<()> {
        let z = CHANGE_CELL_ZOOM as i32;
        let mut stmt = conn.prepare(
            "INSERT INTO match_dirty_cells VALUES (?, ?, ?, ?, now())",
        )?;
        let mut insert = |source: &str, x: i32, y: i32| -> Result<()> {
            for dx in -1..=1 {
                for dy in -1..=1 {
                    stmt.execute(duckdb::params![source, z, x + dx, y + dy])?;
                }
            }
            Ok(())
        };
        for &(x, y) in &self.buildings {
            insert("bdot10k", x, y)?;
            insert("egib", x, y)?;
        }
        for &(x, y) in &self.addresses {
            insert("prg", x, y)?;
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Run collector tests**

Run: `cargo test --lib update::dirty_cells::`
Expected: PASS.

- [ ] **Step 5: Thread the collector through `apply_changes`**

Keep `apply_changes`'s signature `fn(conn, kv, changes) -> Result<()>` unchanged — it has 5 direct test callers (osm.rs lines ~620/651/663/692/724) that must not break, and it already runs inside `apply_sequence`'s transaction. So `apply_changes` **owns** the collector and flushes it just before returning; only the internal `rebuild_*` helpers (no direct test callers) gain a `dirty: &mut DirtyCells` parameter.

In `src/update/osm.rs`:

1. At the top of `apply_changes`, after the two `affected_*` sets: `let mut dirty = DirtyCells::new();`
2. At each served-table site, add the note calls (do not change existing SQL):
   - **node Delete:** before `DELETE FROM osm_addresses ... 'node'` → `dirty.note_existing(conn, Layer::Addresses, "osm_addresses", node.id, "node")?;`
   - **node Create|Modify:** before its `DELETE FROM osm_addresses ... 'node'` → the same `note_existing` (leaving cell); and inside the `if let Some(hn)` insert branch, after the INSERT → `dirty.note_point(Layer::Addresses, node.lon, node.lat);` (arriving cell).
   - **way Delete:** before the two DELETEs → `dirty.note_existing(conn, Layer::Buildings, "osm_buildings", way.id, "way")?;` and `dirty.note_existing(conn, Layer::Addresses, "osm_addresses", way.id, "way")?;`
   - **relation Delete:** same as way Delete with `rel.id` and `"relation"`.
3. Give `rebuild_way_geometry` / `rebuild_relation_geometry` a `dirty: &mut DirtyCells` param and pass `&mut dirty` from the two rebuild loops. In each: before its DELETEs call the two `note_existing` (leaving cells) for the id/type; after the building INSERT call `dirty.note_existing(conn, Layer::Buildings, "osm_buildings", id, type)?;` and after the address INSERT call `dirty.note_existing(conn, Layer::Addresses, "osm_addresses", id, type)?;` (the row now holds its new geometry → arriving cell).
4. Immediately before `apply_changes` returns `Ok(())` (after the relation-rebuild loop): `dirty.flush(conn)?;`

`apply_sequence` is unchanged — the flush rides inside its existing `BEGIN`/`COMMIT`. The 5 existing `apply_changes` test callers keep compiling and simply also exercise the (harmless) enqueue.

Add `use crate::update::dirty_cells::{DirtyCells, Layer};` to the imports.

- [ ] **Step 6: Integration test — OSM diff enqueues cells**

Add `tests/cli_update_osm_enqueues.rs` (mirroring `cli_update_bdot10k.rs`'s file-config harness): import osm + bdot10k from fixtures, then apply a hand-written `.osc.gz` (reuse `fixtures/osm.osc.gz` if it adds/moves a building/address, else add a small fixture) via `update osm`, and assert `match_dirty_cells` gained rows for the right source. Minimal assertion:

```rust
let dirty: i64 = conn.query_row(
    "SELECT COUNT(*) FROM match_dirty_cells", [], |r| r.get(0)).unwrap();
assert!(dirty > 0, "an OSM diff touching served objects must enqueue cells");
```

- [ ] **Step 7: Run OSM update tests**

Run: `cargo test --lib update::osm:: && cargo test --test cli_update_osm_enqueues`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
cargo fmt && cargo clippy --all-targets 2>&1 | tail -5
git add src/update/dirty_cells.rs src/update/mod.rs src/update/osm.rs tests/cli_update_osm_enqueues.rs
git commit -m "feat: enqueue 3x3 dirty-cell neighbourhood on OSM updates"
```

## Task 10: Drain — batched, per-cell, cutoff-ordered

The function that drains a bounded batch of dirty cells, recomputing each in its own transaction and deleting the drained queue rows under the `enqueued_at <= batch_start` cutoff.

**Files:**
- Create: `src/compare/drain.rs`
- Modify: `src/compare/mod.rs` (`pub mod drain;`)
- Test: inline

**Interfaces:**
- Consumes: `incremental::recompute_cell`.
- Produces:
  - `pub struct DrainStats { pub cells: u64 }`
  - `pub fn drain_batch(conn: &Connection, batch_size: usize) -> Result<DrainStats>` — drains up to `batch_size` distinct `(source, cell)` whose `enqueued_at <= batch_start`; per cell: `recompute_cell` then delete that cell's queue rows with `enqueued_at <= batch_start`, one transaction each (recompute_cell already wraps its own; the queue delete for a cell rides in a second tiny transaction — see note). Returns count drained.

Note on atomicity: `recompute_cell` opens its own transaction. To keep "rewrite + queue delete" in one transaction, `drain_batch` performs the delete of the queue rows for a cell *inside* a wrapping transaction that also calls the recompute SQL. Implement `drain_batch` to inline the recompute rather than call `recompute_cell`, OR add `recompute_cell_in_txn` that assumes an open transaction. This plan uses the latter: add `pub fn recompute_cell_in_txn(conn, source, cell_x, cell_y) -> Result<()>` (the body of `recompute_cell` minus BEGIN/COMMIT), have `recompute_cell` wrap it, and have `drain_batch` call it inside its own transaction together with the queue delete.

- [ ] **Step 1: Add `recompute_cell_in_txn` (refactor Task 7)**

In `src/compare/incremental.rs`, extract the DELETE+INSERT body into `pub fn recompute_cell_in_txn(conn, source, cell_x, cell_y) -> Result<()>` (no BEGIN/COMMIT) and make `recompute_cell` call it between BEGIN/COMMIT. Run `cargo test --lib compare::incremental::` — still PASS.

- [ ] **Step 2: Write the failing drain test**

Create `src/compare/drain.rs`:

```rust
use anyhow::{Context, Result};
use duckdb::Connection;

use crate::compare::incremental::recompute_cell_in_txn;

pub struct DrainStats {
    pub cells: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    fn conn() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(), "LOAD spatial".to_string(),
            "INSTALL icu".to_string(), "LOAD icu".to_string(),
        ];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch("CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);").unwrap();
        c
    }

    #[test]
    fn drains_up_to_batch_size_and_clears_queue() {
        let c = conn();
        // Enqueue three distinct bdot10k cells.
        c.execute_batch(
            "INSERT INTO match_dirty_cells VALUES
                 ('bdot10k',14,100,100,now()),
                 ('bdot10k',14,101,100,now()),
                 ('bdot10k',14,102,100,now());",
        ).unwrap();
        let s = drain_batch(&c, 2).unwrap();
        assert_eq!(s.cells, 2, "batch_size caps the drain");
        let left: i64 = c.query_row("SELECT COUNT(*) FROM match_dirty_cells", [], |r| r.get(0)).unwrap();
        assert_eq!(left, 1, "two of three cells drained");
        drain_batch(&c, 10).unwrap();
        let left: i64 = c.query_row("SELECT COUNT(*) FROM match_dirty_cells", [], |r| r.get(0)).unwrap();
        assert_eq!(left, 0);
    }

    #[test]
    fn cell_reenqueued_after_batch_start_survives() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO match_dirty_cells VALUES ('bdot10k',14,100,100, TIMESTAMPTZ '2000-01-01');",
        ).unwrap();
        // A newer enqueue of the same cell, timestamped in the future relative to any batch_start now.
        c.execute_batch(
            "INSERT INTO match_dirty_cells VALUES ('bdot10k',14,100,100, TIMESTAMPTZ '2999-01-01');",
        ).unwrap();
        drain_batch(&c, 10).unwrap();
        // The future-timestamped duplicate must remain (its edit is not yet processed).
        let left: i64 = c.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE enqueued_at = TIMESTAMPTZ '2999-01-01'",
            [], |r| r.get(0)).unwrap();
        assert_eq!(left, 1, "a re-dirty after batch_start must not be deleted");
    }
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cargo test --lib compare::drain::`
Expected: FAIL to compile — `drain_batch` not defined.

- [ ] **Step 4: Implement `drain_batch`**

Add above the tests, and `pub mod drain;` to `src/compare/mod.rs`:

```rust
/// Drain up to `batch_size` distinct (source, cell) whose enqueued_at is at or
/// before the batch start. Each cell: recompute + delete its queue rows under
/// the same cutoff, in one transaction. A cell re-dirtied after batch_start
/// keeps a surviving queue row for the next tick.
pub fn drain_batch(conn: &Connection, batch_size: usize) -> Result<DrainStats> {
    // A single wall-clock cutoff for the whole batch.
    let batch_start: String = conn
        .query_row("SELECT now()::VARCHAR", [], |r| r.get(0))
        .context("drain: read batch_start")?;

    let cells: Vec<(String, i32, i32)> = {
        let mut stmt = conn.prepare(
            "SELECT DISTINCT source, cell_x, cell_y FROM match_dirty_cells
             WHERE enqueued_at <= ?::TIMESTAMPTZ
             ORDER BY source, cell_x, cell_y
             LIMIT ?",
        )?;
        let rows = stmt.query_map(duckdb::params![batch_start, batch_size as i64], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i32>(1)?, r.get::<_, i32>(2)?))
        })?;
        let mut v = Vec::new();
        for row in rows {
            v.push(row?);
        }
        v
    };

    let mut drained = 0u64;
    for (source, cx, cy) in &cells {
        conn.execute_batch("BEGIN TRANSACTION")?;
        let res = (|| -> Result<()> {
            recompute_cell_in_txn(conn, source, *cx, *cy)?;
            conn.execute(
                "DELETE FROM match_dirty_cells
                 WHERE source = ? AND cell_x = ? AND cell_y = ? AND enqueued_at <= ?::TIMESTAMPTZ",
                duckdb::params![source, cx, cy, batch_start],
            )?;
            Ok(())
        })();
        match res {
            Ok(()) => {
                conn.execute_batch("COMMIT")?;
                drained += 1;
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(e);
            }
        }
    }
    Ok(DrainStats { cells: drained })
}
```

- [ ] **Step 5: Run to verify pass**

Run: `cargo test --lib compare::drain::`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy --all-targets 2>&1 | tail -5
git add src/compare/incremental.rs src/compare/drain.rs src/compare/mod.rs
git commit -m "feat: add batched, cutoff-ordered dirty-cell drain"
```

## Task 11: `match_refresh` background job

Wire the drain into the scheduler as a job with its own config block.

**Files:**
- Create: `src/server/jobs/match_refresh.rs`
- Modify: `src/server/jobs/mod.rs` (`pub mod match_refresh;`)
- Modify: `src/config.rs` (add `match_refresh` to `JobsConfig` + a `MatchRefreshConfig` with `batch_size`)
- Modify: `src/server/mod.rs` (register the job)
- Modify: `example_config.toml` (document `[jobs.match_refresh]`)
- Test: inline in `match_refresh.rs` and `config.rs`

**Interfaces:**
- Consumes: `compare::drain::drain_batch`, `JobContext`.
- Produces: `pub struct MatchRefreshJob { batch_size: usize }` implementing `Job` with `name() == "match_refresh"`.

- [ ] **Step 1: Add config**

In `src/config.rs`, add a config struct (interval default 30 s, timeout 300 s, `batch_size` 512) and field:

```rust
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct MatchRefreshConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub timeout_seconds: u64,
    pub batch_size: usize,
}

impl Default for MatchRefreshConfig {
    fn default() -> Self {
        Self { enabled: true, interval_seconds: 30, timeout_seconds: 300, batch_size: 512 }
    }
}
```

Add `pub match_refresh: MatchRefreshConfig,` to `JobsConfig` and `match_refresh: MatchRefreshConfig::default(),` to its `Default`. Add a `test_jobs_config_defaults`-style assertion that `config.jobs.match_refresh.interval_seconds == 30`.

- [ ] **Step 2: Write the job (with a unit test driving the drain directly)**

Create `src/server/jobs/match_refresh.rs`:

```rust
use anyhow::{Context, Result};

use crate::server::jobs::{Job, JobContext};

pub struct MatchRefreshJob {
    batch_size: usize,
}

impl MatchRefreshJob {
    pub fn new(batch_size: usize) -> Self {
        Self { batch_size }
    }
}

impl Job for MatchRefreshJob {
    fn name(&self) -> &'static str {
        "match_refresh"
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx.pool.get().context("failed to acquire pool connection")?;
        let stats = crate::compare::drain::drain_batch(&conn, self.batch_size)?;
        if stats.cells > 0 {
            tracing::info!(cells = stats.cells, "match_refresh drained dirty cells");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn name_is_match_refresh() {
        assert_eq!(MatchRefreshJob::new(100).name(), "match_refresh");
    }
}
```

Add `pub mod match_refresh;` to `src/server/jobs/mod.rs`.

- [ ] **Step 3: Register the job**

In `src/server/mod.rs`, after the three `DatasetUpdateJob` registrations, add:

```rust
        (
            Arc::new(jobs::match_refresh::MatchRefreshJob::new(
                config.jobs.match_refresh.batch_size,
            )) as Arc<dyn jobs::Job>,
            jobs::JobConfigResolved {
                enabled: config.jobs.match_refresh.enabled,
                interval: std::time::Duration::from_secs(config.jobs.match_refresh.interval_seconds),
                timeout: std::time::Duration::from_secs(config.jobs.match_refresh.timeout_seconds),
            },
        ),
```

- [ ] **Step 4: Document config**

In `example_config.toml`, add:

```toml
[jobs.match_refresh]
enabled = true
interval_seconds = 30
timeout_seconds = 300
batch_size = 512
```

- [ ] **Step 5: Run**

Run: `cargo test --lib server::jobs::match_refresh:: config::`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy --all-targets 2>&1 | tail -5
git add src/server/jobs/match_refresh.rs src/server/jobs/mod.rs src/config.rs src/server/mod.rs example_config.toml
git commit -m "feat: add match_refresh background job draining the dirty queue"
```

## Task 12: Staleness on `/status`

Add a `match_refresh` staleness summary to the status response: pending cells (total + per source), oldest enqueued, last drain time.

**Files:**
- Modify: `src/server/jobs/status_handler.rs` (extend `StatusResponse`, query the queue)
- Test: inline in `status_handler.rs`

**Interfaces:**
- Consumes: `AppState.pool`, `match_dirty_cells`.
- Produces: `StatusResponse.match_staleness: MatchStaleness` with `pending_total: i64`, `pending_by_source: Vec<(String, i64)>`, `oldest_enqueued_at: Option<String>`.

- [ ] **Step 1: Write the failing test**

Add to `status_handler.rs` a test that seeds `match_dirty_cells` and asserts the computed staleness:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    #[test]
    fn staleness_counts_distinct_cells_per_source() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "INSERT INTO match_dirty_cells VALUES
                 ('bdot10k',14,1,1,now()), ('bdot10k',14,1,1,now()), -- dup, one distinct cell
                 ('prg',14,2,2,now());",
        ).unwrap();
        let s = compute_match_staleness(&conn).unwrap();
        assert_eq!(s.pending_total, 2, "distinct (source,cell) pairs");
        assert!(s.pending_by_source.iter().any(|(k, v)| k == "bdot10k" && *v == 1));
        assert!(s.oldest_enqueued_at.is_some());
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib server::jobs::status_handler::`
Expected: FAIL — `compute_match_staleness`/`MatchStaleness` not defined.

- [ ] **Step 3: Implement**

In `status_handler.rs`:

```rust
use anyhow::{Context, Result};
use duckdb::Connection;

#[derive(Serialize)]
pub struct MatchStaleness {
    pub pending_total: i64,
    pub pending_by_source: Vec<(String, i64)>,
    pub oldest_enqueued_at: Option<String>,
}

pub fn compute_match_staleness(conn: &Connection) -> Result<MatchStaleness> {
    let pending_total: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM (SELECT DISTINCT source, cell_x, cell_y FROM match_dirty_cells)",
            [],
            |r| r.get(0),
        )
        .context("count pending cells")?;
    let mut by_source = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT source, COUNT(*) FROM (SELECT DISTINCT source, cell_x, cell_y FROM match_dirty_cells)
             GROUP BY source ORDER BY source",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
        for row in rows {
            by_source.push(row?);
        }
    }
    let oldest: Option<String> = conn
        .query_row("SELECT MIN(enqueued_at)::VARCHAR FROM match_dirty_cells", [], |r| r.get(0))
        .context("oldest enqueued")?;
    Ok(MatchStaleness { pending_total, pending_by_source: by_source, oldest_enqueued_at: oldest })
}
```

Add `pub match_staleness: MatchStaleness` to `StatusResponse` and populate it in `get_status` (acquire a pool connection via `state.pool`; on error fall back to an empty/zeroed staleness rather than failing the whole endpoint).

- [ ] **Step 4: Run**

Run: `cargo test --lib server::jobs::status_handler::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets 2>&1 | tail -5
git add src/server/jobs/status_handler.rs
git commit -m "feat: report match_refresh staleness on /status"
```

## Task 13: Reconciliation sweep

A path that enqueues every cell containing a government object, so the normal drain repairs any dropped enqueue. Exposed as a CLI subcommand of `compare` (e.g. `compare reconcile`) so it can run offline, and callable from a daily job if desired.

**Files:**
- Create: `src/compare/reconcile.rs`
- Modify: `src/compare/mod.rs`, `src/cli.rs` (add a `Reconcile` compare target), `src/main.rs` (dispatch)
- Test: inline

**Interfaces:**
- Consumes: `dataset::{BDOT10K, EGIB, PRG}`, `tile_math::{cell_x_sql, cell_y_sql, CHANGE_CELL_ZOOM}`.
- Produces: `pub fn enqueue_all(conn: &Connection) -> Result<i64>` — inserts one dirty-cell row per distinct (source, cell) over all three live tables; returns rows enqueued.

- [ ] **Step 1: Write the failing test**

Create `src/compare/reconcile.rs`:

```rust
use anyhow::{Context, Result};
use duckdb::Connection;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    #[test]
    fn enqueue_all_covers_every_live_cell_once() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);
             CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES ('a', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             INSERT INTO prg_addresses VALUES ('p', ST_Point(19.0,50.0));",
        ).unwrap();
        let n = enqueue_all(&c).unwrap();
        assert_eq!(n, 2, "one bdot10k cell + one prg cell");
        let by: Vec<(String, i64)> = {
            let mut s = c.prepare(
                "SELECT source, COUNT(*) FROM match_dirty_cells GROUP BY source ORDER BY source").unwrap();
            s.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(|r| r.unwrap()).collect()
        };
        assert_eq!(by, vec![("bdot10k".into(), 1), ("prg".into(), 1)]);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib compare::reconcile::`
Expected: FAIL to compile.

- [ ] **Step 3: Implement `enqueue_all`**

```rust
use crate::tile_math::{cell_x_sql, cell_y_sql, CHANGE_CELL_ZOOM};

/// Enqueue every distinct (source, z14 cell) present in the live tables, so the
/// drain rebuilds them. Repairs any dropped enqueue; also the offline rebuild path.
pub fn enqueue_all(conn: &Connection) -> Result<i64> {
    let z = CHANGE_CELL_ZOOM;
    let specs = [
        ("bdot10k", "bdot10k_buildings", "ST_Centroid(geom)"),
        ("egib", "egib_buildings", "ST_Centroid(geom)"),
        ("prg", "prg_addresses", "geom"),
    ];
    let mut total = 0i64;
    for (source, table, point) in specs {
        let cx = cell_x_sql(point);
        let cy = cell_y_sql(point);
        conn.execute_batch(&format!(
            "INSERT INTO match_dirty_cells
             SELECT DISTINCT '{source}', {z}, {cx}, {cy}, now()
             FROM {table} WHERE geom IS NOT NULL"
        ))
        .with_context(|| format!("enqueue_all for {source}"))?;
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE source = ?",
            duckdb::params![source], |r| r.get(0))?;
        total += n;
    }
    Ok(total)
}
```

Add `pub mod reconcile;` to `src/compare/mod.rs`. Wire a `CompareTarget::Reconcile` CLI variant that calls `enqueue_all` and logs the count (dispatch in `src/main.rs` and `compare::run`).

- [ ] **Step 4: Run**

Run: `cargo test --lib compare::reconcile::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets 2>&1 | tail -5
git add src/compare/reconcile.rs src/compare/mod.rs src/cli.rs src/main.rs
git commit -m "feat: add reconciliation sweep that enqueues every live cell"
```

## Task 14: Full-suite verification and docs

**Files:**
- Modify: `README.md` (compare now produces `*_unmatched`; `/tiles` and `/package` serve unmatched; `match_refresh` job + `compare reconcile`), `CLAUDE.md` (note the two new invariants: one match-rule home, the drain cutoff ordering)

- [ ] **Step 1: Full test suite**

Run: `cargo test`
Expected: PASS (all unit + integration).

- [ ] **Step 2: Clippy + fmt**

Run: `cargo clippy --all-targets && cargo fmt -- --check`
Expected: clean.

- [ ] **Step 3: End-to-end smoke**

Import osm+bdot10k+egib+prg into a scratch DB, `compare full`, confirm `*_unmatched` populated; `update osm` (or a fixture osc) and confirm `match_dirty_cells` grows; run one `drain_batch` (via a short `run` server or a test hook) and confirm the affected cell's rows change and the queue shrinks.

- [ ] **Step 4: Update docs**

Reflect the new pipeline in `README.md` and add to `CLAUDE.md`'s gotchas: the match rule has one home (`compare/rule.rs`); the drain's `enqueued_at <= batch_start` cutoff on both read and queue-delete is load-bearing; serving tables store rows, not id-references.

- [ ] **Step 5: Commit**

```bash
git add README.md CLAUDE.md
git commit -m "docs: document precomputed unmatched serving pipeline"
```

---

## Self-review notes

- **Spec coverage:** data model (Task 2), serving-table column choices (Task 2), dirty queue keyed per source (Task 2), government producer (Task 8), OSM producer with 3×3 (Task 9), shared match rule + `/package` anti-join removal (Tasks 3, 6), buffered-read/narrow-write (Tasks 3, 7), `compare` full recompute repoint (Task 4), equivalence guard (Task 5), per-cell recompute (Task 7), batched cutoff-ordered drain (Task 10), `match_refresh` job (Task 11), `/status` staleness (Task 12), reconciliation sweep (Task 13). Build-order steps 1–8 all map to tasks.
- **`apply_changes` signature preserved:** it owns the `DirtyCells` and flushes before returning, rather than taking it as a parameter, so its 5 existing test callers keep compiling (verified against osm.rs). Only the internal `rebuild_*` helpers gain the parameter.
- **Open implementation choice deferred to the executor:** whether `note_existing` after-insert calls in `rebuild_*` should read back the row or compute from the known geom expression — either satisfies "record the arriving cell"; the plan uses read-back for uniformity.
- **Fixture dependency:** Task 9's integration test needs an `.osc.gz` that touches a served object within the fixtures' extent. If `fixtures/osm.osc.gz` does not, add a small one via `fixtures/scripts/`.
- **Existing `dataset.rs` tests now also run `insert_dirty_cells`:** every `refresh()` in those tests enqueues cells (harmless; no test asserts the queue is empty). `init_db` creates `match_dirty_cells`, so the inserts succeed.
