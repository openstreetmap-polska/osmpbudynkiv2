# Persist and index a `centroid` column on bdot10k/egib — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the per-cell recompute full-scan (`docs/per_cell_recompute_full_scan.md`) by storing `ST_Centroid(geom)` as an indexed column on `bdot10k_buildings`/`egib_buildings` instead of recomputing it inside `compare::rule::unmatched_buildings_sql`'s hot-loop predicate, where an RTREE index can't see through the function call.

**Architecture:** Add `centroid GEOMETRY` to `bdot10k_buildings`/`egib_buildings` only, populated by `import::bdot10k::load_into`/`import::egib::load_into` (shared by `import` and `update`'s staging load) as an outer wrap around `hashed_select`'s output — so `_row_hash` never sees the new column and no `ROW_HASH_VERSION` bump is needed. Every place that currently computes `ST_Centroid(geom)` against these two tables switches to reading the stored column; RTREE-index it the same way `geom` already is.

**Tech Stack:** Rust, DuckDB (spatial extension), existing `anyhow`/`duckdb` crate patterns already used throughout the codebase.

**Design doc:** `docs/superpowers/specs/2026-08-01-bdot10k-egib-centroid-index-design.md`

## Global Constraints

- Scope is `bdot10k_buildings`/`egib_buildings` only. Do not touch `prg_addresses`, `bdot10k_unmatched`/`egib_unmatched` (serving tables), or `osm_buildings`/`osm_addresses`.
- `centroid` must be added **outside** `hashed_select`'s projection — never let it become part of `hash(s)`'s input, or every row's `_row_hash` changes and `ROW_HASH_VERSION` must bump (it must not, for this change).
- No migration code for already-existing databases. Ship as "re-import `bdot10k`/`egib` to get the speedup," documented in CLAUDE.md.
- **Fixture rule, applies to every test touched in this plan:** any hand-built `bdot10k_buildings`/`egib_buildings` table must include `centroid GEOMETRY` in its `CREATE TABLE`, **even if the test never inserts a row into it** — `compare::reconcile::enqueue_all` and `compare::incremental::recompute_cell_in_txn` reference the `centroid` column by name whenever they touch that table, and DuckDB's binder requires the column to exist regardless of row count. Where rows ARE inserted and are expected to flow through matching logic, populate `centroid` with `UPDATE <table> SET centroid = ST_Centroid(geom);` after the insert (switching the `INSERT` to an explicit column list — `(LOKALNYID, geom)` / `(id_budynku, geom)` — since the table now has 3 columns instead of 2).

---

### Task 1: `dataset.rs` — stored-centroid plumbing

**Files:**
- Modify: `src/dataset.rs`

**Interfaces:**
- Produces: `DatasetSpec::with_centroid_select(&self, select_sql: &str) -> String` — wraps `select_sql` (already passed through `hashed_select`) so `GeomKind::Polygon` sources gain a `centroid GEOMETRY` column computed from `geom`; a no-op passthrough for `GeomKind::Point`.
- Produces (changed signature): `DatasetSpec::representative_point_sql(&self, alias: &str) -> String` — was `(&self, geom_expr: &str) -> String`. Now takes a bare table alias (e.g. `"l"`, `"s"`) and returns `"{alias}.geom"` for `Point`, `"{alias}.centroid"` for `Polygon`.

- [ ] **Step 1: Change `representative_point_sql`'s signature and update its doc comment**

In `src/dataset.rs`, replace:

```rust
    /// SQL for the point that represents this object when assigning it to a
    /// change cell.
    pub fn representative_point_sql(&self, geom_expr: &str) -> String {
        match self.geom_kind {
            GeomKind::Point => geom_expr.to_string(),
            GeomKind::Polygon => format!("ST_Centroid({geom_expr})"),
        }
    }
```

with:

```rust
    /// SQL for the point that represents this object when assigning it to a
    /// change cell. `alias` is the table alias in the surrounding query
    /// (e.g. `"l"` for the live table, `"s"` for staging). For `Point`
    /// sources the geometry itself is the point; for `Polygon` sources this
    /// reads the persisted `centroid` column (see `with_centroid_select`)
    /// rather than recomputing `ST_Centroid` — the whole reason that column
    /// exists is so this stops being a per-row function call.
    pub fn representative_point_sql(&self, alias: &str) -> String {
        match self.geom_kind {
            GeomKind::Point => format!("{alias}.geom"),
            GeomKind::Polygon => format!("{alias}.centroid"),
        }
    }
```

- [ ] **Step 2: Add `with_centroid_select` next to `representative_point_sql`**

Add this method to the same `impl DatasetSpec` block:

```rust
    /// Wrap `select_sql` (the output of [`hashed_select`]) so a `Polygon`
    /// source also gains a persisted `centroid GEOMETRY` column, computed
    /// from `geom`. Added OUTSIDE `hashed_select`'s projection deliberately:
    /// `hash(s)` inside `hashed_select` already ran over the inner columns
    /// only, so wrapping here cannot change any row's `_row_hash` and needs
    /// no `ROW_HASH_VERSION` bump. A no-op passthrough for `Point` sources
    /// (PRG), which have no separate centroid to store.
    pub fn with_centroid_select(&self, select_sql: &str) -> String {
        match self.geom_kind {
            GeomKind::Point => select_sql.to_string(),
            GeomKind::Polygon => {
                format!("SELECT *, ST_Centroid(geom) AS centroid FROM ({select_sql}) t")
            }
        }
    }
```

- [ ] **Step 3: Update the existing `representative_point_sql` unit tests**

In the `#[cfg(test)] mod tests` block, replace:

```rust
    #[test]
    fn representative_point_uses_centroid_for_polygons() {
        assert_eq!(
            BDOT10K.representative_point_sql("geom"),
            "ST_Centroid(geom)"
        );
        assert_eq!(EGIB.representative_point_sql("geom"), "ST_Centroid(geom)");
    }

    #[test]
    fn representative_point_passes_through_for_points() {
        assert_eq!(PRG.representative_point_sql("geom"), "geom");
    }
```

with:

```rust
    #[test]
    fn representative_point_reads_the_stored_centroid_for_polygons() {
        assert_eq!(BDOT10K.representative_point_sql("l"), "l.centroid");
        assert_eq!(EGIB.representative_point_sql("s"), "s.centroid");
    }

    #[test]
    fn representative_point_passes_through_for_points() {
        assert_eq!(PRG.representative_point_sql("l"), "l.geom");
    }
```

- [ ] **Step 4: Add tests for `with_centroid_select`**

Add to the same test module:

```rust
    #[test]
    fn with_centroid_select_wraps_polygon_sources_outside_the_hash() {
        let hashed = hashed_select("SELECT 1 AS a, ST_Point(1, 2) AS geom");
        let wrapped = BDOT10K.with_centroid_select(&hashed);
        assert_eq!(
            wrapped,
            format!("SELECT *, ST_Centroid(geom) AS centroid FROM ({hashed}) t")
        );
    }

    #[test]
    fn with_centroid_select_is_a_noop_for_points() {
        let hashed = hashed_select("SELECT 1 AS a, ST_Point(1, 2) AS geom");
        assert_eq!(PRG.with_centroid_select(&hashed), hashed);
    }

    /// The load-bearing invariant from the module doc: adding `centroid` via
    /// `with_centroid_select` must not change `_row_hash`, since it wraps
    /// `hashed_select`'s output rather than feeding into it. If this ever
    /// regresses, every refresh would compare every row as modified forever
    /// (see `ROW_HASH_VERSION`) without anyone bumping the constant.
    #[test]
    fn with_centroid_select_does_not_change_the_row_hash() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE src (id VARCHAR, geom GEOMETRY);
             INSERT INTO src VALUES
                 ('1', ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')),
                 ('2', NULL);",
        )
        .unwrap();

        let inner = "SELECT id, geom FROM src";
        let hashed = hashed_select(inner);
        let with_centroid = BDOT10K.with_centroid_select(&hashed);

        conn.execute_batch(&format!(
            "CREATE TABLE plain AS {hashed};
             CREATE TABLE with_centroid AS {with_centroid};"
        ))
        .unwrap();

        let disagreements: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM plain p JOIN with_centroid c USING (id)
                 WHERE p._row_hash IS DISTINCT FROM c._row_hash",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            disagreements, 0,
            "adding centroid outside hashed_select's wrap must not change _row_hash"
        );

        let has_centroid: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM information_schema.columns
                 WHERE table_name = 'with_centroid' AND column_name = 'centroid'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            has_centroid,
            "the wrapped table must actually carry the centroid column"
        );
    }
```

- [ ] **Step 5: Run the tests**

Run: `cargo test --lib dataset::`
Expected: all pass, including the four new/renamed ones above.

- [ ] **Step 6: Commit**

```bash
git add src/dataset.rs
git commit -m "$(cat <<'EOF'
feat(dataset): add DatasetSpec::with_centroid_select, alias-based representative_point_sql

Prepares to persist ST_Centroid(geom) as a stored column on bdot10k/egib
instead of recomputing it in the hot match predicate. Kept outside
hashed_select's projection so _row_hash is unaffected -- no
ROW_HASH_VERSION bump needed.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: `import/bdot10k.rs` — populate and index `centroid`

**Files:**
- Modify: `src/import/bdot10k.rs`

**Interfaces:**
- Consumes: `crate::dataset::BDOT10K.with_centroid_select(&str) -> String` (Task 1).

- [ ] **Step 1: Wrap `load_into`'s `CREATE TABLE` with `with_centroid_select`**

Replace:

```rust
pub fn load_into(conn: &Connection, target_table: &str, parquet_path: &str) -> Result<LoadStats> {
    let inner = format!(
        "SELECT * EXCLUDE(GEOM), \
         ST_Transform(ST_GeomFromWKB(GEOM), 'EPSG:2180', 'EPSG:4326') AS geom \
         FROM '{parquet_path}'"
    );
    conn.execute_batch(&format!(
        "SET enable_geoparquet_conversion = false;
         DROP TABLE IF EXISTS {target_table};
         CREATE TABLE {target_table} AS {};
         SET enable_geoparquet_conversion = true;",
        crate::dataset::hashed_select(&inner)
    ))
    .with_context(|| format!("Failed to load BDOT10k data into {target_table}"))?;

    crate::dataset::filter_invalid_geometry(conn, target_table, "LOKALNYID")
}
```

with:

```rust
pub fn load_into(conn: &Connection, target_table: &str, parquet_path: &str) -> Result<LoadStats> {
    let inner = format!(
        "SELECT * EXCLUDE(GEOM), \
         ST_Transform(ST_GeomFromWKB(GEOM), 'EPSG:2180', 'EPSG:4326') AS geom \
         FROM '{parquet_path}'"
    );
    let select =
        crate::dataset::BDOT10K.with_centroid_select(&crate::dataset::hashed_select(&inner));
    conn.execute_batch(&format!(
        "SET enable_geoparquet_conversion = false;
         DROP TABLE IF EXISTS {target_table};
         CREATE TABLE {target_table} AS {select};
         SET enable_geoparquet_conversion = true;"
    ))
    .with_context(|| format!("Failed to load BDOT10k data into {target_table}"))?;

    crate::dataset::filter_invalid_geometry(conn, target_table, "LOKALNYID")
}
```

- [ ] **Step 2: Add the centroid RTREE index in `import()`**

Replace:

```rust
        let t = std::time::Instant::now();
        conn.execute_batch(
            "CREATE INDEX bdot10k_buildings_geom_idx ON bdot10k_buildings USING RTREE (geom);",
        )
        .context("Failed to create spatial index on bdot10k_buildings")?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: create spatial index"
        );
```

with:

```rust
        let t = std::time::Instant::now();
        conn.execute_batch(
            "CREATE INDEX bdot10k_buildings_geom_idx ON bdot10k_buildings USING RTREE (geom);
             CREATE INDEX bdot10k_buildings_centroid_idx ON bdot10k_buildings USING RTREE (centroid);",
        )
        .context("Failed to create spatial indexes on bdot10k_buildings")?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: create spatial indexes"
        );
```

- [ ] **Step 3: Extend `load_into_the_fixture_has_no_invalid_geometry` to assert `centroid`**

Add to the end of that test (after the existing `count` assertion):

```rust
        let mismatched: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM bdot10k_buildings
                 WHERE centroid IS DISTINCT FROM ST_Centroid(geom)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            mismatched, 0,
            "centroid must equal ST_Centroid(geom) for every row"
        );
```

- [ ] **Step 4: Run this file's tests**

Run: `cargo test --lib import::bdot10k::`
Expected: all pass, including the extended fixture test.

- [ ] **Step 5: Commit**

```bash
git add src/import/bdot10k.rs
git commit -m "$(cat <<'EOF'
feat(import): persist and RTREE-index centroid on bdot10k_buildings

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: `import/egib.rs` — mirror Task 2

**Files:**
- Modify: `src/import/egib.rs`

**Interfaces:**
- Consumes: `crate::dataset::EGIB.with_centroid_select(&str) -> String` (Task 1).

- [ ] **Step 1: Wrap `load_into`'s `CREATE TABLE` with `with_centroid_select`**

Replace:

```rust
pub fn load_into(conn: &Connection, target_table: &str, parquet_path: &str) -> Result<LoadStats> {
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
    .with_context(|| format!("Failed to load EGIB data into {target_table}"))?;

    crate::dataset::filter_invalid_geometry(conn, target_table, "id_budynku")
}
```

with:

```rust
pub fn load_into(conn: &Connection, target_table: &str, parquet_path: &str) -> Result<LoadStats> {
    let inner = format!(
        "SELECT * EXCLUDE(geometry, geometry_bbox), \
         ST_Transform(geometry, 'EPSG:2180', 'EPSG:4326') AS geom \
         FROM '{parquet_path}'"
    );
    let select =
        crate::dataset::EGIB.with_centroid_select(&crate::dataset::hashed_select(&inner));
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {target_table};
         CREATE TABLE {target_table} AS {select};"
    ))
    .with_context(|| format!("Failed to load EGIB data into {target_table}"))?;

    crate::dataset::filter_invalid_geometry(conn, target_table, "id_budynku")
}
```

- [ ] **Step 2: Add the centroid RTREE index in `import()`**

Replace:

```rust
        let t = std::time::Instant::now();
        conn.execute_batch(
            "CREATE INDEX egib_buildings_geom_idx ON egib_buildings USING RTREE (geom);",
        )
        .context("Failed to create spatial index on egib_buildings")?;
```

with:

```rust
        let t = std::time::Instant::now();
        conn.execute_batch(
            "CREATE INDEX egib_buildings_geom_idx ON egib_buildings USING RTREE (geom);
             CREATE INDEX egib_buildings_centroid_idx ON egib_buildings USING RTREE (centroid);",
        )
        .context("Failed to create spatial indexes on egib_buildings")?;
```

(Leave the surrounding `info!(...)` log lines as-is; only the `execute_batch` call and its `.context(...)` message change.)

- [ ] **Step 3: Extend `load_into_the_fixture_has_no_invalid_geometry` to assert `centroid`**

Add to the end of that test:

```rust
        let mismatched: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM egib_buildings
                 WHERE centroid IS DISTINCT FROM ST_Centroid(geom)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            mismatched, 0,
            "centroid must equal ST_Centroid(geom) for every row"
        );
```

- [ ] **Step 4: Run this file's tests**

Run: `cargo test --lib import::egib::`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/import/egib.rs
git commit -m "$(cat <<'EOF'
feat(import): persist and RTREE-index centroid on egib_buildings

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: `compare/rule.rs` — the actual hot-loop fix

**Files:**
- Modify: `src/compare/rule.rs`

**Interfaces:**
- Produces (unchanged name, changed SQL): `unmatched_buildings_sql(source_table: &str, select_list: &str, area: Bounds) -> String` now requires `source_table` to have a `centroid GEOMETRY` column.

- [ ] **Step 1: Swap the predicate to read `b.centroid`**

Replace:

```rust
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
```

with:

```rust
/// Unmatched building rows: government centroid within `area` and NOT contained
/// by any osm_buildings polygon (osm filtered to `area` for the R-tree scan —
/// no buffer needed: any polygon containing an in-`area` point has a bbox that
/// intersects `area`).
///
/// `source_table` must carry a `centroid GEOMETRY` column (bdot10k_buildings
/// and egib_buildings both do — see `DatasetSpec::with_centroid_select`).
/// Reading the stored column instead of computing `ST_Centroid(b.geom)` here
/// is the fix for the full-table-scan bottleneck in
/// docs/per_cell_recompute_full_scan.md: an RTREE index cannot be used
/// through a function wrapped around the indexed column, but it can be used
/// against a plain column reference.
pub fn unmatched_buildings_sql(source_table: &str, select_list: &str, area: Bounds) -> String {
    let (x1, y1, x2, y2) = area;
    format!(
        "SELECT {select_list}
         FROM {source_table} b
         WHERE ST_Intersects(b.centroid, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
           AND NOT EXISTS (
               SELECT 1 FROM osm_buildings osm
               WHERE ST_Intersects(osm.geom, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
                 AND ST_Contains(osm.geom, b.centroid)
           )"
    )
}
```

- [ ] **Step 2: Update the `conn()` test fixture to include `centroid`**

Replace:

```rust
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
```

with:

```rust
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
            "CREATE TABLE bsrc (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE asrc (lokalny_id VARCHAR, numer_porzadkowy VARCHAR, geom GEOMETRY);",
        )
        .unwrap();
        c
    }
```

- [ ] **Step 3: Update `building_contained_by_osm_is_not_unmatched` to populate `centroid`**

Replace:

```rust
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
```

with:

```rust
    #[test]
    fn building_contained_by_osm_is_not_unmatched() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES
                 (1,'way',NULL, ST_MakeEnvelope(20.0,52.0,20.002,52.002));
             INSERT INTO bsrc (LOKALNYID, geom) VALUES
                 ('in', ST_MakeEnvelope(20.0005,52.0005,20.0007,52.0007)),
                 ('out', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             UPDATE bsrc SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
```

(the rest of that test is unchanged.)

- [ ] **Step 4: Add the RTREE-plan regression test**

This is the actual guard for the performance fix — without it, a future edit could silently reintroduce `ST_Centroid(b.geom)` and no test would fail. Add to the test module (mirrors `server::tiles::tests::mvt_bbox_filter_uses_the_rtree_index`'s approach of using enough rows that the optimizer prefers an index):

```rust
    /// The actual regression guard for the per-cell-recompute fix
    /// (docs/per_cell_recompute_full_scan.md): if `unmatched_buildings_sql`
    /// ever goes back to wrapping the indexed column in `ST_Centroid()`, this
    /// fails, because an RTREE index cannot be used through a function
    /// applied to the indexed column.
    #[test]
    fn unmatched_buildings_predicate_uses_the_centroid_rtree_index() {
        let c = conn();
        c.execute_batch(
            "CREATE INDEX bsrc_centroid_idx ON bsrc USING RTREE (centroid);
             INSERT INTO bsrc (LOKALNYID, geom)
                 SELECT 'b' || i,
                        ST_MakeEnvelope(20.0 + i * 0.0001, 52.0,
                                        20.0 + i * 0.0001 + 0.00005, 52.00005)
                 FROM range(20000) t(i);
             UPDATE bsrc SET centroid = ST_Centroid(geom);",
        )
        .unwrap();

        let sql = unmatched_buildings_sql("bsrc", "b.LOKALNYID", (20.5, 52.0, 20.6, 52.1));
        let mut stmt = c.prepare(&format!("EXPLAIN {sql}")).unwrap();
        let mut rows = stmt.query([]).unwrap();
        let mut plan = String::new();
        while let Some(row) = rows.next().unwrap() {
            plan.push_str(&row.get::<_, String>(1).unwrap_or_default());
        }
        assert!(
            plan.contains("RTREE_INDEX_SCAN"),
            "the predicate must be able to use the centroid RTREE index, got plan: {plan}"
        );
    }
```

- [ ] **Step 5: Run this file's tests**

Run: `cargo test --lib compare::rule::`
Expected: all pass, including the new RTREE-plan test.

- [ ] **Step 6: Commit**

```bash
git add src/compare/rule.rs
git commit -m "$(cat <<'EOF'
fix(compare): read stored centroid instead of ST_Centroid(geom) in the match rule

Fixes the full-table-scan bottleneck measured in
docs/per_cell_recompute_full_scan.md: an RTREE index cannot be used
through a function wrapped around the indexed column. Both full building
compare and the per-cell incremental recompute share this predicate, so
both get faster from this one change.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: `compare/buildings.rs` — cell tagging and its five fixtures

**Files:**
- Modify: `src/compare/buildings.rs`

- [ ] **Step 1: Swap `cx`/`cy` and the grid boundary guard to `b.centroid`**

Replace:

```rust
    let cx = cell_x_sql("ST_Centroid(b.geom)");
    let cy = cell_y_sql("ST_Centroid(b.geom)");
    let select = format!("b.{id_col}, b.geom, {cx}, {cy}, now()");
```

with:

```rust
    let cx = cell_x_sql("b.centroid");
    let cy = cell_y_sql("b.centroid");
    let select = format!("b.{id_col}, b.geom, {cx}, {cy}, now()");
```

Replace:

```rust
            conn.execute_batch(&format!(
                "INSERT INTO {dest} ({id_col}, geom, cell_x, cell_y, computed_at)
                 {inner}
                   AND ST_X(ST_Centroid(b.geom)) >= {x} AND ST_X(ST_Centroid(b.geom)) < {x_hi}
                   AND ST_Y(ST_Centroid(b.geom)) >= {y} AND ST_Y(ST_Centroid(b.geom)) < {y_hi};"
            ))
```

with:

```rust
            conn.execute_batch(&format!(
                "INSERT INTO {dest} ({id_col}, geom, cell_x, cell_y, computed_at)
                 {inner}
                   AND ST_X(b.centroid) >= {x} AND ST_X(b.centroid) < {x_hi}
                   AND ST_Y(b.centroid) >= {y} AND ST_Y(b.centroid) < {y_hi};"
            ))
```

- [ ] **Step 2: Update `writes_only_unmatched_rows_with_cell_tags`'s fixture**

Replace:

```rust
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES
                 ('inside', ST_MakeEnvelope(20.0002,52.0002,20.0008,52.0008)),
                 ('lonely', ST_MakeEnvelope(21.0,52.2,21.001,52.201));",
        )
        .unwrap();
        compare_bdot10k(&conn).unwrap();
```

with:

```rust
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('inside', ST_MakeEnvelope(20.0002,52.0002,20.0008,52.0008)),
                 ('lonely', ST_MakeEnvelope(21.0,52.2,21.001,52.201));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
        compare_bdot10k(&conn).unwrap();
```

- [ ] **Step 3: Update `compare_is_idempotent`'s fixture**

Replace:

```rust
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES ('lonely', ST_MakeEnvelope(21.0,52.2,21.001,52.201));",
        )
        .unwrap();
```

with:

```rust
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES ('lonely', ST_MakeEnvelope(21.0,52.2,21.001,52.201));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
```

- [ ] **Step 4: Update `refuses_to_build_a_grid_around_a_wild_coordinate`'s fixture**

Replace:

```rust
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES
                 ('sane', ST_MakeEnvelope(20.0,52.0,20.001,52.001)),
                 ('wild', ST_MakeEnvelope(-180.0,-90.0,-179.999,-89.999));",
        )
        .unwrap();
```

with:

```rust
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('sane', ST_MakeEnvelope(20.0,52.0,20.001,52.001)),
                 ('wild', ST_MakeEnvelope(-180.0,-90.0,-179.999,-89.999));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
```

- [ ] **Step 5: Update `covers_a_row_outside_the_historical_poland_bbox`'s fixture**

Replace:

```rust
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES
                 ('stray', ST_MakeEnvelope(30.0,60.0,30.001,60.001));",
        )
        .unwrap();
```

with:

```rust
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('stray', ST_MakeEnvelope(30.0,60.0,30.001,60.001));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
```

- [ ] **Step 6: Update `compare_chunked_duplicates_source_on_cell_boundary`'s fixture**

Replace:

```rust
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES
                 -- centroid (14.5, 52.25): x = 14.5 is the boundary between
                 -- the [14.0,14.5) and [14.5,15.0) grid columns; y = 52.25 is
                 -- safely mid-row.
                 ('boundary', ST_MakeEnvelope(14.4998,52.2498,14.5002,52.2502)),
                 -- Widens the source extent so both neighbouring columns are
                 -- actually scanned by the chunked loop -- a lone boundary
                 -- point would otherwise sit at a tightly-snapped extent edge
                 -- and only ever be scanned by one cell, which would not
                 -- exercise the guard at all.
                 ('anchor', ST_MakeEnvelope(14.05,52.25,14.06,52.26));",
        )
        .unwrap();
```

with:

```rust
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 -- centroid (14.5, 52.25): x = 14.5 is the boundary between
                 -- the [14.0,14.5) and [14.5,15.0) grid columns; y = 52.25 is
                 -- safely mid-row.
                 ('boundary', ST_MakeEnvelope(14.4998,52.2498,14.5002,52.2502)),
                 -- Widens the source extent so both neighbouring columns are
                 -- actually scanned by the chunked loop -- a lone boundary
                 -- point would otherwise sit at a tightly-snapped extent edge
                 -- and only ever be scanned by one cell, which would not
                 -- exercise the guard at all.
                 ('anchor', ST_MakeEnvelope(14.05,52.25,14.06,52.26));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
```

- [ ] **Step 7: Run this file's tests**

Run: `cargo test --lib compare::buildings::`
Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add src/compare/buildings.rs
git commit -m "$(cat <<'EOF'
fix(compare): tag full-compare rows from the stored centroid, not ST_Centroid(geom)

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: `compare/incremental.rs` — per-cell recompute tagging

**Files:**
- Modify: `src/compare/incremental.rs`

- [ ] **Step 1: Swap `cx`/`cy` in the bdot10k/egib branch to `b.centroid`**

Replace:

```rust
            let cx = cell_x_sql("ST_Centroid(b.geom)");
            let cy = cell_y_sql("ST_Centroid(b.geom)");
```

with:

```rust
            let cx = cell_x_sql("b.centroid");
            let cy = cell_y_sql("b.centroid");
```

- [ ] **Step 2: Add `centroid` to the shared test `conn()` fixture**

Replace:

```rust
        c.execute_batch("CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);")
            .unwrap();
        c
    }
```

with:

```rust
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);",
        )
        .unwrap();
        c
    }
```

- [ ] **Step 3: Populate `centroid` in `recompute_replaces_only_that_cell`**

Replace:

```rust
        c.execute_batch(
            "INSERT INTO bdot10k_buildings VALUES
                 ('p', ST_MakeEnvelope(21.0,52.0,21.001,52.001)),
                 ('q', ST_MakeEnvelope(19.0,50.0,19.001,50.001));",
        )
        .unwrap();
```

with:

```rust
        c.execute_batch(
            "INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('p', ST_MakeEnvelope(21.0,52.0,21.001,52.001)),
                 ('q', ST_MakeEnvelope(19.0,50.0,19.001,50.001));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
```

- [ ] **Step 4: Populate `centroid` in `write_narrow_by_cell_tag_prevents_boundary_duplicates`**

Replace:

```rust
        c.execute_batch(&format!(
            "INSERT INTO bdot10k_buildings VALUES ('boundary', ST_Point({boundary_lon}, {mid_lat}));"
        ))
        .unwrap();
```

with:

```rust
        c.execute_batch(&format!(
            "INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES ('boundary', ST_Point({boundary_lon}, {mid_lat}));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);"
        ))
        .unwrap();
```

Leave the rest of that test (the `true_cx` lookup via `cell_x_sql("geom")`, both `recompute_cell` calls, and the final assertion) unchanged — that lookup queries the raw `geom` column directly and is independent of this change.

- [ ] **Step 5: Run this file's tests**

Run: `cargo test --lib compare::incremental::`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/compare/incremental.rs
git commit -m "$(cat <<'EOF'
fix(compare): tag per-cell recompute rows from the stored centroid, not ST_Centroid(geom)

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: `compare/reconcile.rs` — `enqueue_all`'s point expression

**Files:**
- Modify: `src/compare/reconcile.rs`

- [ ] **Step 1: Swap the bdot10k/egib point-expression literals**

Replace:

```rust
    let specs = [
        ("bdot10k", "bdot10k_buildings", "ST_Centroid(geom)"),
        ("egib", "egib_buildings", "ST_Centroid(geom)"),
        ("prg", "prg_addresses", "geom"),
    ];
```

with:

```rust
    let specs = [
        ("bdot10k", "bdot10k_buildings", "centroid"),
        ("egib", "egib_buildings", "centroid"),
        ("prg", "prg_addresses", "geom"),
    ];
```

- [ ] **Step 2: Update `enqueue_all_covers_every_live_cell_once`'s fixture**

Replace:

```rust
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);
             CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES ('a', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             INSERT INTO prg_addresses VALUES ('p', ST_Point(19.0,50.0));",
        )
        .unwrap();
```

with:

```rust
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES ('a', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             INSERT INTO prg_addresses VALUES ('p', ST_Point(19.0,50.0));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
```

(`egib_buildings` stays empty in this test, but still needs the `centroid` column per the Global Constraints fixture rule — `enqueue_all` queries it unconditionally.)

- [ ] **Step 3: Update `enqueue_all_collapses_cells_skips_null_geom_and_covers_egib`'s fixture**

Replace:

```rust
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);
             CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);

             -- Three bdot10k buildings inside ONE z14 cell (cells are ~0.022
             -- deg wide here, so these cannot straddle a boundary), plus one
             -- far away: 2 distinct cells, not 4 rows.
             INSERT INTO bdot10k_buildings VALUES
                 ('a', ST_MakeEnvelope(21.0000,52.0000,21.0005,52.0005)),
                 ('b', ST_MakeEnvelope(21.0006,52.0006,21.0010,52.0010)),
                 ('c', ST_MakeEnvelope(21.0011,52.0011,21.0015,52.0015)),
                 ('far', ST_MakeEnvelope(19.0000,50.0000,19.0005,50.0005)),
                 -- NULL geometry must be skipped, not counted or crashed on.
                 ('nogeom', NULL);

             -- egib goes through the same ST_Centroid path as bdot10k; two
             -- buildings in one cell plus a NULL.
             INSERT INTO egib_buildings VALUES
                 ('e1', ST_MakeEnvelope(22.0000,53.0000,22.0005,53.0005)),
                 ('e2', ST_MakeEnvelope(22.0006,53.0006,22.0010,53.0010)),
                 ('e_nogeom', NULL);

             -- prg uses the raw point, not a centroid.
             INSERT INTO prg_addresses VALUES
                 ('p1', ST_Point(23.0000,54.0000)),
                 ('p2', ST_Point(23.0005,54.0005)),
                 ('p_nogeom', NULL);",
        )
        .unwrap();
```

with:

```rust
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);

             -- Three bdot10k buildings inside ONE z14 cell (cells are ~0.022
             -- deg wide here, so these cannot straddle a boundary), plus one
             -- far away: 2 distinct cells, not 4 rows.
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('a', ST_MakeEnvelope(21.0000,52.0000,21.0005,52.0005)),
                 ('b', ST_MakeEnvelope(21.0006,52.0006,21.0010,52.0010)),
                 ('c', ST_MakeEnvelope(21.0011,52.0011,21.0015,52.0015)),
                 ('far', ST_MakeEnvelope(19.0000,50.0000,19.0005,50.0005)),
                 -- NULL geometry must be skipped, not counted or crashed on.
                 ('nogeom', NULL);

             -- egib goes through the same stored-centroid path as bdot10k;
             -- two buildings in one cell plus a NULL.
             INSERT INTO egib_buildings (id_budynku, geom) VALUES
                 ('e1', ST_MakeEnvelope(22.0000,53.0000,22.0005,53.0005)),
                 ('e2', ST_MakeEnvelope(22.0006,53.0006,22.0010,53.0010)),
                 ('e_nogeom', NULL);

             -- prg uses the raw point, not a centroid.
             INSERT INTO prg_addresses VALUES
                 ('p1', ST_Point(23.0000,54.0000)),
                 ('p2', ST_Point(23.0005,54.0005)),
                 ('p_nogeom', NULL);

             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);
             UPDATE egib_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
```

(the doc comment above this test still says "the egib branch's ST_Centroid path" — leave it; it is still describing what the branch is testing, not the SQL literally.)

- [ ] **Step 4: Update `enqueue_all_returns_only_newly_inserted_rows`'s fixture**

Replace:

```rust
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);
             CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES ('a', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             -- A pre-existing, unrelated dirty-cell row for the same source,
             -- as if an OSM update had enqueued it moments earlier.
             INSERT INTO match_dirty_cells VALUES ('bdot10k', 14, 1, 1, now());",
        )
        .unwrap();
```

with:

```rust
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES ('a', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);
             -- A pre-existing, unrelated dirty-cell row for the same source,
             -- as if an OSM update had enqueued it moments earlier.
             INSERT INTO match_dirty_cells VALUES ('bdot10k', 14, 1, 1, now());",
        )
        .unwrap();
```

- [ ] **Step 5: Run this file's tests**

Run: `cargo test --lib compare::reconcile::`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/compare/reconcile.rs
git commit -m "$(cat <<'EOF'
fix(compare): enqueue_all reads the stored centroid, not ST_Centroid(geom)

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: `update/changeset.rs` — `representative_point_sql` call sites

**Files:**
- Modify: `src/update/changeset.rs`

- [ ] **Step 1: Update the two call sites in `insert_change_areas`**

Replace:

```rust
    let point_live = spec.representative_point_sql("l.geom");
    let point_stg = spec.representative_point_sql("s.geom");
```

(the first occurrence, inside `insert_change_areas`) with:

```rust
    let point_live = spec.representative_point_sql("l");
    let point_stg = spec.representative_point_sql("s");
```

- [ ] **Step 2: Update the two call sites in `insert_dirty_cells`**

Replace the second occurrence of the same two lines (inside `insert_dirty_cells`) identically:

```rust
    let point_live = spec.representative_point_sql("l.geom");
    let point_stg = spec.representative_point_sql("s.geom");
```

with:

```rust
    let point_live = spec.representative_point_sql("l");
    let point_stg = spec.representative_point_sql("s");
```

No test fixtures in this file need to change: `TEST_SPEC` (in the `#[cfg(test)]` module) is `GeomKind::Point`, so `representative_point_sql` still resolves to `"{alias}.geom"` — identical output to before the signature change.

- [ ] **Step 3: Run this file's tests**

Run: `cargo test --lib update::changeset::`
Expected: all pass, unchanged behavior.

- [ ] **Step 4: Commit**

```bash
git add src/update/changeset.rs
git commit -m "$(cat <<'EOF'
refactor(update): adapt to representative_point_sql's alias-based signature

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Remaining fixture sweep — `update/osm.rs` and `compare/mod.rs`

**Files:**
- Modify: `src/update/osm.rs`
- Modify: `src/compare/mod.rs`

- [ ] **Step 1: `update/osm.rs` — `osc_xml_flows_through_parse_apply_drain_into_the_serving_table`**

This test drives a real `compare_bdot10k` + `drain_batch` (which also drains an egib cell, since an OSM building edit enqueues both sources per CLAUDE.md — `egib_buildings` needs the `centroid` column even though this test never inserts an egib row).

Replace:

```rust
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 geom GEOMETRY);
             -- Sits inside way 100's footprint, so OSM currently covers it.
             INSERT INTO bdot10k_buildings VALUES
                 ('gov1', ST_MakeEnvelope(20.0002, 50.0002, 20.0008, 50.0008));",
        )?;
```

with:

```rust
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 geom GEOMETRY);
             -- Sits inside way 100's footprint, so OSM currently covers it.
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('gov1', ST_MakeEnvelope(20.0002, 50.0002, 20.0008, 50.0008));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )?;
```

- [ ] **Step 2: `compare/mod.rs` — `full_vs_incremental_equivalence::conn()`**

`enqueue_all` (called by the bdot10k test in this module) touches all three government tables unconditionally, per the existing comment already on this fixture.

Replace:

```rust
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 geom GEOMETRY);",
        )
        .unwrap();
        c
    }
```

with:

```rust
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 geom GEOMETRY);",
        )
        .unwrap();
        c
    }
```

- [ ] **Step 3: `compare/mod.rs` — `full_compare_and_reconcile_drain_agree_on_bdot10k`**

Replace:

```rust
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES
                 (1, 'way', NULL, ST_MakeEnvelope(20.0, 52.0, 20.001, 52.001));
             INSERT INTO bdot10k_buildings VALUES
                 ('inside', ST_MakeEnvelope(20.0002,52.0002,20.0008,52.0008)),
                 ('lonely', ST_MakeEnvelope(21.0,52.2,21.001,52.201)),
                 -- Outside the old hardcoded (14,49,25,55) compare_buildings
                 -- bbox: the extent-divergence scenario the extent fix
                 -- exists to close, and the scenario this test must be able
                 -- to catch a regression of.
                 ('stray', ST_MakeEnvelope(30.0,60.0,30.001,60.001));",
        )
        .unwrap();
```

with:

```rust
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES
                 (1, 'way', NULL, ST_MakeEnvelope(20.0, 52.0, 20.001, 52.001));
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('inside', ST_MakeEnvelope(20.0002,52.0002,20.0008,52.0008)),
                 ('lonely', ST_MakeEnvelope(21.0,52.2,21.001,52.201)),
                 -- Outside the old hardcoded (14,49,25,55) compare_buildings
                 -- bbox: the extent-divergence scenario the extent fix
                 -- exists to close, and the scenario this test must be able
                 -- to catch a regression of.
                 ('stray', ST_MakeEnvelope(30.0,60.0,30.001,60.001));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
```

(`full_compare_and_reconcile_drain_agree_on_prg` needs no change — it never inserts a bdot10k/egib row, and the shared `conn()` fixture from Step 2 already gives both tables the `centroid` column `enqueue_all` needs.)

- [ ] **Step 4: `compare/mod.rs` — `drain_refresh_concurrency::conn(n)`**

Replace:

```rust
    fn conn(n: i64) -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch(&format!(
            "CREATE TABLE bdot10k_buildings AS {};
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 geom GEOMETRY);",
            hashed_select(&rows_sql(n, "v1"))
        ))
        .unwrap();
```

with:

```rust
    fn conn(n: i64) -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch(&format!(
            "CREATE TABLE bdot10k_buildings AS {};
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 geom GEOMETRY);",
            BDOT10K.with_centroid_select(&hashed_select(&rows_sql(n, "v1")))
        ))
        .unwrap();
```

- [ ] **Step 5: `compare/mod.rs` — the `refresh()` staging closure inside `drain_and_dataset_refresh_do_not_collide`**

Staging and live must have identical schemas for `refresh()`'s `INSERT INTO {live} SELECT * FROM {staging}` to work — this closure builds the staging table by hand, so it needs the same `with_centroid_select` wrap `conn(n)` now applies to the live table.

Replace:

```rust
            let res = refresh(
                &conn,
                &BDOT10K,
                move |c: &Connection, target: &str| {
                    c.execute_batch(&format!(
                        "CREATE TABLE {target} AS {}",
                        hashed_select(&rows)
                    ))?;
                    Ok(crate::dataset::LoadStats::default())
                },
                None,
            );
```

with:

```rust
            let res = refresh(
                &conn,
                &BDOT10K,
                move |c: &Connection, target: &str| {
                    c.execute_batch(&format!(
                        "CREATE TABLE {target} AS {}",
                        BDOT10K.with_centroid_select(&hashed_select(&rows))
                    ))?;
                    Ok(crate::dataset::LoadStats::default())
                },
                None,
            );
```

- [ ] **Step 6: Run both files' tests**

Run: `cargo test --lib update::osm:: compare::mod::`
Expected: all pass, including `drain_and_dataset_refresh_do_not_collide` (this one takes noticeably longer than the others — it runs 12 refresh cycles against a background drain thread — that's expected, not a hang).

- [ ] **Step 7: Commit**

```bash
git add src/update/osm.rs src/compare/mod.rs
git commit -m "$(cat <<'EOF'
test: add centroid column to remaining bdot10k/egib test fixtures

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: Whole-workspace verification

**Files:** none (verification only)

- [ ] **Step 1: Full test suite**

Run: `cargo test`
Expected: all tests pass (this repeats every test touched in Tasks 1–9 plus everything else in the workspace, including the integration tests under `tests/`, which use the checked-in `fixtures/bdot10k.parquet`/`fixtures/egib.parquet` and go through the real `import` path — they should pass unmodified since `load_into` now always produces the `centroid` column).

- [ ] **Step 2: Clippy**

Run: `cargo clippy --all-targets`
Expected: no warnings.

- [ ] **Step 3: Format check**

Run: `cargo fmt -- --check`
Expected: no diff. If it reports one, run `cargo fmt` and re-check.

- [ ] **Step 4: Commit if `cargo fmt` made changes**

```bash
git add -A
git commit -m "$(cat <<'EOF'
chore: cargo fmt pass

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

(Skip this step entirely if Step 3 reported no diff.)

---

### Task 11: Real-data measurement

**Files:**
- Create: a short follow-up doc, e.g. `docs/centroid_index_measured.md` (exact content depends on real numbers gathered in this task — see Step 4).

**Context:** there's an existing Poland-scale DB at `./osmpbudynkiv2.duckdb` (14 GB) and RocksDB at `./osmpbudynkiv2.rocksdb`, from the 2026-07-30 investigation `docs/per_cell_recompute_full_scan.md` and `docs/followups_precomputed_unmatched_serving.md` reference. `example_data/BDOT10k/OT_BUBD_A_2026-08-01.parquet` and `example_data/EGiB/0_budynki_2026-08-01.parquet` are fresh dated snapshots the user downloaded for this measurement. Re-importing bdot10k/egib into the existing file rebuilds just those two tables (`import`'s `load_into` does `DROP TABLE IF EXISTS` + `CREATE TABLE AS`) — OSM, PRG, and the serving tables are untouched.

- [ ] **Step 1: Build the binary in release mode**

Run: `cargo build --release`
Expected: succeeds (this is a large import; debug-mode DuckDB spatial operations would make it needlessly slow).

- [ ] **Step 2: Re-import bdot10k and egib into the existing DB, in the background**

The CLI subcommands are `import bdot10k --file <path>` and `import egib --file <path>` (see `ImportSource::Bdot10k`/`ImportSource::Egib` in `src/cli.rs:87-98`, each taking a `--file` flag that skips downloading). Confirm which config file (if any) the existing `./osmpbudynkiv2.duckdb` was built with — check for a config file in the repo root or wherever the DB was originally created, since `db_path`/`rocksdb_path` must point at the existing files, not fresh ones (`example_config.toml` is the checked-in template, not necessarily the one in use). Pass `--config <path>` if a local one exists.

```bash
./target/release/osmpbudynkiv2 import bdot10k --file example_data/BDOT10k/OT_BUBD_A_2026-08-01.parquet > /tmp/import_bdot10k.log 2>&1
```

Run this as a background command (`run_in_background: true` if using the Bash tool) since it may take several minutes; do not block on it.

Then, once it completes, run the egib import the same way:

```bash
./target/release/osmpbudynkiv2 import egib --file example_data/EGiB/0_budynki_2026-08-01.parquet > /tmp/import_egib.log 2>&1
```

- [ ] **Step 3: Confirm both imports succeeded and note wall time**

Check `/tmp/import_bdot10k.log` and `/tmp/import_egib.log` for "import complete" (see the `info!(... "BDOT10k import complete")` / `"EGIB import complete"` log lines in `src/import/bdot10k.rs` / `src/import/egib.rs`) and no errors. Record the elapsed times reported in the logs (`format_duration(total.elapsed())`).

- [ ] **Step 4: Measure the old vs. new predicate plan and timing on the real data**

With the server **stopped** (DuckDB is single-writer; per CLAUDE.md's architecture note), connect directly via the `duckdb` CLI (or a small throwaway Rust/SQL script) against `./osmpbudynkiv2.duckdb`:

```sql
LOAD spatial;
SET explain_output='physical_only';

-- Old form (function wraps the indexed column -- should NOT use the index):
EXPLAIN SELECT LOKALNYID FROM bdot10k_buildings
WHERE ST_Intersects(ST_Centroid(geom),
                    ST_MakeEnvelope(21.0058,52.2278,21.0278,52.2413));

-- New form (reads the stored, indexed column -- should use RTREE_INDEX_SCAN):
EXPLAIN SELECT LOKALNYID FROM bdot10k_buildings
WHERE ST_Intersects(centroid,
                    ST_MakeEnvelope(21.0058,52.2278,21.0278,52.2413));
```

Confirm the plan shape changes from `SEQ_SCAN` to `RTREE_INDEX_SCAN`, matching the reproduction in `docs/per_cell_recompute_full_scan.md`. Then time both forms for real (`.timer on` in the `duckdb` CLI, or wrap in `\timing` equivalent) on a handful of real cells — reuse the same Warsaw cell coordinates the original doc measured (`21.0058,52.2278,21.0278,52.2413`) plus 2–3 others of your choosing (a dense city cell and a rural cell, for contrast). Record before/after timings for both bdot10k and egib.

- [ ] **Step 5: Measure an end-to-end number**

Either:
- Time a full `./target/release/osmpbudynkiv2 compare buildings` run (compare to the doc's baseline: bdot10k 6m41s, egib 7m53s), or
- Seed `match_dirty_cells` with a batch of real cells and time `compare reconcile`'s drain via the running server's `/status` endpoint, similar to the original doc's drain-tick measurement.

Pick whichever is more convenient given time constraints; a full `compare buildings` re-run is simpler to reason about and directly comparable to the doc's existing baseline numbers.

- [ ] **Step 6: Write the results into a follow-up doc**

Create `docs/centroid_index_measured.md` in the same style as `docs/per_cell_recompute_full_scan.md`: a "Written 2026-08-01, measured on..." header, the EXPLAIN plan evidence (Step 4), the timing table (Step 4/5), and a one-line conclusion comparing the measured speedup to the doc's projected ~6.3× / ~0.9s→~0.15s figures. Note explicitly which numbers are freshly measured this session vs. carried over from the earlier doc.

- [ ] **Step 7: Commit the measurement doc**

```bash
git add docs/centroid_index_measured.md
git commit -m "$(cat <<'EOF'
docs: measure the centroid-index fix against real Poland-scale data

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 12: CLAUDE.md update

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add a new gotcha next to the existing match-rule and invalid-geometry ones**

In `CLAUDE.md`, immediately after the paragraph starting `**Gotcha — the match rule has one home.**`, insert a new paragraph:

```markdown

**Gotcha — bdot10k/egib's representative point is a stored column, not computed.** `bdot10k_buildings` and `egib_buildings` carry a `centroid GEOMETRY` column, populated by `import::bdot10k::load_into` / `import::egib::load_into` (shared by `import` and `update`'s staging load) and RTREE-indexed the same way `geom` is. `rule::unmatched_buildings_sql`, `compare::buildings`, `compare::incremental`, `compare::reconcile::enqueue_all`, and `update::changeset` (via `DatasetSpec::representative_point_sql`) all read this column instead of computing `ST_Centroid(geom)` inline — an RTREE index cannot be used through a function wrapped around the indexed column, which was the root cause of the full-table-scan bottleneck in `docs/per_cell_recompute_full_scan.md`. The column is added *outside* `hashed_select`'s projection (`DatasetSpec::with_centroid_select`), so it never affects `_row_hash` and needs no `ROW_HASH_VERSION` bump. Scope is bdot10k/egib only — PRG's `geom` already is its representative point, and `bdot10k_unmatched`/`egib_unmatched` (the serving tables) and `osm_buildings` are untouched, so `server/package.rs` and `update/dirty_cells.rs` still compute `ST_Centroid` inline on those. **No migration path exists for databases built before this change** — `import bdot10k` / `import egib` must be re-run (which rebuilds the table wholesale) to gain the column; there is no `ALTER TABLE`/auto-backfill.
```

- [ ] **Step 2: Verify the insertion renders correctly**

Run: `grep -n "Gotcha — bdot10k/egib's representative point" CLAUDE.md`
Expected: one match, in the right place (between the match-rule and invalid-geometry gotchas).

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "$(cat <<'EOF'
docs: document the persisted/indexed centroid column in CLAUDE.md

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review Notes

- **Spec coverage:** Schema/population (Tasks 1–3), all six call sites from the design's table (Tasks 4–8, with `update/changeset.rs` as a signature-only change since PRG is unaffected), out-of-scope confirmations (`server/package.rs`, `update/dirty_cells.rs` — untouched, per design), testing plan (unit/integration tests woven into Tasks 1–9, real-data measurement as Task 11), CLAUDE.md update (Task 12). Every design section has a task.
- **Fixture completeness:** cross-checked every `CREATE TABLE bdot10k_buildings` / `CREATE TABLE egib_buildings` site in `src/` via `grep -rn` against this plan's task list — all 14 sites are covered (Tasks 4, 5 ×5, 6, 7 ×3, 9 ×3) except `import/egib.rs`'s `load_into_drops_a_deliberately_invalid_row` (doesn't call `load_into`, tests `filter_invalid_geometry` directly, no centroid involved) and `server/mod.rs`'s `check_startup_conditions` test (only ever runs `SELECT COUNT(*)`, never references `centroid`) — both deliberately left unchanged, reasons noted inline in Task descriptions / this section.
- **Type/name consistency:** `with_centroid_select` and the new `representative_point_sql(&self, alias: &str)` signature (Task 1) are used identically in every later task (`BDOT10K.with_centroid_select(...)` in Tasks 2, 3, 9; `spec.representative_point_sql("l")`/`"s"` in Task 8) — no drift between the producing and consuming tasks.
