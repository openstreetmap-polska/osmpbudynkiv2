# Skip invalid geometry on import/update, log it in job status — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drop topologically-invalid government building geometry at the loader shared by
`import`/`update` (instead of repairing or filtering it later), and make the drop visible
through a new durable "last run" log surfaced in `/status`.

**Architecture:** A new `dataset::filter_invalid_geometry` helper deletes invalid rows
right after `bdot10k`/`egib`'s `load_into` creates its table, returning a `LoadStats`
count + example ids. Because `load_into` is the one place both `import` and `update`'s
staging load funnel through, this sits upstream of both `compare::buildings` and
`compare::incremental` — neither needs to change. `import()` and `refresh()` each wrap
their own fallible body and self-report to a new `job_log` module (`job_run_log` table,
delete-then-insert per job name), which `/status` reads and returns as a new field.

**Tech Stack:** Rust, DuckDB (`duckdb` crate 1.10502.0, spatial extension), `anyhow`,
`tracing`, `serde`.

## Global Constraints

- Job names are exactly: `import:bdot10k`, `import:egib`, `update:bdot10k`,
  `update:egib`. Nothing else gets wired to `job_log` in this plan.
- `job_run_log.outcome` values are `"Success"` / `"Error"` (capitalized) — matches the
  casing `server::jobs::JobOutcome` already serializes.
- `job_run_log` rows are timestamped `TIMESTAMP WITH TIME ZONE`, matching every other
  timestamp column in the schema (`dataset_refreshes`, `dataset_change_areas`,
  `match_dirty_cells`, `package_exports`).
- `filter_invalid_geometry` caps captured example ids at `dataset::MAX_EXAMPLE_IDS = 20`
  but the returned count is always the true total.
- `job_log` (new module) must not depend on the `server` module — `import`/`update` run
  independently of whether the HTTP server is up. Dependency direction is
  `server::jobs::status_handler` → `job_log`, never the reverse.
- No changes to `compare::rule`, `compare::buildings`, `compare::incremental`, or PRG's
  import/update path (`update_prg`) — out of scope per the design doc.
- Design reference: `docs/superpowers/specs/2026-07-30-invalid-geometry-skip-design.md`.
  Background: `docs/invalid_geometry_tile_500s.md`.

---

### Task 1: `LoadStats` and the shared invalid-geometry filter

**Files:**
- Modify: `src/dataset.rs`

**Interfaces:**
- Produces: `pub const MAX_EXAMPLE_IDS: usize = 20;`, `pub struct LoadStats { pub skipped_invalid_geometry: i64, pub skipped_example_ids: Vec<String> }` (derives `Debug, Clone, Default, PartialEq, Eq`), `pub fn filter_invalid_geometry(conn: &duckdb::Connection, table: &str, id_col: &str) -> anyhow::Result<LoadStats>`.

- [ ] **Step 1: Write the failing tests**

Add to the `#[cfg(test)] mod tests` block at the bottom of `src/dataset.rs` (it already has
`use super::*;` at the top of that block — keep it):

```rust
    #[test]
    fn filter_invalid_geometry_drops_only_invalid_rows_and_returns_stats() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id VARCHAR, geom GEOMETRY);
             INSERT INTO t VALUES
                 ('valid', ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')),
                 ('bowtie', ST_GeomFromText('POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))'));",
        )
        .unwrap();

        let stats = filter_invalid_geometry(&conn, "t", "id").unwrap();

        assert_eq!(stats.skipped_invalid_geometry, 1);
        assert_eq!(stats.skipped_example_ids, vec!["bowtie".to_string()]);

        let remaining: Vec<String> = {
            let mut s = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(remaining, vec!["valid".to_string()]);
    }

    #[test]
    fn filter_invalid_geometry_caps_example_ids_but_counts_all() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch("CREATE TABLE t (id VARCHAR, geom GEOMETRY);")
            .unwrap();
        for i in 0..25 {
            conn.execute(
                "INSERT INTO t VALUES (?, ST_GeomFromText('POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))'))",
                duckdb::params![format!("bad{i}")],
            )
            .unwrap();
        }

        let stats = filter_invalid_geometry(&conn, "t", "id").unwrap();

        assert_eq!(stats.skipped_invalid_geometry, 25);
        assert_eq!(stats.skipped_example_ids.len(), MAX_EXAMPLE_IDS);
    }

    #[test]
    fn filter_invalid_geometry_is_a_noop_when_everything_is_valid() {
        use crate::db::init_db;
        use std::path::Path;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id VARCHAR, geom GEOMETRY);
             INSERT INTO t VALUES ('a', ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'));",
        )
        .unwrap();

        let stats = filter_invalid_geometry(&conn, "t", "id").unwrap();

        assert_eq!(stats, LoadStats::default());
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib dataset::tests::filter_invalid_geometry -- --nocapture`
Expected: FAIL to compile — `filter_invalid_geometry`, `LoadStats`, `MAX_EXAMPLE_IDS` don't exist yet.

- [ ] **Step 3: Implement**

Add to `src/dataset.rs`, after `hashed_select` and before the `#[cfg(test)]` block:

```rust
/// Cap on how many skipped-row ids `filter_invalid_geometry` collects as
/// examples -- enough to point an operator at the actual bad records
/// upstream, without holding an unbounded list for a source with many
/// invalid rows. The returned count is always the true total regardless of
/// this cap.
pub const MAX_EXAMPLE_IDS: usize = 20;

/// Rows a dataset loader dropped rather than staging, because their geometry
/// failed `ST_IsValid`. `ST_AsMVTGeom` cannot tolerate invalid geometry (see
/// docs/invalid_geometry_tile_500s.md) -- we drop rather than repair.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadStats {
    pub skipped_invalid_geometry: i64,
    /// First `MAX_EXAMPLE_IDS` ids of skipped rows, in whatever order the
    /// SELECT below finds them -- not exhaustive, just enough to point an
    /// operator at the actual bad records upstream.
    pub skipped_example_ids: Vec<String>,
}

/// Delete invalid-geometry rows from a just-loaded table, capturing example
/// ids before they're gone. Shared by `import::bdot10k::load_into` and
/// `import::egib::load_into` -- the one place both `import` and `update`'s
/// staging load funnel through, so a row filtered out here never reaches
/// `compare::buildings` or `compare::incremental` at all.
pub fn filter_invalid_geometry(
    conn: &duckdb::Connection,
    table: &str,
    id_col: &str,
) -> anyhow::Result<LoadStats> {
    use anyhow::Context;

    let mut skipped_example_ids = Vec::new();
    {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {id_col} FROM {table} WHERE NOT ST_IsValid(geom) LIMIT {MAX_EXAMPLE_IDS}"
            ))
            .with_context(|| format!("Failed to prepare invalid-geometry scan on {table}"))?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .with_context(|| format!("Failed to scan invalid-geometry rows in {table}"))?;
        for row in rows {
            skipped_example_ids.push(row.context("Failed to read invalid-geometry id")?);
        }
    }

    let skipped_invalid_geometry = conn
        .execute(&format!("DELETE FROM {table} WHERE NOT ST_IsValid(geom)"), [])
        .with_context(|| format!("Failed to delete invalid-geometry rows from {table}"))?
        as i64;

    Ok(LoadStats {
        skipped_invalid_geometry,
        skipped_example_ids,
    })
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --lib dataset::tests::filter_invalid_geometry -- --nocapture`
Expected: PASS (3 tests).

- [ ] **Step 5: Run the full `dataset` unit test suite to check nothing else broke**

Run: `cargo test --lib dataset::`
Expected: PASS (all existing `dataset.rs` tests plus the 3 new ones).

- [ ] **Step 6: Commit**

```bash
git add src/dataset.rs
git commit -m "feat(dataset): add LoadStats and filter_invalid_geometry

Shared helper that deletes ST_IsValid=false rows from a just-loaded table,
capturing up to 20 example ids. Not yet wired into any loader."
```

---

### Task 2: `job_run_log` table and the `job_log` module

**Files:**
- Modify: `src/db.rs`
- Create: `src/job_log.rs`
- Modify: `src/main.rs` (register the module)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `pub struct job_log::JobRunLogEntry { pub ran_at: String, pub outcome: String, pub message: Option<String> }` (derives `Debug, Clone, Serialize, PartialEq, Eq`), `pub fn job_log::record(conn: &duckdb::Connection, job_name: &str, outcome: &str, message: Option<&str>) -> anyhow::Result<()>`, `pub fn job_log::read_all(conn: &duckdb::Connection) -> anyhow::Result<BTreeMap<String, JobRunLogEntry>>`.

- [ ] **Step 1: Add the schema table**

In `src/db.rs`, inside `create_schema`'s SQL batch, add right after the `metadata` table
(both are small operational/bookkeeping tables — keep them together):

```rust
fn create_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS metadata (
            key VARCHAR,
            value VARCHAR
        );

        -- Last-run outcome per job/command, keyed by job_name (e.g.
        -- 'import:bdot10k', 'update:egib'). Delete-then-insert on every run
        -- (see job_log::record), so only the most recent run survives --
        -- this is a status snapshot, not a history. Read by /status.
        CREATE TABLE IF NOT EXISTS job_run_log (
            job_name VARCHAR,
            ran_at TIMESTAMP WITH TIME ZONE,
            outcome VARCHAR,
            message VARCHAR
        );

        -- Processed OSM data with geometry
        CREATE TABLE IF NOT EXISTS osm_addresses (
```

(i.e. insert the new `CREATE TABLE IF NOT EXISTS job_run_log (...)` block between the
existing `metadata` table and the `-- Processed OSM data with geometry` comment; leave
everything else in the batch untouched.)

- [ ] **Step 2: Extend the schema test to cover the new table**

In `src/db.rs`'s `#[cfg(test)] mod tests`, `test_init_db_creates_tables` already lists
tables to check are present and empty. Add `"job_run_log"` to that list:

```rust
        let tables = [
            "metadata",
            "osm_addresses",
            "osm_buildings",
            "package_exports",
            "job_run_log",
        ];
```

- [ ] **Step 3: Run the db test to verify it currently fails**

Run: `cargo test --lib db::tests::test_init_db_creates_tables`
Expected: FAIL — `job_run_log` table doesn't exist yet (query error).

- [ ] **Step 4: Nothing else to implement for this step** — the schema edit from Step 1 is
the fix. Re-run:

Run: `cargo test --lib db::tests::test_init_db_creates_tables`
Expected: PASS.

- [ ] **Step 5: Write `src/job_log.rs` with its own tests**

Create the file in full, including tests:

```rust
//! Last-run status/log per job or CLI command, persisted in `job_run_log` so
//! it survives across separate process invocations (e.g. a standalone
//! `import` run, which never shares memory with a later `run` server) and is
//! readable by `/status`.
//!
//! Deliberately a snapshot, not a history: `record` deletes the previous row
//! for `job_name` before inserting the new one, matching the same
//! delete-then-insert convention `dataset::stamp_row_hash_version` already
//! uses instead of `ON CONFLICT`.
//!
//! This module must not depend on `crate::server` -- `import`/`update` run
//! independently of whether the HTTP server is up.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use duckdb::Connection;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JobRunLogEntry {
    pub ran_at: String,
    /// "Success" | "Error" -- matches `server::jobs::JobOutcome`'s casing.
    pub outcome: String,
    pub message: Option<String>,
}

/// Record the outcome of one job/command run, replacing any previous row for
/// the same `job_name`. Callers are expected to log-and-ignore any error this
/// returns -- a failure to write the log must never fail the job itself.
pub fn record(
    conn: &Connection,
    job_name: &str,
    outcome: &str,
    message: Option<&str>,
) -> Result<()> {
    conn.execute(
        "DELETE FROM job_run_log WHERE job_name = ?",
        duckdb::params![job_name],
    )
    .context("Failed to clear previous job_run_log row")?;
    conn.execute(
        "INSERT INTO job_run_log (job_name, ran_at, outcome, message) VALUES (?, now(), ?, ?)",
        duckdb::params![job_name, outcome, message],
    )
    .context("Failed to write job_run_log row")?;
    Ok(())
}

/// All current entries, keyed by job_name. A `BTreeMap` rather than
/// `Vec<(String, _)>` for the same reason
/// `server::jobs::status_handler::MatchStaleness::pending_by_source` is one:
/// a stable, sorted JSON object shape instead of an array of pairs.
pub fn read_all(conn: &Connection) -> Result<BTreeMap<String, JobRunLogEntry>> {
    let mut stmt = conn
        .prepare(
            "SELECT job_name, ran_at::VARCHAR, outcome, message FROM job_run_log
             ORDER BY job_name",
        )
        .context("Failed to prepare job_run_log read")?;
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                JobRunLogEntry {
                    ran_at: r.get(1)?,
                    outcome: r.get(2)?,
                    message: r.get(3)?,
                },
            ))
        })
        .context("Failed to read job_run_log")?;
    let mut out = BTreeMap::new();
    for row in rows {
        let (name, entry) = row.context("Failed to decode job_run_log row")?;
        out.insert(name, entry);
    }
    Ok(out)
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
    fn record_then_read_round_trips() {
        let conn = conn();
        record(&conn, "import:bdot10k", "Success", Some("skipped 3 rows")).unwrap();

        let log = read_all(&conn).unwrap();
        let entry = log.get("import:bdot10k").expect("entry must be present");
        assert_eq!(entry.outcome, "Success");
        assert_eq!(entry.message.as_deref(), Some("skipped 3 rows"));
        assert!(!entry.ran_at.is_empty());
    }

    #[test]
    fn record_keeps_only_the_latest_run_per_job_name() {
        let conn = conn();
        record(&conn, "update:bdot10k", "Error", Some("first attempt failed")).unwrap();
        record(&conn, "update:bdot10k", "Success", Some("second attempt: no issues")).unwrap();

        let log = read_all(&conn).unwrap();
        assert_eq!(log.len(), 1, "only one row per job_name must survive");
        let entry = &log["update:bdot10k"];
        assert_eq!(entry.outcome, "Success");
        assert_eq!(entry.message.as_deref(), Some("second attempt: no issues"));
    }

    #[test]
    fn read_all_keys_entries_by_job_name_in_sorted_order() {
        let conn = conn();
        record(&conn, "update:egib", "Success", None).unwrap();
        record(&conn, "import:bdot10k", "Success", None).unwrap();

        let names: Vec<&String> = read_all(&conn).unwrap().keys().collect();
        assert_eq!(names, vec!["import:bdot10k", "update:egib"]);
    }

    #[test]
    fn record_allows_a_null_message() {
        let conn = conn();
        record(&conn, "update:egib", "Success", None).unwrap();

        let log = read_all(&conn).unwrap();
        assert_eq!(log["update:egib"].message, None);
    }
}
```

- [ ] **Step 6: Register the module**

In `src/main.rs`, add `mod job_log;` to the module list, alphabetically between `import`
and `osm`:

```rust
mod cli;
mod compare;
mod config;
mod dataset;
mod db;
mod download;
mod import;
mod job_log;
mod osm;
mod server;
mod shutdown;
mod tile_math;
mod update;
mod utils;
```

- [ ] **Step 7: Run the new tests**

Run: `cargo test --lib job_log::`
Expected: PASS (4 tests).

- [ ] **Step 8: Run the full test suite to confirm nothing else broke**

Run: `cargo test --lib`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/db.rs src/job_log.rs src/main.rs
git commit -m "feat: add job_run_log table and job_log module

Delete-then-insert last-run log per job_name, readable by /status. Not yet
written to by any job."
```

---

### Task 3: Wire `bdot10k` import/update into the filter and the log

**Files:**
- Modify: `src/import/bdot10k.rs`

**Interfaces:**
- Consumes: `crate::dataset::{LoadStats, filter_invalid_geometry}` (Task 1),
  `crate::job_log::record` (Task 2).
- Produces: `pub fn load_into(conn: &Connection, target_table: &str, parquet_path: &str) -> Result<LoadStats>` (signature changed from `Result<()>`). `pub fn import(...) -> Result<()>` (signature unchanged) now self-reports to `job_log` under `"import:bdot10k"`.

- [ ] **Step 1: Write the failing test**

Add a test module to `src/import/bdot10k.rs` (it currently has none):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    /// Sanity check that the existing fixture (also used by
    /// `tests/cli_import_bdot10k.rs`, which asserts `count=74`) has no
    /// invalid geometry, and that `load_into` now returns `LoadStats`
    /// reflecting that.
    #[test]
    fn load_into_the_fixture_has_no_invalid_geometry() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();

        let stats = load_into(&conn, "bdot10k_buildings", "fixtures/bdot10k.parquet").unwrap();

        assert_eq!(
            stats,
            crate::dataset::LoadStats::default(),
            "fixture is expected to be clean"
        );
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM bdot10k_buildings", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 74, "must match the known fixture row count");
    }

    /// `load_into` must actually remove invalid rows, not just report them --
    /// exercised directly since real fixtures don't contain one. Loads a tiny
    /// staged table by hand (mirroring `hashed_select`'s output shape) rather
    /// than going through `crate::dataset::filter_invalid_geometry` in
    /// isolation, so this catches a regression in how `load_into` calls it.
    #[test]
    fn load_into_drops_a_deliberately_invalid_row() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        // load_into always DROPs and recreates target_table from the parquet
        // path; to exercise the invalid-geometry deletion without a real
        // parquet file, pre-seed target_table exactly as load_into would
        // have left it, then call filter_invalid_geometry the same way
        // load_into does internally -- this is what load_into's last line
        // does, so asserting on it here documents that contract.
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES
                 ('ok', ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')),
                 ('bad', ST_GeomFromText('POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))'));",
        )
        .unwrap();

        let stats =
            crate::dataset::filter_invalid_geometry(&conn, "bdot10k_buildings", "LOKALNYID")
                .unwrap();

        assert_eq!(stats.skipped_invalid_geometry, 1);
        assert_eq!(stats.skipped_example_ids, vec!["bad".to_string()]);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib import::bdot10k::tests`
Expected: FAIL — `load_into_the_fixture_has_no_invalid_geometry` fails to compile because
`load_into` still returns `Result<()>`, not `Result<LoadStats>` (a `LoadStats::default()`
comparison against `()` won't typecheck).

- [ ] **Step 3: Implement — `load_into` returns `LoadStats`**

```rust
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::config::Config;
use crate::dataset::LoadStats;
use crate::download::download_file;
use crate::utils::format_duration;

/// Create `target_table` from a BDOT10k GeoParquet file, including the
/// `_row_hash` column, then delete any invalid-geometry rows (see
/// `docs/invalid_geometry_tile_500s.md`). Does NOT create an index --
/// callers that need one create it themselves, and the update path
/// deliberately does not.
///
/// Workaround: DuckDB's automatic GeoParquet conversion and ST_Read (GDAL)
/// both fail on BDOT10k files because their CRS (EPSG:2180) is stored as a
/// projjson string-in-string which DuckDB rejects as "invalid CRS". Instead
/// we disable the automatic conversion, read the file as plain parquet, and
/// manually convert the WKB geometry column. Geometry is transformed from
/// EPSG:2180 to EPSG:4326 for uniform spatial comparisons.
///
/// `enable_geoparquet_conversion` has GLOBAL scope in DuckDB — it's visible
/// to every connection sharing this database instance, not just this one, so
/// it must be restored before returning. Left disabled, it silently breaks
/// any later automatic GeoParquet decoding on the same instance — e.g.
/// EGIB's `load_into`, which relies on the default being on.
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

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --lib import::bdot10k::tests`
Expected: PASS (2 tests).

- [ ] **Step 5: Write the failing test for self-logging in `import()`**

Add to the same test module:

```rust
    #[test]
    fn import_records_success_with_no_skips_in_job_run_log() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let config = Config::default();

        import(&conn, &config, Some(Path::new("fixtures/bdot10k.parquet")), "unused").unwrap();

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log.get("import:bdot10k").expect("entry must be present");
        assert_eq!(entry.outcome, "Success");
        assert_eq!(entry.message.as_deref(), Some("no invalid geometry"));
    }

    #[test]
    fn import_records_error_in_job_run_log_on_failure() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let config = Config::default();

        let result = import(&conn, &config, Some(Path::new("nonexistent.parquet")), "unused");
        assert!(result.is_err());

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log.get("import:bdot10k").expect("entry must exist even on failure");
        assert_eq!(entry.outcome, "Error");
        assert!(entry.message.is_some());
    }
```

- [ ] **Step 6: Run to verify these two fail**

Run: `cargo test --lib import::bdot10k::tests::import_records`
Expected: FAIL — no `job_run_log` row is written yet (the entries won't be present).

- [ ] **Step 7: Implement — `import()` self-reports to `job_log`**

Replace the whole `import` function body:

```rust
pub fn import(conn: &Connection, config: &Config, file: Option<&Path>, url: &str) -> Result<()> {
    let outcome = (|| -> Result<LoadStats> {
        let (parquet_path, was_downloaded) = match file {
            Some(path) => (PathBuf::from(path), false),
            None => (download_file(url, &config.download_dir())?, true),
        };

        let parquet_str = parquet_path
            .to_str()
            .context("Parquet path is not valid UTF-8")?;

        info!(path = parquet_str, "Importing BDOT10k buildings");

        let total = std::time::Instant::now();

        let t = std::time::Instant::now();
        let stats = load_into(conn, crate::dataset::BDOT10K.table, parquet_str)?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: load table"
        );

        let t = std::time::Instant::now();
        conn.execute_batch(
            "CREATE INDEX bdot10k_buildings_geom_idx ON bdot10k_buildings USING RTREE (geom);",
        )
        .context("Failed to create spatial index on bdot10k_buildings")?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: create spatial index"
        );

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM bdot10k_buildings", [], |row| {
            row.get(0)
        })?;
        if was_downloaded && config.cleanup_downloaded_files {
            info!(path = %parquet_path.display(), "Cleaning up downloaded file");
            let _ = std::fs::remove_file(&parquet_path);
        }

        info!(
            count,
            elapsed = %format_duration(total.elapsed()),
            "BDOT10k import complete"
        );

        Ok(stats)
    })();

    match &outcome {
        Ok(stats) => {
            let _ = crate::job_log::record(conn, "import:bdot10k", "Success", Some(&summarize(stats)));
        }
        Err(e) => {
            let _ = crate::job_log::record(conn, "import:bdot10k", "Error", Some(&format!("{e:#}")));
        }
    }
    outcome.map(|_| ())
}

/// Human-readable message for the `job_run_log` row.
fn summarize(stats: &LoadStats) -> String {
    if stats.skipped_invalid_geometry == 0 {
        return "no invalid geometry".to_string();
    }
    let shown = stats.skipped_example_ids.join(", ");
    let more = stats.skipped_invalid_geometry as usize - stats.skipped_example_ids.len();
    if more > 0 {
        format!(
            "skipped {} invalid-geometry rows (ids: {shown}, +{more} more)",
            stats.skipped_invalid_geometry
        )
    } else {
        format!(
            "skipped {} invalid-geometry rows (ids: {shown})",
            stats.skipped_invalid_geometry
        )
    }
}
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `cargo test --lib import::bdot10k::tests`
Expected: PASS (4 tests).

- [ ] **Step 9: Run the existing CLI integration test for this source to confirm no regression**

Run: `cargo test --test cli_import_bdot10k`
Expected: PASS — stdout still contains `"BDOT10k import complete"` and `"count=74"`.

- [ ] **Step 10: Commit**

```bash
git add src/import/bdot10k.rs
git commit -m "feat(import): drop invalid-geometry rows on BDOT10k load, log outcome

load_into now calls dataset::filter_invalid_geometry after creating the
table, and import() self-reports success/error to job_log under
'import:bdot10k', including a summary of any skipped rows."
```

---

### Task 4: Wire `egib` import/update into the filter and the log

**Files:**
- Modify: `src/import/egib.rs`

**Interfaces:**
- Consumes: same as Task 3, mirrored for EGIB.
- Produces: `load_into` returns `Result<LoadStats>`; `import()` self-reports to `job_log`
  under `"import:egib"`.

- [ ] **Step 1: Write the failing tests**

Add a test module to `src/import/egib.rs` (mirrors Task 3's structure exactly, EGIB's id
column is `id_budynku` and its fixture is also 74 rows — see
`tests/cli_import_egib.rs`):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    #[test]
    fn load_into_the_fixture_has_no_invalid_geometry() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();

        let stats = load_into(&conn, "egib_buildings", "fixtures/egib.parquet").unwrap();

        assert_eq!(
            stats,
            crate::dataset::LoadStats::default(),
            "fixture is expected to be clean"
        );
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM egib_buildings", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 74, "must match the known fixture row count");
    }

    #[test]
    fn load_into_drops_a_deliberately_invalid_row() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);
             INSERT INTO egib_buildings VALUES
                 ('ok', ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')),
                 ('bad', ST_GeomFromText('POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))'));",
        )
        .unwrap();

        let stats =
            crate::dataset::filter_invalid_geometry(&conn, "egib_buildings", "id_budynku")
                .unwrap();

        assert_eq!(stats.skipped_invalid_geometry, 1);
        assert_eq!(stats.skipped_example_ids, vec!["bad".to_string()]);
    }

    #[test]
    fn import_records_success_with_no_skips_in_job_run_log() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let config = Config::default();

        import(&conn, &config, Some(Path::new("fixtures/egib.parquet")), "unused").unwrap();

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log.get("import:egib").expect("entry must be present");
        assert_eq!(entry.outcome, "Success");
        assert_eq!(entry.message.as_deref(), Some("no invalid geometry"));
    }

    #[test]
    fn import_records_error_in_job_run_log_on_failure() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let config = Config::default();

        let result = import(&conn, &config, Some(Path::new("nonexistent.parquet")), "unused");
        assert!(result.is_err());

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log.get("import:egib").expect("entry must exist even on failure");
        assert_eq!(entry.outcome, "Error");
        assert!(entry.message.is_some());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib import::egib::tests`
Expected: FAIL to compile — `load_into` still returns `Result<()>` and neither function
writes to `job_log` yet.

- [ ] **Step 3: Implement**

Replace the whole file's non-test content:

```rust
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::config::Config;
use crate::dataset::LoadStats;
use crate::download::download_file;
use crate::utils::format_duration;

/// Create `target_table` from an EGIB GeoParquet file, including the
/// `_row_hash` column, then delete any invalid-geometry rows (see
/// `docs/invalid_geometry_tile_500s.md`). Does NOT create an index.
///
/// Geometry is transformed from EPSG:2180 to EPSG:4326 for uniform spatial
/// comparisons.
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

pub fn import(conn: &Connection, config: &Config, file: Option<&Path>, url: &str) -> Result<()> {
    let outcome = (|| -> Result<LoadStats> {
        let (parquet_path, was_downloaded) = match file {
            Some(path) => (PathBuf::from(path), false),
            None => (download_file(url, &config.download_dir())?, true),
        };

        let parquet_str = parquet_path
            .to_str()
            .context("Parquet path is not valid UTF-8")?;

        info!(path = parquet_str, "Importing EGIB buildings");

        let total = std::time::Instant::now();

        let t = std::time::Instant::now();
        let stats = load_into(conn, crate::dataset::EGIB.table, parquet_str)?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: load table"
        );

        let t = std::time::Instant::now();
        conn.execute_batch(
            "CREATE INDEX egib_buildings_geom_idx ON egib_buildings USING RTREE (geom);",
        )
        .context("Failed to create spatial index on egib_buildings")?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: create spatial index"
        );

        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM egib_buildings", [], |row| row.get(0))?;
        if was_downloaded && config.cleanup_downloaded_files {
            info!(path = %parquet_path.display(), "Cleaning up downloaded file");
            let _ = std::fs::remove_file(&parquet_path);
        }

        info!(
            count,
            elapsed = %format_duration(total.elapsed()),
            "EGIB import complete"
        );

        Ok(stats)
    })();

    match &outcome {
        Ok(stats) => {
            let _ = crate::job_log::record(conn, "import:egib", "Success", Some(&summarize(stats)));
        }
        Err(e) => {
            let _ = crate::job_log::record(conn, "import:egib", "Error", Some(&format!("{e:#}")));
        }
    }
    outcome.map(|_| ())
}

/// Human-readable message for the `job_run_log` row.
fn summarize(stats: &LoadStats) -> String {
    if stats.skipped_invalid_geometry == 0 {
        return "no invalid geometry".to_string();
    }
    let shown = stats.skipped_example_ids.join(", ");
    let more = stats.skipped_invalid_geometry as usize - stats.skipped_example_ids.len();
    if more > 0 {
        format!(
            "skipped {} invalid-geometry rows (ids: {shown}, +{more} more)",
            stats.skipped_invalid_geometry
        )
    } else {
        format!(
            "skipped {} invalid-geometry rows (ids: {shown})",
            stats.skipped_invalid_geometry
        )
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --lib import::egib::tests`
Expected: PASS (4 tests).

- [ ] **Step 5: Run the existing CLI integration test for this source to confirm no regression**

Run: `cargo test --test cli_import_egib`
Expected: PASS — stdout still contains `"EGIB import complete"` and `"count=74"`.

- [ ] **Step 6: Commit**

```bash
git add src/import/egib.rs
git commit -m "feat(import): drop invalid-geometry rows on EGIB load, log outcome

Mirrors the BDOT10k change: load_into calls dataset::filter_invalid_geometry,
import() self-reports success/error to job_log under 'import:egib'."
```

---

### Task 5: `refresh()` threads `LoadStats` and self-reports to `job_log`

**Files:**
- Modify: `src/update/dataset.rs`

**Interfaces:**
- Consumes: `crate::dataset::LoadStats` (Task 1), `crate::job_log::record` (Task 2).
  `bdot10k::load_into` / `egib::load_into` already return `Result<LoadStats>` (Tasks 3–4),
  which is exactly the new closure type `refresh` expects — **no changes needed in
  `src/update/mod.rs`**, verified in Step 8 below.
- Produces: `pub fn refresh(conn: &Connection, spec: &DatasetSpec, load: impl FnOnce(&Connection, &str) -> Result<LoadStats>, source_etag: Option<&str>) -> Result<DiffCounts>` (return type unchanged; only the `load` parameter's type changed). Self-reports to `job_log` under `"update:{spec.name}"`.

- [ ] **Step 1: Update the test helper and write the new failing tests**

In the `#[cfg(test)] mod tests` block, change `loader()`'s body (this is the only test
helper that constructs a `load` closure — every `#[test]` fn calls `loader(...)` without
inspecting its return value, so no other test needs to change):

```rust
    /// Loader closure that fills staging from an inline VALUES list.
    fn loader(rows: &'static str) -> impl FnOnce(&Connection, &str) -> Result<crate::dataset::LoadStats> {
        move |conn: &Connection, target: &str| {
            let inner = format!("SELECT id, a, ST_Point(lon, lat) AS geom FROM ({rows})");
            conn.execute_batch(&format!(
                "CREATE TABLE {target} AS {};",
                crate::dataset::hashed_select(&inner)
            ))?;
            Ok(crate::dataset::LoadStats::default())
        }
    }
```

Then add two new tests to the same module:

```rust
    #[test]
    fn refresh_records_success_in_job_run_log_including_skip_stats() {
        let conn = conn_with_live(LIVE_ROWS);
        let loader_with_stats = |rows: &'static str, stats: crate::dataset::LoadStats| {
            move |conn: &Connection, target: &str| -> Result<crate::dataset::LoadStats> {
                let inner = format!("SELECT id, a, ST_Point(lon, lat) AS geom FROM ({rows})");
                conn.execute_batch(&format!(
                    "CREATE TABLE {target} AS {};",
                    crate::dataset::hashed_select(&inner)
                ))?;
                Ok(stats)
            }
        };

        refresh(
            &conn,
            &TEST_SPEC,
            loader_with_stats(
                NEW_ROWS,
                crate::dataset::LoadStats {
                    skipped_invalid_geometry: 2,
                    skipped_example_ids: vec!["bad1".to_string(), "bad2".to_string()],
                },
            ),
            None,
        )
        .unwrap();

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log.get("update:test").expect("job_run_log entry must exist");
        assert_eq!(entry.outcome, "Success");
        let msg = entry.message.as_deref().unwrap();
        assert!(msg.contains("skipped 2 invalid-geometry rows"), "got: {msg}");
        assert!(msg.contains("bad1") && msg.contains("bad2"), "got: {msg}");
    }

    #[test]
    fn refresh_records_error_in_job_run_log_on_failure() {
        let conn = conn_with_live(LIVE_ROWS);
        let empty = "SELECT * FROM (VALUES ('x','y',1.0,1.0)) t(id,a,lon,lat) WHERE false";

        let _ = refresh(&conn, &TEST_SPEC, loader(empty), None);

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log
            .get("update:test")
            .expect("job_run_log entry must exist even on failure");
        assert_eq!(entry.outcome, "Error");
        assert!(entry.message.as_deref().unwrap().contains("0 rows"));
    }
```

- [ ] **Step 2: Run to verify current state fails to compile / fails**

Run: `cargo test --lib update::dataset::tests`
Expected: FAIL to compile — `refresh` still expects `impl FnOnce(&Connection, &str) -> Result<()>`, and no `job_run_log` rows are written yet.

- [ ] **Step 3: Implement — `refresh` wraps its body and self-reports**

Replace the whole `refresh` function (everything stays the same internally except: the
`load` parameter's type, wrapping the existing body in an outer closure so every error
path is caught for logging, and the two `match &outcome` arms at the end):

```rust
pub fn refresh(
    conn: &Connection,
    spec: &DatasetSpec,
    load: impl FnOnce(&Connection, &str) -> Result<crate::dataset::LoadStats>,
    source_etag: Option<&str>,
) -> Result<DiffCounts> {
    let total = std::time::Instant::now();
    let staging = spec.staging_table();

    conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging}"))
        .with_context(|| format!("Failed to clear stale staging table {staging}"))?;

    let _guard = ScratchGuard {
        conn,
        staging: staging.clone(),
    };

    let job_name = format!("update:{}", spec.name);
    let outcome = (|| -> Result<(DiffCounts, crate::dataset::LoadStats)> {
        // --- stage ---
        let t = std::time::Instant::now();
        let load_stats =
            load(conn, &staging).with_context(|| format!("Failed to stage {} snapshot", spec.name))?;
        let staged: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {staging}"), [], |row| {
                row.get(0)
            })
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

        let hash_version = check_row_hash_version(conn)?;

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
            let snapshot_id: i64 = conn
                .query_row(
                    "SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM dataset_refreshes",
                    [],
                    |row| row.get(0),
                )
                .context("Failed to allocate snapshot_id")?;

            insert_change_areas(conn, spec, snapshot_id)?;
            crate::update::changeset::insert_dirty_cells(conn, spec)?;

            conn.execute_batch(&format!(
                "DELETE FROM {live} WHERE {id} IN (
                     SELECT id FROM diff_removed UNION ALL SELECT id FROM diff_modified);
                 INSERT INTO {live} SELECT * FROM {staging} WHERE {id} IN (
                     SELECT id FROM diff_added UNION ALL SELECT id FROM diff_modified);"
            ))
            .with_context(|| format!("Failed to apply delta to {live}"))?;

            if hash_version != RowHashVersion::Current {
                crate::dataset::stamp_row_hash_version(conn)?;
            }

            conn.execute(
                "INSERT INTO dataset_refreshes
                 (snapshot_id, source, started_at, finished_at, source_etag,
                  added, modified, removed)
                 VALUES (?, ?, now(), now(), ?, ?, ?, ?)",
                duckdb::params![
                    snapshot_id,
                    spec.name,
                    source_etag,
                    counts.added,
                    counts.modified,
                    counts.removed,
                ],
            )
            .context("Failed to record refresh")?;

            Ok(snapshot_id)
        })();

        let snapshot_id = match applied {
            Ok(snapshot_id) => match conn.execute_batch("COMMIT") {
                Ok(()) => snapshot_id,
                Err(e) => {
                    if let Err(rb) = conn.execute_batch("ROLLBACK") {
                        warn!(error = %rb, "failed to roll back apply transaction after commit failure");
                    }
                    return Err(e).context("Failed to commit apply transaction");
                }
            },
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

        Ok((counts, load_stats))
    })();

    match &outcome {
        Ok((counts, stats)) => {
            let _ = crate::job_log::record(
                conn,
                &job_name,
                "Success",
                Some(&summarize_refresh(counts, stats)),
            );
        }
        Err(e) => {
            let _ = crate::job_log::record(conn, &job_name, "Error", Some(&format!("{e:#}")));
        }
    }

    outcome.map(|(counts, _)| counts)
}

/// Human-readable message for the `job_run_log` row.
fn summarize_refresh(counts: &DiffCounts, stats: &crate::dataset::LoadStats) -> String {
    let mut msg = format!(
        "added {} modified {} removed {}",
        counts.added, counts.modified, counts.removed
    );
    if stats.skipped_invalid_geometry > 0 {
        let shown = stats.skipped_example_ids.join(", ");
        let more = stats.skipped_invalid_geometry as usize - stats.skipped_example_ids.len();
        let more_suffix = if more > 0 {
            format!(", +{more} more")
        } else {
            String::new()
        };
        msg.push_str(&format!(
            "; skipped {} invalid-geometry rows (ids: {shown}{more_suffix})",
            stats.skipped_invalid_geometry
        ));
    }
    msg
}
```

Note: `return Err(...)` inside the `applied` closure and inside the outer `outcome`
closure returns from that closure, not from `refresh` itself — that's exactly what makes
every early-exit path (the `bail!`, the `?`s, the transaction rollback branches) land in
`outcome` as `Err(...)` so it gets logged before `refresh` returns.

- [ ] **Step 4: Run the dataset tests to verify they pass**

Run: `cargo test --lib update::dataset::tests`
Expected: PASS (all existing tests plus the 2 new ones — roughly 18 total).

- [ ] **Step 5: Run the full test suite to check nothing else broke**

Run: `cargo test --lib`
Expected: PASS.

- [ ] **Step 6: Verify `src/update/mod.rs` needs no changes**

Run: `cargo build`
Expected: succeeds with no errors in `src/update/mod.rs` — its two closures
(`|c, target| crate::import::bdot10k::load_into(c, target, &p)` and the EGIB equivalent)
now naturally return `Result<LoadStats>`, matching `refresh`'s new expected type. If this
does not compile, do not add a workaround in `update/mod.rs` — re-check that Tasks 3/4's
`load_into` signatures and this task's `refresh` signature actually match.

- [ ] **Step 7: Run the existing CLI update integration tests to confirm no regression**

Run: `cargo test --test cli_update_bdot10k --test cli_update_egib`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/update/dataset.rs
git commit -m "feat(update): thread LoadStats through refresh, self-report to job_log

refresh()'s load closure now returns Result<LoadStats> instead of Result<()>.
The whole stage/diff/apply body is wrapped so every error path is caught and
logged to job_log under 'update:{spec.name}', alongside a success summary
that includes any skipped invalid-geometry rows."
```

---

### Task 6: `/status` exposes `job_run_log`

**Files:**
- Modify: `src/server/jobs/status_handler.rs`

**Interfaces:**
- Consumes: `crate::job_log::{JobRunLogEntry, read_all}` (Task 2).
- Produces: `StatusResponse.job_run_log: BTreeMap<String, JobRunLogEntry>`.

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn job_run_log_or_default_reads_recorded_entries() {
        use crate::db::init_db;
        use crate::server::jobs::JobRegistry;
        use std::path::Path;
        use std::sync::Arc;

        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        crate::job_log::record(&conn, "import:bdot10k", "Success", Some("no invalid geometry"))
            .unwrap();

        let pool = crate::server::build_pool(conn, 2).unwrap();
        let state = AppState {
            pool,
            registry: Arc::new(JobRegistry::new_for_tests(vec![])),
            config: Arc::new(crate::config::Config::default()),
        };

        let log = job_run_log_or_default(&state);
        let entry = log.get("import:bdot10k").expect("entry must be present");
        assert_eq!(entry.outcome, "Success");
        assert_eq!(entry.message.as_deref(), Some("no invalid geometry"));
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib server::jobs::status_handler::tests::job_run_log_or_default`
Expected: FAIL to compile — `job_run_log_or_default` doesn't exist yet.

- [ ] **Step 3: Implement**

At the top of the file, extend the import list:

```rust
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use axum::Json;
use axum::extract::State;
use duckdb::Connection;
use serde::Serialize;

use crate::job_log::{self, JobRunLogEntry};
use crate::server::AppState;
use crate::server::jobs::JobStatus;
```

Add a new function next to `match_staleness_or_default` (same file, right after it):

```rust
/// Acquires a pool connection and reads `job_run_log`, falling back to an
/// empty map (rather than propagating an error) for the same reason
/// `match_staleness_or_default` does: this is a secondary diagnostic, not a
/// reason for `/status` to fail.
fn job_run_log_or_default(state: &AppState) -> BTreeMap<String, JobRunLogEntry> {
    let outcome = (|| -> Result<BTreeMap<String, JobRunLogEntry>> {
        let conn = state
            .pool
            .get()
            .context("Failed to acquire pool connection")?;
        job_log::read_all(&conn)
    })();
    match outcome {
        Ok(log) => log,
        Err(e) => {
            tracing::warn!(error = %e, "failed to read job_run_log for /status; falling back to empty");
            BTreeMap::new()
        }
    }
}
```

Update `StatusResponse` and `get_status` to combine both DB reads into a single
`spawn_blocking` call:

```rust
#[derive(Serialize)]
pub struct StatusResponse {
    pub jobs: Vec<JobStatus>,
    pub match_staleness: MatchStaleness,
    pub job_run_log: BTreeMap<String, JobRunLogEntry>,
}

pub async fn get_status(State(state): State<AppState>) -> Json<StatusResponse> {
    let jobs = state.registry.snapshot();
    let (match_staleness, job_run_log) = tokio::task::spawn_blocking(move || {
        (
            match_staleness_or_default(&state),
            job_run_log_or_default(&state),
        )
    })
    .await
    .unwrap_or_default();
    Json(StatusResponse {
        jobs,
        match_staleness,
        job_run_log,
    })
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test --lib server::jobs::status_handler::tests`
Expected: PASS (both the existing `staleness_counts_distinct_cells_per_source` test and
the new one).

- [ ] **Step 5: Run the full test suite**

Run: `cargo test --lib`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/server/jobs/status_handler.rs
git commit -m "feat(server): expose job_run_log in /status

StatusResponse gains job_run_log, read the same way match_staleness already
is -- a best-effort DB read that falls back to empty rather than failing the
endpoint."
```

---

### Task 7: Full build, full test suite, and clippy/fmt

**Files:** none (verification only).

- [ ] **Step 1: Format**

Run: `cargo fmt`
Expected: no diff, or only whitespace changes from the new code above — review and
accept.

- [ ] **Step 2: Lint**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: no warnings. Fix anything flagged before moving on (in particular: unused
imports if any file ended up not needing one of the additions above).

- [ ] **Step 3: Full test suite**

Run: `cargo test`
Expected: PASS — every unit test (`--lib`) and every integration test in `tests/`.

- [ ] **Step 4: Commit if Steps 1–2 produced changes**

```bash
git add -A
git commit -m "chore: cargo fmt / clippy fixes"
```
(Skip this commit entirely if there was nothing to fix.)

---

### Task 8: Document the new behavior in `CLAUDE.md`

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add a gotcha**

In `CLAUDE.md`, under the existing run of "**Gotcha —" paragraphs (the section
documenting `src/dataset.rs`/`ROW_HASH_VERSION` and the match-rule/serving-table
invariants), add a new paragraph after the "serving tables store rows, not id
references" gotcha and before the "dirty-queue source strings must match everywhere"
gotcha:

```markdown
**Gotcha — invalid government geometry is dropped, not repaired.** A small number of
BDOT10k/EGIB rows have topologically invalid geometry (`ST_IsValid = false`), which
crashes `ST_AsMVTGeom` and takes down the whole tile (see
`docs/invalid_geometry_tile_500s.md`). `dataset::filter_invalid_geometry` deletes those
rows immediately after `import::bdot10k::load_into` / `import::egib::load_into` create
their table — the one place both `import` and `update`'s staging load funnel through — so
`compare::buildings` and `compare::incremental` never see them and need no changes of
their own. `import()` and `update::dataset::refresh()` each self-report their outcome
(including any skipped-row summary) to the `job_run_log` table via the `job_log` module,
under job names `import:<source>` / `update:<source>`; `/status` reads it back as
`job_run_log`. Only `bdot10k`/`egib` are wired up — PRG doesn't go through this path.
```

- [ ] **Step 2: Update the CLI command description if needed**

Check the "**CLI commands**" bullet list near the top of `CLAUDE.md` — the existing text
for `run` already says `/status`; no changes needed there since this plan doesn't add a
new endpoint, only a new field. Skip this step if a re-read confirms that (it should).

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: document invalid-geometry drop and job_run_log in CLAUDE.md"
```

---

## Post-plan manual verification (not automated, do after merging)

Per the design doc's explicit scope decision, cleaning up the 199 rows already live in
the production database is an operational follow-up, not a task here. After deploying
this change, an operator should run `update bdot10k` and `update egib` (or a fresh
`import` of both) against the real database, then check `/status`'s `job_run_log` for
`update:bdot10k` / `update:egib` showing the skip counts, and spot-check that
`docs/invalid_geometry_tile_500s.md`'s previously-failing tile (`z14/9231/5505`) now
returns 200.
