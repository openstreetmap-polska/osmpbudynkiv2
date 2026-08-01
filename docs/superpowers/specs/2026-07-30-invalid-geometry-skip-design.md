# Buildings: skip invalid geometry on import/update, log it in job status

## Motivation

`docs/invalid_geometry_tile_500s.md` records a live production fault: 199 government
building rows (1 bdot10k, 198 egib) have topologically invalid geometry
(`ST_IsValid = false`), and 23 z14 tiles 500 because `ST_AsMVTGeom` aborts the whole tile
query on the first invalid row it hits — taking every *valid* building and the addresses
layer in that tile down with it.

That doc lays out three options (repair via `ST_MakeValid`, filter at tile-query time,
repair at import time) and leaves the choice open. This spec picks a fourth path agreed
in discussion: **drop invalid rows instead of repairing them**, at the government dataset
loader (shared by both `import` and `update`), and make the drop visible through
`/status` instead of silent.

## Scope

- Filter invalid-geometry rows out of `bdot10k_buildings`/`egib_buildings` at the one
  place both `import` and `update` load through: `load_into` in `src/import/bdot10k.rs`
  and `src/import/egib.rs`.
- Because that's upstream of both `compare::buildings` (full) and
  `compare::incremental::recompute_cell_in_txn`, neither compare path nor
  `compare::rule` needs to change — they only ever see already-clean source tables. The
  `full_vs_incremental_equivalence` test is unaffected.
- A new durable "last run" log per job, `job_run_log`, readable via `/status`. Built as a
  small reusable mechanism, but only wired up for the four job names this problem
  actually needs: `import:bdot10k`, `import:egib`, `update:bdot10k`, `update:egib`.
- PRG is untouched: it doesn't go through `load_into`/`dataset::refresh` (it has its own
  `update_prg` path), and addresses are points — not the failure mode this doc is about.

Out of scope (deliberately, per discussion):

- Cleaning up the 199 rows already live in production. Shipping this code does not
  retroactively touch `bdot10k_buildings`/`egib_buildings` or the stale rows already
  sitting in `*_unmatched` — an `update bdot10k`/`update egib` run (which re-stages with
  the new filter, diffs, and lands the now-missing rows as `removed`) or an operator-run
  `import` will clear it, but that's a deploy/ops step, not part of this change.
- `ST_MakeValid` geometry repair. We're dropping rows, not fixing them.
- Extending `job_run_log` to `osm_update`, `match_refresh`, `match_reconcile`, or
  `export_log_prune`. No current need; the mechanism doesn't preclude it later.

## Design

### `src/dataset.rs` — `LoadStats` and the shared filter

```rust
/// Rows a dataset loader dropped rather than staging, because their geometry
/// failed `ST_IsValid`. `ST_AsMVTGeom` cannot tolerate invalid geometry (see
/// docs/invalid_geometry_tile_500s.md) -- we drop rather than repair.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadStats {
    pub skipped_invalid_geometry: i64,
    /// First `MAX_EXAMPLE_IDS` ids of skipped rows, in whatever order the
    /// DELETE's backing scan finds them -- not exhaustive, just enough to
    /// point an operator at the actual bad records upstream.
    pub skipped_example_ids: Vec<String>,
}

pub const MAX_EXAMPLE_IDS: usize = 20;

/// Delete invalid-geometry rows from a just-loaded table, capturing examples
/// before they're gone. Shared by `import::bdot10k::load_into` and
/// `import::egib::load_into` -- the one place both `import` and `update`'s
/// staging load funnel through, so a row filtered out here never reaches
/// `compare::buildings` or `compare::incremental` at all.
pub fn filter_invalid_geometry(conn: &Connection, table: &str, id_col: &str) -> Result<LoadStats> {
    let mut skipped_example_ids = Vec::new();
    {
        let mut stmt = conn.prepare(&format!(
            "SELECT {id_col} FROM {table} WHERE NOT ST_IsValid(geom) LIMIT {MAX_EXAMPLE_IDS}"
        ))?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        for row in rows {
            skipped_example_ids.push(row?);
        }
    }
    let skipped_invalid_geometry = conn
        .execute(&format!("DELETE FROM {table} WHERE NOT ST_IsValid(geom)"), [])
        .with_context(|| format!("Failed to delete invalid-geometry rows from {table}"))?
        as i64;
    Ok(LoadStats { skipped_invalid_geometry, skipped_example_ids })
}
```

### `src/import/bdot10k.rs` / `src/import/egib.rs` — `load_into` returns `LoadStats`

`load_into`'s signature changes from `Result<()>` to `Result<LoadStats>`; the only new
line is the call to `filter_invalid_geometry` right after the `CREATE TABLE`:

```rust
pub fn load_into(conn: &Connection, target_table: &str, parquet_path: &str) -> Result<LoadStats> {
    let inner = format!(/* unchanged */);
    conn.execute_batch(&format!(/* unchanged CREATE TABLE */))
        .with_context(|| format!("Failed to load BDOT10k data into {target_table}"))?;

    crate::dataset::filter_invalid_geometry(conn, target_table, "LOKALNYID")
}
```

(EGIB: same shape, `id_col = "id_budynku"`.)

### Self-contained logging in `import()` and `refresh()`

The important constraint: the log entry must reflect the *whole* operation's outcome
(download + load + index + count for import; stage + diff + apply for update), not just
whether `load_into` itself succeeded. Both functions already have — or gain — an inner
closure wrapping their fallible body, matching the pattern `update::dataset::refresh`
already uses for its apply transaction (`let applied = (|| -> Result<i64> { ... })();`).
Each function becomes responsible for writing its own `job_run_log` row; no caller
(`import::run`, `update::run`) needs to change.

`src/import/bdot10k.rs`:

```rust
pub fn import(conn: &Connection, config: &Config, file: Option<&Path>, url: &str) -> Result<()> {
    let outcome = (|| -> Result<LoadStats> {
        // ... existing body unchanged, down to load_into returning `stats` ...
        let stats = load_into(conn, crate::dataset::BDOT10K.table, parquet_str)?;
        // ... index creation, count, cleanup, existing "BDOT10k import complete" log ...
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

fn summarize(stats: &LoadStats) -> String {
    if stats.skipped_invalid_geometry == 0 {
        "no invalid geometry".to_string()
    } else {
        format!(
            "skipped {} invalid-geometry rows (ids: {}{})",
            stats.skipped_invalid_geometry,
            stats.skipped_example_ids.join(", "),
            if stats.skipped_invalid_geometry as usize > stats.skipped_example_ids.len() {
                format!(", +{} more", stats.skipped_invalid_geometry as usize - stats.skipped_example_ids.len())
            } else {
                String::new()
            }
        )
    }
}
```

`import()`'s public signature stays `Result<()>` — `LoadStats` only needs to live long
enough to build the log message, so nothing upstream in `import::run` changes.
(EGIB: identical shape, job name `"import:egib"`.)

`src/update/dataset.rs` — `refresh`'s `load` closure changes from
`impl FnOnce(&Connection, &str) -> Result<()>` to
`impl FnOnce(&Connection, &str) -> Result<LoadStats>`. `refresh` itself keeps returning
`Result<DiffCounts>` (unchanged signature for its callers) but now wraps its own existing
body the same way, capturing `load_stats` alongside `counts`, and logs
`"update:{spec.name}"` on both the success and error branch before returning:

```rust
pub fn refresh(
    conn: &Connection,
    spec: &DatasetSpec,
    load: impl FnOnce(&Connection, &str) -> Result<LoadStats>,
    source_etag: Option<&str>,
) -> Result<DiffCounts> {
    let job_name = format!("update:{}", spec.name);
    let outcome = (|| -> Result<(DiffCounts, LoadStats)> {
        // ... existing stage/diff/apply body, unchanged, except:
        let load_stats = load(conn, &staging).with_context(|| format!("Failed to stage {} snapshot", spec.name))?;
        // ... existing diff + apply transaction ...
        Ok((counts, load_stats))
    })();

    match &outcome {
        Ok((counts, stats)) => {
            let _ = crate::job_log::record(conn, &job_name, "Success", Some(&summarize_refresh(counts, stats)));
        }
        Err(e) => {
            let _ = crate::job_log::record(conn, &job_name, "Error", Some(&format!("{e:#}")));
        }
    }
    outcome.map(|(counts, _)| counts)
}
```

**Migration cost:** the only existing call site that constructs a `load` closure not
tied to bdot10k/egib is the test helper `loader()` in `update/dataset.rs`'s test module
(~line 311) — its body changes its last line from `Ok(())` to `Ok(LoadStats::default())`.
No individual `#[test]` fn needs to change; they all call `loader(...)` without inspecting
its return value. `load_into` has exactly three references in the whole codebase
(`bdot10k.rs`, `egib.rs`, `update/mod.rs`'s two closures) and no direct unit tests — the
blast radius is small.

A no-op refresh (unchanged ETag, `update/mod.rs` returns before calling
`dataset::refresh` at all — see `record_noop_refresh`) leaves the previous
`job_run_log` row for that job untouched rather than writing a fresh empty one.

### `src/job_log.rs` (new module)

Not under `src/server/jobs/` — `import`/`update` are core CLI operations independent of
whether the HTTP server is running, so they must not depend on the `server` module.
`server::jobs::status_handler` depends on `job_log`, not the reverse.

```rust
use std::collections::BTreeMap;
use anyhow::{Context, Result};
use duckdb::Connection;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct JobRunLogEntry {
    pub ran_at: String,   // RFC3339
    pub outcome: String,  // "Success" | "Error" -- matches JobOutcome's existing casing
    pub message: Option<String>,
}

/// Delete-then-insert, matching the existing `stamp_row_hash_version`
/// convention in `src/dataset.rs` (not `ON CONFLICT`) -- only the latest run
/// per `job_name` is kept. Best-effort: callers ignore the returned error
/// rather than let a logging failure fail the job itself.
pub fn record(conn: &Connection, job_name: &str, outcome: &str, message: Option<&str>) -> Result<()> {
    conn.execute("DELETE FROM job_run_log WHERE job_name = ?", duckdb::params![job_name])
        .context("Failed to clear previous job_run_log row")?;
    conn.execute(
        "INSERT INTO job_run_log (job_name, ran_at, outcome, message) VALUES (?, now(), ?, ?)",
        duckdb::params![job_name, outcome, message],
    )
    .context("Failed to write job_run_log row")?;
    Ok(())
}

pub fn read_all(conn: &Connection) -> Result<BTreeMap<String, JobRunLogEntry>> {
    let mut stmt = conn.prepare(
        "SELECT job_name, ran_at::VARCHAR, outcome, message FROM job_run_log ORDER BY job_name",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            JobRunLogEntry {
                ran_at: r.get(1)?,
                outcome: r.get(2)?,
                message: r.get(3)?,
            },
        ))
    })?;
    let mut out = BTreeMap::new();
    for row in rows {
        let (name, entry) = row?;
        out.insert(name, entry);
    }
    Ok(out)
}
```

### `src/db.rs` — schema

```sql
CREATE TABLE IF NOT EXISTS job_run_log (
    job_name VARCHAR,
    ran_at   TIMESTAMP,
    outcome  VARCHAR,
    message  VARCHAR
);
```

### `src/server/jobs/status_handler.rs` — `/status` field

`StatusResponse` gains a field, populated the same way `match_staleness` already is
(acquire a pool connection, fall back to empty on failure rather than failing the whole
endpoint):

```rust
#[derive(Serialize)]
pub struct StatusResponse {
    pub jobs: Vec<JobStatus>,
    pub match_staleness: MatchStaleness,
    pub job_run_log: BTreeMap<String, crate::job_log::JobRunLogEntry>,
}
```

Example response fragment:

```json
"job_run_log": {
  "update:bdot10k": {
    "ran_at": "2026-07-30T14:02:11Z",
    "outcome": "Success",
    "message": "skipped 12 invalid-geometry rows (ids: 062008_2.0016.283/13.3_BUD, ...+11 more)"
  }
}
```

A map (not an array) for the same reason `pending_by_source` already is one — see the
comment on `MatchStaleness::pending_by_source`.

## Testing

- `dataset::filter_invalid_geometry`: unit test with one deliberately-invalid polygon
  among valid rows — asserts the invalid row is gone, `skipped_invalid_geometry == 1`,
  and `skipped_example_ids` contains its id.
- `load_into` (bdot10k/egib): fixture-level test asserting an invalid-geometry row
  present in the source parquet never lands in `bdot10k_buildings`/`egib_buildings`.
- Regression test for the actual production bug: a fixture with an invalid polygon in
  the cell that used to 500, asserting the tile query now succeeds and the valid
  buildings in that cell are still served.
- `job_log::record`/`read_all` round-trip test, including the "only the latest row per
  job_name survives" delete-then-insert behavior.
- `/status` integration test asserting `job_run_log` appears and reflects a prior
  `job_log::record` call.
- Existing `full_vs_incremental_equivalence` and `compare_buildings`/`rule.rs` tests
  require no changes — confirm they still pass unmodified as evidence the filter truly
  sits upstream of both compare paths.

## Risks

Low. The behavior change (dropping invalid-geometry rows instead of serving them broken)
is the one already discussed and accepted in `docs/invalid_geometry_tile_500s.md`'s
tradeoff writeup: a small number of real government buildings become invisible to the
comparison instead of crashing their tile. `job_run_log` makes that omission visible
rather than silent, which is the whole point of this design.

The main mechanical risk is the `refresh`/`load_into` signature change threading through
three call sites (`bdot10k.rs`, `egib.rs`, `update/mod.rs`) plus one test helper — small,
but touch all four when implementing so nothing is left calling the old `Result<()>`
shape.
