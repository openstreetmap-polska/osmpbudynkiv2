# Export Log & /updates Endpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Log every `/package` export into a new `package_exports` table, and add `GET /updates` serving recent exports as a browser-cacheable GeoJSON `FeatureCollection`.

**Architecture:** `build_package` (in `src/server/package.rs`) counts features per category and inserts one row into `package_exports` via the existing `state.write` connection — best-effort, never failing the response. A new `ExportLogPruneJob` (following the `OsmUpdateJob` pattern) deletes old rows on a schedule. A new `src/server/updates.rs` module serves `GET /updates`, reading `package_exports` via `state.write` (locked briefly) rather than `state.read_pool` — `read_pool` never observes writes made through `write` while the server is running (see `docs/duckdb_connection_visibility_investigation.md`); fixing that is explicitly out of scope for this feature.

**Tech Stack:** Rust (edition 2024), axum 0.8, DuckDB spatial + icu extensions (bundled), serde_json (`raw_value` feature, already enabled), tower `oneshot` for handler tests.

**Spec:** `docs/superpowers/specs/2026-07-20-export-log-updates-endpoint-design.md`

## Global Constraints

- All new DB work that reads or writes `package_exports` MUST go through `state.write` (locked briefly per call), never `state.read_pool` — `read_pool` cannot see writes made through `write` while the server runs (verified; see `docs/duckdb_connection_visibility_investigation.md`). This applies to the `/package` export-log insert, the `ExportLogPruneJob` delete, and the `/updates` select.
- `package_exports` schema (exact, no surrogate key, no index): `exported_at TIMESTAMP WITH TIME ZONE, area GEOMETRY('epsg:4326'), datasets VARCHAR[], address_count INTEGER, building_count INTEGER`.
- `GEOMETRY('epsg:4326')` requires the `spatial` extension loaded before `create_schema()` runs to create the table at all (verified: it errors with "unrecognized coordinate system" otherwise) — already guaranteed since `spatial` loads before `create_schema()` in every existing call site and in the default config.
- `TIMESTAMP WITH TIME ZONE` arithmetic (`now() - INTERVAL`) requires the `icu` extension loaded — add `INSTALL icu; LOAD icu;` to `duckdb_init_commands`. Table creation and plain `now()` inserts do NOT need icu; only interval arithmetic does.
- Formatting a `TIMESTAMPTZ` to a UTC string requires `AT TIME ZONE 'UTC'` before `strftime` — `strftime(exported_at, ...)` alone silently uses the session's local timezone despite a `Z` suffix (verified).
- `VARCHAR[]` values cannot be bound as query parameters (binding panics with "not implemented" in this `duckdb` crate version). Write list values as inlined SQL list literals built from the closed `Dataset` enum (never raw request text — safe, same pattern as inlining `source_table` in `src/compare/buildings.rs`). Read list values back via `to_json(column)`, embedding the resulting JSON-array string as a `RawValue`.
- All interval-arithmetic SQL (`now() - INTERVAL ...`) MUST use parentheses: `(now() - INTERVAL '{n} unit')`, not bare `now() - INTERVAL ... compared_to`.
- `datasets` logged is the full requested selection (not just datasets that yielded features). `address_count` = PRG features count; `building_count` = BDOT10k + EGIB combined.
- Every `/package` call is logged — GET and POST, matched or unmatched, even zero-feature results. Logging failures (DB error or poisoned write mutex) are caught, logged via `tracing::warn!`, and never fail the package response.
- `/updates`: `?minutes=` optional, default `config.updates.default_minutes` (60), rejected with `400` above `config.updates.max_minutes` (1440) or if non-positive/non-integer. Response: `Content-Type: application/geo+json`, `Cache-Control: public, max-age=60`, no `Content-Disposition`. Empty window → `200` with empty `features`.
- Retention job config field is `retention_days` (not hours) — default 365, `interval_seconds` default 86400 (daily), `timeout_seconds` default 60.
- Before every commit: `cargo fmt` and `cargo clippy --all-targets`. Commit messages end with the trailer: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

## File Structure

- **Modify `src/config.rs`** — add `"INSTALL icu"`/`"LOAD icu"` to default `duckdb_init_commands`; add `ExportLogPruneConfig` + `JobsConfig.export_log_prune`; add `UpdatesConfig` + `Config.updates`.
- **Modify `example_config.toml`** — document the new init commands and the two new config sections.
- **Modify `src/db.rs`** — add `package_exports` to `create_schema()`; extend/add tests.
- **Modify `src/server/package.rs`** — add `Dataset::sql_name()`; add best-effort export logging to `build_package`.
- **Create `src/server/jobs/export_log_prune.rs`** — `ExportLogPruneJob`.
- **Modify `src/server/jobs/mod.rs`** — `pub mod export_log_prune;`.
- **Modify `src/server/mod.rs`** — register the new job; add the `/updates` route.
- **Create `src/server/updates.rs`** — the whole `/updates` endpoint: params, query, feature types, handler, tests.
- **Modify `README.md`, `CLAUDE.md`** — roadmap tick, endpoint docs.

---

### Task 1: `icu` extension, `package_exports` table, and its config surface

**Files:**
- Modify: `src/config.rs`
- Modify: `example_config.toml`
- Modify: `src/db.rs`

**Interfaces:**
- Produces:
  - `config.package.max_area_sq_deg` unaffected; new: `config.jobs.export_log_prune: ExportLogPruneConfig { enabled, interval_seconds, timeout_seconds, retention_days }` (defaults: `true, 86400, 60, 365`).
  - `config.updates: UpdatesConfig { default_minutes: u64, max_minutes: u64 }` (defaults: `60, 1440`).
  - `duckdb_init_commands` default list gains `"INSTALL icu"`, `"LOAD icu"` (length 7 → 9).
  - `package_exports` table exists after `init_db`/`create_schema`.

- [ ] **Step 1: Write the failing config tests**

In `src/config.rs`, update the two existing assertions that hardcode the init-commands count. Change both occurrences of:
```rust
        assert_eq!(config.duckdb_init_commands.len(), 7);
```
to:
```rust
        assert_eq!(config.duckdb_init_commands.len(), 9);
```
(These appear in `test_load_config_none_returns_defaults` and `test_load_config_partial_file`.)

Then add new tests, inserting them into the `tests` module right before `test_teryt_partial_override`:

```rust
    #[test]
    fn test_export_log_prune_config_defaults() {
        let config = load_config(None).unwrap();
        assert!(config.jobs.export_log_prune.enabled);
        assert_eq!(config.jobs.export_log_prune.interval_seconds, 86400);
        assert_eq!(config.jobs.export_log_prune.timeout_seconds, 60);
        assert_eq!(config.jobs.export_log_prune.retention_days, 365);
    }

    #[test]
    fn test_export_log_prune_config_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
[jobs.export_log_prune]
enabled = false
interval_seconds = 3600
timeout_seconds = 30
retention_days = 30
"#
        )
        .unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert!(!config.jobs.export_log_prune.enabled);
        assert_eq!(config.jobs.export_log_prune.interval_seconds, 3600);
        assert_eq!(config.jobs.export_log_prune.timeout_seconds, 30);
        assert_eq!(config.jobs.export_log_prune.retention_days, 30);
    }

    #[test]
    fn test_updates_config_defaults() {
        let config = load_config(None).unwrap();
        assert_eq!(config.updates.default_minutes, 60);
        assert_eq!(config.updates.max_minutes, 1440);
    }

    #[test]
    fn test_updates_config_override() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
[updates]
default_minutes = 30
max_minutes = 720
"#
        )
        .unwrap();

        let config = load_config(Some(tmp.path())).unwrap();
        assert_eq!(config.updates.default_minutes, 30);
        assert_eq!(config.updates.max_minutes, 720);
    }
```

- [ ] **Step 2: Write the failing db.rs tests**

In `src/db.rs`, replace the `tables` array in `test_init_db_creates_tables`:
```rust
        let tables = ["metadata", "osm_addresses", "osm_buildings"];
```
with:
```rust
        let tables = [
            "metadata",
            "osm_addresses",
            "osm_buildings",
            "package_exports",
        ];
```

Then add a new test after `test_init_db_is_idempotent`:
```rust
    #[test]
    fn test_package_exports_column_types_round_trip() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;

        conn.execute(
            "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now(), ST_Point(21.0, 52.0), ['prg', 'bdot10k'], 3, 5)",
            [],
        )?;

        let (geojson, datasets_json, address_count, building_count): (String, String, i32, i32) =
            conn.query_row(
                "SELECT ST_AsGeoJSON(area), to_json(datasets), address_count, building_count
                 FROM package_exports",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;

        assert!(geojson.contains("\"Point\""));
        assert_eq!(datasets_json, r#"["prg","bdot10k"]"#);
        assert_eq!(address_count, 3);
        assert_eq!(building_count, 5);

        Ok(())
    }
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test --bin osmpbudynkiv2 config::tests db::tests`
Expected: config tests FAIL on the `len(), 9` assertions (currently 7); `db::tests` FAIL — `package_exports` table doesn't exist, `Config` has no `jobs.export_log_prune`/`updates` fields (compile errors for those two new config tests).

- [ ] **Step 4: Implement the config additions**

In `src/config.rs`, add after the `PackageConfig` block (after its `impl Default for PackageConfig`):
```rust
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct ExportLogPruneConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub timeout_seconds: u64,
    /// How long package_exports rows are kept before being pruned.
    pub retention_days: u64,
}

impl Default for ExportLogPruneConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_seconds: 86400,
            timeout_seconds: 60,
            retention_days: 365,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct UpdatesConfig {
    pub default_minutes: u64,
    pub max_minutes: u64,
}

impl Default for UpdatesConfig {
    fn default() -> Self {
        Self {
            default_minutes: 60,
            max_minutes: 1440,
        }
    }
}
```

Add `export_log_prune` to `JobsConfig`:
```rust
#[derive(Debug, Deserialize, Clone, Default)]
#[serde(default)]
pub struct JobsConfig {
    pub osm_update: JobConfig,
    pub export_log_prune: ExportLogPruneConfig,
}
```

Add `updates` to `Config`:
```rust
    pub jobs: JobsConfig,
    pub package: PackageConfig,
    pub updates: UpdatesConfig,
}
```

Add to `impl Default for Config`, after `package: PackageConfig::default(),`:
```rust
            updates: UpdatesConfig::default(),
```

Add `icu` to the default `duckdb_init_commands` list, right after the spatial lines:
```rust
            duckdb_init_commands: vec![
                "INSTALL spatial".to_string(),
                "LOAD spatial".to_string(),
                "INSTALL icu".to_string(),
                "LOAD icu".to_string(),
                "SET preserve_insertion_order = false".to_string(),
                "SET geometry_always_xy = true".to_string(),
                "SET temp_directory = './osmpbudynkiv2.duckdb.tmp'".to_string(),
                "SET memory_limit = '4GB'".to_string(),
                "SET threads = 8".to_string(),
            ],
```

- [ ] **Step 5: Implement the `package_exports` table**

In `src/db.rs`, add to `create_schema()`'s SQL, after the `osm_buildings` table:
```sql
        -- Export log for the /package endpoint (see GET /updates). Requires
        -- the spatial extension to already be loaded (via duckdb_init_commands)
        -- before this runs, since GEOMETRY('epsg:4326') needs spatial to
        -- resolve the CRS string -- unlike the bare GEOMETRY columns above.
        CREATE TABLE IF NOT EXISTS package_exports (
            exported_at TIMESTAMP WITH TIME ZONE,
            area GEOMETRY('epsg:4326'),
            datasets VARCHAR[],
            address_count INTEGER,
            building_count INTEGER
        );
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test --bin osmpbudynkiv2 config::tests db::tests`
Expected: all PASS.

- [ ] **Step 7: Document in example_config.toml**

In `example_config.toml`, update the `duckdb_init_commands` list:
```toml
# SQL statements executed on DuckDB initialization (after opening the database).
# These run before schema creation.
# WARNING: replacing this list removes the defaults — include everything you need.
duckdb_init_commands = [
    "INSTALL spatial",
    "LOAD spatial",
    "INSTALL icu",
    "LOAD icu",
    "SET enable_progress_bar = false",
    "SET preserve_insertion_order = false",
    "SET geometry_always_xy = true",
    "SET temp_directory = './osmpbudynkiv2.duckdb.tmp'",
    "SET max_temp_directory_size = '8GB'",
    "SET memory_limit = '4GB'",
    "SET threads = 8",
]
```

Append after the existing `[jobs.osm_update]` section:
```toml

# Prunes old rows from the /package export log (see [updates] and the
# GET /updates endpoint below).
[jobs.export_log_prune]
enabled = true
interval_seconds = 86400
# How long package_exports rows are kept before being deleted.
retention_days = 365
```

Append after the existing `[package]` section:
```toml

# GET /updates endpoint (recent /package export activity as GeoJSON).
[updates]
# Time window used when the request omits ?minutes=.
default_minutes = 60
# Largest ?minutes= value accepted; larger requests get a 400.
max_minutes = 1440
```

- [ ] **Step 8: Commit**

```bash
cargo fmt && cargo clippy --all-targets
git add src/config.rs src/db.rs example_config.toml
git commit -m "feat: add icu extension, package_exports table, and its config surface

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Write-path export logging in `/package`

**Files:**
- Modify: `src/server/package.rs`

**Interfaces:**
- Consumes: `Dataset`, `RequestArea`, `AppState` (all existing in `package.rs`).
- Produces: `impl Dataset { fn sql_name(self) -> &'static str }`; `build_package` now logs every call as a side effect (no signature change).

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `src/server/package.rs` (near the other handler tests, e.g. right before `unmatched_buildings_respects_polygon_via_centroid`):

```rust
    #[tokio::test]
    async fn get_package_logs_export_with_counts_datasets_and_geometry() {
        let (state, _dir) = make_seeded_state();
        let write = state.write.clone();

        let response = package_app(state)
            .oneshot(
                Request::builder()
                    .uri("/package?bbox=21.0,52.2,21.01,52.21")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let conn = write.lock().unwrap();
        let (address_count, building_count): (i32, i32) = conn
            .query_row(
                "SELECT address_count, building_count FROM package_exports
                 ORDER BY exported_at DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(address_count, 1); // a1
        assert_eq!(building_count, 2); // b1 + e1

        let datasets_json: String = conn
            .query_row(
                "SELECT to_json(datasets) FROM package_exports ORDER BY exported_at DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(datasets_json, r#"["prg","bdot10k","egib"]"#);

        let area_geojson: String = conn
            .query_row(
                "SELECT ST_AsGeoJSON(area) FROM package_exports ORDER BY exported_at DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(area_geojson.contains("\"Polygon\""));
    }

    #[tokio::test]
    async fn get_package_logs_export_even_when_empty() {
        let (state, _dir) = make_seeded_state();
        let write = state.write.clone();

        let response = package_app(state)
            .oneshot(
                Request::builder()
                    .uri("/package?bbox=22.0,53.0,22.01,53.01")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let conn = write.lock().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM package_exports", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
        let (address_count, building_count): (i32, i32) = conn
            .query_row(
                "SELECT address_count, building_count FROM package_exports",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(address_count, 0);
        assert_eq!(building_count, 0);
    }

    #[tokio::test]
    async fn post_package_logs_the_submitted_polygon_not_its_envelope() {
        let (state, _dir) = make_seeded_state();
        let write = state.write.clone();
        let triangle = r#"{"type":"Polygon","coordinates":[[[21.0,52.2],[21.01,52.2],[21.0,52.21],[21.0,52.2]]]}"#;

        let response = package_app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/package")
                    .body(Body::from(triangle))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let conn = write.lock().unwrap();
        let logged_geojson: String = conn
            .query_row("SELECT ST_AsGeoJSON(area) FROM package_exports", [], |row| {
                row.get(0)
            })
            .unwrap();
        // The logged area is the submitted triangle, a Polygon with 4 ring points
        // (closed), not a rectangle envelope.
        let parsed: serde_json::Value = serde_json::from_str(&logged_geojson).unwrap();
        assert_eq!(parsed["type"], "Polygon");
        assert_eq!(parsed["coordinates"][0].as_array().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn get_package_succeeds_even_if_write_mutex_is_poisoned() {
        let (state, _dir) = make_seeded_state();
        let write = state.write.clone();
        // Poison the write mutex the same way a job panic would.
        let _ = std::thread::spawn(move || {
            let _guard = write.lock().unwrap();
            panic!("simulated panic while holding the write lock");
        })
        .join();

        let response = package_app(state)
            .oneshot(
                Request::builder()
                    .uri("/package?bbox=21.0,52.2,21.01,52.21")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            axum::http::StatusCode::OK,
            "logging failure must not fail the package response"
        );
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --bin osmpbudynkiv2 server::package::tests::get_package_logs server::package::tests::post_package_logs server::package::tests::get_package_succeeds_even_if_write_mutex`
Expected: FAIL — either a query error (`package_exports` has no rows since nothing writes to it yet) or the poisoned-mutex test panics/hangs (currently `build_package` never touches `state.write`, so nothing logs and the assertions on row content fail; the poisoned-mutex test should still pass status-wise today since nothing there uses `write` yet — it's included now so it fails once logging is added if the poison-handling is wrong, and to lock in the requirement).

- [ ] **Step 3: Implement `Dataset::sql_name()` and the logging step**

In `src/server/package.rs`, add right after the `ALL_DATASETS` const:
```rust
impl Dataset {
    fn sql_name(self) -> &'static str {
        match self {
            Dataset::Prg => "prg",
            Dataset::Bdot10k => "bdot10k",
            Dataset::Egib => "egib",
        }
    }
}
```

Replace `build_package`'s body:
```rust
fn build_package(state: &AppState, area: &RequestArea, datasets: &[Dataset]) -> Result<String> {
    let conn = state
        .read_pool
        .get()
        .context("Failed to acquire read connection")?;
    let mut features = Vec::new();
    for dataset in datasets {
        match dataset {
            Dataset::Prg => {
                for row in unmatched_addresses(&conn, area)? {
                    let properties = address_tags(&row);
                    features.push(feature(row.geometry_geojson, properties)?);
                }
            }
            Dataset::Bdot10k => {
                for geometry in unmatched_buildings(&conn, "bdot10k_buildings", area)? {
                    features.push(feature(geometry, building_tags())?);
                }
            }
            Dataset::Egib => {
                for geometry in unmatched_buildings(&conn, "egib_buildings", area)? {
                    features.push(feature(geometry, building_tags())?);
                }
            }
        }
    }
    let collection = FeatureCollection {
        kind: "FeatureCollection",
        features,
    };
    Ok(serde_json::to_string(&collection)?)
}
```
with:
```rust
fn build_package(state: &AppState, area: &RequestArea, datasets: &[Dataset]) -> Result<String> {
    let conn = state
        .read_pool
        .get()
        .context("Failed to acquire read connection")?;
    let mut features = Vec::new();
    let mut address_count: i32 = 0;
    let mut building_count: i32 = 0;
    for dataset in datasets {
        match dataset {
            Dataset::Prg => {
                for row in unmatched_addresses(&conn, area)? {
                    let properties = address_tags(&row);
                    features.push(feature(row.geometry_geojson, properties)?);
                    address_count += 1;
                }
            }
            Dataset::Bdot10k => {
                for geometry in unmatched_buildings(&conn, "bdot10k_buildings", area)? {
                    features.push(feature(geometry, building_tags())?);
                    building_count += 1;
                }
            }
            Dataset::Egib => {
                for geometry in unmatched_buildings(&conn, "egib_buildings", area)? {
                    features.push(feature(geometry, building_tags())?);
                    building_count += 1;
                }
            }
        }
    }
    drop(conn);
    log_export(state, area, datasets, address_count, building_count);
    let collection = FeatureCollection {
        kind: "FeatureCollection",
        features,
    };
    Ok(serde_json::to_string(&collection)?)
}

/// Best-effort export logging: failures are logged via `tracing::warn!` and
/// never affect the package response. `datasets` is drawn from the closed
/// `Dataset` enum, so building the SQL list literal directly from it is safe
/// -- no request text reaches this string. Runs via `state.write` (not
/// `state.read_pool`), since a later read of this data through `/updates`
/// must also go through `write` -- see
/// docs/duckdb_connection_visibility_investigation.md.
fn log_export(
    state: &AppState,
    area: &RequestArea,
    datasets: &[Dataset],
    address_count: i32,
    building_count: i32,
) {
    let datasets_sql = format!(
        "[{}]",
        datasets
            .iter()
            .map(|d| format!("'{}'", d.sql_name()))
            .collect::<Vec<_>>()
            .join(",")
    );
    let conn = match state.write.lock() {
        Ok(conn) => conn,
        Err(_) => {
            tracing::warn!("write mutex poisoned, skipping package export log");
            return;
        }
    };
    let sql = format!(
        "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
         VALUES (now(), ST_GeomFromGeoJSON(?), {datasets_sql}, ?, ?)"
    );
    if let Err(e) = conn.execute(
        &sql,
        duckdb::params![area.polygon_geojson, address_count, building_count],
    ) {
        tracing::warn!(error = %e, "failed to log package export");
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --bin osmpbudynkiv2 server::package`
Expected: all PASS, including the 4 new tests and every pre-existing test in the module.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets
git add src/server/package.rs
git commit -m "feat: log every /package export into package_exports

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: `ExportLogPruneJob` retention job

**Files:**
- Create: `src/server/jobs/export_log_prune.rs`
- Modify: `src/server/jobs/mod.rs`
- Modify: `src/server/mod.rs`

**Interfaces:**
- Consumes: `Job`, `JobContext`, `JobConfigResolved` (all existing in `jobs/mod.rs`); `config.jobs.export_log_prune.retention_days` (Task 1).
- Produces: `pub struct ExportLogPruneJob;` implementing `Job`, registered in the server's job list.

- [ ] **Step 1: Declare the module**

In `src/server/jobs/mod.rs`, change:
```rust
pub mod osm_update;
pub mod status_handler;
```
to:
```rust
pub mod export_log_prune;
pub mod osm_update;
pub mod status_handler;
```

- [ ] **Step 2: Write the failing test**

Create `src/server/jobs/export_log_prune.rs`:

```rust
//! Background job that deletes package_exports rows older than
//! `config.jobs.export_log_prune.retention_days`.

use anyhow::Result;

use crate::server::jobs::{Job, JobContext};

pub struct ExportLogPruneJob;

impl Job for ExportLogPruneJob {
    fn name(&self) -> &'static str {
        "export_log_prune"
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx.write.lock().expect("write mutex poisoned");
        let days = ctx.config.jobs.export_log_prune.retention_days;
        conn.execute(
            &format!(
                "DELETE FROM package_exports WHERE exported_at < (now() - INTERVAL '{days} days')"
            ),
            [],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::Mutex as StdMutex;

    use super::*;
    use crate::config::Config as AppConfig;
    use crate::db::init_db;

    fn make_ctx(retention_days: u64) -> (JobContext, tempfile::TempDir) {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(crate::osm::kvstore::open(dir.path(), 8, 4).unwrap());

        let mut config = AppConfig::default();
        config.jobs.export_log_prune.retention_days = retention_days;

        let ctx = JobContext {
            write: Arc::new(StdMutex::new(conn)),
            kv,
            config: Arc::new(config),
            cancel: Arc::new(AtomicBool::new(false)),
        };
        (ctx, dir)
    }

    #[test]
    fn deletes_rows_older_than_retention_keeps_newer_ones() {
        let (ctx, _dir) = make_ctx(365);
        {
            let conn = ctx.write.lock().unwrap();
            conn.execute_batch(
                "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
                 VALUES (now() - INTERVAL '400 days', ST_Point(21.0, 52.0), ['prg'], 1, 1);
                 INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
                 VALUES (now() - INTERVAL '10 days', ST_Point(21.0, 52.0), ['prg'], 2, 2);",
            )
            .unwrap();
        }

        ExportLogPruneJob.run(&ctx).unwrap();

        let conn = ctx.write.lock().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM package_exports", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
        let remaining_count: i32 = conn
            .query_row("SELECT address_count FROM package_exports", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(remaining_count, 2, "the 10-day-old row must survive");
    }

    #[test]
    fn no_op_when_nothing_is_old_enough() {
        let (ctx, _dir) = make_ctx(365);
        {
            let conn = ctx.write.lock().unwrap();
            conn.execute(
                "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
                 VALUES (now(), ST_Point(21.0, 52.0), ['prg'], 1, 1)",
                [],
            )
            .unwrap();
        }

        ExportLogPruneJob.run(&ctx).unwrap();

        let conn = ctx.write.lock().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM package_exports", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
}
```

- [ ] **Step 3: Run the test to verify it fails, then check it actually passes**

Run: `cargo test --bin osmpbudynkiv2 server::jobs::export_log_prune`
Expected: PASS immediately, since the implementation is written inline above (this task writes test and implementation together since the job body is a single small SQL statement with no separable "red" step — the important verification is that it behaves correctly, not that it was typed in two phases). Confirm both tests pass and that removing the `(...)` parentheses around `now() - INTERVAL ...` in the `DELETE` statement — as a manual sanity check, not a permanent change — would still work too (parentheses here are for readability per project convention, not correctness; DuckDB's operator precedence already evaluates `-` before `<`). Leave the parentheses in place.

- [ ] **Step 4: Register the job in the server**

In `src/server/mod.rs`, change:
```rust
    let osm_cfg = jobs::JobConfigResolved::from(&config.jobs.osm_update);
    let job_list: Vec<(Arc<dyn jobs::Job>, jobs::JobConfigResolved)> = vec![(
        Arc::new(jobs::osm_update::OsmUpdateJob) as Arc<dyn jobs::Job>,
        osm_cfg,
    )];
```
to:
```rust
    let osm_cfg = jobs::JobConfigResolved::from(&config.jobs.osm_update);
    let export_prune_cfg = jobs::JobConfigResolved {
        enabled: config.jobs.export_log_prune.enabled,
        interval: std::time::Duration::from_secs(config.jobs.export_log_prune.interval_seconds),
        timeout: std::time::Duration::from_secs(config.jobs.export_log_prune.timeout_seconds),
    };
    let job_list: Vec<(Arc<dyn jobs::Job>, jobs::JobConfigResolved)> = vec![
        (
            Arc::new(jobs::osm_update::OsmUpdateJob) as Arc<dyn jobs::Job>,
            osm_cfg,
        ),
        (
            Arc::new(jobs::export_log_prune::ExportLogPruneJob) as Arc<dyn jobs::Job>,
            export_prune_cfg,
        ),
    ];
```

- [ ] **Step 5: Run the full server test suite to confirm nothing broke**

Run: `cargo test --bin osmpbudynkiv2 server::`
Expected: all PASS, including the pre-existing `/status` tests (unaffected — they use a synthetic `JobRegistry::new_for_tests`, not the production job list).

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy --all-targets
git add src/server/jobs/export_log_prune.rs src/server/jobs/mod.rs src/server/mod.rs
git commit -m "feat: add export_log_prune background job

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: `GET /updates` endpoint

**Files:**
- Create: `src/server/updates.rs`
- Modify: `src/server/mod.rs`

**Interfaces:**
- Consumes: `AppState` (`state.write`, `state.config.updates`).
- Produces:
  - `pub fn parse_minutes(s: Option<&str>, default_minutes: u64, max_minutes: u64) -> Result<u64, String>`
  - `pub async fn get_updates(State<AppState>, Query<UpdatesParams>) -> Response`
  - Route `GET /updates`.

- [ ] **Step 1: Declare the module and write the failing parsing tests**

In `src/server/mod.rs`, change:
```rust
mod package;
mod tiles;
```
to:
```rust
mod package;
mod tiles;
mod updates;
```

Create `src/server/updates.rs`:

```rust
//! GET /updates: recent /package export activity as a GeoJSON FeatureCollection,
//! browser-cacheable for 60 seconds.
//!
//! See docs/superpowers/specs/2026-07-20-export-log-updates-endpoint-design.md
//! and docs/duckdb_connection_visibility_investigation.md. Reads run via
//! state.write (not state.read_pool) -- read_pool never observes writes made
//! through write while the server is running.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minutes_default_when_absent() {
        assert_eq!(parse_minutes(None, 60, 1440).unwrap(), 60);
        assert_eq!(parse_minutes(Some("  "), 60, 1440).unwrap(), 60);
    }

    #[test]
    fn parse_minutes_accepts_valid_override() {
        assert_eq!(parse_minutes(Some("15"), 60, 1440).unwrap(), 15);
        assert_eq!(parse_minutes(Some(" 90 "), 60, 1440).unwrap(), 90);
    }

    #[test]
    fn parse_minutes_rejects_over_cap() {
        let err = parse_minutes(Some("1441"), 60, 1440).unwrap_err();
        assert!(err.contains("1440"));
    }

    #[test]
    fn parse_minutes_rejects_zero_and_non_numeric() {
        assert!(parse_minutes(Some("0"), 60, 1440).is_err());
        assert!(parse_minutes(Some("-5"), 60, 1440).is_err());
        assert!(parse_minutes(Some("abc"), 60, 1440).is_err());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --bin osmpbudynkiv2 server::updates`
Expected: compile error — `cannot find function parse_minutes`.

- [ ] **Step 3: Implement `parse_minutes`**

Add above the `tests` module in `src/server/updates.rs`:

```rust
pub fn parse_minutes(s: Option<&str>, default_minutes: u64, max_minutes: u64) -> Result<u64, String> {
    let s = match s {
        None => return Ok(default_minutes),
        Some(s) if s.trim().is_empty() => return Ok(default_minutes),
        Some(s) => s.trim(),
    };
    let minutes: u64 = s
        .parse()
        .map_err(|_| format!("minutes value '{s}' is not a positive integer"))?;
    if minutes == 0 {
        return Err("minutes must be at least 1".to_string());
    }
    if minutes > max_minutes {
        return Err(format!(
            "minutes {minutes} exceeds maximum {max_minutes}"
        ));
    }
    Ok(minutes)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --bin osmpbudynkiv2 server::updates`
Expected: all 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets
git add src/server/updates.rs src/server/mod.rs
git commit -m "feat: parse and validate ?minutes= for GET /updates

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: `/updates` query, feature assembly, and handler

**Files:**
- Modify: `src/server/updates.rs`
- Modify: `src/server/mod.rs`

**Interfaces:**
- Consumes: `parse_minutes` (Task 4), `AppState` (`state.write`, `state.config`).
- Produces:
  - `pub struct UpdateProperties { pub exported_at: String, pub datasets: Box<RawValue>, pub address_count: i32, pub building_count: i32 }`
  - `pub struct UpdateFeature { pub kind: &'static str, pub geometry: Box<RawValue>, pub properties: UpdateProperties }`
  - `pub struct UpdatesFeatureCollection { pub kind: &'static str, pub features: Vec<UpdateFeature> }`
  - `pub fn recent_exports(conn: &duckdb::Connection, minutes: u64) -> anyhow::Result<Vec<UpdateFeature>>`
  - `pub struct UpdatesParams { pub minutes: Option<String> }`
  - `pub async fn get_updates(State<AppState>, Query<UpdatesParams>) -> Response`

- [ ] **Step 1: Write the failing DB-level and feature-assembly tests**

Add to the `tests` module in `src/server/updates.rs`:

```rust
    use std::path::Path;

    use crate::db::init_db;

    fn setup_db() -> duckdb::Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    #[test]
    fn recent_exports_includes_within_window_excludes_outside() {
        let conn = setup_db();
        conn.execute_batch(
            "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now() - INTERVAL '5 minutes', ST_Point(21.0, 52.0), ['prg'], 3, 4);
             INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now() - INTERVAL '120 minutes', ST_Point(22.0, 53.0), ['bdot10k'], 1, 1);",
        )
        .unwrap();

        let features = recent_exports(&conn, 60).unwrap();
        assert_eq!(features.len(), 1);
        assert_eq!(features[0].properties.address_count, 3);
        assert_eq!(features[0].properties.building_count, 4);
    }

    #[test]
    fn recent_exports_orders_most_recent_first() {
        let conn = setup_db();
        conn.execute_batch(
            "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now() - INTERVAL '50 minutes', ST_Point(21.0, 52.0), ['prg'], 1, 0);
             INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now() - INTERVAL '5 minutes', ST_Point(21.0, 52.0), ['prg'], 2, 0);",
        )
        .unwrap();

        let features = recent_exports(&conn, 60).unwrap();
        assert_eq!(features.len(), 2);
        assert_eq!(features[0].properties.address_count, 2, "most recent first");
        assert_eq!(features[1].properties.address_count, 1);
    }

    #[test]
    fn recent_exports_feature_shape_is_correct() {
        let conn = setup_db();
        conn.execute(
            "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now(), ST_Point(21.0, 52.0), ['prg', 'egib'], 7, 2)",
            [],
        )
        .unwrap();

        let features = recent_exports(&conn, 60).unwrap();
        assert_eq!(features.len(), 1);
        let f = &features[0];
        assert_eq!(f.kind, "Feature");
        let geom_json: serde_json::Value = serde_json::from_str(f.geometry.get()).unwrap();
        assert_eq!(geom_json["type"], "Point");
        assert_eq!(f.properties.address_count, 7);
        assert_eq!(f.properties.building_count, 2);
        let datasets: Vec<String> = serde_json::from_str(f.properties.datasets.get()).unwrap();
        assert_eq!(datasets, vec!["prg", "egib"]);
        // exported_at is a UTC ISO-8601 string ending in Z.
        assert!(f.properties.exported_at.ends_with('Z'));
        assert_eq!(f.properties.exported_at.len(), 20); // "YYYY-MM-DDTHH:MM:SSZ"
    }

    #[test]
    fn recent_exports_empty_window_returns_empty_vec() {
        let conn = setup_db();
        assert_eq!(recent_exports(&conn, 60).unwrap().len(), 0);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --bin osmpbudynkiv2 server::updates`
Expected: compile errors — `cannot find function recent_exports`, `cannot find struct UpdateFeature`, etc.

- [ ] **Step 3: Implement the feature types and query function**

Add near the top of `src/server/updates.rs` (with a module doc comment already present), before the `#[cfg(test)]` block:

```rust
use anyhow::{Context, Result};
use duckdb::Connection;
use serde::Serialize;
use serde_json::value::RawValue;

#[derive(Serialize)]
pub struct UpdateProperties {
    pub exported_at: String,
    pub datasets: Box<RawValue>,
    pub address_count: i32,
    pub building_count: i32,
}

#[derive(Serialize)]
pub struct UpdateFeature {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub geometry: Box<RawValue>,
    pub properties: UpdateProperties,
}

#[derive(Serialize)]
pub struct UpdatesFeatureCollection {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub features: Vec<UpdateFeature>,
}

/// Recent package_exports rows within the last `minutes`, most recent first.
/// `minutes` is a validated positive integer (see `parse_minutes`), safe to
/// inline into the SQL text.
///
/// Runs on whatever connection it's given -- callers MUST pass a connection
/// derived from `state.write`, not `state.read_pool` (see the module doc
/// comment and docs/duckdb_connection_visibility_investigation.md).
pub fn recent_exports(conn: &Connection, minutes: u64) -> Result<Vec<UpdateFeature>> {
    let sql = format!(
        "SELECT ST_AsGeoJSON(area),
                strftime(exported_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ'),
                to_json(datasets), address_count, building_count
         FROM package_exports
         WHERE exported_at >= (now() - INTERVAL '{minutes} minutes')
         ORDER BY exported_at DESC"
    );
    let mut stmt = conn
        .prepare(&sql)
        .context("Failed to prepare /updates query")?;
    let rows = stmt
        .query_map([], |row| {
            let geometry_geojson: String = row.get(0)?;
            let exported_at: String = row.get(1)?;
            let datasets_json: String = row.get(2)?;
            let address_count: i32 = row.get(3)?;
            let building_count: i32 = row.get(4)?;
            Ok((geometry_geojson, exported_at, datasets_json, address_count, building_count))
        })
        .context("Failed to run /updates query")?;

    let mut out = Vec::new();
    for row in rows {
        let (geometry_geojson, exported_at, datasets_json, address_count, building_count) =
            row.context("Failed to read /updates row")?;
        out.push(UpdateFeature {
            kind: "Feature",
            geometry: RawValue::from_string(geometry_geojson)?,
            properties: UpdateProperties {
                exported_at,
                datasets: RawValue::from_string(datasets_json)?,
                address_count,
                building_count,
            },
        });
    }
    Ok(out)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --bin osmpbudynkiv2 server::updates`
Expected: all PASS (the 4 parsing tests plus the 4 new query/assembly tests).

- [ ] **Step 5: Write the failing handler tests**

Add to the `tests` module in `src/server/updates.rs`:

```rust
    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    use super::super::AppState;

    fn make_state_with_write(conn: duckdb::Connection) -> (AppState, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        // read_pool is unused by /updates but AppState requires it; a
        // throwaway file-backed read-only pool satisfies the type.
        let db_path = dir.path().join("updates_test.duckdb");
        let _ = duckdb::Connection::open(&db_path).unwrap();
        let read_cfg = duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
            .unwrap();
        let manager = duckdb::DuckdbConnectionManager::file_with_flags(&db_path, read_cfg).unwrap();
        let read_pool = r2d2::Pool::builder().max_size(1).build(manager).unwrap();

        let state = AppState {
            write: std::sync::Arc::new(std::sync::Mutex::new(conn)),
            read_pool,
            registry: std::sync::Arc::new(crate::server::jobs::JobRegistry::new_for_tests(vec![])),
            config: std::sync::Arc::new(crate::config::Config::default()),
        };
        (state, dir)
    }

    fn updates_app(state: AppState) -> Router {
        Router::new()
            .route("/updates", axum::routing::get(get_updates))
            .with_state(state)
    }

    #[tokio::test]
    async fn get_updates_returns_recent_exports_with_cache_header() {
        let conn = setup_db();
        conn.execute(
            "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now(), ST_Point(21.0, 52.0), ['prg'], 3, 1)",
            [],
        )
        .unwrap();
        let (state, _dir) = make_state_with_write(conn);

        let response = updates_app(state)
            .oneshot(Request::builder().uri("/updates").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/geo+json");
        assert_eq!(response.headers()["cache-control"], "public, max-age=60");
        assert!(
            response.headers().get("content-disposition").is_none(),
            "/updates is not a download, unlike /package"
        );

        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["type"], "FeatureCollection");
        let features = json["features"].as_array().unwrap();
        assert_eq!(features.len(), 1);
        assert_eq!(features[0]["properties"]["address_count"], 3);
        assert_eq!(features[0]["properties"]["datasets"][0], "prg");
    }

    #[tokio::test]
    async fn get_updates_respects_minutes_param() {
        let conn = setup_db();
        conn.execute_batch(
            "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now() - INTERVAL '5 minutes', ST_Point(21.0, 52.0), ['prg'], 1, 0);
             INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now() - INTERVAL '120 minutes', ST_Point(21.0, 52.0), ['prg'], 2, 0);",
        )
        .unwrap();
        let (state, _dir) = make_state_with_write(conn);
        let app = updates_app(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/updates?minutes=200")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["features"].as_array().unwrap().len(), 2);

        let response = app
            .oneshot(Request::builder().uri("/updates").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            json["features"].as_array().unwrap().len(),
            1,
            "default 60 minutes excludes the 120-minute-old row"
        );
    }

    #[tokio::test]
    async fn get_updates_rejects_over_cap_minutes() {
        let (state, _dir) = make_state_with_write(setup_db());
        let response = updates_app(state)
            .oneshot(
                Request::builder()
                    .uri("/updates?minutes=999999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn get_updates_empty_window_returns_empty_collection() {
        let (state, _dir) = make_state_with_write(setup_db());
        let response = updates_app(state)
            .oneshot(Request::builder().uri("/updates").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["features"].as_array().unwrap().len(), 0);
    }
```

- [ ] **Step 6: Run tests to verify they fail, then implement the handler**

Run: `cargo test --bin osmpbudynkiv2 server::updates`
Expected: compile error — `cannot find function get_updates`.

Add near the top of `src/server/updates.rs` (with the other `use` items):
```rust
use axum::extract::{Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use super::AppState;
```

Add below `recent_exports`:
```rust
#[derive(Debug, Deserialize)]
pub struct UpdatesParams {
    pub minutes: Option<String>,
}

pub async fn get_updates(
    State(state): State<AppState>,
    Query(params): Query<UpdatesParams>,
) -> Response {
    let minutes = match parse_minutes(
        params.minutes.as_deref(),
        state.config.updates.default_minutes,
        state.config.updates.max_minutes,
    ) {
        Ok(m) => m,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };

    let result = tokio::task::spawn_blocking(move || build_updates(&state, minutes)).await;
    match result {
        Ok(Ok(body)) => {
            let mut resp = body.into_response();
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/geo+json"),
            );
            resp.headers_mut().insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=60"),
            );
            resp
        }
        Ok(Err(e)) => {
            tracing::error!(error = %e, "updates query failed");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
        Err(e) => {
            tracing::error!(error = %e, "updates task panicked");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

fn build_updates(state: &AppState, minutes: u64) -> anyhow::Result<String> {
    let conn = state
        .write
        .lock()
        .map_err(|_| anyhow::anyhow!("write mutex poisoned"))?;
    let features = recent_exports(&conn, minutes)?;
    let collection = UpdatesFeatureCollection {
        kind: "FeatureCollection",
        features,
    };
    Ok(serde_json::to_string(&collection)?)
}

fn error_response(status: StatusCode, message: &str) -> Response {
    let body = serde_json::json!({ "error": message }).to_string();
    (
        status,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        body,
    )
        .into_response()
}
```

- [ ] **Step 7: Register the route**

In `src/server/mod.rs`, add after the `/package` route:
```rust
        .route("/updates", axum::routing::get(updates::get_updates))
```

- [ ] **Step 8: Run the full test suite**

Run: `cargo test`
Expected: everything PASSES, including all `server::updates` tests and every pre-existing test in the workspace.

- [ ] **Step 9: Commit**

```bash
cargo fmt && cargo clippy --all-targets
git add src/server/updates.rs src/server/mod.rs
git commit -m "feat: GET /updates endpoint serving recent package export activity

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Documentation updates

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`

**Interfaces:** none (docs only).

- [ ] **Step 1: Update the README roadmap**

In `README.md`, in `## Implemented`, add after the `/package` line:
```markdown
- [x] `GET /updates` — recent `/package` export activity as a GeoJSON `FeatureCollection`, browser-cacheable for 60 seconds (`?minutes=`, default 60, capped at 1440)
```

- [ ] **Step 2: Document the endpoint in the `run` section**

In `README.md`, extend the `### run — HTTP service` bullet list to add:
```markdown
- `/updates` — recent `/package` export activity (timestamp, area, datasets, feature counts) as GeoJSON, `Cache-Control: public, max-age=60`. A background job prunes entries older than `[jobs.export_log_prune] retention_days` (default 365).
```

And extend the example `curl` block:
```bash
# Recent export activity (default: last 60 minutes)
curl 'http://127.0.0.1:3000/updates'
curl 'http://127.0.0.1:3000/updates?minutes=1440'
```

In the `### Configuration` bullet list, add:
```markdown
- **`[updates]`** — `/updates` time window limits (`default_minutes`, `max_minutes`)
```

- [ ] **Step 3: Update CLAUDE.md's endpoint list**

In `CLAUDE.md`, replace:
```markdown
- `run` — HTTP service (`/health`, `/status`, `/tiles/{z}/{x}/{y}`, `/package` GeoJSON import packages) with background OSM updates
```
with:
```markdown
- `run` — HTTP service (`/health`, `/status`, `/tiles/{z}/{x}/{y}`, `/package` GeoJSON import packages, `/updates` recent export activity) with background OSM updates and export log pruning
```

- [ ] **Step 4: Commit**

```bash
git add README.md CLAUDE.md
git commit -m "docs: document GET /updates and export log pruning

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```
