# Export Log & /updates Endpoint Design

## Summary

Extend the `/package` endpoint to log every export (timestamp, requested area, dataset selection, and feature counts) into a new `package_exports` table. Add a `GET /updates` endpoint that serves recent export log entries as a GeoJSON `FeatureCollection`, browser-cacheable for 60 seconds, so a map client can show "what areas were recently reviewed/exported" (per `docs/project_ideas.md`'s "geojson with info about latest updated areas"). A background job prunes log entries older than a configurable retention period, following the existing `Job`/`Scheduler` pattern used by `OsmUpdateJob`.

## Verified DuckDB behavior (bundled v1.5.2, via the Rust `duckdb` crate)

These were confirmed by direct testing against the project's own bundled DuckDB build before finalizing the schema, since they materially affect it:

- **`GEOMETRY('epsg:4326')` is valid** — a built-in parameterized type since DuckDB v1.5. It round-trips through `ST_GeomFromGeoJSON`/`ST_AsGeoJSON` and is interchangeable with plain `GEOMETRY` columns in spatial predicates (e.g. `ST_Intersects` between a plain-`GEOMETRY` table and a `GEOMETRY('epsg:4326')` table works with no cast).
- **`TIMESTAMP WITH TIME ZONE` arithmetic requires the `icu` extension loaded.** Without `LOAD icu`, `now() - INTERVAL '60 minutes'` fails to bind (`Binder Error: No function matches the given name and argument types '-(TIMESTAMP WITH TIME ZONE, INTERVAL)'`). `duckdb_init_commands` must add `INSTALL icu; LOAD icu;` (same runtime-install pattern already used for `spatial`).
- **Formatting a `TIMESTAMPTZ` to a UTC ISO-8601 string requires an explicit `AT TIME ZONE 'UTC'` conversion.** `strftime(exported_at, '%Y-%m-%dT%H:%M:%SZ')` silently formats in the session's local timezone (verified: on a UTC+2 host it returned local wall-clock time with a `Z` suffix incorrectly implying UTC). The correct form is `strftime(exported_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ')`.
- **`LIST` types (`VARCHAR[]`) cannot be bound as query parameters** in this version of the Rust `duckdb` crate — binding a `duckdb::types::Value::List` panics with "not implemented" (`value_ref.rs`). Consequently:
  - Writing a list column requires inlining a SQL list literal (e.g. `['prg','bdot10k']`) directly into the statement text. This is safe here because the values are drawn from the closed 3-variant `Dataset` enum, never raw request text — the same trust boundary already used for inlining `source_table` names in `src/compare/buildings.rs`.
  - Reading a list column back works via SQL-side `to_json(column)`, which returns a ready JSON-array string (e.g. `["prg","bdot10k"]`) that can be embedded directly as a `RawValue`, exactly like the `/package` endpoint already embeds `ST_AsGeoJSON` output.
- **`AppState.read_pool` never observes writes made by `AppState.write` while the server keeps running** — a separately-opened, long-lived `Connection` in DuckDB does not see another connection's commits, not even after `CHECKPOINT`; see `docs/duckdb_connection_visibility_investigation.md` for the full investigation and root cause. This is a pre-existing issue (it already affects `/tiles` and `/package`'s reads of OSM data) and is **out of scope to fix here**. The only consequence for this feature: `/updates` cannot use `read_pool` — see below.

## Data model: `package_exports` table

Added to `src/db.rs::create_schema()` alongside the existing tables (idempotent `CREATE TABLE IF NOT EXISTS`, no migration machinery needed since the whole schema is recreated this way):

```sql
CREATE TABLE IF NOT EXISTS package_exports (
    exported_at TIMESTAMP WITH TIME ZONE,
    area GEOMETRY('epsg:4326'),
    datasets VARCHAR[],
    address_count INTEGER,
    building_count INTEGER
);
```

- `exported_at` — set via SQL `now()` at insert time; no Rust-side clock or timezone handling.
- `area` — the exact requested geometry. For a GET bbox request this is the envelope-as-Polygon; for a POST request it's the submitted polygon/multipolygon. Both cases are already unified by the existing `RequestArea.polygon_geojson` field (built by `RequestArea::from_envelope` for GET, and directly from the parsed body for POST) — no GET/POST branching needed in the logging code.
- `datasets` — the requested datasets (not necessarily all datasets that yielded features — the full requested selection), as a `VARCHAR[]`.
- `address_count` — number of PRG-address features included in that export's response.
- `building_count` — number of building features (BDOT10k + EGIB combined) included in that export's response.

No surrogate key, no index — the table is only ever inserted into, range-scanned by `exported_at`, and pruned by age.

## Write path: logging every `/package` call

`build_package` (in `src/server/package.rs`) already receives `&AppState`, which already carries `write: Arc<Mutex<Connection>>` (added specifically for a future write-path handler in the prior feature). While assembling the `FeatureCollection`, it tracks how many features came from `Dataset::Prg` (`address_count`) vs `Dataset::Bdot10k`/`Dataset::Egib` combined (`building_count`). After assembly:

```rust
let datasets_sql = format!(
    "[{}]",
    datasets.iter().map(|d| format!("'{}'", d.sql_name())).collect::<Vec<_>>().join(",")
);
match state.write.lock() {
    Ok(conn) => {
        let sql = format!(
            "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now(), ST_GeomFromGeoJSON(?), {datasets_sql}, ?, ?)"
        );
        if let Err(e) = conn.execute(&sql, duckdb::params![area.polygon_geojson, address_count, building_count]) {
            tracing::warn!(error = %e, "failed to log package export");
        }
    }
    Err(_) => tracing::warn!("write mutex poisoned, skipping package export log"),
}
```

(`Dataset::sql_name()` is a small new helper returning `"prg"`/`"bdot10k"`/`"egib"` — the same strings `parse_datasets` already parses, kept as a single source of truth.)

This runs for **every** `/package` call — GET and POST, matched or unmatched results, even zero-feature responses. Logging failures (a DB error, or a poisoned write mutex) are caught, logged via `tracing::warn!`, and never turn a successful export into a failed response. This happens inside the same `spawn_blocking` closure that already does the read queries, so briefly locking the `std::sync::Mutex` there doesn't block the async runtime.

## Retention: `export_log_prune` background job

New job following the exact pattern of `OsmUpdateJob` (`src/server/jobs/osm_update.rs`) — reads everything from `ctx.config`, no per-job struct fields:

```rust
// src/server/jobs/export_log_prune.rs
pub struct ExportLogPruneJob;
impl Job for ExportLogPruneJob {
    fn name(&self) -> &'static str { "export_log_prune" }
    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx.write.lock().expect("write mutex poisoned");
        let days = ctx.config.jobs.export_log_prune.retention_days;
        conn.execute(
            &format!("DELETE FROM package_exports WHERE exported_at < (now() - INTERVAL '{days} days')"),
            [],
        )?;
        Ok(())
    }
}
```

Registered in `server/mod.rs`'s job list alongside `OsmUpdateJob`. New config struct (not reusing the generic `JobConfig`, since this job needs an extra field):

```rust
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct ExportLogPruneConfig {
    pub enabled: bool,          // default true
    pub interval_seconds: u64,  // default 86400 (daily — the log grows slowly, no need to prune more often)
    pub timeout_seconds: u64,   // default 60
    pub retention_days: u64,    // default 365 (about a year)
}
```

`JobsConfig` gains `pub export_log_prune: ExportLogPruneConfig`. The scheduling `JobConfigResolved` (enabled/interval/timeout) is constructed directly from these three fields in `server/mod.rs` — no new `From` impl needed, since `JobConfigResolved`'s fields are already `pub`.

## `GET /updates` endpoint

- **Query param:** `?minutes=N`, optional. Default `config.updates.default_minutes` (60). Values above `config.updates.max_minutes` (1440 = 24h) are rejected with `400`; non-positive or non-integer `minutes` is also `400`.
- **New config section:**
  ```toml
  [updates]
  default_minutes = 60
  max_minutes = 1440
  ```
- **Query runs via `state.write` (locked briefly), not `read_pool`.** Because `read_pool` never observes writes made through `write` while the server is running (see the DuckDB findings above), reading `package_exports` through it would always return empty. `/updates` instead locks `state.write`, runs the `SELECT`, and releases the lock — same connection the export-log insert and the retention job already use. This is a narrow, scoped workaround for this one endpoint; it does not touch `read_pool` or fix `/tiles`/`/package`'s pre-existing staleness.
  ```sql
  SELECT ST_AsGeoJSON(area),
         strftime(exported_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ'),
         to_json(datasets), address_count, building_count
  FROM package_exports
  WHERE exported_at >= (now() - INTERVAL '{minutes} minutes')
  ORDER BY exported_at DESC
  ```
- **Response:** `FeatureCollection`, one Feature per export-log row. `geometry` is the stored area (`RawValue`, same technique as `/package`). `properties` = `{ exported_at: string, datasets: [...] (RawValue from to_json), address_count: number, building_count: number }`. Since properties here are mixed types (not all strings like `/package`'s `BTreeMap<String, String>`), this needs its own small properties-building path — a `serde_json::Map<String, serde_json::Value>` populated with a mix of `Value::String`, raw-JSON-embedded array (via `RawValue`), and `Value::Number`.
- **Headers:** `Content-Type: application/geo+json`, `Cache-Control: public, max-age=60`. Unlike `/package`, **no** `Content-Disposition: attachment` — this endpoint is meant to be fetched by a map/script for display, not downloaded as a file.
- Empty window → `200` with empty `features` (consistent with `/package`).

## Testing

- Unit tests: `?minutes=` parsing/validation (default, override, cap rejection, non-numeric, non-positive), `Dataset::sql_name()` round-trips against `parse_datasets`.
- DB-level tests: insert `package_exports` rows at various ages directly, confirm the `/updates` time-window filter includes/excludes correctly at the boundary; confirm `ExportLogPruneJob` deletes rows older than `retention_days` and keeps newer ones.
- Handler tests (tower `oneshot`, same pattern as `/package`): a `/package` call followed by a `/updates` call shows the logged export with correct `datasets`/counts/geometry; `Cache-Control` header present and correct; over-cap `minutes` → 400; empty window → empty collection; a `/package` call that finds zero features is still logged and appears in `/updates`.

## Out of scope

Spatial filtering on `/updates` (e.g. `?bbox=`), deduplicating/merging overlapping export areas, OSM changeset linkage (dropped from this design — may return as a separate future feature), exposing requester identity (no auth/identity in this app).
