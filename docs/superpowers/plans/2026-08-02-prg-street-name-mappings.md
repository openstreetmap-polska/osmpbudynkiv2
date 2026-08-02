# PRG Street Name Mappings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand abbreviated PRG street names (`gen. Kruka`) into the forms OSM Poland uses (`Generała Kruka`) in the `/package` GeoJSON, so downloaded data is importable without hand-editing in JOSM.

**Architecture:** A curated CSV is loaded into a `street_name_mappings` DuckDB table. `package.rs::unmatched_addresses` gains two `LEFT JOIN`s against it, resolved by a `COALESCE` chain that implements settlement-row → global-row → raw-name priority. Nothing else in the pipeline changes: matching never reads street names, so this cannot alter which addresses are unmatched.

**Tech Stack:** Rust, DuckDB (embedded), axum, clap, anyhow. CSV parsing goes through DuckDB's `read_csv` table function rather than a new crate dependency — it handles the quoting already present in the file (`"Generała Emila Fieldorfa ""Nila"""`) and accepts the path as a bound parameter.

## Global Constraints

- The mapping is **serving-time only**. Do not touch anything under `src/compare/` — `compare_addresses` joins on housenumber and a spatial grid key, never on street names.
- Lookup is **case-insensitive on trimmed values**: `lower(trim(...))` on both sides of every join. Never match exactly.
- An **empty or unpopulated `street_name_mappings` table must leave output unchanged** — raw PRG names, no error.
- `street_name_mappings` is created by `db.rs::create_schema`, so **no migration path is needed** for existing databases.
- Table columns are exactly `teryt_simc_code VARCHAR, prg_street_name VARCHAR, osm_street_name VARCHAR`. No derived key column — measured at 26.57 ms vs 27.07 ms per max-size package query, i.e. noise.
- `NULL`/empty `teryt_simc_code` means a global rule; a non-empty one scopes the row to that settlement.
- Do not alias a SQL table `glob` — it is a DuckDB operator and parses as a syntax error. Use `gl`.
- Transactions use the codebase idiom: `conn.execute_batch("BEGIN TRANSACTION")` / `"COMMIT"` / `"ROLLBACK"`, not a wrapper type.
- Run `cargo fmt` and `cargo clippy` before every commit.

**Branch first.** The repo is on `main` and none of the artifacts below are committed yet. Create a branch (e.g. `street-name-mappings`) before Task 1.

---

### Task 1: Commit the curated mapping file and pin its invariants

The CSV, the migration script and the spec already exist on disk, uncommitted. This task commits them and adds a test that stops the file from rotting.

**Files:**
- Commit (already on disk): `mappings/street_names_mappings.csv`, `scripts/migrate_legacy_street_mappings.py`, `docs/superpowers/specs/2026-08-01-prg-street-name-mappings-design.md`
- Create: `tests/street_mappings_file.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: the committed file at `mappings/street_names_mappings.csv`, relied on by Tasks 3–6.

- [ ] **Step 1: Confirm the artifacts are present and unmodified**

Run:
```bash
wc -l mappings/street_names_mappings.csv
head -1 mappings/street_names_mappings.csv
```
Expected: `3273` lines (3,272 rows + header) and header `teryt_simc_code,prg_street_name,osm_street_name`.

- [ ] **Step 2: Write the failing test**

Create `tests/street_mappings_file.rs`:

```rust
//! Structural invariants of the committed mapping file. The repo has no CI
//! workflow, so these run as part of `cargo test` instead.

use std::collections::HashSet;

const PATH: &str = "mappings/street_names_mappings.csv";

/// Split one CSV line into 3 fields, honouring `""`-escaped quoted fields.
fn split_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                cur.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => fields.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    fields.push(cur);
    fields
}

fn rows() -> Vec<Vec<String>> {
    let text = std::fs::read_to_string(PATH).expect("mapping file must exist");
    let mut lines = text.lines();
    let header = lines.next().expect("file must have a header");
    assert_eq!(
        header, "teryt_simc_code,prg_street_name,osm_street_name",
        "unexpected header"
    );
    lines.filter(|l| !l.is_empty()).map(split_csv_line).collect()
}

#[test]
fn every_row_has_three_fields_and_a_non_empty_mapping() {
    for (i, r) in rows().iter().enumerate() {
        assert_eq!(r.len(), 3, "row {} has {} fields", i + 2, r.len());
        assert!(!r[1].is_empty(), "row {} has empty prg_street_name", i + 2);
        assert!(!r[2].is_empty(), "row {} has empty osm_street_name", i + 2);
    }
}

#[test]
fn no_field_has_leading_or_trailing_whitespace() {
    for (i, r) in rows().iter().enumerate() {
        for (col, v) in r.iter().enumerate() {
            assert_eq!(v.trim(), v, "row {} col {} is not trimmed: {v:?}", i + 2, col);
        }
    }
}

#[test]
fn keys_are_unique_case_insensitively() {
    let mut seen = HashSet::new();
    for (i, r) in rows().iter().enumerate() {
        let key = (r[0].clone(), r[1].to_lowercase());
        assert!(seen.insert(key), "row {} duplicates an earlier key", i + 2);
    }
}

#[test]
fn file_is_sorted_for_stable_diffs() {
    let all = rows();
    let mut sorted = all.clone();
    sorted.sort_by_key(|r| (r[1].to_lowercase(), r[0].clone()));
    assert_eq!(all, sorted, "file must be sorted by (lower(name), simc)");
}

#[test]
fn a_known_global_and_a_known_settlement_row_are_present() {
    let all = rows();
    assert!(
        all.iter()
            .any(|r| r[0].is_empty() && r[1] == "Kościuszki" && r[2] == "Tadeusza Kościuszki"),
        "expected the global Kościuszki row"
    );
    assert!(
        all.iter().any(|r| r[0] == "0212529"
            && r[1] == "Kościuszki"
            && r[2] == "Generała Tadeusza Kościuszki"),
        "expected the Dobieszowice exception row"
    );
}
```

- [ ] **Step 3: Run the test to verify it passes against the committed file**

Run: `cargo test --test street_mappings_file`
Expected: 5 tests pass. If `file_is_sorted_for_stable_diffs` fails, the file was edited by hand out of order — re-sort rather than weakening the test.

- [ ] **Step 4: Commit**

```bash
git add mappings/street_names_mappings.csv scripts/migrate_legacy_street_mappings.py \
        docs/superpowers/specs/2026-08-01-prg-street-name-mappings-design.md \
        docs/superpowers/plans/2026-08-02-prg-street-name-mappings.md \
        tests/street_mappings_file.rs
git commit -m "feat(mappings): add curated PRG->OSM street name mapping file

3,272 rows migrated from the legacy gugik2osm file (12,221 rows), collapsed
to global rules where every settlement agreed, re-keyed onto current PRG
spellings, and spell-checked against OSM's street vocabulary."
```

---

### Task 2: Table schema and the serving join

The user-visible feature. After this task, rows inserted into `street_name_mappings` change what `/package` emits.

**Files:**
- Modify: `src/db.rs` — add the table to `create_schema`'s batch
- Modify: `src/server/package.rs` — the SQL in `unmatched_addresses`, and the `SEED` constant in the test module
- Modify: `CLAUDE.md` — new gotcha paragraph

**Interfaces:**
- Consumes: nothing.
- Produces: table `street_name_mappings(teryt_simc_code VARCHAR, prg_street_name VARCHAR, osm_street_name VARCHAR)`, populated by Task 3's loader. `unmatched_addresses` keeps its existing signature `(conn: &Connection, area: &RequestArea) -> Result<Vec<AddressRow>>` and `AddressRow` is unchanged — only the value of its `street` field changes.

- [ ] **Step 1: Write the failing tests**

Add to the `mod tests` block in `src/server/package.rs` (it already has `setup_db()`, which calls `init_db` and therefore gets the real schema):

```rust
/// Seed one unmatched address whose street is the abbreviated PRG form.
fn seed_abbreviated_address(conn: &duckdb::Connection) {
    conn.execute_batch(
        "INSERT INTO prg_unmatched
             (lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
              teryt_miejscowosc, geom, cell_x, cell_y, computed_at)
         VALUES ('a1', '12', 'gen. Kruka', 'Kock', '21-150', '0956069',
                 ST_Point(21.001, 52.201), 8000, 4900, now());",
    )
    .unwrap();
}

#[test]
fn street_is_returned_raw_when_no_mapping_is_loaded() {
    let conn = setup_db();
    seed_abbreviated_address(&conn);
    let rows = unmatched_addresses(&conn, &test_area()).unwrap();
    assert_eq!(rows[0].street.as_deref(), Some("gen. Kruka"));
}

#[test]
fn global_mapping_row_rewrites_the_street() {
    let conn = setup_db();
    seed_abbreviated_address(&conn);
    conn.execute_batch(
        "INSERT INTO street_name_mappings VALUES (NULL, 'gen. Kruka', 'Generała Kruka');",
    )
    .unwrap();
    let rows = unmatched_addresses(&conn, &test_area()).unwrap();
    assert_eq!(rows[0].street.as_deref(), Some("Generała Kruka"));
}

#[test]
fn settlement_mapping_row_beats_the_global_row() {
    let conn = setup_db();
    seed_abbreviated_address(&conn);
    conn.execute_batch(
        "INSERT INTO street_name_mappings VALUES
             (NULL, 'gen. Kruka', 'Generała Kruka'),
             ('0956069', 'gen. Kruka', 'Generała Michała Heydenreicha \"Kruka\"');",
    )
    .unwrap();
    let rows = unmatched_addresses(&conn, &test_area()).unwrap();
    assert_eq!(
        rows[0].street.as_deref(),
        Some("Generała Michała Heydenreicha \"Kruka\"")
    );
}

/// PRG has re-capitalised its leading tokens once already; an exact match
/// would silently stop rewriting instead of failing loudly.
#[test]
fn lookup_ignores_case_and_surrounding_whitespace() {
    let conn = setup_db();
    conn.execute_batch(
        "INSERT INTO prg_unmatched
             (lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
              teryt_miejscowosc, geom, cell_x, cell_y, computed_at)
         VALUES ('a1', '12', '  Gen. Kruka ', 'Kock', '21-150', '0956069',
                 ST_Point(21.001, 52.201), 8000, 4900, now());
         INSERT INTO street_name_mappings VALUES (NULL, 'gen. Kruka', 'Generała Kruka');",
    )
    .unwrap();
    let rows = unmatched_addresses(&conn, &test_area()).unwrap();
    assert_eq!(rows[0].street.as_deref(), Some("Generała Kruka"));
}

/// A settlement row must not leak into a different settlement.
#[test]
fn settlement_row_does_not_apply_to_another_settlement() {
    let conn = setup_db();
    seed_abbreviated_address(&conn);
    conn.execute_batch(
        "INSERT INTO street_name_mappings VALUES
             ('9999999', 'gen. Kruka', 'Generała Someone Else');",
    )
    .unwrap();
    let rows = unmatched_addresses(&conn, &test_area()).unwrap();
    assert_eq!(rows[0].street.as_deref(), Some("gen. Kruka"));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib server::package::tests 2>&1 | tail -30`
Expected: FAIL — `Table with name street_name_mappings does not exist`. `street_is_returned_raw_when_no_mapping_is_loaded` may pass already; the other four must fail.

- [ ] **Step 3: Add the table to the schema**

In `src/db.rs::create_schema`, add to the `execute_batch` string, just before the `match_dirty_cells` block:

```sql
        -- Curated PRG -> OSM street-name expansions, applied by
        -- server/package.rs when building addr:street. A row with a NULL
        -- teryt_simc_code is a global rule; a non-NULL one scopes the rule to
        -- that settlement and takes priority. Populated from
        -- mappings/street_names_mappings.csv; an empty table is a valid state
        -- and simply means names are served exactly as PRG publishes them.
        CREATE TABLE IF NOT EXISTS street_name_mappings (
            teryt_simc_code VARCHAR,
            prg_street_name VARCHAR,
            osm_street_name VARCHAR
        );
```

- [ ] **Step 4: Change the serving query**

In `src/server/package.rs::unmatched_addresses`, replace the `sql` binding with:

```rust
    let sql = format!(
        "SELECT ST_AsGeoJSON(a.geom), a.numer_porzadkowy,
                COALESCE(loc.osm_street_name, gl.osm_street_name, a.ulica),
                a.miejscowosc, a.kod_pocztowy, a.teryt_miejscowosc
         FROM prg_unmatched a
         LEFT JOIN street_name_mappings loc
                ON lower(trim(loc.prg_street_name)) = lower(trim(a.ulica))
               AND loc.teryt_simc_code = a.teryt_miejscowosc
         LEFT JOIN street_name_mappings gl
                ON lower(trim(gl.prg_street_name)) = lower(trim(a.ulica))
               AND gl.teryt_simc_code IS NULL
         WHERE ST_Intersects(a.geom, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
           AND ST_Intersects(a.geom, ST_GeomFromGeoJSON(?))"
    );
```

Add above the function, extending its existing doc comment:

```rust
/// `addr:street` is resolved through `street_name_mappings` here — the only
/// place PRG street names reach the outside world. The COALESCE chain *is* the
/// priority rule: settlement row, then global row, then the raw PRG name, so
/// an empty mapping table degrades to serving names verbatim rather than
/// erroring. Matching never reads street names (see compare::addresses), so
/// this cannot change which addresses are unmatched.
```

- [ ] **Step 5: Fix the axum-level test seed**

`make_seeded_state` opens a plain in-memory connection and builds its tables from the `SEED` constant rather than from `create_schema`, so it does **not** get the new table and every `/package` HTTP test will fail with "table does not exist". Add to the end of `SEED` in `src/server/package.rs`:

```sql
        CREATE TABLE street_name_mappings (
            teryt_simc_code VARCHAR,
            prg_street_name VARCHAR,
            osm_street_name VARCHAR);
```

- [ ] **Step 6: Add an HTTP-level test through the real handler**

The tests in Step 1 call `unmatched_addresses` directly. This one goes through the axum route so the rewrite is pinned in the actual GeoJSON response. Add to the same test module, after `make_seeded_state`/`package_app` are in scope:

```rust
#[tokio::test]
async fn package_response_serves_the_expanded_street_name() {
    let state = make_seeded_state();
    {
        let conn = state.pool.get().unwrap();
        conn.execute_batch(
            "UPDATE prg_unmatched SET ulica = 'gen. Kruka' WHERE lokalny_id = 'a1';
             INSERT INTO street_name_mappings VALUES (NULL, 'gen. Kruka', 'Generała Kruka');",
        )
        .unwrap();
    }
    let app = package_app(state);
    let res = app
        .oneshot(
            Request::builder()
                .uri("/package?bbox=21.0,52.2,21.01,52.21&datasets=prg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let feature = &json["features"][0];
    assert_eq!(feature["properties"]["addr:street"], "Generała Kruka");
}
```

The dataset selector is `datasets=prg` (not `addresses`) — see the existing tests at `src/server/package.rs:787`. `#[tokio::test]` matches the async test attribute already used in this module.

- [ ] **Step 7: Run the full test suite**

Run: `cargo test 2>&1 | tail -20`
Expected: all tests pass, including the six new ones and the pre-existing `/package` HTTP tests.

- [ ] **Step 8: Document the invariant in CLAUDE.md**

Add after the "serving tables store rows, not id references" gotcha:

```markdown
**Gotcha — street-name mapping is serving-time only.** `street_name_mappings`
(loaded from `mappings/street_names_mappings.csv`) is applied in exactly one
place: the `COALESCE(loc, gl, a.ulica)` chain in
`server::package::unmatched_addresses`. It rewrites `addr:street` on the way
out and nothing else — `compare::addresses` joins on housenumber and a grid
key and never reads street names, so a mapping change can never alter which
addresses are unmatched, and needs no `compare`, reconcile or drain. `/tiles`
is unaffected (its address layer emits no street). Lookup is
`lower(trim(...))` on both sides and priority is settlement row → global row
(NULL `teryt_simc_code`) → raw name, so an empty table serves PRG names
verbatim instead of failing. Note `make_seeded_state` in `server/package.rs`
builds its tables from a local `SEED` constant rather than `create_schema` —
a new serving table has to be added in both places.
```

- [ ] **Step 9: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings
git add src/db.rs src/server/package.rs CLAUDE.md
git commit -m "feat(package): expand PRG street names via street_name_mappings"
```

---

### Task 3: The loader module

**Files:**
- Create: `src/mappings.rs`
- Modify: `src/main.rs` — add `mod mappings;`

**Interfaces:**
- Consumes: the `street_name_mappings` table from Task 2.
- Produces:
  - `pub struct LoadStats { pub rows_loaded: usize, pub rows_absent_from_prg: i64 }`
  - `pub fn load_from_path(conn: &duckdb::Connection, path: &std::path::Path) -> anyhow::Result<LoadStats>`
  - `pub const MAPPINGS_TABLE: &str = "street_name_mappings";`

  Task 4 calls `load_from_path` and formats `LoadStats` into a `job_run_log` message.

- [ ] **Step 1: Write the failing tests**

Create `src/mappings.rs` containing only the test module for now:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::io::Write;
    use std::path::Path;

    fn setup_db() -> duckdb::Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    fn write_csv(body: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "teryt_simc_code,prg_street_name,osm_street_name\n{body}").unwrap();
        f.flush().unwrap();
        f
    }

    fn loaded(conn: &duckdb::Connection) -> Vec<(Option<String>, String, String)> {
        let mut stmt = conn
            .prepare(
                "SELECT teryt_simc_code, prg_street_name, osm_street_name
                 FROM street_name_mappings ORDER BY prg_street_name, teryt_simc_code",
            )
            .unwrap();
        let rows = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap();
        rows.map(|r| r.unwrap()).collect()
    }

    #[test]
    fn loads_global_and_settlement_rows() {
        let conn = setup_db();
        let f = write_csv(",gen. Kruka,Generała Kruka\n0956069,gen. Kruka,Generała Michała Kruka\n");
        let stats = load_from_path(&conn, f.path()).unwrap();
        assert_eq!(stats.rows_loaded, 2);
        let rows = loaded(&conn);
        assert_eq!(rows.len(), 2);
        // Empty SIMC must land as NULL, so the `IS NULL` join in package.rs matches it.
        assert!(rows.iter().any(|r| r.0.is_none()));
        assert!(rows.iter().any(|r| r.0.as_deref() == Some("0956069")));
    }

    #[test]
    fn quoted_fields_survive_parsing() {
        let conn = setup_db();
        let f = write_csv(",gen. Fieldorfa,\"Generała Emila Fieldorfa \"\"Nila\"\"\"\n");
        load_from_path(&conn, f.path()).unwrap();
        assert_eq!(loaded(&conn)[0].2, "Generała Emila Fieldorfa \"Nila\"");
    }

    #[test]
    fn reload_replaces_previous_contents() {
        let conn = setup_db();
        let first = write_csv(",A,Aaa\n");
        load_from_path(&conn, first.path()).unwrap();
        let second = write_csv(",B,Bbb\n");
        let stats = load_from_path(&conn, second.path()).unwrap();
        assert_eq!(stats.rows_loaded, 1);
        let rows = loaded(&conn);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, "B");
    }

    #[test]
    fn duplicate_key_rejects_the_load_and_leaves_the_table_untouched() {
        let conn = setup_db();
        let good = write_csv(",A,Aaa\n");
        load_from_path(&conn, good.path()).unwrap();
        // Same key differing only by case is still a duplicate.
        let bad = write_csv(",B,Bbb\n,b,Bbb2\n");
        let err = load_from_path(&conn, bad.path()).unwrap_err();
        assert!(
            format!("{err:#}").contains("duplicate"),
            "unexpected error: {err:#}"
        );
        let rows = loaded(&conn);
        assert_eq!(rows.len(), 1, "previous contents must survive a failed load");
        assert_eq!(rows[0].1, "A");
    }

    #[test]
    fn empty_name_rejects_the_load() {
        let conn = setup_db();
        let bad = write_csv(",,Aaa\n");
        let err = load_from_path(&conn, bad.path()).unwrap_err();
        assert!(format!("{err:#}").contains("empty"), "unexpected: {err:#}");
    }

    #[test]
    fn values_are_trimmed_on_load() {
        let conn = setup_db();
        let f = write_csv(",\"  gen. Kruka \",\" Generała Kruka \"\n");
        load_from_path(&conn, f.path()).unwrap();
        assert_eq!(loaded(&conn)[0].1, "gen. Kruka");
        assert_eq!(loaded(&conn)[0].2, "Generała Kruka");
    }

    /// `prg_addresses` is created by the PRG import, NOT by `create_schema`,
    /// so the test has to build it and the loader has to cope without it.
    #[test]
    fn counts_rows_whose_prg_name_is_absent_from_prg_addresses() {
        let conn = setup_db();
        conn.execute_batch(
            "CREATE TABLE prg_addresses (lokalny_id VARCHAR, ulica VARCHAR);
             INSERT INTO prg_addresses VALUES ('1', 'gen. Kruka');",
        )
        .unwrap();
        let f = write_csv(",gen. Kruka,Generała Kruka\n,gone. Street,Whatever Street\n");
        let stats = load_from_path(&conn, f.path()).unwrap();
        assert_eq!(stats.rows_loaded, 2);
        assert_eq!(stats.rows_absent_from_prg, 1);
    }

    /// Loading into a database that has never had PRG imported must succeed --
    /// `import street-mappings` may legitimately run first.
    #[test]
    fn load_succeeds_when_prg_addresses_does_not_exist() {
        let conn = setup_db();
        let f = write_csv(",gen. Kruka,Generała Kruka\n");
        let stats = load_from_path(&conn, f.path()).unwrap();
        assert_eq!(stats.rows_loaded, 1);
        assert_eq!(stats.rows_absent_from_prg, 0);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

First add `mod mappings;` to `src/main.rs` next to the other `mod` declarations.

Run: `cargo test --lib mappings 2>&1 | tail -20`
Expected: compile error — `load_from_path` and `LoadStats` not found.

- [ ] **Step 3: Write the implementation**

Put this above the test module in `src/mappings.rs`:

```rust
//! Loading the curated PRG -> OSM street-name mapping file into DuckDB.
//!
//! Parsing goes through DuckDB's `read_csv` rather than a Rust CSV crate: it
//! already handles the `""`-escaped quoting the file uses for nicknames, and
//! it keeps the dependency list unchanged.
//!
//! Validation is all-or-nothing. A bad file leaves the previous table intact
//! rather than half-replacing it, because serving a slightly stale mapping is
//! strictly better than serving a partial one.

use std::path::Path;

use anyhow::{Context, Result, bail};
use duckdb::Connection;
use tracing::{info, warn};

pub const MAPPINGS_TABLE: &str = "street_name_mappings";

const STAGING_TABLE: &str = "street_name_mappings__staging";

/// Outcome of one successful load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadStats {
    pub rows_loaded: usize,
    /// Rows whose `prg_street_name` appears nowhere in `prg_addresses.ulica`.
    /// Not an error -- the database may simply not have PRG imported yet --
    /// but a large count against a populated database means the file has
    /// drifted from what PRG currently publishes.
    pub rows_absent_from_prg: i64,
}

/// Replace the contents of `street_name_mappings` with the rows in `path`.
pub fn load_from_path(conn: &Connection, path: &Path) -> Result<LoadStats> {
    let path_str = path
        .to_str()
        .with_context(|| format!("mapping path is not valid UTF-8: {path:?}"))?;

    conn.execute_batch(&format!("DROP TABLE IF EXISTS {STAGING_TABLE}"))
        .context("Failed to drop stale mapping staging table")?;

    // read_csv accepts the path as a bound parameter, so no escaping needed.
    conn.execute(
        &format!(
            "CREATE TABLE {STAGING_TABLE} AS
             SELECT NULLIF(trim(teryt_simc_code), '') AS teryt_simc_code,
                    trim(prg_street_name) AS prg_street_name,
                    trim(osm_street_name) AS osm_street_name
             FROM read_csv(?, header = true, all_varchar = true)"
        ),
        duckdb::params![path_str],
    )
    .with_context(|| format!("Failed to read mapping CSV at {path_str}"))?;

    let result = validate_and_swap(conn, path_str);
    let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {STAGING_TABLE}"));
    result
}

fn validate_and_swap(conn: &Connection, path_str: &str) -> Result<LoadStats> {
    let empty: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM {STAGING_TABLE}
                 WHERE prg_street_name IS NULL OR prg_street_name = ''
                    OR osm_street_name IS NULL OR osm_street_name = ''"
            ),
            [],
            |r| r.get(0),
        )
        .context("Failed to check for empty names")?;
    if empty > 0 {
        bail!("{path_str}: {empty} row(s) have an empty street name; refusing to load");
    }

    let dupes: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM (
                     SELECT 1 FROM {STAGING_TABLE}
                     GROUP BY lower(prg_street_name), teryt_simc_code
                     HAVING COUNT(*) > 1)"
            ),
            [],
            |r| r.get(0),
        )
        .context("Failed to check for duplicate keys")?;
    if dupes > 0 {
        bail!(
            "{path_str}: {dupes} duplicate (street name, settlement) key(s); refusing to load. \
             Lookup is case-insensitive, so two rows differing only in case collide."
        );
    }

    let untrimmed: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM {STAGING_TABLE} s
                 JOIN read_csv(?, header = true, all_varchar = true) r
                   ON r.prg_street_name = s.prg_street_name
                 WHERE r.prg_street_name <> trim(r.prg_street_name)
                    OR r.osm_street_name <> trim(r.osm_street_name)"
            ),
            duckdb::params![path_str],
            |r| r.get(0),
        )
        .unwrap_or(0);
    if untrimmed > 0 {
        warn!(
            file = path_str,
            rows = untrimmed,
            "Mapping rows had surrounding whitespace; trimmed on load"
        );
    }

    let rows_loaded: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {STAGING_TABLE}"), [], |r| {
            r.get(0)
        })
        .context("Failed to count staged mapping rows")?;

    // `prg_addresses` is created by the PRG import, not by create_schema, so
    // it is legitimately absent on a database where `import street-mappings`
    // ran first. Treat the query failing as "no staleness signal available"
    // rather than as a load failure.
    let rows_absent_from_prg: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM {STAGING_TABLE} s
                 WHERE NOT EXISTS (
                     SELECT 1 FROM prg_addresses p
                     WHERE lower(trim(p.ulica)) = lower(s.prg_street_name))"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    conn.execute_batch("BEGIN TRANSACTION")
        .context("Failed to begin mapping swap")?;
    let swap = conn.execute_batch(&format!(
        "DELETE FROM {MAPPINGS_TABLE};
         INSERT INTO {MAPPINGS_TABLE} (teryt_simc_code, prg_street_name, osm_street_name)
         SELECT teryt_simc_code, prg_street_name, osm_street_name FROM {STAGING_TABLE};"
    ));
    match swap {
        Ok(()) => conn
            .execute_batch("COMMIT")
            .context("Failed to commit mapping swap")?,
        Err(e) => {
            if let Err(rb) = conn.execute_batch("ROLLBACK") {
                warn!(error = %rb, "Failed to roll back mapping swap");
            }
            return Err(e).context("Failed to replace mapping table contents");
        }
    }

    info!(
        rows = rows_loaded,
        absent_from_prg = rows_absent_from_prg,
        "Loaded street name mappings"
    );
    Ok(LoadStats {
        rows_loaded: rows_loaded as usize,
        rows_absent_from_prg,
    })
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib mappings 2>&1 | tail -20`
Expected: all 7 tests pass.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings
git add src/mappings.rs src/main.rs
git commit -m "feat(mappings): add transactional loader for the street name mapping file"
```

---

### Task 4: `import street-mappings` CLI subcommand

**Files:**
- Modify: `src/cli.rs` — new `ImportSource::StreetMappings` variant
- Modify: `src/import/mod.rs` — dispatch arm
- Create: `tests/cli_import_street_mappings.rs`

**Interfaces:**
- Consumes: `mappings::load_from_path`, `mappings::LoadStats` from Task 3.
- Produces: CLI `import street-mappings --file <PATH>`; a `job_run_log` row under `import:street-mappings`. Task 5 adds `--url` to the same variant.

- [ ] **Step 1: Write the failing integration test**

Create `tests/cli_import_street_mappings.rs`:

```rust
use std::io::Write;
use std::path::PathBuf;

use assert_cmd::Command;
use duckdb::Connection;

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("osmpbudynkiv2").unwrap();
    cmd.env("NO_COLOR", "1");
    cmd
}

fn persistent_config() -> (tempfile::NamedTempFile, tempfile::TempDir, tempfile::TempDir, PathBuf) {
    let db_dir = tempfile::TempDir::new().unwrap();
    let rocksdb_dir = tempfile::TempDir::new().unwrap();
    let db_path = db_dir.path().join("test.duckdb");
    let mut cfg = tempfile::NamedTempFile::new().unwrap();
    write!(
        cfg,
        "db_path = \"{}\"\nrocksdb_path = \"{}\"\n",
        db_path.display(),
        rocksdb_dir.path().display()
    )
    .unwrap();
    (cfg, db_dir, rocksdb_dir, db_path)
}

#[test]
fn imports_the_committed_mapping_file() {
    let (cfg, _db_dir, _rocks_dir, db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    cmd()
        .args([
            "--config",
            &cfg_path,
            "import",
            "street-mappings",
            "--file",
            "mappings/street_names_mappings.csv",
        ])
        .assert()
        .success();

    let conn = Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM street_name_mappings", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 3272);

    let globals: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM street_name_mappings WHERE teryt_simc_code IS NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(globals, 3244);

    let outcome: String = conn
        .query_row(
            "SELECT outcome FROM job_run_log WHERE job_name = 'import:street-mappings'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(outcome, "Success");
}

#[test]
fn a_bad_file_fails_the_command_and_records_the_error() {
    let (cfg, _db_dir, _rocks_dir, db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    let mut bad = tempfile::NamedTempFile::new().unwrap();
    write!(
        bad,
        "teryt_simc_code,prg_street_name,osm_street_name\n,A,Aaa\n,a,Bbb\n"
    )
    .unwrap();
    bad.flush().unwrap();

    cmd()
        .args([
            "--config",
            &cfg_path,
            "import",
            "street-mappings",
            "--file",
            bad.path().to_str().unwrap(),
        ])
        .assert()
        .failure();

    let conn = Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM street_name_mappings", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0, "a rejected file must leave the table empty");

    let outcome: String = conn
        .query_row(
            "SELECT outcome FROM job_run_log WHERE job_name = 'import:street-mappings'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(outcome, "Error");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --test cli_import_street_mappings 2>&1 | tail -20`
Expected: FAIL — clap rejects the unknown subcommand `street-mappings`.

- [ ] **Step 3: Add the CLI variant**

In `src/cli.rs`, add to `enum ImportSource` after the `Prg` variant:

```rust
    /// Import the curated PRG -> OSM street name mappings from CSV
    StreetMappings {
        /// Path to a local mapping CSV (skips download)
        #[arg(long)]
        file: Option<PathBuf>,
    },
```

- [ ] **Step 4: Add the dispatch arm**

In `src/import/mod.rs`, inside `run`'s `match source`, add:

```rust
        ImportSource::StreetMappings { file } => {
            let path = file.ok_or_else(|| {
                anyhow::anyhow!("import street-mappings requires --file")
            })?;
            let outcome = crate::mappings::load_from_path(conn, &path);
            match &outcome {
                Ok(stats) => {
                    let msg = format!(
                        "loaded {} mapping rows ({} not present in current PRG data)",
                        stats.rows_loaded, stats.rows_absent_from_prg
                    );
                    let _ = crate::job_log::record(
                        conn,
                        "import:street-mappings",
                        "Success",
                        Some(&msg),
                    );
                }
                Err(e) => {
                    let _ = crate::job_log::record(
                        conn,
                        "import:street-mappings",
                        "Error",
                        Some(&format!("{e:#}")),
                    );
                }
            }
            outcome.map(|_| ())
        }
```

Note this arm does **not** call `stamp_row_hash_version`: the mapping table has no `_row_hash` column and takes no part in the dataset diffing.

- [ ] **Step 5: Run the tests**

Run: `cargo test --test cli_import_street_mappings 2>&1 | tail -20`
Expected: both tests pass.

- [ ] **Step 6: Verify the whole suite still passes**

Run: `cargo test 2>&1 | tail -10`
Expected: all tests pass. `ImportSource::Full` is a separate variant and is deliberately not extended — mapping import is not part of a data import run.

- [ ] **Step 7: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings
git add src/cli.rs src/import/mod.rs tests/cli_import_street_mappings.rs
git commit -m "feat(cli): add 'import street-mappings' subcommand"
```

---

### Task 5: Download URL config and `--url`

**Files:**
- Modify: `src/config.rs` — `DownloadUrls.street_mappings` field + default
- Modify: `src/cli.rs` — `--url` on the `StreetMappings` variant
- Modify: `src/import/mod.rs` — download when no `--file` is given
- Modify: `example_config.toml` — document the new URL

**Interfaces:**
- Consumes: Task 4's `ImportSource::StreetMappings`, `download::download_file_as`.
- Produces: `config.download_urls.street_mappings: String`, read by Task 6's job.

- [ ] **Step 1: Write the failing test**

Add to `src/config.rs`'s test module:

```rust
#[test]
fn street_mappings_url_defaults_to_this_repo() {
    let cfg = Config::default();
    assert_eq!(
        cfg.download_urls.street_mappings,
        "https://raw.githubusercontent.com/openstreetmap-polska/osmpbudynkiv2/main/mappings/street_names_mappings.csv"
    );
}

#[test]
fn street_mappings_url_can_be_overridden() {
    let toml = r#"
[download_urls]
street_mappings = "https://example.test/m.csv"
"#;
    let cfg: Config = toml::from_str(toml).unwrap();
    assert_eq!(cfg.download_urls.street_mappings, "https://example.test/m.csv");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib config 2>&1 | tail -20`
Expected: FAIL — no field `street_mappings` on `DownloadUrls`.

- [ ] **Step 3: Add the config field**

In `src/config.rs`, add to `pub struct DownloadUrls`:

```rust
    pub street_mappings: String,
```

and to its `Default` impl:

```rust
            street_mappings:
                "https://raw.githubusercontent.com/openstreetmap-polska/osmpbudynkiv2/main/mappings/street_names_mappings.csv"
                    .to_string(),
```

- [ ] **Step 4: Run the config tests**

Run: `cargo test --lib config 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Add `--url` and the download path**

In `src/cli.rs`, extend the `StreetMappings` variant:

```rust
    /// Import the curated PRG -> OSM street name mappings from CSV
    StreetMappings {
        /// Path to a local mapping CSV (skips download)
        #[arg(long)]
        file: Option<PathBuf>,
        /// Override the configured download URL
        #[arg(long)]
        url: Option<String>,
    },
```

In `src/import/mod.rs`, replace the `path` binding in the dispatch arm from Task 4 with:

```rust
        ImportSource::StreetMappings { file, url } => {
            let downloaded;
            let path = match file {
                Some(p) => p,
                None => {
                    let src = url.as_deref().unwrap_or(&urls.street_mappings);
                    downloaded = crate::download::download_file_as(
                        src,
                        &config.download_dir(),
                        "street_names_mappings.csv",
                    )?;
                    downloaded
                }
            };
            // ... unchanged body from Task 4, using `&path`
```

`config.download_dir()` is the existing helper every importer uses (see
`import/bdot10k.rs:53`, `import/prg.rs:130`). Do not introduce a second way of
resolving the download directory.

- [ ] **Step 6: Verify the CLI still imports from a file**

Run: `cargo test --test cli_import_street_mappings 2>&1 | tail -20`
Expected: PASS — `--file` still short-circuits before any download.

- [ ] **Step 7: Document in example_config.toml**

Under `[download_urls]`, add:

```toml
# Curated PRG -> OSM street name mappings (see mappings/street_names_mappings.csv).
# Used by `import street-mappings` when no --file is given, and by the
# street_mappings_update job. The mapping only affects addr:street in /package
# output; it never changes which addresses are considered unmatched.
street_mappings = "https://raw.githubusercontent.com/openstreetmap-polska/osmpbudynkiv2/main/mappings/street_names_mappings.csv"
```

- [ ] **Step 8: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings
git add src/config.rs src/cli.rs src/import/mod.rs example_config.toml
git commit -m "feat(config): add street_mappings download URL and --url override"
```

---

### Task 6: Background refresh job

**Files:**
- Create: `src/server/jobs/street_mappings_update.rs`
- Modify: `src/server/jobs/mod.rs` — `pub mod street_mappings_update;`
- Modify: `src/server/mod.rs` — register the job
- Modify: `src/config.rs` — `JobsConfig.street_mappings_update`
- Modify: `example_config.toml`

**Interfaces:**
- Consumes: `Job` / `JobContext` from `server::jobs`, `mappings::load_from_path`, `download::fetch_etag`, `config.download_urls.street_mappings`.
- Produces: `pub struct StreetMappingsUpdateJob;` with `StreetMappingsUpdateJob::new() -> Self`, registered under the name `"street_mappings_update"`.

- [ ] **Step 1: Write the failing test**

Create `src/server/jobs/street_mappings_update.rs` with only:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::jobs::Job;

    #[test]
    fn job_is_named_for_its_config_key() {
        assert_eq!(StreetMappingsUpdateJob::new().name(), "street_mappings_update");
    }
}
```

Add `pub mod street_mappings_update;` to `src/server/jobs/mod.rs`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib street_mappings_update 2>&1 | tail -20`
Expected: compile error — `StreetMappingsUpdateJob` not found.

- [ ] **Step 3: Implement the job**

Above the test module in `src/server/jobs/street_mappings_update.rs`:

```rust
//! Periodic refresh of the curated street-name mapping file.
//!
//! Unlike the dataset refreshes this touches no geometry and enqueues no
//! dirty cells: the mapping is applied at serving time, so a new file takes
//! effect on the next `/package` request with no recompute. The last seen
//! ETag lives in `metadata` rather than `dataset_refreshes`, whose columns
//! exist for snapshot diffing that does not apply here.

use anyhow::{Context, Result};
use tracing::info;

use crate::server::jobs::{Job, JobContext};

const ETAG_KEY: &str = "street_mappings_etag";

pub struct StreetMappingsUpdateJob;

impl StreetMappingsUpdateJob {
    pub fn new() -> Self {
        Self
    }
}

impl Default for StreetMappingsUpdateJob {
    fn default() -> Self {
        Self::new()
    }
}

impl Job for StreetMappingsUpdateJob {
    fn name(&self) -> &'static str {
        "street_mappings_update"
    }

    fn run(&self, ctx: &JobContext) -> Result<()> {
        let conn = ctx.pool.get().context("failed to acquire pool connection")?;
        let url = &ctx.config.download_urls.street_mappings;

        let etag = crate::download::fetch_etag(url).unwrap_or(None);
        if let Some(current) = &etag {
            let previous: Option<String> = conn
                .query_row(
                    "SELECT value FROM metadata WHERE key = ?",
                    duckdb::params![ETAG_KEY],
                    |r| r.get(0),
                )
                .ok();
            if previous.as_ref() == Some(current) {
                info!(url, "Street mappings unchanged (ETag match), skipping");
                return Ok(());
            }
        }

        let path = crate::download::download_file_as(
            url,
            &ctx.config.download_dir(),
            "street_names_mappings.csv",
        )?;
        let stats = crate::mappings::load_from_path(&conn, &path)?;

        if let Some(current) = etag {
            conn.execute(
                "DELETE FROM metadata WHERE key = ?",
                duckdb::params![ETAG_KEY],
            )?;
            conn.execute(
                "INSERT INTO metadata (key, value) VALUES (?, ?)",
                duckdb::params![ETAG_KEY, current],
            )?;
        }

        let msg = format!(
            "loaded {} mapping rows ({} not present in current PRG data)",
            stats.rows_loaded, stats.rows_absent_from_prg
        );
        let _ = crate::job_log::record(&conn, "update:street-mappings", "Success", Some(&msg));
        Ok(())
    }
}
```

Note the caveat `example_config.toml` documents for `cleanup_downloaded_files`: `download_file_as` **skips the download when the destination already exists**. Because this job always writes to the same filename, a leftover file would be reused forever and the job would silently stop picking up new mappings. Delete the downloaded file after a successful load when `ctx.config.cleanup_downloaded_files` is true, and log at `warn` if it is false, exactly as the dataset refreshes do.

- [ ] **Step 4: Run the test**

Run: `cargo test --lib street_mappings_update 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Add the job config**

In `src/config.rs`, add to `pub struct JobsConfig`:

```rust
    pub street_mappings_update: JobConfig,
```

and to its `Default` impl:

```rust
            street_mappings_update: JobConfig {
                enabled: false,
                interval_seconds: 86400,
                timeout_seconds: 300,
            },
```

Add a config test:

```rust
#[test]
fn street_mappings_job_is_disabled_by_default() {
    let cfg = Config::default();
    assert!(!cfg.jobs.street_mappings_update.enabled);
    assert_eq!(cfg.jobs.street_mappings_update.interval_seconds, 86400);
}
```

- [ ] **Step 6: Register the job**

In `src/server/mod.rs`, alongside the existing `Arc::new(jobs::...)` registrations around line 112–135, add:

```rust
            Arc::new(jobs::street_mappings_update::StreetMappingsUpdateJob::new()),
```

wired to `config.jobs.street_mappings_update` exactly as the neighbouring jobs wire theirs.

- [ ] **Step 7: Document in example_config.toml**

```toml
# Re-downloads the curated street-name mapping file and reloads it. A HEAD
# request compares the ETag against the last load, so a daily poll costs one
# round-trip when the file has not changed.
#
# Disabled by default: the mapping ships in the binary's repository and most
# deployments will load it once via `import street-mappings`. Enable this only
# if you want a running instance to pick up mapping PRs without a redeploy.
#
# Unlike the dataset refreshes this enqueues no dirty cells and triggers no
# recompute -- the mapping is applied when /package builds its response.
[jobs.street_mappings_update]
enabled = false
interval_seconds = 86400
timeout_seconds = 300
```

- [ ] **Step 8: Run the full suite**

Run: `cargo test 2>&1 | tail -15`
Expected: all tests pass.

- [ ] **Step 9: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings
git add src/server/jobs/street_mappings_update.rs src/server/jobs/mod.rs \
        src/server/mod.rs src/config.rs example_config.toml
git commit -m "feat(jobs): add street_mappings_update background refresh"
```

---

### Task 7: End-to-end verification against real data

No new production code. This confirms the feature works on the Poland database rather than only on fixtures.

**Files:**
- Modify: `README.md` — a short section on the mapping file

- [ ] **Step 1: Load the mapping into the real database**

Run:
```bash
cargo run --release -- import street-mappings --file mappings/street_names_mappings.csv
```
Expected: log line `Loaded street name mappings rows=3272 absent_from_prg=0`.

- [ ] **Step 2: Confirm the rewrite happens on real rows**

Run:
```bash
duckdb osmpbudynkiv2.duckdb -readonly -box -c "
SELECT a.ulica AS prg, COALESCE(loc.osm_street_name, gl.osm_street_name, a.ulica) AS served
FROM prg_unmatched a
LEFT JOIN street_name_mappings loc
       ON lower(trim(loc.prg_street_name)) = lower(trim(a.ulica))
      AND loc.teryt_simc_code = a.teryt_miejscowosc
LEFT JOIN street_name_mappings gl
       ON lower(trim(gl.prg_street_name)) = lower(trim(a.ulica))
      AND gl.teryt_simc_code IS NULL
WHERE COALESCE(loc.osm_street_name, gl.osm_street_name) IS NOT NULL
LIMIT 10;"
```
Expected: ten rows where `served` is the expanded form of `prg`.

- [ ] **Step 3: Confirm total coverage matches the spec**

Run the same query with `SELECT COUNT(*)` and no `LIMIT`.
Expected: **15,675**. A materially different number means the file or the join drifted from what the spec measured — investigate before continuing.

- [ ] **Step 4: Confirm /package serves expanded names**

Start the server (`cargo run --release -- run`), then:
```bash
curl -s 'http://127.0.0.1:3000/package?bbox=19.9,50.0,19.95,50.05&datasets=prg' \
  | grep -o '"addr:street":"[^"]*"' | sort -u | head
```
Expected: expanded forms (`Świętego …`, `Generała …`), no `św.`/`gen.` prefixes among mapped streets.

- [ ] **Step 5: Document in README.md**

Add a short section:

```markdown
### Street name mappings

PRG publishes abbreviated street names (`gen. Kruka`); OSM Poland uses expanded
ones (`Generała Kruka`). `mappings/street_names_mappings.csv` maps between them
and is applied to `addr:street` when `/package` builds its response, so
downloaded data is importable without hand-editing.

Load it with:

    cargo run -- import street-mappings --file mappings/street_names_mappings.csv

A row with an empty `teryt_simc_code` applies nationwide; one with a code
applies only to that settlement and overrides the nationwide row. Lookup is
case-insensitive. The file is optional — without it, names are served exactly
as PRG publishes them.

To propose a change, edit the CSV and open a PR; `cargo test --test
street_mappings_file` checks its structure.
```

- [ ] **Step 6: Commit**

```bash
git add README.md
git commit -m "docs: document the street name mapping file"
```

---

## Open questions for the plan's owner

1. **Committing to `main`.** Task 1 assumes a feature branch. Confirm the branch name before starting.
2. **The 83 prefix-artefact rows.** The spec records these as a known gap (`osiedle Os. Modrzewiowe` → PRG now `Os. Modrzewiowe`, target `Osiedle Modrzewiowe`). Recovering them means one more normalisation rule in the migration script and a regenerated CSV — worth ~83 rows of extra coverage. Not in this plan; decide whether it happens before or after these tasks.
