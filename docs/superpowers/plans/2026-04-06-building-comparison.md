# Building Comparison Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Compare government building datasets (BDOT10k, EGIB) against OSM buildings using centroid containment and store results for map rendering.

**Architecture:** Pure SQL comparison via DuckDB lateral joins. A gov building is "matched" if its centroid falls inside any OSM building polygon. Two independent comparisons (BDOT10k vs OSM, EGIB vs OSM) with results in separate tables. New `compare buildings` CLI subcommand.

**Tech Stack:** Rust, DuckDB (spatial extension, RTREE indexes), clap (CLI)

**Spec:** `docs/superpowers/specs/2026-04-06-building-comparison-design.md`

---

## File Structure

| Action | Path | Responsibility |
|--------|------|----------------|
| Modify | `src/cli.rs` | Add `Compare` command variant, `CompareTarget` and `BuildingsSource` enums |
| Modify | `src/main.rs` | Add `mod compare` and `Command::Compare` dispatch |
| Create | `src/compare/mod.rs` | Route compare subcommands to building comparison functions |
| Create | `src/compare/buildings.rs` | BDOT10k and EGIB comparison SQL execution + stats logging |
| Create | `tests/cli_compare_buildings.rs` | Integration tests for compare CLI |

---

### Task 1: CLI types, module skeleton, and failing integration test

**Files:**
- Create: `tests/cli_compare_buildings.rs`
- Modify: `src/cli.rs:19-33`
- Create: `src/compare/mod.rs`
- Create: `src/compare/buildings.rs`
- Modify: `src/main.rs:1,44-54`

- [ ] **Step 1: Write the failing integration test**

Create `tests/cli_compare_buildings.rs`:

```rust
use assert_cmd::Command;
use predicates::prelude::*;

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("osmpbudynkiv2").unwrap();
    cmd.env("NO_COLOR", "1");
    cmd
}

fn persistent_config() -> (tempfile::NamedTempFile, tempfile::TempDir, tempfile::TempDir) {
    let db_dir = tempfile::TempDir::new().unwrap();
    let rocksdb_dir = tempfile::TempDir::new().unwrap();
    let db_path = db_dir.path().join("test.duckdb");
    let mut cfg = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    write!(
        cfg,
        "db_path = \"{}\"\nrocksdb_path = \"{}\"\n",
        db_path.display(),
        rocksdb_dir.path().display()
    )
    .unwrap();
    (cfg, db_dir, rocksdb_dir)
}

fn import_all(cfg_path: &str) {
    cmd()
        .args(["--config", cfg_path, "import", "osm", "--file", "fixtures/osm.pbf"])
        .assert()
        .success();
    cmd()
        .args(["--config", cfg_path, "import", "bdot10k", "--file", "fixtures/bdot10k.parquet"])
        .assert()
        .success();
    cmd()
        .args(["--config", cfg_path, "import", "egib", "--file", "fixtures/egib.parquet"])
        .assert()
        .success();
}

#[test]
fn test_compare_buildings_both() {
    let (cfg, _db_dir, _rocksdb_dir) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_all(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("BDOT10k comparison complete")
                .and(predicate::str::contains("total=74"))
                .and(predicate::str::contains("EGIB comparison complete")),
        );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_compare_buildings_both -- --nocapture 2>&1 | tail -20`
Expected: compilation error — `compare` is not a recognized subcommand.

- [ ] **Step 3: Add CLI types to cli.rs**

In `src/cli.rs`, add the `Compare` variant to the `Command` enum and the new enums:

```rust
#[derive(Subcommand)]
pub enum Command {
    /// Import data from various sources
    Import {
        #[command(subcommand)]
        source: ImportSource,
    },
    /// Update data from various sources
    Update {
        #[command(subcommand)]
        source: UpdateSource,
    },
    /// Compare government data against OSM
    Compare {
        #[command(subcommand)]
        target: CompareTarget,
    },
    /// Run HTTP service with background data updates
    Run,
}

#[derive(Subcommand)]
pub enum CompareTarget {
    /// Compare building datasets against OSM buildings
    Buildings {
        #[command(subcommand)]
        source: Option<BuildingsSource>,
    },
}

#[derive(Subcommand)]
pub enum BuildingsSource {
    /// Compare only BDOT10k buildings against OSM
    Bdot10k,
    /// Compare only EGIB buildings against OSM
    Egib,
}
```

- [ ] **Step 4: Create compare module skeleton**

Create `src/compare/mod.rs`:

```rust
pub mod buildings;

use anyhow::Result;
use duckdb::Connection;

use crate::cli::{BuildingsSource, CompareTarget};

pub fn run(conn: &Connection, target: CompareTarget) -> Result<()> {
    match target {
        CompareTarget::Buildings { source } => match source {
            None => {
                buildings::compare_bdot10k(conn)?;
                buildings::compare_egib(conn)?;
            }
            Some(BuildingsSource::Bdot10k) => buildings::compare_bdot10k(conn)?,
            Some(BuildingsSource::Egib) => buildings::compare_egib(conn)?,
        },
    }
    Ok(())
}
```

Create `src/compare/buildings.rs`:

```rust
use anyhow::Result;
use duckdb::Connection;

pub fn compare_bdot10k(conn: &Connection) -> Result<()> {
    let _ = conn;
    anyhow::bail!("BDOT10k comparison not yet implemented")
}

pub fn compare_egib(conn: &Connection) -> Result<()> {
    let _ = conn;
    anyhow::bail!("EGIB comparison not yet implemented")
}
```

- [ ] **Step 5: Wire compare into main.rs**

Add `mod compare;` to the module declarations in `src/main.rs` and add the match arm:

```rust
mod compare;
```

In the `match cli.command` block, add:

```rust
Command::Compare { target } => compare::run(&conn, target)?,
```

- [ ] **Step 6: Run test to verify it fails with "not yet implemented"**

Run: `cargo test test_compare_buildings_both -- --nocapture 2>&1 | tail -20`
Expected: FAIL — the compare command runs but exits with error "BDOT10k comparison not yet implemented".

- [ ] **Step 7: Commit**

```bash
git add src/cli.rs src/main.rs src/compare/mod.rs src/compare/buildings.rs tests/cli_compare_buildings.rs
git commit -m "feat: add compare buildings CLI skeleton

Adds Compare command with Buildings subcommand and optional
bdot10k/egib source filter. Comparison functions are stubs
that will be implemented next.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 2: Implement building comparison logic

**Files:**
- Modify: `src/compare/buildings.rs`

- [ ] **Step 1: Implement compare_bdot10k**

Replace the stub in `src/compare/buildings.rs` with the full implementation:

```rust
use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::utils::format_duration;

pub fn compare_bdot10k(conn: &Connection) -> Result<()> {
    info!("Comparing BDOT10k buildings against OSM");
    let t = std::time::Instant::now();

    conn.execute_batch(
        "
        DROP TABLE IF EXISTS bdot10k_comparison;
        CREATE TABLE bdot10k_comparison AS
        SELECT
            b.LOKALNYID AS lokalnyid,
            m.osm_id AS matched_osm_id,
            m.osm_type AS matched_osm_type,
            m.osm_id IS NOT NULL AS matched
        FROM bdot10k_buildings b
        LEFT JOIN LATERAL (
            SELECT osm.osm_id, osm.osm_type
            FROM osm_buildings osm
            WHERE ST_Contains(osm.geom, ST_Centroid(b.geom))
            LIMIT 1
        ) m ON TRUE;
        ",
    )
    .context("Failed to compare BDOT10k buildings against OSM")?;

    let (total, matched): (i64, i64) = conn.query_row(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE matched) FROM bdot10k_comparison",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    info!(
        total,
        matched,
        unmatched = total - matched,
        elapsed = %format_duration(t.elapsed()),
        "BDOT10k comparison complete"
    );

    Ok(())
}
```

- [ ] **Step 2: Implement compare_egib**

Add to the same file, below `compare_bdot10k`:

```rust
pub fn compare_egib(conn: &Connection) -> Result<()> {
    info!("Comparing EGIB buildings against OSM");
    let t = std::time::Instant::now();

    conn.execute_batch(
        "
        DROP TABLE IF EXISTS egib_comparison;
        CREATE TABLE egib_comparison AS
        SELECT
            b.id_budynku,
            m.osm_id AS matched_osm_id,
            m.osm_type AS matched_osm_type,
            m.osm_id IS NOT NULL AS matched
        FROM egib_buildings b
        LEFT JOIN LATERAL (
            SELECT osm.osm_id, osm.osm_type
            FROM osm_buildings osm
            WHERE ST_Contains(osm.geom, ST_Centroid(b.geom))
            LIMIT 1
        ) m ON TRUE;
        ",
    )
    .context("Failed to compare EGIB buildings against OSM")?;

    let (total, matched): (i64, i64) = conn.query_row(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE matched) FROM egib_comparison",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    info!(
        total,
        matched,
        unmatched = total - matched,
        elapsed = %format_duration(t.elapsed()),
        "EGIB comparison complete"
    );

    Ok(())
}
```

- [ ] **Step 3: Run integration test**

Run: `cargo test test_compare_buildings_both -- --nocapture 2>&1 | tail -30`
Expected: PASS — imports complete, comparison runs, output contains "BDOT10k comparison complete" with "total=74" and "EGIB comparison complete".

- [ ] **Step 4: Commit**

```bash
git add src/compare/buildings.rs
git commit -m "feat: implement building comparison via centroid containment

BDOT10k and EGIB buildings are matched against OSM using
ST_Contains with the gov building centroid. Results stored
in bdot10k_comparison and egib_comparison tables.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 3: Single-source comparison tests and full test run

**Files:**
- Modify: `tests/cli_compare_buildings.rs`

- [ ] **Step 1: Add single-source tests**

Append to `tests/cli_compare_buildings.rs`:

```rust
#[test]
fn test_compare_buildings_bdot10k_only() {
    let (cfg, _db_dir, _rocksdb_dir) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_all(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings", "bdot10k"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("BDOT10k comparison complete")
                .and(predicate::str::contains("total=74"))
                .and(predicate::str::contains("EGIB comparison complete").not()),
        );
}

#[test]
fn test_compare_buildings_egib_only() {
    let (cfg, _db_dir, _rocksdb_dir) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_all(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings", "egib"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("EGIB comparison complete")
                .and(predicate::str::contains("total=74"))
                .and(predicate::str::contains("BDOT10k comparison complete").not()),
        );
}

#[test]
fn test_compare_buildings_without_imported_data_fails() {
    let (cfg, _db_dir, _rocksdb_dir) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    // No imports — comparison should fail because source tables don't exist
    cmd()
        .args(["--config", &cfg_path, "compare", "buildings"])
        .assert()
        .failure();
}
```

- [ ] **Step 2: Run all compare tests**

Run: `cargo test cli_compare_buildings -- --nocapture 2>&1 | tail -20`
Expected: all 4 tests pass.

- [ ] **Step 3: Run full test suite**

Run: `cargo test 2>&1 | tail -20`
Expected: all tests pass, no regressions.

- [ ] **Step 4: Run clippy and fmt**

Run: `cargo clippy 2>&1 | tail -10 && cargo fmt -- --check`
Expected: no warnings, no formatting issues.

- [ ] **Step 5: Commit**

```bash
git add tests/cli_compare_buildings.rs
git commit -m "test: add integration tests for building comparison CLI

Tests single-source (bdot10k-only, egib-only), both sources,
and failure when source data hasn't been imported.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```
