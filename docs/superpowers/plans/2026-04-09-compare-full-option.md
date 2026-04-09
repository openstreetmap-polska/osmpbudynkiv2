# Compare `full` / `buildings all` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `compare full` (top-level) and `compare buildings all` (buildings-level) CLI options, and document them.

**Architecture:** Pure additive CLI surface. Add one variant to `CompareTarget` (`Full`) and one variant to `BuildingsSource` (`All`). `Full` fans out to every comparison (today: buildings BDOT10k + EGIB). `All` at the buildings level is a synonym for "no source" — both BDOT10k and EGIB.

**Tech Stack:** Rust, clap (derive API), DuckDB, `assert_cmd` + `predicates` for CLI integration tests.

---

## File Structure

Files touched:

- **Modify** `src/cli.rs` — add `CompareTarget::Full` and `BuildingsSource::All` variants with clap doc comments.
- **Modify** `src/compare/mod.rs` — extend `run` to dispatch `CompareTarget::Full` and treat `Some(BuildingsSource::All)` the same as `None`.
- **Modify** `tests/cli_compare_buildings.rs` — add `test_compare_full` and `test_compare_buildings_all`.
- **Modify** `CLAUDE.md` — update the `compare <target>` bullet in the CLI commands section.

No new files.

---

## Task 1: Add `compare full` and `compare buildings all`

**Files:**
- Modify: `src/cli.rs:41-55`
- Modify: `src/compare/mod.rs:8-20`
- Test: `tests/cli_compare_buildings.rs` (append two new tests)

- [ ] **Step 1: Write the failing integration tests**

Append these two tests to the end of `tests/cli_compare_buildings.rs`:

```rust
#[test]
fn test_compare_full() {
    let (cfg, _db_dir, _rocksdb_dir) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_all(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "full"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("BDOT10k comparison complete")
                .and(predicate::str::contains("total=74"))
                .and(predicate::str::contains("EGIB comparison complete")),
        );
}

#[test]
fn test_compare_buildings_all() {
    let (cfg, _db_dir, _rocksdb_dir) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_all(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings", "all"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("BDOT10k comparison complete")
                .and(predicate::str::contains("total=74"))
                .and(predicate::str::contains("EGIB comparison complete")),
        );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --test cli_compare_buildings test_compare_full test_compare_buildings_all`

Expected: build failure or test failures — clap will reject `full` as an unknown subcommand of `compare`, and `all` as an unknown subcommand of `compare buildings`. If the build itself fails, that still counts as "red" for TDD purposes (the tests can't produce a passing assertion).

- [ ] **Step 3: Add the new clap variants**

Replace the `CompareTarget` and `BuildingsSource` enums in `src/cli.rs` (currently lines 41-55) with:

```rust
#[derive(Subcommand)]
pub enum CompareTarget {
    /// Compare building datasets against OSM buildings
    Buildings {
        #[command(subcommand)]
        source: Option<BuildingsSource>,
    },
    /// Run all available comparisons
    Full,
}

#[derive(Subcommand)]
pub enum BuildingsSource {
    /// Compare only BDOT10k buildings against OSM
    Bdot10k,
    /// Compare only EGIB buildings against OSM
    Egib,
    /// Compare all building sources against OSM
    All,
}
```

- [ ] **Step 4: Extend the dispatch in `src/compare/mod.rs`**

Replace the body of `run` in `src/compare/mod.rs` (currently lines 8-20) with:

```rust
pub fn run(conn: &Connection, target: CompareTarget) -> Result<()> {
    match target {
        CompareTarget::Buildings { source } => match source {
            None | Some(BuildingsSource::All) => {
                buildings::compare_bdot10k(conn)?;
                buildings::compare_egib(conn)?;
            }
            Some(BuildingsSource::Bdot10k) => buildings::compare_bdot10k(conn)?,
            Some(BuildingsSource::Egib) => buildings::compare_egib(conn)?,
        },
        // When new comparison targets are added, fan out to them here.
        CompareTarget::Full => {
            buildings::compare_bdot10k(conn)?;
            buildings::compare_egib(conn)?;
        }
    }
    Ok(())
}
```

Rationale for the match arms:
- `None | Some(BuildingsSource::All)` collapses the "no source given" and "explicit `all`" cases — they are semantically identical.
- `CompareTarget::Full` duplicates the buildings fan-out rather than delegating to `CompareTarget::Buildings { source: None }`, because when new comparison targets are added, `Full` needs to call them directly. The comment marks the extension point.

- [ ] **Step 5: Run the new tests to verify they pass**

Run: `cargo test --test cli_compare_buildings test_compare_full test_compare_buildings_all`

Expected: both tests PASS. Note: the fixtures for this test suite are large and the tests do real imports, so this may take a minute or more.

- [ ] **Step 6: Run the full compare test file to confirm no regression**

Run: `cargo test --test cli_compare_buildings`

Expected: all five tests pass (`test_compare_buildings_both`, `test_compare_buildings_bdot10k_only`, `test_compare_buildings_egib_only`, `test_compare_buildings_without_imported_data_fails`, plus the two new ones).

- [ ] **Step 7: Run clippy and fmt check**

Run: `cargo clippy --all-targets -- -D warnings && cargo fmt -- --check`

Expected: no warnings, no formatting diffs. If clippy complains about the new match arms (e.g. "this match could be written as an if let"), fix it; if fmt wants changes, run `cargo fmt` and re-check.

- [ ] **Step 8: Commit**

```bash
git add src/cli.rs src/compare/mod.rs tests/cli_compare_buildings.rs
git commit -m "feat(compare): add full and buildings all options"
```

---

## Task 2: Document the new options in CLAUDE.md

**Files:**
- Modify: `CLAUDE.md` (the `compare <target>` bullet in the "CLI commands" section)

- [ ] **Step 1: Update the CLAUDE.md bullet**

In `CLAUDE.md`, find this line in the "CLI commands" list:

```
- `compare <target>` — compare government data against OSM (e.g. `compare buildings`)
```

Replace with:

```
- `compare <target>` — compare government data against OSM. Targets: `buildings` (optionally `bdot10k`, `egib`, or `all` — default runs all), `full` (runs every comparison)
```

- [ ] **Step 2: Verify `--help` output matches the docs**

Run: `cargo run -- compare --help`

Expected output includes:
```
Commands:
  buildings  Compare building datasets against OSM buildings
  full       Run all available comparisons
```

Run: `cargo run -- compare buildings --help`

Expected output includes:
```
Commands:
  bdot10k  Compare only BDOT10k buildings against OSM
  egib     Compare only EGIB buildings against OSM
  all      Compare all building sources against OSM
```

If the help text and `CLAUDE.md` bullet disagree, fix `CLAUDE.md`.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: mention compare full and buildings all in CLAUDE.md"
```

---

## Self-Review Notes

**Spec coverage:**
- `CompareTarget::Full` — Task 1 Step 3.
- `BuildingsSource::All` — Task 1 Step 3.
- Dispatch for `Full` + `All` — Task 1 Step 4.
- Integration test for `compare full` — Task 1 Step 1.
- Integration test for `compare buildings all` — Task 1 Step 1.
- `CLAUDE.md` update — Task 2 Step 1.
- Extension-point comment for future targets — Task 1 Step 4.

**Placeholder scan:** no TBDs, every code step includes the code, every command includes the expected outcome.

**Type consistency:** `CompareTarget`, `BuildingsSource`, and `buildings::compare_bdot10k` / `compare_egib` match the names used in the existing codebase (`src/compare/mod.rs:6`, `src/compare/buildings.rs:11,41`).
