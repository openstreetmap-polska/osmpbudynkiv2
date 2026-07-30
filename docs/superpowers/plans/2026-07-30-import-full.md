# Import Full Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `import full`, which today is a CLI stub (`ImportSource::Full`) that
immediately `bail!`s, so it actually runs OSM, BDOT10k, EGIB, and PRG imports in sequence
against one connection.

**Architecture:** `ImportSource::Full` becomes a struct variant with one optional
`--file`-style flag per source (plus `--terc-file` for PRG), mirroring the flags each
individual `import <source>` subcommand already has. `import::run`'s `Full` match arm
calls the four existing `import()` functions in order (OSM → BDOT10k → EGIB → PRG),
propagating the first error with `?` (fail-fast, same pattern `CompareTarget::Full` in
`src/compare/mod.rs` already uses), then stamps the row-hash version once at the end.

**Tech Stack:** Rust, clap (`#[derive(Subcommand)]`), assert_cmd + predicates for CLI
integration tests, existing fixtures (`fixtures/osm.pbf`, `fixtures/bdot10k.parquet`,
`fixtures/egib.parquet`, `fixtures/prg.zip`, `fixtures/teryt.zip`).

## Global Constraints

- Fail-fast: the first source to error stops the remaining sources (no partial-continue,
  no error collection) — spec decision, matches `CompareTarget::Full`.
- Import order is OSM, BDOT10k, EGIB, PRG — matches the README's existing example and the
  `ImportSource` enum's declaration order.
- `stamp_row_hash_version` is called exactly once, after all four imports succeed — it
  writes a single global `metadata.row_hash_version` key, so calling it per-source would
  just redundantly overwrite the same value three times.
- No behavioral change to `import osm` / `import bdot10k` / `import egib` / `import prg`
  as individually-invoked commands.

---

### Task 1: Add file-override flags to `ImportSource::Full`

**Files:**
- Modify: `src/cli.rs:108-109`

**Interfaces:**
- Produces: `ImportSource::Full { osm_file: Option<PathBuf>, bdot10k_file: Option<PathBuf>, egib_file: Option<PathBuf>, prg_file: Option<PathBuf>, terc_file: Option<PathBuf> }` — consumed by Task 2.

- [ ] **Step 1: Replace the unit `Full` variant with a struct variant**

In `src/cli.rs`, the `ImportSource` enum currently ends with:

```rust
    /// Run all imports in sequence
    Full,
}
```

Replace those two lines with:

```rust
    /// Run all imports in sequence (OSM, BDOT10k, EGIB, PRG)
    Full {
        /// Path to local OSM PBF file (skips download)
        #[arg(long)]
        osm_file: Option<PathBuf>,
        /// Path to local BDOT10k file (skips download)
        #[arg(long)]
        bdot10k_file: Option<PathBuf>,
        /// Path to local EGIB file (skips download)
        #[arg(long)]
        egib_file: Option<PathBuf>,
        /// Path to local PRG file (skips download)
        #[arg(long)]
        prg_file: Option<PathBuf>,
        /// Path to a TERC (TERYT) dictionary file (.zip or .xml), for the PRG import
        #[arg(long)]
        terc_file: Option<PathBuf>,
    },
}
```

- [ ] **Step 2: Verify it compiles (expect a match-arm error in `import/mod.rs`)**

Run: `cargo build 2>&1 | tail -30`
Expected: FAIL — `src/import/mod.rs` no longer matches `ImportSource` exhaustively
(`ImportSource::Full` pattern in `src/import/mod.rs:40` doesn't match the new struct
variant shape). This confirms the enum change took effect; Task 2 fixes the match arm.

- [ ] **Step 3: Commit**

```bash
git add src/cli.rs
git commit -m "feat(cli): add per-source file flags to import full"
```

---

### Task 2: Implement the `ImportSource::Full` dispatch

**Files:**
- Modify: `src/import/mod.rs:6` (imports), `src/import/mod.rs:40-42` (match arm)

**Interfaces:**
- Consumes: `ImportSource::Full { osm_file, bdot10k_file, egib_file, prg_file, terc_file }` from Task 1; `osm::import`, `bdot10k::import`, `egib::import`, `prg::import`, `stamp_row_hash_version` — all already defined in this file/module, signatures unchanged.
- Produces: `import::run` returns `Ok(())` after all four imports succeed and the row-hash version is stamped; on any failure, returns that error immediately without running later sources.

- [ ] **Step 1: Drop the now-unused `bail` import**

In `src/import/mod.rs:6`, change:

```rust
use anyhow::{Result, bail};
```

to:

```rust
use anyhow::Result;
```

- [ ] **Step 2: Replace the `Full` match arm**

In `src/import/mod.rs`, replace:

```rust
        ImportSource::Full => {
            bail!("Full import is not yet implemented");
        }
```

with:

```rust
        ImportSource::Full {
            osm_file,
            bdot10k_file,
            egib_file,
            prg_file,
            terc_file,
        } => {
            osm::import(conn, kv, config, osm_file.as_deref(), &urls.osm_pbf)?;
            bdot10k::import(conn, config, bdot10k_file.as_deref(), &urls.bdot10k)?;
            egib::import(conn, config, egib_file.as_deref(), &urls.egib)?;
            prg::import(
                conn,
                config,
                prg_file.as_deref(),
                terc_file.as_deref(),
                &urls.prg,
            )?;
            stamp_row_hash_version(conn)
        }
```

- [ ] **Step 3: Build and lint**

Run: `cargo build && cargo clippy --all-targets 2>&1 | tail -30`
Expected: both succeed with no warnings about unused imports or non-exhaustive matches.

- [ ] **Step 4: Commit**

```bash
git add src/import/mod.rs
git commit -m "feat(import): implement import full"
```

---

### Task 3: Integration tests for `import full`

**Files:**
- Create: `tests/cli_import_full.rs`

**Interfaces:**
- Consumes: the `osmpbudynkiv2` binary's `import full` subcommand from Task 2, and existing fixtures `fixtures/osm.pbf`, `fixtures/bdot10k.parquet`, `fixtures/egib.parquet`, `fixtures/prg.zip`, `fixtures/teryt.zip`.

- [ ] **Step 1: Write the test file**

Create `tests/cli_import_full.rs`:

```rust
use assert_cmd::Command;
use predicates::prelude::*;

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("osmpbudynkiv2").unwrap();
    cmd.env("NO_COLOR", "1");
    cmd
}

fn memory_config() -> (tempfile::NamedTempFile, tempfile::TempDir) {
    let rocksdb_dir = tempfile::TempDir::new().unwrap();
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    write!(
        tmp,
        "db_path = \":memory:\"\nrocksdb_path = \"{}\"\n",
        rocksdb_dir.path().display()
    )
    .unwrap();
    (tmp, rocksdb_dir)
}

#[test]
fn test_import_full_from_fixtures() {
    let (cfg, _rocksdb_dir) = memory_config();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "import",
            "full",
            "--osm-file",
            "fixtures/osm.pbf",
            "--bdot10k-file",
            "fixtures/bdot10k.parquet",
            "--egib-file",
            "fixtures/egib.parquet",
            "--prg-file",
            "fixtures/prg.zip",
            "--terc-file",
            "fixtures/teryt.zip",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("OSM import complete")
                .and(predicate::str::contains("BDOT10k import complete"))
                .and(predicate::str::contains("EGIB import complete"))
                .and(predicate::str::contains("PRG import complete")),
        );
}

#[test]
fn test_import_full_stops_on_first_failure() {
    let (cfg, _rocksdb_dir) = memory_config();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "import",
            "full",
            "--osm-file",
            "nonexistent.pbf",
            "--bdot10k-file",
            "fixtures/bdot10k.parquet",
            "--egib-file",
            "fixtures/egib.parquet",
            "--prg-file",
            "fixtures/prg.zip",
            "--terc-file",
            "fixtures/teryt.zip",
        ])
        .assert()
        .failure()
        .stdout(predicate::str::contains("BDOT10k import complete").not());
}
```

- [ ] **Step 2: Run the new tests and verify they pass**

Run: `cargo test --test cli_import_full -- --nocapture`
Expected: both `test_import_full_from_fixtures` and
`test_import_full_stops_on_first_failure` PASS.

- [ ] **Step 3: Run the full test suite to check for regressions**

Run: `cargo test`
Expected: all tests pass (existing per-source import tests untouched, so they should be
unaffected).

- [ ] **Step 4: Commit**

```bash
git add tests/cli_import_full.rs
git commit -m "test: add import full CLI integration tests"
```

---

### Task 4: Update docs

**Files:**
- Modify: `README.md:31` (roadmap bullet), `README.md:87-90` (CLI commands example)

**Interfaces:**
- None — documentation only.

- [ ] **Step 1: Flip the roadmap bullet**

In `README.md`, change:

```markdown
- [ ] `import full` — running all imports in one command (individual imports work)
```

to:

```markdown
- [x] `import full` — running all imports (OSM, BDOT10k, EGIB, PRG) in one command
```

(Move it up into the "Implemented" section, alongside the other `import`/`update`/
`compare` bullets, rather than leaving it under "Not yet implemented".)

- [ ] **Step 2: Replace the `import full` example**

In `README.md`, under `### import — bulk-load data`, change:

```markdown
# Import everything (OSM, BDOT10k, EGIB, PRG) in sequence (not yet implemented)
cargo run -- import full
```

to:

```markdown
# Import everything (OSM, BDOT10k, EGIB, PRG) in sequence
cargo run -- import full

# Import everything from local files instead of downloading (any subset of flags works;
# omitted sources still download)
cargo run -- import full \
  --osm-file poland-latest.osm.pbf \
  --bdot10k-file bdot10k.parquet \
  --egib-file egib.parquet \
  --prg-file prg.zip \
  --terc-file terc.zip
```

- [ ] **Step 3: Proofread the diff**

Run: `git diff README.md`
Expected: only the two edits above; roadmap line now under "Implemented", CLI example
updated and no longer says "not yet implemented".

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: document import full"
```
