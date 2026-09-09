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
fn test_import_bdot10k_from_fixture() {
    let (cfg, _rocksdb_dir) = memory_config();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "import",
            "bdot10k",
            "--file",
            "fixtures/bdot10k.parquet",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("BDOT10k import complete")
                .and(predicate::str::contains("count=74")),
        );
}

/// `fixtures/bdot10k_v2.parquet` holds 76 rows: the 74-row v1 set plus one
/// deliberate NULL-`LOKALNYID` row and one duplicate-key row (see
/// `fixtures/scripts/prepare_update_fixtures.sh`). Pins that `import`, not
/// just `load_into` in isolation, ends up reporting the deduplicated/
/// NULL-filtered count -- 76 - 1 (null key) - 1 (duplicate) = 74 -- rather
/// than the raw parquet row count.
#[test]
fn test_import_bdot10k_from_v2_fixture_reports_deduplicated_count() {
    let (cfg, _rocksdb_dir) = memory_config();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "import",
            "bdot10k",
            "--file",
            "fixtures/bdot10k_v2.parquet",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("BDOT10k import complete")
                .and(predicate::str::contains("count=74")),
        );
}

#[test]
fn test_import_bdot10k_missing_file() {
    let (cfg, _rocksdb_dir) = memory_config();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "import",
            "bdot10k",
            "--file",
            "nonexistent.parquet",
        ])
        .assert()
        .failure();
}

/// Reads the live table back through a direct `duckdb::Connection` (rather
/// than trusting the CLI's own stdout `count=` line, which
/// `test_import_bdot10k_from_fixture` above already pins) to confirm the
/// import actually persisted the expected row count to
/// `bdot10k_buildings`. This used to also assert a non-NULL `_row_hash` on
/// every row and a `metadata.row_hash_version` stamp; both are gone along
/// with the whole-row-hash diff mechanism (see
/// `docs/superpowers/plans/2026-08-14-key-based-diff.md`) and have no
/// replacement to assert here — the file-backed-database setup is kept
/// because `test_import_bdot10k_enqueues_no_dirty_tiles` below relies on the
/// same technique and refers back to this test by name.
#[test]
fn test_import_bdot10k_persists_expected_row_count() {
    let db = tempfile::TempDir::new().unwrap();
    let rocksdb_dir = tempfile::TempDir::new().unwrap();
    let db_path = db.path().join("test.duckdb");
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    write!(
        tmp,
        "db_path = \"{}\"\nrocksdb_path = \"{}\"\n",
        db_path.display(),
        rocksdb_dir.path().display()
    )
    .unwrap();

    cmd()
        .args([
            "--config",
            tmp.path().to_str().unwrap(),
            "import",
            "bdot10k",
            "--file",
            "fixtures/bdot10k.parquet",
        ])
        .assert()
        .success();

    let conn = duckdb::Connection::open(&db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;")
        .unwrap();
    let total: i64 = conn
        .query_row("SELECT COUNT(*) FROM bdot10k_buildings", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(total, 74);
}

/// An import rewrites `bdot10k_buildings` wholesale, so every tile in the
/// country is potentially stale -- but it must invalidate them by **wiping the
/// tile store**, never by enqueueing. Enqueueing here would mean a queue row
/// per z14 cell in Poland (~200k of them) for work a single `drop_cf` does,
/// and `main.rs`'s dispatch calls `clear_tile_store` for exactly that reason.
///
/// This pins the half that is observable from DuckDB. The wipe itself is
/// pinned by `tile_store::clear_empties_the_store_but_leaves_the_osm_families_alone`;
/// asserting it here would need the RocksDB handle, which an integration test
/// driving the binary does not have.
///
/// Uses a file-backed database (like `test_import_bdot10k_persists_expected_row_count`
/// above, not `memory_config()`) so the state can be read back after the CLI
/// process exits.
#[test]
fn test_import_bdot10k_enqueues_no_dirty_tiles() {
    let db = tempfile::TempDir::new().unwrap();
    let rocksdb_dir = tempfile::TempDir::new().unwrap();
    let db_path = db.path().join("test.duckdb");
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    write!(
        tmp,
        "db_path = \"{}\"\nrocksdb_path = \"{}\"\n",
        db_path.display(),
        rocksdb_dir.path().display()
    )
    .unwrap();

    cmd()
        .args([
            "--config",
            tmp.path().to_str().unwrap(),
            "import",
            "bdot10k",
            "--file",
            "fixtures/bdot10k.parquet",
        ])
        .assert()
        .success();

    let conn = duckdb::Connection::open(&db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;")
        .unwrap();
    let queued: i64 = conn
        .query_row("SELECT COUNT(*) FROM tile_dirty_cells", [], |row| {
            row.get(0)
        })
        .expect("tile_dirty_cells must exist after an import");
    assert_eq!(
        queued, 0,
        "an import invalidates by wiping the tile store, never by enqueueing \
         one row per cell in the country"
    );
}
