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
fn test_import_egib_from_fixture() {
    let (cfg, _rocksdb_dir) = memory_config();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "import",
            "egib",
            "--file",
            "fixtures/egib.parquet",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("EGIB import complete")
                .and(predicate::str::contains("count=74")),
        );
}

/// `fixtures/egib_v2.parquet` holds 76 rows: the 74-row v1 set plus one
/// deliberate NULL-`id_budynku` row and one duplicate-key row (see
/// `fixtures/scripts/prepare_update_fixtures.sh`). Pins that `import`, not
/// just `load_into` in isolation, ends up reporting the deduplicated/
/// NULL-filtered count -- 76 - 1 (null key) - 1 (duplicate) = 74 -- rather
/// than the raw parquet row count.
#[test]
fn test_import_egib_from_v2_fixture_reports_deduplicated_count() {
    let (cfg, _rocksdb_dir) = memory_config();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "import",
            "egib",
            "--file",
            "fixtures/egib_v2.parquet",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("EGIB import complete")
                .and(predicate::str::contains("count=74")),
        );
}

#[test]
fn test_import_egib_missing_file() {
    let (cfg, _rocksdb_dir) = memory_config();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "import",
            "egib",
            "--file",
            "nonexistent.parquet",
        ])
        .assert()
        .failure();
}

/// Reads the live table back through a direct `duckdb::Connection` (rather
/// than trusting the CLI's own stdout `count=` line, which
/// `test_import_egib_from_fixture` above already pins) to confirm the
/// import actually persisted the expected row count to `egib_buildings`.
/// This used to assert a non-NULL `_row_hash` on every row; that column is
/// gone along with the whole-row-hash diff mechanism (see
/// `docs/superpowers/plans/2026-08-14-key-based-diff.md`) and has no
/// replacement to assert here.
#[test]
fn test_import_egib_persists_expected_row_count() {
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
            "egib",
            "--file",
            "fixtures/egib.parquet",
        ])
        .assert()
        .success();

    let conn = duckdb::Connection::open(&db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;")
        .unwrap();
    let total: i64 = conn
        .query_row("SELECT COUNT(*) FROM egib_buildings", [], |row| row.get(0))
        .unwrap();
    assert_eq!(total, 74);
}
