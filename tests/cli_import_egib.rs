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

#[test]
fn test_import_egib_writes_row_hash() {
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
    let null_hashes: i64 = conn
        .query_row(
            "SELECT COUNT(*) FILTER (WHERE _row_hash IS NULL) FROM egib_buildings",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(null_hashes, 0, "every row must carry a hash");
}
