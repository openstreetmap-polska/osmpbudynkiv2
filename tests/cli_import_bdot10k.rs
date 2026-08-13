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

#[test]
fn test_import_bdot10k_writes_row_hash() {
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
    let (total, null_hashes): (i64, i64) = conn
        .query_row(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE _row_hash IS NULL) FROM bdot10k_buildings",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(total, 74);
    assert_eq!(null_hashes, 0, "every row must carry a hash");

    // The import must also stamp the version those hashes were built with.
    // Without it, a later `update` cannot tell "these hashes are comparable"
    // from "the expression changed underneath them".
    let stamp: String = conn
        .query_row(
            "SELECT value FROM metadata WHERE key = 'row_hash_version'",
            [],
            |row| row.get(0),
        )
        .expect("import must stamp row_hash_version");
    // Tracks `dataset::ROW_HASH_VERSION` by hand — this is an integration test
    // against the binary, so the constant isn't importable. Bump both together.
    assert_eq!(stamp, "2");
}

/// An import rewrites `bdot10k_buildings` wholesale, including the
/// `centroid` column `/tiles`' adjacency CTE reads with no per-cell version
/// covering it -- see `serving_version`'s module doc. The import dispatch
/// must bump `metadata.serving_epoch` alongside `row_hash_version`. Uses a
/// file-backed database (like `test_import_bdot10k_writes_row_hash` above,
/// not `memory_config()`) so the state can be read back after the CLI
/// process exits.
#[test]
fn test_import_bdot10k_bumps_serving_epoch() {
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
    let epoch: String = conn
        .query_row(
            "SELECT value FROM metadata WHERE key = 'serving_epoch'",
            [],
            |row| row.get(0),
        )
        .expect("import must bump metadata.serving_epoch");
    assert_eq!(epoch, "1", "first bump on a fresh database must land at 1");
}
