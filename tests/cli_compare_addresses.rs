use std::path::{Path, PathBuf};

use assert_cmd::Command;
use duckdb::Connection;
use predicates::prelude::*;

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("osmpbudynkiv2").unwrap();
    cmd.env("NO_COLOR", "1");
    cmd
}

fn persistent_config() -> (
    tempfile::NamedTempFile,
    tempfile::TempDir,
    tempfile::TempDir,
    PathBuf,
) {
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
    (cfg, db_dir, rocksdb_dir, db_path)
}

fn import_osm(cfg_path: &str) {
    cmd()
        .args([
            "--config",
            cfg_path,
            "import",
            "osm",
            "--file",
            "fixtures/osm.pbf",
        ])
        .assert()
        .success();
}

fn import_prg(cfg_path: &str) {
    cmd()
        .args([
            "--config",
            cfg_path,
            "import",
            "prg",
            "--file",
            "fixtures/prg.zip",
            "--terc-file",
            "fixtures/teryt.zip",
        ])
        .assert()
        .success();
}

fn table_exists(db_path: &Path, table: &str) -> bool {
    let conn = Connection::open(db_path).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = ?",
            [table],
            |row| row.get(0),
        )
        .unwrap();
    count > 0
}

#[test]
fn test_compare_addresses_default() {
    let (cfg, _db_dir, _rocksdb_dir, db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_osm(&cfg_path);
    import_prg(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "addresses"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("PRG comparison complete")
                .and(predicate::str::contains("total=3")),
        );

    assert!(table_exists(&db_path, "prg_unmatched"));

    // The PRG fixture has 3 lubuskie addresses. The OSM fixture has Warsaw
    // addresses. None of them share housenumber + proximity, so all 3 PRG
    // rows become candidates.
    let conn = Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM prg_unmatched", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 3, "all 3 lubuskie PRG addresses are candidates");
}

#[test]
fn test_compare_addresses_prg_only() {
    let (cfg, _db_dir, _rocksdb_dir, _db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_osm(&cfg_path);
    import_prg(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "addresses", "prg"])
        .assert()
        .success()
        .stdout(predicate::str::contains("PRG comparison complete"));
}

#[test]
fn test_compare_addresses_all() {
    let (cfg, _db_dir, _rocksdb_dir, _db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_osm(&cfg_path);
    import_prg(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "addresses", "all"])
        .assert()
        .success()
        .stdout(predicate::str::contains("PRG comparison complete"));
}

#[test]
fn test_compare_addresses_without_data_fails() {
    let (cfg, _db_dir, _rocksdb_dir, _db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    cmd()
        .args(["--config", &cfg_path, "compare", "addresses"])
        .assert()
        .failure();
}

/// `compare addresses` is idempotent — running it twice produces the same
/// candidate count.
#[test]
fn test_compare_addresses_is_idempotent() {
    let (cfg, _db_dir, _rocksdb_dir, db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_osm(&cfg_path);
    import_prg(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "addresses"])
        .assert()
        .success();
    let first: i64 = {
        let conn = Connection::open(&db_path).unwrap();
        conn.query_row("SELECT COUNT(*) FROM prg_unmatched", [], |row| row.get(0))
            .unwrap()
    };

    cmd()
        .args(["--config", &cfg_path, "compare", "addresses"])
        .assert()
        .success();
    let second: i64 = {
        let conn = Connection::open(&db_path).unwrap();
        conn.query_row("SELECT COUNT(*) FROM prg_unmatched", [], |row| row.get(0))
            .unwrap()
    };

    assert_eq!(first, second);
}
