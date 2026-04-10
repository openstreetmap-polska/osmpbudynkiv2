use std::path::PathBuf;

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

#[test]
fn test_import_prg_from_fixture() {
    let (cfg, _db_dir, _rocksdb_dir, db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    cmd()
        .args([
            "--config",
            &cfg_path,
            "import",
            "prg",
            "--file",
            "fixtures/prg.zip",
            "--terc-file",
            "fixtures/teryt.zip",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("PRG import complete")
                .and(predicate::str::contains("count=3")),
        );

    // Verify table contents.
    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;")
        .unwrap();

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM prg_addresses", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 3, "fixture has 3 PRG 2021 addresses");

    // All geoms should be non-null and within Poland's bounding box.
    let bad_geom: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM prg_addresses
             WHERE geom IS NULL
                OR ST_X(geom) < 14 OR ST_X(geom) > 25
                OR ST_Y(geom) < 49 OR ST_Y(geom) > 55",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(bad_geom, 0, "all geoms should be inside Poland's extent");

    // Verify TERC mapping was applied — all 3 addresses are in lubuskie.
    let woj: String = conn
        .query_row(
            "SELECT DISTINCT wojewodztwo FROM prg_addresses",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(woj, "lubuskie");

    // Verify housenumbers are present.
    let housenumbers: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT numer_porzadkowy FROM prg_addresses ORDER BY numer_porzadkowy")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    };
    assert_eq!(housenumbers, vec!["1A", "21A", "2A"]);
}

#[test]
fn test_import_prg_missing_file() {
    let (cfg, _db_dir, _rocksdb_dir, _db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    cmd()
        .args([
            "--config",
            &cfg_path,
            "import",
            "prg",
            "--file",
            "nonexistent.zip",
            "--terc-file",
            "fixtures/teryt.zip",
        ])
        .assert()
        .failure();
}

#[test]
fn test_import_prg_missing_terc_fails() {
    let (cfg, _db_dir, _rocksdb_dir, _db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    // Append teryt config that disables download but provides no file
    use std::io::Write;
    let mut cfg_file = std::fs::OpenOptions::new()
        .append(true)
        .open(cfg.path())
        .unwrap();
    writeln!(cfg_file, "\n[teryt]\ndownload = false").unwrap();

    // No --terc-file and teryt.download = false with no file_path → should fail
    cmd()
        .args([
            "--config",
            &cfg_path,
            "import",
            "prg",
            "--file",
            "fixtures/prg.zip",
        ])
        .assert()
        .failure();
}

#[test]
fn test_import_prg_teryt_from_config_file_path() {
    let (cfg, _db_dir, _rocksdb_dir, db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    // Set teryt.file_path in config instead of using --terc-file CLI flag
    use std::io::Write;
    let mut cfg_file = std::fs::OpenOptions::new()
        .append(true)
        .open(cfg.path())
        .unwrap();
    writeln!(
        cfg_file,
        "\n[teryt]\ndownload = false\nfile_path = \"fixtures/teryt.zip\""
    )
    .unwrap();

    cmd()
        .args([
            "--config",
            &cfg_path,
            "import",
            "prg",
            "--file",
            "fixtures/prg.zip",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("PRG import complete"));

    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;")
        .unwrap();

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM prg_addresses", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_import_prg_download_teryt_missing_credentials_fails() {
    let (cfg, _db_dir, _rocksdb_dir, _db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    // teryt.download defaults to true, but no credentials in config or env
    cmd()
        .env_remove("TERYT_API_USERNAME")
        .env_remove("TERYT_API_PASSWORD")
        .args([
            "--config",
            &cfg_path,
            "import",
            "prg",
            "--file",
            "fixtures/prg.zip",
        ])
        .assert()
        .failure();
}
