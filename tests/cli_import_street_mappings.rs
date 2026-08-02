use std::io::Write;
use std::path::PathBuf;

use assert_cmd::Command;
use duckdb::Connection;

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
        .query_row("SELECT COUNT(*) FROM street_name_mappings", [], |r| {
            r.get(0)
        })
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

    assert!(
        std::path::Path::new("mappings/street_names_mappings.csv").exists(),
        "a --file path must never be deleted"
    );
}

#[test]
fn a_downloaded_file_is_cleaned_up_after_a_successful_import() {
    let db_dir = tempfile::TempDir::new().unwrap();
    let rocksdb_dir = tempfile::TempDir::new().unwrap();
    let db_path = db_dir.path().join("test.duckdb");

    // `download_file_as` skips downloading (and hitting the network) when
    // the destination already exists, so pre-seeding the download dir with
    // the destination filename simulates "a file was downloaded" without
    // needing a live server -- this is exactly the stale-file scenario the
    // cleanup guards against.
    let download_dir = tempfile::TempDir::new().unwrap();
    let dest = download_dir.path().join("street_names_mappings.csv");
    std::fs::copy("mappings/street_names_mappings.csv", &dest).unwrap();

    let mut cfg = tempfile::NamedTempFile::new().unwrap();
    write!(
        cfg,
        "db_path = \"{}\"\nrocksdb_path = \"{}\"\ndownload_dir = \"{}\"\n",
        db_path.display(),
        rocksdb_dir.path().display(),
        download_dir.path().display(),
    )
    .unwrap();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    cmd()
        .args([
            "--config",
            &cfg_path,
            "import",
            "street-mappings",
            "--url",
            "http://unused.invalid/street_names_mappings.csv",
        ])
        .assert()
        .success();

    let conn = Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM street_name_mappings", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 3272);

    assert!(
        !dest.exists(),
        "a downloaded file must be cleaned up after a successful import (cleanup_downloaded_files defaults to true)"
    );
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
        .query_row("SELECT COUNT(*) FROM street_name_mappings", [], |r| {
            r.get(0)
        })
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
