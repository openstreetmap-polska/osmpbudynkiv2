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
fn imports_both_committed_mapping_files() {
    let (cfg, _db_dir, _rocks_dir, db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    cmd()
        .args([
            "--config",
            &cfg_path,
            "import",
            "building-types",
            "--bdot10k-file",
            "mappings/bdot10k_building_types.csv",
            "--egib-file",
            "mappings/egib_building_types.csv",
        ])
        .assert()
        .success();

    let conn = Connection::open(&db_path).unwrap();
    let bdot10k: i64 = conn
        .query_row("SELECT COUNT(*) FROM bdot10k_building_types", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(bdot10k, 178);
    let egib: i64 = conn
        .query_row("SELECT COUNT(*) FROM egib_building_types", [], |r| r.get(0))
        .unwrap();
    assert_eq!(egib, 13);

    let outcome: String = conn
        .query_row(
            "SELECT outcome FROM job_run_log WHERE job_name = 'import:building-types'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(outcome, "Success");

    for p in [
        "mappings/bdot10k_building_types.csv",
        "mappings/egib_building_types.csv",
    ] {
        assert!(
            std::path::Path::new(p).exists(),
            "a --*-file path must never be deleted"
        );
    }
}

#[test]
fn a_bad_bdot10k_file_fails_before_egib_loads_and_records_the_error() {
    let (cfg, _db_dir, _rocks_dir, db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    let mut bad = tempfile::NamedTempFile::new().unwrap();
    write!(
        bad,
        "tier,key,min_levels,max_levels,max_neighbours,tags\n1,a,,,,man_made=silo\n"
    )
    .unwrap();
    bad.flush().unwrap();

    cmd()
        .args([
            "--config",
            &cfg_path,
            "import",
            "building-types",
            "--bdot10k-file",
            bad.path().to_str().unwrap(),
            "--egib-file",
            "mappings/egib_building_types.csv",
        ])
        .assert()
        .failure();

    let conn = Connection::open(&db_path).unwrap();
    let bdot10k: i64 = conn
        .query_row("SELECT COUNT(*) FROM bdot10k_building_types", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(bdot10k, 0, "a rejected file must leave the table empty");
    let egib: i64 = conn
        .query_row("SELECT COUNT(*) FROM egib_building_types", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        egib, 0,
        "bdot10k failing must stop the command before egib ever loads"
    );

    let outcome: String = conn
        .query_row(
            "SELECT outcome FROM job_run_log WHERE job_name = 'import:building-types'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(outcome, "Error");
}
