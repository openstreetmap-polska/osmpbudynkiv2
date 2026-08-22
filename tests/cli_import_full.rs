use assert_cmd::Command;
use duckdb::Connection;
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

fn persistent_config() -> (
    tempfile::NamedTempFile,
    tempfile::TempDir,
    tempfile::TempDir,
    std::path::PathBuf,
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
            "--street-mappings-file",
            "mappings/street_names_mappings.csv",
            "--bdot10k-building-types-file",
            "mappings/bdot10k_building_types.csv",
            "--egib-building-types-file",
            "mappings/egib_building_types.csv",
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
fn test_import_full_loads_the_mappings_too() {
    let (cfg, _db_dir, _rocksdb_dir, db_path) = persistent_config();
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
            "--street-mappings-file",
            "mappings/street_names_mappings.csv",
            "--bdot10k-building-types-file",
            "mappings/bdot10k_building_types.csv",
            "--egib-building-types-file",
            "mappings/egib_building_types.csv",
        ])
        .assert()
        .success();

    let conn = Connection::open(&db_path).unwrap();

    let mapping_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM street_name_mappings", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(mapping_count, 3272);

    for job_name in ["import:street-mappings", "import:building-types"] {
        let outcome: String = conn
            .query_row(
                "SELECT outcome FROM job_run_log WHERE job_name = ?",
                [job_name],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(outcome, "Success", "job {job_name} did not succeed");
    }
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
