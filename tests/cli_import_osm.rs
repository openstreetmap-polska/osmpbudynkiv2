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
fn test_import_osm_from_fixture() {
    let (cfg, _rocksdb_dir) = memory_config();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "import",
            "osm",
            "--file",
            "fixtures/osm.pbf",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("OSM import complete")
                .and(predicate::str::contains("buildings=2"))
                .and(predicate::str::contains("addresses=3"))
                .and(predicate::str::contains("former_buildings=1")),
        );
}

#[test]
fn test_import_osm_missing_file() {
    let (cfg, _rocksdb_dir) = memory_config();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "import",
            "osm",
            "--file",
            "nonexistent.pbf",
        ])
        .assert()
        .failure();
}

#[test]
fn test_import_osm_twice_reimports_cleanly() {
    // This test needs a persistent database between two CLI invocations
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.duckdb");
    let rocksdb_dir = tempfile::TempDir::new().unwrap();

    let mut cfg_file = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    write!(
        cfg_file,
        "db_path = \"{}\"\nrocksdb_path = \"{}\"\n",
        db_path.display(),
        rocksdb_dir.path().display()
    )
    .unwrap();
    let cfg_path = cfg_file.path().to_str().unwrap().to_string();

    let args = [
        "--config",
        &cfg_path,
        "import",
        "osm",
        "--file",
        "fixtures/osm.pbf",
    ];

    // First import succeeds
    cmd().args(args.iter()).assert().success().stdout(
        predicate::str::contains("buildings=2")
            .and(predicate::str::contains("addresses=3"))
            .and(predicate::str::contains("former_buildings=1")),
    );

    // Second import also succeeds — tables are dropped and recreated, counts stay the same
    cmd().args(args.iter()).assert().success().stdout(
        predicate::str::contains("buildings=2")
            .and(predicate::str::contains("addresses=3"))
            .and(predicate::str::contains("former_buildings=1")),
    );
}
