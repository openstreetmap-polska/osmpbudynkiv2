use assert_cmd::Command;
use predicates::prelude::*;

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("osmpbudynkiv2").unwrap();
    cmd.env("NO_COLOR", "1");
    cmd
}

fn memory_config() -> tempfile::NamedTempFile {
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    write!(tmp, "db_path = \":memory:\"\n").unwrap();
    tmp
}

#[test]
fn test_import_osm_from_fixture() {
    let cfg = memory_config();
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
                .and(predicate::str::contains("addresses=3")),
        );
}

#[test]
fn test_import_osm_missing_file() {
    let cfg = memory_config();
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
fn test_import_osm_twice_fails_on_duplicates() {
    // This test needs a persistent database between two CLI invocations
    let db_path = "target/test_cli_import_twice.duckdb";
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}.wal"));

    let mut cfg_file = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    write!(cfg_file, "db_path = \"{db_path}\"\n").unwrap();
    let cfg_path = cfg_file.path().to_str().unwrap().to_string();

    // First import succeeds
    cmd()
        .args([
            "--config",
            &cfg_path,
            "import",
            "osm",
            "--file",
            "fixtures/osm.pbf",
        ])
        .assert()
        .success();

    // Second import fails — osm_nodes has a PRIMARY KEY constraint
    cmd()
        .args([
            "--config",
            &cfg_path,
            "import",
            "osm",
            "--file",
            "fixtures/osm.pbf",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Duplicate key"));

    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}.wal"));
}
