use assert_cmd::Command;
use predicates::prelude::*;

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("osmpbudynkiv2").unwrap();
    cmd.env("NO_COLOR", "1");
    cmd
}

#[test]
fn test_import_osm_from_fixture() {
    cmd()
        .args([
            "--db-path",
            ":memory:",
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
    cmd()
        .args([
            "--db-path",
            ":memory:",
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

    // First import succeeds
    cmd()
        .args([
            "--db-path",
            db_path,
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
            "--db-path",
            db_path,
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
