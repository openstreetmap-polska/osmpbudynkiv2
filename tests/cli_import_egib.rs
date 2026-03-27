use assert_cmd::Command;
use predicates::prelude::*;

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("osmpbudynkiv2").unwrap();
    cmd.env("NO_COLOR", "1");
    cmd
}

#[test]
fn test_import_egib_from_fixture() {
    cmd()
        .args([
            "--db-path",
            ":memory:",
            "import",
            "egib",
            "--file",
            "fixtures/egib.parquet",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("EGIB buildings imported")
                .and(predicate::str::contains("count=74")),
        );
}

#[test]
fn test_import_egib_missing_file() {
    cmd()
        .args([
            "--db-path",
            ":memory:",
            "import",
            "egib",
            "--file",
            "nonexistent.parquet",
        ])
        .assert()
        .failure();
}
