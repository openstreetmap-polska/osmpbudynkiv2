use assert_cmd::Command;
use predicates::prelude::*;

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("osmpbudynkiv2").unwrap();
    cmd.env("NO_COLOR", "1");
    cmd
}

#[test]
fn test_import_bdot10k_from_fixture() {
    cmd()
        .args([
            "--db-path",
            ":memory:",
            "import",
            "bdot10k",
            "--file",
            "fixtures/bdot10k.parquet",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("BDOT10k buildings imported")
                .and(predicate::str::contains("count=74")),
        );
}

#[test]
fn test_import_bdot10k_missing_file() {
    cmd()
        .args([
            "--db-path",
            ":memory:",
            "import",
            "bdot10k",
            "--file",
            "nonexistent.parquet",
        ])
        .assert()
        .failure();
}
