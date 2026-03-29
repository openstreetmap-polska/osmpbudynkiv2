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
fn test_import_bdot10k_from_fixture() {
    let cfg = memory_config();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
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
    let cfg = memory_config();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "import",
            "bdot10k",
            "--file",
            "nonexistent.parquet",
        ])
        .assert()
        .failure();
}
