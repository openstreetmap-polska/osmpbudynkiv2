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
