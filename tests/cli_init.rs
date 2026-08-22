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

// `init`'s `update osm` / `compare full` / `queue drain` steps need network
// access (or a mocked replication server, which nothing in this test binary
// sets up -- see `src/update/osm.rs`'s own tests for that machinery), so this
// only exercises the short-circuit: a failing `import full` must stop `init`
// before it ever reaches those steps.
#[test]
fn test_init_stops_on_import_failure() {
    let (cfg, _rocksdb_dir) = memory_config();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "init",
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
