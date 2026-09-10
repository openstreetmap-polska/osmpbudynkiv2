use assert_cmd::Command;
use predicates::prelude::*;

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("osmpbudynkiv2").unwrap();
    cmd.env("NO_COLOR", "1");
    cmd
}

/// DuckDB in memory, RocksDB in a temp dir that outlives each command -- so a
/// second invocation sees what the first wrote to the store.
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
fn import_osm_ends_by_compacting_the_store() {
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
            predicate::str::contains("Step done: compact RocksDB")
                .and(predicate::str::contains(r#"family="nodes""#)),
        );
}

/// The manual verb works on a store `import osm` already compacted -- the
/// state a re-run finds -- and on one that has never held anything.
#[test]
fn kv_compact_runs_on_an_imported_store_and_on_an_empty_one() {
    let (cfg, _rocksdb_dir) = memory_config();
    let config = cfg.path().to_str().unwrap();
    cmd()
        .args(["--config", config, "kv", "compact"])
        .assert()
        .success()
        .stdout(predicate::str::contains("RocksDB compaction complete"));

    cmd()
        .args([
            "--config",
            config,
            "import",
            "osm",
            "--file",
            "fixtures/osm.pbf",
        ])
        .assert()
        .success();
    cmd()
        .args(["--config", config, "kv", "compact"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains(r#"family="way_to_relations""#)
                .and(predicate::str::contains("RocksDB compaction complete")),
        );
}
