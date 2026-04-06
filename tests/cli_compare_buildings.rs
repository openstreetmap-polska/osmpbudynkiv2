use assert_cmd::Command;
use predicates::prelude::*;

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("osmpbudynkiv2").unwrap();
    cmd.env("NO_COLOR", "1");
    cmd
}

fn persistent_config() -> (tempfile::NamedTempFile, tempfile::TempDir, tempfile::TempDir) {
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
    (cfg, db_dir, rocksdb_dir)
}

fn import_all(cfg_path: &str) {
    cmd()
        .args(["--config", cfg_path, "import", "osm", "--file", "fixtures/osm.pbf"])
        .assert()
        .success();
    cmd()
        .args(["--config", cfg_path, "import", "bdot10k", "--file", "fixtures/bdot10k.parquet"])
        .assert()
        .success();
    cmd()
        .args(["--config", cfg_path, "import", "egib", "--file", "fixtures/egib.parquet"])
        .assert()
        .success();
}

#[test]
fn test_compare_buildings_both() {
    let (cfg, _db_dir, _rocksdb_dir) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_all(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("BDOT10k comparison complete")
                .and(predicate::str::contains("total=74"))
                .and(predicate::str::contains("EGIB comparison complete")),
        );
}
