use std::path::{Path, PathBuf};

use assert_cmd::Command;
use duckdb::Connection;
use predicates::prelude::*;

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("osmpbudynkiv2").unwrap();
    cmd.env("NO_COLOR", "1");
    cmd
}

fn persistent_config() -> (
    tempfile::NamedTempFile,
    tempfile::TempDir,
    tempfile::TempDir,
    PathBuf,
) {
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
    (cfg, db_dir, rocksdb_dir, db_path)
}

fn import_osm(cfg_path: &str) {
    cmd()
        .args([
            "--config",
            cfg_path,
            "import",
            "osm",
            "--file",
            "fixtures/osm.pbf",
        ])
        .assert()
        .success();
}

fn import_bdot10k(cfg_path: &str) {
    cmd()
        .args([
            "--config",
            cfg_path,
            "import",
            "bdot10k",
            "--file",
            "fixtures/bdot10k.parquet",
        ])
        .assert()
        .success();
}

fn import_egib(cfg_path: &str) {
    cmd()
        .args([
            "--config",
            cfg_path,
            "import",
            "egib",
            "--file",
            "fixtures/egib.parquet",
        ])
        .assert()
        .success();
}

fn import_prg(cfg_path: &str) {
    cmd()
        .args([
            "--config",
            cfg_path,
            "import",
            "prg",
            "--file",
            "fixtures/prg.zip",
            "--terc-file",
            "fixtures/teryt.zip",
        ])
        .assert()
        .success();
}

fn import_all(cfg_path: &str) {
    import_osm(cfg_path);
    import_bdot10k(cfg_path);
    import_egib(cfg_path);
    import_prg(cfg_path);
}

/// Query the row count of a `*_unmatched` serving table.
fn unmatched_count(db_path: &Path, table: &str) -> i64 {
    let conn = Connection::open(db_path).unwrap();
    conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })
    .unwrap()
}

/// Returns true if `table` exists in the main schema.
fn table_exists(db_path: &Path, table: &str) -> bool {
    let conn = Connection::open(db_path).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = ?",
            [table],
            |row| row.get(0),
        )
        .unwrap();
    count > 0
}

#[test]
fn test_compare_buildings_both() {
    let (cfg, _db_dir, _rocksdb_dir, _db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_all(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("buildings comparison complete")
                .and(predicate::str::contains(r#"source="bdot10k""#))
                .and(predicate::str::contains(r#"source="egib""#))
                .and(predicate::str::contains("total=74")),
        );
}

#[test]
fn test_compare_buildings_bdot10k_only() {
    let (cfg, _db_dir, _rocksdb_dir, _db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_all(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings", "bdot10k"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains(r#"source="bdot10k""#)
                .and(predicate::str::contains("total=74"))
                .and(predicate::str::contains(r#"source="egib""#).not()),
        );
}

#[test]
fn test_compare_buildings_egib_only() {
    let (cfg, _db_dir, _rocksdb_dir, _db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_all(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings", "egib"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains(r#"source="egib""#)
                .and(predicate::str::contains("total=74"))
                .and(predicate::str::contains(r#"source="bdot10k""#).not()),
        );
}

#[test]
fn test_compare_buildings_without_imported_data_fails() {
    let (cfg, _db_dir, _rocksdb_dir, _db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    // No imports — comparison should fail because source tables don't exist
    cmd()
        .args(["--config", &cfg_path, "compare", "buildings"])
        .assert()
        .failure();
}

#[test]
fn test_compare_full() {
    let (cfg, _db_dir, _rocksdb_dir, _db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_all(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "full"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("buildings comparison complete")
                .and(predicate::str::contains(r#"source="bdot10k""#))
                .and(predicate::str::contains(r#"source="egib""#))
                .and(predicate::str::contains("total=74"))
                .and(predicate::str::contains("PRG comparison complete")),
        );
}

#[test]
fn test_compare_buildings_all() {
    let (cfg, _db_dir, _rocksdb_dir, _db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_all(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings", "all"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("buildings comparison complete")
                .and(predicate::str::contains(r#"source="bdot10k""#))
                .and(predicate::str::contains(r#"source="egib""#))
                .and(predicate::str::contains("total=74")),
        );
}

/// Correctness: verify the comparison actually produces the expected unmatched
/// set (not just that the command runs and logs "complete"). The fixture has
/// 74 rows in each source table and exactly one real match against OSM
/// building 947235698 (a way), with the other OSM building — relation
/// 1891415, a school — never matching because no government centroid falls
/// inside it. So each `*_unmatched` serving table ends up with 73 rows, and
/// the matched id is absent from it.
#[test]
fn test_compare_buildings_correctness() {
    let (cfg, _db_dir, _rocksdb_dir, db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_all(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings"])
        .assert()
        .success();

    // Row counts: 73 of 74 are unmatched (1 real match).
    assert_eq!(
        unmatched_count(&db_path, "bdot10k_unmatched"),
        73,
        "bdot10k_unmatched: expected 73 unmatched rows"
    );
    assert_eq!(
        unmatched_count(&db_path, "egib_unmatched"),
        73,
        "egib_unmatched: expected 73 unmatched rows"
    );

    let conn = Connection::open(&db_path).unwrap();

    // The matched BDOT10k row must be absent from the serving table.
    let bdot10k_matched_present: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM bdot10k_unmatched
             WHERE LOKALNYID = '38F62226-DC07-F520-E053-CA2BA8C0BE14'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        bdot10k_matched_present, 0,
        "the matched BDOT10k building must not appear in bdot10k_unmatched"
    );

    // The matched EGIB row must be absent from the serving table.
    let egib_matched_present: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM egib_unmatched WHERE id_budynku = '146505_8.0110.32_BUD'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        egib_matched_present, 0,
        "the matched EGIB building must not appear in egib_unmatched"
    );

    // Every row in the serving tables must carry cell tags and a timestamp.
    let bad_bdot10k: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM bdot10k_unmatched
             WHERE cell_x IS NULL OR cell_y IS NULL OR computed_at IS NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(bad_bdot10k, 0);
    let bad_egib: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM egib_unmatched
             WHERE cell_x IS NULL OR cell_y IS NULL OR computed_at IS NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(bad_egib, 0);
}

/// Running `compare buildings` twice in a row should produce identical
/// serving tables — `compare` clears then re-inserts each time.
#[test]
fn test_compare_buildings_is_idempotent() {
    let (cfg, _db_dir, _rocksdb_dir, db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_all(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings"])
        .assert()
        .success();
    let first_bdot10k = unmatched_count(&db_path, "bdot10k_unmatched");
    let first_egib = unmatched_count(&db_path, "egib_unmatched");

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings"])
        .assert()
        .success();
    let second_bdot10k = unmatched_count(&db_path, "bdot10k_unmatched");
    let second_egib = unmatched_count(&db_path, "egib_unmatched");

    assert_eq!(first_bdot10k, second_bdot10k);
    assert_eq!(first_egib, second_egib);
}

/// `compare full` should fail early when the source tables don't exist,
/// mirroring the behavior of `compare buildings` in that state.
#[test]
fn test_compare_full_without_imported_data_fails() {
    let (cfg, _db_dir, _rocksdb_dir, _db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();

    cmd()
        .args(["--config", &cfg_path, "compare", "full"])
        .assert()
        .failure();
}

/// Partial-import behavior: if BDOT10k is imported but EGIB is not,
/// `compare buildings` runs BDOT10k first, writes its serving table, then
/// fails when it tries to read the missing `egib_buildings` source table.
/// This test documents that behavior — the first stage's output persists
/// and there is no transactional rollback across stages.
///
/// Note: `bdot10k_unmatched`/`egib_unmatched` are created once by `init_db`
/// and always exist, unlike the old `*_comparison` tables which were
/// drop/created per stage. The EGIB stage's `DELETE FROM egib_unmatched`
/// succeeds (it's a no-op on the already-empty table) before the first
/// INSERT fails on the missing `egib_buildings` source, so `egib_unmatched`
/// is left empty after the failure.
#[test]
fn test_compare_buildings_partial_imports_fails_after_bdot10k_ran() {
    let (cfg, _db_dir, _rocksdb_dir, db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_osm(&cfg_path);
    import_bdot10k(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings"])
        .assert()
        .failure();

    // BDOT10k stage wrote its full output before EGIB stage failed.
    assert!(table_exists(&db_path, "bdot10k_unmatched"));
    assert_eq!(unmatched_count(&db_path, "bdot10k_unmatched"), 73);

    // egib_unmatched exists (created by init_db) but stayed empty: the
    // stage's DELETE ran, but the source-reading INSERT failed.
    assert!(table_exists(&db_path, "egib_unmatched"));
    assert_eq!(unmatched_count(&db_path, "egib_unmatched"), 0);
}
