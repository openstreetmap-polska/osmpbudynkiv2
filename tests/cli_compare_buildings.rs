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

/// Query `(total, matched)` counts from a comparison table.
fn comparison_counts(db_path: &Path, table: &str) -> (i64, i64) {
    let conn = Connection::open(db_path).unwrap();
    conn.query_row(
        &format!("SELECT COUNT(*), COUNT(*) FILTER (WHERE matched) FROM {table}"),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
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
            predicate::str::contains("BDOT10k comparison complete")
                .and(predicate::str::contains("total=74"))
                .and(predicate::str::contains("EGIB comparison complete")),
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
            predicate::str::contains("BDOT10k comparison complete")
                .and(predicate::str::contains("total=74"))
                .and(predicate::str::contains("EGIB comparison complete").not()),
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
            predicate::str::contains("EGIB comparison complete")
                .and(predicate::str::contains("total=74"))
                .and(predicate::str::contains("BDOT10k comparison complete").not()),
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
            predicate::str::contains("BDOT10k comparison complete")
                .and(predicate::str::contains("total=74"))
                .and(predicate::str::contains("EGIB comparison complete"))
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
            predicate::str::contains("BDOT10k comparison complete")
                .and(predicate::str::contains("total=74"))
                .and(predicate::str::contains("EGIB comparison complete")),
        );
}

/// Correctness: verify the comparison actually produces the expected matches
/// (not just that the command runs and logs "complete"). The fixture has 74
/// rows in each source table and exactly one real match against OSM building
/// 947235698 (a way), with the other OSM building — relation 1891415, a
/// school — never matching because no government centroid falls inside it.
#[test]
fn test_compare_buildings_correctness() {
    let (cfg, _db_dir, _rocksdb_dir, db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_all(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings"])
        .assert()
        .success();

    // Row counts
    assert_eq!(
        comparison_counts(&db_path, "bdot10k_comparison"),
        (74, 1),
        "bdot10k_comparison: expected (total=74, matched=1)"
    );
    assert_eq!(
        comparison_counts(&db_path, "egib_comparison"),
        (74, 1),
        "egib_comparison: expected (total=74, matched=1)"
    );

    let conn = Connection::open(&db_path).unwrap();

    // Specific matched row — BDOT10k
    let (lokalnyid, osm_id, osm_type): (String, i64, String) = conn
        .query_row(
            "SELECT lokalnyid, matched_osm_id, matched_osm_type
             FROM bdot10k_comparison WHERE matched",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(lokalnyid, "38F62226-DC07-F520-E053-CA2BA8C0BE14");
    assert_eq!(osm_id, 947235698);
    assert_eq!(osm_type, "way");

    // Specific matched row — EGIB
    let (id_budynku, osm_id, osm_type): (String, i64, String) = conn
        .query_row(
            "SELECT id_budynku, matched_osm_id, matched_osm_type
             FROM egib_comparison WHERE matched",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(id_budynku, "146505_8.0110.32_BUD");
    assert_eq!(osm_id, 947235698);
    assert_eq!(osm_type, "way");

    // Unmatched rows must have NULL osm_id / osm_type.
    let bad_bdot10k: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM bdot10k_comparison
             WHERE NOT matched AND (matched_osm_id IS NOT NULL OR matched_osm_type IS NOT NULL)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(bad_bdot10k, 0);
    let bad_egib: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM egib_comparison
             WHERE NOT matched AND (matched_osm_id IS NOT NULL OR matched_osm_type IS NOT NULL)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(bad_egib, 0);
}

/// Running `compare buildings` twice in a row should produce identical
/// result tables — `compare_chunked` drops and recreates them each time.
#[test]
fn test_compare_buildings_is_idempotent() {
    let (cfg, _db_dir, _rocksdb_dir, db_path) = persistent_config();
    let cfg_path = cfg.path().to_str().unwrap().to_string();
    import_all(&cfg_path);

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings"])
        .assert()
        .success();
    let first_bdot10k = comparison_counts(&db_path, "bdot10k_comparison");
    let first_egib = comparison_counts(&db_path, "egib_comparison");

    cmd()
        .args(["--config", &cfg_path, "compare", "buildings"])
        .assert()
        .success();
    let second_bdot10k = comparison_counts(&db_path, "bdot10k_comparison");
    let second_egib = comparison_counts(&db_path, "egib_comparison");

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
/// `compare buildings` runs BDOT10k first, writes its result table, then
/// fails when it tries to read the missing `egib_buildings` source table.
/// This test documents that behavior — the first stage's output persists
/// and there is no transactional rollback across stages.
///
/// Note: `compare_chunked` creates the result table before the grid loop
/// reads the source, so `egib_comparison` is left as an *empty* table
/// after the failure. If you ever move the DROP/CREATE to happen lazily
/// (or wrap stages in a transaction), update this test.
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
    assert!(table_exists(&db_path, "bdot10k_comparison"));
    assert_eq!(comparison_counts(&db_path, "bdot10k_comparison"), (74, 1));

    // EGIB stage created its result table (empty) before the first INSERT
    // failed on the missing egib_buildings source.
    assert!(table_exists(&db_path, "egib_comparison"));
    assert_eq!(comparison_counts(&db_path, "egib_comparison"), (0, 0));
}
