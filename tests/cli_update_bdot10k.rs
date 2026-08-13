use assert_cmd::Command;

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("osmpbudynkiv2").unwrap();
    cmd.env("NO_COLOR", "1");
    cmd
}

/// Update needs a file-backed database: import and update are separate
/// process invocations, so ":memory:" would start each with an empty DB.
fn file_config() -> (
    tempfile::NamedTempFile,
    tempfile::TempDir,
    std::path::PathBuf,
) {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("test.duckdb");
    let rocksdb_path = dir.path().join("test.rocksdb");
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    write!(
        tmp,
        "db_path = \"{}\"\nrocksdb_path = \"{}\"\n",
        db_path.display(),
        rocksdb_path.display()
    )
    .unwrap();
    (tmp, dir, db_path)
}

#[test]
fn test_update_bdot10k_applies_delta_and_records_changeset() {
    let (cfg, _dir, db_path) = file_config();

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
        .success();

    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "update",
            "bdot10k",
            "--file",
            "fixtures/bdot10k_v2.parquet",
        ])
        .assert()
        .success();

    let conn = duckdb::Connection::open(&db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;")
        .unwrap();

    // v2 has 1 added, 1 removed, 1 modified relative to v1.
    let (added, modified, removed): (i32, i32, i32) = conn
        .query_row(
            "SELECT added, modified, removed FROM dataset_refreshes WHERE source = 'bdot10k'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!((added, modified, removed), (1, 1, 1));

    // Row count is unchanged (one in, one out) and the added row is present.
    let total: i64 = conn
        .query_row("SELECT COUNT(*) FROM bdot10k_buildings", [], |r| r.get(0))
        .unwrap();
    assert_eq!(total, 74);

    let added_present: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM bdot10k_buildings WHERE LOKALNYID LIKE '%_ADDED'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(added_present, 1);

    let cells: i64 = conn
        .query_row("SELECT COUNT(*) FROM dataset_change_areas", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(cells > 0, "expected change areas to be recorded");

    // Staging must not survive the run.
    let staging: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables
             WHERE table_name = 'bdot10k_buildings__staging'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(staging, 0, "staging table leaked");
}

#[test]
fn test_update_bdot10k_unchanged_snapshot_is_a_noop() {
    let (cfg, _dir, db_path) = file_config();

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
        .success();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "update",
            "bdot10k",
            "--file",
            "fixtures/bdot10k.parquet",
        ])
        .assert()
        .success();

    let conn = duckdb::Connection::open(&db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;")
        .unwrap();
    let (added, modified, removed): (i32, i32, i32) = conn
        .query_row(
            "SELECT added, modified, removed FROM dataset_refreshes",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!((added, modified, removed), (0, 0, 0));

    let cells: i64 = conn
        .query_row("SELECT COUNT(*) FROM dataset_change_areas", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(cells, 0);
}

/// A landed refresh rewrites raw columns (`centroid`) `/tiles`' adjacency
/// CTE reads outside the diffed row set, so `update::dataset::refresh` must
/// bump `metadata.serving_epoch` on every landed refresh, not just a
/// non-empty diff -- see `serving_version`'s module doc. `import` itself
/// also bumps (pinned by `cli_import_bdot10k`'s twin test), so this asserts
/// the epoch moved again on top of that, i.e. `update` bumps too rather than
/// relying on the import's earlier bump.
#[test]
fn test_update_bdot10k_bumps_serving_epoch() {
    let (cfg, _dir, db_path) = file_config();

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
        .success();

    let epoch_after_import: String = {
        let conn = duckdb::Connection::open(&db_path).unwrap();
        conn.execute_batch("INSTALL spatial; LOAD spatial;")
            .unwrap();
        conn.query_row(
            "SELECT value FROM metadata WHERE key = 'serving_epoch'",
            [],
            |row| row.get(0),
        )
        .expect("import must bump metadata.serving_epoch")
    };

    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "update",
            "bdot10k",
            "--file",
            "fixtures/bdot10k_v2.parquet",
        ])
        .assert()
        .success();

    let conn = duckdb::Connection::open(&db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;")
        .unwrap();
    let epoch_after_update: String = conn
        .query_row(
            "SELECT value FROM metadata WHERE key = 'serving_epoch'",
            [],
            |row| row.get(0),
        )
        .expect("update must also bump metadata.serving_epoch");
    assert_ne!(
        epoch_after_import, epoch_after_update,
        "update must move the epoch again on top of import's own bump"
    );
}

#[test]
fn test_update_bdot10k_missing_file_fails() {
    let (cfg, _dir, _db) = file_config();
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
        .success();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "update",
            "bdot10k",
            "--file",
            "nonexistent.parquet",
        ])
        .assert()
        .failure();
}
