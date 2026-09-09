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

/// A landed refresh must enqueue the cells it touched into `tile_dirty_cells`,
/// end to end through the real CLI.
///
/// The tile half is not implied by the match half: a refresh rewrites the raw
/// source tables, which `/tiles`' `addresses_all`/`buildings_all` layers read
/// directly, so those layers are stale the moment the apply commits -- before
/// any drain runs, and regardless of whether the delta changed a match
/// decision. `import` deliberately enqueues nothing (it wipes the store
/// instead -- see `cli_import_bdot10k`'s twin test), so a non-empty queue here
/// can only have come from the refresh.
#[test]
fn test_update_bdot10k_enqueues_dirty_tiles() {
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

    let queued_after_import: i64 = {
        let conn = duckdb::Connection::open(&db_path).unwrap();
        conn.execute_batch("INSTALL spatial; LOAD spatial;")
            .unwrap();
        conn.query_row("SELECT COUNT(*) FROM tile_dirty_cells", [], |row| {
            row.get(0)
        })
        .unwrap()
    };
    assert_eq!(
        queued_after_import, 0,
        "an import wipes the store rather than enqueueing, so the queue starts empty"
    );

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
    let queued_after_update: i64 = conn
        .query_row("SELECT COUNT(*) FROM tile_dirty_cells", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert!(
        queued_after_update > 0,
        "a landed refresh must enqueue the cells it touched for tile regeneration"
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
