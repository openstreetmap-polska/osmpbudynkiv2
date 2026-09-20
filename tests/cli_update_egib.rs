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
fn test_update_egib_applies_delta_and_records_changeset() {
    let (cfg, _dir, db_path) = file_config();

    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "import",
            "egib",
            "--file",
            "fixtures/egib.parquet",
        ])
        .assert()
        .success();

    // The v2 fixture drops the lexicographically smallest id_budynku and bumps
    // the largest one's storey count (see fixtures/scripts/prepare_update_
    // fixtures.sh). Read both out of v1 now, so the assertions below can name
    // the exact rows rather than inferring the delta from a row count -- which
    // cannot tell "the right row was removed" from "some row was removed".
    let (removed_id, modified_id, storeys_before): (String, String, Option<i32>) = {
        let conn = duckdb::Connection::open(&db_path).unwrap();
        conn.execute_batch("INSTALL spatial; LOAD spatial;")
            .unwrap();
        conn.query_row(
            "SELECT min(id_budynku), max(id_budynku),
                    max_by(kondygnacje_nadziemne, id_budynku)
             FROM egib_buildings",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap()
    };

    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "update",
            "egib",
            "--file",
            "fixtures/egib_v2.parquet",
        ])
        .assert()
        .success();

    let conn = duckdb::Connection::open(&db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;")
        .unwrap();

    // v2 has 1 added, 1 removed, 1 modified relative to v1.
    let (added, modified, removed): (i32, i32, i32) = conn
        .query_row(
            "SELECT added, modified, removed FROM dataset_refreshes WHERE source = 'egib'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!((added, modified, removed), (1, 1, 1));

    // Row count is unchanged (one in, one out); the EGIB fixture has 74 rows
    // (see tests/cli_import_egib.rs: test_import_egib_from_fixture asserts
    // count=74).
    let total: i64 = conn
        .query_row("SELECT COUNT(*) FROM egib_buildings", [], |r| r.get(0))
        .unwrap();
    assert_eq!(total, 74);

    let added_present: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM egib_buildings WHERE id_budynku LIKE '%_ADDED'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(added_present, 1);

    // The DELETE half of the apply actually removed the right row...
    let removed_left: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM egib_buildings WHERE id_budynku = ?",
            duckdb::params![removed_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(removed_left, 0, "the removed record must be gone");

    // ...and the INSERT half wrote the modified record's NEW value, not the
    // old one. Without this, the DELETE+INSERT could re-insert the live row
    // and the counts above would still read 1/1/1.
    let storeys_after: Option<i32> = conn
        .query_row(
            "SELECT kondygnacje_nadziemne FROM egib_buildings WHERE id_budynku = ?",
            duckdb::params![modified_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        storeys_after,
        Some(storeys_before.unwrap_or(0) + 1),
        "the modified record must carry v2's storey count"
    );

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
             WHERE table_name = 'egib_buildings__staging'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(staging, 0, "staging table leaked");
}

#[test]
fn test_update_egib_unchanged_snapshot_is_a_noop() {
    let (cfg, _dir, db_path) = file_config();

    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "import",
            "egib",
            "--file",
            "fixtures/egib.parquet",
        ])
        .assert()
        .success();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "update",
            "egib",
            "--file",
            "fixtures/egib.parquet",
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

/// The twin of `cli_update_bdot10k`'s `test_update_bdot10k_enqueues_dirty_tiles`,
/// which EGIB was missing. Each source reaches `insert_dirty_cells` through
/// its own `DatasetSpec`, and a polygon source's cell comes from its stored
/// `centroid` column (`representative_point_sql`) rather than `geom` -- a
/// column that never self-heals, so a loader that stopped populating it would
/// silently stop enqueueing here while every unit test, which uses a
/// point-geometry spec, kept passing.
///
/// `import` deliberately enqueues nothing (it wipes the tile store instead --
/// see `cli_import_egib`'s twin test), so a non-empty queue here can only
/// have come from the refresh.
#[test]
fn test_update_egib_enqueues_dirty_tiles() {
    let (cfg, _dir, db_path) = file_config();

    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "import",
            "egib",
            "--file",
            "fixtures/egib.parquet",
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
            "egib",
            "--file",
            "fixtures/egib_v2.parquet",
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
fn test_update_egib_missing_file_fails() {
    let (cfg, _dir, _db) = file_config();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "import",
            "egib",
            "--file",
            "fixtures/egib.parquet",
        ])
        .assert()
        .success();
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "update",
            "egib",
            "--file",
            "nonexistent.parquet",
        ])
        .assert()
        .failure();
}
