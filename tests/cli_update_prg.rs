//! `update prg`, end to end through the real CLI.
//!
//! PRG had no update coverage at any level before this file: no CLI test, no
//! `prg_v2` fixture, and nothing in `src/` reaching `import::prg::update_prg`
//! or `update::dataset::refresh(&dataset::PRG, ...)`. It is the source with
//! the most unusual staging loader -- `stream_gml_into` fills a `_raw` table
//! from parsed GML, then `materialize_into` projects it into the staging
//! table -- and the only one whose loader normalizes a value the match rule
//! reads (`ULICA_PREFIX_STRIP_SQL`).
//!
//! The fixture pair is `fixtures/prg.zip` -> `fixtures/prg_v2.zip`, built by
//! `fixtures/scripts/prepare_prg_update_fixture.py`. Its delta is the same
//! 1/1/1 shape the BDOT10k and EGIB v2 fixtures use, with one deliberate
//! difference: the modified record is a pure GEOMETRY move with every
//! compared attribute left alone. PRG is `compare_geometry: true`, so that is
//! what makes the modification visible at all -- and it is what lets
//! `test_update_prg_enqueues_the_cell_the_moved_address_left` assert on the
//! origin cell.

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

fn import_v1(cfg: &tempfile::NamedTempFile) {
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
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

fn update_with(cfg: &tempfile::NamedTempFile, file: &str) -> assert_cmd::assert::Assert {
    cmd()
        .args([
            "--config",
            cfg.path().to_str().unwrap(),
            "update",
            "prg",
            "--file",
            file,
            "--terc-file",
            "fixtures/teryt.zip",
        ])
        .assert()
}

fn open(db_path: &std::path::Path) -> duckdb::Connection {
    let conn = duckdb::Connection::open(db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;")
        .unwrap();
    conn
}

#[test]
fn test_update_prg_applies_delta_and_records_changeset() {
    let (cfg, _dir, db_path) = file_config();
    import_v1(&cfg);
    update_with(&cfg, "fixtures/prg_v2.zip").success();

    let conn = open(&db_path);

    let (added, modified, removed): (i32, i32, i32) = conn
        .query_row(
            "SELECT added, modified, removed FROM dataset_refreshes WHERE source = 'prg'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (added, modified, removed),
        (1, 1, 1),
        "v2 drops the smallest housenumber, moves the largest, and adds a copy \
         of it -- the modification is geometry-only, so a `modified` of 0 here \
         means PRG stopped comparing geometry"
    );

    // One in, one out: the fixture's 3 addresses stay 3.
    let total: i64 = conn
        .query_row("SELECT COUNT(*) FROM prg_addresses", [], |r| r.get(0))
        .unwrap();
    assert_eq!(total, 3);

    let housenumbers: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT numer_porzadkowy FROM prg_addresses ORDER BY numer_porzadkowy")
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    };
    assert_eq!(
        housenumbers,
        vec!["21A", "2A", "2A_ADDED"],
        "the added record must be present and the removed one ('1A') gone -- \
         a row count alone cannot tell those two apart"
    );

    let cells: i64 = conn
        .query_row("SELECT COUNT(*) FROM dataset_change_areas", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(cells > 0, "expected change areas to be recorded");

    // Staging must not survive the run -- neither the staging table itself nor
    // the `_raw` table `stream_gml_into` fills on the way to it, which is
    // PRG's alone (the parquet loaders stage in one step).
    for table in ["prg_addresses__staging", "prg_addresses__staging_raw"] {
        let leaked: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                duckdb::params![table],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(leaked, 0, "{table} leaked");
    }
}

/// The moved address has to dirty the cell it LEFT as well as the one it
/// entered, in both queues. Nothing else in the suite drives that through a
/// real government refresh: `changeset::tests` builds the `diff_*` tables by
/// hand, and the BDOT10k/EGIB fixtures modify an attribute in place, so every
/// row they touch stays in one cell.
///
/// The fixture's three contributions land in three different cells (the
/// removed address is ~30 km from the other two, and the modified one moves
/// ~3 km east, more than a z14 cell's ~1340 m width), so a branch that stops
/// contributing is a cell that goes missing rather than a duplicate that
/// disappears.
#[test]
fn test_update_prg_enqueues_the_cell_the_moved_address_left() {
    let (cfg, _dir, db_path) = file_config();
    import_v1(&cfg);

    {
        let conn = open(&db_path);
        let queued: i64 = conn
            .query_row("SELECT COUNT(*) FROM tile_dirty_cells", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            queued, 0,
            "an import wipes the tile store rather than enqueueing, so the \
             queue starts empty"
        );
    }

    update_with(&cfg, "fixtures/prg_v2.zip").success();

    let conn = open(&db_path);
    let match_cells = distinct_cells(
        &conn,
        "SELECT DISTINCT cell_x, cell_y FROM match_dirty_cells WHERE source = 'prg'",
    );
    let tile_cells = distinct_cells(
        &conn,
        "SELECT DISTINCT cell_x, cell_y FROM tile_dirty_cells",
    );

    assert_eq!(
        match_cells.len(),
        3,
        "added, removed, and both ends of the move are four contributions in \
         three distinct cells, got {match_cells:?}"
    );
    assert_eq!(
        tile_cells, match_cells,
        "the tile queue is fed from a second copy of the same four-branch \
         UNION, so the two must agree cell for cell"
    );
}

fn distinct_cells(conn: &duckdb::Connection, sql: &str) -> Vec<(i32, i32)> {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut cells: Vec<(i32, i32)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    cells.sort_unstable();
    cells
}

/// Re-applying the snapshot already loaded must be a clean no-op: no delta,
/// no change areas, and nothing enqueued. A government source that
/// republished an identical snapshot would otherwise flush cached tiles for a
/// change that never happened.
#[test]
fn test_update_prg_unchanged_snapshot_is_a_noop() {
    let (cfg, _dir, db_path) = file_config();
    import_v1(&cfg);
    update_with(&cfg, "fixtures/prg.zip").success();

    let conn = open(&db_path);
    let (added, modified, removed): (i32, i32, i32) = conn
        .query_row(
            "SELECT added, modified, removed FROM dataset_refreshes",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!((added, modified, removed), (0, 0, 0));

    for table in [
        "dataset_change_areas",
        "match_dirty_cells",
        "tile_dirty_cells",
    ] {
        let n: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0, "an identical snapshot must leave {table} empty");
    }
}

#[test]
fn test_update_prg_missing_file_fails() {
    let (cfg, _dir, _db) = file_config();
    import_v1(&cfg);
    update_with(&cfg, "nonexistent.zip").failure();
}
