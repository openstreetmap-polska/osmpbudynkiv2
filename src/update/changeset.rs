use anyhow::{Context, Result};
use duckdb::Connection;

use crate::dataset::DatasetSpec;
use crate::tile_math::CHANGE_CELL_ZOOM;

/// Temp table holding this refresh's per-row cell set, built once by
/// [`create_changed_cells`] and read by all three consumers.
pub const CHANGED_CELLS_TABLE: &str = "refresh_changed_cells";

/// Materialize, once, the (kind, cell) row set that `insert_change_areas` and
/// both halves of `insert_dirty_cells` all need.
///
/// **Why this is a table and not three copies of one SELECT.** The cell set is
/// derived by joining the diff tables back against `live`/`staging`, and
/// neither carries an index on the key columns -- the only indexes in the
/// schema are the RTREE ones on `geom`/`centroid` -- so every such join is a
/// full scan of a 16-18M row table. Spelling the four-way union out at each
/// consumer cost twelve of those scans per apply, whether the delta held
/// 115 rows or 170,000: measured on the real EGIB tables, that was the bulk of
/// a churn-independent apply floor. Building it once costs two scans and the
/// consumers then read a table of roughly `churn` rows.
///
/// **Two scans, not four.** `staging` is read once for added+modified and
/// `live` once for removed+modified, with `kind` carried through the join
/// rather than fixed per branch. The row set is identical to the four-branch
/// form: `diff_added` and `diff_modified` are disjoint by construction (the
/// former is an ANTI JOIN on keys absent from `live`, the latter an inner join
/// on keys present in both), as are `diff_removed` and `diff_modified`.
///
/// **`UNION ALL`, deliberately.** A modified row contributes its cell twice --
/// once from `live`, once from `staging` -- which is what lets
/// `insert_change_areas` count an object that moved in both the cell it left
/// and the cell it entered, and what makes a modified-but-stationary object
/// count 2 for its cell. See `insert_change_areas`' doc. The two queue
/// consumers apply their own `DISTINCT`, so deduplicating here would change
/// the change-area counts while leaving the queues identical.
///
/// Must run **before** the apply transaction's delta: the `live` half reads
/// the pre-update geometry of removed and modified rows, which the DELETE
/// would otherwise have thrown away. It is a temp table, so building it
/// outside the caller's transaction lands nothing durable.
pub fn create_changed_cells(conn: &Connection, spec: &DatasetSpec) -> Result<()> {
    let live = spec.table;
    let staging = spec.staging_table();
    let keys = spec.key_columns.join(", ");

    let point_live = spec.representative_point_sql("l");
    let point_stg = spec.representative_point_sql("s");
    let sx = crate::tile_math::cell_x_sql(&point_stg);
    let sy = crate::tile_math::cell_y_sql(&point_stg);
    let lx = crate::tile_math::cell_x_sql(&point_live);
    let ly = crate::tile_math::cell_y_sql(&point_live);

    let sql = format!(
        "DROP TABLE IF EXISTS {CHANGED_CELLS_TABLE};
         CREATE TEMP TABLE {CHANGED_CELLS_TABLE} AS
         SELECT d.kind AS kind, {sx} AS cell_x, {sy} AS cell_y
         FROM {staging} s JOIN (
             SELECT {keys}, 'added' AS kind FROM diff_added
             UNION ALL
             SELECT {keys}, 'modified' AS kind FROM diff_modified
         ) d USING ({keys})
         WHERE s.geom IS NOT NULL
         UNION ALL
         SELECT d.kind AS kind, {lx} AS cell_x, {ly} AS cell_y
         FROM {live} l JOIN (
             SELECT {keys}, 'removed' AS kind FROM diff_removed
             UNION ALL
             SELECT {keys}, 'modified' AS kind FROM diff_modified
         ) d USING ({keys})
         WHERE l.geom IS NOT NULL"
    );

    conn.execute_batch(&sql)
        .with_context(|| format!("Failed to build changed-cell set for {}", spec.name))
}

/// Aggregate the diff tables into per-tile change counts and insert them
/// into `dataset_change_areas`. Returns the number of cell rows written.
///
/// Must be called inside the caller's transaction so the changeset commits
/// atomically with the data delta it describes, and after
/// [`create_changed_cells`] has built the row set it reads.
///
/// Contributions:
/// - added: new geometry (from staging)
/// - removed: old geometry (from live)
/// - modified: BOTH old and new geometry, so an object that moves marks the
///   cell it left as well as the cell it entered.
///
/// Rows with NULL geometry contribute no cell (they have no location), but
/// are still counted in `dataset_refreshes`.
///
/// The counts measure churn events touching a cell, not distinct objects: a
/// modified object that did NOT move contributes its cell twice (once from
/// live, once from staging), so that cell's `modified` is 2 for one object.
/// That is intended -- consumers use these cells to decide what to re-render,
/// not to report object counts.
pub fn insert_change_areas(conn: &Connection, spec: &DatasetSpec, snapshot_id: i64) -> Result<i64> {
    let z = CHANGE_CELL_ZOOM;

    let sql = format!(
        "INSERT INTO dataset_change_areas
         SELECT {snapshot_id}, '{source}', {z}, cell_x, cell_y,
                COUNT(*) FILTER (WHERE kind = 'added')::INTEGER,
                COUNT(*) FILTER (WHERE kind = 'modified')::INTEGER,
                COUNT(*) FILTER (WHERE kind = 'removed')::INTEGER,
                now()
         FROM {CHANGED_CELLS_TABLE}
         GROUP BY cell_x, cell_y",
        source = spec.name,
    );

    conn.execute_batch(&sql)
        .with_context(|| format!("Failed to write change areas for {}", spec.name))?;

    conn.query_row(
        "SELECT COUNT(*) FROM dataset_change_areas WHERE snapshot_id = ?",
        duckdb::params![snapshot_id],
        |row| row.get(0),
    )
    .context("Failed to count inserted change areas")
}

/// Enqueue one dirty-cell row per distinct z14 cell this refresh touches
/// (added from staging, removed/modified from both live and staging). Must run
/// inside the apply transaction so the queue commits atomically with the delta,
/// and after [`create_changed_cells`] has built the row set it reads.
///
/// Feeds **both** queues from the one materialized cell set, and the tile half
/// is not redundant with the match half. A refresh rewrites the raw source
/// tables, which `/tiles`' `addresses_all`/`buildings_all` layers read
/// directly -- so those layers are stale the moment the apply commits, before
/// any drain has run. Waiting for the drain to enqueue the tile would leave a
/// window where the legend layers show the old rows; and a refresh whose delta
/// changes no match decision would never close it at all.
pub fn insert_dirty_cells(conn: &Connection, spec: &DatasetSpec) -> Result<()> {
    let z = crate::tile_math::CHANGE_CELL_ZOOM;

    let sql = format!(
        "INSERT INTO match_dirty_cells
         SELECT DISTINCT '{source}', {z}, cell_x, cell_y, now()
         FROM {CHANGED_CELLS_TABLE}",
        source = spec.name,
    );
    conn.execute_batch(&sql)
        .with_context(|| format!("Failed to enqueue dirty cells for {}", spec.name))?;

    let tile_sql = crate::server::tile_dirty::enqueue_from_select_sql(
        "cell_x",
        "cell_y",
        &format!("FROM {CHANGED_CELLS_TABLE}"),
    );
    conn.execute_batch(&tile_sql)
        .with_context(|| format!("Failed to enqueue dirty tiles for {}", spec.name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{DatasetSpec, GeomKind};
    use crate::db::init_db;
    use crate::tile_math::lonlat_to_tile;
    use std::path::Path;

    const TEST_SPEC: DatasetSpec = DatasetSpec {
        name: "test",
        table: "live",
        key_columns: &["id"],
        compared_columns: &["a"],
        compare_geometry: true,
        geom_kind: GeomKind::Point,
    };

    /// Build live/staging tables plus the three diff tables by hand, so this
    /// test does not depend on the diff engine's internals.
    fn setup() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE live AS
                 SELECT * FROM (VALUES
                     ('del', ST_Point(21.0, 52.0)),
                     ('mov', ST_Point(21.0, 52.0))
                 ) t(id, geom);
             CREATE TABLE live__staging AS
                 SELECT * FROM (VALUES
                     ('add', ST_Point(21.0, 52.0)),
                     ('mov', ST_Point(19.0, 50.0))
                 ) t(id, geom);
             CREATE TEMP TABLE diff_added    AS SELECT 'add' AS id;
             CREATE TEMP TABLE diff_removed  AS SELECT 'del' AS id;
             CREATE TEMP TABLE diff_modified AS SELECT 'mov' AS id;",
        )
        .unwrap();
        // Production order: the cell set is materialized once, before the
        // apply transaction, and every consumer below reads it.
        create_changed_cells(&conn, &TEST_SPEC).unwrap();
        conn
    }

    #[test]
    fn aggregates_counts_per_cell() {
        let conn = setup();
        let rows = insert_change_areas(&conn, &TEST_SPEC, 7).unwrap();

        let (home_x, home_y) = lonlat_to_tile(21.0, 52.0, CHANGE_CELL_ZOOM);
        let (added, modified, removed): (i32, i32, i32) = conn
            .query_row(
                "SELECT added, modified, removed FROM dataset_change_areas
                 WHERE cell_x = ? AND cell_y = ?",
                duckdb::params![home_x, home_y],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        // 'add' added here, 'del' removed here, and 'mov' left from here.
        assert_eq!((added, modified, removed), (1, 1, 1));
        assert_eq!(rows, 2, "two distinct cells were touched");
    }

    /// An object that moves marks BOTH the cell it left and the one it entered.
    #[test]
    fn moved_object_marks_both_cells() {
        let conn = setup();
        insert_change_areas(&conn, &TEST_SPEC, 7).unwrap();

        let (dest_x, dest_y) = lonlat_to_tile(19.0, 50.0, CHANGE_CELL_ZOOM);
        let modified: i32 = conn
            .query_row(
                "SELECT modified FROM dataset_change_areas WHERE cell_x = ? AND cell_y = ?",
                duckdb::params![dest_x, dest_y],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(modified, 1, "destination cell must be marked too");
    }

    /// The three count columns are written positionally, so a fixture whose
    /// added/modified/removed totals are all equal cannot catch a transposed
    /// SELECT list. Pin them with three distinct values.
    #[test]
    fn counts_land_in_their_own_columns() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE live AS
                 SELECT * FROM (VALUES
                     ('r1', ST_Point(21.0, 52.0)),
                     ('r2', ST_Point(21.0, 52.0)),
                     ('r3', ST_Point(21.0, 52.0))
                 ) t(id, geom);
             CREATE TABLE live__staging AS
                 SELECT * FROM (VALUES ('a1', ST_Point(21.0, 52.0))) t(id, geom);
             CREATE TEMP TABLE diff_added   AS SELECT 'a1' AS id;
             CREATE TEMP TABLE diff_removed AS
                 SELECT unnest(['r1', 'r2', 'r3']) AS id;
             CREATE TEMP TABLE diff_modified AS SELECT 'x' AS id WHERE false;",
        )
        .unwrap();

        create_changed_cells(&conn, &TEST_SPEC).unwrap();
        insert_change_areas(&conn, &TEST_SPEC, 7).unwrap();

        let (home_x, home_y) = lonlat_to_tile(21.0, 52.0, CHANGE_CELL_ZOOM);
        let counts: (i32, i32, i32) = conn
            .query_row(
                "SELECT added, modified, removed FROM dataset_change_areas
                 WHERE cell_x = ? AND cell_y = ?",
                duckdb::params![home_x, home_y],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(counts, (1, 0, 3));
    }

    #[test]
    fn stamps_snapshot_id_source_and_zoom() {
        let conn = setup();
        insert_change_areas(&conn, &TEST_SPEC, 7).unwrap();

        let (snapshot_id, source, z): (i64, String, i32) = conn
            .query_row(
                "SELECT DISTINCT snapshot_id, source, cell_z FROM dataset_change_areas",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(snapshot_id, 7);
        assert_eq!(source, "test");
        assert_eq!(z, CHANGE_CELL_ZOOM as i32);
    }

    #[test]
    fn enqueues_distinct_touched_cells() {
        let conn = setup();
        insert_dirty_cells(&conn, &TEST_SPEC).unwrap();
        // 'del'/'mov' left the home cell; 'add'/'mov' arrive — 2 distinct cells.
        let (home_x, home_y) = lonlat_to_tile(21.0, 52.0, CHANGE_CELL_ZOOM);
        let (dest_x, dest_y) = lonlat_to_tile(19.0, 50.0, CHANGE_CELL_ZOOM);
        let cells: Vec<(String, i32, i32)> = {
            let mut s = conn
                .prepare(
                    "SELECT source, cell_x, cell_y FROM match_dirty_cells ORDER BY cell_x, cell_y",
                )
                .unwrap();
            s.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            cells.len(),
            2,
            // Pins the enqueued *set*, not the dedup mechanism: the outer
            // SELECT DISTINCT collapses duplicates on its own (now() is
            // statement-stable, so it does not defeat DISTINCT), which means
            // this would still pass if the inner UNION became UNION ALL.
            // Duplicate queue rows are harmless anyway -- the drain dedups.
            "exactly the two distinct touched cells"
        );
        assert!(cells.iter().all(|(s, _, _)| s == "test"));
        assert!(cells.contains(&("test".to_string(), home_x as i32, home_y as i32)));
        assert!(cells.contains(&("test".to_string(), dest_x as i32, dest_y as i32)));
    }

    /// `insert_dirty_cells` spells its four-branch UNION out twice -- once
    /// for `match_dirty_cells`, once for `tile_dirty_cells` -- so the two can
    /// drift. Nothing noticed: deleting three of the four branches from the
    /// TILE copy alone left the whole suite green (mutation-checked,
    /// 2026-09-20), because every fixture that looked at the tile queue put
    /// every row at one point, so any single surviving branch produced the
    /// expected cell.
    ///
    /// Here each branch is the ONLY contributor of its own cell: `add`
    /// arrives in A (staging/diff_added), `del` leaves from B (live/
    /// diff_removed), and `mov` travels D -> C (live and staging sides of
    /// diff_modified). Dropping any one branch from either copy loses exactly
    /// one cell, and asserting the two queues hold the SAME set is what keeps
    /// the duplication honest.
    #[test]
    fn every_diff_branch_contributes_its_cell_to_both_queues() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE live AS
                 SELECT * FROM (VALUES
                     ('del', ST_Point(19.0, 50.0)),
                     ('mov', ST_Point(15.0, 46.0))
                 ) t(id, geom);
             CREATE TABLE live__staging AS
                 SELECT * FROM (VALUES
                     ('add', ST_Point(21.0, 52.0)),
                     ('mov', ST_Point(17.0, 48.0))
                 ) t(id, geom);
             CREATE TEMP TABLE diff_added    AS SELECT 'add' AS id;
             CREATE TEMP TABLE diff_removed  AS SELECT 'del' AS id;
             CREATE TEMP TABLE diff_modified AS SELECT 'mov' AS id;",
        )
        .unwrap();

        create_changed_cells(&conn, &TEST_SPEC).unwrap();
        insert_dirty_cells(&conn, &TEST_SPEC).unwrap();

        let mut expected: Vec<(i32, i32)> = [
            (21.0, 52.0), // added, from staging
            (19.0, 50.0), // removed, from live
            (17.0, 48.0), // modified, from staging (the cell it entered)
            (15.0, 46.0), // modified, from live (the cell it left)
        ]
        .iter()
        .map(|(lon, lat)| {
            let (x, y) = lonlat_to_tile(*lon, *lat, CHANGE_CELL_ZOOM);
            (x as i32, y as i32)
        })
        .collect();
        expected.sort_unstable();
        assert_eq!(
            expected.len(),
            4,
            "the fixture's four points must not share a cell"
        );

        let read = |sql: &str| -> Vec<(i32, i32)> {
            let mut s = conn.prepare(sql).unwrap();
            let mut v: Vec<(i32, i32)> = s
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            v.sort_unstable();
            v
        };

        assert_eq!(
            read("SELECT DISTINCT cell_x, cell_y FROM match_dirty_cells WHERE source = 'test'"),
            expected,
            "every branch must reach the match queue"
        );
        assert_eq!(
            read("SELECT DISTINCT cell_x, cell_y FROM tile_dirty_cells"),
            expected,
            "every branch must reach the tile queue too -- the `*_all` layers \
             read the raw tables, so they are stale the moment the apply commits"
        );
    }
}
