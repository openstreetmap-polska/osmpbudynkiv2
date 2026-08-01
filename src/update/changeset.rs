use anyhow::{Context, Result};
use duckdb::Connection;

use crate::dataset::DatasetSpec;
use crate::tile_math::CHANGE_CELL_ZOOM;

/// Aggregate the diff tables into per-tile change counts and insert them
/// into `dataset_change_areas`. Returns the number of cell rows written.
///
/// Must be called inside the caller's transaction so the changeset commits
/// atomically with the data delta it describes.
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
/// That is intended — consumers use these cells to decide what to re-render,
/// not to report object counts.
pub fn insert_change_areas(conn: &Connection, spec: &DatasetSpec, snapshot_id: i64) -> Result<i64> {
    let live = spec.table;
    let staging = spec.staging_table();
    let id = spec.id_column;
    let z = CHANGE_CELL_ZOOM;

    let point_live = spec.representative_point_sql("l");
    let point_stg = spec.representative_point_sql("s");

    let sx = crate::tile_math::cell_x_sql(&point_stg);
    let sy = crate::tile_math::cell_y_sql(&point_stg);
    let lx = crate::tile_math::cell_x_sql(&point_live);
    let ly = crate::tile_math::cell_y_sql(&point_live);

    let sql = format!(
        "INSERT INTO dataset_change_areas
         SELECT {snapshot_id}, '{source}', {z}, cell_x, cell_y,
                COUNT(*) FILTER (WHERE kind = 'added')::INTEGER,
                COUNT(*) FILTER (WHERE kind = 'modified')::INTEGER,
                COUNT(*) FILTER (WHERE kind = 'removed')::INTEGER,
                now()
         FROM (
             SELECT 'added' AS kind, {sx} AS cell_x, {sy} AS cell_y
             FROM {staging} s JOIN diff_added d ON s.{id} = d.id
             WHERE s.geom IS NOT NULL
             UNION ALL
             SELECT 'removed', {lx}, {ly}
             FROM {live} l JOIN diff_removed d ON l.{id} = d.id
             WHERE l.geom IS NOT NULL
             UNION ALL
             SELECT 'modified', {sx}, {sy}
             FROM {staging} s JOIN diff_modified d ON s.{id} = d.id
             WHERE s.geom IS NOT NULL
             UNION ALL
             SELECT 'modified', {lx}, {ly}
             FROM {live} l JOIN diff_modified d ON l.{id} = d.id
             WHERE l.geom IS NOT NULL
         )
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
/// inside the apply transaction so the queue commits atomically with the delta.
pub fn insert_dirty_cells(conn: &Connection, spec: &DatasetSpec) -> Result<()> {
    let live = spec.table;
    let staging = spec.staging_table();
    let id = spec.id_column;
    let z = crate::tile_math::CHANGE_CELL_ZOOM;
    let point_live = spec.representative_point_sql("l");
    let point_stg = spec.representative_point_sql("s");
    let sx = crate::tile_math::cell_x_sql(&point_stg);
    let sy = crate::tile_math::cell_y_sql(&point_stg);
    let lx = crate::tile_math::cell_x_sql(&point_live);
    let ly = crate::tile_math::cell_y_sql(&point_live);

    let sql = format!(
        "INSERT INTO match_dirty_cells
         SELECT DISTINCT '{source}', {z}, cell_x, cell_y, now()
         FROM (
             SELECT {sx} AS cell_x, {sy} AS cell_y
             FROM {staging} s JOIN diff_added d ON s.{id} = d.id WHERE s.geom IS NOT NULL
             UNION
             SELECT {lx}, {ly}
             FROM {live} l JOIN diff_removed d ON l.{id} = d.id WHERE l.geom IS NOT NULL
             UNION
             SELECT {sx}, {sy}
             FROM {staging} s JOIN diff_modified d ON s.{id} = d.id WHERE s.geom IS NOT NULL
             UNION
             SELECT {lx}, {ly}
             FROM {live} l JOIN diff_modified d ON l.{id} = d.id WHERE l.geom IS NOT NULL
         )",
        source = spec.name,
    );
    conn.execute_batch(&sql)
        .with_context(|| format!("Failed to enqueue dirty cells for {}", spec.name))
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
        id_column: "id",
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
}
