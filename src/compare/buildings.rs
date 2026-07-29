use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::compare::rule::unmatched_buildings_sql;
use crate::tile_math::{cell_x_sql, cell_y_sql};
use crate::utils::format_duration;

/// Grid cell size in degrees for chunked spatial comparison (memory bound).
const GRID_STEP: f64 = 0.5;

pub fn compare_bdot10k(conn: &Connection) -> Result<()> {
    compare_buildings(
        conn,
        "bdot10k",
        "bdot10k_buildings",
        "LOKALNYID",
        "bdot10k_unmatched",
    )
}

pub fn compare_egib(conn: &Connection) -> Result<()> {
    compare_buildings(
        conn,
        "egib",
        "egib_buildings",
        "id_budynku",
        "egib_unmatched",
    )
}

fn compare_buildings(
    conn: &Connection,
    label: &str,
    source_table: &str,
    id_col: &str,
    dest: &str,
) -> Result<()> {
    info!(source = label, "Comparing buildings against OSM");
    let t = std::time::Instant::now();

    conn.execute_batch(&format!("DELETE FROM {dest};"))
        .with_context(|| format!("Failed to clear {dest}"))?;

    let (min_x, min_y, max_x, max_y) = source_grid_extent(conn, source_table, GRID_STEP)
        .with_context(|| format!("Failed to compute source extent for {source_table}"))?;
    let cx = cell_x_sql("ST_Centroid(b.geom)");
    let cy = cell_y_sql("ST_Centroid(b.geom)");
    let select = format!("b.{id_col}, b.geom, {cx}, {cy}, now()");

    let mut y = min_y;
    while y < max_y {
        let mut x = min_x;
        while x < max_x {
            let (x_hi, y_hi) = (x + GRID_STEP, y + GRID_STEP);
            let area = (x, y, x_hi, y_hi);
            let inner = unmatched_buildings_sql(source_table, &select, area);
            // Write-narrow: unmatched_buildings_sql's ST_Intersects test is
            // closed on all four cell edges, so a centroid exactly on a grid
            // line would satisfy two neighbouring cells' predicates. Restrict
            // the actual write to this cell's half-open interval so a
            // boundary row is written by exactly the cell that owns it (the
            // z14 analogue of this guard lives in
            // incremental::recompute_cell_in_txn).
            conn.execute_batch(&format!(
                "INSERT INTO {dest} ({id_col}, geom, cell_x, cell_y, computed_at)
                 {inner}
                   AND ST_X(ST_Centroid(b.geom)) >= {x} AND ST_X(ST_Centroid(b.geom)) < {x_hi}
                   AND ST_Y(ST_Centroid(b.geom)) >= {y} AND ST_Y(ST_Centroid(b.geom)) < {y_hi};"
            ))
            .with_context(|| format!("Failed comparing {label} in cell ({x},{y})"))?;
            x += GRID_STEP;
        }
        y += GRID_STEP;
    }

    // total is now accurate: the grid above covers the source table's full
    // extent (source_grid_extent), and the write-narrow guard means each row
    // is written by exactly one cell, so matched = total - unmatched holds.
    let (total, unmatched): (i64, i64) = conn.query_row(
        &format!("SELECT (SELECT COUNT(*) FROM {source_table}), (SELECT COUNT(*) FROM {dest})"),
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    info!(
        source = label, total, unmatched, matched = total - unmatched,
        elapsed = %format_duration(t.elapsed()),
        "buildings comparison complete"
    );
    Ok(())
}

/// Grid-aligned bounding box covering every row in `source_table`, snapped
/// outward to `grid_step` multiples so the chunked scan above never misses a
/// row whose centroid falls outside the historical Poland bbox (a coordinate
/// error, a stray far-away row). The upper bound is snapped via
/// `floor(x/step)*step + step` rather than `ceil`, so a value that already
/// sits exactly on a grid line still gets a full extra cell of headroom
/// (`ceil` would leave it exactly on the loop's exclusive upper bound,
/// covered by nothing). Falls back to the historical (14.0, 49.0, 25.0, 55.0)
/// box when the table has no rows, so an empty table still produces a
/// (degenerate, zero-iteration) grid rather than an error.
fn source_grid_extent(
    conn: &Connection,
    source_table: &str,
    grid_step: f64,
) -> Result<(f64, f64, f64, f64)> {
    const FALLBACK: (f64, f64, f64, f64) = (14.0, 49.0, 25.0, 55.0);
    let row: (Option<f64>, Option<f64>, Option<f64>, Option<f64>) = conn.query_row(
        &format!(
            "SELECT ST_XMin(ST_Extent_Agg(geom)), ST_YMin(ST_Extent_Agg(geom)),
                    ST_XMax(ST_Extent_Agg(geom)), ST_YMax(ST_Extent_Agg(geom))
             FROM {source_table}"
        ),
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?;
    Ok(match row {
        (Some(x1), Some(y1), Some(x2), Some(y2)) => (
            (x1 / grid_step).floor() * grid_step,
            (y1 / grid_step).floor() * grid_step,
            (x2 / grid_step).floor() * grid_step + grid_step,
            (y2 / grid_step).floor() * grid_step + grid_step,
        ),
        _ => FALLBACK,
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::db::init_db;

    /// Spin up an in-memory DuckDB with spatial loaded and the serving
    /// tables created by `init_db`. `osm_buildings` is seeded with a single
    /// polygon; tests create their own government-source table.
    fn setup() -> Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "INSERT INTO osm_buildings VALUES
                 (1, 'way', NULL, ST_MakeEnvelope(20.0, 52.0, 20.001, 52.001));",
        )
        .unwrap();
        conn
    }

    #[test]
    fn writes_only_unmatched_rows_with_cell_tags() {
        let conn = setup();
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES
                 ('inside', ST_MakeEnvelope(20.0002,52.0002,20.0008,52.0008)),
                 ('lonely', ST_MakeEnvelope(21.0,52.2,21.001,52.201));",
        )
        .unwrap();
        compare_bdot10k(&conn).unwrap();
        let ids: Vec<String> = {
            let mut s = conn
                .prepare("SELECT LOKALNYID FROM bdot10k_unmatched ORDER BY LOKALNYID")
                .unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            ids,
            vec!["lonely".to_string()],
            "only the uncontained building is stored"
        );
        let (cx, cy): (i32, i32) = conn
            .query_row("SELECT cell_x, cell_y FROM bdot10k_unmatched", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        let (ex, ey) =
            crate::tile_math::lonlat_to_tile(21.0005, 52.2005, crate::tile_math::CHANGE_CELL_ZOOM);
        assert_eq!((cx as u32, cy as u32), (ex, ey));
    }

    #[test]
    fn compare_is_idempotent() {
        let conn = setup();
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES ('lonely', ST_MakeEnvelope(21.0,52.2,21.001,52.201));",
        )
        .unwrap();
        compare_bdot10k(&conn).unwrap();
        compare_bdot10k(&conn).unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM bdot10k_unmatched", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1, "re-running compare must not duplicate rows");
    }

    /// A row whose centroid falls outside the old hardcoded (14,49,25,55)
    /// Poland bbox (a coordinate error, a stray 0/0-adjacent row) must still
    /// be compared -- the grid now derives its extent from the source table
    /// instead of a fixed box, so `compare full` and the incremental path
    /// (which has no extent restriction at all) agree on every row.
    #[test]
    fn covers_a_row_outside_the_historical_poland_bbox() {
        let conn = setup();
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES
                 ('stray', ST_MakeEnvelope(30.0,60.0,30.001,60.001));",
        )
        .unwrap();
        compare_bdot10k(&conn).unwrap();
        let ids: Vec<String> = {
            let mut s = conn
                .prepare("SELECT LOKALNYID FROM bdot10k_unmatched")
                .unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            ids,
            vec!["stray".to_string()],
            "a row outside the hardcoded bbox must still be compared"
        );
    }

    /// Regression test for the write-narrow guard (re-added equivalent of the
    /// test deleted in the Task 4 refactor -- see
    /// `git show 0331d15^:src/compare/buildings.rs`, which asserted 2 rows
    /// before the chunked scan's write predicate was narrowed). A centroid
    /// exactly on a 0.5° grid line satisfies both neighbouring cells'
    /// ST_Intersects envelope test (closed on all four edges), so without a
    /// narrow write it would be inserted twice.
    #[test]
    fn compare_chunked_duplicates_source_on_cell_boundary() {
        let conn = setup();
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES
                 -- centroid (14.5, 52.25): x = 14.5 is the boundary between
                 -- the [14.0,14.5) and [14.5,15.0) grid columns; y = 52.25 is
                 -- safely mid-row.
                 ('boundary', ST_MakeEnvelope(14.4998,52.2498,14.5002,52.2502)),
                 -- Widens the source extent so both neighbouring columns are
                 -- actually scanned by the chunked loop -- a lone boundary
                 -- point would otherwise sit at a tightly-snapped extent edge
                 -- and only ever be scanned by one cell, which would not
                 -- exercise the guard at all.
                 ('anchor', ST_MakeEnvelope(14.05,52.25,14.06,52.26));",
        )
        .unwrap();
        compare_bdot10k(&conn).unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM bdot10k_unmatched WHERE LOKALNYID = 'boundary'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 1,
            "a centroid exactly on a grid boundary must be written by exactly one cell"
        );
    }
}
