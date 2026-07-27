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

    let (min_x, min_y, max_x, max_y) = (14.0, 49.0, 25.0, 55.0);
    let cx = cell_x_sql("ST_Centroid(b.geom)");
    let cy = cell_y_sql("ST_Centroid(b.geom)");
    let select = format!("b.{id_col}, b.geom, {cx}, {cy}, now()");

    let mut y = min_y;
    while y < max_y {
        let mut x = min_x;
        while x < max_x {
            let area = (x, y, x + GRID_STEP, y + GRID_STEP);
            let inner = unmatched_buildings_sql(source_table, &select, area);
            conn.execute_batch(&format!(
                "INSERT INTO {dest} ({id_col}, geom, cell_x, cell_y, computed_at) {inner};"
            ))
            .with_context(|| format!("Failed comparing {label} in cell ({x},{y})"))?;
            x += GRID_STEP;
        }
        y += GRID_STEP;
    }

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
}
