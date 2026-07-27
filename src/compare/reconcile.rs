use anyhow::{Context, Result};
use duckdb::Connection;

use crate::tile_math::{CHANGE_CELL_ZOOM, cell_x_sql, cell_y_sql};

/// Enqueue every distinct (source, z14 cell) present in the live tables, so the
/// drain rebuilds them. Repairs any dropped enqueue; also the offline rebuild path.
pub fn enqueue_all(conn: &Connection) -> Result<i64> {
    let z = CHANGE_CELL_ZOOM;
    let specs = [
        ("bdot10k", "bdot10k_buildings", "ST_Centroid(geom)"),
        ("egib", "egib_buildings", "ST_Centroid(geom)"),
        ("prg", "prg_addresses", "geom"),
    ];
    let mut total = 0i64;
    for (source, table, point) in specs {
        let cx = cell_x_sql(point);
        let cy = cell_y_sql(point);
        conn.execute_batch(&format!(
            "INSERT INTO match_dirty_cells
             SELECT DISTINCT '{source}', {z}, {cx}, {cy}, now()
             FROM {table} WHERE geom IS NOT NULL"
        ))
        .with_context(|| format!("enqueue_all for {source}"))?;
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE source = ?",
            duckdb::params![source],
            |r| r.get(0),
        )?;
        total += n;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    #[test]
    fn enqueue_all_covers_every_live_cell_once() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);
             CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES ('a', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             INSERT INTO prg_addresses VALUES ('p', ST_Point(19.0,50.0));",
        )
        .unwrap();
        let n = enqueue_all(&c).unwrap();
        assert_eq!(n, 2, "one bdot10k cell + one prg cell");
        let by: Vec<(String, i64)> = {
            let mut s = c.prepare(
                "SELECT source, COUNT(*) FROM match_dirty_cells GROUP BY source ORDER BY source").unwrap();
            s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(by, vec![("bdot10k".into(), 1), ("prg".into(), 1)]);
    }
}
