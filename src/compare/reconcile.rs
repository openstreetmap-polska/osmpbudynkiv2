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
    // Each INSERT below runs in DuckDB's auto-commit mode (no explicit
    // transaction wraps this loop), so a mid-sweep failure leaves a partial
    // enqueue for whichever sources ran before the failing one. That's
    // harmless: the drain is idempotent per (source, cell), and a later
    // `compare reconcile` re-covers whatever this run missed.
    for (source, table, point) in specs {
        let cx = cell_x_sql(point);
        let cy = cell_y_sql(point);
        // `execute` (not `execute_batch`) so its return value is the number
        // of rows this INSERT actually added -- a COUNT(*) query afterwards
        // would include any dirty cells for this source enqueued by another
        // producer before this sweep ran, inflating the "enqueued" total
        // `compare reconcile` logs.
        let n = conn
            .execute(
                &format!(
                    "INSERT INTO match_dirty_cells
                     SELECT DISTINCT '{source}', {z}, {cx}, {cy}, now()
                     FROM {table} WHERE geom IS NOT NULL"
                ),
                [],
            )
            .with_context(|| format!("enqueue_all for {source}"))?;
        total += n as i64;
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

    /// The returned total must be the number of rows *this sweep* inserted,
    /// not the queue's overall depth for that source -- a pre-existing dirty
    /// cell (e.g. left over from an OSM update moments earlier) must not
    /// inflate the "enqueued" count `compare reconcile` logs.
    #[test]
    fn enqueue_all_returns_only_newly_inserted_rows() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);
             CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings VALUES ('a', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             -- A pre-existing, unrelated dirty-cell row for the same source,
             -- as if an OSM update had enqueued it moments earlier.
             INSERT INTO match_dirty_cells VALUES ('bdot10k', 14, 1, 1, now());",
        )
        .unwrap();
        let n = enqueue_all(&c).unwrap();
        assert_eq!(
            n, 1,
            "the returned count is rows this sweep inserted, not the queue's total depth"
        );
    }
}
