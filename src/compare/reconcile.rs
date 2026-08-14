use anyhow::{Context, Result};
use duckdb::Connection;

use crate::tile_math::{CHANGE_CELL_ZOOM, cell_x_sql, cell_y_sql};

/// Enqueue every distinct (source, z14 cell) present in the live tables, so the
/// drain rebuilds them. Repairs any dropped enqueue; also the offline rebuild path.
pub fn enqueue_all(conn: &Connection) -> Result<i64> {
    let z = CHANGE_CELL_ZOOM;
    let specs = [
        ("bdot10k", "bdot10k_buildings", "centroid"),
        ("egib", "egib_buildings", "centroid"),
        ("prg", "prg_addresses", "geom"),
    ];
    let mut total = 0i64;
    // Each INSERT below runs in DuckDB's auto-commit mode (no explicit
    // transaction wraps this loop), so a mid-sweep failure leaves a partial
    // enqueue for whichever sources ran before the failing one. That's
    // harmless: the drain is idempotent per (source, cell), and a later
    // `compare reconcile` re-covers whatever this run missed.
    for (source, table, point) in specs {
        // Checked once per source, same granularity as `compare::run`'s
        // between-sub-compares check. This loop has no transaction at all
        // (see the comment above), so bailing here doesn't undo anything --
        // it just stops enqueueing the sources that haven't run yet, which
        // is the same partial-sweep outcome the loop already tolerates on a
        // real INSERT failure (an `Err` propagated via `?` below). A later
        // `compare reconcile` re-covers whatever a cancelled sweep missed.
        crate::shutdown::check_requested()?;
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
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES ('a', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             INSERT INTO prg_addresses VALUES ('p', ST_Point(19.0,50.0));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
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

    /// The original test seeded one row per source and none for egib, so three
    /// behaviours of `enqueue_all` went uncovered: the DISTINCT collapse (many
    /// objects sharing a cell must enqueue that cell once), the
    /// `geom IS NOT NULL` skip, and the egib branch's ST_Centroid path.
    #[test]
    fn enqueue_all_collapses_cells_skips_null_geom_and_covers_egib() {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);

             -- Three bdot10k buildings inside ONE z14 cell (cells are ~0.022
             -- deg wide here, so these cannot straddle a boundary), plus one
             -- far away: 2 distinct cells, not 4 rows.
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('a', ST_MakeEnvelope(21.0000,52.0000,21.0005,52.0005)),
                 ('b', ST_MakeEnvelope(21.0006,52.0006,21.0010,52.0010)),
                 ('c', ST_MakeEnvelope(21.0011,52.0011,21.0015,52.0015)),
                 ('far', ST_MakeEnvelope(19.0000,50.0000,19.0005,50.0005)),
                 -- NULL geometry must be skipped, not counted or crashed on.
                 ('nogeom', NULL);

             -- egib goes through the same stored-centroid path as bdot10k;
             -- two buildings in one cell plus a NULL.
             INSERT INTO egib_buildings (id_budynku, geom) VALUES
                 ('e1', ST_MakeEnvelope(22.0000,53.0000,22.0005,53.0005)),
                 ('e2', ST_MakeEnvelope(22.0006,53.0006,22.0010,53.0010)),
                 ('e_nogeom', NULL);

             -- prg uses the raw point, not a centroid.
             INSERT INTO prg_addresses VALUES
                 ('p1', ST_Point(23.0000,54.0000)),
                 ('p2', ST_Point(23.0005,54.0005)),
                 ('p_nogeom', NULL);

             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);
             UPDATE egib_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();

        let n = enqueue_all(&c).unwrap();
        assert_eq!(
            n, 4,
            "2 bdot10k cells + 1 egib cell + 1 prg cell, NULL geometries skipped"
        );

        let by: Vec<(String, i64)> = {
            let mut s = c
                .prepare(
                    "SELECT source, COUNT(*) FROM match_dirty_cells
                     GROUP BY source ORDER BY source",
                )
                .unwrap();
            s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            by,
            vec![("bdot10k".into(), 2), ("egib".into(), 1), ("prg".into(), 1)],
            "co-located objects must collapse to one cell per source"
        );

        // Every enqueued row must carry the change-cell zoom, since the drain
        // reinterprets cell_x/cell_y at CHANGE_CELL_ZOOM unconditionally.
        let wrong_zoom: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE cell_z <> ?",
                duckdb::params![CHANGE_CELL_ZOOM as i32],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(wrong_zoom, 0);
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
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES ('a', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);
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
