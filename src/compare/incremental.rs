use anyhow::{Context, Result, bail};
use duckdb::Connection;

use crate::compare::rule::{
    OSM_MATCH_BUFFER_DEG, buffer, unmatched_addresses_in_cell_sql, unmatched_buildings_sql,
};
use crate::tile_math::{CHANGE_CELL_ZOOM, cell_x_sql, cell_y_sql, tile_to_bbox};

/// Rebuild one z14 cell's slice of `<source>_unmatched` from current live data.
/// Read wide (buffered OSM for addresses), write narrow (only rows whose
/// representative point is inside the cell). Assumes an open transaction —
/// callers that need atomicity with other statements should wrap this
/// themselves (see `drain_batch`, which pairs it with a queue delete).
pub fn recompute_cell_in_txn(
    conn: &Connection,
    source: &str,
    cell_x: i32,
    cell_y: i32,
) -> Result<()> {
    let write = tile_to_bbox(CHANGE_CELL_ZOOM, cell_x as u32, cell_y as u32);
    let (dest, insert_cols, inner) = match source {
        "bdot10k" | "egib" => {
            let (src, id, dest) = if source == "bdot10k" {
                ("bdot10k_buildings", "LOKALNYID", "bdot10k_unmatched")
            } else {
                ("egib_buildings", "id_budynku", "egib_unmatched")
            };
            let cx = cell_x_sql("ST_Centroid(b.geom)");
            let cy = cell_y_sql("ST_Centroid(b.geom)");
            let select = format!("b.{id}, b.geom, {cx}, {cy}, now()");
            (
                dest,
                format!("{id}, geom, cell_x, cell_y, computed_at"),
                unmatched_buildings_sql(src, &select, write),
            )
        }
        "prg" => {
            let read = buffer(write, OSM_MATCH_BUFFER_DEG);
            let cx = cell_x_sql("a.geom");
            let cy = cell_y_sql("a.geom");
            let select = format!(
                "a.geom, a.lokalny_id, a.numer_porzadkowy, a.ulica, a.miejscowosc, \
                 a.kod_pocztowy, a.teryt_miejscowosc, {cx}, {cy}, now()"
            );
            (
                "prg_unmatched",
                "geom, lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy, \
                 teryt_miejscowosc, cell_x, cell_y, computed_at"
                    .to_string(),
                unmatched_addresses_in_cell_sql("prg_addresses", &select, write, read),
            )
        }
        other => bail!("recompute_cell: unknown source {other}"),
    };

    conn.execute(
        &format!("DELETE FROM {dest} WHERE cell_x = ? AND cell_y = ?"),
        duckdb::params![cell_x, cell_y],
    )?;
    conn.execute_batch(&format!("INSERT INTO {dest} ({insert_cols}) {inner};"))?;
    Ok(())
}

/// Rebuild one z14 cell's slice of `<source>_unmatched` from current live data,
/// in a single transaction of its own. Thin wrapper around
/// `recompute_cell_in_txn` — see that function for what actually runs.
#[allow(dead_code)] // not yet consumed: wired up by later tasks in this plan (reconcile sweep)
pub fn recompute_cell(conn: &Connection, source: &str, cell_x: i32, cell_y: i32) -> Result<()> {
    conn.execute_batch("BEGIN TRANSACTION")
        .context("recompute_cell: begin")?;
    let res = recompute_cell_in_txn(conn, source, cell_x, cell_y);
    match res {
        Ok(()) => conn
            .execute_batch("COMMIT")
            .context("recompute_cell: commit"),
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use crate::tile_math::lonlat_to_tile;
    use std::path::Path;

    fn conn() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch("CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);")
            .unwrap();
        c
    }

    #[test]
    fn recompute_replaces_only_that_cell() {
        let c = conn();
        // Two buildings in different z14 cells, neither matched.
        c.execute_batch(
            "INSERT INTO bdot10k_buildings VALUES
                 ('p', ST_MakeEnvelope(21.0,52.0,21.001,52.001)),
                 ('q', ST_MakeEnvelope(19.0,50.0,19.001,50.001));",
        )
        .unwrap();
        let (px, py) = lonlat_to_tile(21.0005, 52.0005, CHANGE_CELL_ZOOM);
        let (qx, qy) = lonlat_to_tile(19.0005, 50.0005, CHANGE_CELL_ZOOM);

        recompute_cell(&c, "bdot10k", px as i32, py as i32).unwrap();
        recompute_cell(&c, "bdot10k", qx as i32, qy as i32).unwrap();
        let n: i64 = c
            .query_row("SELECT COUNT(*) FROM bdot10k_unmatched", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2);

        // Now 'p' becomes matched (add an osm building over it). Recompute only p's cell.
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES (1,'way',NULL, ST_MakeEnvelope(20.9,51.9,21.1,52.1));",
        )
        .unwrap();
        recompute_cell(&c, "bdot10k", px as i32, py as i32).unwrap();

        let ids: Vec<String> = {
            let mut s = c
                .prepare("SELECT LOKALNYID FROM bdot10k_unmatched ORDER BY LOKALNYID")
                .unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            ids,
            vec!["q".to_string()],
            "p's cell rebuilt to matched; q's cell untouched"
        );
    }
}
