use anyhow::{Context, Result, bail};
use duckdb::Connection;

use crate::compare::columns::classification_columns;
use crate::compare::rule::{
    BDOT10K_EKSPLOATOWANY_FILTER, OSM_MATCH_BUFFER_DEG, buffer, unmatched_addresses_in_cell_sql,
    unmatched_buildings_sql,
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
            let (src, id, dest, extra_filter) = if source == "bdot10k" {
                (
                    "bdot10k_buildings",
                    "LOKALNYID",
                    "bdot10k_unmatched",
                    Some(BDOT10K_EKSPLOATOWANY_FILTER),
                )
            } else {
                ("egib_buildings", "id_budynku", "egib_unmatched", None)
            };
            let cx = cell_x_sql("b.centroid");
            let cy = cell_y_sql("b.centroid");
            let cc = classification_columns(src);
            let select = format!("b.{id}, b.geom, {cx}, {cy}, now(), {}", cc.source_exprs);
            // Write-narrow: unmatched_buildings_sql's ST_Intersects test is
            // closed on all four cell edges, so a centroid exactly on a
            // shared boundary would satisfy both neighbours' predicates.
            // Restrict the write to rows whose canonical cell tag (the same
            // cell_x_sql/cell_y_sql expression stored in the row) matches
            // this cell, so a boundary row is written by exactly the cell
            // that owns it.
            let inner = format!(
                "{} AND {cx} = {cell_x} AND {cy} = {cell_y}",
                unmatched_buildings_sql(src, &select, write, extra_filter)
            );
            (
                dest,
                format!("{id}, geom, cell_x, cell_y, computed_at, {}", cc.dest_names),
                inner,
            )
        }
        "prg" => {
            let read = buffer(write, OSM_MATCH_BUFFER_DEG);
            let cx = cell_x_sql("a.geom");
            let cy = cell_y_sql("a.geom");
            let select = format!(
                "a.geom, a.lokalny_id, a.numer_porzadkowy, a.ulica, a.miejscowosc, \
                 a.kod_pocztowy, a.teryt_miejscowosc, a.wazny_od_lub_data_nadania, {cx}, {cy}, now()"
            );
            // Same write-narrow guard as the buildings branch above.
            let inner = format!(
                "{} AND {cx} = {cell_x} AND {cy} = {cell_y}",
                unmatched_addresses_in_cell_sql("prg_addresses", &select, write, read)
            );
            (
                "prg_unmatched",
                "geom, lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy, \
                 teryt_miejscowosc, wazny_od_lub_data_nadania, cell_x, cell_y, computed_at"
                    .to_string(),
                inner,
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
///
/// Standalone transactional single-cell recompute; the drain pairs
/// `recompute_cell_in_txn` with its own queue-delete in one transaction instead
/// of calling this, so this wrapper is currently only exercised by tests — kept
/// as a coherent, tested public API for manual use or future callers that want
/// a recompute without a queue delete.
#[allow(dead_code)]
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
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR, LICZBAKONDYGNACJI SMALLINT,
                 KATEGORIAISTNIENIA VARCHAR DEFAULT 'eksploatowany',
                 NAZWA VARCHAR, FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);",
        )
        .unwrap();
        c
    }

    #[test]
    fn recompute_replaces_only_that_cell() {
        let c = conn();
        // Two buildings in different z14 cells, neither matched.
        c.execute_batch(
            "INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('p', ST_MakeEnvelope(21.0,52.0,21.001,52.001)),
                 ('q', ST_MakeEnvelope(19.0,50.0,19.001,50.001));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
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

    /// A representative point lying exactly on a shared z14 cell edge
    /// satisfies both neighbours' ST_Intersects envelope test (closed on all
    /// four edges), but must carry only one canonical cell_x/cell_y tag (the
    /// row's SELECT always computes the true cell from its geometry, not from
    /// which cell's recompute is running). Recomputing the row's *canonical*
    /// cell first, then its neighbour, must not leave a second copy behind:
    /// the neighbour's DELETE is keyed to the neighbour's own cell number, so
    /// it cannot remove a row tagged with the canonical cell, and its INSERT
    /// (without the guard) would compute that same canonical tag again --
    /// this is precisely the ordering that reproduces the duplicate.
    #[test]
    fn write_narrow_by_cell_tag_prevents_boundary_duplicates() {
        let c = conn();
        let (cx, cy) = (9147u32, 5411u32);
        let (_, min_lat, boundary_lon, max_lat) = tile_to_bbox(CHANGE_CELL_ZOOM, cx, cy);
        let mid_lat = (min_lat + max_lat) / 2.0;
        // boundary_lon is simultaneously this cell's max_lon and (cx+1)'s
        // min_lon (same tile_to_bbox formula, same float bits) -- exactly the
        // closed-edge ambiguity the cell-tag guard exists to resolve.
        c.execute_batch(&format!(
            "INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES ('boundary', ST_Point({boundary_lon}, {mid_lat}));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);"
        ))
        .unwrap();

        // Determine which of the two candidate neighbours (cx, cx+1) is the
        // row's true canonical cell -- the same expression the INSERT itself
        // uses -- so the two recompute calls below run canonical-cell-first,
        // the ordering that actually exercises the guard.
        let true_cx: i32 = c
            .query_row(
                &format!(
                    "SELECT {} FROM bdot10k_buildings WHERE LOKALNYID = 'boundary'",
                    cell_x_sql("geom")
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        let other_cx = if true_cx == cx as i32 { cx + 1 } else { cx } as i32;
        assert!(
            true_cx == cx as i32 || true_cx == (cx + 1) as i32,
            "sanity: the boundary point must canonically belong to one of the two candidate cells"
        );

        recompute_cell(&c, "bdot10k", true_cx, cy as i32).unwrap();
        recompute_cell(&c, "bdot10k", other_cx, cy as i32).unwrap();

        let n: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM bdot10k_unmatched WHERE LOKALNYID = 'boundary'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 1,
            "a representative point on a shared cell edge must be written by exactly one neighbour"
        );
    }

    /// The per-cell recompute must apply the same eksploatowany-only filter
    /// as the full compare -- otherwise an incremental recompute could serve
    /// a "w budowie" building that a full `compare` would never have
    /// written, breaking `full_vs_incremental_equivalence`.
    #[test]
    fn recompute_excludes_non_eksploatowany_buildings() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO bdot10k_buildings (LOKALNYID, geom, KATEGORIAISTNIENIA) VALUES
                 ('lonely', ST_MakeEnvelope(21.0,52.0,21.001,52.001), 'eksploatowany'),
                 ('under_construction', ST_MakeEnvelope(21.0,52.0,21.001,52.001), 'w budowie');
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
        let (cx, cy) = lonlat_to_tile(21.0005, 52.0005, CHANGE_CELL_ZOOM);

        recompute_cell(&c, "bdot10k", cx as i32, cy as i32).unwrap();

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
            vec!["lonely".to_string()],
            "the under-construction building must never be served as unmatched"
        );
    }
}
