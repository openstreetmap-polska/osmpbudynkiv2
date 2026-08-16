use anyhow::{Context, Result, bail};
use duckdb::Connection;

use crate::compare::columns::classification_columns;
use crate::compare::rule::{
    BDOT10K_EKSPLOATOWANY_FILTER, OSM_MATCH_BUFFER_DEG, buffer, envelope_sql,
    unmatched_addresses_in_cell_sql, unmatched_buildings_sql,
};
use crate::tile_math::{CHANGE_CELL_ZOOM, cell_x_sql, cell_y_sql, tile_to_bbox};

/// Builds the `(dest_table, insert_cols, inner_select)` triple
/// `recompute_cell_in_txn` inserts from. Extracted into its own function so a
/// regression test can `EXPLAIN` the actual generated SQL, not a
/// hand-reconstructed copy of it.
///
/// The source-table scan is wrapped in a `MATERIALIZED` candidate CTE before
/// `unmatched_buildings_sql`/`unmatched_addresses_in_cell_sql` and the
/// trailing cell-tag guard (`AND cx = ... AND cy = ...`) are applied on top of
/// it. Without the candidate CTE at all, appending the guard directly to the
/// flat predicate loses the centroid RTREE index and forces a full table
/// scan (measured ~1.09s/cell on real data; see the identical CTE-vs-JOIN
/// trap documented at the top of `src/server/tiles.rs`). `MATERIALIZED` is
/// kept for the same reason it's kept there: it is the documented defense
/// against DuckDB's join-order optimizer folding a filtered CTE back into a
/// joint plan with whatever consumes it, which would silently reopen this
/// exact hole. Be precise about which half is doing the work, though: on the
/// real 16.35M-row `bdot10k_buildings`, the flat pre-fix shape plans as 2
/// RTREE scans + 1 `Sequential Scan` at 0.974s, while *both* a bare `WITH`
/// and `WITH ... MATERIALIZED` plan as 3 RTREE scans at ~0.098s. The CTE
/// itself is what restores the index here; `MATERIALIZED` is insurance
/// against a future re-plan, not the active ingredient. The envelope (and
/// `extra_filter`, when
/// present) ends up applied twice -- once building `candidates`, once inside
/// `unmatched_buildings_sql`'s own predicate on `candidates` -- deliberately:
/// both filters are idempotent, and this is what lets `rule.rs`'s predicate
/// text stay untouched (see `CLAUDE.md`'s "match rule has one home" gotcha)
/// rather than needing a signature change to skip the now-redundant outer
/// check.
fn build_sql(source: &str, cell_x: i32, cell_y: i32) -> Result<(&'static str, String, String)> {
    let write = tile_to_bbox(CHANGE_CELL_ZOOM, cell_x as u32, cell_y as u32);
    match source {
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
            let extra = extra_filter
                .map(|f| format!(" AND {f}"))
                .unwrap_or_default();
            let candidates = format!(
                "WITH candidates AS MATERIALIZED (
                     SELECT * FROM {src} b
                     WHERE ST_Intersects(b.centroid, {}){extra}
                 )\n",
                envelope_sql(write)
            );
            // Write-narrow: unmatched_buildings_sql's ST_Intersects test is
            // closed on all four cell edges, so a centroid exactly on a
            // shared boundary would satisfy both neighbours' predicates.
            // Restrict the write to rows whose canonical cell tag (the same
            // cell_x_sql/cell_y_sql expression stored in the row) matches
            // this cell, so a boundary row is written by exactly the cell
            // that owns it.
            let inner = format!(
                "{candidates}{} AND {cx} = {cell_x} AND {cy} = {cell_y}",
                unmatched_buildings_sql("candidates", &select, write, extra_filter)
            );
            Ok((
                dest,
                format!("{id}, geom, cell_x, cell_y, computed_at, {}", cc.dest_names),
                inner,
            ))
        }
        "prg" => {
            let read = buffer(write, OSM_MATCH_BUFFER_DEG);
            let cx = cell_x_sql("a.geom");
            let cy = cell_y_sql("a.geom");
            let select = format!(
                "a.geom, a.lokalny_id, a.numer_porzadkowy, a.ulica, a.miejscowosc, \
                 a.kod_pocztowy, a.teryt_miejscowosc, a.wazny_od_lub_data_nadania, \
                 a.teryt_gmina, a.gmina, {cx}, {cy}, now()"
            );
            // Unlike the buildings branch above, this one does *not* build its
            // own candidate CTE: `unmatched_addresses_in_cell_sql` owns a
            // two-CTE chain of its own (`addr_candidates` -> `addr_resolved`,
            // resolving the street name through `street_name_mappings` for the
            // name rules), and two `WITH` keywords in one statement is a
            // syntax error. So the source table goes straight through and the
            // rule applies the write envelope itself; the outer alias `a` is
            // `addr_resolved`, which carries `SELECT c.*` — every
            // `prg_addresses` column, so `select` below still binds.
            //
            // Same write-narrow guard as the buildings branch above, and it
            // stays *outside* the rule's CTEs for the same reason: an
            // expression-equality filter on the indexed column alongside the
            // ST_Intersects flips RTREE_INDEX_SCAN to a sequential scan
            // (docs/per_cell_recompute_cell_guard_scan.md), and the CTE
            // boundary is what defuses that.
            let inner = format!(
                "{} AND {cx} = {cell_x} AND {cy} = {cell_y}",
                unmatched_addresses_in_cell_sql("prg_addresses", &select, write, read)
            );
            Ok((
                "prg_unmatched",
                "geom, lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy, \
                 teryt_miejscowosc, wazny_od_lub_data_nadania, teryt_gmina, gmina, \
                 cell_x, cell_y, computed_at"
                    .to_string(),
                inner,
            ))
        }
        other => bail!("recompute_cell: unknown source {other}"),
    }
}

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
    let (dest, insert_cols, inner) = build_sql(source, cell_x, cell_y)?;

    conn.execute(
        &format!("DELETE FROM {dest} WHERE cell_x = ? AND cell_y = ?"),
        duckdb::params![cell_x, cell_y],
    )?;
    conn.execute_batch(&format!("INSERT INTO {dest} ({insert_cols}) {inner};"))?;
    // The cell's denominator, recomputed in the caller's transaction alongside
    // the numerator above so the two can never be read from different passes.
    // Cheap: it re-reads the same envelope through the same RTREE index the
    // INSERT just used.
    crate::compare::totals::recompute_cell_in_txn(conn, source, cell_x, cell_y)?;
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
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             -- Created by `import prg`, not by create_schema, so the fixture
             -- has to declare it the same way compare::mod's does.
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 wazny_od_lub_data_nadania DATE, teryt_gmina VARCHAR, gmina VARCHAR,
                 geom GEOMETRY);",
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

    /// The per-cell recompute must apply the same former-building veto as the
    /// full compare -- a government building fully covered by an
    /// `osm_former_buildings` polygon must not be served here either, or an
    /// incremental recompute could disagree with `full_vs_incremental_equivalence`.
    #[test]
    fn recompute_excludes_former_buildings() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('suppressed', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);
             INSERT INTO osm_former_buildings VALUES
                 (1, 'way', 'demolished:building', 'yes',
                  ST_MakeEnvelope(20.9999,51.9999,21.0011,52.0011));",
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
            Vec::<String>::new(),
            "a former-building-suppressed building must never be served as unmatched"
        );
    }

    /// The regression guard for the per-cell drain's full-table-scan fix
    /// (measured 60 cells 65.48s -> 3.03s): without wrapping the
    /// envelope-filtered source-table scan in a `candidates` CTE at all,
    /// appending the trailing cell-tag guard (`AND cx = ... AND cy = ...`)
    /// directly to the flat predicate loses the centroid RTREE index for a
    /// sequential scan -- confirmed by hand against the pre-fix SQL shape
    /// (`unmatched_buildings_sql` + the guard, no CTE), which reproduces 0
    /// RTREE scans / a full sequential scan of `bdot10k_buildings` at the
    /// same fixture size used below.
    ///
    /// **What this test does NOT currently pin**: whether the CTE needs to
    /// be `MATERIALIZED` specifically, as opposed to a bare `WITH`. That's
    /// the documented risk in `src/server/tiles.rs` for a CTE consumed by a
    /// downstream `JOIN`, and `build_sql` keeps `MATERIALIZED` for the same
    /// defensive reason. But repeated attempts to reproduce a fold-back
    /// *without* it here -- at 20k and 500k synthetic rows, with and without
    /// production-like `threads`/`memory_limit` settings -- all still showed
    /// `RTREE_IN` in the plan on the bundled DuckDB build this project pins
    /// (`duckdb = "1.10505.0"`). The same holds on the real 16.35M-row table:
    /// flat = 2 RTREE + 1 `Sequential Scan` @ 0.974s, bare `WITH` = 3 RTREE @
    /// 0.098s, `MATERIALIZED` = 3 RTREE @ 0.099s. So the CTE is the active
    /// ingredient and `MATERIALIZED` is insurance. It's possible the
    /// anti-join (`NOT EXISTS`) shape here is folded differently than the
    /// `LEFT JOIN` case tiles.rs guards against, or that the fold-back
    /// tiles.rs saw needs a non-empty, indexed table on the other side of the
    /// join to trigger real cost-based re-planning (this fixture leaves
    /// `osm_buildings` / `osm_former_buildings` empty on purpose -- see
    /// below). This is a discrepancy against this test's original design
    /// brief, which expected removing `MATERIALIZED` alone to fail this
    /// assertion; it's recorded here rather than silently asserting something
    /// unverified.
    ///
    /// osm_buildings / osm_former_buildings are left empty and unindexed --
    /// exactly what `db::create_schema` gives you before `import osm`'s
    /// `create_spatial_indexes` ever runs -- so a false-positive "RTREE_IN"
    /// match from either anti-join side is structurally impossible: the only
    /// RTREE index anywhere in this fixture is the one built below on
    /// `bdot10k_buildings.centroid`. Only the substring "RTREE_IN" is
    /// asserted (DuckDB's EXPLAIN pretty-printer truncates operator labels in
    /// a wide plan -- see `server::tiles::tests::mvt_bbox_filter_uses_the_rtree_index`),
    /// and the absence of "Sequential Scan" is deliberately NOT asserted: the
    /// fixed plan still legitimately contains one, from the empty anti-join
    /// tables.
    #[test]
    fn drain_candidate_cte_uses_the_centroid_rtree_index() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO bdot10k_buildings (LOKALNYID, geom)
                 SELECT 'b' || i,
                        ST_MakeEnvelope(20.0 + i * 0.0001, 52.0,
                                        20.0 + i * 0.0001 + 0.00005, 52.00005)
                 FROM range(20000) t(i);
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);
             CREATE INDEX bdot10k_buildings_centroid_idx ON bdot10k_buildings USING RTREE (centroid);",
        )
        .unwrap();

        let (cx, cy) = lonlat_to_tile(20.5005, 52.00002, CHANGE_CELL_ZOOM);
        let (_, _, inner) = build_sql("bdot10k", cx as i32, cy as i32).unwrap();

        let mut stmt = c.prepare(&format!("EXPLAIN {inner}")).unwrap();
        let mut rows = stmt.query([]).unwrap();
        let mut plan = String::new();
        while let Some(row) = rows.next().unwrap() {
            plan.push_str(&row.get::<_, String>(1).unwrap_or_default());
        }
        assert!(
            plan.contains("RTREE_IN"),
            "the drained cell's candidate CTE must use the centroid RTREE index, got plan: {plan}"
        );
    }

    /// The prg twin of the test above, and the one that changed shape: the
    /// address branch no longer builds its own candidate CTE — the rule owns
    /// an `addr_candidates` -> `addr_resolved` chain, and `addr_resolved` is a
    /// `LEFT JOIN` against `street_name_mappings` sitting directly downstream
    /// of the filtered CTE. That is exactly the configuration
    /// `server::tiles`'s doc comment warns can be re-planned into a
    /// `SEQ_SCAN` + `FILTER`, so the mapping table is seeded here rather than
    /// left empty — an empty build side gives the optimizer nothing to chew on
    /// and the test would pass for the wrong reason.
    ///
    /// `osm_addresses` is left empty and unindexed (what `db::create_schema`
    /// gives you before `import osm`), so a "RTREE_IN" match can only be
    /// coming from `prg_addresses`.
    #[test]
    fn drain_prg_candidate_cte_uses_the_geom_rtree_index() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO street_name_mappings VALUES (NULL, 'gen. Kruka', 'Generała Kruka');
             INSERT INTO prg_addresses (lokalny_id, numer_porzadkowy, geom)
                 SELECT 'a' || i, '1', ST_Point(20.0 + i * 0.0001, 52.0)
                 FROM range(20000) t(i);
             CREATE INDEX prg_addresses_geom_idx ON prg_addresses USING RTREE (geom);",
        )
        .unwrap();

        let (cx, cy) = lonlat_to_tile(20.5005, 52.0, CHANGE_CELL_ZOOM);
        let (_, _, inner) = build_sql("prg", cx as i32, cy as i32).unwrap();

        let mut stmt = c.prepare(&format!("EXPLAIN {inner}")).unwrap();
        let mut rows = stmt.query([]).unwrap();
        let mut plan = String::new();
        while let Some(row) = rows.next().unwrap() {
            plan.push_str(&row.get::<_, String>(1).unwrap_or_default());
        }
        assert!(
            plan.contains("RTREE_IN"),
            "the drained cell's address CTE must use the geom RTREE index, got plan: {plan}"
        );
    }

    /// A drained cell must apply the name rules, not just proximity. The
    /// address here is ~133 m from its OSM neighbour — outside
    /// `MATCH_DISTANCE_METERS`, inside `NAME_MATCH_DISTANCE_METERS` — and the
    /// street name agrees only after the mapping is applied, so this fails
    /// both if the branch is missing and if it compares the raw PRG name.
    #[test]
    fn recompute_matches_an_address_via_the_street_rule() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO street_name_mappings VALUES (NULL, 'gen. Kruka', 'Generała Kruka');
             INSERT INTO prg_addresses (lokalny_id, numer_porzadkowy, ulica, geom) VALUES
                 ('mapped',    '5', 'gen. Kruka', ST_Point(21.010, 52.2112)),
                 ('raw-equal', '6', 'gen. Kruka', ST_Point(21.011, 52.2112));
             INSERT INTO osm_addresses VALUES
                 (1,'node','5','Generała Kruka',NULL,NULL, ST_Point(21.010, 52.210)),
                 (2,'node','6','gen. Kruka',NULL,NULL, ST_Point(21.011, 52.210));",
        )
        .unwrap();

        let (cx, cy) = lonlat_to_tile(21.010, 52.2112, CHANGE_CELL_ZOOM);
        recompute_cell(&c, "prg", cx as i32, cy as i32).unwrap();

        let ids: Vec<String> = {
            let mut s = c
                .prepare("SELECT lokalny_id FROM prg_unmatched ORDER BY lokalny_id")
                .unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            ids,
            vec!["raw-equal".to_string()],
            "the mapped name matches at ~133m; raw equality does not"
        );
    }
}
