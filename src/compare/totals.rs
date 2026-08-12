//! Per-z14-cell counts of *comparable* government objects — the denominator of
//! the low-zoom completeness ratio, whose numerator is `<source>_unmatched`.
//!
//! **Why this needs a table.** `/tiles` renders z5–z10 by right-shifting
//! `cell_x`/`cell_y` into parent bins, which is free because the unmatched
//! tables carry those columns already. The source tables do not: they carry
//! only `geom`/`centroid`, so counting a bin's total at serve time would mean
//! deriving the cell for every government row under the tile — at z5 a bin
//! covers 512×512 z14 cells, i.e. millions of rows per request. Precomputing
//! per z14 cell keeps the same free bit-shift roll-up the numerator gets.
//!
//! **Filter parity is the correctness condition.** The ratio is only meaningful
//! if numerator and denominator range over the same row set, so the denominator
//! reuses `rule::BDOT10K_EKSPLOATOWANY_FILTER` verbatim rather than restating
//! it. Counting BDOT10k rows the match rule refuses to consider (`w budowie`,
//! `nieczynny`, `zniszczony`) would inflate the denominator by ~106k rows
//! nationally and make BDOT10k's ratio read systematically low — and silently
//! incomparable with EGIB's, which has no such filter. Every source is aliased
//! `b` here purely so that constant drops in unchanged.
//!
//! **Cell-tag parity is the other one.** Both writers below re-use the same
//! `cell_x_sql`/`cell_y_sql` expression on the same representative point that
//! the unmatched writers use, so a point lying exactly on a shared cell edge
//! (the envelope test is closed on all four) is counted by exactly the one cell
//! that also stores its unmatched row. Without that guard a boundary object
//! could land in one cell's denominator and its neighbour's numerator, pushing
//! that neighbour's ratio above 1.
//!
//! Both writers assume an open transaction and are always called from the same
//! one that writes the matching unmatched rows — `compare::buildings` /
//! `compare::addresses` for the full rebuild, `compare::incremental` for the
//! drain — so a cell's numerator and denominator can never be observed from
//! different comparisons.
//!
//! **Deliberately does not mirror the former-building veto.** A building
//! suppressed by `rule::suppressed_buildings_sql` is comparable and OSM has
//! effectively handled it, unlike a `BDOT10K_EKSPLOATOWANY_FILTER`-excluded
//! row, which is not comparable at all — so it stays in this denominator
//! exactly as a matched building does. Do not "fix" this parity later; see
//! Step 6 of the design for the reasoning.

use anyhow::{Context, Result, bail};
use duckdb::Connection;

use crate::compare::rule::BDOT10K_EKSPLOATOWANY_FILTER;
use crate::tile_math::{CHANGE_CELL_ZOOM, cell_x_sql, cell_y_sql, tile_to_bbox};

struct TotalsSpec {
    table: &'static str,
    /// Representative point, `b`-aliased: the same column the source's
    /// unmatched writer derives `cell_x`/`cell_y` from, so both agree on which
    /// cell owns a given row.
    point: &'static str,
    /// ANDed in when set. Must be exactly what the match rule applies to this
    /// source, or the ratio compares two different row sets.
    filter: Option<&'static str>,
}

fn spec(source: &str) -> Result<TotalsSpec> {
    Ok(match source {
        "bdot10k" => TotalsSpec {
            table: "bdot10k_buildings",
            point: "b.centroid",
            filter: Some(BDOT10K_EKSPLOATOWANY_FILTER),
        },
        "egib" => TotalsSpec {
            table: "egib_buildings",
            point: "b.centroid",
            filter: None,
        },
        "prg" => TotalsSpec {
            table: "prg_addresses",
            point: "b.geom",
            filter: None,
        },
        other => bail!("cell_totals: unknown source {other}"),
    })
}

/// Rebuild every cell's total for one source. Assumes an open transaction;
/// called from inside the full compare's, so the totals and the unmatched rows
/// they describe commit together or not at all.
pub fn rebuild_all_in_txn(conn: &Connection, source: &str) -> Result<()> {
    let s = spec(source)?;
    let cx = cell_x_sql(s.point);
    let cy = cell_y_sql(s.point);
    let extra = s.filter.map(|f| format!("AND {f} ")).unwrap_or_default();

    conn.execute(
        "DELETE FROM cell_totals WHERE source = ?",
        duckdb::params![source],
    )
    .with_context(|| format!("cell_totals: failed to clear totals for {source}"))?;
    conn.execute_batch(&format!(
        "INSERT INTO cell_totals (source, cell_x, cell_y, total)
         SELECT '{source}', {cx}, {cy}, count(*)::INTEGER
         FROM {} b
         WHERE {} IS NOT NULL {extra}
         GROUP BY 1, 2, 3;",
        s.table, s.point
    ))
    .with_context(|| format!("cell_totals: failed to rebuild totals for {source}"))?;
    Ok(())
}

/// Rebuild one z14 cell's total for one source. Assumes an open transaction —
/// the drain calls this from the same one as the cell's unmatched recompute.
///
/// A cell holding no comparable government object writes no row (the `HAVING`
/// suppresses the single zero-count row an ungrouped aggregate always returns),
/// so its stale row is removed and not replaced. That matters: a `total = 0`
/// row would be a zero denominator at serve time, and it is not the same state
/// as a fully-imported cell, which has a positive total and no unmatched rows.
pub fn recompute_cell_in_txn(
    conn: &Connection,
    source: &str,
    cell_x: i32,
    cell_y: i32,
) -> Result<()> {
    let s = spec(source)?;
    let (x1, y1, x2, y2) = tile_to_bbox(CHANGE_CELL_ZOOM, cell_x as u32, cell_y as u32);
    let cx = cell_x_sql(s.point);
    let cy = cell_y_sql(s.point);
    let extra = s.filter.map(|f| format!("AND {f} ")).unwrap_or_default();

    conn.execute(
        "DELETE FROM cell_totals WHERE source = ? AND cell_x = ? AND cell_y = ?",
        duckdb::params![source, cell_x, cell_y],
    )
    .with_context(|| {
        format!("cell_totals: failed to clear cell ({cell_x},{cell_y}) for {source}")
    })?;
    // ST_Intersects against a constant envelope so the point column's RTREE
    // index is usable (see rule::unmatched_buildings_sql); the cell-tag
    // equality then narrows the closed-edge overlap to the owning cell.
    conn.execute_batch(&format!(
        "INSERT INTO cell_totals (source, cell_x, cell_y, total)
         SELECT '{source}', {cell_x}, {cell_y}, count(*)::INTEGER
         FROM {} b
         WHERE ST_Intersects({}, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
           {extra}AND {cx} = {cell_x} AND {cy} = {cell_y}
         HAVING count(*) > 0;",
        s.table, s.point
    ))
    .with_context(|| {
        format!("cell_totals: failed to recompute cell ({cell_x},{cell_y}) for {source}")
    })?;
    Ok(())
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
        // Full column set: `a_fully_matched_cell_still_gets_a_total` drives the
        // real `incremental::recompute_cell`, which selects the carried
        // classification columns.
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR, LICZBAKONDYGNACJI SMALLINT,
                 KATEGORIAISTNIENIA VARCHAR DEFAULT 'eksploatowany',
                 NAZWA VARCHAR, FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             CREATE TABLE prg_addresses (lokalny_id VARCHAR, geom GEOMETRY);",
        )
        .unwrap();
        c
    }

    fn totals(c: &Connection, source: &str) -> Vec<(i32, i32, i32)> {
        let mut s = c
            .prepare(
                "SELECT cell_x, cell_y, total FROM cell_totals
                 WHERE source = ? ORDER BY cell_x, cell_y",
            )
            .unwrap();
        s.query_map(duckdb::params![source], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
    }

    /// The denominator must range over exactly the rows the match rule
    /// considers. Counting a `w budowie` building here would deflate every
    /// BDOT10k ratio, and no functional test elsewhere would notice — the
    /// unmatched set stays correct either way.
    #[test]
    fn bdot10k_totals_exclude_non_eksploatowany_buildings() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO bdot10k_buildings (LOKALNYID, geom, KATEGORIAISTNIENIA) VALUES
                 ('standing', ST_MakeEnvelope(21.0,52.0,21.001,52.001), 'eksploatowany'),
                 ('also', ST_MakeEnvelope(21.0002,52.0002,21.0003,52.0003), 'eksploatowany'),
                 ('building', ST_MakeEnvelope(21.0004,52.0004,21.0005,52.0005), 'w budowie'),
                 ('gone', ST_MakeEnvelope(21.0006,52.0006,21.0007,52.0007), 'zniszczony');
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
        rebuild_all_in_txn(&c, "bdot10k").unwrap();
        let t = totals(&c, "bdot10k");
        assert_eq!(t.len(), 1, "all four points fall in one z14 cell");
        assert_eq!(
            t[0].2, 2,
            "only the two eksploatowany buildings count towards the denominator"
        );
    }

    /// The whole point of the table: a cell whose government objects are all
    /// matched has a total but no unmatched rows, which is what distinguishes
    /// a finished area from one with no government data at all.
    #[test]
    fn a_fully_matched_cell_still_gets_a_total() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('covered', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);
             INSERT INTO osm_buildings VALUES
                 (1,'way',NULL, ST_MakeEnvelope(20.9,51.9,21.1,52.1));",
        )
        .unwrap();
        let (cx, cy) = lonlat_to_tile(21.0005, 52.0005, CHANGE_CELL_ZOOM);
        crate::compare::incremental::recompute_cell(&c, "bdot10k", cx as i32, cy as i32).unwrap();

        let unmatched: i64 = c
            .query_row("SELECT COUNT(*) FROM bdot10k_unmatched", [], |r| r.get(0))
            .unwrap();
        assert_eq!(unmatched, 0, "the building is covered by OSM");
        assert_eq!(
            totals(&c, "bdot10k"),
            vec![(cx as i32, cy as i32, 1)],
            "a matched cell still records its denominator"
        );
    }

    /// Emptying a cell must remove its row, not leave a stale denominator
    /// behind — the same DELETE-then-INSERT discipline the unmatched tables
    /// use, and for the same reason.
    #[test]
    fn emptying_a_cell_removes_its_total_rather_than_zeroing_it() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('gone_soon', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
        let (cx, cy) = lonlat_to_tile(21.0005, 52.0005, CHANGE_CELL_ZOOM);
        rebuild_all_in_txn(&c, "bdot10k").unwrap();
        assert_eq!(totals(&c, "bdot10k").len(), 1);

        c.execute_batch("DELETE FROM bdot10k_buildings;").unwrap();
        recompute_cell_in_txn(&c, "bdot10k", cx as i32, cy as i32).unwrap();
        assert!(
            totals(&c, "bdot10k").is_empty(),
            "an emptied cell must leave no row at all, not a total of 0"
        );
    }

    /// The full rebuild and the per-cell recompute must produce identical
    /// totals, exactly as the full and incremental unmatched paths must (see
    /// `compare::full_vs_incremental_equivalence`). They are different SQL —
    /// one groups the whole table, the other filters an envelope and counts —
    /// so nothing but a test keeps them agreeing.
    #[test]
    fn full_rebuild_and_per_cell_recompute_agree() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO bdot10k_buildings (LOKALNYID, geom)
                 SELECT 'b' || i,
                        ST_MakeEnvelope(20.0 + i * 0.003, 52.0 + i * 0.002,
                                        20.0 + i * 0.003 + 0.0001, 52.0 + i * 0.002 + 0.0001)
                 FROM range(60) t(i);
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();

        rebuild_all_in_txn(&c, "bdot10k").unwrap();
        let full = totals(&c, "bdot10k");
        assert!(
            full.len() > 1,
            "sanity: the fixture must span several cells"
        );

        // Rebuild the same cells one at a time from an empty table.
        c.execute_batch("DELETE FROM cell_totals;").unwrap();
        for (cx, cy, _) in &full {
            recompute_cell_in_txn(&c, "bdot10k", *cx, *cy).unwrap();
        }
        assert_eq!(
            totals(&c, "bdot10k"),
            full,
            "per-cell totals must match the full rebuild cell for cell"
        );
    }

    #[test]
    fn prg_totals_count_addresses() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO prg_addresses VALUES
                 ('a', ST_Point(21.0, 52.0)),
                 ('b', ST_Point(21.0001, 52.0001)),
                 ('c', ST_Point(19.0, 50.0));",
        )
        .unwrap();
        rebuild_all_in_txn(&c, "prg").unwrap();
        let t = totals(&c, "prg");
        assert_eq!(t.len(), 2, "two distinct z14 cells");
        assert_eq!(t.iter().map(|r| r.2).sum::<i32>(), 3);
    }

    #[test]
    fn unknown_source_is_rejected() {
        let c = conn();
        let err = rebuild_all_in_txn(&c, "nope").unwrap_err().to_string();
        assert!(err.contains("unknown source"), "got: {err}");
    }
}
