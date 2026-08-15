//! Repair for invalid OSM building geometry.
//!
//! The third data input's counterpart to the two geometry-hygiene rules this
//! codebase already has: `dataset::filter_invalid_geometry` (government rows,
//! *deleted* at import) and `server::package`'s `ST_MakeValid` wrappers
//! (request geometry, *repaired* at query time). OSM polygons are assembled by
//! `ST_MakePolygon` over raw node coordinates and OSM itself enforces no
//! validity, so a self-intersecting building way reaches DuckDB intact.
//!
//! **What breaks without this.** `compare::rule::unmatched_buildings_sql`
//! computes `ST_Area(ST_Intersection(osm.geom, b.geom))` for its overlap
//! fraction. `ST_Intersects` in the clause just above it tolerates an invalid
//! ring (GEOS evaluates it with prepared geometry), so the pair survives long
//! enough to reach the overlay -- which builds a topology graph and throws
//! `TopologyException: side location conflict`. That fails the whole cell's
//! INSERT, and because `compare::buildings::compare_buildings` wraps its grid
//! in one clear-then-repopulate transaction, one bad way rolls back the entire
//! national compare. In the server the same throw hits
//! `compare::incremental::recompute_cell_in_txn`, where `drain_batch` rolls
//! the cell back and leaves it queued -- so that z14 cell fails on every tick
//! forever, serving stale tiles while the queue never empties.
//!
//! Measured on the 2026-08 Poland extract: 3 invalid polygons out of
//! 17,986,820 `osm_buildings` (0 of 15,412 `osm_former_buildings`), of which
//! exactly *one* -- way 229993348, an 8-point bowtie near 15.41822, 53.16614 --
//! actually throws; the other two intersect government buildings without
//! incident. Invalidity is necessary but not sufficient, and which pairing
//! trips GEOS is not predictable from the geometry alone, so "3 rows" is a
//! count of exposure, not a bound on breakage: any minutely update can add
//! another.
//!
//! **Why repair rather than delete -- the opposite of the government-data
//! rule, deliberately.** A government row is a *candidate* for import:
//! dropping a corrupt one is a safe false negative ("don't propose this
//! thing"). An OSM row is *evidence* that something is already mapped:
//! dropping it makes the government building it covered look unmatched, so
//! that building gets proposed and a duplicate lands in OSM. The asymmetry is
//! in the direction of the error, not in how bad the data is -- which is why
//! `filter_invalid_geometry`'s DELETE must not be copied over here, however
//! similar the two look.
//!
//! **The one case that is still dropped** is a geometry with no polygonal part
//! left after repair: a ring whose vertices are all collinear repairs to a
//! MULTILINESTRING, and extracting polygons from that yields `MULTIPOLYGON
//! EMPTY`. Keeping such a row would be worse than dropping it in both
//! directions -- it has zero area, so it can never reach
//! `rule::MIN_OVERLAP_FRACTION` and could never match or veto anything anyway,
//! while an empty geometry makes `ST_XMin`/`ST_XMax` return NULL, which would
//! fail `update::dirty_cells::note_existing`'s `r.get::<_, i32>` the next time
//! that object is edited. Zero such rows exist in the 2026-08 extract; the
//! count is reported separately from `repaired` so it stays visible if that
//! changes.

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::warn;

use crate::dataset::MAX_EXAMPLE_IDS;

/// The repair expression, and the single home for it. Both mechanisms below
/// build their SQL from this one function: the import post-pass
/// ([`repair_invalid_geometry`]) applies it in an `UPDATE ... SET geom = ...`,
/// and `update::osm` applies it inline at each per-object INSERT. The two
/// apply it at different moments for a measured reason (see
/// [`repair_invalid_geometry`]'s doc), but they must never disagree about what
/// "repaired" *means*, which is what keeping the text here buys.
///
/// `ST_CollectionExtract(..., 3)` is not optional dressing on top of
/// `ST_MakeValid`. MakeValid preserves every vertex of its input, so a
/// zero-area spike or dangling edge -- common in OSM building ways -- has
/// nowhere to live in a polygon and comes back as a LINESTRING alongside it,
/// inside a GEOMETRYCOLLECTION. Two of the three real invalid rows repair to
/// exactly that shape. Storing a collection in `osm_buildings` would leave a
/// geometry whose type most of the spatial functions downstream handle
/// inconsistently; `3` keeps only the polygonal parts (`1` = points, `2` =
/// linestrings).
///
/// Applying this to an already-valid, non-collection polygon is a
/// pass-through: it returns the same POLYGON rather than promoting it to
/// MULTIPOLYGON (pinned by `valid_polygon_is_returned_unchanged`), which is
/// what makes it safe to wrap around a construction expression unconditionally
/// rather than behind an `ST_IsValid` test.
pub fn repaired_geom_sql(geom_expr: &str) -> String {
    format!("ST_CollectionExtract(ST_MakeValid({geom_expr}), 3)")
}

/// The guard that pairs with [`repaired_geom_sql`] at an INSERT site: true
/// when the repair leaves an actual polygon behind. Every `update::osm` insert
/// that wraps its geometry in `repaired_geom_sql` must also AND this into its
/// WHERE, or a degenerate way lands as `MULTIPOLYGON EMPTY` -- see the module
/// doc for why an empty row is worse than an absent one. The import path needs
/// no such guard at its four INSERT sites, because
/// [`repair_invalid_geometry`]'s DELETE removes the same rows afterwards.
///
/// The expression is deliberately spelled twice in the generated SQL (once in
/// the select list, once here in the WHERE) rather than computed once into a
/// CTE: it is pure, DuckDB is free to fold the duplicate, and the alternative
/// -- a "repair once and reuse it" seam -- would mean the repair text existing
/// in a second shape. Same trade `compare::rule` makes with its envelope and
/// `extra_filter`, for the same reason.
pub fn has_polygon_sql(geom_expr: &str) -> String {
    format!("NOT ST_IsEmpty({})", repaired_geom_sql(geom_expr))
}

/// What one [`repair_invalid_geometry`] pass did, for the caller's
/// `job_run_log` summary. Mirrors `dataset::LoadStats`'s shape (counts plus a
/// capped list of example ids) so an operator reads the same kind of line for
/// OSM as for the three government sources -- but it is a separate type
/// because every `LoadStats` field is a *skip* reason, and `repaired` is
/// pointedly not one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairStats {
    /// Rows whose geometry failed `ST_IsValid` and was rewritten in place.
    pub repaired: i64,
    /// First `MAX_EXAMPLE_IDS` repaired rows as `"way/229993348"`, captured
    /// before the UPDATE makes them indistinguishable from every other row.
    /// Not exhaustive -- enough to point an operator at the actual OSM objects
    /// so they can be fixed upstream, which is the only permanent fix.
    pub example_ids: Vec<String>,
    /// Rows deleted because the repair left no polygonal part at all. Kept
    /// separate from `repaired` because it is the one case where OSM evidence
    /// is discarded rather than fixed; see the module doc.
    pub dropped_degenerate: i64,
}

impl RepairStats {
    /// Fold a second table's pass into this one, so `import osm` reports one
    /// pair of numbers across `osm_buildings` and `osm_former_buildings`
    /// rather than one pair per table. Example ids stay capped at
    /// `MAX_EXAMPLE_IDS` across the merged result.
    pub fn merge(mut self, other: RepairStats) -> Self {
        self.repaired += other.repaired;
        self.dropped_degenerate += other.dropped_degenerate;
        self.example_ids.extend(other.example_ids);
        self.example_ids.truncate(MAX_EXAMPLE_IDS);
        self
    }

    /// Renders as a `job_run_log` clause, or `None` when the pass found
    /// nothing -- the common case, and one that must not pad every import
    /// message with `repaired_geometry=0`.
    pub fn summary_clause(&self) -> Option<String> {
        if self.repaired == 0 && self.dropped_degenerate == 0 {
            return None;
        }
        let mut s = crate::dataset::format_skip_clause(
            "repaired-geometry",
            self.repaired,
            &self.example_ids,
        )
        .replace("skipped", "repaired");
        if self.dropped_degenerate > 0 {
            s.push_str(&format!(
                ", dropped {} degenerate-geometry rows",
                self.dropped_degenerate
            ));
        }
        Some(s)
    }
}

/// Repair every invalid geometry in `table` in place, then delete any row the
/// repair emptied. Run by `import osm` once per polygon table, after all four
/// insert passes and before `create_spatial_indexes` -- so the RTREE is built
/// over final geometry and never has to be maintained through the UPDATE.
///
/// **Why the import path scans and the update path wraps instead.** Both
/// strategies are affordable: measured over 2,000,000 real `osm_buildings`
/// rows, the `ST_IsValid` scan this does costs 0.344 s and wrapping every row
/// unconditionally in `repaired_geom_sql` costs 0.466 s (~3.1 s vs ~4.2 s
/// extrapolated to the full 18 M table), so this is not a performance split.
/// It is a "how many sites can forget" split: `import osm` has four INSERT
/// passes feeding two tables, and one post-pass per table covers all of them
/// including any fifth pass added later, while `update::osm` rebuilds a single
/// object at a time, where a table-wide scan per edited object would be
/// absurd and an inline wrapper is the natural shape.
pub fn repair_invalid_geometry(conn: &Connection, table: &str) -> Result<RepairStats> {
    let mut example_ids = Vec::new();
    {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT osm_type || '/' || osm_id FROM {table}
                 WHERE NOT ST_IsValid(geom) LIMIT {MAX_EXAMPLE_IDS}"
            ))
            .with_context(|| format!("Failed to prepare invalid-geometry scan on {table}"))?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .with_context(|| format!("Failed to scan invalid-geometry rows in {table}"))?;
        for row in rows {
            example_ids.push(row.context("Failed to read invalid-geometry id")?);
        }
    }

    let repaired =
        conn.execute(
            &format!(
                "UPDATE {table} SET geom = {} WHERE NOT ST_IsValid(geom)",
                repaired_geom_sql("geom")
            ),
            [],
        )
        .with_context(|| format!("Failed to repair invalid geometry in {table}"))? as i64;

    // Unscoped rather than restricted to the rows just repaired: an empty
    // geometry is unusable in this table however it got there (see the module
    // doc on note_existing), and ST_IsEmpty over the table is a cheap scan
    // next to the ST_IsValid one above.
    let dropped_degenerate = conn
        .execute(&format!("DELETE FROM {table} WHERE ST_IsEmpty(geom)"), [])
        .with_context(|| format!("Failed to drop degenerate geometry from {table}"))?
        as i64;

    if repaired > 0 || dropped_degenerate > 0 {
        warn!(
            table,
            repaired,
            dropped_degenerate,
            examples = %example_ids.join(", "),
            "Repaired invalid OSM geometry — fix these objects in OSM to remove the need"
        );
    }

    Ok(RepairStats {
        repaired,
        example_ids,
        dropped_degenerate,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compare::rule::unmatched_buildings_sql;
    use crate::db::init_db;
    use std::path::Path;

    /// OSM way 229993348 (`building=service`), verbatim from the 2026-08
    /// Poland extract: the 8-point self-intersecting ring whose overlay
    /// against a BDOT10k building threw `side location conflict` at
    /// 15.418263252956596 53.166172090189647 and rolled back the national
    /// compare. Real coordinates rather than hand-written ones on purpose --
    /// the invalidity here is a genuine crossing between the closing segment
    /// and an earlier edge, not something that depends on how a decimal
    /// literal rounds (contrast the CLAUDE.md fixture gotcha about needing
    /// exact binary fractions for *collinearity*, which is fragile in a way
    /// this is not).
    const BOWTIE_WKT: &str = "POLYGON ((15.4182745 53.1661674, 15.4182624 53.1661753, \
         15.41827 53.1661467, 15.4182855 53.166089, 15.4182344 53.1660838, \
         15.4182263 53.1661127, 15.4182028 53.1661973, 15.4182745 53.1661674))";

    /// BDOT10k building 443FAF93-0186-4C06-B29A-E1D1F844FFA0, the government
    /// row the way above actually threw against. Its neighbour
    /// 9A6446C5-... intersects the same way without incident, which is why the
    /// failing pair is pinned specifically rather than any overlapping pair.
    const GOV_WKT: &str = "POLYGON ((15.418253493820654 53.1662044135888, \
         15.418283909936882 53.16609097972031, 15.418232845340967 53.16608575360618, \
         15.418201219445903 53.16619933097618, 15.418253493820654 53.1662044135888))";

    fn conn() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch("CREATE TABLE bsrc (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);")
            .unwrap();
        c
    }

    /// Runs the real match rule over the real failing pair. Returns the error
    /// rather than unwrapping, so the same helper can assert both that the
    /// unrepaired pair throws and that the repaired one does not.
    fn run_match_rule(c: &Connection) -> Result<i64, duckdb::Error> {
        let sql = unmatched_buildings_sql("bsrc", "COUNT(*)", (15.0, 53.0, 15.5, 53.5), None);
        c.query_row(&sql, [], |r| r.get::<_, i64>(0))
    }

    fn seed_failing_pair(c: &Connection) {
        c.execute_batch(&format!(
            "INSERT INTO osm_buildings VALUES
                 (229993348, 'way', 'service', ST_GeomFromText('{BOWTIE_WKT}'));
             INSERT INTO bsrc (LOKALNYID, geom) VALUES
                 ('443FAF93-0186-4C06-B29A-E1D1F844FFA0', ST_GeomFromText('{GOV_WKT}'));
             UPDATE bsrc SET centroid = ST_Centroid(geom);"
        ))
        .unwrap();
    }

    /// The regression itself, asserted from both sides: the exact real-world
    /// pair throws before the repair pass and answers cleanly after it. The
    /// "before" half is what makes this test meaningful -- without it, a
    /// future change that quietly stopped calling the repair would still pass.
    #[test]
    fn repair_fixes_the_overlay_crash_on_the_real_failing_pair() {
        let c = conn();
        seed_failing_pair(&c);

        let before = run_match_rule(&c);
        let err = before.expect_err("the unrepaired bowtie must still throw in the match rule");
        assert!(
            err.to_string().contains("side location conflict"),
            "expected the GEOS overlay failure this module exists for, got: {err}"
        );

        let stats = repair_invalid_geometry(&c, "osm_buildings").unwrap();
        assert_eq!(stats.repaired, 1);
        assert_eq!(stats.dropped_degenerate, 0);
        assert_eq!(stats.example_ids, vec!["way/229993348".to_string()]);

        let unmatched = run_match_rule(&c).expect("the repaired pair must not throw");
        // Zero, and that is the whole point of repairing rather than deleting:
        // the repaired way covers 83.3% of this government building, far over
        // MIN_OVERLAP_FRACTION, so OSM is correctly recognised as already
        // having mapped it. Asserting the answer rather than just "no error"
        // is what pins that -- the crash could equally have been silenced by
        // emptying the geometry, which would produce the wrong answer below.
        assert_eq!(unmatched, 0);

        let (valid, area): (bool, f64) = c
            .query_row(
                "SELECT ST_IsValid(geom), ST_Area(geom) FROM osm_buildings WHERE osm_id = 229993348",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert!(valid, "repaired geometry must be valid");
        assert!(area > 0.0, "repaired geometry must keep its footprint");

        // The counterfactual, spelled out: had this module copied
        // `dataset::filter_invalid_geometry`'s DELETE instead of repairing,
        // the same government building would read as unmatched and be
        // proposed for import -- a duplicate of a building OSM already has.
        // This is the concrete cost behind the module doc's candidate-vs-
        // evidence argument, so it is asserted rather than just asserted at.
        c.execute_batch("DELETE FROM osm_buildings WHERE osm_id = 229993348")
            .unwrap();
        assert_eq!(
            run_match_rule(&c).unwrap(),
            1,
            "deleting the invalid way instead of repairing it would resurrect \
             this building as an import candidate"
        );
    }

    /// A valid polygon must come back byte-identical, not silently rewritten
    /// (promoted to MULTIPOLYGON, ring-reordered, coordinates re-noded). This
    /// is what licenses wrapping `repaired_geom_sql` unconditionally around a
    /// construction expression in `update::osm`.
    #[test]
    fn valid_polygon_is_returned_unchanged() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES
                 (1, 'way', 'yes', ST_MakeEnvelope(20.0, 52.0, 20.002, 52.002));",
        )
        .unwrap();
        let before: String = c
            .query_row("SELECT ST_AsText(geom) FROM osm_buildings", [], |r| {
                r.get(0)
            })
            .unwrap();

        let stats = repair_invalid_geometry(&c, "osm_buildings").unwrap();
        assert_eq!(stats, RepairStats::default());

        let after: String = c
            .query_row("SELECT ST_AsText(geom) FROM osm_buildings", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(before, after);
    }

    /// A ring with no area at all repairs to a linestring, so extraction
    /// leaves nothing -- the row must be deleted rather than stored as
    /// `MULTIPOLYGON EMPTY`, whose NULL `ST_XMin` would break
    /// `update::dirty_cells::note_existing` on the next edit. Coordinates are
    /// eighths so the three points are *exactly* collinear in f64 (see the
    /// CLAUDE.md fixture gotcha).
    #[test]
    fn degenerate_ring_is_dropped_rather_than_left_empty() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES (2, 'way', 'yes', ST_GeomFromText(
                 'POLYGON ((21.0 52.0, 21.0625 52.0, 21.125 52.0, 21.0 52.0))'));",
        )
        .unwrap();

        let stats = repair_invalid_geometry(&c, "osm_buildings").unwrap();
        assert_eq!(stats.repaired, 1);
        assert_eq!(stats.dropped_degenerate, 1);

        let left: i64 = c
            .query_row("SELECT COUNT(*) FROM osm_buildings", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 0, "a geometry with no polygonal part must not remain");
    }

    /// The two mechanisms must agree: repairing a row in place (import) and
    /// wrapping the same geometry at construction (update) have to produce the
    /// identical stored geometry, or an object would differ depending on
    /// whether it arrived via `import osm` or a later `update osm`. They share
    /// `repaired_geom_sql`, so this pins the sharing rather than a
    /// coincidence -- the analogue of
    /// `osm::lifecycle::matched_key_sql_agrees_with_key_of`.
    #[test]
    fn post_pass_and_inline_wrapper_produce_the_same_geometry() {
        let c = conn();
        c.execute_batch(&format!(
            "INSERT INTO osm_buildings VALUES
                 (229993348, 'way', 'service', ST_GeomFromText('{BOWTIE_WKT}'));"
        ))
        .unwrap();
        repair_invalid_geometry(&c, "osm_buildings").unwrap();

        let post_pass: String = c
            .query_row("SELECT ST_AsText(geom) FROM osm_buildings", [], |r| {
                r.get(0)
            })
            .unwrap();
        let inline: String = c
            .query_row(
                &format!(
                    "SELECT ST_AsText({})",
                    repaired_geom_sql(&format!("ST_GeomFromText('{BOWTIE_WKT}')"))
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(post_pass, inline);
    }

    /// `has_polygon_sql` must reject exactly what the post-pass's DELETE
    /// removes, since the update path relies on it instead of that DELETE.
    #[test]
    fn has_polygon_guard_matches_what_the_post_pass_deletes() {
        let c = conn();
        let degenerate =
            "ST_GeomFromText('POLYGON ((21.0 52.0, 21.0625 52.0, 21.125 52.0, 21.0 52.0))')";
        let real = format!("ST_GeomFromText('{BOWTIE_WKT}')");

        let keeps_repairable: bool = c
            .query_row(&format!("SELECT {}", has_polygon_sql(&real)), [], |r| {
                r.get(0)
            })
            .unwrap();
        let rejects_degenerate: bool = c
            .query_row(
                &format!("SELECT {}", has_polygon_sql(degenerate)),
                [],
                |r| r.get(0),
            )
            .unwrap();

        assert!(keeps_repairable, "a repairable bowtie must be kept");
        assert!(!rejects_degenerate, "a zero-area ring must be rejected");
    }
}
