//! The single home for "which government object is unmatched against OSM".
//!
//! Full building `compare` and the per-cell incremental recompute both call
//! `unmatched_buildings_sql` here, so they share the predicate text itself.
//! Full address `compare` uses its own grid-key SQL for performance and shares
//! only `MATCH_DISTANCE_METERS`; two tests pin the paths together —
//! `addresses::full_and_per_cell_paths_agree` (grid-key vs. per-cell rule) and
//! `compare::full_vs_incremental_equivalence` (full `compare` vs.
//! reconcile+drain, end to end).

/// (min_lon, min_lat, max_lon, max_lat).
pub type Bounds = (f64, f64, f64, f64);

pub const MATCH_DISTANCE_METERS: f64 = 50.0;
/// OSM read buffer around a cell for address matching. Matches /package.
///
/// **Coupled to `MATCH_DISTANCE_METERS`.** 0.001° is ~64 m of longitude at
/// Poland's northern edge (54.8 °N) and ~111 m of latitude, so it covers the
/// 50 m match distance with only 1.28× headroom east-west. Raising the distance
/// past ~64 m would silently break read-wide/write-narrow — an OSM address just
/// outside the buffered read would stop matching — with no test failure. Raise
/// this buffer alongside it.
pub const OSM_MATCH_BUFFER_DEG: f64 = 0.001;

pub fn buffer(b: Bounds, deg: f64) -> Bounds {
    (b.0 - deg, b.1 - deg, b.2 + deg, b.3 + deg)
}

/// SQL for a constant `ST_MakeEnvelope` literal describing `area`. One home for
/// the format, so a candidate CTE's envelope and the predicate's own envelope
/// cannot drift apart -- a mis-ordered argument in one but not the other would
/// silently narrow to the wrong cell.
pub fn envelope_sql(area: Bounds) -> String {
    let (x1, y1, x2, y2) = area;
    format!("ST_MakeEnvelope({x1}, {y1}, {x2}, {y2})")
}

/// BDOT10k-only pre-filter for `unmatched_buildings_sql`'s `extra_filter`:
/// only rows still standing count as a government building to compare at
/// all — excludes `w budowie` (under construction), `nieczynny` (inactive)
/// and `zniszczony` (destroyed) BDOT10k buildings from ever being matched or
/// unmatched. EGIB carries no equivalent column, so its callers pass `None`.
pub const BDOT10K_EKSPLOATOWANY_FILTER: &str = "b.KATEGORIAISTNIENIA = 'eksploatowany'";

/// Minimum fraction of a government building's footprint area that an
/// OSM building's footprint must cover for `unmatched_buildings_sql` to
/// count it as matched. Guards the full-geometry `ST_Intersects` test below
/// against bare edge/corner touches — two adjacent, genuinely distinct
/// buildings sharing a party wall (or a digitization sliver between them)
/// intersect with ~0 overlap area, and that must not count as a match.
/// Chosen empirically (see the investigation behind this predicate,
/// id `146518_8.0502.122_BUD`): on a dense Warsaw sample, sweeping this from
/// 2% to 50% moved the unmatched count by only ~10% end to end — there is no
/// sharp elbow, so this is a round middle-of-the-curve value, not a
/// precisely derived one.
pub const MIN_OVERLAP_FRACTION: f64 = 0.10;

/// Minimum fraction of a government building's footprint that an
/// `osm_former_buildings` polygon must cover for that building to be
/// *suppressed* — neither matched nor offered for import, because OSM mappers
/// have recorded that a building here is gone.
///
/// Deliberately separate from MIN_OVERLAP_FRACTION even though it holds the
/// same value: this answers "is this veto trustworthy", not "did OSM already
/// map this", and the two must be free to move apart. See
/// docs/former_buildings.md for the measured sweep behind 0.10, and note the
/// asymmetry if it is ever retuned: a false veto costs a missed import (the
/// building simply is not proposed, and returns as soon as the OSM tag is
/// corrected); a missed veto costs a wrong import.
pub const FORMER_BUILDING_MIN_OVERLAP_FRACTION: f64 = 0.10;

/// The `osm_buildings` half of the building rule: does a live OSM building
/// polygon cover at least `MIN_OVERLAP_FRACTION` of `b`'s own footprint?
///
/// `unmatched_buildings_sql` and `suppressed_buildings_sql` both *negate*
/// this — a suppressed row is "unmatched but for the veto" — so the clause
/// text lives here once instead of being spelled out in each. Correlates on
/// `b`, so a caller must alias its government-source relation `b`.
fn osm_building_covers_sql(envelope: &str) -> String {
    format!(
        "EXISTS (
               SELECT 1 FROM osm_buildings osm
               WHERE ST_Intersects(osm.geom, {envelope})
                 AND ST_Intersects(osm.geom, b.geom)
                 AND ST_Area(ST_Intersection(osm.geom, b.geom)) / ST_Area(b.geom) >= {MIN_OVERLAP_FRACTION}
           )"
    )
}

/// The `osm_former_buildings` half — the suppression veto itself.
///
/// Same one-home reasoning as `osm_building_covers_sql`, and the reason the
/// two polarities can never drift: `unmatched_buildings_sql` negates this,
/// `suppressed_buildings_sql` requires it, and both read this one text.
/// That is what makes `matched + unmatched + suppressed = total` exact
/// rather than merely intended. Correlates on `b`, same as above.
fn former_building_covers_sql(envelope: &str) -> String {
    format!(
        "EXISTS (
               SELECT 1 FROM osm_former_buildings f
               WHERE ST_Intersects(f.geom, {envelope})
                 AND ST_Intersects(f.geom, b.geom)
                 AND ST_Area(ST_Intersection(f.geom, b.geom)) / ST_Area(b.geom) >= {FORMER_BUILDING_MIN_OVERLAP_FRACTION}
           )"
    )
}

/// Unmatched building rows: government centroid within `area`, and no
/// osm_buildings polygon whose footprint covers at least
/// `MIN_OVERLAP_FRACTION` of the government building's own footprint (osm
/// filtered to `area` for the R-tree scan), and no `osm_former_buildings`
/// polygon covers at least `FORMER_BUILDING_MIN_OVERLAP_FRACTION` of it
/// either — a building OSM mappers have recorded as gone is suppressed, not
/// offered for import (see `suppressed_buildings_sql`, which selects exactly
/// what this clause excludes).
///
/// Matching on full-geometry overlap rather than centroid-containment is
/// deliberate: a government building's centroid can legitimately fall
/// outside every individual OSM building polygon when OSM has split the
/// same physical building into multiple adjacent ways (e.g. a tenement
/// block mapped as separate wings) — the true footprint is covered, but no
/// single OSM polygon contains the centroid point. See
/// `146518_8.0502.122_BUD`, where two adjacent OSM ways together covered
/// 99.98% of the government footprint yet neither contained its centroid.
///
/// `source_table` must carry a `centroid GEOMETRY` column (bdot10k_buildings
/// and egib_buildings both do — see `DatasetSpec::with_centroid_select`).
/// The *outer* `ST_Intersects(b.centroid, ...)` scoping filter (which cells'
/// worth of government buildings to even consider) still reads that stored
/// column rather than computing `ST_Centroid(b.geom)` inline, for the same
/// RTREE-index reason as before (docs/per_cell_recompute_full_scan.md): an
/// RTREE index cannot be used through a function wrapped around the indexed
/// column, but it can be used against a plain column reference. The *match*
/// test itself now reads `b.geom`/`osm.geom` directly — DuckDB lowers a
/// correlated `ST_Intersects(indexed_col, expr)` to a dedicated
/// `SPATIAL_JOIN` physical operator fed by both sides' RTREE-narrowed
/// candidates rather than a nested loop (verified via `EXPLAIN`), so this
/// stays index-accelerated on both `b.geom` and `osm.geom`; the former-building
/// clause uses the same constant-envelope scoping so it stays index-accelerated
/// too.
///
/// `extra_filter`, when set, is ANDed into the WHERE clause alongside the
/// `b`-aliased source row (see `BDOT10K_EKSPLOATOWANY_FILTER`).
pub fn unmatched_buildings_sql(
    source_table: &str,
    select_list: &str,
    area: Bounds,
    extra_filter: Option<&str>,
) -> String {
    let envelope = envelope_sql(area);
    let extra = extra_filter
        .map(|f| format!("AND {f}\n           "))
        .unwrap_or_default();
    format!(
        "SELECT {select_list}
         FROM {source_table} b
         WHERE ST_Intersects(b.centroid, {envelope})
           {extra}AND NOT {osm_covers}
           AND NOT {former_covers}",
        osm_covers = osm_building_covers_sql(&envelope),
        former_covers = former_building_covers_sql(&envelope),
    )
}

/// The mirror image of `unmatched_buildings_sql`'s former-building clause:
/// rows that are "unmatched but for the veto" — no `osm_buildings` polygon
/// covers them, but an `osm_former_buildings` polygon does. This is exactly
/// the row set the veto removes from `unmatched_buildings_sql`, so
/// `matched + unmatched + suppressed = total` is exact: a plain "overlaps a
/// former polygon" count would double-count rows also covered by a live OSM
/// building and could push `matched` negative.
///
/// **The clause order here is load-bearing, and it is why this doesn't just
/// call the same flat predicate `unmatched_buildings_sql` uses.** Its one
/// caller (`compare::buildings::compare_buildings`) runs this *once over the
/// whole source extent* rather than per grid cell, so the two clauses face
/// wildly different row counts and the cheap one must go first:
///
/// - `former_building_covers_sql` is savagely selective — nationally ~6k of
///   16.35M BDOT10k rows overlap a former polygon at all — and it is driven
///   by the ~15k-row `osm_former_buildings` table.
/// - `osm_building_covers_sql` is the expensive one: an anti-join against
///   17.99M `osm_buildings` rows.
///
/// Written flat, DuckDB de-correlates both `EXISTS` clauses into nested
/// `DELIM_JOIN`s and plans the *anti*-join underneath the semi-join, so it
/// computes the unmatched set for all of Poland and only then keeps the ~4k
/// rows the veto covers. The delim machinery deduplicates and materializes
/// the correlated column — which is `b.geom`, 2.18 GB of WKB at national
/// scale — and the query dies with DuckDB's "failed to pin block" OOM
/// against a 4 GB `memory_limit` (measured: 71 s to failure, ~15 GB spilled
/// to temp on the way, so raising `max_temp_directory_size` does not save
/// it). Filtering to the veto's candidates first turns the same query into
/// 4.7 s / 3.8 GB for bdot10k and 4.9 s / 3.9 GB for egib.
///
/// Reordering also restores the index, which is the non-obvious half:
/// `osm_buildings`'s scan is an `RTREE_INDEX_SCAN` in *both* shapes, but its
/// search window is `Bounds: deferred (from join filter)` — derived at
/// runtime from the probe side. Probing with every building in Poland yields
/// a whole-country bound that prunes nothing, so the index was never the
/// problem and no index hint or `rtree_index_scan_ratio` tweak can help
/// (forcing an R-tree walk over the full table measured 13.4 s vs 0.53 s for
/// the sequential scan). Shrinking the probe side to ~4k geometries is what
/// makes that deferred bound selective again.
///
/// **`MATERIALIZED` is insurance, not the active ingredient — the CTE is.**
/// Measured on the real 16.35M-row table, a bare `WITH` plans identically
/// (it differs only in losing the `CTE_SCAN` operator) and runs in 5.4 s /
/// 3.8 GB for the same answer. The keyword is kept so a future re-plan can't
/// fold the CTE back into the outer query, the same call this codebase makes
/// in `compare::incremental` — and, as there, don't read its presence as
/// something a test pins. What is pinned, by
/// `suppressed_buildings_predicate_filters_by_the_veto_first`, is the clause
/// order.
pub fn suppressed_buildings_sql(
    source_table: &str,
    select_list: &str,
    area: Bounds,
    extra_filter: Option<&str>,
) -> String {
    let envelope = envelope_sql(area);
    let extra = extra_filter
        .map(|f| format!("AND {f}\n               "))
        .unwrap_or_default();
    format!(
        "WITH candidates AS MATERIALIZED (
             SELECT b.*
             FROM {source_table} b
             WHERE ST_Intersects(b.centroid, {envelope})
               {extra}AND {former_covers}
         )
         SELECT {select_list}
         FROM candidates b
         WHERE NOT {osm_covers}",
        former_covers = former_building_covers_sql(&envelope),
        osm_covers = osm_building_covers_sql(&envelope),
    )
}

/// Unmatched address rows: government point within `write` and no osm_addresses
/// point (read from `read`) with equal normalized housenumber within 50 m.
/// NULL housenumber never matches (SQL `= NULL` is never true).
pub fn unmatched_addresses_in_cell_sql(
    source_table: &str,
    select_list: &str,
    write: Bounds,
    read: Bounds,
) -> String {
    let write_envelope = envelope_sql(write);
    let read_envelope = envelope_sql(read);
    let dist = MATCH_DISTANCE_METERS;
    format!(
        "SELECT {select_list}
         FROM {source_table} a
         WHERE ST_Intersects(a.geom, {write_envelope})
           AND NOT EXISTS (
               SELECT 1 FROM osm_addresses o
               WHERE ST_Intersects(o.geom, {read_envelope})
                 AND UPPER(TRIM(o.housenumber)) = UPPER(TRIM(a.numer_porzadkowy))
                 AND ST_Distance_Sphere(o.geom, a.geom) <= {dist}
           )"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    fn conn() -> duckdb::Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch(
            "CREATE TABLE bsrc (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE asrc (lokalny_id VARCHAR, numer_porzadkowy VARCHAR, geom GEOMETRY);",
        )
        .unwrap();
        c
    }

    #[test]
    fn building_contained_by_osm_is_not_unmatched() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES
                 (1,'way',NULL, ST_MakeEnvelope(20.0,52.0,20.002,52.002));
             INSERT INTO bsrc (LOKALNYID, geom) VALUES
                 ('in', ST_MakeEnvelope(20.0005,52.0005,20.0007,52.0007)),
                 ('out', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             UPDATE bsrc SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
        let sql = unmatched_buildings_sql("bsrc", "b.LOKALNYID", (14.0, 49.0, 25.0, 55.0), None);
        let ids: Vec<String> = {
            let mut s = c.prepare(&sql).unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            ids,
            vec!["out".to_string()],
            "only the uncontained building is unmatched"
        );
    }

    /// Reproduces `146518_8.0502.122_BUD`: OSM maps the same physical
    /// building as two adjacent ways ('a' and 'b') with a small gap between
    /// them, and the government building's centroid falls in that gap —
    /// so neither way *contains* the centroid, but each individually covers
    /// well over `MIN_OVERLAP_FRACTION` of the government footprint. Under
    /// the old centroid-containment rule this building was (wrongly)
    /// unmatched.
    #[test]
    fn building_split_across_two_osm_ways_is_matched_via_overlap() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES
                 (1,'way',NULL, ST_MakeEnvelope(20.0,52.0,20.001,52.002)),
                 (2,'way',NULL, ST_MakeEnvelope(20.0015,52.0,20.0025,52.002));
             INSERT INTO bsrc (LOKALNYID, geom) VALUES
                 ('split', ST_MakeEnvelope(20.0,52.0,20.0025,52.002));
             UPDATE bsrc SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
        // Sanity check the fixture actually reproduces the gap: the
        // centroid must land outside both OSM ways for this test to be
        // exercising the fix rather than the old behaviour by accident.
        let centroid_uncontained: bool = c
            .query_row(
                "SELECT NOT EXISTS (
                     SELECT 1 FROM osm_buildings osm, bsrc b
                     WHERE b.LOKALNYID = 'split' AND ST_Contains(osm.geom, b.centroid)
                 )",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            centroid_uncontained,
            "fixture must reproduce a centroid landing outside every OSM way"
        );

        let sql = unmatched_buildings_sql("bsrc", "b.LOKALNYID", (14.0, 49.0, 25.0, 55.0), None);
        let ids: Vec<String> = {
            let mut s = c.prepare(&sql).unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            ids,
            Vec::<String>::new(),
            "a building split across adjacent OSM ways must count as matched"
        );
    }

    /// The overlap-fraction floor's other side: a government building that
    /// merely clips the corner of an unrelated OSM building (well under
    /// `MIN_OVERLAP_FRACTION`) must still count as unmatched — plain
    /// `ST_Intersects` with no floor would wrongly match it.
    #[test]
    fn building_barely_touching_osm_neighbor_stays_unmatched() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES
                 (1,'way',NULL, ST_MakeEnvelope(20.0,52.0,20.001,52.001));
             INSERT INTO bsrc (LOKALNYID, geom) VALUES
                 ('barely_touching', ST_MakeEnvelope(20.0005,52.0005,20.0505,52.0105));
             UPDATE bsrc SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
        let sql = unmatched_buildings_sql("bsrc", "b.LOKALNYID", (14.0, 49.0, 25.0, 55.0), None);
        let ids: Vec<String> = {
            let mut s = c.prepare(&sql).unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            ids,
            vec!["barely_touching".to_string()],
            "a mere corner-clip below MIN_OVERLAP_FRACTION must not count as a match"
        );
    }

    /// The actual regression guard for the per-cell-recompute fix
    /// (docs/per_cell_recompute_full_scan.md): if `unmatched_buildings_sql`
    /// ever goes back to wrapping the indexed column in `ST_Centroid()`, this
    /// fails, because an RTREE index cannot be used through a function
    /// applied to the indexed column.
    #[test]
    fn unmatched_buildings_predicate_uses_the_centroid_rtree_index() {
        let c = conn();
        c.execute_batch(
            "CREATE INDEX bsrc_centroid_idx ON bsrc USING RTREE (centroid);
             INSERT INTO bsrc (LOKALNYID, geom)
                 SELECT 'b' || i,
                        ST_MakeEnvelope(20.0 + i * 0.0001, 52.0,
                                        20.0 + i * 0.0001 + 0.00005, 52.00005)
                 FROM range(20000) t(i);
             UPDATE bsrc SET centroid = ST_Centroid(geom);",
        )
        .unwrap();

        let sql = unmatched_buildings_sql("bsrc", "b.LOKALNYID", (20.5, 52.0, 20.6, 52.1), None);
        let mut stmt = c.prepare(&format!("EXPLAIN {sql}")).unwrap();
        let mut rows = stmt.query([]).unwrap();
        let mut plan = String::new();
        while let Some(row) = rows.next().unwrap() {
            plan.push_str(&row.get::<_, String>(1).unwrap_or_default());
        }
        assert!(
            plan.contains("RTREE_INDEX_SCAN"),
            "the predicate must be able to use the centroid RTREE index, got plan: {plan}"
        );
    }

    /// `suppressed_buildings_sql` must apply the former-building veto *first*,
    /// building the candidate set the `osm_buildings` anti-join then runs over
    /// — not spell both clauses flat the way `unmatched_buildings_sql` does.
    /// Its one caller runs it once over the whole source extent, where the
    /// flat shape plans the 17.99M-row anti-join *underneath* the semi-join
    /// and OOMs; see the function's doc comment for the measurements.
    ///
    /// Deliberately a structural assertion rather than an `EXPLAIN` one: on
    /// the real 16.35M-row table the plan is the same with and without
    /// `MATERIALIZED` (bare `WITH` measured 5.4 s / 3.8 GB against the
    /// keyword's 4.7 s / 3.8 GB, identical answer), so asserting on plan text
    /// would pin the keyword instead of the property that actually matters.
    #[test]
    fn suppressed_buildings_predicate_filters_by_the_veto_first() {
        let sql = suppressed_buildings_sql("bsrc", "COUNT(*)", (14.0, 49.0, 25.0, 55.0), None);

        // "osm_former_buildings" does not contain "osm_buildings" as a
        // substring, so these two finds cannot collide.
        let veto_at = sql
            .find("FROM osm_former_buildings")
            .expect("the veto clause must be present");
        let anti_join_at = sql
            .find("FROM osm_buildings")
            .expect("the osm_buildings anti-join must be present");
        assert!(
            veto_at < anti_join_at,
            "the selective former-building veto must be evaluated before the \
             expensive osm_buildings anti-join, got: {sql}"
        );
        assert!(
            sql.contains("WITH candidates AS MATERIALIZED"),
            "the veto must build a candidates CTE that the anti-join then \
             filters, got: {sql}"
        );
    }

    /// The CTE restructure moved the `ST_Intersects(b.centroid, ...)` scoping
    /// filter inside `candidates`, so this is the suppressed-side twin of
    /// `unmatched_buildings_predicate_uses_the_centroid_rtree_index`: the
    /// source table's centroid RTREE index must still be reachable through
    /// the CTE boundary.
    #[test]
    fn suppressed_buildings_predicate_uses_the_centroid_rtree_index() {
        let c = conn();
        c.execute_batch(
            "CREATE INDEX bsrc_centroid_idx ON bsrc USING RTREE (centroid);
             INSERT INTO osm_former_buildings VALUES
                 (1, 'way', 'demolished:building', 'yes',
                  ST_MakeEnvelope(20.55, 52.05, 20.551, 52.051));
             INSERT INTO bsrc (LOKALNYID, geom)
                 SELECT 'b' || i,
                        ST_MakeEnvelope(20.0 + i * 0.0001, 52.0,
                                        20.0 + i * 0.0001 + 0.00005, 52.00005)
                 FROM range(20000) t(i);
             UPDATE bsrc SET centroid = ST_Centroid(geom);",
        )
        .unwrap();

        let sql = suppressed_buildings_sql("bsrc", "b.LOKALNYID", (20.5, 52.0, 20.6, 52.1), None);
        let mut stmt = c.prepare(&format!("EXPLAIN {sql}")).unwrap();
        let mut rows = stmt.query([]).unwrap();
        let mut plan = String::new();
        while let Some(row) = rows.next().unwrap() {
            plan.push_str(&row.get::<_, String>(1).unwrap_or_default());
        }
        // Substring "RTREE_IN" rather than the full operator name: DuckDB's
        // EXPLAIN truncates labels to fit box width in wide plans (same
        // reason as server::tiles and compare::totals).
        assert!(
            plan.contains("RTREE_IN"),
            "the candidates CTE must still reach the centroid RTREE index, got plan: {plan}"
        );
    }

    /// The veto's basic shape: an `osm_former_buildings` polygon that fully
    /// covers the government building's footprint suppresses it — it must not
    /// count as unmatched, mirroring `extra_filter_excludes_non_eksploatowany_buildings`
    /// but for the former-building clause instead of `extra_filter`.
    #[test]
    fn former_building_fully_overlapping_suppresses_the_government_building() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO bsrc (LOKALNYID, geom) VALUES
                 ('gone', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             UPDATE bsrc SET centroid = ST_Centroid(geom);
             INSERT INTO osm_former_buildings VALUES
                 (1, 'way', 'demolished:building', 'yes',
                  ST_MakeEnvelope(20.9999,51.9999,21.0011,52.0011));",
        )
        .unwrap();
        let sql = unmatched_buildings_sql("bsrc", "b.LOKALNYID", (14.0, 49.0, 25.0, 55.0), None);
        let ids: Vec<String> = {
            let mut s = c.prepare(&sql).unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            ids,
            Vec::<String>::new(),
            "a government building fully covered by a former-building polygon must be suppressed"
        );
    }

    /// The overlap-fraction floor's other side, for the former-building clause
    /// this time: a government building that merely clips the corner of an
    /// unrelated former-building polygon (well under
    /// `FORMER_BUILDING_MIN_OVERLAP_FRACTION`) must still count as unmatched.
    /// A bare `ST_Intersects` veto (no floor) would wrongly suppress it —
    /// this is the test that catches someone "simplifying" the veto that way.
    /// Modelled on `building_barely_touching_osm_neighbor_stays_unmatched`,
    /// reusing the same fixture numbers against `osm_former_buildings`
    /// instead of `osm_buildings`.
    #[test]
    fn building_barely_touching_former_neighbor_stays_unmatched() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_former_buildings VALUES
                 (1, 'way', 'demolished:building', 'yes',
                  ST_MakeEnvelope(20.0,52.0,20.001,52.001));
             INSERT INTO bsrc (LOKALNYID, geom) VALUES
                 ('barely_touching', ST_MakeEnvelope(20.0005,52.0005,20.0505,52.0105));
             UPDATE bsrc SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
        let sql = unmatched_buildings_sql("bsrc", "b.LOKALNYID", (14.0, 49.0, 25.0, 55.0), None);
        let ids: Vec<String> = {
            let mut s = c.prepare(&sql).unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            ids,
            vec!["barely_touching".to_string()],
            "a mere corner-clip below FORMER_BUILDING_MIN_OVERLAP_FRACTION must not suppress it"
        );
    }

    /// The two `NOT EXISTS` clauses in `unmatched_buildings_sql` are
    /// independent: the former-building veto must fire even when
    /// `osm_buildings` is completely empty (no match rule to interact with).
    #[test]
    fn former_building_veto_fires_with_osm_buildings_empty() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO bsrc (LOKALNYID, geom) VALUES
                 ('gone', ST_MakeEnvelope(21.0,52.0,21.001,52.001));
             UPDATE bsrc SET centroid = ST_Centroid(geom);
             INSERT INTO osm_former_buildings VALUES
                 (1, 'way', 'demolished:building', 'yes',
                  ST_MakeEnvelope(20.9999,51.9999,21.0011,52.0011));",
        )
        .unwrap();
        let empty_osm_buildings: i64 = c
            .query_row("SELECT COUNT(*) FROM osm_buildings", [], |r| r.get(0))
            .unwrap();
        assert_eq!(empty_osm_buildings, 0, "sanity: osm_buildings is empty");

        let sql = unmatched_buildings_sql("bsrc", "b.LOKALNYID", (14.0, 49.0, 25.0, 55.0), None);
        let ids: Vec<String> = {
            let mut s = c.prepare(&sql).unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            ids,
            Vec::<String>::new(),
            "the former-building veto must fire independently of osm_buildings"
        );
    }

    /// `suppressed_buildings_sql` returns exactly what the veto removes from
    /// `unmatched_buildings_sql`, so on a small fixture with one matched, one
    /// suppressed and one plain-unmatched building, matched + unmatched +
    /// suppressed must equal total.
    #[test]
    fn suppressed_plus_unmatched_plus_matched_equals_total() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES
                 (1,'way',NULL, ST_MakeEnvelope(20.0,52.0,20.001,52.001));
             INSERT INTO osm_former_buildings VALUES
                 (2, 'way', 'demolished:building', 'yes',
                  ST_MakeEnvelope(21.9999,52.9999,22.0011,53.0011));
             INSERT INTO bsrc (LOKALNYID, geom) VALUES
                 ('matched', ST_MakeEnvelope(20.0002,52.0002,20.0008,52.0008)),
                 ('suppressed', ST_MakeEnvelope(22.0,53.0,22.001,53.001)),
                 ('plain_unmatched', ST_MakeEnvelope(23.0,54.0,23.001,54.001));
             UPDATE bsrc SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
        let area = (14.0, 49.0, 25.0, 55.0);

        let unmatched_sql = unmatched_buildings_sql("bsrc", "b.LOKALNYID", area, None);
        let unmatched: Vec<String> = {
            let mut s = c.prepare(&unmatched_sql).unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(unmatched, vec!["plain_unmatched".to_string()]);

        let suppressed_sql = suppressed_buildings_sql("bsrc", "b.LOKALNYID", area, None);
        let suppressed: Vec<String> = {
            let mut s = c.prepare(&suppressed_sql).unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(suppressed, vec!["suppressed".to_string()]);

        let total: i64 = c
            .query_row("SELECT COUNT(*) FROM bsrc", [], |r| r.get(0))
            .unwrap();
        assert_eq!(total, 3);
        let matched = total - unmatched.len() as i64 - suppressed.len() as i64;
        assert_eq!(
            matched, 1,
            "matched + unmatched + suppressed must equal total"
        );
    }

    #[test]
    fn extra_filter_excludes_non_eksploatowany_buildings() {
        let c = conn();
        c.execute_batch(
            "ALTER TABLE bsrc ADD COLUMN KATEGORIAISTNIENIA VARCHAR;
             INSERT INTO bsrc (LOKALNYID, geom, KATEGORIAISTNIENIA) VALUES
                 ('standing', ST_MakeEnvelope(21.0,52.0,21.001,52.001), 'eksploatowany'),
                 ('under_construction', ST_MakeEnvelope(22.0,53.0,22.001,53.001), 'w budowie');
             UPDATE bsrc SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
        let sql = unmatched_buildings_sql(
            "bsrc",
            "b.LOKALNYID",
            (14.0, 49.0, 25.0, 55.0),
            Some(BDOT10K_EKSPLOATOWANY_FILTER),
        );
        let ids: Vec<String> = {
            let mut s = c.prepare(&sql).unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            ids,
            vec!["standing".to_string()],
            "the non-eksploatowany building must never count as unmatched"
        );
    }

    #[test]
    fn address_within_50m_same_hn_is_not_unmatched() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_addresses VALUES (1,'node','12',NULL,NULL,NULL, ST_Point(21.01,52.2102));
             INSERT INTO asrc VALUES ('match','12', ST_Point(21.01,52.21));
             INSERT INTO asrc VALUES ('far','12', ST_Point(21.01,52.212));",
        )
        .unwrap();
        let area = (21.0, 52.2, 21.02, 52.22);
        let sql = unmatched_addresses_in_cell_sql(
            "asrc",
            "a.lokalny_id",
            area,
            buffer(area, OSM_MATCH_BUFFER_DEG),
        );
        let ids: Vec<String> = {
            let mut s = c.prepare(&sql).unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            ids,
            vec!["far".to_string()],
            "the ~22m match drops out, the ~220m one stays"
        );
    }
}
