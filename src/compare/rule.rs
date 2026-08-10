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

/// Unmatched building rows: government centroid within `area`, and no
/// osm_buildings polygon whose footprint covers at least
/// `MIN_OVERLAP_FRACTION` of the government building's own footprint (osm
/// filtered to `area` for the R-tree scan).
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
/// stays index-accelerated on both `b.geom` and `osm.geom`.
///
/// `extra_filter`, when set, is ANDed into the WHERE clause alongside the
/// `b`-aliased source row (see `BDOT10K_EKSPLOATOWANY_FILTER`).
pub fn unmatched_buildings_sql(
    source_table: &str,
    select_list: &str,
    area: Bounds,
    extra_filter: Option<&str>,
) -> String {
    let (x1, y1, x2, y2) = area;
    let extra = extra_filter
        .map(|f| format!("AND {f}\n           "))
        .unwrap_or_default();
    format!(
        "SELECT {select_list}
         FROM {source_table} b
         WHERE ST_Intersects(b.centroid, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
           {extra}AND NOT EXISTS (
               SELECT 1 FROM osm_buildings osm
               WHERE ST_Intersects(osm.geom, ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}))
                 AND ST_Intersects(osm.geom, b.geom)
                 AND ST_Area(ST_Intersection(osm.geom, b.geom)) / ST_Area(b.geom) >= {MIN_OVERLAP_FRACTION}
           )"
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
    let (wx1, wy1, wx2, wy2) = write;
    let (rx1, ry1, rx2, ry2) = read;
    let dist = MATCH_DISTANCE_METERS;
    format!(
        "SELECT {select_list}
         FROM {source_table} a
         WHERE ST_Intersects(a.geom, ST_MakeEnvelope({wx1}, {wy1}, {wx2}, {wy2}))
           AND NOT EXISTS (
               SELECT 1 FROM osm_addresses o
               WHERE ST_Intersects(o.geom, ST_MakeEnvelope({rx1}, {ry1}, {rx2}, {ry2}))
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
