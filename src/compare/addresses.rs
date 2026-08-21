use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::compare::in_transaction;
use crate::compare::rule::{
    MATCH_DISTANCE_METERS, NAME_MATCH_DISTANCE_METERS, normalized_housenumber_sql,
    normalized_name_sql,
};
use crate::mappings::street_names::{resolved_street_expr_sql, resolved_street_join_sql};
use crate::utils::format_duration;

/// Grid cell size in degrees for the spatial grid-key matching strategy.
/// 0.005° ≈ 343 m east-west at 52 °N (≈ 320 m at Poland's northern edge) and
/// ≈ 556 m north-south — both above the *widest* match distance any rule uses,
/// `NAME_MATCH_DISTANCE_METERS`. Any two addresses within that distance
/// therefore fall in the same or adjacent grid cells, so a ±1 cell
/// neighbourhood is always sufficient.
///
/// **Headroom is now 2.1×, not 6.4×** — widening the name rules to 150 m spent
/// most of what 50 m left over. `grid_key_cell_is_wider_than_the_widest_match_distance`
/// computes the requirement rather than trusting this comment, because the
/// failure mode is silent: a match that straddles two cells simply stops being
/// found, with no error.
const GRID_KEY_DEG: f64 = 0.005;

pub fn compare_prg(conn: &Connection) -> Result<()> {
    info!("Comparing PRG addresses against OSM");
    let t = std::time::Instant::now();

    compare_addresses(conn).context("Failed to compare PRG addresses against OSM")?;

    let total: i64 = conn.query_row("SELECT COUNT(*) FROM prg_addresses", [], |row| row.get(0))?;
    let candidates: i64 =
        conn.query_row("SELECT COUNT(*) FROM prg_unmatched", [], |row| row.get(0))?;

    info!(
        total,
        candidates,
        matched = total - candidates,
        elapsed = %format_duration(t.elapsed()),
        "PRG comparison complete"
    );

    Ok(())
}

/// Compare addresses in a single parallel pass using a spatial grid-key strategy.
///
/// The naive grid-chunked approach suffers from an O(n²) fan-out: common short
/// housenumbers (1, 2, 3 …) appear thousands of times within a 0.5° cell, producing
/// hundreds of millions of distance calculations per cell. The Warsaw cell alone had
/// ~591 million intermediate pairs, taking ~52 s; the full dataset of 264 cells
/// took several hours.
///
/// This function avoids that by assigning each address an integer (gx, gy) key
/// derived from `floor(coord / GRID_KEY_DEG)`. Each source address is expanded to
/// its 3×3 neighbourhood (9 rows) and equality-joined against OSM on
/// (normalised_housenumber, gx, gy). Because GRID_KEY_DEG ≈ 320 m at Poland's
/// northern edge >> the widest match distance, any genuine match is guaranteed
/// to land in the same or an adjacent cell — no valid match is missed. DuckDB
/// parallelises the hash join across all available threads in a single pass; no
/// Rust-level loop is needed.
///
/// **This is the one place that legitimately restates `compare::rule`'s address
/// predicate** rather than calling it — the iteration strategy genuinely
/// differs, and `full_and_per_cell_paths_agree` is what pins the two texts to
/// the same answer. What it does *not* restate is the distance constants, the
/// name normalization, the housenumber normalization
/// (`rule::normalized_housenumber_sql` — comparison-only; the row this INSERT
/// writes still carries `s.numer_porzadkowy`'s original, merely-trimmed
/// value), or the street-mapping resolution chain: those are imported.
///
/// **The name rules are extra `OR` branches on the existing join, not extra
/// UNION-ed branches keyed on the street name.** The equi-key stays
/// `(_hn, _gx, _gy)`, so the join emits exactly the pair set it emitted before
/// the name rules existed — the fan-out above is bit-for-bit unchanged and the
/// O(n²) analysis stands verbatim. A keyed-branch variant would be strictly
/// more work for an identical answer, since every pair the name rules can match
/// is already a pair this join produces (they only relax the *distance*, never
/// the key).
///
/// No index concerns apply here, unlike the per-cell path in
/// `compare::incremental`: this is a designed full scan over both tables, so
/// there is no RTREE window to lose and no candidate CTE to preserve one.
///
/// Writes only unmatched rows into `prg_unmatched`, tagged with the z14 cell of
/// their point and `computed_at`. This clears the table then inserts, and the
/// pair runs in one transaction so a failed insert leaves the previous
/// comparison in place instead of an empty serving table — see
/// `compare::in_transaction`. (`compare` runs offline, with no concurrent
/// readers to isolate from; the transaction is here for atomicity on failure,
/// not for concurrency.)
fn compare_addresses(conn: &Connection) -> Result<()> {
    in_transaction(conn, "prg", || compare_addresses_in_txn(conn))
}

fn compare_addresses_in_txn(conn: &Connection) -> Result<()> {
    conn.execute_batch("DELETE FROM prg_unmatched;")?;
    let cx = crate::tile_math::cell_x_sql("s.geom");
    let cy = crate::tile_math::cell_y_sql("s.geom");
    // The name rules resolve PRG's street through `street_name_mappings`, the
    // same chain `compare::rule` and `/package` use. The joins go in
    // `src_norm`, *before* the 9× CROSS JOIN: two probes against a ~3k-row
    // build side for each of 8.6M addresses, not for each of 77.5M expanded
    // tuples.
    let src_street = normalized_name_sql(&resolved_street_expr_sql("a"));
    let src_place = normalized_name_sql("a.miejscowosc");
    let mapping_joins = resolved_street_join_sql("a");
    let osm_street = normalized_name_sql("street");
    let osm_city = normalized_name_sql("city");
    let src_hn = normalized_housenumber_sql("a.numer_porzadkowy");
    let osm_hn = normalized_housenumber_sql("housenumber");
    // The user-report veto, spliced in from `compare::rule` rather than
    // restated. This query legitimately restates the *match* rule for
    // performance (see the module doc), but the veto is a clause, not a
    // strategy -- calling the shared builder keeps its text single-homed even
    // though everything around it differs from the per-cell path. Correlates
    // on `s`, the final projection's alias over `prg_addresses`.
    let reported = crate::compare::rule::reported_sql(&crate::dataset::PRG, "s");
    conn.execute_batch(&format!(
        "INSERT INTO prg_unmatched
         (geom, lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
          teryt_miejscowosc, wazny_od_lub_data_nadania, teryt_gmina, gmina, cell_x, cell_y, computed_at)
         WITH
         neighbor_offsets(dx, dy) AS (
             VALUES (-1,-1),(-1,0),(-1,1),(0,-1),(0,0),(0,1),(1,-1),(1,0),(1,1)
         ),
         src_norm AS (
             SELECT a.lokalny_id,
                    {src_hn} AS _hn,
                    FLOOR(ST_X(a.geom) / {GRID_KEY_DEG})::BIGINT AS _gx,
                    FLOOR(ST_Y(a.geom) / {GRID_KEY_DEG})::BIGINT AS _gy,
                    {src_street} AS _street,
                    {src_place} AS _place,
                    a.geom
             FROM prg_addresses a
             {mapping_joins}
         ),
         osm_norm AS (
             SELECT {osm_hn} AS _hn,
                    FLOOR(ST_X(geom) / {GRID_KEY_DEG})::BIGINT AS _gx,
                    FLOOR(ST_Y(geom) / {GRID_KEY_DEG})::BIGINT AS _gy,
                    {osm_street} AS _street,
                    {osm_city} AS _city,
                    geom
             FROM osm_addresses
         ),
         src_expanded AS (
             SELECT s.lokalny_id, s._hn, s.geom, s._street, s._place,
                    s._gx + o.dx AS _sgx, s._gy + o.dy AS _sgy
             FROM src_norm s CROSS JOIN neighbor_offsets o
         ),
         matched_ids AS (
             SELECT DISTINCT s.lokalny_id
             FROM src_expanded s
             JOIN osm_norm o
               ON  s._hn = o._hn AND s._sgx = o._gx AND s._sgy = o._gy
               AND (
                        ST_Distance_Sphere(o.geom, s.geom) <= {MATCH_DISTANCE_METERS}
                     OR (
                             ST_Distance_Sphere(o.geom, s.geom) <= {NAME_MATCH_DISTANCE_METERS}
                         AND (
                                  s._street = o._street
                               OR (
                                      s._street IS NULL
                                  AND o._street IS NULL
                                  AND s._place = o._city
                                  )
                             )
                         )
                   )
         )
         SELECT s.geom, s.lokalny_id, s.numer_porzadkowy, s.ulica, s.miejscowosc,
                s.kod_pocztowy, s.teryt_miejscowosc, s.wazny_od_lub_data_nadania,
                s.teryt_gmina, s.gmina, {cx}, {cy}, now()
         FROM prg_addresses s
         WHERE NOT EXISTS (SELECT 1 FROM matched_ids m WHERE m.lokalny_id = s.lokalny_id)
           AND NOT {reported};"
    ))
    .context("Failed to run address comparison query")?;

    // Inside the same transaction as the rows it counts, so a cell's numerator
    // and denominator always come from one comparison.
    crate::compare::totals::rebuild_all_in_txn(conn, "prg")
        .context("Failed to rebuild cell totals for prg")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::db::init_db;

    fn setup() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR,
                 numer_porzadkowy VARCHAR,
                 ulica VARCHAR,
                 miejscowosc VARCHAR,
                 kod_pocztowy VARCHAR,
                 teryt_miejscowosc VARCHAR,
                 wazny_od_lub_data_nadania DATE,
                 teryt_gmina VARCHAR,
                 gmina VARCHAR,
                 geom GEOMETRY
             );",
        )
        .unwrap();
        conn
    }

    /// Insert a `prg_addresses` row with only the id/housenumber/geom populated
    /// — the other serving columns are irrelevant to the matching logic.
    fn insert_prg(conn: &Connection, id: &str, hn: &str, point_sql: &str) {
        conn.execute_batch(&format!(
            "INSERT INTO prg_addresses VALUES ('{id}', '{hn}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, {point_sql});"
        ))
        .unwrap();
    }

    /// Like [`insert_prg`], but populating the two columns the name rules
    /// read: `ulica` (resolved through `street_name_mappings`) and
    /// `miejscowosc`.
    fn insert_prg_named(
        conn: &Connection,
        id: &str,
        hn: &str,
        ulica: Option<&str>,
        miejscowosc: Option<&str>,
        point_sql: &str,
    ) {
        let lit = |v: Option<&str>| match v {
            Some(s) => format!("'{}'", s.replace('\'', "''")),
            None => "NULL".to_string(),
        };
        conn.execute_batch(&format!(
            "INSERT INTO prg_addresses VALUES ('{id}', '{hn}', {}, {}, NULL, NULL, NULL, NULL, NULL, {point_sql});",
            lit(ulica),
            lit(miejscowosc),
        ))
        .unwrap();
    }

    fn unmatched_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM prg_unmatched", [], |row| row.get(0))
            .unwrap()
    }

    fn unmatched_ids(conn: &Connection) -> Vec<String> {
        let mut s = conn
            .prepare("SELECT lokalny_id FROM prg_unmatched ORDER BY lokalny_id")
            .unwrap();
        s.query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    fn run(conn: &Connection) {
        compare_addresses(conn).unwrap();
    }

    /// Same housenumber, ~22 m apart → match → excluded from prg_unmatched.
    #[test]
    fn matched_within_50m_excluded_from_unmatched() {
        let conn = setup();
        insert_prg(&conn, "p1", "12", "ST_Point(21.01, 52.21)");
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES (1, 'node', '12', NULL, NULL, NULL, ST_Point(21.01, 52.2102));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(unmatched_count(&conn), 0);
        assert!(
            unmatched_ids(&conn).is_empty(),
            "the matched address must be absent from prg_unmatched"
        );
    }

    /// Same housenumber, ~200 m apart → too far → 1 unmatched row.
    #[test]
    fn same_number_but_too_far_is_unmatched() {
        let conn = setup();
        insert_prg(&conn, "p1", "12", "ST_Point(21.01, 52.21)");
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES (1, 'node', '12', NULL, NULL, NULL, ST_Point(21.01, 52.212));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(unmatched_count(&conn), 1);
        assert_eq!(unmatched_ids(&conn), vec!["p1".to_string()]);
    }

    /// Different housenumbers at the same point → no match → 1 unmatched row.
    #[test]
    fn different_numbers_within_50m_is_unmatched() {
        let conn = setup();
        insert_prg(&conn, "p1", "12", "ST_Point(21.01, 52.21)");
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES (1, 'node', '14', NULL, NULL, NULL, ST_Point(21.01, 52.21));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(unmatched_count(&conn), 1);
    }

    /// TRIM + UPPER normalization: ' 12a ' vs '12A' → match.
    #[test]
    fn trim_and_upper_normalization() {
        let conn = setup();
        insert_prg(&conn, "p1", " 12a ", "ST_Point(21.01, 52.21)");
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES (1, 'node', '12A', NULL, NULL, NULL, ST_Point(21.01, 52.21));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(unmatched_count(&conn), 0);
    }

    /// The grid-key path's own copy of the motivating record for
    /// `normalized_housenumber_sql` (see the twin test in
    /// `compare::rule::tests`): PRG "45-47" must match OSM "45/47". This
    /// path restates the predicate in different SQL, so it needs its own
    /// pin rather than trusting the per-cell rule's test alone.
    #[test]
    fn housenumber_dash_folds_to_slash_for_matching() {
        let conn = setup();
        insert_prg(&conn, "p1", "45-47", "ST_Point(21.01, 52.21)");
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES (1, 'node', '45/47', NULL, NULL, NULL, ST_Point(21.01, 52.21));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(
            unmatched_count(&conn),
            0,
            "PRG '45-47' must match OSM '45/47'"
        );
    }

    /// "12 A" and "12A" must match on the grid-key path too.
    #[test]
    fn housenumber_space_before_letter_suffix_collapses_for_matching() {
        let conn = setup();
        insert_prg(&conn, "p1", "12 A", "ST_Point(21.01, 52.21)");
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES (1, 'node', '12A', NULL, NULL, NULL, ST_Point(21.01, 52.21));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(unmatched_count(&conn), 0, "PRG '12 A' must match OSM '12A'");
    }

    /// NULL housenumbers on both sides → SQL NULL ≠ NULL in joins → no match → unmatched.
    #[test]
    fn null_housenumbers_dont_match() {
        let conn = setup();
        conn.execute_batch(
            "INSERT INTO prg_addresses VALUES ('p1', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ST_Point(21.01, 52.21));
             INSERT INTO osm_addresses VALUES (1, 'node', NULL, NULL, NULL, NULL, ST_Point(21.01, 52.21));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(unmatched_count(&conn), 1);
    }

    /// PRG and OSM points straddle a grid-cell boundary but are within 50 m.
    /// PRG at lon=14.4997 → gx=floor(14.4997/0.005)=2899
    /// OSM at lon=14.5003 → gx=floor(14.5003/0.005)=2900  (adjacent cell)
    /// Separation ≈ 41 m — within the 50 m threshold → should match → 0 unmatched.
    #[test]
    fn adjacent_grid_cells_within_50m_match() {
        let conn = setup();
        insert_prg(&conn, "p1", "5", "ST_Point(14.4997, 52.25)");
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES (1, 'node', '5', NULL, NULL, NULL, ST_Point(14.5003, 52.25));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(
            unmatched_count(&conn),
            0,
            "addresses in adjacent grid cells within 50 m should match"
        );
    }

    /// An unmatched source address produces exactly one row, tagged with its
    /// z14 cell. The single-pass approach never duplicates rows, even when an
    /// address falls exactly on a grid-cell boundary (unlike the old chunked
    /// approach which emitted boundary rows twice).
    #[test]
    fn unmatched_address_produces_exactly_one_row_with_cell_tags() {
        let conn = setup();
        // lon=14.5 lands exactly on a grid boundary (14.5/0.005=2900.0).
        insert_prg(&conn, "boundary", "99", "ST_Point(14.5, 52.25)");

        run(&conn);
        assert_eq!(
            unmatched_count(&conn),
            1,
            "each source address should appear at most once in prg_unmatched"
        );
        let (cx, cy): (i32, i32) = conn
            .query_row("SELECT cell_x, cell_y FROM prg_unmatched", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        let (ex, ey) =
            crate::tile_math::lonlat_to_tile(14.5, 52.25, crate::tile_math::CHANGE_CELL_ZOOM);
        assert_eq!((cx as u32, cy as u32), (ex, ey));
    }

    /// Running the comparison twice must not duplicate rows in prg_unmatched.
    #[test]
    fn compare_addresses_is_idempotent() {
        let conn = setup();
        insert_prg(&conn, "p1", "12", "ST_Point(21.01, 52.21)");

        run(&conn);
        run(&conn);
        assert_eq!(
            unmatched_count(&conn),
            1,
            "re-running must not duplicate rows"
        );
    }

    /// Rule B in the grid-key path: ~133 m apart, same housenumber, agreeing
    /// street → matched. The twin with a differing street stays unmatched, so
    /// the test can't pass by the distance branch alone.
    #[test]
    fn matched_at_133m_via_street_excluded_from_unmatched() {
        let conn = setup();
        insert_prg_named(
            &conn,
            "same",
            "44",
            Some("Warszawska"),
            None,
            "ST_Point(21.01, 52.2112)",
        );
        insert_prg_named(
            &conn,
            "other",
            "44",
            Some("Polna"),
            None,
            "ST_Point(21.02, 52.2112)",
        );
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES
                 (1, 'node', '44', 'Warszawska', NULL, NULL, ST_Point(21.01, 52.21)),
                 (2, 'node', '44', 'Warszawska', NULL, NULL, ST_Point(21.02, 52.21));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(unmatched_ids(&conn), vec!["other".to_string()]);
    }

    /// Rule B reads the *mapped* street name. Raw equality is not enough once a
    /// mapping rewrites PRG's side — the same property `compare::rule`'s
    /// `address_matches_on_the_mapped_name_not_the_raw_name` pins for the
    /// per-cell path.
    #[test]
    fn grid_key_path_matches_on_the_mapped_street_name() {
        let conn = setup();
        conn.execute_batch(
            "INSERT INTO street_name_mappings VALUES (NULL, 'gen. Kruka', 'Generała Kruka');",
        )
        .unwrap();
        insert_prg_named(
            &conn,
            "raw-equal",
            "5",
            Some("gen. Kruka"),
            None,
            "ST_Point(21.01, 52.2112)",
        );
        insert_prg_named(
            &conn,
            "mapped",
            "6",
            Some("gen. Kruka"),
            None,
            "ST_Point(21.02, 52.2112)",
        );
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES
                 (1, 'node', '5', 'gen. Kruka', NULL, NULL, ST_Point(21.01, 52.21)),
                 (2, 'node', '6', 'Generała Kruka', NULL, NULL, ST_Point(21.02, 52.21));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(unmatched_ids(&conn), vec!["raw-equal".to_string()]);
    }

    /// Rule C in the grid-key path: streetless on both sides, agreeing
    /// locality. The `osm_addresses.city` column is `COALESCE(addr:city,
    /// addr:place)` at every insert site, which is what makes a Polish
    /// place-address reachable here at all.
    #[test]
    fn matched_at_133m_via_locality_excluded_from_unmatched() {
        let conn = setup();
        insert_prg_named(
            &conn,
            "same-place",
            "7",
            None,
            Some("Rychnowo"),
            "ST_Point(21.01, 52.2112)",
        );
        insert_prg_named(
            &conn,
            "other-place",
            "7",
            None,
            Some("Inne"),
            "ST_Point(21.02, 52.2112)",
        );
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES
                 (1, 'node', '7', NULL, 'Rychnowo', NULL, ST_Point(21.01, 52.21)),
                 (2, 'node', '7', NULL, 'Rychnowo', NULL, ST_Point(21.02, 52.21));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(unmatched_ids(&conn), vec!["other-place".to_string()]);
    }

    /// The ±1 neighbourhood must still cover the *widened* distance. PRG at
    /// lon=14.4991 → gx=2899, OSM at lon=14.5009 → gx=2900 (adjacent), ~123 m
    /// apart — beyond rule A, inside the name rules. Under a grid key too
    /// narrow for 150 m this pair would silently never be compared.
    #[test]
    fn adjacent_grid_cells_within_150m_match() {
        let conn = setup();
        insert_prg_named(
            &conn,
            "p1",
            "5",
            Some("Warszawska"),
            None,
            "ST_Point(14.4991, 52.25)",
        );
        conn.execute_batch(
            "INSERT INTO osm_addresses VALUES
                 (1, 'node', '5', 'Warszawska', NULL, NULL, ST_Point(14.5009, 52.25));",
        )
        .unwrap();

        run(&conn);
        assert_eq!(
            unmatched_count(&conn),
            0,
            "a name match in an adjacent grid cell must still be found"
        );
    }

    /// `GRID_KEY_DEG`'s headroom over the widest match distance dropped from
    /// 6.4× to 2.1× when the name rules landed. Compute the requirement rather
    /// than trusting the constant's doc comment: a grid cell narrower than the
    /// match distance would make a straddling pair vanish from the ±1
    /// neighbourhood, with no error and no failing assertion anywhere else.
    #[test]
    fn grid_key_cell_is_wider_than_the_widest_match_distance() {
        // Poland's northern edge, where a degree of longitude is shortest and
        // a grid cell therefore spans the fewest metres.
        let m_per_deg_lon = 111_320.0 * 54.84_f64.to_radians().cos();
        let cell_width_m = GRID_KEY_DEG * m_per_deg_lon;
        assert!(
            cell_width_m > NAME_MATCH_DISTANCE_METERS,
            "GRID_KEY_DEG spans {cell_width_m} m at 54.84N, which must exceed \
             NAME_MATCH_DISTANCE_METERS ({NAME_MATCH_DISTANCE_METERS} m) for the \
             +/-1 cell neighbourhood to be sufficient"
        );
    }

    /// The full grid-key path and the per-cell rule must agree on the unmatched
    /// set. Seed a spread of addresses (some matched, some not, some near cell
    /// edges) and compare the two id sets.
    ///
    /// **The single most valuable test in this module.** The two paths express
    /// the same three rules in structurally different SQL — an `OR`-ed join
    /// condition over a 3×3 grid-key expansion here, a correlated `NOT EXISTS`
    /// over a resolved-CTE chain in `compare::rule` — so nothing but this
    /// keeps them answering the same question. The fixture deliberately
    /// exercises every branch *and its negative*, including a non-empty
    /// `street_name_mappings`, since a rule that never fires in the fixture is
    /// a rule this test does not pin.
    #[test]
    fn full_and_per_cell_paths_agree() {
        use crate::compare::rule::{OSM_MATCH_BUFFER_DEG, buffer, unmatched_addresses_in_cell_sql};
        use crate::tile_math::{CHANGE_CELL_ZOOM, lonlat_to_tile, tile_to_bbox};
        use std::collections::BTreeSet;

        let conn = setup(); // creates prg_addresses + osm_addresses via init_db
        conn.execute_batch(
            "INSERT INTO street_name_mappings VALUES
                (NULL,      'gen. Kruka', 'Generała Kruka'),
                ('0956069', 'gen. Kruka', 'Generała Michała Kruka');
             INSERT INTO prg_addresses
                 (lokalny_id, numer_porzadkowy, ulica, miejscowosc, teryt_miejscowosc, geom) VALUES
                -- rule A and its negative
                ('a','12', NULL, NULL, NULL, ST_Point(21.010, 52.210)),   -- matched (osm ~22m)
                ('b','12', NULL, NULL, NULL, ST_Point(21.010, 52.212)),   -- too far -> unmatched
                ('c','7',  NULL, NULL, NULL, ST_Point(21.050, 52.250)),   -- no osm -> unmatched
                ('d','9',  NULL, NULL, NULL, ST_Point(21.0001, 52.2001)), -- near a cell edge
                -- rule B: agreeing street at ~133m
                ('e','44', 'Warszawska', NULL, NULL, ST_Point(21.020, 52.2112)),
                -- rule B negative: differing street at ~133m
                ('f','44', 'Polna', NULL, NULL, ST_Point(21.030, 52.2112)),
                -- rule B through a mapping: raw-equal but mapped away -> unmatched
                ('g','5', 'gen. Kruka', NULL, NULL, ST_Point(21.040, 52.2112)),
                -- rule B through the settlement-scoped mapping -> matched
                ('h','6', 'gen. Kruka', NULL, '0956069', ST_Point(21.060, 52.2112)),
                -- rule C: streetless, agreeing locality at ~133m
                ('i','7', NULL, 'Rychnowo', NULL, ST_Point(21.070, 52.2112)),
                -- rule C negative: OSM carries a street, so the gate closes
                ('j','7', NULL, 'Rychnowo', NULL, ST_Point(21.080, 52.2112)),
                -- empty-string street must behave as absent, routing through rule C
                ('k','8', '', 'Rychnowo', NULL, ST_Point(21.090, 52.2112)),
                -- beyond every distance, despite an agreeing street
                ('l','44', 'Warszawska', NULL, NULL, ST_Point(21.100, 52.2116)),
                -- user-report veto and its negative: 'm' and 'n' are identical
                -- in every respect the match rule looks at (both unmatched on
                -- distance), so the ONLY thing separating them is the report on
                -- 'm'. A path that dropped the veto would put 'm' back in the
                -- expected set below and fail.
                ('m','21', NULL, NULL, NULL, ST_Point(21.110, 52.250)),
                ('n','21', NULL, NULL, NULL, ST_Point(21.120, 52.250));
             -- Reported directly rather than through `reports::insert` so the
             -- fixture states the stored row plainly; the signature is not read
             -- by the veto (only `reports::reconcile_source` reads it).
             INSERT INTO object_reports
                 (report_id, source, record_key, signature, reason, note,
                  reported_at, cell_x, cell_y, status, resolved_at)
             VALUES (1, 'prg', ['m'], 'sig', 'does_not_exist', NULL,
                     now(), NULL, NULL, 'active', NULL);
             INSERT INTO osm_addresses VALUES
                (1,'node','12',NULL,NULL,NULL, ST_Point(21.010, 52.2102)),
                (2,'node','44','Warszawska',NULL,NULL, ST_Point(21.020, 52.210)),
                (3,'node','44','Warszawska',NULL,NULL, ST_Point(21.030, 52.210)),
                (4,'node','5','gen. Kruka',NULL,NULL, ST_Point(21.040, 52.210)),
                (5,'node','6','Generała Michała Kruka',NULL,NULL, ST_Point(21.060, 52.210)),
                (6,'node','7',NULL,'Rychnowo',NULL, ST_Point(21.070, 52.210)),
                (7,'node','7','Polna','Rychnowo',NULL, ST_Point(21.080, 52.210)),
                (8,'node','8','',   'Rychnowo',NULL, ST_Point(21.090, 52.210)),
                (9,'node','44','Warszawska',NULL,NULL, ST_Point(21.100, 52.210));",
        )
        .unwrap();

        // Full path.
        compare_prg(&conn).unwrap();
        let full: BTreeSet<String> = {
            let mut s = conn
                .prepare("SELECT lokalny_id FROM prg_unmatched")
                .unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        // Pin the expected set outright, not just "the two agree": two paths
        // that both dropped the name rules would agree perfectly and pass.
        assert_eq!(
            full,
            ["b", "c", "d", "f", "g", "j", "l", "n"]
                .iter()
                .map(|s| s.to_string())
                .collect::<BTreeSet<_>>(),
            "every rule and its negative must land where the fixture says"
        );

        // Per-cell path over the distinct cells the addresses fall in.
        let mut cells = BTreeSet::new();
        for (lon, lat) in [
            (21.010, 52.210),
            (21.010, 52.212),
            (21.050, 52.250),
            (21.0001, 52.2001),
            (21.020, 52.2112),
            (21.030, 52.2112),
            (21.040, 52.2112),
            (21.060, 52.2112),
            (21.070, 52.2112),
            (21.080, 52.2112),
            (21.090, 52.2112),
            (21.100, 52.2116),
            // 'm' (reported) and 'n' (its unreported twin)
            (21.110, 52.250),
            (21.120, 52.250),
        ] {
            cells.insert(lonlat_to_tile(lon, lat, CHANGE_CELL_ZOOM));
        }
        let mut per_cell = BTreeSet::new();
        for (cx, cy) in cells {
            let w = tile_to_bbox(CHANGE_CELL_ZOOM, cx, cy);
            let sql = unmatched_addresses_in_cell_sql(
                &crate::dataset::PRG,
                "prg_addresses",
                "a.lokalny_id",
                w,
                buffer(w, OSM_MATCH_BUFFER_DEG),
            );
            let mut s = conn.prepare(&sql).unwrap();
            for id in s.query_map([], |r| r.get::<_, String>(0)).unwrap() {
                per_cell.insert(id.unwrap());
            }
        }

        assert_eq!(full, per_cell, "full grid-key and per-cell rule disagree");
    }
}
