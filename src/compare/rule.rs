//! The single home for "which government object is unmatched against OSM".
//!
//! Full building `compare` and the per-cell incremental recompute both call
//! `unmatched_buildings_sql` here, so they share the predicate text itself.
//! Full address `compare` uses its own grid-key SQL for performance and so
//! restates the predicate rather than calling it; what it shares are the two
//! distance constants, `normalized_name_sql`, and the street-resolution
//! builders in `mappings::street_names`. Two tests pin the paths together —
//! `addresses::full_and_per_cell_paths_agree` (grid-key vs. per-cell rule) and
//! `compare::full_vs_incremental_equivalence` (full `compare` vs.
//! reconcile+drain, end to end).

use crate::dataset::DatasetSpec;
use crate::mappings::street_names::{resolved_street_expr_sql, resolved_street_join_sql};

/// (min_lon, min_lat, max_lon, max_lat).
pub type Bounds = (f64, f64, f64, f64);

/// `object_reports.status` for a report that is currently in force. The other
/// two values (`expired`, `revoked`) are inert — see `reports::status`, which
/// owns the vocabulary; this constant exists so the veto below and the reports
/// module cannot disagree about which string means "applies".
pub const REPORT_ACTIVE: &str = "active";

/// Distance within which a bare housenumber agreement is enough to call an
/// address matched — proximity alone, no name evidence.
pub const MATCH_DISTANCE_METERS: f64 = 50.0;

/// Distance within which a housenumber agreement *plus* a name agreement
/// counts as matched. Covers both name rules: an agreeing street name, and —
/// for streetless addresses only — an agreeing locality.
///
/// The looser distance exists because PRG publishes a point on the parcel
/// while OSM usually puts the node on the building, and on deep plots and
/// corner lots those legitimately sit further apart than
/// `MATCH_DISTANCE_METERS`. The motivating record, PRG
/// `7077839d-e180-4030-a679-f968741386f6` (Zakroczym, ul. Warszawska 44), is
/// 51.8 m from the OSM way carrying the identical street and housenumber.
///
/// Widening `MATCH_DISTANCE_METERS` itself to 150 m instead would be wrong:
/// measured nationally, housenumber-only at 150 m matches 56,205 more
/// addresses, while requiring a name agreement matches 43,256 — the name test
/// rejects 35,225 pairs that agree on nothing but a house number and a
/// neighbourhood.
pub const NAME_MATCH_DISTANCE_METERS: f64 = 150.0;

/// OSM read buffer around a cell for address matching. Matches /package.
///
/// **Coupled to `NAME_MATCH_DISTANCE_METERS`** — the *widest* distance any
/// branch of the rule uses, not the narrowest. 0.003° is ~192 m of longitude
/// at Poland's northern edge (54.8 °N, where 1° of longitude is ~64.1 km) and
/// ~333 m of latitude, so it covers the 150 m name-match distance with 1.28×
/// headroom east-west — the same headroom 0.001° gave the old 50 m rule.
/// Raising a match distance past what this covers would silently break
/// read-wide/write-narrow — an OSM address just outside the buffered read
/// would stop matching — with no test failure, which is why
/// `osm_match_buffer_covers_the_widest_match_distance` computes the
/// requirement rather than trusting this comment.
///
/// `update::dirty_cells::layer_buffer_deg` imports this constant for
/// `Layer::Addresses` rather than restating the number, so the OSM producer's
/// enqueue reach grows with it automatically.
pub const OSM_MATCH_BUFFER_DEG: f64 = 0.003;

/// Canonical form of a name for comparison: case- and whitespace-insensitive,
/// with the empty string collapsed to NULL.
///
/// The `NULLIF(..., '')` is load-bearing, not hygiene. Without it a PRG
/// `ulica = ''` and an OSM `addr:street=` satisfy the street rule's
/// `_street = _street` at 150 m, matching two *streetless* addresses on two
/// empty strings — precisely the case the locality rule exists to gate behind
/// a locality agreement.
pub fn normalized_name_sql(expr: &str) -> String {
    format!("NULLIF(lower(trim({expr})), '')")
}

/// Canonical form of a housenumber for *matching* — never for display or
/// export, which keep the original (merely trimmed) value carried in
/// `prg_unmatched`/OSM tags untouched.
///
/// Case-folds (DuckDB's `UPPER`/`TRIM` are backed by utf8proc, which
/// case-folds Polish diacritics correctly — Polish, unlike Turkish, has no
/// casing rule that needs a locale-aware collation, so no ICU dependency is
/// needed here), trims, folds `-` and `\` to `/` (PRG spells a double
/// housenumber "45-47", OSM spells the identical one "45/47" — see PRG
/// `9d0f1c57-797c-4035-96ef-11ab4100197f`, 2.4 m from OSM node 4365400981,
/// both "Przyrodnicza" in Zgierz), and collapses a 1–2 digit number
/// separated from a single letter suffix by a space onto it ("12 A" ->
/// "12A"). Measured nationally: applying this in place of bare
/// `UPPER(TRIM(...))` picks up 877 additional proximity matches among
/// addresses `prg_unmatched` already carried, with no macro-scale change in
/// match volume.
///
/// The `-`/`\` -> `/` fold is deliberately generic rather than scoped to
/// "double housenumber" — a bare hyphen or backslash never has a different
/// meaning in a Polish housenumber. It does *not* attempt to canonicalize
/// "oficyna"/"of."/"blok"/"bl" annex markers — those were measured
/// separately and found too inconsistent between PRG and OSM (bare number,
/// "-of", or doubled "-ofof" all appear for the same annex concept) to
/// canonicalize without a design decision on which OSM shape is authoritative.
pub fn normalized_housenumber_sql(expr: &str) -> String {
    format!(
        "regexp_replace(
             UPPER(TRIM(replace(replace({expr}, '\\', '/'), '-', '/'))),
             '^(\\d{{1,2}}) ([A-Za-z])$', '\\1\\2'
         )"
    )
}

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

/// The user-report veto: true when `alias` names a government record someone
/// has reported as one that should not be proposed for import.
///
/// Same one-home reasoning as the two clause builders above, and the same
/// negate-here/require-there pairing: `unmatched_buildings_sql` and
/// `unmatched_addresses_in_cell_sql` negate it, `reported_buildings_sql`
/// requires it, and `compare::addresses::compare_addresses_in_txn` — which
/// legitimately restates the surrounding address query for performance —
/// splices in this same text rather than writing its own. That is the only
/// reason this one is `pub` where the former-building pair is private.
///
/// **Phrased `EXISTS`, never a `LEFT JOIN`, and that is load-bearing.**
/// `object_reports` carries no UNIQUE constraint, so an object can legitimately
/// carry several reports (two users, two reasons). Under `EXISTS` that is
/// simply "reported"; under a join it would emit one `<source>_unmatched` row
/// per report — the exact fan-out that `street_name_mappings` has to prevent
/// with a duplicate-key check at load. Here it costs nothing to be immune
/// instead. `rule::tests::two_reports_on_one_object_still_suppress_exactly_one_row`
/// is the guard.
///
/// Unlike the geometric vetoes above this needs no envelope: it correlates on
/// the record key, which is already narrowed by whatever scoping filter the
/// surrounding query applies to `alias`.
pub fn reported_sql(spec: &DatasetSpec, alias: &str) -> String {
    format!(
        "EXISTS (
               SELECT 1 FROM object_reports r
               WHERE r.status = '{REPORT_ACTIVE}'
                 AND r.source = '{source}'
                 AND r.record_key = {key}
           )",
        source = spec.name,
        key = spec.key_list_sql(alias),
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
///
/// `spec` identifies the source for the user-report veto (`reported_sql`) and
/// is deliberately a separate parameter from `source_table`: the per-cell path
/// passes the string `"candidates"` there, a CTE over the live table, so the
/// spec cannot be recovered from the table name. The CTE is `SELECT *`, so the
/// key columns the veto correlates on are present on the alias either way.
pub fn unmatched_buildings_sql(
    spec: &DatasetSpec,
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
           AND NOT {former_covers}
           AND NOT {reported}",
        osm_covers = osm_building_covers_sql(&envelope),
        former_covers = former_building_covers_sql(&envelope),
        reported = reported_sql(spec, "b"),
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

/// Rows the *user-report* veto removes from `unmatched_buildings_sql`, and
/// nothing else does — the operator-facing count that shows whether reporting
/// is doing anything at all, in the same spirit as `suppressed_buildings_sql`.
///
/// The four categories are disjoint by construction, in this precedence:
/// covered by OSM (`matched`) > covered by a former building (`suppressed`) >
/// reported (`reported`) > `unmatched`. So this requires the report veto *and*
/// negates both geometric ones, exactly mirroring the clause set
/// `unmatched_buildings_sql` uses, which is what keeps
/// `matched + suppressed + reported + unmatched = total` exact. Folding
/// reporting into `matched` instead would make it invisible — an operator
/// checking whether the feature works would have no number to look at.
///
/// **Same CTE-first shape as `suppressed_buildings_sql`, for the same reason.**
/// Its one caller runs it once over the whole national extent, not per grid
/// cell, and the clauses meet wildly different row counts: the report join is
/// savagely selective (a handful of rows out of 16.35M), while
/// `osm_building_covers_sql` anti-joins ~18M `osm_buildings`. Written flat,
/// DuckDB de-correlates both `EXISTS` clauses into nested `DELIM_JOIN`s and can
/// plan the anti-join underneath, computing the unmatched set for all of Poland
/// and materializing the correlated `b.geom` — 2.18 GB of WKB, which is how
/// `suppressed_buildings_sql` OOMed before it was reordered. Build the veto's
/// candidates first and anti-join over those. `MATERIALIZED` is insurance
/// against a future re-plan, not the active ingredient; the CTE is.
pub fn reported_buildings_sql(
    spec: &DatasetSpec,
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
               {extra}AND {reported}
         )
         SELECT {select_list}
         FROM candidates b
         WHERE NOT {osm_covers}
           AND NOT {former_covers}",
        reported = reported_sql(spec, "b"),
        osm_covers = osm_building_covers_sql(&envelope),
        former_covers = former_building_covers_sql(&envelope),
    )
}

/// Unmatched address rows: a government point within `write` for which no
/// `osm_addresses` point (read from `read`) satisfies any of three rules.
/// Every rule requires an equal normalized housenumber — `hn(...)` below is
/// `normalized_housenumber_sql`, matching-only, never the raw stored value —
/// and NULL housenumber never matches, since SQL `= NULL` is never true.
///
/// ```text
/// matched(a) := EXISTS osm o WHERE hn(o) = hn(a) AND (
///        dist <= MATCH_DISTANCE_METERS                            -- A proximity
///     OR ( dist <= NAME_MATCH_DISTANCE_METERS AND (
///              a._street = o._street                              -- B street
///           OR ( a._street IS NULL AND o._street IS NULL
///                AND a._place = o._city ) ) ) )                   -- C locality
/// ```
///
/// Three things about the name rules are deliberate and load-bearing.
///
/// **The comparison is `=`, never `IS NOT DISTINCT FROM`.** `=` is never true
/// for NULL, which is exactly right: two addresses that merely both lack a
/// locality must not match on two NULLs. Rule C's *guard* tests each street for
/// NULL explicitly; its *payload* (`a._place = o._city`) does not, so a
/// streetless address with no locality falls back to rule A alone.
///
/// **Rule B reads the street name resolved through `street_name_mappings`, not
/// the raw PRG `ulica`.** That is why this predicate joins the mapping table at
/// all, and it is what makes the mapping a *match* input rather than the
/// serving-time-only lookup it used to be — see
/// `mappings::street_names::validate_and_swap`, which now enqueues dirty cells
/// for the addresses a mapping edit can flip. Measured nationally, the mapped
/// name matches 20,980 addresses and the raw name 20,243; OR-ing both adds only
/// 154 over mapped-only, which is not worth a third branch.
///
/// **Rule C compares PRG's `miejscowosc` against `osm_addresses.city`, and that
/// column is `COALESCE(addr:city, addr:place)`** at all six of its insert sites
/// (`import::osm`, `update::osm`). Polish place-addresses — the streetless ones
/// rule C is entirely about — carry `addr:place`, so that COALESCE is the whole
/// reason the rule finds anything.
///
/// **Do not run this predicate at full national extent.** The `NOT EXISTS` now
/// correlates on four columns instead of two and DuckDB de-correlates it into a
/// `DELIM_JOIN`; per z14 cell that is hundreds of rows, over Poland it is the
/// `suppressed_buildings_sql` out-of-memory story. The full compare has its own
/// grid-key implementation in `compare::addresses` for this reason.
///
/// The rule owns the whole `WITH` chain because it is *structurally* required:
/// `compare::incremental` used to concatenate its own `WITH candidates AS ...`
/// onto this output, and two `WITH` keywords is a syntax error — one side has
/// to own it, and owning it here means the two bare-table test callers get the
/// same shape production does. `MATERIALIZED` is insurance against the
/// `server::tiles` fold-back (a `LEFT JOIN` downstream of a filtered CTE can be
/// re-planned into a `SEQ_SCAN` plus `FILTER`), **not** a measured index fix:
/// both shapes were `EXPLAIN`ed against the real national tables and both keep
/// `RTREE_INDEX_SCAN` on `prg_addresses.geom` and `osm_addresses.geom`. Don't
/// read a measurement into the keyword that isn't there.
pub fn unmatched_addresses_in_cell_sql(
    spec: &DatasetSpec,
    source_table: &str,
    select_list: &str,
    write: Bounds,
    read: Bounds,
) -> String {
    let write_envelope = envelope_sql(write);
    let read_envelope = envelope_sql(read);
    let dist = MATCH_DISTANCE_METERS;
    let name_dist = NAME_MATCH_DISTANCE_METERS;
    let resolved_street = normalized_name_sql(&resolved_street_expr_sql("c"));
    let place = normalized_name_sql("c.miejscowosc");
    let mapping_joins = resolved_street_join_sql("c");
    let osm_street = normalized_name_sql("o.street");
    let osm_city = normalized_name_sql("o.city");
    let src_hn = normalized_housenumber_sql("a.numer_porzadkowy");
    let osm_hn = normalized_housenumber_sql("o.housenumber");
    // Correlates on `a`, the outer alias (`addr_resolved`, which carries
    // `SELECT c.*` and so still has the key columns). Sits outside the
    // `NOT EXISTS` on `osm_addresses` rather than inside it: a report means
    // "never propose this", which is independent of whether OSM has anything
    // nearby at all.
    let reported = reported_sql(spec, "a");
    // The CTEs are named `addr_*` rather than `candidates`/`resolved` so a
    // future caller wanting its own outer CTE cannot collide with them.
    format!(
        "WITH addr_candidates AS MATERIALIZED (
             SELECT a.*
             FROM {source_table} a
             WHERE ST_Intersects(a.geom, {write_envelope})
         ),
         addr_resolved AS (
             SELECT c.*,
                    {resolved_street} AS _street,
                    {place} AS _place
             FROM addr_candidates c
             {mapping_joins}
         )
         SELECT {select_list}
         FROM addr_resolved a
         WHERE ST_Intersects(a.geom, {write_envelope})
           AND NOT {reported}
           AND NOT EXISTS (
               SELECT 1 FROM osm_addresses o
               WHERE ST_Intersects(o.geom, {read_envelope})
                 AND {osm_hn} = {src_hn}
                 AND (
                      ST_Distance_Sphere(o.geom, a.geom) <= {dist}
                   OR (
                          ST_Distance_Sphere(o.geom, a.geom) <= {name_dist}
                      AND (
                               a._street = {osm_street}
                            OR (
                                   a._street IS NULL
                               AND {osm_street} IS NULL
                               AND a._place = {osm_city}
                               )
                          )
                      )
                 )
           )"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{BDOT10K, PRG};
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
            // `asrc` mirrors the columns of `prg_addresses` that the address
            // rule reads: the housenumber, plus the three the name rules
            // resolve through (`ulica` and `teryt_miejscowosc` select the
            // street mapping, `miejscowosc` is rule C's locality).
            // `bsrc` carries PRZESTRZENNAZW as well as LOKALNYID so these
            // tests can pass the real `dataset::BDOT10K` spec and exercise its
            // genuine *composite* key through the report veto, rather than a
            // single-column stand-in that would hide a list-arity bug. Rows
            // that leave it NULL are fine: a report can only match a row whose
            // whole key list is equal, and no test inserts a NULL-keyed report.
            "CREATE TABLE bsrc (PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR,
                                geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE asrc (lokalny_id VARCHAR, numer_porzadkowy VARCHAR,
                                ulica VARCHAR, miejscowosc VARCHAR,
                                teryt_miejscowosc VARCHAR, geom GEOMETRY);",
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
        let sql = unmatched_buildings_sql(
            &BDOT10K,
            "bsrc",
            "b.LOKALNYID",
            (14.0, 49.0, 25.0, 55.0),
            None,
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

        let sql = unmatched_buildings_sql(
            &BDOT10K,
            "bsrc",
            "b.LOKALNYID",
            (14.0, 49.0, 25.0, 55.0),
            None,
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
        let sql = unmatched_buildings_sql(
            &BDOT10K,
            "bsrc",
            "b.LOKALNYID",
            (14.0, 49.0, 25.0, 55.0),
            None,
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
            vec!["barely_touching".to_string()],
            "a mere corner-clip below MIN_OVERLAP_FRACTION must not count as a match"
        );
    }

    /// The actual regression guard for the per-cell-recompute fix
    /// (docs/per_cell_recompute_full_scan.md): if `unmatched_buildings_sql`
    /// ever goes back to wrapping the indexed column in `ST_Centroid()`, this
    /// fails, because an RTREE index cannot be used through a function
    /// applied to the indexed column.
    ///
    /// Asserts against `EXPLAIN (FORMAT JSON)`, not the default box-drawing
    /// pretty-printer, and that is deliberate. The pretty-printer sizes its
    /// boxes to fit the plan's width and *truncates operator labels* in a wide
    /// multi-branch plan — adding the user-report veto made this plan wide
    /// enough that `RTREE_INDEX_SCAN` rendered as `RTREE_INDE...` and this
    /// assertion failed while the index was still very much in use (verified
    /// against the JSON plan, which showed `RTREE_INDEX_SCAN` on `bsrc` via
    /// `bsrc_centroid_idx`). Elsewhere in this codebase that hazard is worked
    /// around by asserting the substring `"RTREE_IN"`; JSON is the better
    /// answer, because it also lets the *index name* be pinned, so the test
    /// cannot pass on some unrelated table's R-tree scan.
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

        let sql = unmatched_buildings_sql(
            &BDOT10K,
            "bsrc",
            "b.LOKALNYID",
            (20.5, 52.0, 20.6, 52.1),
            None,
        );
        let mut stmt = c.prepare(&format!("EXPLAIN (FORMAT JSON) {sql}")).unwrap();
        let mut rows = stmt.query([]).unwrap();
        let mut plan = String::new();
        while let Some(row) = rows.next().unwrap() {
            plan.push_str(&row.get::<_, String>(1).unwrap_or_default());
        }
        assert!(
            plan.contains("RTREE_INDEX_SCAN"),
            "the predicate must be able to use the centroid RTREE index, got plan: {plan}"
        );
        assert!(
            plan.contains("bsrc_centroid_idx"),
            "the R-tree scan must be the source table's centroid index, not some \
             other table's, got plan: {plan}"
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
        let sql = unmatched_buildings_sql(
            &BDOT10K,
            "bsrc",
            "b.LOKALNYID",
            (14.0, 49.0, 25.0, 55.0),
            None,
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
        let sql = unmatched_buildings_sql(
            &BDOT10K,
            "bsrc",
            "b.LOKALNYID",
            (14.0, 49.0, 25.0, 55.0),
            None,
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

        let sql = unmatched_buildings_sql(
            &BDOT10K,
            "bsrc",
            "b.LOKALNYID",
            (14.0, 49.0, 25.0, 55.0),
            None,
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

        let unmatched_sql = unmatched_buildings_sql(&BDOT10K, "bsrc", "b.LOKALNYID", area, None);
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
            &BDOT10K,
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

    // ---- address rule helpers -------------------------------------------
    //
    // Every address test works in the same z14-ish window, so distances read
    // off the latitude offset directly: ST_Distance_Sphere puts 1e-4 deg of
    // latitude at ~11.1 m, so 0.0002 = ~22 m (rule A), 0.0012 = ~133 m (name
    // rules only) and 0.0016 = ~178 m (beyond every rule).
    const ADDR_AREA: Bounds = (21.0, 52.2, 21.02, 52.22);
    const ADDR_LON: f64 = 21.01;
    const ADDR_LAT: f64 = 52.21;

    fn sql_str(v: Option<&str>) -> String {
        match v {
            Some(s) => format!("'{}'", s.replace('\'', "''")),
            None => "NULL".to_string(),
        }
    }

    /// One PRG-shaped row, `dlat` degrees of latitude north of `ADDR_LAT`.
    fn prg(
        c: &duckdb::Connection,
        id: &str,
        hn: &str,
        ulica: Option<&str>,
        miejscowosc: Option<&str>,
        simc: Option<&str>,
        dlat: f64,
    ) {
        c.execute_batch(&format!(
            "INSERT INTO asrc VALUES ('{id}', '{hn}', {}, {}, {},
                 ST_Point({ADDR_LON}, {}))",
            sql_str(ulica),
            sql_str(miejscowosc),
            sql_str(simc),
            ADDR_LAT + dlat,
        ))
        .unwrap();
    }

    /// One OSM address node at `ADDR_LAT` exactly, so a PRG row's `dlat` *is*
    /// its distance from every OSM node in the fixture.
    fn osm_addr(
        c: &duckdb::Connection,
        id: i64,
        hn: &str,
        street: Option<&str>,
        city: Option<&str>,
    ) {
        c.execute_batch(&format!(
            "INSERT INTO osm_addresses VALUES ({id}, 'node', '{hn}', {}, {}, NULL,
                 ST_Point({ADDR_LON}, {ADDR_LAT}))",
            sql_str(street),
            sql_str(city),
        ))
        .unwrap();
    }

    fn mapping(c: &duckdb::Connection, simc: Option<&str>, from: &str, to: &str) {
        c.execute_batch(&format!(
            "INSERT INTO street_name_mappings VALUES ({}, '{from}', '{to}')",
            sql_str(simc),
        ))
        .unwrap();
    }

    fn unmatched_addr_ids(c: &duckdb::Connection) -> Vec<String> {
        let sql = unmatched_addresses_in_cell_sql(
            &PRG,
            "asrc",
            "a.lokalny_id",
            ADDR_AREA,
            buffer(ADDR_AREA, OSM_MATCH_BUFFER_DEG),
        );
        let mut s = c.prepare(&format!("{sql} ORDER BY a.lokalny_id")).unwrap();
        s.query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    /// The motivating record for `normalized_housenumber_sql`: PRG
    /// `9d0f1c57-797c-4035-96ef-11ab4100197f` spells a double housenumber
    /// "45-47", OSM node 4365400981 spells the identical address "45/47",
    /// 2.4 m apart on "Przyrodnicza" in Zgierz. Without the `-`/`\` -> `/`
    /// fold this pair fails rule A on a bare text mismatch despite agreeing
    /// on everything else.
    #[test]
    fn housenumber_dash_folds_to_slash_for_matching() {
        let c = conn();
        osm_addr(&c, 1, "45/47", None, None);
        prg(&c, "dash", "45-47", None, None, None, 0.0002);
        assert!(
            unmatched_addr_ids(&c).is_empty(),
            "PRG '45-47' must match OSM '45/47'"
        );
    }

    /// A backslash is the same fold as a dash — some PRG exports use it in
    /// place of a hyphen for the identical double-housenumber notation.
    #[test]
    fn housenumber_backslash_folds_to_slash_for_matching() {
        let c = conn();
        osm_addr(&c, 1, "45/47", None, None);
        prg(&c, "backslash", "45\\47", None, None, None, 0.0002);
        assert!(
            unmatched_addr_ids(&c).is_empty(),
            "PRG '45\\47' must match OSM '45/47'"
        );
    }

    /// "12 A" and "12A" are the same housenumber, differing only in whether
    /// a space separates the number from a single-letter suffix.
    #[test]
    fn housenumber_space_before_letter_suffix_collapses_for_matching() {
        let c = conn();
        osm_addr(&c, 1, "12A", None, None);
        prg(&c, "spaced", "12 A", None, None, None, 0.0002);
        assert!(
            unmatched_addr_ids(&c).is_empty(),
            "PRG '12 A' must match OSM '12A'"
        );
    }

    #[test]
    fn address_within_50m_same_hn_is_not_unmatched() {
        let c = conn();
        osm_addr(&c, 1, "12", None, None);
        prg(&c, "match", "12", None, None, None, 0.0002);
        prg(&c, "far", "12", None, None, None, 0.002);
        assert_eq!(
            unmatched_addr_ids(&c),
            vec!["far".to_string()],
            "the ~22m match drops out, the ~220m one stays"
        );
    }

    /// The motivating record, in miniature: PRG
    /// `7077839d-e180-4030-a679-f968741386f6` (Zakroczym, ul. Warszawska 44)
    /// sits 51.8 m from an OSM way carrying the identical street and
    /// housenumber — 1.8 m outside rule A, unambiguously the same address.
    /// With no mapping row present the resolution chain falls through to the
    /// raw PRG name, which is the common case nationally.
    #[test]
    fn address_within_150m_with_agreeing_street_is_not_unmatched() {
        let c = conn();
        osm_addr(&c, 1, "44", Some("Warszawska"), None);
        prg(&c, "same", "44", Some("Warszawska"), None, None, 0.0012);
        prg(&c, "other", "44", Some("Polna"), None, None, 0.0012);
        assert_eq!(
            unmatched_addr_ids(&c),
            vec!["other".to_string()],
            "an agreeing street matches at ~133m; a differing one does not"
        );
    }

    /// Case and surrounding whitespace must not decide a match — that is what
    /// `normalized_name_sql` is for, on both sides.
    #[test]
    fn street_comparison_ignores_case_and_whitespace() {
        let c = conn();
        osm_addr(&c, 1, "44", Some("  WARSZAWSKA "), None);
        prg(&c, "a", "44", Some("Warszawska"), None, None, 0.0012);
        assert!(unmatched_addr_ids(&c).is_empty());
    }

    /// Rule B compares the *mapped* name, not the raw one. Counter-intuitive
    /// and the first thing someone will "fix": here the raw names are
    /// byte-identical, and the address is still unmatched, because the mapping
    /// rewrites PRG's side to something OSM does not carry.
    #[test]
    fn address_matches_on_the_mapped_name_not_the_raw_name() {
        let c = conn();
        mapping(&c, None, "gen. Kruka", "Generała Kruka");
        osm_addr(&c, 1, "5", Some("gen. Kruka"), None);
        osm_addr(&c, 2, "6", Some("Generała Kruka"), None);
        prg(&c, "raw-equal", "5", Some("gen. Kruka"), None, None, 0.0012);
        prg(&c, "mapped", "6", Some("gen. Kruka"), None, None, 0.0012);
        assert_eq!(
            unmatched_addr_ids(&c),
            vec!["raw-equal".to_string()],
            "the mapped name decides the match, so raw equality is not enough"
        );
    }

    #[test]
    fn settlement_mapping_wins_over_the_global_one_for_matching() {
        let c = conn();
        mapping(&c, None, "gen. Kruka", "Generała Kruka");
        mapping(&c, Some("0956069"), "gen. Kruka", "Generała Michała Kruka");
        osm_addr(&c, 1, "5", Some("Generała Michała Kruka"), None);
        osm_addr(&c, 2, "6", Some("Generała Kruka"), None);
        // Both PRG rows carry the settlement SIMC, so the settlement mapping
        // applies to both -- only the one whose OSM neighbour carries the
        // settlement-scoped name may match.
        prg(
            &c,
            "settlement",
            "5",
            Some("gen. Kruka"),
            None,
            Some("0956069"),
            0.0012,
        );
        prg(
            &c,
            "global-name",
            "6",
            Some("gen. Kruka"),
            None,
            Some("0956069"),
            0.0012,
        );
        assert_eq!(
            unmatched_addr_ids(&c),
            vec!["global-name".to_string()],
            "the settlement row must override the global one at match time"
        );
    }

    /// The two mapping joins must stay two separate joins. Merging them into
    /// one `OR`-ed join makes an address with both a settlement row and a
    /// global row emit *two* rows, silently duplicating it in `prg_unmatched`.
    #[test]
    fn a_global_and_a_settlement_mapping_for_the_same_name_emit_one_row_per_address() {
        let c = conn();
        mapping(&c, None, "gen. Kruka", "Generała Kruka");
        mapping(&c, Some("0956069"), "gen. Kruka", "Generała Michała Kruka");
        // No OSM address at all, so the row is unmatched and must appear
        // exactly once.
        prg(
            &c,
            "dup-risk",
            "5",
            Some("gen. Kruka"),
            None,
            Some("0956069"),
            0.0,
        );
        assert_eq!(unmatched_addr_ids(&c), vec!["dup-risk".to_string()]);
    }

    #[test]
    fn place_address_within_150m_matches_when_both_streets_are_absent() {
        let c = conn();
        osm_addr(&c, 1, "7", None, Some("Kolonia Rychnowska"));
        prg(
            &c,
            "same-place",
            "7",
            None,
            Some("Kolonia Rychnowska"),
            None,
            0.0012,
        );
        prg(&c, "other-place", "7", None, Some("Rychnowo"), None, 0.0012);
        assert_eq!(
            unmatched_addr_ids(&c),
            vec!["other-place".to_string()],
            "an agreeing locality matches at ~133m only when neither side has a street"
        );
    }

    /// Rule C is gated on *both* sides lacking a street. One side carrying one
    /// means the two records disagree about whether this is a street address
    /// at all, which is not evidence they are the same address.
    #[test]
    fn place_rule_does_not_fire_when_either_side_has_a_street() {
        let c = conn();
        osm_addr(&c, 1, "7", Some("Polna"), Some("Rychnowo"));
        osm_addr(&c, 2, "8", None, Some("Rychnowo"));
        prg(
            &c,
            "osm-has-street",
            "7",
            None,
            Some("Rychnowo"),
            None,
            0.0012,
        );
        prg(
            &c,
            "prg-has-street",
            "8",
            Some("Polna"),
            Some("Rychnowo"),
            None,
            0.0012,
        );
        let ids = unmatched_addr_ids(&c);
        assert_eq!(
            ids,
            vec!["osm-has-street".to_string(), "prg-has-street".to_string()],
            "neither asymmetric case may match by locality"
        );
    }

    /// An empty string is absence, not a name. Without `normalized_name_sql`'s
    /// `NULLIF(..., '')` these two would match on rule B by comparing `''` to
    /// `''` — skipping rule C's locality gate entirely.
    #[test]
    fn empty_string_street_counts_as_absent() {
        let c = conn();
        osm_addr(&c, 1, "7", Some(""), Some("Rychnowo"));
        prg(
            &c,
            "matches",
            "7",
            Some("  "),
            Some("Rychnowo"),
            None,
            0.0012,
        );
        prg(&c, "wrong-place", "7", Some(""), Some("Inne"), None, 0.0012);
        assert_eq!(
            unmatched_addr_ids(&c),
            vec!["wrong-place".to_string()],
            "an empty street must route through rule C, not satisfy rule B on ''"
        );
    }

    /// Two addresses that merely both lack a locality must not match on two
    /// NULLs. Guards against "improving" the `=` to `IS NOT DISTINCT FROM`.
    #[test]
    fn null_locality_never_matches_by_place() {
        let c = conn();
        osm_addr(&c, 1, "7", None, None);
        prg(&c, "no-place", "7", None, None, None, 0.0012);
        assert_eq!(unmatched_addr_ids(&c), vec!["no-place".to_string()]);
    }

    #[test]
    fn beyond_150m_with_a_matching_street_is_unmatched() {
        let c = conn();
        osm_addr(&c, 1, "44", Some("Warszawska"), None);
        // ~178 m: the name agrees, the distance does not.
        prg(&c, "too-far", "44", Some("Warszawska"), None, None, 0.0016);
        assert_eq!(unmatched_addr_ids(&c), vec!["too-far".to_string()]);
    }

    /// Matching is per-row existence, not a bipartite assignment: one OSM node
    /// can satisfy any number of PRG rows. Measured nationally this "steals" a
    /// node from a closer PRG address in ~0.9% of the new matches, and the
    /// samples are genuine PRG registry duplicates (two `Warszawska 10`
    /// records in Narol, 1.2 m and 50.2 m from the same node). Accepted
    /// deliberately — a steal guard would make the two compare paths disagree,
    /// since the grid-key path has no notion of "closest". This test exists so
    /// that removing the behaviour is a decision, not an accident.
    #[test]
    fn an_osm_node_matched_at_50m_can_also_match_a_second_address_via_street() {
        let c = conn();
        osm_addr(&c, 1, "10", Some("Warszawska"), None);
        prg(&c, "near", "10", Some("Warszawska"), None, None, 0.0002);
        prg(
            &c,
            "duplicate",
            "10",
            Some("Warszawska"),
            None,
            None,
            0.0012,
        );
        assert!(
            unmatched_addr_ids(&c).is_empty(),
            "both PRG duplicates match the one OSM node"
        );
    }

    /// The name rules widened the correlated `NOT EXISTS` from two columns to
    /// four and added two `LEFT JOIN`s to the mapping table. Neither may cost
    /// the source table its geometry index — that is the difference between a
    /// z14 cell recompute taking ~0.07 s and taking a full-table scan.
    /// `osm_addresses` is left unindexed here, so a plan mentioning
    /// `RTREE_IN` can only be coming from `asrc`.
    #[test]
    fn unmatched_addresses_predicate_uses_the_geom_rtree_index() {
        let c = conn();
        mapping(&c, None, "gen. Kruka", "Generała Kruka");
        c.execute_batch(
            "CREATE INDEX asrc_geom_idx ON asrc USING RTREE (geom);
             INSERT INTO asrc
                 SELECT 'a' || i, '1', NULL, NULL, NULL,
                        ST_Point(20.0 + i * 0.0001, 52.0)
                 FROM range(20000) t(i);",
        )
        .unwrap();
        let area = (20.5, 51.99, 20.6, 52.01);
        let sql = unmatched_addresses_in_cell_sql(
            &PRG,
            "asrc",
            "a.lokalny_id",
            area,
            buffer(area, OSM_MATCH_BUFFER_DEG),
        );
        let mut stmt = c.prepare(&format!("EXPLAIN {sql}")).unwrap();
        let mut rows = stmt.query([]).unwrap();
        let mut plan = String::new();
        while let Some(row) = rows.next().unwrap() {
            plan.push_str(&row.get::<_, String>(1).unwrap_or_default());
        }
        // Substring "RTREE_IN" rather than the full operator name: DuckDB's
        // EXPLAIN truncates operator labels to fit box width in a wide plan.
        assert!(
            plan.contains("RTREE_IN"),
            "the candidates CTE must still reach the geom RTREE index, got plan: {plan}"
        );
    }

    /// `OSM_MATCH_BUFFER_DEG`'s own doc admits the failure mode is silent: an
    /// OSM address just outside the buffered read simply stops matching, with
    /// no error anywhere. Compute the requirement instead of trusting prose,
    /// and check it against the *widest* distance any branch uses.
    #[test]
    fn osm_match_buffer_covers_the_widest_match_distance() {
        // Poland's northern edge, where a degree of longitude is shortest and
        // the buffer therefore covers the fewest metres.
        let m_per_deg_lon = 111_320.0 * 54.84_f64.to_radians().cos();
        let required = NAME_MATCH_DISTANCE_METERS / m_per_deg_lon;
        assert!(
            OSM_MATCH_BUFFER_DEG >= required,
            "OSM_MATCH_BUFFER_DEG ({OSM_MATCH_BUFFER_DEG}) must cover \
             NAME_MATCH_DISTANCE_METERS ({NAME_MATCH_DISTANCE_METERS} m = \
             {required} deg of longitude at 54.84N)"
        );
        // A `const` block, so this one is checked at compile time: the name
        // rules are the wider ones, and if that ever inverts the assertion
        // above is checking the wrong constant.
        const {
            assert!(NAME_MATCH_DISTANCE_METERS >= MATCH_DISTANCE_METERS);
        }
    }
}
