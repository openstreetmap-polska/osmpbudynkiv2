use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use anyhow::Context;

use super::AppState;
use super::http_cache;
use std::sync::LazyLock;

use super::package::{ADJACENCY_READ_BUFFER_DEG, BDOT10K_ADJACENCY_KEY, EGIB_ADJACENCY_KEY};
use crate::compare::rule::reported_sql;
use crate::dataset::{BDOT10K, EGIB, PRG};
use crate::mappings::street_names::{resolved_street_expr_sql, resolved_street_join_sql};
use crate::serving_version;
use crate::tile_math::tile_to_bbox;

// ST_AsMVTGeom's bounds argument is BOX_2D, not GEOMETRY -- ST_MakeEnvelope
// returns GEOMETRY, so it must be narrowed via ST_Extent() first or DuckDB's
// binder rejects the call outright (verified: no (GEOMETRY, GEOMETRY, ...)
// overload exists). BUILDINGS_MVT_SQL's UNION ALL branches also had a bare
// `geom` column reference ambiguous between the source table and the `bbox`
// CTE (both have a `geom` column) -- qualified below. Both bugs bit every
// /tiles request at z=14 regardless of row content, since binder errors
// happen before execution -- caught only by actually running a query against
// real data, not by any prior test (there were none). See
// docs/duckdb_connection_visibility_investigation.md.
// The filter is `ST_Intersects(geom, ST_MakeEnvelope(?, ?, ?, ?))`, NOT the
// shorter `geom && bbox.geom` against the CTE, and that is load-bearing rather
// than stylistic: DuckDB's RTREE index scan only fires for a spatial predicate
// whose second argument is *constant*. Joining the one-row `bbox` CTE in makes
// the bbox a joined value, so `&&` against it plans as SEQ_SCAN + SPATIAL_JOIN
// over the whole serving table even when the RTREE index exists -- measured,
// not assumed (see docs/followups_precomputed_unmatched_serving.md). Bound `?`
// parameters still count as constant, so the bbox stays parameterised.
//
// The two forms serve identical features: `&&` is a bounding-box test and
// ST_Intersects is exact, but ST_AsMVTGeom returns NULL for anything that does
// not truly meet the tile and the outer `WHERE t.geom IS NOT NULL` already
// dropped those. Verified equal feature counts across 7 tiles x 2 tables.
//
// `bbox` therefore survives only as ST_AsMVTGeom's bounds argument -- it is no
// longer joined against the source tables, which is what frees the index.
//
// Attribute names default to whatever the underlying column is already
// called (raw government field, or the name `compare` already carries it
// under on `*_unmatched`) rather than inventing English aliases -- see
// docs/vector_tile_attributes.md. `id`/`source`/`levels_above_ground`/`tags`
// are the exceptions: `id`/`source` unify two differently-named source
// columns so the UNION ALL has one column to project, and
// `levels_above_ground` unifies bdot10k's `liczba_kondygnacji` and egib's
// `kondygnacje_nadziemne` because they're genuinely the same concept
// (storeys above ground) under different names/case; `tags` is computed.
// bdot10k's `funkcja_szczegolowa`/`funkcja_ogolna` (raw
// `PRZEWAZAJACAFUNKCJABUDYNKU`/`FUNKCJAOGOLNABUDYNKU`) are NOT unified with
// anything egib-side -- EGIB has no equivalent two-tier function
// classification (only the unrelated single-letter `rodzaj`/`rodzaj_kod`
// scheme), so they stay under their own names, NULL on egib's branch.
//
// Only the two *unmatched* layers (`addresses`/`buildings`) carry resolved
// OSM tags -- a matched object would never be imported, so there's nothing
// to preview for it, matching `server::package`'s own precedent of only ever
// resolving tags for `*_unmatched` rows. Address tag resolution
// (`resolved` CTE below) mirrors `package::unmatched_addresses`'s street-name
// join; building tag resolution (`bdot10k_final`/`egib_final` below) mirrors
// `package::unmatched_bdot10k_buildings`/`unmatched_egib_buildings`'s
// adjacency + mapping-table LATERAL join, reusing the same
// `ADJACENCY_READ_BUFFER_DEG`/`BDOT10K_ADJACENCY_KEY`/`EGIB_ADJACENCY_KEY`
// constants rather than re-typing them. Both omit `package`'s polygon-clip
// predicate (`ST_Intersects(_, ST_GeomFromGeoJSON(?))`) since tiles are
// always rectangular, unlike a `/package` request area.
/// Built once at first use rather than declared `const`, because the street
/// resolution comes from `mappings::street_names`'s shared builders — the same
/// text `/package` and both compare paths use. The resulting SQL is
/// semantically identical to the hand-written chain this replaced, so
/// `serving_version::TILE_FORMAT_VERSION` must **not** move for it: nothing
/// about what a tile contains changed.
static ADDRESSES_MVT_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "
    WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(?, ?, ?, ?)) AS geom),
    candidates AS MATERIALIZED (
        SELECT a.geom, a.lokalny_id, a.numer_porzadkowy, a.ulica, a.miejscowosc,
               a.kod_pocztowy, a.teryt_miejscowosc, a.wazny_od_lub_data_nadania,
               a.teryt_gmina, a.gmina
        FROM prg_unmatched a
        WHERE ST_Intersects(a.geom, ST_MakeEnvelope(?, ?, ?, ?))
    ),
    resolved AS (
        SELECT candidates.*,
               NULLIF(trim({resolved_street}), '') AS resolved_street
        FROM candidates
        {mapping_joins}
    )
    SELECT ST_AsMVT(t, 'addresses', 4096, 'geom') AS mvt
    FROM (
        SELECT ST_AsMVTGeom(resolved.geom, bbox.geom, 4096, 256, true) AS geom,
               resolved.lokalny_id,
               resolved.numer_porzadkowy,
               resolved.ulica,
               resolved.miejscowosc,
               resolved.kod_pocztowy,
               resolved.wazny_od_lub_data_nadania::VARCHAR AS wazny_od_lub_data_nadania,
               resolved.teryt_gmina,
               resolved.gmina,
               NULLIF(trim(resolved.numer_porzadkowy), '') AS \"addr:housenumber\",
               resolved.resolved_street AS \"addr:street\",
               CASE WHEN resolved.resolved_street IS NOT NULL THEN NULLIF(trim(resolved.miejscowosc), '') END AS \"addr:city\",
               CASE WHEN resolved.resolved_street IS NULL THEN NULLIF(trim(resolved.miejscowosc), '') END AS \"addr:place\",
               NULLIF(trim(resolved.kod_pocztowy), '') AS \"addr:postcode\",
               NULLIF(trim(resolved.teryt_miejscowosc), '') AS \"addr:city:simc\",
               'gugik.gov.pl' AS \"source:addr\"
        FROM resolved, bbox
    ) t
    WHERE t.geom IS NOT NULL
",
        resolved_street = resolved_street_expr_sql("candidates"),
        mapping_joins = resolved_street_join_sql("candidates"),
    )
});

const BUILDINGS_MVT_SQL: &str = "
    WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(?, ?, ?, ?)) AS geom),
    bdot10k_pkg AS MATERIALIZED (
        SELECT b.rowid AS rid, b.LOKALNYID AS id, b.PRZESTRZENNAZW, b.geom,
               ST_X(ST_Centroid(b.geom)) AS cx, ST_Y(ST_Centroid(b.geom)) AS cy,
               b.funkcja_szczegolowa, b.funkcja_ogolna, b.liczba_kondygnacji,
               b.KATEGORIAISTNIENIA, b.NAZWA, b.FSBUD, b.INFORMACJADODATKOWA,
               b.KODKST, b.ZRODLODANYCHGEOMETRYCZNYCH
        FROM bdot10k_unmatched b
        WHERE ST_Intersects(b.geom, ST_MakeEnvelope(?, ?, ?, ?))
    ),
    bdot10k_nb AS MATERIALIZED (
        SELECT geom, ST_X(centroid) AS cx, ST_Y(centroid) AS cy
        FROM bdot10k_buildings
        WHERE ST_Intersects(geom, ST_MakeEnvelope(?, ?, ?, ?))
          AND lower(trim(PRZEWAZAJACAFUNKCJABUDYNKU)) = ?
    ),
    bdot10k_cnt AS (
        SELECT p.rid, count(*) AS neighbours
        FROM bdot10k_pkg p JOIN bdot10k_nb nb
          ON (p.cx <> nb.cx OR p.cy <> nb.cy) AND ST_Intersects(p.geom, nb.geom)
        GROUP BY p.rid
    ),
    bdot10k_final AS (
        SELECT pkg.geom, 'bdot10k' AS source, pkg.id, pkg.PRZESTRZENNAZW,
               pkg.funkcja_szczegolowa, pkg.funkcja_ogolna,
               pkg.liczba_kondygnacji::INTEGER AS levels_above_ground,
               pkg.KATEGORIAISTNIENIA, pkg.NAZWA, pkg.FSBUD, pkg.INFORMACJADODATKOWA,
               pkg.KODKST::INTEGER AS KODKST, pkg.ZRODLODANYCHGEOMETRYCZNYCH,
               NULL::INTEGER AS kondygnacje_podziemne, NULL::VARCHAR AS rodzaj,
               COALESCE(t.tags, 'building=yes') AS tags
        FROM bdot10k_pkg pkg
        LEFT JOIN bdot10k_cnt cnt USING (rid)
        LEFT JOIN LATERAL (
            SELECT m.tags FROM bdot10k_building_types m
            WHERE ((m.tier = 1 AND m.key = lower(trim(pkg.funkcja_szczegolowa)))
                OR (m.tier = 2 AND m.key = lower(trim(pkg.funkcja_ogolna))))
              AND (m.min_levels IS NULL OR pkg.liczba_kondygnacji >= m.min_levels)
              AND (m.max_levels IS NULL OR pkg.liczba_kondygnacji <= m.max_levels)
              AND (m.max_neighbours IS NULL OR coalesce(cnt.neighbours, 0) <= m.max_neighbours)
            ORDER BY m.tier ASC,
                     (m.min_levels IS NOT NULL)::INT
                   + (m.max_levels IS NOT NULL)::INT
                   + (m.max_neighbours IS NOT NULL)::INT DESC
            LIMIT 1
        ) t ON TRUE
    ),
    egib_pkg AS MATERIALIZED (
        SELECT b.rowid AS rid, b.id_budynku AS id, b.geom,
               ST_X(ST_Centroid(b.geom)) AS cx, ST_Y(ST_Centroid(b.geom)) AS cy,
               b.rodzaj_kod, b.kondygnacje_nadziemne, b.kondygnacje_podziemne, b.rodzaj
        FROM egib_unmatched b
        WHERE ST_Intersects(b.geom, ST_MakeEnvelope(?, ?, ?, ?))
    ),
    egib_nb AS MATERIALIZED (
        SELECT geom, ST_X(centroid) AS cx, ST_Y(centroid) AS cy
        FROM egib_buildings
        WHERE ST_Intersects(geom, ST_MakeEnvelope(?, ?, ?, ?))
          AND rodzaj_kod = ?
    ),
    egib_cnt AS (
        SELECT p.rid, count(*) AS neighbours
        FROM egib_pkg p JOIN egib_nb nb
          ON (p.cx <> nb.cx OR p.cy <> nb.cy) AND ST_Intersects(p.geom, nb.geom)
        GROUP BY p.rid
    ),
    egib_final AS (
        SELECT pkg.geom, 'egib' AS source, pkg.id, NULL::VARCHAR AS PRZESTRZENNAZW,
               NULL::VARCHAR AS funkcja_szczegolowa, NULL::VARCHAR AS funkcja_ogolna,
               pkg.kondygnacje_nadziemne AS levels_above_ground,
               NULL::VARCHAR AS KATEGORIAISTNIENIA, NULL::VARCHAR AS NAZWA,
               NULL::VARCHAR AS FSBUD, NULL::VARCHAR AS INFORMACJADODATKOWA,
               NULL::INTEGER AS KODKST, NULL::VARCHAR AS ZRODLODANYCHGEOMETRYCZNYCH,
               pkg.kondygnacje_podziemne, pkg.rodzaj,
               COALESCE(t.tags, 'building=yes') AS tags
        FROM egib_pkg pkg
        LEFT JOIN egib_cnt cnt USING (rid)
        LEFT JOIN LATERAL (
            SELECT m.tags FROM egib_building_types m
            WHERE m.tier = 1 AND m.key = pkg.rodzaj_kod
              AND (m.min_levels IS NULL OR pkg.kondygnacje_nadziemne >= m.min_levels)
              AND (m.max_levels IS NULL OR pkg.kondygnacje_nadziemne <= m.max_levels)
              AND (m.max_neighbours IS NULL OR coalesce(cnt.neighbours, 0) <= m.max_neighbours)
            ORDER BY (m.min_levels IS NOT NULL)::INT
                   + (m.max_levels IS NOT NULL)::INT
                   + (m.max_neighbours IS NOT NULL)::INT DESC
            LIMIT 1
        ) t ON TRUE
    )
    SELECT ST_AsMVT(t, 'buildings', 4096, 'geom') AS mvt
    FROM (
        SELECT ST_AsMVTGeom(u.geom, bbox.geom, 4096, 256, true) AS geom, u.id, u.source,
               -- Identity, not display: the other half of BDOT10k's composite
               -- key, so the frontend's report action can send a complete
               -- record key. NULL for egib, whose id_budynku is the whole key.
               u.PRZESTRZENNAZW,
               u.funkcja_szczegolowa, u.funkcja_ogolna, u.levels_above_ground,
               u.KATEGORIAISTNIENIA, u.NAZWA, u.FSBUD, u.INFORMACJADODATKOWA,
               u.KODKST, u.ZRODLODANYCHGEOMETRYCZNYCH,
               u.kondygnacje_podziemne, u.rodzaj, u.tags
        FROM (SELECT * FROM bdot10k_final UNION ALL SELECT * FROM egib_final) u, bbox
    ) t
    WHERE t.geom IS NOT NULL
";

// Same shape as ADDRESSES_MVT_SQL/BUILDINGS_MVT_SQL above, reading the full
// government tables (`prg_addresses`, `bdot10k_buildings`, `egib_buildings`)
// instead of the `*_unmatched` serving tables, for the legend's "all" layer.
// These three tables are in `server::REQUIRED_TABLES` -- `run` refuses to
// start without them -- so, unlike the serving tables, no empty-table
// fallback is needed here. They carry the same RTREE(geom) indexes the
// serving tables do (created at import time; see `import::bdot10k`,
// `import::egib`, `import::prg`), so the constant-argument `ST_Intersects`
// form keeps this on the index for the same reason documented above. No tag
// resolution here -- these layers show every government object, matched or
// not, so there's nothing to "preview importing" for most of them.
//
// `reported` is the one exception to that "no resolution" rule, and it is
// what makes these two layers the *only* place a user-reported object is
// visible at all. An active report vetoes its object out of `*_unmatched`
// (see `rule::reported_sql`), so the object vanishes from the `addresses`/
// `buildings` layers and reappears only here, where it would otherwise be
// indistinguishable from an ordinary matched record -- the frontend showed
// it as plain "W rejestrze" with nothing saying why it stopped being
// offered. The flag lets the popup say "Zgłoszony" instead.
//
// Three things about it:
//
// 1. The predicate is `rule::reported_sql`, the same builder both compare
//    paths negate, so what the popup calls "reported" is by construction the
//    same condition that removed the row from the serving table -- the
//    "match rule has one home" property, extended to the read path.
// 2. `CASE WHEN ... THEN TRUE END`, not a bare boolean: `ST_AsMVT` omits a
//    NULL attribute from the feature that has it, so only the handful of
//    reported features pay for the flag and every other feature's bytes are
//    unchanged. Note what this does *not* mean: the layer's key dictionary
//    still gains the string `reported` as soon as the column exists, whether
//    or not any feature is flagged -- verified against a real tile, and the
//    reason the tests below read the flag per row rather than grepping bytes.
// 3. The `MATERIALIZED` CTEs are not decoration. Before this, each source's
//    spatial filter sat directly in the scan and reached the RTREE index; a
//    correlated `EXISTS` added alongside it gives the optimizer licence to
//    re-plan the filtered scan into SEQ_SCAN + FILTER, exactly as a
//    downstream `LEFT JOIN` does (see the ADDRESSES_MVT_SQL/BUILDINGS_MVT_SQL
//    comment above). Computing the bbox-filtered candidate set first and
//    correlating over *that* keeps the index scan.
//    `mvt_bbox_filter_uses_the_rtree_index` asserts on the plan and is the
//    only thing that would catch a regression here.
//
// Cache correctness: `POST /report` is on `serving_version`'s must-not-bump
// list, and this attribute does not change that. The insert enqueues the
// object's z14 cell, the drain recomputes it, and the row leaving
// `<source>_unmatched` moves that cell's per-cell version -- which every tile
// rendering the object already folds in, since a surviving building's reach
// from its own centroid's cell is <= 1 (`dataset::filter_oversized_geometry`)
// and `z14_tile_version` reads a 3x3 ring. So the flag becomes visible in the
// same version change that removes the object from the unmatched layer, not
// before and not later. Revoke and expiry enqueue the same way.
//
// Both are built through a `projection` seam for the same reason
// `compare::incremental` has one: `ST_AsMVT` returns opaque protobuf bytes, so
// a test that only searches those bytes can see that a *key* exists but never
// which features carry it -- a `reported` clause that flagged every building
// would look identical to one that flagged the right one. Passing a plain
// column list instead of the `ST_AsMVT(...)` call lets the tests read the flag
// per row off the real generated SQL, wrapper aside.
static ALL_ADDRESSES_MVT_SQL: LazyLock<String> =
    LazyLock::new(|| all_addresses_sql("ST_AsMVT(t, 'addresses_all', 4096, 'geom') AS mvt"));

fn all_addresses_sql(projection: &str) -> String {
    format!(
        "
    WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(?, ?, ?, ?)) AS geom),
    candidates AS MATERIALIZED (
        SELECT a.geom, a.lokalny_id, a.numer_porzadkowy, a.ulica, a.miejscowosc,
               a.kod_pocztowy, a.wazny_od_lub_data_nadania, a.teryt_gmina, a.gmina
        FROM prg_addresses a
        WHERE ST_Intersects(a.geom, ST_MakeEnvelope(?, ?, ?, ?))
    )
    SELECT {projection}
    FROM (
        SELECT ST_AsMVTGeom(a.geom, bbox.geom, 4096, 256, true) AS geom,
               a.lokalny_id,
               a.numer_porzadkowy,
               a.ulica,
               a.miejscowosc,
               a.kod_pocztowy,
               a.wazny_od_lub_data_nadania::VARCHAR AS wazny_od_lub_data_nadania,
               a.teryt_gmina,
               a.gmina,
               CASE WHEN {reported} THEN TRUE END AS reported
        FROM candidates a, bbox
    ) t
    WHERE t.geom IS NOT NULL
",
        reported = reported_sql(&PRG, "a"),
    )
}

static ALL_BUILDINGS_MVT_SQL: LazyLock<String> =
    LazyLock::new(|| all_buildings_sql("ST_AsMVT(t, 'buildings_all', 4096, 'geom') AS mvt"));

fn all_buildings_sql(projection: &str) -> String {
    format!(
        "
    WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(?, ?, ?, ?)) AS geom),
    bdot10k_candidates AS MATERIALIZED (
        SELECT b.geom, b.PRZESTRZENNAZW, b.LOKALNYID, b.PRZEWAZAJACAFUNKCJABUDYNKU,
               b.FUNKCJAOGOLNABUDYNKU, b.LICZBAKONDYGNACJI, b.KATEGORIAISTNIENIA,
               b.NAZWA, b.FSBUD, b.INFORMACJADODATKOWA, b.KODKST,
               b.ZRODLODANYCHGEOMETRYCZNYCH
        FROM bdot10k_buildings b
        WHERE ST_Intersects(b.geom, ST_MakeEnvelope(?, ?, ?, ?))
    ),
    egib_candidates AS MATERIALIZED (
        SELECT b.geom, b.id_budynku, b.kondygnacje_nadziemne, b.kondygnacje_podziemne,
               b.rodzaj
        FROM egib_buildings b
        WHERE ST_Intersects(b.geom, ST_MakeEnvelope(?, ?, ?, ?))
    )
    SELECT {projection}
    FROM (
        SELECT ST_AsMVTGeom(raw.geom, bbox.geom, 4096, 256, true) AS geom,
               raw.id, raw.source, raw.PRZEWAZAJACAFUNKCJABUDYNKU, raw.FUNKCJAOGOLNABUDYNKU,
               raw.levels_above_ground, raw.KATEGORIAISTNIENIA, raw.NAZWA, raw.FSBUD,
               raw.INFORMACJADODATKOWA, raw.KODKST, raw.ZRODLODANYCHGEOMETRYCZNYCH,
               raw.kondygnacje_podziemne, raw.rodzaj, raw.reported
        FROM (
            SELECT b.geom, b.LOKALNYID AS id, 'bdot10k' AS source,
                   b.PRZEWAZAJACAFUNKCJABUDYNKU, b.FUNKCJAOGOLNABUDYNKU,
                   b.LICZBAKONDYGNACJI::INTEGER AS levels_above_ground,
                   b.KATEGORIAISTNIENIA, b.NAZWA, b.FSBUD, b.INFORMACJADODATKOWA,
                   b.KODKST::INTEGER AS KODKST, b.ZRODLODANYCHGEOMETRYCZNYCH,
                   NULL::INTEGER AS kondygnacje_podziemne, NULL::VARCHAR AS rodzaj,
                   CASE WHEN {reported_bdot10k} THEN TRUE END AS reported
            FROM bdot10k_candidates b
            UNION ALL
            SELECT b.geom, b.id_budynku AS id, 'egib' AS source,
                   NULL::VARCHAR AS PRZEWAZAJACAFUNKCJABUDYNKU, NULL::VARCHAR AS FUNKCJAOGOLNABUDYNKU,
                   b.kondygnacje_nadziemne AS levels_above_ground,
                   NULL::VARCHAR AS KATEGORIAISTNIENIA, NULL::VARCHAR AS NAZWA, NULL::VARCHAR AS FSBUD,
                   NULL::VARCHAR AS INFORMACJADODATKOWA, NULL::INTEGER AS KODKST,
                   NULL::VARCHAR AS ZRODLODANYCHGEOMETRYCZNYCH,
                   b.kondygnacje_podziemne, b.rodzaj,
                   CASE WHEN {reported_egib} THEN TRUE END AS reported
            FROM egib_candidates b
        ) raw, bbox
    ) t
    WHERE t.geom IS NOT NULL
",
        reported_bdot10k = reported_sql(&BDOT10K, "b"),
        reported_egib = reported_sql(&EGIB, "b"),
    )
}

// --- Tier A (z5..=z11): aggregated bins -------------------------------------
//
// bdot10k_unmatched/egib_unmatched/prg_unmatched all carry cell_x/cell_y: the
// exact, duplicate-free z14 XYZ tile of the row's representative point (see
// CLAUDE.md's "serving tables store rows, not id references" gotcha). That
// means the parent tile at any zoom z <= 14 is a pure bit shift -- no spatial
// index, no geometry read, no RTREE involved at all, unlike every other query
// in this file.
//
// bz ("bin zoom") is the zoom at which bins are counted: z+5 capped at 14
// (K=5, so bins are 32x32 per tile for z=5..9; z=10 and z=11 are both capped
// by the z14 ceiling and get progressively fewer bins per tile -- 16x16 and
// 8x8 respectively -- since there's no finer cell data to aggregate below
// z14). shift = 14 - bz is how far cell_x/cell_y are right-shifted to get
// the bin coordinate; n = 2^bz is the bin grid's full width, used to invert
// bin coordinates back to lon/lat via the standard Web Mercator XYZ formula
// -- the same one `tile_math::tile_to_bbox` implements in Rust, just spelled
// in SQL with DuckDB's degrees/atan/sinh/pi builtins (all verified
// available).
//
// The filter is deliberately `cell_x BETWEEN lo AND hi`, not
// `cell_x >> shift = x` -- same rows, but the range form is zonemap-prunable
// (measured 0.10s vs 0.31s for the full z5..10 pyramid in one query). bz/
// shift/n are derived from z in Rust, not user input (z is already
// range-checked by the dispatcher below), so interpolating them directly
// into the SQL text is safe; the four cell-range bounds stay bound `?`
// parameters.
//
// agg_cells and agg_points are two different final SELECTs over the *same*
// bins/geo CTEs (`agg_bin_ctes`, shared by both), so the frontend can switch
// visual styles (grid vs circles vs heatmap) with no backend change -- both
// layers always describe the same aggregate.
fn agg_bin_ctes(shift: u32, n: u32) -> String {
    format!(
        "WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(?, ?, ?, ?)) AS geom),
        bins AS (
            SELECT bin_x, bin_y,
                   sum(nb)::INTEGER AS n_bdot10k,
                   sum(ne)::INTEGER AS n_egib,
                   sum(np)::INTEGER AS n_prg,
                   sum(tb)::INTEGER AS t_bdot10k,
                   sum(te)::INTEGER AS t_egib,
                   sum(tp)::INTEGER AS t_prg
            FROM (
                SELECT cell_x >> {shift} AS bin_x, cell_y >> {shift} AS bin_y,
                       count(*) AS nb, 0 AS ne, 0 AS np,
                       0 AS tb, 0 AS te, 0 AS tp
                  FROM bdot10k_unmatched
                 WHERE cell_x BETWEEN ? AND ? AND cell_y BETWEEN ? AND ?
                 GROUP BY 1, 2
                UNION ALL
                SELECT cell_x >> {shift}, cell_y >> {shift}, 0, count(*), 0, 0, 0, 0
                  FROM egib_unmatched
                 WHERE cell_x BETWEEN ? AND ? AND cell_y BETWEEN ? AND ?
                 GROUP BY 1, 2
                UNION ALL
                SELECT cell_x >> {shift}, cell_y >> {shift}, 0, 0, count(*), 0, 0, 0
                  FROM prg_unmatched
                 WHERE cell_x BETWEEN ? AND ? AND cell_y BETWEEN ? AND ?
                 GROUP BY 1, 2
                UNION ALL
                -- Denominators. One row per (source, cell) rather than one per
                -- object, so all three sources come from a single scan with a
                -- CASE fan-out instead of three unioned subqueries.
                SELECT cell_x >> {shift}, cell_y >> {shift}, 0, 0, 0,
                       sum(CASE WHEN source = 'bdot10k' THEN total ELSE 0 END),
                       sum(CASE WHEN source = 'egib' THEN total ELSE 0 END),
                       sum(CASE WHEN source = 'prg' THEN total ELSE 0 END)
                  FROM cell_totals
                 WHERE cell_x BETWEEN ? AND ? AND cell_y BETWEEN ? AND ?
                 GROUP BY 1, 2
            ) src
            GROUP BY 1, 2
        ),
        geo AS (
            SELECT bin_x, bin_y, n_bdot10k, n_egib, n_prg,
                   (n_bdot10k + n_egib + n_prg)::INTEGER AS n_total,
                   t_bdot10k, t_egib, t_prg,
                   (t_bdot10k + t_egib + t_prg)::INTEGER AS t_total,
                   {r_bdot10k}, {r_egib}, {r_prg}, {r_total},
                   bin_x / {n}.0 * 360 - 180 AS lon0,
                   (bin_x + 1) / {n}.0 * 360 - 180 AS lon1,
                   degrees(atan(sinh(pi() * (1 - 2 * bin_y / {n}.0)))) AS lat_north,
                   degrees(atan(sinh(pi() * (1 - 2 * (bin_y + 1) / {n}.0)))) AS lat_south
            FROM bins
        )
        ",
        r_bdot10k = ratio_sql("n_bdot10k", "t_bdot10k", "r_bdot10k"),
        r_egib = ratio_sql("n_egib", "t_egib", "r_egib"),
        r_prg = ratio_sql("n_prg", "t_prg", "r_prg"),
        r_total = ratio_sql(
            "(n_bdot10k + n_egib + n_prg)",
            "(t_bdot10k + t_egib + t_prg)",
            "r_total"
        ),
    )
}

/// Completeness ratio (`unmatched ÷ total`) as a DOUBLE in 0..1, or
/// `RATIO_UNKNOWN` when the bin has no denominator.
///
/// The unknown case is not hypothetical and must not read as 0: `cell_totals`
/// is populated by `compare` (or reconcile + drain), so any database whose
/// serving tables predate this feature has unmatched rows and no totals at all
/// — see CLAUDE.md's standing note that this codebase has no `ALTER TABLE` or
/// backfill path. Emitting 0.0 there would paint the entire country as fully
/// imported, which is both wrong and indistinguishable from the genuinely
/// finished case; a sentinel lets the frontend render it as its own state.
///
/// The `least(..., 1.0)` clamp is defensive rather than expected: numerator and
/// denominator are always written in one transaction (`compare::totals`), so a
/// bin should never exceed its own total. If one ever does, clamping keeps the
/// colour ramp's domain intact instead of letting a single bin stretch it.
fn ratio_sql(numerator: &str, denominator: &str, alias: &str) -> String {
    format!(
        "CASE WHEN {denominator} > 0 \
              THEN least({numerator}::DOUBLE / {denominator}, 1.0) \
              ELSE {RATIO_UNKNOWN} END AS {alias}"
    )
}

/// Sentinel emitted for a bin with unmatched rows but no denominator. Negative
/// so it can never collide with a real ratio; the frontend tests for `< 0`.
const RATIO_UNKNOWN: f64 = -1.0;

/// `agg_cells`: each bin as a square polygon covering its Web Mercator
/// extent. Latitude decreases as bin_y increases, so `lat_south` (derived
/// from `bin_y + 1`) is the envelope's min and `lat_north` (from `bin_y`) is
/// its max -- `ST_MakeEnvelope` wants (min_lon, min_lat, max_lon, max_lat).
fn agg_cells_sql(shift: u32, n: u32) -> String {
    format!(
        "{}
        SELECT ST_AsMVT(t, 'agg_cells', 4096, 'geom') AS mvt
        FROM (
            SELECT ST_AsMVTGeom(
                       ST_MakeEnvelope(geo.lon0, geo.lat_south, geo.lon1, geo.lat_north),
                       bbox.geom, 4096, 256, true) AS geom,
                   geo.n_bdot10k, geo.n_egib, geo.n_prg, geo.n_total,
                   geo.t_bdot10k, geo.t_egib, geo.t_prg, geo.t_total,
                   geo.r_bdot10k, geo.r_egib, geo.r_prg, geo.r_total
            FROM geo, bbox
        ) t
        WHERE t.geom IS NOT NULL",
        agg_bin_ctes(shift, n)
    )
}

/// `agg_points`: each bin's centre as a point, same attributes as
/// `agg_cells` -- the frontend picks between the two layers purely by
/// toggling layer visibility client-side, no query difference beyond
/// geometry shape.
fn agg_points_sql(shift: u32, n: u32) -> String {
    format!(
        "{}
        SELECT ST_AsMVT(t, 'agg_points', 4096, 'geom') AS mvt
        FROM (
            SELECT ST_AsMVTGeom(
                       ST_Point((geo.lon0 + geo.lon1) / 2, (geo.lat_north + geo.lat_south) / 2),
                       bbox.geom, 4096, 256, true) AS geom,
                   geo.n_bdot10k, geo.n_egib, geo.n_prg, geo.n_total,
                   geo.t_bdot10k, geo.t_egib, geo.t_prg, geo.t_total,
                   geo.r_bdot10k, geo.r_egib, geo.r_prg, geo.r_total
            FROM geo, bbox
        ) t
        WHERE t.geom IS NOT NULL",
        agg_bin_ctes(shift, n)
    )
}

// --- Tier B (z12..=z13): individual points ----------------------------------
//
// One feature per unmatched object -- same cell_x/cell_y BETWEEN filter as
// Tier A, just not binned. Buildings are recentred to their centroid since
// bdot10k_unmatched/egib_unmatched carry polygons; prg_unmatched's `geom` is
// already a point (PRG addresses always were). Worst measured tile is ~14k
// features / ~0.1s -- acceptable for a look-and-feel MVP where performance is
// explicitly not the point.
const POINTS_MVT_SQL: &str = "
    WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(?, ?, ?, ?)) AS geom)
    SELECT ST_AsMVT(t, 'points', 4096, 'geom') AS mvt
    FROM (
        SELECT ST_AsMVTGeom(p.geom, bbox.geom, 4096, 256, true) AS geom, p.source
        FROM (
            SELECT ST_Centroid(b.geom) AS geom, 'bdot10k' AS source
              FROM bdot10k_unmatched b
             WHERE b.cell_x BETWEEN ? AND ? AND b.cell_y BETWEEN ? AND ?
            UNION ALL
            SELECT ST_Centroid(e.geom) AS geom, 'egib' AS source
              FROM egib_unmatched e
             WHERE e.cell_x BETWEEN ? AND ? AND e.cell_y BETWEEN ? AND ?
            UNION ALL
            SELECT a.geom AS geom, 'prg' AS source
              FROM prg_unmatched a
             WHERE a.cell_x BETWEEN ? AND ? AND a.cell_y BETWEEN ? AND ?
        ) p, bbox
    ) t
    WHERE t.geom IS NOT NULL
";

/// Outcome of the z14 blocking task: either the client's `If-None-Match`
/// already covers this version and there is nothing further to do (the pool
/// connection has already been dropped by the time this variant is built --
/// see `serve_tile`'s comment on why that happens inside the blocking task),
/// or the tile bytes plus whatever `ETag` the version computed to. `etag` is
/// `None` only when `serving_version::z14_tile_version` itself failed --
/// see the comment at that call site for why that degrades instead of 500s.
///
/// `Body` covers both a freshly queried tile and a `tile_cache::TileCache`
/// hit, deliberately as one variant rather than two: a hit must be
/// indistinguishable from a miss in the response it produces, and giving the
/// two their own variants would mean two response-shaping paths that could
/// drift apart on a header, a status code, or the empty-tile 204 (see
/// `z14_tile_response`). `Bytes` either way, so a cache hit is a refcount
/// bump rather than a copy of a several-hundred-KB tile.
enum Z14TaskOutcome {
    NotModified(HeaderValue),
    Body {
        bytes: Bytes,
        etag: Option<HeaderValue>,
    },
}

pub async fn serve_tile(
    State(state): State<AppState>,
    Path((z, x, y)): Path<(u32, u32, u32)>,
    headers: HeaderMap,
) -> Response {
    // Tiers A/B (z5..=z13) are dispatched before the z14 guard below, which
    // is otherwise left byte-for-byte as it was -- see the module-level
    // tiers documented above `agg_bin_ctes`/`POINTS_MVT_SQL`.
    if (5..=11).contains(&z) {
        return serve_tile_agg(state, z, x, y).await;
    }
    if (12..=13).contains(&z) {
        return serve_tile_points(state, z, x, y).await;
    }
    if z != 14 {
        // A property of the binary's zoom dispatch table, not of the data --
        // see http_cache::OUT_OF_RANGE_ZOOM_MAX_AGE_SECONDS's doc comment.
        let mut resp = StatusCode::NO_CONTENT.into_response();
        resp.headers_mut().insert(
            header::CACHE_CONTROL,
            state.cache_headers.out_of_range_zoom.clone(),
        );
        return resp;
    }

    let cache_header = state.cache_headers.tile.clone();
    // Only z14 gets an ETag. z5..=z13 have no version faithful to their
    // aggregated/point content -- serving_version's per-cell coverage is
    // z14-cell-shaped, and falling back to the epoch-only global signal
    // alone would pin those tiers stale until the next epoch bump, which is
    // strictly worse than the plain TTL they already get from cache_header.
    let if_none_match = headers.get(header::IF_NONE_MATCH).cloned();
    let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(z, x, y);
    let (buf_min_lon, buf_min_lat, buf_max_lon, buf_max_lat) = (
        min_lon - ADJACENCY_READ_BUFFER_DEG,
        min_lat - ADJACENCY_READ_BUFFER_DEG,
        max_lon + ADJACENCY_READ_BUFFER_DEG,
        max_lat + ADJACENCY_READ_BUFFER_DEG,
    );

    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<Z14TaskOutcome> {
        let conn = state
            .pool
            .get()
            .context("Failed to acquire pool connection")?;

        // Computed and checked against If-None-Match before any MVT SQL
        // runs, and inside this blocking task rather than around it -- that
        // is what lets a match `drop(conn)` and return right here, instead
        // of holding a pool slot through the tile queries below. A 16-tile
        // viewport refresh then releases 16 pool slots in ~2ms total instead
        // of the ~250ms the full queries would take.
        //
        // `cache_key_version` is kept around (not just folded into `etag`
        // immediately) because `tile_cache::TileCache` is keyed on the same
        // raw version string that `etag` wraps -- see the cache lookup right
        // below. It stays `None` exactly when `etag` does: a version
        // computation failure means there is no version to key a cache
        // entry on, so both the lookup and the eventual insert are skipped
        // together (an entry with no version could never be invalidated
        // correctly).
        let (etag, cache_key_version) = match serving_version::z14_tile_version(&conn, x, y) {
            Ok(version) => {
                if http_cache::if_none_match_matches(if_none_match.as_ref(), &version) {
                    let etag = http_cache::weak_etag(&version);
                    drop(conn);
                    return Ok(Z14TaskOutcome::NotModified(etag));
                }
                let etag = http_cache::weak_etag(&version);
                (Some(etag), Some(version))
            }
            Err(e) => {
                // A version hiccup should not take down tiles -- fall
                // through and serve the tile without an ETag rather than
                // failing the whole request over a metadata read, matching
                // z14_tile_version's own "degrade, don't fail" choices for
                // the pieces of its computation that can go individually
                // stale (see its doc comment).
                tracing::warn!(
                    error = %e, x, y,
                    "failed to compute z14 tile version; serving without an ETag"
                );
                (None, None)
            }
        };

        // Cache lookup, only reached once we know this isn't a 304 (that
        // returned above already). A hit drops the pool connection and
        // returns without running any MVT SQL, same reasoning as the 304
        // early-release right above: a 16-tile viewport refresh that's
        // already fully cached costs no pool slots at all beyond this
        // version read.
        if let Some(version) = cache_key_version.as_deref()
            && let Some(bytes) = state.tile_cache.get((z, x, y), version)
        {
            drop(conn);
            return Ok(Z14TaskOutcome::Body { bytes, etag });
        }

        // The bbox is repeated once per `?` group: the `bbox` CTE, then one
        // ST_MakeEnvelope per filtered table. Each group must stay in
        // min_lon, min_lat, max_lon, max_lat order.
        let addresses = query_mvt_layer(
            &conn,
            ADDRESSES_MVT_SQL.as_str(),
            duckdb::params![
                min_lon, min_lat, max_lon, max_lat, // bbox CTE
                min_lon, min_lat, max_lon, max_lat, // resolved (prg_unmatched) filter
            ],
        )?;
        let buildings = query_mvt_layer(
            &conn,
            BUILDINGS_MVT_SQL,
            duckdb::params![
                min_lon,
                min_lat,
                max_lon,
                max_lat, // bbox CTE
                min_lon,
                min_lat,
                max_lon,
                max_lat, // bdot10k_pkg (bdot10k_unmatched) filter
                buf_min_lon,
                buf_min_lat,
                buf_max_lon,
                buf_max_lat, // bdot10k_nb buffered filter
                BDOT10K_ADJACENCY_KEY,
                min_lon,
                min_lat,
                max_lon,
                max_lat, // egib_pkg (egib_unmatched) filter
                buf_min_lon,
                buf_min_lat,
                buf_max_lon,
                buf_max_lat, // egib_nb buffered filter
                EGIB_ADJACENCY_KEY,
            ],
        )?;
        let addresses_all = query_mvt_layer(
            &conn,
            ALL_ADDRESSES_MVT_SQL.as_str(),
            duckdb::params![
                min_lon, min_lat, max_lon, max_lat, // bbox CTE
                min_lon, min_lat, max_lon, max_lat, // candidates (prg_addresses) filter
            ],
        )?;
        let buildings_all = query_mvt_layer(
            &conn,
            ALL_BUILDINGS_MVT_SQL.as_str(),
            duckdb::params![
                min_lon, min_lat, max_lon, max_lat, // bbox CTE
                min_lon, min_lat, max_lon, max_lat, // bdot10k_candidates filter
                min_lon, min_lat, max_lon, max_lat, // egib_candidates filter
            ],
        )?;
        // Into `Bytes` once, not once per consumer: the cache stores `Bytes`
        // and the response is built from `Bytes`, so going through `Vec<u8>`
        // for either would memcpy a tile that runs to several hundred KB.
        let bytes = Bytes::from([addresses, buildings, addresses_all, buildings_all].concat());

        // Only cache when the version computed successfully -- see this
        // function's comment on `cache_key_version` above for why an
        // entry with no version must never be written.
        if let Some(version) = cache_key_version {
            state.tile_cache.insert((z, x, y), version, bytes.clone());
        }

        Ok(Z14TaskOutcome::Body { bytes, etag })
    })
    .await;

    match result {
        Ok(Ok(Z14TaskOutcome::NotModified(etag))) => http_cache::not_modified(etag, cache_header),
        Ok(Ok(Z14TaskOutcome::Body { bytes, etag })) => {
            z14_tile_response(bytes, cache_header, etag)
        }
        Ok(Err(e)) => finish_tile_response(Ok(Err(e)), z, x, y, cache_header, None),
        Err(e) => finish_tile_response(Err(e), z, x, y, cache_header, None),
    }
}

/// Response shaping for every successful z14 tile -- freshly queried or
/// served from `tile_cache::TileCache`, which is the point: one path, so a
/// hit cannot differ from a miss on a header, a status code, or the
/// empty-tile 204. (A z14 tile covering open country renders no features at
/// all, so an empty body is the *common* case over most of Poland, not an
/// edge case.)
///
/// Deliberately not routed through `finish_tile_response` below, which the
/// z5..=z13 tiers still use: its `bytes: Vec<u8>` parameter would force a
/// copy out of the cache's `Bytes` on every hit, defeating the entire point
/// of storing `Bytes` there (see `tile_cache`'s module doc). The two are
/// otherwise the same shaping, and the success branches must stay in step.
fn z14_tile_response(
    bytes: Bytes,
    cache_header: HeaderValue,
    etag: Option<HeaderValue>,
) -> Response {
    let mut resp = if bytes.is_empty() {
        StatusCode::NO_CONTENT.into_response()
    } else {
        let mut resp = bytes.into_response();
        resp.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.mapbox-vector-tile"),
        );
        resp
    };
    resp.headers_mut()
        .insert(header::CACHE_CONTROL, cache_header);
    if let Some(etag) = etag {
        resp.headers_mut().insert(header::ETAG, etag);
    }
    resp
}

fn query_mvt_layer(
    conn: &duckdb::Connection,
    sql: &str,
    params: impl duckdb::Params,
) -> anyhow::Result<Vec<u8>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query(params)?;
    match rows.next()? {
        Some(row) => {
            let blob: Vec<u8> = row.get(0)?;
            Ok(blob)
        }
        None => Ok(vec![]),
    }
}

/// Tier A dispatch (z5..=z11): two layers (`agg_cells`/`agg_points`), one
/// shared aggregate. See `agg_bin_ctes` above for the design rationale.
async fn serve_tile_agg(state: AppState, z: u32, x: u32, y: u32) -> Response {
    let cache_header = state.cache_headers.agg_tile.clone();
    let bz = (z + 5).min(14);
    let shift = 14 - bz;
    let n: u32 = 1u32 << bz;

    // Tile -> z14 cell range: a plain bit shift, since cell_x/cell_y already
    // *are* the z14 XYZ tile of each row's representative point.
    let cell_shift = 14 - z;
    let lo_x = (x << cell_shift) as i32;
    let hi_x = (((x + 1) << cell_shift) - 1) as i32;
    let lo_y = (y << cell_shift) as i32;
    let hi_y = (((y + 1) << cell_shift) - 1) as i32;

    let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(z, x, y);
    let cells_sql = agg_cells_sql(shift, n);
    let points_sql = agg_points_sql(shift, n);

    let result = tokio::task::spawn_blocking(move || {
        let conn = state
            .pool
            .get()
            .context("Failed to acquire pool connection")?;
        // Same four-bound-group shape as every other query in this file: the
        // bbox CTE, then one (lo_x, hi_x, lo_y, hi_y) group per unioned
        // source table.
        let cells = query_mvt_layer(
            &conn,
            &cells_sql,
            duckdb::params![
                min_lon, min_lat, max_lon, max_lat, // bbox CTE
                lo_x, hi_x, lo_y, hi_y, // bdot10k_unmatched filter
                lo_x, hi_x, lo_y, hi_y, // egib_unmatched filter
                lo_x, hi_x, lo_y, hi_y, // prg_unmatched filter
                lo_x, hi_x, lo_y, hi_y, // cell_totals filter (denominators)
            ],
        )?;
        let points = query_mvt_layer(
            &conn,
            &points_sql,
            duckdb::params![
                min_lon, min_lat, max_lon, max_lat, // bbox CTE
                lo_x, hi_x, lo_y, hi_y, // bdot10k_unmatched filter
                lo_x, hi_x, lo_y, hi_y, // egib_unmatched filter
                lo_x, hi_x, lo_y, hi_y, // prg_unmatched filter
                lo_x, hi_x, lo_y, hi_y, // cell_totals filter (denominators)
            ],
        )?;
        Ok::<Vec<u8>, anyhow::Error>([cells, points].concat())
    })
    .await;

    // z5..=z13 never carry an ETag -- see serve_tile's comment on why.
    finish_tile_response(result, z, x, y, cache_header, None)
}

/// Tier B dispatch (z12..=z13): one `points` layer, one feature per
/// unmatched object (not binned).
async fn serve_tile_points(state: AppState, z: u32, x: u32, y: u32) -> Response {
    let cache_header = state.cache_headers.agg_tile.clone();
    let cell_shift = 14 - z;
    let lo_x = (x << cell_shift) as i32;
    let hi_x = (((x + 1) << cell_shift) - 1) as i32;
    let lo_y = (y << cell_shift) as i32;
    let hi_y = (((y + 1) << cell_shift) - 1) as i32;

    let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(z, x, y);

    let result = tokio::task::spawn_blocking(move || {
        let conn = state
            .pool
            .get()
            .context("Failed to acquire pool connection")?;
        let points = query_mvt_layer(
            &conn,
            POINTS_MVT_SQL,
            duckdb::params![
                min_lon, min_lat, max_lon, max_lat, // bbox CTE
                lo_x, hi_x, lo_y, hi_y, // bdot10k_unmatched filter
                lo_x, hi_x, lo_y, hi_y, // egib_unmatched filter
                lo_x, hi_x, lo_y, hi_y, // prg_unmatched filter
            ],
        )?;
        Ok::<Vec<u8>, anyhow::Error>(points)
    })
    .await;

    // z5..=z13 never carry an ETag -- see serve_tile's comment on why.
    finish_tile_response(result, z, x, y, cache_header, None)
}

/// Response shaping shared by all three tiers (z5..=z11, z12..=z13, and --
/// as of Phase 2's cache-header work -- z14 too, via `serve_tile`'s tail).
/// `cache_header` is set only on a genuine tile response (200 with bytes, or
/// 204 for a query that legitimately found nothing) -- deliberately *not* on
/// the error branches below, so a query failure or panic stays header-less
/// and falls through to the API-default `no-store` the outer
/// `SetResponseHeaderLayer` in `build_router` stamps on anything without its
/// own `Cache-Control`. Caching a 500 would turn a transient DB hiccup into
/// an outage that outlives the hiccup itself.
///
/// `etag`, likewise, is only ever applied on the 200/204 branches, never on
/// a 500 -- an `ETag` on an error response would tell a client "this failure
/// is a representation of the resource, cache it and compare against it
/// later", which is exactly as wrong as caching the 500 itself. z5..=z13
/// callers always pass `None` (see `serve_tile`'s comment on why those
/// tiers get no `ETag` at all); z14 passes `Some` unless
/// `serving_version::z14_tile_version` itself failed.
fn finish_tile_response(
    result: Result<anyhow::Result<Vec<u8>>, tokio::task::JoinError>,
    z: u32,
    x: u32,
    y: u32,
    cache_header: HeaderValue,
    etag: Option<HeaderValue>,
) -> Response {
    match result {
        Ok(Ok(bytes)) if bytes.is_empty() => {
            let mut resp = StatusCode::NO_CONTENT.into_response();
            resp.headers_mut()
                .insert(header::CACHE_CONTROL, cache_header);
            if let Some(etag) = etag {
                resp.headers_mut().insert(header::ETAG, etag);
            }
            resp
        }
        Ok(Ok(bytes)) => {
            let mut resp = bytes.into_response();
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/vnd.mapbox-vector-tile"),
            );
            resp.headers_mut()
                .insert(header::CACHE_CONTROL, cache_header);
            if let Some(etag) = etag {
                resp.headers_mut().insert(header::ETAG, etag);
            }
            resp
        }
        Ok(Err(e)) => {
            tracing::error!(error = %e, z, x, y, "tile query failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, z, x, y, "tile task panicked");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;
    use crate::server::build_pool;

    /// The whole point of phrasing the filter as `ST_Intersects(geom,
    /// ST_MakeEnvelope(?, ?, ?, ?))` instead of `geom && bbox.geom`: only the
    /// constant-argument form lets DuckDB use the serving tables' RTREE index.
    /// Rewriting these queries back to a bbox joined in from the CTE would keep
    /// every test passing while quietly restoring a full table scan on every
    /// tile request, so assert on the plan itself. The index half of the pair
    /// is pinned by `db::tests::test_init_db_creates_serving_table_rtree_indexes`.
    ///
    /// `BUILDINGS_MVT_SQL` now scans four RTREE-indexed tables (bdot10k_unmatched
    /// and bdot10k_buildings for the bdot10k branch's pkg/nb reads, same pair for
    /// egib) -- counting occurrences rather than a single `.contains()` check
    /// matters here, since a regression on just one of the four scans would
    /// otherwise pass silently.
    #[test]
    fn mvt_bbox_filter_uses_the_rtree_index() {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let conn = crate::db::init_db(Path::new(":memory:"), &init, None).unwrap();
        // Enough rows that an index scan is plausibly cheaper than a seq scan;
        // the optimizer will not reach for an index on a handful of rows.
        conn.execute_batch(
            "INSERT INTO bdot10k_unmatched (LOKALNYID, geom, cell_x, cell_y, computed_at)
                 SELECT 'b' || i, ST_MakeEnvelope(20.0 + i*0.0001, 52.0, 20.0 + i*0.0001 + 0.00005, 52.00005), 0, 0, now()
                 FROM range(20000) t(i);
             INSERT INTO egib_unmatched (id_budynku, geom, cell_x, cell_y, computed_at)
                 SELECT 'e' || i, ST_MakeEnvelope(20.0 + i*0.0001, 52.0, 20.0 + i*0.0001 + 0.00005, 52.00005), 0, 0, now()
                 FROM range(20000) t(i);
             INSERT INTO prg_unmatched
                 (geom, lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
                  teryt_miejscowosc, wazny_od_lub_data_nadania, cell_x, cell_y, computed_at)
                 SELECT ST_Point(20.0 + i*0.0001, 52.0), 'p' || i, '1', NULL, NULL, NULL, NULL, NULL, 0, 0, now()
                 FROM range(20000) t(i);
             CREATE TABLE bdot10k_buildings (
                 PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR,
                 LICZBAKONDYGNACJI SMALLINT, KATEGORIAISTNIENIA VARCHAR, NAZWA VARCHAR,
                 FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             CREATE INDEX bdot10k_buildings_geom_idx ON bdot10k_buildings USING RTREE (geom);
             INSERT INTO bdot10k_buildings (LOKALNYID, geom, centroid)
                 SELECT 'b' || i,
                        ST_MakeEnvelope(20.0 + i*0.0001, 52.0, 20.0 + i*0.0001 + 0.00005, 52.00005),
                        ST_Point(20.0 + i*0.0001, 52.0)
                 FROM range(20000) t(i);
             CREATE TABLE egib_buildings (
                 id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY, rodzaj_kod VARCHAR,
                 kondygnacje_nadziemne INTEGER, kondygnacje_podziemne INTEGER, rodzaj VARCHAR);
             CREATE INDEX egib_buildings_geom_idx ON egib_buildings USING RTREE (geom);
             -- rodzaj_kod = 'm' on every row: egib_nb's filter is a plain
             -- column equality (unlike bdot10k_nb's lower(trim(...)) = ?,
             -- which defeats zonemap pruning), so if every row were NULL here
             -- DuckDB's optimizer proves the filter empty from column
             -- statistics alone and replaces the scan with EMPTY_RESULT --
             -- skipping the RTREE index entirely, not because it stopped
             -- using it but because it proved there was nothing to scan for.
             -- A real value avoids that and forces an actual (index) scan.
             INSERT INTO egib_buildings (id_budynku, geom, centroid, rodzaj_kod)
                 SELECT 'e' || i,
                        ST_MakeEnvelope(20.0 + i*0.0001, 52.0, 20.0 + i*0.0001 + 0.00005, 52.00005),
                        ST_Point(20.0 + i*0.0001, 52.0),
                        'm'
                 FROM range(20000) t(i);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR, miejscowosc VARCHAR,
                 kod_pocztowy VARCHAR, wazny_od_lub_data_nadania DATE, teryt_gmina VARCHAR,
                 gmina VARCHAR, geom GEOMETRY);
             CREATE INDEX prg_addresses_geom_idx ON prg_addresses USING RTREE (geom);
             INSERT INTO prg_addresses (lokalny_id, numer_porzadkowy, geom)
                 SELECT 'p' || i, '1', ST_Point(20.0 + i*0.0001, 52.0)
                 FROM range(20000) t(i);",
        )
        .unwrap();

        let plan_of = |sql: &str, params: &[&dyn duckdb::ToSql]| -> String {
            let mut stmt = conn.prepare(&format!("EXPLAIN {sql}")).unwrap();
            let mut rows = stmt.query(params).unwrap();
            let mut out = String::new();
            while let Some(row) = rows.next().unwrap() {
                out.push_str(&row.get::<_, String>(1).unwrap_or_default());
            }
            out
        };

        let b = [20.5_f64, 52.0, 20.6, 52.1];
        let addr_params: Vec<f64> = b.iter().chain(b.iter()).copied().collect();
        let addr_params_dyn: Vec<&dyn duckdb::ToSql> = addr_params
            .iter()
            .map(|v| v as &dyn duckdb::ToSql)
            .collect();

        // "RTREE_IN" rather than the full "RTREE_INDEX_SCAN": DuckDB's EXPLAIN
        // pretty-printer truncates operator labels to fit the box width, and
        // a plan with many sibling branches (BUILDINGS_MVT_SQL's four scans)
        // renders as "RTREE_IN..." -- verified by printing the plan and
        // comparing against the untruncated single-scan ADDRESSES_MVT_SQL case.
        let addr_plan = plan_of(ADDRESSES_MVT_SQL.as_str(), &addr_params_dyn);
        assert!(
            addr_plan.contains("RTREE_IN"),
            "addresses MVT query must use the RTREE index, got plan:\n{addr_plan}"
        );

        // bbox(4), bdot10k_pkg(4), bdot10k_nb(4)+key, egib_pkg(4), egib_nb(4)+key.
        // The nb reads don't need a real buffer here -- this test only checks
        // which scan operator the optimizer picks, not adjacency correctness.
        let bldg_f64: Vec<f64> = (0..5).flat_map(|_| b).collect();
        let mut bldg_params: Vec<&dyn duckdb::ToSql> = Vec::new();
        for v in &bldg_f64[0..12] {
            bldg_params.push(v as &dyn duckdb::ToSql);
        }
        bldg_params.push(&BDOT10K_ADJACENCY_KEY);
        for v in &bldg_f64[12..20] {
            bldg_params.push(v as &dyn duckdb::ToSql);
        }
        bldg_params.push(&EGIB_ADJACENCY_KEY);

        let bldg_plan = plan_of(BUILDINGS_MVT_SQL, &bldg_params);
        let bldg_scans = bldg_plan.matches("RTREE_IN").count();
        assert_eq!(
            bldg_scans, 4,
            "buildings MVT query must use all four RTREE indexes \
             (bdot10k_unmatched, bdot10k_buildings, egib_unmatched, egib_buildings), \
             got {bldg_scans} in plan:\n{bldg_plan}"
        );

        let all_addr_plan = plan_of(ALL_ADDRESSES_MVT_SQL.as_str(), &addr_params_dyn);
        assert!(
            all_addr_plan.contains("RTREE_IN"),
            "all-addresses MVT query must use the RTREE index, got plan:\n{all_addr_plan}"
        );

        // bbox(4), then bdot10k_buildings(4) and egib_buildings(4).
        let all_bldg_params: Vec<f64> = b.iter().chain(b.iter()).chain(b.iter()).copied().collect();
        let all_bldg_params_dyn: Vec<&dyn duckdb::ToSql> = all_bldg_params
            .iter()
            .map(|v| v as &dyn duckdb::ToSql)
            .collect();

        // Counted, not just `.contains()`: reading the new raw columns
        // alongside `geom` widens the projection the scan has to produce, and
        // a widened projection is exactly the kind of change that can tip
        // DuckDB into preferring a SEQ_SCAN for one branch while the other
        // still shows an index scan -- which a single `.contains()` would
        // happily pass.
        let all_bldg_plan = plan_of(ALL_BUILDINGS_MVT_SQL.as_str(), &all_bldg_params_dyn);
        let all_bldg_scans = all_bldg_plan.matches("RTREE_IN").count();
        assert_eq!(
            all_bldg_scans, 2,
            "all-buildings MVT query must use both RTREE indexes \
             (bdot10k_buildings, egib_buildings), got {all_bldg_scans} in plan:\n{all_bldg_plan}"
        );
    }

    /// In-memory DB with the government/OSM tables `/tiles` queries touch.
    /// `prg_unmatched`/`bdot10k_unmatched`/`egib_unmatched` (plus
    /// `street_name_mappings`/`bdot10k_building_types`/`egib_building_types`,
    /// all empty by default) come from `crate::db::init_db`'s real schema
    /// rather than a hand-rolled copy, so this fixture can't drift from
    /// `src/db.rs` the way a fourth hand-duplicated schema would -- only the
    /// three raw government tables `init_db` doesn't own are created here.
    fn make_state(seed_sql: &str) -> AppState {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let conn = crate::db::init_db(Path::new(":memory:"), &init, None).unwrap();
        conn.execute_batch(
            "CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR,
                 wazny_od_lub_data_nadania DATE, teryt_gmina VARCHAR, gmina VARCHAR,
                 geom GEOMETRY);
             CREATE TABLE bdot10k_buildings (
                 PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR,
                 LICZBAKONDYGNACJI SMALLINT, KATEGORIAISTNIENIA VARCHAR, NAZWA VARCHAR,
                 FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             CREATE TABLE egib_buildings (
                 id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY, rodzaj_kod VARCHAR,
                 kondygnacje_nadziemne INTEGER, kondygnacje_podziemne INTEGER, rodzaj VARCHAR);",
        )
        .unwrap();
        if !seed_sql.is_empty() {
            conn.execute_batch(seed_sql).unwrap();
        }
        let pool = build_pool(conn, 2).unwrap();
        AppState::for_tests(pool)
    }

    /// Mounts the real shipping router (`server::build_router`) rather than a
    /// `/tiles`-only stand-in, so these tests exercise the router as it is
    /// actually assembled in production.
    fn tiles_app(state: AppState) -> Router {
        crate::server::build_router(state)
    }

    async fn request_tile(state: AppState, z: u32, x: u32, y: u32) -> Response {
        request_tile_with_if_none_match(state, z, x, y, None).await
    }

    /// Same as `request_tile`, plus an optional `If-None-Match` request
    /// header -- used by the ETag/304 tests below.
    async fn request_tile_with_if_none_match(
        state: AppState,
        z: u32,
        x: u32,
        y: u32,
        if_none_match: Option<&str>,
    ) -> Response {
        let mut builder = Request::builder().uri(format!("/tiles/{z}/{x}/{y}"));
        if let Some(value) = if_none_match {
            builder = builder.header(header::IF_NONE_MATCH, value);
        }
        tiles_app(state)
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    /// z5..=z13 are now served (Tiers A/B below) -- only outside that plus
    /// z14 should a request fall through to 204. Picks z3 (below Tier A) as
    /// the "genuinely out of range" case; z16 (above z14) would do equally
    /// well.
    #[tokio::test]
    async fn out_of_range_zoom_returns_no_content() {
        let state = make_state("");
        let response = request_tile(state, 3, 1, 1).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    /// The out-of-range-zoom 204 is a property of the binary's dispatch
    /// table, not of the data underneath it, so it gets a long, fixed
    /// max-age (`http_cache::OUT_OF_RANGE_ZOOM_MAX_AGE_SECONDS`) independent
    /// of the configured tile/aggregate TTLs.
    #[tokio::test]
    async fn out_of_range_zoom_caches_for_a_day() {
        let state = make_state("");
        let response = request_tile(state, 3, 1, 1).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(response.headers()["cache-control"], "public, max-age=86400");
    }

    /// z10 sits inside Tier A (aggregated bins) and used to return 204 before
    /// Tiers A/B existed (see `out_of_range_zoom_returns_no_content` above,
    /// which used to cover z10 itself). An empty DB is enough here since
    /// `ST_AsMVT` emits a layer header even with zero features, same as the
    /// z14 `empty_tile_returns_ok_not_500` case below.
    #[tokio::test]
    async fn z10_aggregated_tile_returns_ok_not_no_content() {
        let state = make_state("");
        let response = request_tile(state, 10, 1, 1).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// z11 is the top of Tier A (grid features stay legible one zoom further
    /// in than the original z5..=z10 cutoff, before handing off to Tier B's
    /// unbinned points at z12) -- pins the moved boundary directly, since
    /// nothing else here would catch a dispatcher off-by-one at z11 itself.
    #[tokio::test]
    async fn z11_aggregated_tile_returns_ok_not_no_content() {
        let state = make_state("");
        let response = request_tile(state, 11, 1, 1).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8_lossy(&bytes);
        assert!(body.contains("agg_cells"), "missing agg_cells layer");
    }

    /// Tier A (z5..=z11): seeds one row per source table at z14 cell
    /// (8000, 4900) and requests the z6 tile that bit-shift-contains it
    /// (31, 19 -- verified: 8000 >> 8 = 31, 4900 >> 8 = 19, matching
    /// shift = 14 - 6 = 8). Asserts both MVT layers this tier emits are
    /// present, sharing the one aggregate (`agg_bin_ctes`), plus at least
    /// one of the integer count attributes they both carry.
    #[tokio::test]
    async fn aggregated_tile_exposes_agg_cells_and_agg_points_layers() {
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 8000, 4900);
        let (mid_lon, mid_lat) = ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0);
        let seed = format!(
            "INSERT INTO prg_unmatched (lokalny_id, numer_porzadkowy, miejscowosc, geom, cell_x, cell_y, computed_at) VALUES
                 ('a1', '12', 'Warszawa', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());
             INSERT INTO bdot10k_unmatched (LOKALNYID, geom, cell_x, cell_y, computed_at) VALUES
                 ('b1', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());
             INSERT INTO egib_unmatched (id_budynku, geom, cell_x, cell_y, computed_at) VALUES
                 ('e1', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());"
        );
        let state = make_state(&seed);
        let response = request_tile(state, 6, 31, 19).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8_lossy(&bytes);
        assert!(body.contains("agg_cells"), "missing agg_cells layer");
        assert!(body.contains("agg_points"), "missing agg_points layer");
        for attr in ["n_bdot10k", "n_egib", "n_prg", "n_total"] {
            assert!(body.contains(attr), "missing {attr} count attribute");
        }
        for attr in ["t_bdot10k", "t_egib", "t_prg", "t_total"] {
            assert!(body.contains(attr), "missing {attr} total attribute");
        }
        for attr in ["r_bdot10k", "r_egib", "r_prg", "r_total"] {
            assert!(body.contains(attr), "missing {attr} ratio attribute");
        }
    }

    /// The payoff of carrying denominators at all: a cell whose government
    /// objects are *all* matched has a `cell_totals` row and no unmatched rows,
    /// and must still produce a bin. Without this the unmatched tables alone
    /// cannot distinguish a finished area from one holding no government data
    /// -- both are simply absent -- and the map can never render "done".
    #[tokio::test]
    async fn a_bin_with_only_totals_and_no_unmatched_rows_still_renders() {
        let seed = "INSERT INTO cell_totals (source, cell_x, cell_y, total) VALUES
                        ('bdot10k', 8000, 4900, 42);";
        let state = make_state(seed);
        let response = request_tile(state, 6, 31, 19).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(
            !bytes.is_empty(),
            "a fully-matched cell must still produce a tile, not an empty one"
        );
        let body = String::from_utf8_lossy(&bytes);
        assert!(body.contains("agg_cells"), "missing agg_cells layer");
    }

    /// `ratio_sql`'s four cases, evaluated as SQL rather than inferred from
    /// the encoded tile (MVT attribute *values* are protobuf-encoded, so the
    /// string matching the tile tests above use can only see attribute names).
    /// The unknown case is the one that matters most: it is what any database
    /// whose `cell_totals` has not been built yet will hit on every bin.
    #[test]
    fn ratio_handles_missing_denominators_full_matches_and_overflow() {
        let conn = crate::db::init_db(
            std::path::Path::new(":memory:"),
            &["INSTALL spatial".to_string(), "LOAD spatial".to_string()],
            None,
        )
        .unwrap();
        // (case, numerator, denominator), ordered by `case` so the expected
        // vector below reads in the same order as the comments.
        let sql = format!(
            "SELECT {} FROM (VALUES
                 (1, 0, 5),   -- every object matched -> 0.0
                 (2, 3, 12),  -- a quarter still missing -> 0.25
                 (3, 7, 7),   -- nothing matched yet -> 1.0
                 (4, 9, 4),   -- numerator > denominator, clamped -> 1.0
                 (5, 7, 0)    -- no denominator at all -> the sentinel
             ) v(case_no, n, t) ORDER BY case_no",
            ratio_sql("n", "t", "r")
        );
        let mut stmt = conn.prepare(&sql).unwrap();
        let got: Vec<f64> = stmt
            .query_map([], |r| r.get::<_, f64>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(got, vec![0.0, 0.25, 1.0, 1.0, RATIO_UNKNOWN]);
    }

    /// Tier B (z12..=z13): same seeded cell as the aggregated-tile test
    /// above, requested at the z12 tile that contains it (2000, 1225 --
    /// 8000 >> 2 = 2000, 4900 >> 2 = 1225, shift = 14 - 12 = 2). Asserts the
    /// `points` layer and all three `source` values are present.
    #[tokio::test]
    async fn points_tile_at_z12_exposes_points_layer() {
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 8000, 4900);
        let (mid_lon, mid_lat) = ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0);
        let seed = format!(
            "INSERT INTO prg_unmatched (lokalny_id, numer_porzadkowy, miejscowosc, geom, cell_x, cell_y, computed_at) VALUES
                 ('a1', '12', 'Warszawa', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());
             INSERT INTO bdot10k_unmatched (LOKALNYID, geom, cell_x, cell_y, computed_at) VALUES
                 ('b1', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());
             INSERT INTO egib_unmatched (id_budynku, geom, cell_x, cell_y, computed_at) VALUES
                 ('e1', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());"
        );
        let state = make_state(&seed);
        let response = request_tile(state, 12, 2000, 1225).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8_lossy(&bytes);
        assert!(body.contains("points"), "missing points layer");
        for source in ["bdot10k", "egib", "prg"] {
            assert!(body.contains(source), "missing source={source} feature");
        }
    }

    /// Regression test for the two binder errors this fix addresses:
    /// ST_AsMVTGeom needing BOX_2D (not GEOMETRY), and the ambiguous `geom`
    /// column reference in BUILDINGS_MVT_SQL's UNION ALL branches. Both bugs
    /// broke every z=14 request at bind time, regardless of row content, so
    /// a completely empty DB is enough to catch them: before the fix this
    /// returned 500, not 200.
    #[tokio::test]
    async fn empty_tile_returns_ok_not_500() {
        let state = make_state("");
        let response = request_tile(state, 14, 8000, 4900).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            "application/vnd.mapbox-vector-tile"
        );
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(
            !bytes.is_empty(),
            "ST_AsMVT emits a layer header even with zero features"
        );
    }

    /// z14 is the finest zoom -- the one `match_refresh` keeps freshest -- so
    /// it gets `tile_max_age_seconds`, the shorter of the two configured TTLs.
    #[tokio::test]
    async fn z14_tile_carries_the_configured_tile_cache_control() {
        let state = make_state("");
        let response = request_tile(state, 14, 8000, 4900).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["cache-control"], "public, max-age=60");
    }

    /// z5..=z13 (both the aggregated-bin tier and the unbinned-points tier)
    /// share `agg_tile_max_age_seconds` -- checked at one representative zoom
    /// from each tier so a regression in either dispatch branch is caught.
    #[tokio::test]
    async fn agg_and_points_tiles_carry_the_configured_aggregate_cache_control() {
        let state = make_state("");
        let response = request_tile(state, 6, 31, 19).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["cache-control"], "public, max-age=300");

        let state = make_state("");
        let response = request_tile(state, 12, 2000, 1225).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["cache-control"], "public, max-age=300");
    }

    /// A failed tile query must come back header-less from `finish_tile_response`
    /// so the API-default `SetResponseHeaderLayer` in `build_router` stamps
    /// `no-store` on it -- caching a 500 would turn a transient DB hiccup into
    /// an outage that outlives the hiccup. `DROP`s a table `BUILDINGS_MVT_SQL`
    /// reads so the z14 query fails at execution time.
    #[tokio::test]
    async fn failed_tile_query_is_not_cached() {
        let state = make_state("DROP TABLE bdot10k_buildings;");
        let response = request_tile(state, 14, 8000, 4900).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(response.headers()["cache-control"], "no-store");
        // `z14_tile_version` itself would have succeeded here (it never
        // reads `bdot10k_buildings`) -- the point of this assertion is that
        // `finish_tile_response`'s error branch discards the etag it was
        // handed rather than stamping one on a 500, same reasoning as
        // `no-store` above.
        assert!(
            response.headers().get(header::ETAG).is_none(),
            "a failed tile query must not carry an ETag"
        );
    }

    #[tokio::test]
    async fn tile_with_matching_data_returns_features_from_all_three_sources() {
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 8000, 4900);
        let (mid_lon, mid_lat) = ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0);
        let seed = format!(
            "INSERT INTO prg_unmatched (lokalny_id, numer_porzadkowy, miejscowosc, geom, cell_x, cell_y, computed_at) VALUES
                 ('a1', '12', 'Warszawa', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());
             INSERT INTO bdot10k_unmatched (LOKALNYID, geom, cell_x, cell_y, computed_at) VALUES
                 ('b1', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());
             INSERT INTO egib_unmatched (id_budynku, geom, cell_x, cell_y, computed_at) VALUES
                 ('e1', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());
             INSERT INTO prg_addresses (lokalny_id, numer_porzadkowy, miejscowosc, geom) VALUES
                 ('a1', '12', 'Warszawa', ST_Point({mid_lon}, {mid_lat}));
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('b1', ST_Point({mid_lon}, {mid_lat}));
             INSERT INTO egib_buildings (id_budynku, geom) VALUES
                 ('e1', ST_Point({mid_lon}, {mid_lat}));"
        );
        let state = make_state(&seed);
        let response = request_tile(state, 14, 8000, 4900).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8_lossy(&bytes);
        assert!(body.contains("addresses"), "missing addresses layer");
        assert!(body.contains("buildings"), "missing buildings layer");
        assert!(
            body.contains("addresses_all"),
            "missing addresses_all layer"
        );
        assert!(
            body.contains("buildings_all"),
            "missing buildings_all layer"
        );
        assert!(body.contains("bdot10k"), "missing bdot10k source tag");
        assert!(body.contains("egib"), "missing egib source tag");
    }

    #[tokio::test]
    async fn tile_with_no_nearby_data_returns_ok_with_no_matching_features() {
        // Data exists, but nowhere near the requested tile.
        let seed = "INSERT INTO prg_unmatched (lokalny_id, numer_porzadkowy, miejscowosc, geom, cell_x, cell_y, computed_at) VALUES
            ('a1', '12', 'Warszawa', ST_Point(0.0, 0.0), 0, 0, now());";
        let state = make_state(seed);
        let response = request_tile(state, 14, 8000, 4900).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8_lossy(&bytes);
        assert!(
            !body.contains("Warszawa"),
            "out-of-tile address must not appear"
        );
    }

    /// Every new raw/carried column added across all four layers, non-NULL on
    /// at least one seeded row, plus the resolved OSM tag columns -- the
    /// regression this guards is a `DATE`/`TIMESTAMP`/`TINYINT`/`SMALLINT`
    /// column slipping into an MVT projection uncast (`ST_AsMVT` only accepts
    /// `VARCHAR, FLOAT, DOUBLE, INTEGER, BIGINT, BOOLEAN`, verified against a
    /// live DuckDB+spatial instance) -- a 500 here means a cast was missed.
    #[tokio::test]
    async fn tile_exposes_new_attributes_without_binder_errors() {
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 8000, 4900);
        let (mid_lon, mid_lat) = ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0);
        let seed = format!(
            "INSERT INTO prg_unmatched
                 (lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
                  teryt_miejscowosc, wazny_od_lub_data_nadania, teryt_gmina, gmina,
                  geom, cell_x, cell_y, computed_at)
             VALUES
                 ('a1', '12', 'Marszalkowska', 'Warszawa', '00-590', '0918123',
                  DATE '2012-04-27', '146501', 'Warszawa',
                  ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());
             INSERT INTO prg_addresses
                 (lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
                  wazny_od_lub_data_nadania, teryt_gmina, gmina, geom)
             VALUES
                 ('a1', '12', 'Marszalkowska', 'Warszawa', '00-590', DATE '2012-04-27',
                  '146501', 'Warszawa', ST_Point({mid_lon}, {mid_lat}));
             INSERT INTO bdot10k_unmatched
                 (LOKALNYID, geom, cell_x, cell_y, computed_at,
                  funkcja_szczegolowa, funkcja_ogolna, liczba_kondygnacji,
                  KATEGORIAISTNIENIA, NAZWA, FSBUD, INFORMACJADODATKOWA, KODKST,
                  ZRODLODANYCHGEOMETRYCZNYCH)
             VALUES
                 ('b1', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now(),
                  'budynek wielorodzinny', 'budynki mieszkalne', 4,
                  'eksploatowany', 'Blok Slonecznik', 'budynek wielorodzinny', 'info', 110,
                  'EGiB');
             INSERT INTO bdot10k_buildings
                 (LOKALNYID, geom, centroid, PRZEWAZAJACAFUNKCJABUDYNKU, FUNKCJAOGOLNABUDYNKU,
                  LICZBAKONDYGNACJI, KATEGORIAISTNIENIA, NAZWA, FSBUD, INFORMACJADODATKOWA,
                  KODKST, ZRODLODANYCHGEOMETRYCZNYCH)
             VALUES
                 ('b1', ST_Point({mid_lon}, {mid_lat}), ST_Point({mid_lon}, {mid_lat}),
                  'budynek wielorodzinny', 'budynki mieszkalne', 4,
                  'eksploatowany', 'Blok Slonecznik', 'budynek wielorodzinny', 'info', 110,
                  'EGiB');
             INSERT INTO egib_unmatched
                 (id_budynku, geom, cell_x, cell_y, computed_at,
                  rodzaj_kod, kondygnacje_nadziemne, kondygnacje_podziemne, rodzaj)
             VALUES
                 ('e1', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now(), 'm', 3, 1, 'm');
             INSERT INTO egib_buildings
                 (id_budynku, geom, centroid, rodzaj_kod, kondygnacje_nadziemne,
                  kondygnacje_podziemne, rodzaj)
             VALUES
                 ('e1', ST_Point({mid_lon}, {mid_lat}), ST_Point({mid_lon}, {mid_lat}),
                  'm', 3, 1, 'm');"
        );
        let state = make_state(&seed);
        let response = request_tile(state, 14, 8000, 4900).await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a cast slipping (DATE/TIMESTAMP -> VARCHAR, TINYINT/SMALLINT -> INTEGER) \
             would surface here as a 500, not a panic"
        );
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8_lossy(&bytes);
        for expected in [
            "ulica",
            "kod_pocztowy",
            "wazny_od_lub_data_nadania",
            "2012-04-27",
            "teryt_gmina",
            "146501",
            "gmina",
            "addr:housenumber",
            "addr:street",
            "addr:postcode",
            "source:addr",
            "Marszalkowska",
            "KATEGORIAISTNIENIA",
            "NAZWA",
            "FSBUD",
            "INFORMACJADODATKOWA",
            "ZRODLODANYCHGEOMETRYCZNYCH",
            "Blok Slonecznik",
            "eksploatowany",
            "rodzaj",
            "tags",
            "building=yes",
        ] {
            assert!(
                body.contains(expected),
                "expected tile bytes to contain {expected:?}"
            );
        }
    }

    /// --- `reported` on the two `*_all` layers ---------------------------
    ///
    /// These read the flag per feature through `all_buildings_sql`/
    /// `all_addresses_sql`'s projection seam rather than searching the tile
    /// bytes, because searching them cannot answer the question that matters.
    /// `ST_AsMVT` writes one key dictionary per layer and omits only the
    /// per-feature *value* for a NULL, so `reported` appears in the bytes as
    /// soon as the column exists — verified: the byte-search version of this
    /// test passed identically with nothing reported at all. Per-row it is
    /// exact.
    ///
    /// Seeds two bdot10k buildings, one egib building and two PRG addresses in
    /// the requested tile; `reports` is spliced in so each case varies only the
    /// report rows.
    fn seed_with_reports(reports: &str) -> String {
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 8000, 4900);
        let (mid_lon, mid_lat) = ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0);
        format!(
            "INSERT INTO bdot10k_buildings (PRZESTRZENNAZW, LOKALNYID, geom, centroid) VALUES
                 ('PL.PZGiK.BDOT10k.1234', 'b1', ST_Point({mid_lon}, {mid_lat}),
                  ST_Point({mid_lon}, {mid_lat})),
                 ('PL.PZGiK.BDOT10k.1234', 'b2', ST_Point({mid_lon}, {mid_lat}),
                  ST_Point({mid_lon}, {mid_lat}));
             INSERT INTO egib_buildings (id_budynku, geom, centroid) VALUES
                 ('e1', ST_Point({mid_lon}, {mid_lat}), ST_Point({mid_lon}, {mid_lat}));
             INSERT INTO prg_addresses (lokalny_id, numer_porzadkowy, miejscowosc, geom) VALUES
                 ('a1', '12', 'Warszawa', ST_Point({mid_lon}, {mid_lat})),
                 ('a2', '14', 'Warszawa', ST_Point({mid_lon}, {mid_lat}));
             {reports}"
        )
    }

    /// `(id, reported)` for every feature the two `*_all` layers would emit
    /// for tile 14/8000/4900, with the same bbox parameters `serve_tile`
    /// binds. `reported IS NOT NULL` rather than the raw value because the
    /// projection is `CASE WHEN ... THEN TRUE END` — the frontend sees
    /// "attribute present", so that is what these assert on.
    fn reported_flags(state: &AppState, sql: &str, params_groups: usize) -> Vec<(String, bool)> {
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 8000, 4900);
        let bbox = [min_lon, min_lat, max_lon, max_lat];
        let flat: Vec<f64> = (0..params_groups).flat_map(|_| bbox).collect();
        let params: Vec<&dyn duckdb::ToSql> =
            flat.iter().map(|v| v as &dyn duckdb::ToSql).collect();
        let conn = state.pool.get().unwrap();
        let mut stmt = conn.prepare(sql).unwrap();
        let mut rows = stmt.query(params.as_slice()).unwrap();
        let mut out = Vec::new();
        while let Some(row) = rows.next().unwrap() {
            out.push((
                row.get::<_, String>(0).unwrap(),
                row.get::<_, bool>(1).unwrap(),
            ));
        }
        out.sort();
        out
    }

    fn building_flags(state: &AppState) -> Vec<(String, bool)> {
        // bbox CTE, bdot10k_candidates, egib_candidates.
        reported_flags(
            state,
            &all_buildings_sql("t.id, t.reported IS NOT NULL AS reported"),
            3,
        )
    }

    fn address_flags(state: &AppState) -> Vec<(String, bool)> {
        // bbox CTE, candidates.
        reported_flags(
            state,
            &all_addresses_sql("t.lokalny_id, t.reported IS NOT NULL AS reported"),
            2,
        )
    }

    /// An active report is the one status the `*_all` layers could not show:
    /// the veto removes its object from `<source>_unmatched`, so it drops out
    /// of the `buildings`/`addresses` layers and reappears only here, where it
    /// was previously indistinguishable from a matched record.
    #[tokio::test]
    async fn an_active_report_flags_its_own_object_and_only_that_one() {
        let state = make_state(&seed_with_reports(
            "INSERT INTO object_reports
                 (report_id, source, record_key, signature, note, reported_at,
                  cell_x, cell_y, status, resolved_at)
             VALUES
                 (1, 'bdot10k', ['PL.PZGiK.BDOT10k.1234', 'b1'], NULL, NULL, now(),
                  8000, 4900, 'active', NULL),
                 (2, 'prg', ['a1'], NULL, NULL, now(), 8000, 4900, 'active', NULL);",
        ));
        assert_eq!(
            building_flags(&state),
            vec![
                ("b1".to_string(), true),
                ("b2".to_string(), false),
                ("e1".to_string(), false),
            ],
            "only the reported bdot10k building may carry the flag — not its \
             unreported neighbour, and not the egib building"
        );
        assert_eq!(
            address_flags(&state),
            vec![("a1".to_string(), true), ("a2".to_string(), false)]
        );
    }

    /// With nothing reported, no feature carries the attribute at all — which
    /// is what keeps the flag free for the overwhelming majority of tiles
    /// (`ST_AsMVT` omits a NULL attribute per feature).
    #[tokio::test]
    async fn nothing_reported_flags_nothing() {
        let state = make_state(&seed_with_reports(""));
        assert!(building_flags(&state).iter().all(|(_, r)| !r));
        assert!(address_flags(&state).iter().all(|(_, r)| !r));
    }

    /// Both halves of `rule::reported_sql`'s correlation, in one negative.
    /// The first row is a *revoked* report on a real key (so the
    /// `status = 'active'` filter is what must reject it); the second is an
    /// active report whose `PRZESTRZENNAZW` differs while `LOKALNYID` matches
    /// (so the composite-key equality is what must reject it) — BDOT10k's key
    /// is the pair, and correlating on `LOKALNYID` alone would flag a building
    /// nobody reported. The third is an active report on the *other* registry's
    /// id, pinning the `r.source` filter: `id_budynku` and `LOKALNYID` are
    /// separate namespaces that can collide.
    #[tokio::test]
    async fn a_revoked_report_a_partial_key_or_another_registry_flags_nothing() {
        let state = make_state(&seed_with_reports(
            "INSERT INTO object_reports
                 (report_id, source, record_key, signature, note, reported_at,
                  cell_x, cell_y, status, resolved_at)
             VALUES
                 (1, 'bdot10k', ['PL.PZGiK.BDOT10k.1234', 'b1'], NULL, NULL, now(),
                  8000, 4900, 'revoked', now()),
                 (2, 'bdot10k', ['PL.PZGiK.BDOT10k.9999', 'b2'], NULL, NULL, now(),
                  8000, 4900, 'active', NULL),
                 (3, 'egib', ['b1'], NULL, NULL, now(), 8000, 4900, 'active', NULL);",
        ));
        assert!(
            building_flags(&state).iter().all(|(_, r)| !r),
            "a revoked report, one keyed on a different PRZESTRZENNAZW, and an \
             egib report carrying a bdot10k id must all flag nothing"
        );
    }

    /// `addr:city`/`addr:place` are mutually exclusive per address
    /// (`package::address_tags`'s Rust-side `if let Some(street) ... else`
    /// re-expressed in SQL): a street gets `addr:city`, no street gets
    /// `addr:place`. Both keys should appear somewhere in the tile (one row
    /// uses each), proving the CASE/WHEN split executes -- this is a
    /// presence check on the shared MVT key dictionary, matching this test
    /// module's existing string-search style, not a per-feature decode.
    #[tokio::test]
    async fn addr_city_and_addr_place_both_appear_for_their_respective_rows() {
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 8000, 4900);
        let (mid_lon, mid_lat) = ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0);
        let seed = format!(
            "INSERT INTO prg_unmatched
                 (lokalny_id, numer_porzadkowy, ulica, miejscowosc, geom, cell_x, cell_y, computed_at)
             VALUES
                 ('with_street', '1', 'Polna', 'Warszawa', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now()),
                 ('no_street', '2', NULL, 'Zubrow', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());"
        );
        let state = make_state(&seed);
        let response = request_tile(state, 14, 8000, 4900).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8_lossy(&bytes);
        assert!(
            body.contains("addr:city"),
            "the row with a street should contribute an addr:city key"
        );
        assert!(
            body.contains("addr:place"),
            "the row without a street should contribute an addr:place key"
        );
        assert!(body.contains("Warszawa") && body.contains("Zubrow"));
    }

    /// Settlement-scoped `street_name_mappings` rows win over the raw PRG
    /// name, mirroring `package::tests::settlement_mapping_row_beats_the_global_row`.
    /// An empty mapping table (the default here) degrades to serving `ulica`
    /// verbatim as `addr:street`, which every other test in this module
    /// already relies on implicitly.
    #[tokio::test]
    async fn resolved_street_name_prefers_the_settlement_mapping_row() {
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 8000, 4900);
        let (mid_lon, mid_lat) = ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0);
        let seed = format!(
            "INSERT INTO prg_unmatched
                 (lokalny_id, numer_porzadkowy, ulica, miejscowosc, teryt_miejscowosc, geom, cell_x, cell_y, computed_at)
             VALUES
                 ('a1', '1', 'Kwiatowa', 'Zubrow', '0188009', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());
             INSERT INTO street_name_mappings (teryt_simc_code, prg_street_name, osm_street_name) VALUES
                 ('0188009', 'kwiatowa', 'Settlement Kwiatowa'),
                 (NULL, 'kwiatowa', 'Global Kwiatowa');"
        );
        let state = make_state(&seed);
        let response = request_tile(state, 14, 8000, 4900).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8_lossy(&bytes);
        assert!(
            body.contains("Settlement Kwiatowa"),
            "settlement-scoped mapping row must win over the global row"
        );
        assert!(
            !body.contains("Global Kwiatowa"),
            "the global row must not be used when a settlement-scoped row matches"
        );
    }

    // --- z14 ETag / conditional 304 -----------------------------------------

    #[tokio::test]
    async fn z14_tile_carries_a_weak_etag_alongside_cache_control() {
        let state = make_state("");
        let response = request_tile(state, 14, 8000, 4900).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["cache-control"], "public, max-age=60");
        let etag = response
            .headers()
            .get(header::ETAG)
            .expect("z14 must carry an ETag")
            .to_str()
            .unwrap();
        assert!(
            etag.starts_with("W/\""),
            "z14's ETag must be weak, got {etag}"
        );
    }

    #[tokio::test]
    async fn matching_if_none_match_returns_304_with_no_body_or_content_headers() {
        let state = make_state("");
        let first = request_tile(state.clone(), 14, 8000, 4900).await;
        assert_eq!(first.status(), StatusCode::OK);
        let etag = first
            .headers()
            .get(header::ETAG)
            .expect("z14 must carry an ETag")
            .clone();

        let second =
            request_tile_with_if_none_match(state, 14, 8000, 4900, Some(etag.to_str().unwrap()))
                .await;
        assert_eq!(second.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(
            second.headers().get(header::ETAG),
            Some(&etag),
            "the 304 must echo back the same ETag"
        );
        assert_eq!(second.headers()["cache-control"], "public, max-age=60");
        assert!(
            second.headers().get(header::CONTENT_TYPE).is_none(),
            "RFC 9110 SS15.4.5: a 304 must not carry Content-Type"
        );
        // Not asserted absent: `http_cache::not_modified` itself never sets
        // Content-Length (pinned directly in `http_cache`'s own unit test,
        // which builds the Response without going through a Router), but
        // axum 0.8's `Route`/`RouteFuture` unconditionally stamps
        // `Content-Length` on any top-level response whose body reports an
        // exact size via `size_hint()` -- verified by reading
        // axum-0.8.9/src/routing/route.rs's `set_content_length`, and by
        // confirming `/health`'s 200 (an unrelated, pre-existing route)
        // carries the same "0" here. RFC 9110 SS15.4.5 only forbids actual
        // *content* on a 304, not this header, so a "0" is informational and
        // harmless, not a spec violation -- there is no way to suppress it
        // through axum's public routing API short of nesting every route
        // (which would also strip it from genuine 200 tile bodies).
        assert_eq!(second.headers()["content-length"], "0");
        let bytes = to_bytes(second.into_body(), 1024).await.unwrap();
        assert!(bytes.is_empty(), "a 304 must have an empty body");
    }

    #[tokio::test]
    async fn stale_if_none_match_returns_200_with_the_full_tile() {
        let state = make_state("");
        let response = request_tile_with_if_none_match(
            state,
            14,
            8000,
            4900,
            Some("W/\"not-the-real-version\""),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::ETAG).is_some());
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(
            !bytes.is_empty(),
            "ST_AsMVT emits a layer header even with zero features"
        );
    }

    /// Pins the ETag's three sources of movement end to end through HTTP --
    /// serving_version's own unit tests already pin each source against
    /// `z14_tile_version` directly; this test is what proves `serve_tile`
    /// actually wires that primitive into a response header a client would
    /// see change.
    #[tokio::test]
    async fn etag_moves_after_a_cell_recompute_an_epoch_bump_and_a_neighbouring_cell_recompute() {
        let state = make_state("");
        let etag_a = request_tile(state.clone(), 14, 8000, 4900)
            .await
            .headers()
            .get(header::ETAG)
            .expect("z14 must carry an ETag")
            .clone();

        // 1. A recompute of the tile's own cell (8000, 4900).
        {
            let conn = state.pool.get().unwrap();
            conn.execute_batch(
                "INSERT INTO bdot10k_unmatched (LOKALNYID, geom, cell_x, cell_y, computed_at) VALUES
                     ('b1', ST_Point(21.0, 52.0), 8000, 4900, now());",
            )
            .unwrap();
        }
        let etag_b = request_tile(state.clone(), 14, 8000, 4900)
            .await
            .headers()
            .get(header::ETAG)
            .unwrap()
            .clone();
        assert_ne!(etag_a, etag_b, "a cell recompute must move the tile's ETag");

        // 2. An epoch bump -- covers the sources with no per-cell signal at
        // all (street/building-type mapping reloads, a `compare full`
        // rebuild; see serving_version's module doc) -- with the cell itself
        // left untouched.
        {
            let conn = state.pool.get().unwrap();
            serving_version::bump_serving_epoch(&conn).unwrap();
        }
        let etag_c = request_tile(state.clone(), 14, 8000, 4900)
            .await
            .headers()
            .get(header::ETAG)
            .unwrap()
            .clone();
        assert_ne!(etag_b, etag_c, "an epoch bump must move the tile's ETag");

        // 3. A recompute of a NEIGHBOURING cell, one z14 cell away -- still
        // inside the 3x3 ring `z14_tile_version` reads (see its doc comment)
        // -- must move it too.
        {
            let conn = state.pool.get().unwrap();
            conn.execute_batch(
                "INSERT INTO bdot10k_unmatched (LOKALNYID, geom, cell_x, cell_y, computed_at) VALUES
                     ('b2', ST_Point(21.0, 52.0), 8001, 4901, now());",
            )
            .unwrap();
        }
        let etag_d = request_tile(state, 14, 8000, 4900)
            .await
            .headers()
            .get(header::ETAG)
            .unwrap()
            .clone();
        assert_ne!(
            etag_c, etag_d,
            "a neighbouring cell's recompute must move the tile's ETag"
        );
    }

    #[tokio::test]
    async fn agg_and_points_tiles_carry_no_etag() {
        for (z, x, y) in [(6, 31, 19), (12, 2000, 1225)] {
            let state = make_state("");
            let response = request_tile(state, z, x, y).await;
            assert_eq!(response.status(), StatusCode::OK, "z{z}");
            assert!(
                response.headers().get(header::ETAG).is_none(),
                "z{z} must not carry an ETag -- see serve_tile's comment on why"
            );
        }
    }

    #[tokio::test]
    async fn out_of_range_zoom_carries_no_etag() {
        let state = make_state("");
        let response = request_tile(state, 3, 1, 1).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(response.headers().get(header::ETAG).is_none());
    }

    // --- Phase 5: z14 tile_cache wiring -------------------------------------

    /// The basic payoff: a second request for the exact same tile, with
    /// nothing having changed in between, is served from `tile_cache`
    /// instead of running the four MVT queries again. Asserted on the
    /// cache's own hit/miss counters, not on timing -- timing would be racy
    /// and wouldn't actually prove which code path served the response.
    /// A cache hit and a cache miss must be indistinguishable in the response
    /// they produce, and the empty-tile case is where that is easiest to get
    /// wrong: `finish_tile_response` answers 204 for empty bytes, so
    /// `z14_tile_response` (the path a cache hit takes, and the only z14 path
    /// since the two were unified) has to as well. An earlier draft returned
    /// a bare 200 with an empty body here, which would have made a z14 tile
    /// over open country -- the common case across most of Poland -- flip
    /// between 204 and 200 depending purely on whether it happened to be
    /// resident in the cache.
    #[test]
    fn z14_response_shaping_is_identical_for_empty_and_cached_empty_tiles() {
        let cache_header = HeaderValue::from_static("public, max-age=60");
        let etag = HeaderValue::from_static("W/\"1-0-0.0-0.0-0.0\"");

        let fresh = finish_tile_response(
            Ok(Ok(Vec::new())),
            14,
            8000,
            4900,
            cache_header.clone(),
            Some(etag.clone()),
        );
        let cached = z14_tile_response(Bytes::new(), cache_header.clone(), Some(etag.clone()));

        assert_eq!(fresh.status(), StatusCode::NO_CONTENT);
        assert_eq!(cached.status(), fresh.status());
        assert_eq!(cached.headers().get(header::ETAG), Some(&etag));
        assert_eq!(
            cached.headers().get(header::CACHE_CONTROL),
            Some(&cache_header)
        );
        assert_eq!(
            cached.headers().get(header::CONTENT_TYPE),
            None,
            "a 204 must not claim a body's content type"
        );
    }

    #[tokio::test]
    async fn repeat_request_for_the_same_tile_is_served_from_the_cache() {
        let state = make_state("");

        let first = request_tile(state.clone(), 14, 8000, 4900).await;
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(
            state.tile_cache.misses(),
            1,
            "first request must miss and populate the cache"
        );
        assert_eq!(state.tile_cache.hits(), 0);

        let second = request_tile(state.clone(), 14, 8000, 4900).await;
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(
            state.tile_cache.hits(),
            1,
            "repeat request must be served from the cache"
        );
        assert_eq!(
            state.tile_cache.misses(),
            1,
            "no additional miss on the repeat request"
        );
    }

    /// A recompute moves `z14_tile_version`'s output (pinned end-to-end
    /// already by `etag_moves_after_a_cell_recompute_...` above), which must
    /// also mean the cache entry keyed on the OLD version is bypassed: the
    /// request after a recompute must miss the cache and return the fresh
    /// bytes, not the stale cached ones.
    #[tokio::test]
    async fn cache_is_bypassed_after_a_version_move() {
        let state = make_state("");

        let first = request_tile(state.clone(), 14, 8000, 4900).await;
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(state.tile_cache.misses(), 1);

        // Inserted at the tile's own midpoint (not an arbitrary coordinate)
        // so the row is actually selected by BUILDINGS_MVT_SQL's spatial
        // `ST_Intersects` filter, not just tagged with the matching
        // cell_x/cell_y -- matching the pattern every other content-bearing
        // seed in this file already uses.
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 8000, 4900);
        let (mid_lon, mid_lat) = ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0);
        {
            let conn = state.pool.get().unwrap();
            conn.execute_batch(&format!(
                "INSERT INTO bdot10k_unmatched (LOKALNYID, geom, cell_x, cell_y, computed_at) VALUES
                     ('b1', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());"
            ))
            .unwrap();
        }

        let second = request_tile(state.clone(), 14, 8000, 4900).await;
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(
            state.tile_cache.misses(),
            2,
            "a version move must miss the cache rather than serve the stale entry"
        );
        assert_eq!(
            state.tile_cache.hits(),
            0,
            "the recompute must never have been served from the cache"
        );

        let bytes = to_bytes(second.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8_lossy(&bytes);
        assert!(
            body.contains("bdot10k"),
            "the recomputed row must appear in the fresh (uncached) tile"
        );
    }

    /// A `304` short-circuits before the cache is even consulted (see
    /// `serve_tile`'s early return on an `If-None-Match` match) -- it must
    /// move neither counter, not just "not register a miss".
    #[tokio::test]
    async fn a_304_registers_neither_a_cache_hit_nor_a_miss() {
        let state = make_state("");

        let first = request_tile(state.clone(), 14, 8000, 4900).await;
        let etag = first
            .headers()
            .get(header::ETAG)
            .expect("z14 must carry an ETag")
            .clone();
        let hits_after_first = state.tile_cache.hits();
        let misses_after_first = state.tile_cache.misses();

        let second = request_tile_with_if_none_match(
            state.clone(),
            14,
            8000,
            4900,
            Some(etag.to_str().unwrap()),
        )
        .await;
        assert_eq!(second.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(
            state.tile_cache.hits(),
            hits_after_first,
            "a 304 must not count as a cache hit"
        );
        assert_eq!(
            state.tile_cache.misses(),
            misses_after_first,
            "a 304 must not count as a cache miss either -- it returns before \
             the cache is ever consulted"
        );
    }

    /// z5..=z13 and out-of-range zooms never touch `tile_cache` at all --
    /// it's a z14-only cache (see the module doc on `tile_cache`).
    #[tokio::test]
    async fn cache_is_never_consulted_below_z14() {
        let state = make_state("");
        for (z, x, y) in [(3, 1, 1), (6, 31, 19), (12, 2000, 1225)] {
            let response = request_tile(state.clone(), z, x, y).await;
            assert!(
                response.status() == StatusCode::OK || response.status() == StatusCode::NO_CONTENT,
                "z{z}: unexpected status {}",
                response.status()
            );
            assert_eq!(state.tile_cache.hits(), 0, "z{z} must never hit tile_cache");
            assert_eq!(
                state.tile_cache.misses(),
                0,
                "z{z} must never miss tile_cache either -- it must not be consulted at all"
            );
        }
    }

    /// A cache hit must be indistinguishable from a fresh response on the
    /// headers a client actually keys revalidation off of.
    #[tokio::test]
    async fn cached_response_carries_the_same_etag_and_cache_control_as_a_fresh_one() {
        let state = make_state("");

        let first = request_tile(state.clone(), 14, 8000, 4900).await;
        let first_etag = first.headers().get(header::ETAG).cloned();
        let first_cache_control = first.headers().get(header::CACHE_CONTROL).cloned();

        let second = request_tile(state.clone(), 14, 8000, 4900).await;
        assert_eq!(
            state.tile_cache.hits(),
            1,
            "this test must actually exercise the cache-hit path"
        );
        assert_eq!(second.headers().get(header::ETAG).cloned(), first_etag);
        assert_eq!(
            second.headers().get(header::CACHE_CONTROL).cloned(),
            first_cache_control
        );
    }
}
