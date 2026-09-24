use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use anyhow::Context;

use duckdb::Connection;

use super::AppState;
use super::http_cache;
use super::tile_dirty;
use super::tile_store::{self, TileBody, TileKey};
use std::collections::HashMap;
use std::sync::LazyLock;

use super::package::{
    ADJACENCY_READ_BUFFER_DEG, BDOT10K_ADJACENCY_KEY, EGIB_ADJACENCY_KEY, SOURCE_BUILDING_BDOT10K,
    SOURCE_BUILDING_EGIB,
};
use crate::compare::rule::reported_sql;
use crate::dataset::{BDOT10K, EGIB, PRG};
use crate::mappings::street_names::{resolved_street_expr_sql, resolved_street_join_sql};
use crate::tile_math::{CHANGE_CELL_ZOOM, tile_to_bbox};

/// Bumped by hand whenever the MVT SQL in this module changes shape -- a new
/// attribute column, a renamed one, a different geometry simplification.
///
/// It lives here, next to the SQL it describes, so the hand-bump rule sits
/// where the thing it guards is edited. `tile_store` stamps it into every
/// stored value and treats a mismatch as a miss, so a binary that changes what
/// a tile *contains* cannot serve tiles rendered by its predecessor. Nothing
/// about the underlying rows changed, so no invalidation would fire on its own
/// -- without this, every warmed tile would keep its old shape until something
/// unrelated dirtied its cell.
///
/// 2: `BUILDINGS_MVT_SQL` gained the `source:building` and `building:levels`
/// attributes, completing the buildings tag preview.
/// 3: bodies are stored gzipped with a content-hash `ETag`; z12..=z13 joined
/// the persisted tiers.
/// 4: `buildings` and `buildings_all` gained `approx_area_m2`, which the
/// frontend's minimum-area filter reads.
pub const TILE_FORMAT_VERSION: u32 = 4;

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
// resolving tags for `*_unmatched` rows. Both previews are the *complete*
// tag set `/package` would export, not a subset: buildings reach that through
// two columns rather than one, for the reason on `BUILDINGS_MVT_SQL` below.
// Address tag resolution
// (`resolved` CTE below) mirrors `package::unmatched_addresses`'s street-name
// join; building tag resolution (`bdot10k_final`/`egib_final` below) mirrors
// `package::unmatched_bdot10k_buildings`/`unmatched_egib_buildings`'s
// adjacency + mapping-table LATERAL join, reusing the same
// `ADJACENCY_READ_BUFFER_DEG`/`BDOT10K_ADJACENCY_KEY`/`EGIB_ADJACENCY_KEY`
// constants rather than re-typing them. Both omit `package`'s polygon-clip
// predicate (`ST_Intersects(_, ST_GeomFromGeoJSON(?))`) since tiles are
// always rectangular, unlike a `/package` request area.
/// The sort that makes a rendered tile a deterministic function of its rows,
/// and hence the content-hash `ETag` stable across re-renders of unchanged
/// data. `alias` names the subquery holding the projected `geom`; `row` is the
/// expression `ST_AsMVT` is being handed.
///
/// Without it `ST_AsMVT` emits features in whatever order the scan produced,
/// which under a parallel scan is not stable. Measured on the real database
/// before this existed: a z12 tile re-rendered while `match_refresh` was
/// disabled -- so `*_unmatched` provably could not have changed -- came back
/// the same length with 331 bytes different and a new `ETag`, costing every
/// revalidating client a 200 where a 304 was owed. With it,
/// `tile_render_determinism_and_thread_count` reports 0 differing renders out
/// of 8 on every tier at 1, 2, 4 and default thread counts.
///
/// Three properties of the key, each measured rather than assumed:
///
/// - **It sorts on the tile-space geometry `ST_AsMVTGeom` produced**, not on
///   the source geometry. That is the value actually encoded, so it is the
///   coarsest key that can still separate distinguishable features, and the
///   projection is already computed.
/// - **The whole row is the final tiebreak**, which makes this a total order
///   over *distinguishable* rows by construction: two rows tying on it are
///   equal structs, hence byte-identical features, so their relative order
///   cannot matter. Explicit identity columns would be provably total for
///   three of the four z14 layers and only *probably* so for `buildings_all`,
///   whose `LOKALNYID` is unique by measurement rather than by schema -- and
///   the row costs nothing measurable: 41.97 ms against 41.65 ms for a
///   `(y, x, source)` key over a 253-tile z13 batch, for identical bytes.
/// - **Y before X**, because feature order is a compression choice as well as
///   a determinism one, and tiles are stored and served **gzipped**. Over a
///   253-tile z13 batch: unordered 235,166 B gzipped, `(y, x)` 223,342 B
///   (-5.0%), `(x, y)` 240,082 B (+2.1%). On the densest z12 tile `(y, x)` is
///   -11.7% against unordered, while `ST_Hilbert` -- the obvious
///   spatial-locality answer -- is +6.9%. On the four densest z14 tiles the
///   win is -1.2% to -5.1%.
///
/// **Do not assume the *raw* tile size is order-invariant.** It is at z12/z13,
/// measured byte-identical across five orderings of three tiles, because
/// geometry deltas never cross a feature boundary -- each feature restarts the
/// cursor at (0,0) -- and that layer's only attribute, `source`, has three
/// distinct values. The z14 layers do move: -0.9% to -1.7% raw on the four
/// densest tiles, which is the attribute dictionary rather than the geometry.
/// Values are interned once per layer and referenced by varint index, so which
/// ones land in the low, one-byte indices depends on which feature is written
/// first.
///
/// **The cost, so the trade is visible.** The sort is paid per query, so it
/// lands hardest where a tile is cheap: a one-object z14 tile goes from
/// ~12.7 ms to ~17.1 ms (+33%, four sorted aggregates over almost nothing),
/// while the densest z14 tiles pay +6.6% (253-296 ms -> 270-316 ms). The
/// points tier pays +22% batched (0.13 -> 0.16 ms/tile over 253 z13 tiles)
/// and +36% at a batch of one (3.26 -> 4.43 ms). Against that: a re-render no
/// longer invalidates every client's copy, and the store shrinks.
fn deterministic_mvt_order_sql(alias: &str, row: &str) -> String {
    format!(" ORDER BY ST_YMin({alias}.geom), ST_XMin({alias}.geom), {row}")
}

/// `ST_AsMVT` over a whole-tile subquery aliased `t`, deterministically
/// ordered. The one home for the four z14 layers' and the aggregate tier's
/// aggregate call; `points_mvt_sql` builds its own because it groups.
fn as_mvt_sql(layer: &str) -> String {
    format!(
        "ST_AsMVT(t, '{layer}', 4096, 'geom'{order}) AS mvt",
        order = deterministic_mvt_order_sql("t", "t")
    )
}

/// How a z14 layer query is bound to the tiles it renders.
///
/// **The calculation is identical in both arms** -- same predicates, same
/// projection text, same constants. The only thing that changes is that the
/// tile stops being a constant and becomes a join variable. Each builder
/// therefore writes its column list exactly once and lets this decide the
/// wrapper around it.
///
/// Why both arms exist rather than one: a batch of one pays for machinery it
/// does not need, and the `buildings` layer is where that lands. Its adjacency
/// count is per (building, tile), so `Batched` has to fan the neighbour set out
/// to tiles and group by `(tx, ty, rid)`; at a single tile that key has one
/// distinct value, DuckDB plans a `HASH_JOIN` on it, and the whole
/// `pkg x nb` cross product materialises before the `ST_Intersects` filter runs
/// -- measured 71.9 -> 202.2 ms on the densest tile. `Single` keeps the request
/// path off that. See `docs/superpowers/plans/2026-09-06-z14-tile-batching.md`.
#[derive(Clone, Copy)]
enum TileScope<'a> {
    /// One tile, envelope bound as `?` parameters. **The text is constant**,
    /// which is what lets `query_mvt_layer` cache the prepared statement --
    /// see its doc comment.
    Single,
    /// A block of tiles, interpolated as a `VALUES` list; one output row per
    /// requested tile. Tile coordinates are `u32`s off a store key, so there is
    /// nothing to inject, and a `VALUES` list cannot be a bound parameter.
    Batched(&'a [(u32, u32)]),
}

impl TileScope<'_> {
    /// The `WITH` header: the one-row `bbox` CTE, or the tile list and the
    /// per-tile envelopes derived from it.
    ///
    /// `Batched` builds each envelope from [`tile_to_bbox`] and carries it in
    /// the `VALUES` list rather than recomputing a Web Mercator inverse in SQL
    /// the way `agg_bin_ctes` has to -- one home for tile -> bbox. `{:?}` on an
    /// `f64` is the shortest representation that round-trips, so the envelope
    /// DuckDB parses is the one `tile_to_bbox` computed.
    fn header(&self) -> String {
        match self {
            TileScope::Single => {
                "WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(?, ?, ?, ?)) AS geom),".to_string()
            }
            TileScope::Batched(tiles) => {
                let values = tiles
                    .iter()
                    .map(|&(x, y)| {
                        let (a, b, c, d) = tile_to_bbox(CHANGE_CELL_ZOOM, x, y);
                        format!("({x}, {y}, {a:?}, {b:?}, {c:?}, {d:?})")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "WITH tiles(tx, ty, x0, y0, x1, y1) AS (VALUES {values}),
    env AS (
        SELECT tx, ty, ST_MakeEnvelope(x0, y0, x1, y1) AS poly,
               ST_Extent(ST_MakeEnvelope(x0, y0, x1, y1)) AS box
        FROM tiles
    ),"
                )
            }
        }
    }

    /// The envelope a source scan filters on, optionally grown by `pad` degrees
    /// (the adjacency reads).
    ///
    /// **This is what keeps the scan on the RTREE index and it cannot be
    /// dropped in favour of the join below.** The index needs a *constant*
    /// bound; a join condition yields `Bounds: deferred (from join filter)`,
    /// which prunes nothing. Measured on `buildings_all` over 16 tiles:
    /// removing it took `RTREE_IN` x2 to x0 and 309.6 ms to 26,970.7 ms.
    fn scan_envelope(&self, pad: f64) -> String {
        match self {
            TileScope::Single => "ST_MakeEnvelope(?, ?, ?, ?)".to_string(),
            TileScope::Batched(tiles) => {
                let boxes = tiles
                    .iter()
                    .map(|&(x, y)| tile_to_bbox(CHANGE_CELL_ZOOM, x, y));
                let (mut a, mut b, mut c, mut d) = (f64::MAX, f64::MAX, f64::MIN, f64::MIN);
                for (x0, y0, x1, y1) in boxes {
                    a = a.min(x0);
                    b = b.min(y0);
                    c = c.max(x1);
                    d = d.max(y1);
                }
                let (a, b, c, d) = (a - pad, b - pad, c + pad, d + pad);
                format!("ST_MakeEnvelope({a:?}, {b:?}, {c:?}, {d:?})")
            }
        }
    }

    /// The envelope `ST_AsMVTGeom` clips each feature to.
    fn box_expr(&self) -> &'static str {
        match self {
            TileScope::Single => "bbox.geom",
            TileScope::Batched(_) => "e.box",
        }
    }

    /// Everything after the CTE chain: the projection `cols` plus the
    /// aggregate that consumes it.
    ///
    /// `Batched` reaches the projection through `CROSS JOIN LATERAL (SELECT
    /// ...)`, which is a projection alias rather than a correlated table scan.
    /// That is what lets `cols` be the *same text* in both arms: `t` ends up
    /// holding exactly the listed columns, so the grouping keys never leak into
    /// the layer's attribute dictionary and no struct has to be spelled out
    /// beside the column list. `LEFT JOIN` plus `FILTER` is what keeps a tile
    /// holding nothing from vanishing: a bare `GROUP BY` emits no row for it,
    /// and a missing row is not an empty tile -- see the no-204 rule in
    /// CLAUDE.md.
    ///
    /// **The `proj` CTE is not optional, and inlining it crashes the database.**
    /// `ST_AsMVT` hits an `INTERNAL Error: Buffer overflow` -- a DuckDB
    /// assertion failure, which poisons the whole connection so every later
    /// query returns `FATAL ... database has been invalidated` -- whenever
    /// `ST_AsMVTGeom` is evaluated *in the aggregate's own input expression*
    /// and a group has no surviving row. Materialising the projected geometry
    /// one step earlier, so the aggregate reads a plain column, avoids it.
    /// Reduced to:
    ///
    /// ```sql
    /// -- crashes:
    /// SELECT ST_AsMVT(struct_pack(geom := ST_AsMVTGeom(r.geom, ...)), ...)
    ///   FROM env e LEFT JOIN rows r ON ... GROUP BY e.tx
    /// -- works:
    /// WITH proj AS (SELECT ST_AsMVTGeom(r.geom, ...) AS geom FROM rows r JOIN env e ON ...)
    /// SELECT ST_AsMVT(struct_pack(geom := g.geom), ...)
    ///   FROM env e LEFT JOIN proj g ON ... GROUP BY e.tx
    /// ```
    ///
    /// `points_mvt_sql` is safe only because it happens to be written the
    /// second way. Pinned by `a_z14_batch_answers_for_tiles_holding_nothing`,
    /// which is worth keeping precisely because the failure is a hard crash
    /// rather than wrong bytes.
    fn body(
        &self,
        layer: &str,
        projection: &str,
        from_sql: &str,
        on_sql: &str,
        cols: &str,
    ) -> String {
        match self {
            TileScope::Single => format!(
                "    SELECT {projection}
    FROM (
        SELECT {cols}
        FROM {from_sql}, bbox
    ) t
    WHERE t.geom IS NOT NULL
"
            ),
            TileScope::Batched(_) => format!(
                ",
    proj AS (
        SELECT e.tx, e.ty, t AS f
        FROM {from_sql} JOIN env e ON {on_sql}
        CROSS JOIN LATERAL (
            SELECT {cols}
        ) t
    )
    SELECT e.tx, e.ty, ST_AsMVT(g.f, '{layer}', 4096, 'geom'{order})
               FILTER (WHERE g.f.geom IS NOT NULL) AS mvt
    FROM env e LEFT JOIN proj g ON g.tx = e.tx AND g.ty = e.ty
    GROUP BY e.tx, e.ty
",
                order = deterministic_mvt_order_sql("g.f", "g.f")
            ),
        }
    }
}

/// Built once at first use rather than declared `const`, because the street
/// resolution comes from `mappings::street_names`'s shared builders — the same
/// text `/package` and both compare paths use. The resulting SQL is
/// semantically identical to the hand-written chain this replaced, so
/// [`TILE_FORMAT_VERSION`] must **not** move for it: nothing about what a tile
/// contains changed.
static ADDRESSES_MVT_SQL: LazyLock<String> =
    LazyLock::new(|| addresses_sql(&as_mvt_sql("addresses"), TileScope::Single));

fn addresses_sql(projection: &str, scope: TileScope) -> String {
    let cols = format!(
        "ST_AsMVTGeom(resolved.geom, {box}, 4096, 256, true) AS geom,
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
               'gugik.gov.pl' AS \"source:addr\"",
        box = scope.box_expr(),
    );
    format!(
        "
    {header}
    candidates AS MATERIALIZED (
        SELECT a.geom, a.lokalny_id, a.numer_porzadkowy, a.ulica, a.miejscowosc,
               a.kod_pocztowy, a.teryt_miejscowosc, a.wazny_od_lub_data_nadania,
               a.teryt_gmina, a.gmina
        FROM prg_unmatched a
        WHERE ST_Intersects(a.geom, {scan})
    ),
    resolved AS (
        SELECT candidates.*,
               NULLIF(trim({resolved_street}), '') AS resolved_street
        FROM candidates
        {mapping_joins}
    )
{body}",
        header = scope.header(),
        scan = scope.scan_envelope(0.0),
        resolved_street = resolved_street_expr_sql("candidates"),
        mapping_joins = resolved_street_join_sql("candidates"),
        body = scope.body(
            "addresses",
            projection,
            "resolved",
            "ST_Intersects(resolved.geom, e.poly)",
            &cols
        ),
    )
}

/// `approx_area_m2` for the two building layers: the footprint in whole
/// square metres, from `dataset::area_m2_sql` -- the same expression
/// `dataset::filter_undersized_geometry` drops rows by, so "under 1 m^2" means
/// one thing at load and on the map. It exists for the frontend's
/// minimum-area filter, which is a plain `>=` against this attribute: MapLibre
/// has no expression for a polygon's area, so the number has to ride in the
/// tile.
///
/// Whole metres rather than the raw `DOUBLE`, for tile size: `ST_AsMVT`
/// interns each distinct attribute value once per layer, so a float that is
/// unique per building would add one dictionary entry per feature, where
/// integers repeat across a dense tile. The slider's thresholds are whole
/// metres anyway. "Approximate" is in the name because it is a latitude-scaled
/// planar area (within 0.05% of the ellipsoidal one, see `area_m2_sql`) and
/// then rounded -- not a surveyed figure, and not to be exported as one.
///
/// Computed at render time rather than stored: it is ~0.13 us per feature
/// (~1 ms on the densest `buildings_all` tile), and a stored column would
/// need a re-import plus a `compare` to appear -- there is no migration path.
fn approx_area_m2_sql(geom: &str) -> String {
    format!("round({})::INTEGER", crate::dataset::area_m2_sql(geom))
}

/// Like `ADDRESSES_MVT_SQL`, built at first use rather than declared `const`,
/// so the `source:building` values come from `package`'s own constants.
///
/// **The OSM tag preview is assembled from two columns, not one**, and the
/// split mirrors where each half comes from in `/package`. `tags` is the
/// `k=v;k=v` string the building-type mapping table produced -- arbitrary
/// keys, which is exactly why it cannot be columnar the way
/// `ADDRESSES_MVT_SQL`'s fixed `addr:*` set is. `source:building` and
/// `building:levels` are the two tags `/package` adds *outside* that string
/// (`package::building_tags` inserts the first unconditionally,
/// `package::with_building_levels` the second when the storey count is >= 1),
/// so they are projected as their own literally-named columns and the
/// frontend merges the two halves back together (`describeFeature` in
/// `web/app.js`). Two consequences worth keeping:
///
/// - `building:levels` is `NULL` below 1, and `ST_AsMVT` drops NULL
///   attributes, so "no storeys to report" reaches the popup as an absent tag
///   -- the same shape `with_building_levels` produces by not inserting.
/// - An explicit column outranks the same key inside `tags`, which is what
///   the frontend's merge implements, because `with_building_levels` inserts
///   into the `BTreeMap` *after* the mapping string was parsed into it.
static BUILDINGS_MVT_SQL: LazyLock<String> =
    LazyLock::new(|| buildings_sql(&as_mvt_sql("buildings"), TileScope::Single));

/// The `projection` seam is `all_buildings_sql`'s, and exists for the same
/// reason: a test asserting on a per-feature attribute has to read it per row,
/// because `ST_AsMVT` writes one key dictionary per layer, so an attribute's
/// *key* appears in the tile bytes as soon as the column exists -- even with
/// every row's value NULL.
fn buildings_sql(projection: &str, scope: TileScope) -> String {
    // The adjacency count is per (building, tile), not per building: each `nb`
    // read is bounded by *that tile's* buffered envelope, so the same building
    // can legitimately land on different `max_neighbours` verdicts -- and so a
    // different `tags` string -- in two tiles that both draw it. `Batched`
    // therefore fans both sides out to tiles and groups by (tx, ty, rid).
    // Widening the neighbour read to the whole batch instead would read as an
    // obvious tidy-up and would silently change tags across the country.
    let batched = matches!(scope, TileScope::Batched(_));
    let tile_cols = if batched { "e.tx, e.ty, " } else { "" };
    let pkg_join = if batched {
        "\n        JOIN env e ON ST_Intersects(b.geom, e.poly)"
    } else {
        ""
    };
    let nb_join = if batched {
        format!(
            "\n        JOIN env e ON ST_Intersects(nb.geom, ST_Expand(e.poly, {ADJACENCY_READ_BUFFER_DEG:?}))"
        )
    } else {
        String::new()
    };
    let cnt_keys = if batched {
        "p.tx, p.ty, p.rid"
    } else {
        "p.rid"
    };
    let cnt_tile_on = if batched {
        "p.tx = nb.tx AND p.ty = nb.ty\n         AND "
    } else {
        ""
    };
    let cnt_using = if batched { "(tx, ty, rid)" } else { "(rid)" };
    let final_tile_cols = if batched { "pkg.tx, pkg.ty, " } else { "" };

    let cols = format!(
        "ST_AsMVTGeom(u.geom, {box}, 4096, 256, true) AS geom, u.id, u.source,
               -- Identity, not display: the other half of BDOT10k's composite
               -- key, so the frontend's report action can send a complete
               -- record key. NULL for egib, whose id_budynku is the whole key.
               u.PRZESTRZENNAZW,
               u.approx_area_m2,
               u.funkcja_szczegolowa, u.funkcja_ogolna, u.levels_above_ground,
               u.KATEGORIAISTNIENIA, u.NAZWA, u.FSBUD, u.INFORMACJADODATKOWA,
               u.KODKST, u.ZRODLODANYCHGEOMETRYCZNYCH,
               u.kondygnacje_podziemne, u.rodzaj, u.tags,
               -- Last, and after `tags`, only as a readability convention:
               -- the frontend merges the two halves by key rather than by
               -- arrival order, so this is not load-bearing.
               u.\"source:building\", u.\"building:levels\"",
        box = scope.box_expr(),
    );
    format!(
        "
    {header}
    bdot10k_pkg AS MATERIALIZED (
        SELECT {tile_cols}b.rowid AS rid, b.LOKALNYID AS id, b.PRZESTRZENNAZW, b.geom,
               {area} AS approx_area_m2,
               ST_X(ST_Centroid(b.geom)) AS cx, ST_Y(ST_Centroid(b.geom)) AS cy,
               b.funkcja_szczegolowa, b.funkcja_ogolna, b.liczba_kondygnacji,
               b.KATEGORIAISTNIENIA, b.NAZWA, b.FSBUD, b.INFORMACJADODATKOWA,
               b.KODKST, b.ZRODLODANYCHGEOMETRYCZNYCH
        FROM bdot10k_unmatched b{pkg_join}
        WHERE ST_Intersects(b.geom, {scan})
    ),
    bdot10k_nb AS MATERIALIZED (
        SELECT {tile_cols}nb.geom, ST_X(nb.centroid) AS cx, ST_Y(nb.centroid) AS cy
        FROM bdot10k_buildings nb{nb_join}
        WHERE ST_Intersects(nb.geom, {scan_buf})
          AND lower(trim(nb.PRZEWAZAJACAFUNKCJABUDYNKU)) = {bdot10k_key}
    ),
    bdot10k_cnt AS (
        SELECT {cnt_keys}, count(*) AS neighbours
        FROM bdot10k_pkg p JOIN bdot10k_nb nb
          ON {cnt_tile_on}(p.cx <> nb.cx OR p.cy <> nb.cy) AND ST_Intersects(p.geom, nb.geom)
        GROUP BY {cnt_keys}
    ),
    bdot10k_final AS (
        SELECT {final_tile_cols}pkg.geom, 'bdot10k' AS source, pkg.id, pkg.PRZESTRZENNAZW,
               pkg.approx_area_m2,
               pkg.funkcja_szczegolowa, pkg.funkcja_ogolna,
               pkg.liczba_kondygnacji::INTEGER AS levels_above_ground,
               pkg.KATEGORIAISTNIENIA, pkg.NAZWA, pkg.FSBUD, pkg.INFORMACJADODATKOWA,
               pkg.KODKST::INTEGER AS KODKST, pkg.ZRODLODANYCHGEOMETRYCZNYCH,
               NULL::INTEGER AS kondygnacje_podziemne, NULL::VARCHAR AS rodzaj,
               COALESCE(t.tags, 'building=yes') AS tags,
               '{bdot10k_source}' AS \"source:building\",
               CASE WHEN pkg.liczba_kondygnacji >= 1
                    THEN pkg.liczba_kondygnacji::VARCHAR END AS \"building:levels\"
        FROM bdot10k_pkg pkg
        LEFT JOIN bdot10k_cnt cnt USING {cnt_using}
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
        SELECT {tile_cols}b.rowid AS rid, b.id_budynku AS id, b.geom,
               {area} AS approx_area_m2,
               ST_X(ST_Centroid(b.geom)) AS cx, ST_Y(ST_Centroid(b.geom)) AS cy,
               b.rodzaj_kod, b.kondygnacje_nadziemne, b.kondygnacje_podziemne, b.rodzaj
        FROM egib_unmatched b{pkg_join}
        WHERE ST_Intersects(b.geom, {scan})
    ),
    egib_nb AS MATERIALIZED (
        SELECT {tile_cols}nb.geom, ST_X(nb.centroid) AS cx, ST_Y(nb.centroid) AS cy
        FROM egib_buildings nb{nb_join}
        WHERE ST_Intersects(nb.geom, {scan_buf})
          AND nb.rodzaj_kod = {egib_key}
    ),
    egib_cnt AS (
        SELECT {cnt_keys}, count(*) AS neighbours
        FROM egib_pkg p JOIN egib_nb nb
          ON {cnt_tile_on}(p.cx <> nb.cx OR p.cy <> nb.cy) AND ST_Intersects(p.geom, nb.geom)
        GROUP BY {cnt_keys}
    ),
    egib_final AS (
        SELECT {final_tile_cols}pkg.geom, 'egib' AS source, pkg.id, NULL::VARCHAR AS PRZESTRZENNAZW,
               pkg.approx_area_m2,
               NULL::VARCHAR AS funkcja_szczegolowa, NULL::VARCHAR AS funkcja_ogolna,
               pkg.kondygnacje_nadziemne AS levels_above_ground,
               NULL::VARCHAR AS KATEGORIAISTNIENIA, NULL::VARCHAR AS NAZWA,
               NULL::VARCHAR AS FSBUD, NULL::VARCHAR AS INFORMACJADODATKOWA,
               NULL::INTEGER AS KODKST, NULL::VARCHAR AS ZRODLODANYCHGEOMETRYCZNYCH,
               pkg.kondygnacje_podziemne, pkg.rodzaj,
               COALESCE(t.tags, 'building=yes') AS tags,
               '{egib_source}' AS \"source:building\",
               CASE WHEN pkg.kondygnacje_nadziemne >= 1
                    THEN pkg.kondygnacje_nadziemne::VARCHAR END AS \"building:levels\"
        FROM egib_pkg pkg
        LEFT JOIN egib_cnt cnt USING {cnt_using}
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
{body}",
        header = scope.header(),
        scan = scope.scan_envelope(0.0),
        scan_buf = scope.scan_envelope(ADJACENCY_READ_BUFFER_DEG),
        area = approx_area_m2_sql("b.geom"),
        bdot10k_key = if batched {
            format!("'{BDOT10K_ADJACENCY_KEY}'")
        } else {
            "?".to_string()
        },
        egib_key = if batched {
            format!("'{EGIB_ADJACENCY_KEY}'")
        } else {
            "?".to_string()
        },
        bdot10k_source = SOURCE_BUILDING_BDOT10K,
        egib_source = SOURCE_BUILDING_EGIB,
        body = scope.body(
            "buildings",
            projection,
            "(SELECT * FROM bdot10k_final UNION ALL SELECT * FROM egib_final) u",
            "u.tx = e.tx AND u.ty = e.ty",
            &cols
        ),
    )
}

static ALL_ADDRESSES_MVT_SQL: LazyLock<String> =
    LazyLock::new(|| all_addresses_sql(&as_mvt_sql("addresses_all"), TileScope::Single));

fn all_addresses_sql(projection: &str, scope: TileScope) -> String {
    let cols = format!(
        "ST_AsMVTGeom(a.geom, {box}, 4096, 256, true) AS geom,
               a.lokalny_id,
               a.numer_porzadkowy,
               a.ulica,
               a.miejscowosc,
               a.kod_pocztowy,
               a.wazny_od_lub_data_nadania::VARCHAR AS wazny_od_lub_data_nadania,
               a.teryt_gmina,
               a.gmina,
               CASE WHEN {reported} THEN TRUE END AS reported",
        box = scope.box_expr(),
        reported = reported_sql(&PRG, "a"),
    );
    format!(
        "
    {header}
    candidates AS MATERIALIZED (
        SELECT a.geom, a.lokalny_id, a.numer_porzadkowy, a.ulica, a.miejscowosc,
               a.kod_pocztowy, a.wazny_od_lub_data_nadania, a.teryt_gmina, a.gmina
        FROM prg_addresses a
        WHERE ST_Intersects(a.geom, {scan})
    )
{body}",
        header = scope.header(),
        scan = scope.scan_envelope(0.0),
        body = scope.body(
            "addresses_all",
            projection,
            "candidates a",
            "ST_Intersects(a.geom, e.poly)",
            &cols
        ),
    )
}

static ALL_BUILDINGS_MVT_SQL: LazyLock<String> =
    LazyLock::new(|| all_buildings_sql(&as_mvt_sql("buildings_all"), TileScope::Single));

fn all_buildings_sql(projection: &str, scope: TileScope) -> String {
    let cols = format!(
        "ST_AsMVTGeom(raw.geom, {box}, 4096, 256, true) AS geom,
               raw.id, raw.source, raw.approx_area_m2,
               raw.PRZEWAZAJACAFUNKCJABUDYNKU, raw.FUNKCJAOGOLNABUDYNKU,
               raw.levels_above_ground, raw.KATEGORIAISTNIENIA, raw.NAZWA, raw.FSBUD,
               raw.INFORMACJADODATKOWA, raw.KODKST, raw.ZRODLODANYCHGEOMETRYCZNYCH,
               raw.kondygnacje_podziemne, raw.rodzaj, raw.reported",
        box = scope.box_expr(),
    );
    let raw = format!(
        "(
            SELECT b.geom, b.LOKALNYID AS id, 'bdot10k' AS source,
                   {area} AS approx_area_m2,
                   b.PRZEWAZAJACAFUNKCJABUDYNKU, b.FUNKCJAOGOLNABUDYNKU,
                   b.LICZBAKONDYGNACJI::INTEGER AS levels_above_ground,
                   b.KATEGORIAISTNIENIA, b.NAZWA, b.FSBUD, b.INFORMACJADODATKOWA,
                   b.KODKST::INTEGER AS KODKST, b.ZRODLODANYCHGEOMETRYCZNYCH,
                   NULL::INTEGER AS kondygnacje_podziemne, NULL::VARCHAR AS rodzaj,
                   CASE WHEN {reported_bdot10k} THEN TRUE END AS reported
            FROM bdot10k_candidates b
            UNION ALL
            SELECT b.geom, b.id_budynku AS id, 'egib' AS source,
                   {area} AS approx_area_m2,
                   NULL::VARCHAR AS PRZEWAZAJACAFUNKCJABUDYNKU, NULL::VARCHAR AS FUNKCJAOGOLNABUDYNKU,
                   b.kondygnacje_nadziemne AS levels_above_ground,
                   NULL::VARCHAR AS KATEGORIAISTNIENIA, NULL::VARCHAR AS NAZWA, NULL::VARCHAR AS FSBUD,
                   NULL::VARCHAR AS INFORMACJADODATKOWA, NULL::INTEGER AS KODKST,
                   NULL::VARCHAR AS ZRODLODANYCHGEOMETRYCZNYCH,
                   b.kondygnacje_podziemne, b.rodzaj,
                   CASE WHEN {reported_egib} THEN TRUE END AS reported
            FROM egib_candidates b
        ) raw",
        reported_bdot10k = reported_sql(&BDOT10K, "b"),
        reported_egib = reported_sql(&EGIB, "b"),
        area = approx_area_m2_sql("b.geom"),
    );
    format!(
        "
    {header}
    bdot10k_candidates AS MATERIALIZED (
        SELECT b.geom, b.PRZESTRZENNAZW, b.LOKALNYID, b.PRZEWAZAJACAFUNKCJABUDYNKU,
               b.FUNKCJAOGOLNABUDYNKU, b.LICZBAKONDYGNACJI, b.KATEGORIAISTNIENIA,
               b.NAZWA, b.FSBUD, b.INFORMACJADODATKOWA, b.KODKST,
               b.ZRODLODANYCHGEOMETRYCZNYCH
        FROM bdot10k_buildings b
        WHERE ST_Intersects(b.geom, {scan})
    ),
    egib_candidates AS MATERIALIZED (
        SELECT b.geom, b.id_budynku, b.kondygnacje_nadziemne, b.kondygnacje_podziemne,
               b.rodzaj
        FROM egib_buildings b
        WHERE ST_Intersects(b.geom, {scan})
    )
{body}",
        header = scope.header(),
        scan = scope.scan_envelope(0.0),
        body = scope.body(
            "buildings_all",
            projection,
            &raw,
            "ST_Intersects(raw.geom, e.poly)",
            &cols
        ),
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
// `agg_bin_ctes` builds the bins/geo CTE chain and `agg_cells_sql` is its only
// consumer. It once fed a second `agg_points` layer -- the same aggregate as
// one point per bin -- kept so the frontend could switch between grid, circle
// and heatmap styles with no backend change. The frontend settled on the grid
// and stopped reading it, at which point every z5..z11 tile was evaluating this
// whole chain twice and throwing half the result away, so the layer is gone.
//
// `geo` also carries a per-source "last changed at" timestamp, LEFT JOINed in
// from `dataset_change_areas` -- see the `changes` CTE for why it is a join
// rather than a fifth union branch.
fn agg_bin_ctes(shift: u32, n: u32, max_age_days: u64) -> String {
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
        -- Per-bin \"when did this source last publish a change here\", as Unix
        -- seconds, for the frontend's recently-updated overlay. LEFT JOINed
        -- below rather than unioned into `bins`, for two reasons.
        --
        -- A union branch *creates* bins. A cell present here and in none of the
        -- four branches above would become an agg_cells feature with n_* = 0
        -- and t_* = 0, so `ratio_sql` returns RATIO_UNKNOWN and the frontend
        -- paints a cell that was not in the grid a moment ago in the \"no
        -- denominator\" colour. cell_totals gets to be a union branch precisely
        -- because a totals row *is* a denominator; a change area is not, and
        -- the mismatch is real rather than hypothetical -- compare::totals
        -- deletes and re-inserts per cell, so a cell that just lost its last
        -- object has a change area and no totals row, as does any cell between
        -- a refresh committing its change areas and the drain recomputing
        -- totals. And the union is positional (`count(*), 0, 0, 0, 0, 0`), so
        -- three more columns would mean editing all four branches in lockstep
        -- and switching these two from sum() to max().
        --
        -- The detected_at bound is the only pruning available: the table has no
        -- index, and insert_change_areas writes via GROUP BY cell_x, cell_y
        -- under preserve_insertion_order = false, so cell order is arbitrary.
        -- detected_at *is* non-decreasing in physical order (rows appended per
        -- refresh, now() being transaction-start-scoped), so row-group zonemaps
        -- prune on it and nothing else. Consequence to design around: every
        -- z5..z11 tile scans every change row inside the window nationwide, so
        -- per-tile cost is linear in changes.max_age_days and independent of
        -- the tile -- keep that value tight. Drop the bound and every tile
        -- full-scans a table that only grows, slowing the *existing* grid
        -- rather than just the overlay. Interpolated, not bound: it comes from
        -- config, never from the request.
        changes AS MATERIALIZED (
            SELECT cell_x >> {shift} AS bin_x, cell_y >> {shift} AS bin_y,
                   max(CASE WHEN source = 'bdot10k' THEN epoch(detected_at) END)::BIGINT AS ts_bdot10k,
                   max(CASE WHEN source = 'egib' THEN epoch(detected_at) END)::BIGINT AS ts_egib,
                   max(CASE WHEN source = 'prg' THEN epoch(detected_at) END)::BIGINT AS ts_prg
              FROM dataset_change_areas
             WHERE detected_at >= (now() - INTERVAL '{max_age_days} days')
               AND cell_x BETWEEN ? AND ? AND cell_y BETWEEN ? AND ?
             GROUP BY 1, 2
        ),
        geo AS (
            SELECT bin_x, bin_y, n_bdot10k, n_egib, n_prg,
                   (n_bdot10k + n_egib + n_prg)::INTEGER AS n_total,
                   t_bdot10k, t_egib, t_prg,
                   (t_bdot10k + t_egib + t_prg)::INTEGER AS t_total,
                   {r_bdot10k}, {r_egib}, {r_prg}, {r_total},
                   -- COALESCE, not a bare column: ST_AsMVT drops NULL
                   -- attributes entirely, so an unmatched join would leave the
                   -- key absent and the frontend's `[\"get\", \"ts_egib\"]`
                   -- comparison silently null instead of false. 0 can never
                   -- collide with a real value, which is Unix seconds.
                   --
                   -- The prefix is ts_, not t_: t_bdot10k/t_egib/t_prg above
                   -- already mean the ratio *denominators*.
                   COALESCE(c.ts_bdot10k, 0) AS ts_bdot10k,
                   COALESCE(c.ts_egib, 0) AS ts_egib,
                   COALESCE(c.ts_prg, 0) AS ts_prg,
                   bin_x / {n}.0 * 360 - 180 AS lon0,
                   (bin_x + 1) / {n}.0 * 360 - 180 AS lon1,
                   degrees(atan(sinh(pi() * (1 - 2 * bin_y / {n}.0)))) AS lat_north,
                   degrees(atan(sinh(pi() * (1 - 2 * (bin_y + 1) / {n}.0)))) AS lat_south
            FROM bins b LEFT JOIN changes c USING (bin_x, bin_y)
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
fn agg_cells_sql(shift: u32, n: u32, max_age_days: u64) -> String {
    format!(
        "{ctes}
        SELECT {mvt}
        FROM (
            SELECT ST_AsMVTGeom(
                       ST_MakeEnvelope(geo.lon0, geo.lat_south, geo.lon1, geo.lat_north),
                       bbox.geom, 4096, 256, true) AS geom,
                   geo.n_bdot10k, geo.n_egib, geo.n_prg, geo.n_total,
                   geo.t_bdot10k, geo.t_egib, geo.t_prg, geo.t_total,
                   geo.r_bdot10k, geo.r_egib, geo.r_prg, geo.r_total,
                   geo.ts_bdot10k, geo.ts_egib, geo.ts_prg
            FROM geo, bbox
        ) t
        WHERE t.geom IS NOT NULL",
        mvt = as_mvt_sql("agg_cells"),
        ctes = agg_bin_ctes(shift, n, max_age_days),
    )
}

// --- Tier B (z12..=z13): individual points ----------------------------------
//
// One feature per unmatched object -- same cell_x/cell_y BETWEEN filter as
// Tier A, just not binned. Buildings are recentred to their centroid since
// bdot10k_unmatched/egib_unmatched carry polygons; prg_unmatched's `geom` is
// already a point (PRG addresses always were).
//
// **This tier renders a batch of tiles per query, and a single request is a
// batch of one.** Everything below is one SQL text, not two -- which is only
// possible because the batching itself is free at a batch of one: 3.80 ms
// against the per-tile query's 3.79 ms on a Warsaw z13 tile, measured before
// either grew an ORDER BY. So the bulk paths need no separate copy of the MVT
// SQL, and "the MVT SQL has one home" survives the 27x speedup.

/// The row `ST_AsMVT` is handed for this layer.
///
/// `struct_pack` rather than the bare projected row, because the query groups:
/// passing `g` directly would publish the grouping keys `tx`/`ty` as feature
/// attributes on every point.
const POINTS_MVT_ROW: &str = "struct_pack(geom := g.geom, source := g.source)";

/// The z12--z13 `points` layer: one query, one row per requested tile.
///
/// Batching is worth **27x**: 253 z13 tiles around Warsaw render in 41.2 ms
/// as one query against 1121.3 ms one at a time, for identical bytes
/// (`points_batch_size_vs_render_cost`). It costs nothing at a batch of one,
/// which is why this *replaces* the per-tile query rather than sitting beside
/// it. `tiles warm` and `jobs::tile_refresh` reach it through
/// [`render_points_tiles`]; a request reaches it through [`render_tile`].
///
/// Three details carry the byte-identity, and each has a silent failure mode:
///
/// 1. **The tile list drives the group set, via `env LEFT JOIN proj`.** A bare
///    `GROUP BY tx, ty` over the rows emits *no row at all* for a requested
///    tile that holds nothing, and a missing row is not the same thing as an
///    empty tile: `ST_AsMVT` is an aggregate with no `GROUP BY` behind it in
///    the per-tile form, so it emits a layer header even over zero features
///    (23 bytes), which is what makes an in-range tile always a 200 -- see
///    `finish_tile_response`'s 204 branch, which must stay unreachable. The
///    `FILTER` is the other half: it drops the LEFT JOIN's null row so the
///    aggregate sees an empty group and produces exactly those 23 bytes.
///    Verified against the per-tile query on eight tiles, empty ones included.
/// 2. **The envelope comes from [`tile_to_bbox`], carried in the `VALUES`
///    list**, rather than being recomputed with a Web Mercator inverse in SQL
///    the way `agg_bin_ctes` has to. One home for tile -> bbox.
/// 3. **The scan is bounded by the batch's cell range and *then* joined to the
///    exact tile list.** The `BETWEEN` bounds are what let the zonemaps prune;
///    the join is what keeps a scattered batch from rendering tiles nobody
///    asked for. Callers should therefore batch spatially coherent runs --
///    both do, since their tile lists are sorted.
///
/// Tile coordinates are interpolated rather than bound: they are `u32`s off
/// the URL path or out of a store key, so there is nothing to inject, and a
/// `VALUES` list cannot be a single bound parameter anyway.
fn points_mvt_sql(z: u32, tiles: &[(u32, u32)]) -> String {
    debug_assert!(!tiles.is_empty(), "points_mvt_sql needs at least one tile");
    let cell_shift = CHANGE_CELL_ZOOM - z;
    let (min_tx, max_tx) = minmax(tiles.iter().map(|t| t.0));
    let (min_ty, max_ty) = minmax(tiles.iter().map(|t| t.1));
    let lo_x = (min_tx << cell_shift) as i32;
    let hi_x = (((max_tx + 1) << cell_shift) - 1) as i32;
    let lo_y = (min_ty << cell_shift) as i32;
    let hi_y = (((max_ty + 1) << cell_shift) - 1) as i32;

    let values = tiles
        .iter()
        .map(|&(x, y)| {
            let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(z, x, y);
            // `{:?}` on an f64 is the shortest representation that round-trips,
            // so the envelope DuckDB parses is the one `tile_to_bbox` computed.
            format!("({x}, {y}, {min_lon:?}, {min_lat:?}, {max_lon:?}, {max_lat:?})")
        })
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "
    WITH tiles(tx, ty, min_lon, min_lat, max_lon, max_lat) AS (VALUES {values}),
    env AS (
        SELECT tx, ty, ST_Extent(ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat)) AS geom
        FROM tiles
    ),
    pts AS (
        SELECT b.cell_x >> {cell_shift} AS tx, b.cell_y >> {cell_shift} AS ty,
               ST_Centroid(b.geom) AS geom, 'bdot10k' AS source
          FROM bdot10k_unmatched b
         WHERE b.cell_x BETWEEN {lo_x} AND {hi_x} AND b.cell_y BETWEEN {lo_y} AND {hi_y}
        UNION ALL
        SELECT e.cell_x >> {cell_shift}, e.cell_y >> {cell_shift},
               ST_Centroid(e.geom), 'egib'
          FROM egib_unmatched e
         WHERE e.cell_x BETWEEN {lo_x} AND {hi_x} AND e.cell_y BETWEEN {lo_y} AND {hi_y}
        UNION ALL
        SELECT a.cell_x >> {cell_shift}, a.cell_y >> {cell_shift}, a.geom, 'prg'
          FROM prg_unmatched a
         WHERE a.cell_x BETWEEN {lo_x} AND {hi_x} AND a.cell_y BETWEEN {lo_y} AND {hi_y}
    ),
    proj AS (
        SELECT e.tx, e.ty, ST_AsMVTGeom(p.geom, e.geom, 4096, 256, true) AS geom, p.source
          FROM pts p JOIN env e USING (tx, ty)
    )
    SELECT e.tx, e.ty,
           ST_AsMVT({POINTS_MVT_ROW}, 'points', 4096, 'geom'{order})
               FILTER (WHERE g.geom IS NOT NULL) AS mvt
      FROM env e LEFT JOIN proj g ON g.tx = e.tx AND g.ty = e.ty
     GROUP BY e.tx, e.ty
",
        order = deterministic_mvt_order_sql("g", POINTS_MVT_ROW),
    )
}

/// `(min, max)` of a non-empty iterator, 0/0 for an empty one.
fn minmax(it: impl Iterator<Item = u32> + Clone) -> (u32, u32) {
    (it.clone().min().unwrap_or(0), it.max().unwrap_or(0))
}

pub async fn serve_tile(
    State(state): State<AppState>,
    Path((z, x, y)): Path<(u32, u32, u32)>,
    headers: HeaderMap,
) -> Response {
    // Three tiers, three caching strategies -- see the tier documentation
    // above `agg_bin_ctes`/`points_mvt_sql`, and `tile_dirty`'s module doc for
    // why the persisted pair can be pushed and this one cannot.
    if (5..=11).contains(&z) {
        return serve_tile_agg(state, z, x, y, &headers).await;
    }
    if (tile_dirty::MIN_PERSISTED_ZOOM..=CHANGE_CELL_ZOOM).contains(&z) {
        return serve_persisted_tile(state, z, x, y, &headers).await;
    }
    // A property of the binary's zoom dispatch table, not of the data --
    // see http_cache::OUT_OF_RANGE_ZOOM_MAX_AGE_SECONDS's doc comment.
    let mut resp = StatusCode::NO_CONTENT.into_response();
    resp.headers_mut().insert(
        header::CACHE_CONTROL,
        state.cache_headers.out_of_range_zoom.clone(),
    );
    resp
}

/// The z12..=z14 path: one lookup in the persistent store, and only on a miss
/// a pool connection and a render.
///
/// The store lookup happens *before* any pool acquisition, which is the whole
/// point: a 16-tile viewport refresh that is already warm costs zero pool
/// slots and touches DuckDB not at all. A 304 likewise returns without ever
/// asking for a connection.
///
/// One function for all three persisted zooms rather than one per tier, so a
/// hit cannot differ from a miss -- nor z12 from z14 -- on status, headers, or
/// body bytes. `render_tile` is the only thing that varies by zoom.
async fn serve_persisted_tile(
    state: AppState,
    z: u32,
    x: u32,
    y: u32,
    headers: &HeaderMap,
) -> Response {
    let cache_header = if z == CHANGE_CELL_ZOOM {
        state.cache_headers.tile.clone()
    } else {
        state.cache_headers.agg_tile.clone()
    };
    let if_none_match = headers.get(header::IF_NONE_MATCH).cloned();
    let wants_gzip = http_cache::accepts_gzip(headers);

    // Served without a pool connection when the store already has it.
    if let Some(stored) = state.tile_store.get((z, x, y)) {
        if http_cache::if_none_match_matches(if_none_match.as_ref(), &stored.etag) {
            return http_cache::not_modified(http_cache::weak_etag(&stored.etag), cache_header);
        }
        return tile_body_response(stored.body, cache_header, Some(&stored.etag), wants_gzip);
    }

    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<(String, TileBody)> {
        let conn = state
            .pool
            .get()
            .context("Failed to acquire pool connection")?;
        let raw = render_tile(&conn, z, x, y)?;
        drop(conn);
        let (etag, body) = tile_store::prepare(&raw)?;
        state.tile_store.put((z, x, y), &etag, &body);
        Ok((etag, body))
    })
    .await;

    match result {
        Ok(Ok((etag, body))) => {
            // Checked after the render too: a client revalidating a tile that
            // fell out of the store (or was never in it) must still get its
            // 304 when the bytes turn out unchanged, which is exactly what a
            // content hash makes possible and the old derived version did not.
            if http_cache::if_none_match_matches(if_none_match.as_ref(), &etag) {
                return http_cache::not_modified(http_cache::weak_etag(&etag), cache_header);
            }
            tile_body_response(body, cache_header, Some(&etag), wants_gzip)
        }
        Ok(Err(e)) => finish_tile_response(Ok(Err(e)), z, x, y, cache_header, None),
        Err(e) => finish_tile_response(Err(e), z, x, y, cache_header, None),
    }
}

/// The one home for "what bytes is the tile at (z, x, y)".
///
/// Called from three places -- the serving path above, `jobs::tile_refresh`,
/// and the `tiles warm` CLI verb -- which is why it is a free function taking
/// a connection rather than living inside a request handler. A second copy of
/// this SQL for the bulk paths is exactly what "the MVT SQL has one home"
/// forbids.
pub fn render_tile(conn: &Connection, z: u32, x: u32, y: u32) -> anyhow::Result<Vec<u8>> {
    if z == CHANGE_CELL_ZOOM {
        render_z14_tile(conn, x, y)
    } else {
        render_points_tile(conn, z, x, y)
    }
}

/// Tier B (z12..=z13): one `points` layer, one feature per unmatched object.
///
/// Reads only the three `*_unmatched` tables, filtered by the z14 cell range
/// the tile covers -- never by geometry. That is what makes this tier an exact
/// function of the cells beneath it, and hence invalidatable by parent alone
/// (see `tile_dirty`).
fn render_points_tile(conn: &Connection, z: u32, x: u32, y: u32) -> anyhow::Result<Vec<u8>> {
    let rendered = render_points_tiles(conn, z, &[(x, y)])?;
    // `points_mvt_sql` drives its group set from the requested tile list, so
    // exactly one row comes back per tile -- an empty tile is a 23-byte layer
    // header, not an absent row. A missing row is therefore a bug in that
    // query, and serving it as an empty body would turn into a 204 the tier
    // must never send.
    match rendered.into_iter().next() {
        Some((_, mvt)) => Ok(mvt),
        None => anyhow::bail!("z{z}/{x}/{y}: the points query returned no row for the tile"),
    }
}

/// One rendered tile: its `(x, y)` within the batch's zoom, and the raw MVT
/// bytes -- raw, because compressing is [`tile_store::prepare`]'s job and the
/// caller decides whether the bytes are worth storing at all.
pub type RenderedTile = ((u32, u32), Vec<u8>);

/// [`render_points_tile`]'s bulk form: every tile in `tiles`, one query.
///
/// Returns one `((x, y), mvt)` per requested tile, in whatever order the group
/// operator produced -- callers index by key rather than by position. All the
/// tiles must be at the same zoom, which is how both callers group them.
///
/// See [`points_mvt_sql`] for why a batch is worth ~18x and a batch of one
/// costs nothing.
pub fn render_points_tiles(
    conn: &Connection,
    z: u32,
    tiles: &[(u32, u32)],
) -> anyhow::Result<Vec<RenderedTile>> {
    if tiles.is_empty() {
        return Ok(Vec::new());
    }
    let sql = points_mvt_sql(z, tiles);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    let mut out = Vec::with_capacity(tiles.len());
    while let Some(row) = rows.next()? {
        let tx: i32 = row.get(0)?;
        let ty: i32 = row.get(1)?;
        // NULL is not reachable -- an all-filtered group still emits the layer
        // header -- but decoding it as empty rather than failing matches
        // `query_mvt_layer`'s own NULL handling.
        let mvt: Option<Vec<u8>> = row.get(2)?;
        out.push(((tx as u32, ty as u32), mvt.unwrap_or_default()));
    }
    Ok(out)
}

/// [`render_z14_tile`]'s bulk form: every tile in `tiles`, four queries total
/// instead of four per tile.
///
/// Measured against rendering the same tiles one at a time: **4.7x** across a
/// random sample of eight z11 blocks (36.1 -> 7.7 ms/tile), and 11x on a rural
/// block where the fixed per-query cost dominates. Projected over the ~140k
/// z14 tiles the warm set covers, ~85 -> ~18 CPU-minutes.
///
/// Callers must hand it a **spatially compact** block -- `tile_warm` and
/// `jobs::tile_refresh` both group by z11 ancestor, whose batch envelope *is*
/// the z11 tile's envelope, so the source scans read exactly the area asked
/// for. A scattered list would make the scans read its bounding box instead.
///
/// The whole batch fails together: one bad geometry takes its block's tiles
/// with it rather than just its own. That is the same "leave them unwarmed /
/// stale" outcome a per-tile failure already had, only coarser, and both
/// callers treat it that way.
pub fn render_z14_tiles(
    conn: &Connection,
    tiles: &[(u32, u32)],
) -> anyhow::Result<Vec<RenderedTile>> {
    if tiles.is_empty() {
        return Ok(Vec::new());
    }
    let scope = TileScope::Batched(tiles);
    // Same four layers in the same order as `render_z14_tile`: a tile is the
    // concatenation of four single-layer tiles, since an MVT tile is just a
    // repeated `layers` field.
    let layers = [
        addresses_sql(&as_mvt_sql("addresses"), scope),
        buildings_sql(&as_mvt_sql("buildings"), scope),
        all_addresses_sql(&as_mvt_sql("addresses_all"), scope),
        all_buildings_sql(&as_mvt_sql("buildings_all"), scope),
    ];

    let mut parts: HashMap<(u32, u32), Vec<u8>> = HashMap::with_capacity(tiles.len());
    for sql in &layers {
        for (key, mvt) in query_batched_layer(conn, sql)? {
            parts.entry(key).or_default().extend_from_slice(&mvt);
        }
    }
    Ok(tiles
        .iter()
        .filter_map(|key| parts.remove(key).map(|mvt| (*key, mvt)))
        .collect())
}

/// One batched layer query: `(tx, ty, mvt)` per requested tile.
///
/// Plain `prepare`, not `prepare_cached`: the text carries the batch's tile
/// list, so it differs per call and caching it would miss every time while
/// evicting the entries that do hit. See `query_mvt_layer`.
fn query_batched_layer(conn: &Connection, sql: &str) -> anyhow::Result<Vec<RenderedTile>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        let tx: i32 = row.get(0)?;
        let ty: i32 = row.get(1)?;
        // NULL is unreachable -- an all-filtered group still emits its layer
        // header -- but decoding it as empty rather than failing matches
        // `query_mvt_layer`'s own NULL handling.
        let mvt: Option<Vec<u8>> = row.get(2)?;
        out.push(((tx as u32, ty as u32), mvt.unwrap_or_default()));
    }
    Ok(out)
}

/// A spatially compact run of same-zoom tiles, rendered by one query per layer.
pub struct RenderBlock {
    pub z: u32,
    pub tiles: Vec<(u32, u32)>,
}

/// How many bits of tile coordinate a block spans: 3, so a block is the 8x8
/// tiles under one common ancestor -- 64 of them.
///
/// **Grouping by ancestor rather than by a run of the sorted key list is what
/// keeps the batch envelope tight.** A block's bounding box *is* its ancestor
/// tile's box, so the batched source scans read exactly the area asked for; a
/// run cut out of a sorted list is a tall thin strip whose box covers far more.
/// 64 also bounds the result set, which is the other reason not to go wider: a
/// dense 64-tile z14 block returns ~9.2 MB of MVT in one go, and 256 would be
/// ~37 MB.
pub const RENDER_BLOCK_SHIFT: u32 = 3;

/// Group tile keys into blocks the batched renderers can take.
///
/// Same-zoom is not a nicety: [`render_block`] derives every tile's envelope
/// and cell range from one `z`, so a block mixing zooms would render half its
/// tiles at the wrong one rather than fail.
pub fn render_blocks(keys: &[TileKey]) -> Vec<RenderBlock> {
    let mut by_block: std::collections::BTreeMap<(u32, u32, u32), Vec<(u32, u32)>> =
        std::collections::BTreeMap::new();
    for &(z, x, y) in keys {
        by_block
            .entry((z, x >> RENDER_BLOCK_SHIFT, y >> RENDER_BLOCK_SHIFT))
            .or_default()
            .push((x, y));
    }
    by_block
        .into_iter()
        .map(|((z, _, _), tiles)| RenderBlock { z, tiles })
        .collect()
}

/// Render one block, dispatching to the tier's batched query.
///
/// The bulk counterpart to [`render_tile`], and the only thing `tiles warm` and
/// `jobs::tile_refresh` need to call.
pub fn render_block(conn: &Connection, block: &RenderBlock) -> anyhow::Result<Vec<RenderedTile>> {
    if block.z == CHANGE_CELL_ZOOM {
        render_z14_tiles(conn, &block.tiles)
    } else {
        render_points_tiles(conn, block.z, &block.tiles)
    }
}

/// Tier C (z14): the four-layer tile -- `addresses`, `buildings`,
/// `addresses_all`, `buildings_all`, concatenated.
///
/// Unlike Tier B this selects by *geometry*, so it renders rows tagged to
/// neighbouring z14 cells; that asymmetry is what forces the 3x3 invalidation
/// ring (see `tile_dirty`).
fn render_z14_tile(conn: &Connection, x: u32, y: u32) -> anyhow::Result<Vec<u8>> {
    let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(CHANGE_CELL_ZOOM, x, y);
    let (buf_min_lon, buf_min_lat, buf_max_lon, buf_max_lat) = (
        min_lon - ADJACENCY_READ_BUFFER_DEG,
        min_lat - ADJACENCY_READ_BUFFER_DEG,
        max_lon + ADJACENCY_READ_BUFFER_DEG,
        max_lat + ADJACENCY_READ_BUFFER_DEG,
    );

    // The bbox is repeated once per `?` group: the `bbox` CTE, then one
    // ST_MakeEnvelope per filtered table. Each group must stay in
    // min_lon, min_lat, max_lon, max_lat order.
    let addresses = query_mvt_layer(
        conn,
        ADDRESSES_MVT_SQL.as_str(),
        duckdb::params![
            min_lon, min_lat, max_lon, max_lat, // bbox CTE
            min_lon, min_lat, max_lon, max_lat, // resolved (prg_unmatched) filter
        ],
    )?;
    let buildings = query_mvt_layer(
        conn,
        BUILDINGS_MVT_SQL.as_str(),
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
        conn,
        ALL_ADDRESSES_MVT_SQL.as_str(),
        duckdb::params![
            min_lon, min_lat, max_lon, max_lat, // bbox CTE
            min_lon, min_lat, max_lon, max_lat, // candidates (prg_addresses) filter
        ],
    )?;
    let buildings_all = query_mvt_layer(
        conn,
        ALL_BUILDINGS_MVT_SQL.as_str(),
        duckdb::params![
            min_lon, min_lat, max_lon, max_lat, // bbox CTE
            min_lon, min_lat, max_lon, max_lat, // bdot10k_candidates filter
            min_lon, min_lat, max_lon, max_lat, // egib_candidates filter
        ],
    )?;
    Ok([addresses, buildings, addresses_all, buildings_all].concat())
}

/// Response shaping for every successful tile that came through a cache --
/// the persistent store or the RAM one, fresh render or hit alike. One path,
/// so a hit cannot differ from a miss on a header, a status code, or the
/// encoding negotiated.
///
/// Deliberately not routed through `finish_tile_response` below, which the
/// error branches still use: its `bytes: Vec<u8>` parameter would force a copy
/// out of the cache's `Bytes` on every hit, defeating the point of holding
/// `Bytes` at all. The two are otherwise the same shaping and the success
/// branches must stay in step.
///
/// Bodies are stored gzipped, so the common path hands the stored buffer to
/// the response verbatim -- no copy, no compression, nothing per-request. The
/// identity branch exists for `curl` and monitoring probes, which send no
/// `Accept-Encoding` at all; it is not dead code, and a browser never takes
/// it.
///
/// Note there is no empty-body 204 here, and that is not an oversight:
/// `ST_AsMVT` emits a layer header even over zero features (pinned by
/// `empty_tile_returns_ok_not_500`), so an in-range tile always has bytes.
/// `finish_tile_response`'s 204 branch is reached only by the out-of-range
/// zoom dispatch.
fn tile_body_response(
    body: TileBody,
    cache_header: HeaderValue,
    etag: Option<&str>,
    wants_gzip: bool,
) -> Response {
    let mut resp = if wants_gzip {
        let mut resp = body.gzip.into_response();
        resp.headers_mut()
            .insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        resp
    } else {
        match body.decompress() {
            Ok(raw) => Bytes::from(raw).into_response(),
            Err(e) => {
                // A stored value that will not decompress is corrupt, not a
                // client problem -- but it is also not worth a 500 when a
                // re-render would fix it, so fall through to the error
                // shaping that leaves the response header-less and uncached.
                tracing::error!(error = %e, "stored tile failed to decompress");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        }
    };
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.mapbox-vector-tile"),
    );
    resp.headers_mut()
        .insert(header::CACHE_CONTROL, cache_header);
    resp.headers_mut()
        .insert(header::VARY, http_cache::VARY_ACCEPT_ENCODING);
    if let Some(etag) = etag {
        resp.headers_mut()
            .insert(header::ETAG, http_cache::weak_etag(etag));
    }
    resp
}

/// Run one `ST_AsMVT` query and return its blob.
///
/// **`prepare_cached`, and every caller must pass a *constant* `sql`.** DuckDB
/// re-plans on every `prepare`, and planning the four z14 layer queries costs
/// ~8 ms per tile independent of how much data the tile holds -- a large share
/// of a sparse tile's whole render. Caching the statement removes most of it:
/// `z14_render_cost_floor_across_densities` moved from 16.7-18.2 ms to
/// 13.9-14.7 ms on one-object tiles, ~3 ms flat per tile, with the densest
/// tiles unchanged inside noise. (A standalone DuckDB-CLI experiment suggested
/// ~6 ms; the in-process figure is the one to believe.)
///
/// The cache is a per-connection LRU keyed on the SQL **text**, which is what
/// makes the constant-text requirement load-bearing rather than stylistic. All
/// five call sites qualify: the four z14 layers are `LazyLock<String>`s, and
/// `agg_cells_sql` produces one text per zoom (seven in total). A caller whose
/// text varied per request -- `points_mvt_sql`, which interpolates its tile
/// list -- would miss every time *and* evict the entries that do hit, so
/// `render_points_tiles` deliberately calls plain `prepare` instead.
fn query_mvt_layer(
    conn: &duckdb::Connection,
    sql: &str,
    params: impl duckdb::Params,
) -> anyhow::Result<Vec<u8>> {
    let mut stmt = conn.prepare_cached(sql)?;
    let mut rows = stmt.query(params)?;
    match rows.next()? {
        Some(row) => {
            let blob: Vec<u8> = row.get(0)?;
            Ok(blob)
        }
        None => Ok(vec![]),
    }
}

/// Tier A dispatch (z5..=z11): one layer (`agg_cells`) -- binned unmatched
/// counts, their denominators, and each source's most recent government
/// change in the bin. See `agg_bin_ctes` above for the design rationale.
///
/// The only tier served from RAM rather than the persistent store, and the
/// only one with no `ETag`. Both follow from the same property: the `changes`
/// CTE is bounded by `now() - max_age_days`, so this tile's content moves with
/// the wall clock even when no row changes. There is no write to hang a push
/// invalidation off, and no stable validator to hand a client -- so it gets a
/// TTL cache whose TTL is the very `max-age` this response advertises.
async fn serve_tile_agg(state: AppState, z: u32, x: u32, y: u32, headers: &HeaderMap) -> Response {
    let cache_header = state.cache_headers.agg_tile.clone();
    let wants_gzip = http_cache::accepts_gzip(headers);
    if let Some(body) = state.tile_cache.get((z, x, y)) {
        return tile_body_response(body, cache_header, None, wants_gzip);
    }

    let max_age_days = state.config.changes.max_age_days;
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
    let cells_sql = agg_cells_sql(shift, n, max_age_days);

    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<TileBody> {
        let conn = state
            .pool
            .get()
            .context("Failed to acquire pool connection")?;
        // Same bound-group shape as every other query in this file: the bbox
        // CTE, then one (lo_x, hi_x, lo_y, hi_y) group per source table, in
        // the order the CTEs spell them out.
        let raw = query_mvt_layer(
            &conn,
            &cells_sql,
            duckdb::params![
                min_lon, min_lat, max_lon, max_lat, // bbox CTE
                lo_x, hi_x, lo_y, hi_y, // bdot10k_unmatched filter
                lo_x, hi_x, lo_y, hi_y, // egib_unmatched filter
                lo_x, hi_x, lo_y, hi_y, // prg_unmatched filter
                lo_x, hi_x, lo_y, hi_y, // cell_totals filter (denominators)
                lo_x, hi_x, lo_y, hi_y, // dataset_change_areas filter
            ],
        )?;
        drop(conn);
        // Compressed here rather than per response, so the RAM cache holds the
        // same representation the store does and the response path needs no
        // per-tier branch. The ETag is discarded: this tier does not send one.
        let (_, body) = tile_store::prepare(&raw)?;
        state.tile_cache.insert((z, x, y), body.clone());
        Ok(body)
    })
    .await;

    match result {
        Ok(Ok(body)) => tile_body_response(body, cache_header, None, wants_gzip),
        Ok(Err(e)) => finish_tile_response(Ok(Err(e)), z, x, y, cache_header, None),
        Err(e) => finish_tile_response(Err(e), z, x, y, cache_header, None),
    }
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
/// later", which is exactly as wrong as caching the 500 itself. Every caller
/// now passes `None`: this function only ever shapes error branches, since the
/// success paths go through [`tile_body_response`], which owns the `ETag` and
/// the content negotiation.
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
    use std::collections::BTreeSet;
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

        let bldg_plan = plan_of(BUILDINGS_MVT_SQL.as_str(), &bldg_params);
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
            // icu, like every other fixture in this crate and like
            // `Config::default`'s own init list: without it DuckDB has no
            // `TIMESTAMP WITH TIME ZONE - INTERVAL` overload, so the Tier A
            // query's `detected_at >= (now() - INTERVAL ...)` bound fails to
            // bind and every z5..z11 tile 500s.
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
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

    /// Same as `request_tile`, plus an `Accept-Encoding` header -- used by
    /// the content-negotiation tests. `None` means the header is absent
    /// entirely, which is `curl`'s default and the case the identity fallback
    /// exists for.
    async fn request_tile_with_accept_encoding(
        state: AppState,
        z: u32,
        x: u32,
        y: u32,
        accept_encoding: Option<&str>,
    ) -> Response {
        let mut builder = Request::builder().uri(format!("/tiles/{z}/{x}/{y}"));
        if let Some(value) = accept_encoding {
            builder = builder.header(header::ACCEPT_ENCODING, value);
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

    /// z10 sits inside Tier A (aggregated bins), so unlike the out-of-range
    /// zooms above it must return a tile rather than 204. An empty DB is enough
    /// here since `ST_AsMVT` emits a layer header even with zero features, same
    /// as the z14 `empty_tile_returns_ok_not_500` case below.
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
    /// shift = 14 - 6 = 8). Asserts the one MVT layer this tier emits, plus
    /// every attribute it carries.
    ///
    /// `agg_points` -- a second layer with the same attributes, one point per
    /// bin -- is asserted *absent*: nothing reads it, and emitting it meant
    /// evaluating `agg_bin_ctes` twice per tile. Without the negative
    /// assertion it could quietly come back.
    #[tokio::test]
    async fn aggregated_tile_exposes_the_agg_cells_layer_and_not_agg_points() {
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
        assert!(
            !body.contains("agg_points"),
            "agg_points layer is unused and must not be emitted"
        );
        for attr in ["n_bdot10k", "n_egib", "n_prg", "n_total"] {
            assert!(body.contains(attr), "missing {attr} count attribute");
        }
        for attr in ["t_bdot10k", "t_egib", "t_prg", "t_total"] {
            assert!(body.contains(attr), "missing {attr} total attribute");
        }
        for attr in ["r_bdot10k", "r_egib", "r_prg", "r_total"] {
            assert!(body.contains(attr), "missing {attr} ratio attribute");
        }
        // Key presence only -- ST_AsMVT leaves an attribute's *key* in the
        // layer dictionary even when it drops every value, so the values
        // themselves are pinned by the agg_bins tests below instead.
        for attr in ["ts_bdot10k", "ts_egib", "ts_prg"] {
            assert!(body.contains(attr), "missing {attr} change-time attribute");
        }
    }

    /// One `geo` row as `agg_bin_ctes` computes it.
    ///
    /// `ts_*` are `Option<i64>` on purpose: the whole point of the `COALESCE`
    /// in `geo` is that an unmatched LEFT JOIN must read 0 rather than NULL,
    /// and `None` here is exactly the state that would make `ST_AsMVT` drop
    /// the attribute and leave the frontend's `["get", "ts_egib"]` comparison
    /// silently null instead of false.
    #[derive(Debug, PartialEq)]
    struct AggBin {
        bin_x: i32,
        bin_y: i32,
        n_total: i32,
        t_total: i32,
        r_total: f64,
        ts_bdot10k: Option<i64>,
        ts_egib: Option<i64>,
        ts_prg: Option<i64>,
    }

    /// Reads the bins for a tile straight out of `geo`, with the same bound
    /// parameters `serve_tile_agg` passes.
    ///
    /// Grepping a rendered tile proves an attribute *key* exists but says
    /// nothing about its values -- `ST_AsMVT` writes the key into the layer
    /// dictionary even when it drops every value it was given. These tests are
    /// about values, so they go around the MVT encoding rather than through it.
    fn agg_bins(state: &AppState, z: u32, x: u32, y: u32) -> Vec<AggBin> {
        let bz = (z + 5).min(14);
        let shift = 14 - bz;
        let n: u32 = 1u32 << bz;
        let cell_shift = 14 - z;
        let lo_x = (x << cell_shift) as i32;
        let hi_x = (((x + 1) << cell_shift) - 1) as i32;
        let lo_y = (y << cell_shift) as i32;
        let hi_y = (((y + 1) << cell_shift) - 1) as i32;
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(z, x, y);

        let sql = format!(
            "{}
             SELECT bin_x, bin_y, n_total, t_total, r_total,
                    ts_bdot10k, ts_egib, ts_prg
             FROM geo ORDER BY bin_x, bin_y",
            agg_bin_ctes(shift, n, state.config.changes.max_age_days)
        );
        let conn = state.pool.get().unwrap();
        let mut stmt = conn.prepare(&sql).unwrap();
        let rows = stmt
            .query_map(
                duckdb::params![
                    min_lon, min_lat, max_lon, max_lat, // bbox CTE
                    lo_x, hi_x, lo_y, hi_y, // bdot10k_unmatched
                    lo_x, hi_x, lo_y, hi_y, // egib_unmatched
                    lo_x, hi_x, lo_y, hi_y, // prg_unmatched
                    lo_x, hi_x, lo_y, hi_y, // cell_totals
                    lo_x, hi_x, lo_y, hi_y, // dataset_change_areas
                ],
                |r| {
                    Ok(AggBin {
                        bin_x: r.get(0)?,
                        bin_y: r.get(1)?,
                        n_total: r.get(2)?,
                        t_total: r.get(3)?,
                        r_total: r.get(4)?,
                        ts_bdot10k: r.get(5)?,
                        ts_egib: r.get(6)?,
                        ts_prg: r.get(7)?,
                    })
                },
            )
            .unwrap();
        rows.map(|r| r.unwrap()).collect()
    }

    fn exec(state: &AppState, sql: &str) {
        state.pool.get().unwrap().execute_batch(sql).unwrap();
    }

    fn unix_now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    /// A `dataset_change_areas` row inside the tile's cell range, `age_days`
    /// old. `cell_z` is `tile_math::CHANGE_CELL_ZOOM` (14), matching what
    /// `update::changeset::insert_change_areas` writes.
    fn change_area(source: &str, cell_x: i32, cell_y: i32, age_days: f64) -> String {
        format!(
            "INSERT INTO dataset_change_areas
                 (snapshot_id, source, cell_z, cell_x, cell_y, added, modified, removed, detected_at)
             VALUES (1, '{source}', 14, {cell_x}, {cell_y}, 1, 0, 0,
                     now() - INTERVAL '{age_days} days');"
        )
    }

    /// One unmatched building and its denominator in z14 cell (8000, 4900),
    /// so the change-area tests below have a bin to decorate. Requested as
    /// z6/31/19, whose cell range is 7936..8191 x 4864..5119 (cell_shift = 8);
    /// binning is a *different* shift (bz = 11, shift = 3), so cell 8000 lands
    /// in bin 8000 >> 3 = 1000 and cell 4900 in bin 4900 >> 3 = 612.
    fn changes_seed() -> String {
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 8000, 4900);
        let (mid_lon, mid_lat) = ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0);
        format!(
            "INSERT INTO bdot10k_unmatched (LOKALNYID, geom, cell_x, cell_y, computed_at) VALUES
                 ('b1', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());
             INSERT INTO cell_totals (source, cell_x, cell_y, total) VALUES
                 ('bdot10k', 8000, 4900, 4);"
        )
    }

    /// Each source's `ts_*` is the *most recent* change anywhere in the bin --
    /// several z14 cells collapse into one bin at every zoom below 9, so the
    /// aggregate has to be `max`, not "whichever row the join happened to see".
    /// A source with no change rows at all reads `Some(0)`, never `None`.
    #[test]
    fn agg_cells_carry_each_sources_most_recent_change_time() {
        let mut seed = changes_seed();
        // Two bdot10k changes in the same bin (8000 >> 3 == 8005 >> 3 == 1000),
        // the newer one second, so a max() regression shows up as the older.
        seed.push_str(&change_area("bdot10k", 8000, 4900, 3.0));
        seed.push_str(&change_area("bdot10k", 8005, 4900, 0.0));
        seed.push_str(&change_area("prg", 8000, 4900, 2.0));
        let state = make_state(&seed);

        let bins = agg_bins(&state, 6, 31, 19);
        assert_eq!(bins.len(), 1, "expected exactly one bin, got {bins:?}");
        let bin = &bins[0];
        assert_eq!((bin.bin_x, bin.bin_y), (1000, 612));

        let now = unix_now();
        let ts_bdot10k = bin.ts_bdot10k.expect("ts_bdot10k must never be NULL");
        assert!(
            (ts_bdot10k - now).abs() < 300,
            "ts_bdot10k should be the newest of the two bdot10k changes \
             (~{now}), got {ts_bdot10k}"
        );
        let ts_prg = bin.ts_prg.expect("ts_prg must never be NULL");
        assert!(
            (ts_prg - (now - 2 * 86_400)).abs() < 300,
            "ts_prg should be ~2 days old, got {ts_prg}"
        );
        // The COALESCE: egib has no change rows here, and the difference
        // between 0 and NULL is the difference between a frontend filter that
        // evaluates to false and one that evaluates to null.
        assert_eq!(bin.ts_egib, Some(0));
    }

    /// The `detected_at` bound is what keeps every z5..z11 tile from scanning
    /// the whole change table, so a row beyond `changes.max_age_days` must not
    /// reach the tile at all -- it reads as "never changed", exactly like a
    /// source with no rows.
    #[test]
    fn a_change_older_than_max_age_days_reads_as_zero() {
        let mut seed = changes_seed();
        seed.push_str(&change_area("egib", 8000, 4900, 60.0));
        let state = make_state(&seed);
        assert_eq!(state.config.changes.max_age_days, 7);

        let bins = agg_bins(&state, 6, 31, 19);
        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0].ts_egib, Some(0));
    }

    /// The cell-range bounds are bound parameters on the change CTE too, not
    /// just on the four source scans -- without them a change anywhere in the
    /// country would decorate every tile whose bin coordinates happened to
    /// collide.
    #[test]
    fn a_change_outside_the_tiles_cell_range_is_ignored() {
        let mut seed = changes_seed();
        // Cell range for z6/31/19 is 7936..8191; 9000 is well outside it.
        seed.push_str(&change_area("bdot10k", 9000, 4900, 0.0));
        let state = make_state(&seed);

        let bins = agg_bins(&state, 6, 31, 19);
        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0].ts_bdot10k, Some(0));
    }

    /// Why the change data is LEFT JOINed rather than unioned into `bins`: a
    /// union branch *creates* bins, and a bin standing on change data alone
    /// has no denominator, so `ratio_sql` gives it RATIO_UNKNOWN and the
    /// frontend paints a cell that was not in the grid a moment ago in the
    /// "no denominator" colour. The situation is ordinary -- a cell whose last
    /// object was just matched away has a change area and no totals row.
    #[test]
    fn change_rows_in_a_cell_with_no_grid_data_add_no_bin() {
        let state = make_state(&changes_seed());
        let before = agg_bins(&state, 6, 31, 19);
        assert_eq!(before.len(), 1);

        // Same tile, a different bin: 8008 >> 3 = 1001, not 1000.
        exec(&state, &change_area("bdot10k", 8008, 4900, 0.0));

        let after = agg_bins(&state, 6, 31, 19);
        assert_eq!(
            after, before,
            "a change area with no grid cell to decorate must draw nothing"
        );
    }

    /// The join must decorate the grid, never alter it. A regression here --
    /// a fan-out duplicating a bin's counts, say -- would repaint the map
    /// while every "does the overlay work" test still passed.
    #[test]
    fn the_change_join_does_not_perturb_the_counts() {
        let state = make_state(&changes_seed());
        let before = agg_bins(&state, 6, 31, 19);
        assert_eq!(before.len(), 1);
        assert_eq!((before[0].n_total, before[0].t_total), (1, 4));

        // Three change rows landing in this one bin, from two sources.
        let mut more = change_area("bdot10k", 8000, 4900, 0.0);
        more.push_str(&change_area("bdot10k", 8001, 4900, 1.0));
        more.push_str(&change_area("prg", 8002, 4900, 0.5));
        exec(&state, &more);

        let after = agg_bins(&state, 6, 31, 19);
        assert_eq!(after.len(), 1);
        assert_eq!(
            (
                after[0].bin_x,
                after[0].bin_y,
                after[0].n_total,
                after[0].t_total
            ),
            (
                before[0].bin_x,
                before[0].bin_y,
                before[0].n_total,
                before[0].t_total
            )
        );
        assert_eq!(after[0].r_total, before[0].r_total);
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
            "source:building",
            "building:levels",
        ] {
            assert!(
                body.contains(expected),
                "expected tile bytes to contain {expected:?}"
            );
        }
    }

    /// --- the buildings OSM tag preview ---------------------------------
    ///
    /// `source:building` and `building:levels` are the two tags `/package`
    /// adds outside the building-type mapping's `tags` string, so they are
    /// their own MVT columns rather than part of it (see `buildings_sql`).
    /// Read per row through the projection seam for the same reason the
    /// `reported` tests below use it: a byte search finds the key in the
    /// layer dictionary whether or not any row carries a value.
    ///
    /// The values are asserted against `package`'s own constants and
    /// `with_building_levels`'s own rule rather than restated literals --
    /// the whole point of the preview is that it agrees with the export.
    fn building_tag_preview(state: &AppState) -> Vec<(String, String, Option<String>)> {
        let mut out = query_unmatched_buildings(
            state,
            "t.id, t.\"source:building\", t.\"building:levels\"",
            |row| {
                (
                    row.get::<_, String>(0).unwrap(),
                    row.get::<_, String>(1).unwrap(),
                    row.get::<_, Option<String>>(2).unwrap(),
                )
            },
        );
        out.sort();
        out
    }

    /// Runs `buildings_sql` for tile 14/8000/4900 with `projection` over the
    /// finished rows, binding exactly what `serve_tile` binds, and maps each
    /// row through `read`.
    fn query_unmatched_buildings<T>(
        state: &AppState,
        projection: &str,
        read: impl Fn(&duckdb::Row) -> T,
    ) -> Vec<T> {
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 8000, 4900);
        let (buf_min_lon, buf_min_lat, buf_max_lon, buf_max_lat) = (
            min_lon - ADJACENCY_READ_BUFFER_DEG,
            min_lat - ADJACENCY_READ_BUFFER_DEG,
            max_lon + ADJACENCY_READ_BUFFER_DEG,
            max_lat + ADJACENCY_READ_BUFFER_DEG,
        );
        let sql = buildings_sql(projection, TileScope::Single);
        let conn = state.pool.get().unwrap();
        let mut stmt = conn.prepare(&sql).unwrap();
        let mut rows = stmt
            .query(duckdb::params![
                min_lon,
                min_lat,
                max_lon,
                max_lat, // bbox CTE
                min_lon,
                min_lat,
                max_lon,
                max_lat, // bdot10k_pkg
                buf_min_lon,
                buf_min_lat,
                buf_max_lon,
                buf_max_lat, // bdot10k_nb
                BDOT10K_ADJACENCY_KEY,
                min_lon,
                min_lat,
                max_lon,
                max_lat, // egib_pkg
                buf_min_lon,
                buf_min_lat,
                buf_max_lon,
                buf_max_lat, // egib_nb
                EGIB_ADJACENCY_KEY,
            ])
            .unwrap();
        let mut out = Vec::new();
        while let Some(row) = rows.next().unwrap() {
            out.push(read(row));
        }
        out
    }

    /// --- `approx_area_m2` on the two building layers ------------------------
    ///
    /// One ~0.0002 x 0.0001 degree rectangle per source, in both the unmatched
    /// and the raw tables. The reference is EPSG:3035, which is equal-area and
    /// so exact wherever it is defined -- not EPSG:2180, whose conformal scale
    /// error reaches several percent this far (the fixture tile sits near 4E)
    /// from its 19E central meridian. Tolerance covers `area_m2_sql`'s
    /// measured 0.05% plus the rounding to whole metres.
    ///
    /// Per row through the projection seam, for the same reason as the
    /// `reported` tests below: the key is in the layer dictionary whether or
    /// not any feature carries a value.
    fn seed_for_area() -> (String, String) {
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 8000, 4900);
        let (lon, lat) = ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0);
        let wkt = format!(
            "POLYGON(({lon} {lat}, {x} {lat}, {x} {y}, {lon} {y}, {lon} {lat}))",
            x = lon + 0.0002,
            y = lat + 0.0001
        );
        let seed = format!(
            "INSERT INTO bdot10k_unmatched (LOKALNYID, geom, cell_x, cell_y, computed_at)
             VALUES ('b1', ST_GeomFromText('{wkt}'), 8000, 4900, now());
             INSERT INTO egib_unmatched (id_budynku, geom, cell_x, cell_y, computed_at)
             VALUES ('e1', ST_GeomFromText('{wkt}'), 8000, 4900, now());
             INSERT INTO bdot10k_buildings (PRZESTRZENNAZW, LOKALNYID, geom, centroid)
             VALUES ('PL.PZGiK.BDOT10k.1234', 'b1', ST_GeomFromText('{wkt}'),
                     ST_Centroid(ST_GeomFromText('{wkt}')));
             INSERT INTO egib_buildings (id_budynku, geom, centroid)
             VALUES ('e1', ST_GeomFromText('{wkt}'), ST_Centroid(ST_GeomFromText('{wkt}')));"
        );
        (seed, wkt)
    }

    fn equal_area_m2(state: &AppState, wkt: &str) -> f64 {
        state
            .pool
            .get()
            .unwrap()
            .query_row(
                "SELECT ST_Area(ST_Transform(ST_GeomFromText(?::VARCHAR), 'EPSG:4326', 'EPSG:3035',
                                             always_xy := true))",
                [wkt],
                |r| r.get(0),
            )
            .unwrap()
    }

    #[test]
    fn both_building_layers_carry_the_footprint_area_in_whole_square_metres() {
        let (seed, wkt) = seed_for_area();
        let state = make_state(&seed);
        let expected = equal_area_m2(&state, &wkt);
        assert!(
            expected > 100.0,
            "fixture must be a real-sized building, got {expected}"
        );

        let mut unmatched = query_unmatched_buildings(&state, "t.id, t.approx_area_m2", |row| {
            (
                row.get::<_, String>(0).unwrap(),
                row.get::<_, Option<i64>>(1).unwrap(),
            )
        });
        unmatched.sort();
        let mut all = {
            let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 8000, 4900);
            let bbox = [min_lon, min_lat, max_lon, max_lat];
            // bbox CTE, bdot10k_candidates, egib_candidates.
            let flat: Vec<f64> = (0..3).flat_map(|_| bbox).collect();
            let params: Vec<&dyn duckdb::ToSql> =
                flat.iter().map(|v| v as &dyn duckdb::ToSql).collect();
            let conn = state.pool.get().unwrap();
            let mut stmt = conn
                .prepare(&all_buildings_sql(
                    "t.id, t.approx_area_m2",
                    TileScope::Single,
                ))
                .unwrap();
            let mut rows = stmt.query(params.as_slice()).unwrap();
            let mut out = Vec::new();
            while let Some(row) = rows.next().unwrap() {
                out.push((
                    row.get::<_, String>(0).unwrap(),
                    row.get::<_, Option<i64>>(1).unwrap(),
                ));
            }
            out
        };
        all.sort();

        for (layer, rows) in [("buildings", &unmatched), ("buildings_all", &all)] {
            let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
            assert_eq!(ids, vec!["b1", "e1"], "{layer}: one feature per source");
            for (id, area) in rows {
                let area = area.unwrap_or_else(|| panic!("{layer}/{id}: approx_area_m2 is NULL"));
                assert!(
                    (area as f64 - expected).abs() <= expected * 0.001 + 0.5,
                    "{layer}/{id}: approx_area_m2 {area}, equal-area reference {expected:.2}"
                );
            }
        }
    }

    /// Seeds one bdot10k and one egib unmatched building in the tile, with
    /// `levels` spliced into each so a case can vary only the storey count.
    fn seed_for_tag_preview(bdot10k_levels: &str, egib_levels: &str) -> String {
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 8000, 4900);
        let (mid_lon, mid_lat) = ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0);
        format!(
            "INSERT INTO bdot10k_unmatched
                 (LOKALNYID, geom, cell_x, cell_y, computed_at, liczba_kondygnacji)
             VALUES ('b1', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now(), {bdot10k_levels});
             INSERT INTO egib_unmatched
                 (id_budynku, geom, cell_x, cell_y, computed_at, kondygnacje_nadziemne)
             VALUES ('e1', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now(), {egib_levels});"
        )
    }

    #[test]
    fn tile_preview_tags_each_source_the_way_package_exports_it() {
        let state = make_state(&seed_for_tag_preview("4", "3"));
        assert_eq!(
            building_tag_preview(&state),
            vec![
                (
                    "b1".to_string(),
                    SOURCE_BUILDING_BDOT10K.to_string(),
                    Some("4".to_string())
                ),
                (
                    "e1".to_string(),
                    SOURCE_BUILDING_EGIB.to_string(),
                    Some("3".to_string())
                ),
            ]
        );
    }

    /// `with_building_levels` reports nothing for a missing count or a `0`
    /// ("budynek nie posiada kondygnacji" in the source's own definition), so
    /// the preview must leave the attribute NULL in both cases -- `ST_AsMVT`
    /// then drops it and the popup shows no `building:levels` row at all,
    /// rather than asserting `building:levels=0`.
    #[test]
    fn a_missing_or_zero_storey_count_previews_no_levels_tag() {
        let state = make_state(&seed_for_tag_preview("NULL", "0"));
        assert_eq!(
            building_tag_preview(&state),
            vec![
                ("b1".to_string(), SOURCE_BUILDING_BDOT10K.to_string(), None),
                ("e1".to_string(), SOURCE_BUILDING_EGIB.to_string(), None),
            ]
        );
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
            &all_buildings_sql(
                "t.id, t.reported IS NOT NULL AS reported",
                TileScope::Single,
            ),
            3,
        )
    }

    fn address_flags(state: &AppState) -> Vec<(String, bool)> {
        // bbox CTE, candidates.
        reported_flags(
            state,
            &all_addresses_sql(
                "t.lokalny_id, t.reported IS NOT NULL AS reported",
                TileScope::Single,
            ),
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
                 (report_id, source, record_key, signature, reported_at,
                  cell_x, cell_y, status, resolved_at)
             VALUES
                 (1, 'bdot10k', ['PL.PZGiK.BDOT10k.1234', 'b1'], NULL, now(),
                  8000, 4900, 'active', NULL),
                 (2, 'prg', ['a1'], NULL, now(), 8000, 4900, 'active', NULL);",
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
                 (report_id, source, record_key, signature, reported_at,
                  cell_x, cell_y, status, resolved_at)
             VALUES
                 (1, 'bdot10k', ['PL.PZGiK.BDOT10k.1234', 'b1'], NULL, now(),
                  8000, 4900, 'revoked', now()),
                 (2, 'bdot10k', ['PL.PZGiK.BDOT10k.9999', 'b2'], NULL, now(),
                  8000, 4900, 'active', NULL),
                 (3, 'egib', ['b1'], NULL, now(), 8000, 4900, 'active', NULL);",
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

    /// The `ETag` is a hash of the tile's bytes, so it moves exactly when the
    /// rendered content moves -- no epoch term, no per-cell term, nothing that
    /// can drift out of step with what the client actually receives.
    ///
    /// Both cases below used to need separate machinery to detect: the tile's
    /// own cell, and a *neighbouring* cell whose rows this tile still renders
    /// (rows are selected by geometry but tagged by their representative
    /// point's cell). A content hash covers both for free, which is the whole
    /// argument for it.
    #[tokio::test]
    async fn the_etag_moves_when_the_tile_bytes_move_including_from_a_neighbouring_cell() {
        let state = make_state("");
        let etag_a = request_tile(state.clone(), 14, 8000, 4900)
            .await
            .headers()
            .get(header::ETAG)
            .expect("z14 must carry an ETag")
            .clone();

        // Seeded at the tile's own midpoint, not an arbitrary coordinate:
        // the layers select by `ST_Intersects`, so a row outside the tile's
        // envelope changes no bytes however it is tagged. (The deleted
        // version query read the cell *tag* instead, so it moved the ETag for
        // rows this tile never draws -- a false invalidation the content hash
        // simply does not have.)
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(14, 8000, 4900);
        let (mid_lon, mid_lat) = ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0);

        // 1. A row landing in the tile's own cell (8000, 4900).
        {
            let conn = state.pool.get().unwrap();
            conn.execute_batch(&format!(
                "INSERT INTO bdot10k_unmatched (LOKALNYID, geom, cell_x, cell_y, computed_at) VALUES
                     ('b1', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());"
            ))
            .unwrap();
        }
        let etag_b = request_tile(state.clone(), 14, 8000, 4900)
            .await
            .headers()
            .get(header::ETAG)
            .unwrap()
            .clone();
        assert_ne!(etag_a, etag_b, "new rows must move the tile's ETag");

        // 2. A row tagged to a NEIGHBOURING cell but geometrically inside this
        // tile -- exactly the case the 3x3 invalidation ring exists for, and
        // the reason a cell's own change is not enough to keep tiles fresh.
        {
            let conn = state.pool.get().unwrap();
            conn.execute_batch(&format!(
                "INSERT INTO bdot10k_unmatched (LOKALNYID, geom, cell_x, cell_y, computed_at) VALUES
                     ('b2', ST_Point({mid_lon}, {mid_lat}), 8001, 4901, now());"
            ))
            .unwrap();
        }
        let etag_c = request_tile(state, 14, 8000, 4900)
            .await
            .headers()
            .get(header::ETAG)
            .unwrap()
            .clone();
        assert_ne!(
            etag_b, etag_c,
            "a neighbouring cell's row renders in this tile, so it must move the ETag"
        );
    }

    /// Two requests that hit the store return the same `ETag`, because they
    /// return the same stored bytes.
    ///
    /// The stronger property -- that a *re-render* returns the same `ETag`
    /// too -- is pinned separately by
    /// `a_re_render_of_unchanged_rows_produces_the_same_bytes`, and holds
    /// because every MVT query sorts (`deterministic_mvt_order_sql`). The
    /// validator stays *weak* regardless: weak is what lets one `ETag` serve
    /// both the gzip and the identity representation of the same tile.
    #[tokio::test]
    async fn two_store_hits_return_the_same_etag() {
        let state = make_state("");
        let first = request_tile(state.clone(), 14, 8000, 4900)
            .await
            .headers()
            .get(header::ETAG)
            .unwrap()
            .clone();
        let second = request_tile(state, 14, 8000, 4900)
            .await
            .headers()
            .get(header::ETAG)
            .unwrap()
            .clone();
        assert_eq!(first, second);
    }

    /// z5..=z11 carries no `ETag` because its content moves with the wall
    /// clock; z12..=z13 now *does* carry one, since a stored content hash is a
    /// faithful validator for a tier that had none before. Both halves in one
    /// test, so the boundary between them cannot silently move.
    #[tokio::test]
    async fn only_the_persisted_tiers_carry_an_etag() {
        for (z, x, y) in [(6, 31, 19), (11, 1000, 612)] {
            let state = make_state("");
            let response = request_tile(state, z, x, y).await;
            assert_eq!(response.status(), StatusCode::OK, "z{z}");
            assert!(
                response.headers().get(header::ETAG).is_none(),
                "z{z} content moves with the clock, so it has no honest validator"
            );
        }
        for (z, x, y) in [(12, 2000, 1225), (13, 4000, 2450), (14, 8000, 4900)] {
            let state = make_state("");
            let response = request_tile(state, z, x, y).await;
            assert_eq!(response.status(), StatusCode::OK, "z{z}");
            assert!(
                response.headers().get(header::ETAG).is_some(),
                "z{z} is persisted and content-hashed, so it must carry an ETag"
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

    // --- Cache and store wiring ---------------------------------------------

    /// A tile with no features is a **200 carrying a layer header**, never a
    /// 204: `ST_AsMVT` is an aggregate with no `GROUP BY`, so it emits one row
    /// -- a layer header -- whatever the input (see
    /// `empty_tile_returns_ok_not_500`). `finish_tile_response`'s empty-bytes
    /// 204 branch therefore only ever fires for the out-of-range zoom
    /// dispatch, and this test pins that the store path does not invent a 204
    /// of its own. "Empty tile ⇒ 204" is the convention in other tile servers,
    /// so it is exactly the assumption a future reader will import by mistake.
    #[tokio::test]
    async fn a_featureless_tile_is_a_200_with_a_layer_header_not_a_204() {
        for (z, x, y) in [(12, 2000, 1225), (14, 8000, 4900)] {
            let state = make_state("");
            let response = request_tile(state, z, x, y).await;
            assert_eq!(response.status(), StatusCode::OK, "z{z}");
            let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            assert!(!bytes.is_empty(), "z{z} must carry its layer header");
        }
    }

    /// The response must not differ by cache residency on anything a client
    /// can see. Compared field by field rather than by a single equality,
    /// because a `Response` is not `PartialEq` and the interesting failure
    /// (an earlier draft returned a bare 200 where the fresh path returned a
    /// 204) is exactly a status/header divergence.
    #[test]
    fn a_cached_body_and_a_fresh_body_shape_identically() {
        let cache_header = HeaderValue::from_static("public, max-age=60");
        let (etag, body) = tile_store::prepare(b"some rendered mvt bytes").unwrap();

        let a = tile_body_response(body.clone(), cache_header.clone(), Some(&etag), true);
        let b = tile_body_response(body, cache_header.clone(), Some(&etag), true);

        assert_eq!(a.status(), b.status());
        assert_eq!(a.headers().get(header::ETAG), b.headers().get(header::ETAG));
        assert_eq!(
            a.headers().get(header::CACHE_CONTROL),
            b.headers().get(header::CACHE_CONTROL)
        );
        assert_eq!(
            a.headers().get(header::CONTENT_TYPE),
            b.headers().get(header::CONTENT_TYPE)
        );
        assert_eq!(
            a.headers().get(header::CONTENT_ENCODING),
            b.headers().get(header::CONTENT_ENCODING)
        );
        assert_eq!(a.headers().get(header::VARY), b.headers().get(header::VARY));
    }

    /// The aggregate tier's payoff: a second request for the same tile inside
    /// the TTL is served from RAM instead of re-running the binned query.
    /// Asserted on the cache's own counters, not on timing -- timing would be
    /// racy and would not prove which path served the response.
    #[tokio::test]
    async fn a_repeat_request_for_an_aggregate_tile_is_served_from_the_cache() {
        let state = make_state("");

        let first = request_tile(state.clone(), 6, 31, 19).await;
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(
            state.tile_cache.misses(),
            1,
            "first request must miss and populate the cache"
        );
        assert_eq!(state.tile_cache.hits(), 0);

        let second = request_tile(state.clone(), 6, 31, 19).await;
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(
            state.tile_cache.hits(),
            1,
            "repeat request must be served from the cache"
        );
        assert_eq!(state.tile_cache.misses(), 1, "and add no second miss");
    }

    /// z12..=z14 must never touch the RAM cache -- they are persisted instead.
    /// Sharing one cache across tiers with different invalidation stories is
    /// exactly the bug this split exists to prevent.
    #[tokio::test]
    async fn the_persisted_tiers_do_not_use_the_ram_cache() {
        let state = make_state("");
        request_tile(state.clone(), 14, 8000, 4900).await;
        request_tile(state.clone(), 12, 2000, 1225).await;
        assert_eq!(state.tile_cache.hits(), 0);
        assert_eq!(
            state.tile_cache.misses(),
            0,
            "the RAM cache must not even be consulted above z11"
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

    /// The RAM cache serves z5..=z11 and nothing else. An out-of-range zoom
    /// returns before any cache is consulted, and z12..=z14 go to the
    /// persistent store instead -- sharing one cache across tiers with
    /// different invalidation stories is the bug the split exists to prevent.
    #[tokio::test]
    async fn the_ram_cache_is_consulted_for_the_aggregate_tier_and_no_other() {
        let state = make_state("");
        for (z, x, y) in [(3, 1, 1), (12, 2000, 1225), (14, 8000, 4900)] {
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
                "z{z} must not consult tile_cache at all"
            );
        }
        request_tile(state.clone(), 6, 31, 19).await;
        assert_eq!(
            state.tile_cache.misses(),
            1,
            "z6 is the tier this cache is for, so it must be consulted"
        );
    }

    /// A cache hit must be indistinguishable from a fresh response on every
    /// header a client keys revalidation off of. Driven through the aggregate
    /// tier, since that is the one the RAM cache serves.
    #[tokio::test]
    async fn a_cached_response_carries_the_same_headers_as_a_fresh_one() {
        let state = make_state("");

        let first = request_tile(state.clone(), 6, 31, 19).await;
        let first_etag = first.headers().get(header::ETAG).cloned();
        let first_cache_control = first.headers().get(header::CACHE_CONTROL).cloned();
        let first_vary = first.headers().get(header::VARY).cloned();

        let second = request_tile(state.clone(), 6, 31, 19).await;
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
        assert_eq!(second.headers().get(header::VARY).cloned(), first_vary);
    }
    // --- Content negotiation ------------------------------------------------

    /// Bodies are stored gzipped, so a client that accepts gzip gets the
    /// stored bytes handed straight to the response -- no per-request
    /// compression, which is the whole point of compressing once at render
    /// time.
    #[tokio::test]
    async fn a_client_that_accepts_gzip_gets_a_gzip_encoded_body() {
        let state = make_state("");
        let response = request_tile_with_accept_encoding(state, 14, 8000, 4900, Some("gzip")).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_ENCODING),
            Some(&HeaderValue::from_static("gzip"))
        );
    }

    /// `curl` and most monitoring probes send no `Accept-Encoding` at all.
    /// They must get the identical tile, decompressed -- asserted against a
    /// gzip response's own decompressed bytes so the fallback cannot drift
    /// away from what everyone else receives.
    #[tokio::test]
    async fn a_client_with_no_accept_encoding_gets_identical_bytes_decompressed() {
        let state = make_state("");
        let gzipped =
            request_tile_with_accept_encoding(state.clone(), 14, 8000, 4900, Some("gzip")).await;
        let gzip_bytes = to_bytes(gzipped.into_body(), 8 * 1024 * 1024)
            .await
            .unwrap();
        let mut expected = Vec::new();
        std::io::Read::read_to_end(
            &mut flate2::read::GzDecoder::new(&gzip_bytes[..]),
            &mut expected,
        )
        .unwrap();

        let identity = request_tile_with_accept_encoding(state, 14, 8000, 4900, None).await;
        assert_eq!(identity.status(), StatusCode::OK);
        assert_eq!(
            identity.headers().get(header::CONTENT_ENCODING),
            None,
            "a client that did not ask for gzip must not be sent gzip"
        );
        let identity_bytes = to_bytes(identity.into_body(), 8 * 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(identity_bytes, expected);
    }

    /// `gzip;q=0` is an explicit refusal. A substring test for "gzip" would
    /// read it as consent and send bytes the client just said it cannot use.
    #[tokio::test]
    async fn a_client_sending_gzip_q_0_is_served_identity() {
        let state = make_state("");
        let response =
            request_tile_with_accept_encoding(state, 14, 8000, 4900, Some("gzip;q=0")).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(header::CONTENT_ENCODING), None);
    }

    /// One URL now has two representations, so a shared cache without `Vary`
    /// could hand gzip to a client that cannot read it. It has to ride on the
    /// 304 too, which is a response about that same negotiated representation.
    #[tokio::test]
    async fn vary_accept_encoding_is_present_on_the_200_and_the_304() {
        let state = make_state("");
        let first = request_tile(state.clone(), 14, 8000, 4900).await;
        assert_eq!(
            first.headers().get(header::VARY),
            Some(&HeaderValue::from_static("accept-encoding"))
        );
        let etag = first.headers().get(header::ETAG).unwrap().clone();

        let second =
            request_tile_with_if_none_match(state, 14, 8000, 4900, Some(etag.to_str().unwrap()))
                .await;
        assert_eq!(second.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(
            second.headers().get(header::VARY),
            Some(&HeaderValue::from_static("accept-encoding"))
        );
    }

    /// The `ETag` hashes the *uncompressed* bytes, so it identifies content
    /// rather than encoding. That is what makes one weak validator honest
    /// across both representations -- and what stops a `flate2` version bump
    /// from invalidating every client's cache.
    #[tokio::test]
    async fn the_etag_is_the_same_in_both_encodings() {
        let state = make_state("");
        let gzipped =
            request_tile_with_accept_encoding(state.clone(), 14, 8000, 4900, Some("gzip")).await;
        let identity = request_tile_with_accept_encoding(state, 14, 8000, 4900, None).await;
        assert_eq!(
            gzipped.headers().get(header::ETAG),
            identity.headers().get(header::ETAG)
        );
    }
    /// A z13 tile and the z14 cell whose rows it renders, as a seed plus the
    /// coordinates to ask for.
    fn points_seed() -> (String, (u32, u32)) {
        let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(CHANGE_CELL_ZOOM, 8000, 4900);
        let (lon, lat) = ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0);
        (
            format!(
                "INSERT INTO bdot10k_unmatched (LOKALNYID, geom, cell_x, cell_y, computed_at)
                 VALUES ('b1', ST_Point({lon}, {lat}), 8000, 4900, now());
                 INSERT INTO prg_unmatched (lokalny_id, geom, cell_x, cell_y, computed_at)
                 VALUES ('a1', ST_Point({lon}, {lat}), 8000, 4900, now());"
            ),
            (4000, 2450),
        )
    }

    /// The batched points query must answer for **every** tile asked about,
    /// not only the ones holding rows.
    ///
    /// A bare `GROUP BY tx, ty` emits no group for a tile with nothing in it,
    /// and a missing row is not an empty tile: `ST_AsMVT` produces a layer
    /// header even over zero features, which is what keeps an in-range tile a
    /// 200 rather than the 204 `finish_tile_response` reserves for
    /// out-of-range zooms. The `env LEFT JOIN proj` + `FILTER` pair is what
    /// reproduces that, and this is the test that notices if either half goes.
    #[tokio::test]
    async fn a_points_batch_answers_for_every_requested_tile_including_empty_ones() {
        let (seed, (x, y)) = points_seed();
        let state = make_state(&seed);
        let conn = state.pool.get().unwrap();
        let asked = [(x, y), (x + 1, y), (x, y + 1)];
        let out = render_points_tiles(&conn, 13, &asked).unwrap();

        assert_eq!(out.len(), asked.len(), "one row per requested tile");
        let keys: BTreeSet<(u32, u32)> = out.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, asked.into_iter().collect::<BTreeSet<_>>());

        let populated = out.iter().find(|(k, _)| *k == (x, y)).unwrap();
        let empty: Vec<_> = out.iter().filter(|(k, _)| *k != (x, y)).collect();
        assert!(
            populated.1.len() > empty[0].1.len(),
            "the seeded tile must carry more than a bare layer header"
        );
        for (key, mvt) in &empty {
            assert!(
                !mvt.is_empty(),
                "{key:?} holds nothing, but an empty tile is a layer header, not zero bytes"
            );
        }
        assert_eq!(
            empty[0].1, empty[1].1,
            "every empty tile is the same header"
        );
    }

    /// A tile rendered inside a batch is byte-identical to the same tile
    /// rendered alone.
    ///
    /// This is the property that lets `points_mvt_sql` be the *only* copy of
    /// the points SQL: `tiles warm` and `tile_refresh` batch, a request does
    /// not, and neither may produce different bytes for the same tile. It
    /// holds only because the aggregate sorts -- without
    /// `deterministic_mvt_order_sql` the two paths would agree in length and
    /// differ in content.
    #[tokio::test]
    async fn a_tile_rendered_in_a_batch_is_byte_identical_to_one_rendered_alone() {
        let (seed, (x, y)) = points_seed();
        let state = make_state(&seed);
        let conn = state.pool.get().unwrap();
        let asked = [(x, y), (x + 1, y), (x, y + 1)];
        let batched = render_points_tiles(&conn, 13, &asked).unwrap();
        for (key, mvt) in batched {
            let alone = render_tile(&conn, 13, key.0, key.1).unwrap();
            assert_eq!(
                mvt, alone,
                "z13/{}/{} differs between the paths",
                key.0, key.1
            );
        }
    }

    /// Re-rendering unchanged rows produces the same bytes, which is what
    /// makes the content-hash `ETag` survive a `tile_refresh` that had nothing
    /// real to change.
    ///
    /// The seeded database here is far too small to *reproduce* the
    /// nondeterminism this pins against -- that needs a parallel scan over
    /// thousands of rows, and is what
    /// `tile_render_determinism_and_thread_count` measures on the real
    /// database. This states the property for all three persisted tiers so a
    /// dropped `ORDER BY` at least has somewhere to fail.
    #[tokio::test]
    async fn a_re_render_of_unchanged_rows_produces_the_same_bytes() {
        let (seed, (x, y)) = points_seed();
        let state = make_state(&seed);
        let conn = state.pool.get().unwrap();
        for (z, x, y) in [
            (12, x / 2, y / 2),
            (13, x, y),
            (CHANGE_CELL_ZOOM, 8000, 4900),
        ] {
            let first = render_tile(&conn, z, x, y).unwrap();
            assert!(!first.is_empty(), "z{z} must render something");
            for _ in 0..4 {
                assert_eq!(
                    first,
                    render_tile(&conn, z, x, y).unwrap(),
                    "z{z}/{x}/{y} is not a deterministic function of its rows"
                );
            }
        }
    }

    /// A z14 seed spread over two adjacent tiles, in every table the four
    /// layers read.
    ///
    /// `bdot10k_buildings` gets a touching pair carrying the adjacency key, so
    /// the `max_neighbours` path is actually exercised rather than trivially
    /// returning zero -- that path is the one place `Batched` computes anything
    /// differently (per (tile, row) rather than per row), so a seed that never
    /// reached it would make this test agree for the wrong reason.
    fn z14_seed(tiles: &[(u32, u32)]) -> String {
        let mut sql = String::new();
        for (i, &(x, y)) in tiles.iter().enumerate() {
            let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(CHANGE_CELL_ZOOM, x, y);
            let (lon, lat) = ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0);
            // A pair of small touching squares, so the second is the first's
            // same-class neighbour.
            let w = (max_lon - min_lon) / 16.0;
            let poly = |x0: f64, y0: f64| {
                format!(
                    "ST_GeomFromText('POLYGON(({x0} {y0}, {x1} {y0}, {x1} {y1}, {x0} {y1}, {x0} {y0}))')",
                    x1 = x0 + w,
                    y1 = y0 + w
                )
            };
            let (a, b) = (poly(lon, lat), poly(lon + w, lat));
            sql.push_str(&format!(
                "INSERT INTO bdot10k_unmatched
                     (LOKALNYID, geom, cell_x, cell_y, computed_at, PRZESTRZENNAZW,
                      funkcja_szczegolowa, funkcja_ogolna, liczba_kondygnacji)
                 VALUES ('b{i}', {a}, {x}, {y}, now(), 'PL.PZGiK', '{key}', 'inny', 2);
                 INSERT INTO egib_unmatched
                     (id_budynku, geom, cell_x, cell_y, computed_at, rodzaj_kod,
                      kondygnacje_nadziemne)
                 VALUES ('e{i}', {b}, {x}, {y}, now(), '{ekey}', 1);
                 INSERT INTO prg_unmatched
                     (geom, lokalny_id, numer_porzadkowy, ulica, miejscowosc,
                      kod_pocztowy, teryt_miejscowosc, cell_x, cell_y, computed_at)
                 VALUES (ST_Point({lon}, {lat}), 'a{i}', '{i}', 'Polna', 'Wies',
                         '00-001', '0918123', {x}, {y}, now());
                 INSERT INTO prg_addresses
                     (lokalny_id, numer_porzadkowy, ulica, miejscowosc, geom)
                 VALUES ('a{i}', '{i}', 'Polna', 'Wies', ST_Point({lon}, {lat}));
                 INSERT INTO bdot10k_buildings
                     (PRZESTRZENNAZW, LOKALNYID, geom, centroid,
                      PRZEWAZAJACAFUNKCJABUDYNKU, LICZBAKONDYGNACJI)
                 VALUES ('PL.PZGiK', 'b{i}', {a}, ST_Centroid({a}), '{key}', 2),
                        ('PL.PZGiK', 'n{i}', {b}, ST_Centroid({b}), '{key}', 2);
                 INSERT INTO egib_buildings
                     (id_budynku, geom, centroid, rodzaj_kod, kondygnacje_nadziemne)
                 VALUES ('e{i}', {b}, ST_Centroid({b}), '{ekey}', 1);\n",
                key = BDOT10K_ADJACENCY_KEY,
                ekey = EGIB_ADJACENCY_KEY,
            ));
        }
        sql
    }

    /// A z14 tile rendered inside a batch is byte-identical to the same tile
    /// rendered alone.
    ///
    /// This is the property that lets `Single` and `Batched` both exist: the
    /// request path renders one tile, `tiles warm` and `jobs::tile_refresh`
    /// render blocks, and neither may produce different bytes for the same
    /// tile. It covers all four layers at once, since `render_tile` returns
    /// them concatenated.
    #[tokio::test]
    async fn a_z14_tile_rendered_in_a_batch_is_byte_identical_to_one_rendered_alone() {
        let tiles = [(8000u32, 4900u32), (8001, 4900), (8000, 4901)];
        let state = make_state(&z14_seed(&tiles));
        let conn = state.pool.get().unwrap();

        let batched = render_z14_tiles(&conn, &tiles).unwrap();
        assert_eq!(batched.len(), tiles.len(), "one entry per requested tile");
        for ((x, y), mvt) in batched {
            let alone = render_tile(&conn, CHANGE_CELL_ZOOM, x, y).unwrap();
            assert_eq!(mvt, alone, "z14/{x}/{y} differs between the two paths");
            assert!(!mvt.is_empty());
        }
    }

    /// A batch must answer for a tile holding nothing, with the same four
    /// layer headers a lone render produces.
    ///
    /// `GROUP BY` alone emits no row for such a tile, and a missing row is not
    /// an empty tile -- it would surface as a 204 on a tier that must never
    /// send one. `TileScope::body`'s `LEFT JOIN` + `FILTER` is what prevents
    /// that, and this is the test that notices if either half goes.
    #[tokio::test]
    async fn a_z14_batch_answers_for_tiles_holding_nothing() {
        let populated = (8000u32, 4900u32);
        let state = make_state(&z14_seed(&[populated]));
        let conn = state.pool.get().unwrap();

        let empty = [(8100u32, 4950u32), (8101, 4950)];
        let asked = [populated, empty[0], empty[1]];
        let rendered = render_z14_tiles(&conn, &asked).unwrap();

        assert_eq!(rendered.len(), 3);
        for (key, mvt) in &rendered {
            assert!(
                !mvt.is_empty(),
                "{key:?} holds nothing, but an empty tile is four layer headers, not zero bytes"
            );
            assert_eq!(
                *mvt,
                render_tile(&conn, CHANGE_CELL_ZOOM, key.0, key.1).unwrap()
            );
        }
        let by_key: std::collections::HashMap<_, _> = rendered.into_iter().collect();
        assert_eq!(
            by_key[&empty[0]], by_key[&empty[1]],
            "every empty z14 tile is the same four headers"
        );
        assert!(by_key[&populated].len() > by_key[&empty[0]].len());
    }

    /// Blocks are same-zoom, grouped by common ancestor, and never larger than
    /// the 8x8 that ancestor holds.
    ///
    /// Same-zoom is the load-bearing half: `render_block` derives every tile's
    /// envelope from one `z`, so a block mixing zooms would render half its
    /// tiles at the wrong zoom rather than fail. Ancestor grouping is the other
    /// half -- it is what keeps a batch's bounding box equal to the area its
    /// tiles actually cover.
    #[test]
    fn render_blocks_group_same_zoom_tiles_under_a_common_ancestor() {
        let mut keys: Vec<TileKey> = Vec::new();
        // One full 8x8 z14 block. Both origins are multiples of 8, i.e. the
        // block is ancestor-aligned -- an unaligned run of 64 straddles two
        // blocks and is split, which is the behaviour and not a bug.
        keys.extend((0..8).flat_map(|dx| (0..8).map(move |dy| (14, 8000 + dx, 4896 + dy))));
        keys.push((14, 8008, 4896));
        // Same x/y numbers at a different zoom must not join them.
        keys.push((13, 8000, 4896));

        let blocks = render_blocks(&keys);
        assert_eq!(
            blocks.iter().map(|b| b.tiles.len()).sum::<usize>(),
            keys.len(),
            "every key lands in exactly one block"
        );
        for b in &blocks {
            assert!(b.tiles.len() <= 1 << (2 * RENDER_BLOCK_SHIFT));
            for &(x, y) in &b.tiles {
                assert_eq!(
                    (x >> RENDER_BLOCK_SHIFT, y >> RENDER_BLOCK_SHIFT),
                    (
                        b.tiles[0].0 >> RENDER_BLOCK_SHIFT,
                        b.tiles[0].1 >> RENDER_BLOCK_SHIFT
                    ),
                    "a block must not straddle its ancestor"
                );
            }
        }
        let mut shape: Vec<(u32, usize)> = blocks.iter().map(|b| (b.z, b.tiles.len())).collect();
        shape.sort();
        assert_eq!(shape, vec![(13, 1), (14, 1), (14, 64)]);

        // An unaligned 8x8 run really does split, rather than being forced
        // into one oversized block.
        let unaligned: Vec<TileKey> = (0..8)
            .flat_map(|dx| (0..8).map(move |dy| (14, 8000 + dx, 4900 + dy)))
            .collect();
        let mut split: Vec<usize> = render_blocks(&unaligned)
            .iter()
            .map(|b| b.tiles.len())
            .collect();
        split.sort();
        assert_eq!(split, vec![32, 32]);
    }

    // --- Determinism / ORDER BY benchmark ------------------------------------
    //
    // Ignored by default: needs the real ./osmpbudynkiv2.duckdb and exclusive
    // access to it (stop any `run` server first). Run with:
    //   cargo test --release tile_render_determinism -- --ignored --nocapture

    fn real_db() -> Option<Connection> {
        let path = Path::new("./osmpbudynkiv2.duckdb");
        if !path.exists() {
            eprintln!("skipping: no ./osmpbudynkiv2.duckdb");
            return None;
        }
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        Some(crate::db::init_db(path, &init, None).unwrap())
    }

    fn time_renders(conn: &Connection, z: u32, x: u32, y: u32, n: u32) -> (f64, usize) {
        let _ = render_tile(conn, z, x, y).unwrap(); // warm
        let t = std::time::Instant::now();
        let mut len = 0;
        for _ in 0..n {
            len = render_tile(conn, z, x, y).unwrap().len();
        }
        (t.elapsed().as_secs_f64() * 1000.0 / n as f64, len)
    }

    fn determinism(conn: &Connection, z: u32, x: u32, y: u32, tries: u32) -> u32 {
        let first = render_tile(conn, z, x, y).unwrap();
        let mut differing = 0;
        for _ in 0..tries {
            if render_tile(conn, z, x, y).unwrap() != first {
                differing += 1;
            }
        }
        differing
    }

    /// The verification the whole z14 batching change rests on: over real
    /// blocks, every tile a batch produces is byte-identical to the same tile
    /// rendered alone, and the batch is faster.
    ///
    /// Needs the real `./osmpbudynkiv2.duckdb` and exclusive access (stop any
    /// `run` server). Blocks are chosen to cover the three cases that matter:
    /// a dense city, a block with many *unmatched* buildings (the adjacency
    /// path, which is the only thing `Batched` computes differently), and sea
    /// (empty tiles, whose aggregate is a DuckDB crash away from wrong -- see
    /// `TileScope::body`).
    #[test]
    #[ignore]
    fn z14_batches_match_per_tile_renders_on_the_real_database() {
        let Some(conn) = real_db() else { return };
        // (label, z11 ancestor)
        let blocks = [
            ("Warsaw centre", 1143u32, 674u32),
            ("dense unmatched", 1132, 687),
            ("rural", 1105, 661),
            ("Baltic (empty)", 1143, 650),
        ];
        for (label, bx, by) in blocks {
            let tiles: Vec<(u32, u32)> = (0..8)
                .flat_map(|dx| (0..8).map(move |dy| ((bx << 3) + dx, (by << 3) + dy)))
                .collect();

            let t = std::time::Instant::now();
            let per_tile: Vec<Vec<u8>> = tiles
                .iter()
                .map(|&(x, y)| render_tile(&conn, CHANGE_CELL_ZOOM, x, y).unwrap())
                .collect();
            let per_ms = t.elapsed().as_secs_f64() * 1000.0;

            let t = std::time::Instant::now();
            let batched = render_z14_tiles(&conn, &tiles).unwrap();
            let bat_ms = t.elapsed().as_secs_f64() * 1000.0;

            assert_eq!(batched.len(), tiles.len(), "{label}: one entry per tile");
            let mut differing = 0;
            for (i, ((x, y), mvt)) in batched.iter().enumerate() {
                assert_eq!((*x, *y), tiles[i], "{label}: order must follow the request");
                if *mvt != per_tile[i] {
                    differing += 1;
                    println!(
                        "  DIFFERS z14/{x}/{y}: {} vs {} B",
                        per_tile[i].len(),
                        mvt.len()
                    );
                }
            }
            let bytes: usize = per_tile.iter().map(|v| v.len()).sum();
            println!(
                "{label:<16} {} tiles  per-tile {per_ms:8.1} ms ({:6.2}/tile)  batched {bat_ms:7.1} ms ({:5.2}/tile)  {:4.1}x  {bytes} B  differing {differing}",
                tiles.len(),
                per_ms / tiles.len() as f64,
                bat_ms / tiles.len() as f64,
                per_ms / bat_ms,
            );
            assert_eq!(
                differing, 0,
                "{label}: batched output must be byte-identical"
            );
        }
    }

    /// What the batched points query buys, as a function of batch size.
    ///
    /// The last row is the one the design rests on: at a batch of one the
    /// batched query costs what the per-tile query it replaced cost, so it
    /// could *replace* that query instead of sitting beside it, and the MVT
    /// SQL still has one home. Measured over a z9 region around Warsaw:
    ///
    /// | tiles | ms/tile |
    /// |---|---|
    /// | 253 | 0.16 |
    /// | 16 | 0.58 |
    /// | 4 | 1.50 |
    /// | 1 | 4.43 |
    ///
    /// Byte totals are identical at every batch size (696,857 B here), which
    /// is the check that the batching is not quietly dropping or duplicating
    /// features.
    #[test]
    #[ignore]
    fn points_batch_size_vs_render_cost() {
        let Some(conn) = real_db() else { return };
        const Z: u32 = 13;
        let cell_shift = CHANGE_CELL_ZOOM - Z;
        // A z9 region around Warsaw -> up to 16x16 z13 tiles with data.
        let (rx, ry) = (285u32, 168u32);
        let shift = Z - 9;
        let (tx0, ty0) = (rx << shift, ry << shift);
        let (tx1, ty1) = (((rx + 1) << shift) - 1, ((ry + 1) << shift) - 1);
        let (lo_x, hi_x) = (
            (tx0 << cell_shift) as i32,
            (((tx1 + 1) << cell_shift) - 1) as i32,
        );
        let (lo_y, hi_y) = (
            (ty0 << cell_shift) as i32,
            (((ty1 + 1) << cell_shift) - 1) as i32,
        );
        let mut stmt = conn
            .prepare(&format!(
                "SELECT DISTINCT cell_x >> {cell_shift}, cell_y >> {cell_shift} FROM (
                     SELECT cell_x, cell_y FROM bdot10k_unmatched
                     UNION ALL SELECT cell_x, cell_y FROM egib_unmatched
                     UNION ALL SELECT cell_x, cell_y FROM prg_unmatched
                 ) WHERE cell_x BETWEEN {lo_x} AND {hi_x} AND cell_y BETWEEN {lo_y} AND {hi_y}
                 ORDER BY 1, 2"
            ))
            .unwrap();
        let tiles: Vec<(u32, u32)> = stmt
            .query_map([], |r| {
                Ok((r.get::<_, i32>(0)? as u32, r.get::<_, i32>(1)? as u32))
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        println!("region holds {} z{Z} tiles with data", tiles.len());

        for chunk in [tiles.len(), 16, 4, 1] {
            let _ = render_points_tiles(&conn, Z, &tiles[..chunk.min(tiles.len())]).unwrap();
            let t = std::time::Instant::now();
            let mut bytes = 0usize;
            let mut count = 0usize;
            for part in tiles.chunks(chunk) {
                for (_, mvt) in render_points_tiles(&conn, Z, part).unwrap() {
                    bytes += mvt.len();
                    count += 1;
                }
            }
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            println!(
                "batch {chunk:>4}: {ms:8.1} ms for {count} tiles ({:.2} ms/tile), {bytes} B",
                ms / count as f64
            );
        }
    }

    /// How much of a z14 render is fixed per-query cost rather than data?
    /// That is what decides whether batching z14 would pay -- if a nearly
    /// empty tile still costs tens of milliseconds, the cost is the four
    /// queries, not the rows.
    #[test]
    #[ignore]
    fn z14_render_cost_floor_across_densities() {
        let Some(conn) = real_db() else { return };
        let mut stmt = conn
            .prepare(
                "SELECT cell_x, cell_y, SUM(total) AS n FROM cell_totals
                  GROUP BY cell_x, cell_y ORDER BY n LIMIT 8",
            )
            .unwrap();
        let sparse: Vec<(u32, u32, i64)> = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i32>(0)? as u32,
                    r.get::<_, i32>(1)? as u32,
                    r.get(2)?,
                ))
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        let mut stmt = conn
            .prepare(
                "SELECT cell_x, cell_y, SUM(total) AS n FROM cell_totals
                  GROUP BY cell_x, cell_y ORDER BY n DESC LIMIT 4",
            )
            .unwrap();
        let dense: Vec<(u32, u32, i64)> = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i32>(0)? as u32,
                    r.get::<_, i32>(1)? as u32,
                    r.get(2)?,
                ))
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        for (label, set) in [("sparsest", &sparse), ("densest", &dense)] {
            for (x, y, n) in set {
                let _ = render_tile(&conn, 14, *x, *y).unwrap();
                let t = std::time::Instant::now();
                let iters = 5;
                let mut len = 0;
                for _ in 0..iters {
                    len = render_tile(&conn, 14, *x, *y).unwrap().len();
                }
                let ms = t.elapsed().as_secs_f64() * 1000.0 / iters as f64;
                // Gzip too: the store holds the served representation, so the
                // number that matters for disk and bandwidth is the compressed
                // one, and feature order moves it.
                let gz =
                    crate::server::tile_store::prepare(&render_tile(&conn, 14, *x, *y).unwrap())
                        .unwrap()
                        .1
                        .gzip
                        .len();
                println!(
                    "{label:<9} z14/{x}/{y:<6} {n:>7} objects  {ms:8.2} ms  {len:>8} B raw  {gz:>8} B gzip"
                );
            }
        }
    }

    #[test]
    #[ignore]
    fn tile_render_determinism_and_thread_count() {
        let Some(conn) = real_db() else { return };
        // Dense Warsaw z14, a quieter z14, and the z12/z13 parents.
        let tiles = [
            (14u32, 9145u32, 5395u32),
            (14, 9144, 5394),
            (13, 4572, 2697),
            (12, 2286, 1348),
        ];

        for threads in [0u32, 1, 2, 4] {
            if threads > 0 {
                conn.execute_batch(&format!("SET threads={threads}"))
                    .unwrap();
            }
            let label = if threads == 0 {
                "default".to_string()
            } else {
                threads.to_string()
            };
            for (z, x, y) in tiles {
                let (ms, len) = time_renders(&conn, z, x, y, 10);
                let diff = determinism(&conn, z, x, y, 8);
                println!(
                    "threads={label:<7} z{z}/{x}/{y:<6} {ms:8.2} ms  {len:>7} B  nondeterministic {diff}/8"
                );
            }
            println!();
        }
    }
}
