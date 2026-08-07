use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use anyhow::Context;

use super::AppState;
use super::package::{ADJACENCY_READ_BUFFER_DEG, BDOT10K_ADJACENCY_KEY, EGIB_ADJACENCY_KEY};
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
const ADDRESSES_MVT_SQL: &str = "
    WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(?, ?, ?, ?)) AS geom),
    candidates AS MATERIALIZED (
        SELECT a.geom, a.lokalny_id, a.numer_porzadkowy, a.ulica, a.miejscowosc,
               a.kod_pocztowy, a.teryt_miejscowosc, a.wazny_od_lub_data_nadania
        FROM prg_unmatched a
        WHERE ST_Intersects(a.geom, ST_MakeEnvelope(?, ?, ?, ?))
    ),
    resolved AS (
        SELECT candidates.*,
               NULLIF(trim(COALESCE(loc.osm_street_name, gl.osm_street_name, candidates.ulica)), '') AS resolved_street
        FROM candidates
        LEFT JOIN street_name_mappings loc
               ON lower(trim(loc.prg_street_name)) = lower(trim(candidates.ulica))
              AND loc.teryt_simc_code = candidates.teryt_miejscowosc
        LEFT JOIN street_name_mappings gl
               ON lower(trim(gl.prg_street_name)) = lower(trim(candidates.ulica))
              AND gl.teryt_simc_code IS NULL
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
";

const BUILDINGS_MVT_SQL: &str = "
    WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(?, ?, ?, ?)) AS geom),
    bdot10k_pkg AS MATERIALIZED (
        SELECT b.rowid AS rid, b.LOKALNYID AS id, b.geom,
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
        SELECT pkg.geom, 'bdot10k' AS source, pkg.id,
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
        SELECT pkg.geom, 'egib' AS source, pkg.id,
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
const ALL_ADDRESSES_MVT_SQL: &str = "
    WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(?, ?, ?, ?)) AS geom)
    SELECT ST_AsMVT(t, 'addresses_all', 4096, 'geom') AS mvt
    FROM (
        SELECT ST_AsMVTGeom(a.geom, bbox.geom, 4096, 256, true) AS geom,
               a.lokalny_id,
               a.numer_porzadkowy,
               a.ulica,
               a.miejscowosc,
               a.kod_pocztowy,
               a.wazny_od_lub_data_nadania::VARCHAR AS wazny_od_lub_data_nadania
        FROM prg_addresses a, bbox
        WHERE ST_Intersects(a.geom, ST_MakeEnvelope(?, ?, ?, ?))
    ) t
    WHERE t.geom IS NOT NULL
";

const ALL_BUILDINGS_MVT_SQL: &str = "
    WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(?, ?, ?, ?)) AS geom)
    SELECT ST_AsMVT(t, 'buildings_all', 4096, 'geom') AS mvt
    FROM (
        SELECT ST_AsMVTGeom(raw.geom, bbox.geom, 4096, 256, true) AS geom,
               raw.id, raw.source, raw.PRZEWAZAJACAFUNKCJABUDYNKU, raw.FUNKCJAOGOLNABUDYNKU,
               raw.levels_above_ground, raw.KATEGORIAISTNIENIA, raw.NAZWA, raw.FSBUD,
               raw.INFORMACJADODATKOWA, raw.KODKST, raw.ZRODLODANYCHGEOMETRYCZNYCH,
               raw.kondygnacje_podziemne, raw.rodzaj
        FROM (
            SELECT bdot10k_buildings.geom, LOKALNYID AS id, 'bdot10k' AS source,
                   PRZEWAZAJACAFUNKCJABUDYNKU, FUNKCJAOGOLNABUDYNKU,
                   LICZBAKONDYGNACJI::INTEGER AS levels_above_ground,
                   KATEGORIAISTNIENIA, NAZWA, FSBUD, INFORMACJADODATKOWA,
                   KODKST::INTEGER AS KODKST, ZRODLODANYCHGEOMETRYCZNYCH,
                   NULL::INTEGER AS kondygnacje_podziemne, NULL::VARCHAR AS rodzaj
            FROM bdot10k_buildings
            WHERE ST_Intersects(bdot10k_buildings.geom, ST_MakeEnvelope(?, ?, ?, ?))
            UNION ALL
            SELECT egib_buildings.geom, id_budynku AS id, 'egib' AS source,
                   NULL::VARCHAR AS PRZEWAZAJACAFUNKCJABUDYNKU, NULL::VARCHAR AS FUNKCJAOGOLNABUDYNKU,
                   kondygnacje_nadziemne AS levels_above_ground,
                   NULL::VARCHAR AS KATEGORIAISTNIENIA, NULL::VARCHAR AS NAZWA, NULL::VARCHAR AS FSBUD,
                   NULL::VARCHAR AS INFORMACJADODATKOWA, NULL::INTEGER AS KODKST,
                   NULL::VARCHAR AS ZRODLODANYCHGEOMETRYCZNYCH,
                   kondygnacje_podziemne, rodzaj
            FROM egib_buildings
            WHERE ST_Intersects(egib_buildings.geom, ST_MakeEnvelope(?, ?, ?, ?))
        ) raw, bbox
    ) t
    WHERE t.geom IS NOT NULL
";

pub async fn serve_tile(
    State(state): State<AppState>,
    Path((z, x, y)): Path<(u32, u32, u32)>,
) -> Response {
    if z != 14 {
        return StatusCode::NO_CONTENT.into_response();
    }

    let (min_lon, min_lat, max_lon, max_lat) = tile_to_bbox(z, x, y);
    let (buf_min_lon, buf_min_lat, buf_max_lon, buf_max_lat) = (
        min_lon - ADJACENCY_READ_BUFFER_DEG,
        min_lat - ADJACENCY_READ_BUFFER_DEG,
        max_lon + ADJACENCY_READ_BUFFER_DEG,
        max_lat + ADJACENCY_READ_BUFFER_DEG,
    );

    let result = tokio::task::spawn_blocking(move || {
        let conn = state
            .pool
            .get()
            .context("Failed to acquire pool connection")?;
        // The bbox is repeated once per `?` group: the `bbox` CTE, then one
        // ST_MakeEnvelope per filtered table. Each group must stay in
        // min_lon, min_lat, max_lon, max_lat order.
        let addresses = query_mvt_layer(
            &conn,
            ADDRESSES_MVT_SQL,
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
            ALL_ADDRESSES_MVT_SQL,
            duckdb::params![
                min_lon, min_lat, max_lon, max_lat, // bbox CTE
                min_lon, min_lat, max_lon, max_lat, // prg_addresses filter
            ],
        )?;
        let buildings_all = query_mvt_layer(
            &conn,
            ALL_BUILDINGS_MVT_SQL,
            duckdb::params![
                min_lon, min_lat, max_lon, max_lat, // bbox CTE
                min_lon, min_lat, max_lon, max_lat, // bdot10k_buildings filter
                min_lon, min_lat, max_lon, max_lat, // egib_buildings filter
            ],
        )?;
        Ok::<Vec<u8>, anyhow::Error>([addresses, buildings, addresses_all, buildings_all].concat())
    })
    .await;

    match result {
        Ok(Ok(bytes)) if bytes.is_empty() => StatusCode::NO_CONTENT.into_response(),
        Ok(Ok(bytes)) => {
            let mut resp = bytes.into_response();
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/vnd.mapbox-vector-tile"),
            );
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

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;
    use crate::server::{build_pool, jobs::JobRegistry};

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
                 LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
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
                 kod_pocztowy VARCHAR, wazny_od_lub_data_nadania DATE, geom GEOMETRY);
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
        let addr_plan = plan_of(ADDRESSES_MVT_SQL, &addr_params_dyn);
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

        let all_addr_plan = plan_of(ALL_ADDRESSES_MVT_SQL, &addr_params_dyn);
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
        let all_bldg_plan = plan_of(ALL_BUILDINGS_MVT_SQL, &all_bldg_params_dyn);
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
                 wazny_od_lub_data_nadania DATE, geom GEOMETRY);
             CREATE TABLE bdot10k_buildings (
                 LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
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
        AppState {
            pool,
            registry: Arc::new(JobRegistry::new_for_tests(vec![])),
            config: Arc::new(crate::config::Config::default()),
        }
    }

    fn tiles_app(state: AppState) -> Router {
        Router::new()
            .route("/tiles/{z}/{x}/{y}", axum::routing::get(serve_tile))
            .with_state(state)
    }

    async fn request_tile(state: AppState, z: u32, x: u32, y: u32) -> Response {
        tiles_app(state)
            .oneshot(
                Request::builder()
                    .uri(format!("/tiles/{z}/{x}/{y}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn non_z14_returns_no_content() {
        let state = make_state("");
        let response = request_tile(state, 10, 1, 1).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
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
                  teryt_miejscowosc, wazny_od_lub_data_nadania, geom, cell_x, cell_y, computed_at)
             VALUES
                 ('a1', '12', 'Marszalkowska', 'Warszawa', '00-590', '0918123',
                  DATE '2012-04-27', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());
             INSERT INTO prg_addresses
                 (lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
                  wazny_od_lub_data_nadania, geom)
             VALUES
                 ('a1', '12', 'Marszalkowska', 'Warszawa', '00-590', DATE '2012-04-27',
                  ST_Point({mid_lon}, {mid_lat}));
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
}
