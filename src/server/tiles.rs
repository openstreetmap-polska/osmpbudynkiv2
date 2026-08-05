use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use anyhow::Context;

use super::AppState;
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
const ADDRESSES_MVT_SQL: &str = "
    WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(?, ?, ?, ?)) AS geom)
    SELECT ST_AsMVT(t, 'addresses', 4096, 'geom') AS mvt
    FROM (
        SELECT ST_AsMVTGeom(a.geom, bbox.geom, 4096, 256, true) AS geom,
               a.lokalny_id,
               a.numer_porzadkowy AS housenumber,
               a.miejscowosc AS city
        FROM prg_unmatched a, bbox
        WHERE ST_Intersects(a.geom, ST_MakeEnvelope(?, ?, ?, ?))
    ) t
    WHERE t.geom IS NOT NULL
";

const BUILDINGS_MVT_SQL: &str = "
    WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(?, ?, ?, ?)) AS geom)
    SELECT ST_AsMVT(t, 'buildings', 4096, 'geom') AS mvt
    FROM (
        SELECT ST_AsMVTGeom(raw.geom, bbox.geom, 4096, 256, true) AS geom,
               raw.id, raw.source
        FROM (
            SELECT bdot10k_unmatched.geom, LOKALNYID AS id, 'bdot10k' AS source
            FROM bdot10k_unmatched
            WHERE ST_Intersects(bdot10k_unmatched.geom, ST_MakeEnvelope(?, ?, ?, ?))
            UNION ALL
            SELECT egib_unmatched.geom, id_budynku AS id, 'egib' AS source
            FROM egib_unmatched
            WHERE ST_Intersects(egib_unmatched.geom, ST_MakeEnvelope(?, ?, ?, ?))
        ) raw, bbox
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
// form keeps this on the index for the same reason documented above.
const ALL_ADDRESSES_MVT_SQL: &str = "
    WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(?, ?, ?, ?)) AS geom)
    SELECT ST_AsMVT(t, 'addresses_all', 4096, 'geom') AS mvt
    FROM (
        SELECT ST_AsMVTGeom(a.geom, bbox.geom, 4096, 256, true) AS geom,
               a.lokalny_id,
               a.numer_porzadkowy AS housenumber,
               a.miejscowosc AS city
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
               raw.id, raw.source
        FROM (
            SELECT bdot10k_buildings.geom, LOKALNYID AS id, 'bdot10k' AS source
            FROM bdot10k_buildings
            WHERE ST_Intersects(bdot10k_buildings.geom, ST_MakeEnvelope(?, ?, ?, ?))
            UNION ALL
            SELECT egib_buildings.geom, id_budynku AS id, 'egib' AS source
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

    let result = tokio::task::spawn_blocking(move || {
        let conn = state
            .pool
            .get()
            .context("Failed to acquire pool connection")?;
        // The bbox is repeated once per `?` group: the `bbox` CTE, then one
        // ST_MakeEnvelope per filtered table (one for addresses, two for the
        // buildings UNION). Each group must stay in min_lon, min_lat, max_lon,
        // max_lat order.
        let addresses = query_mvt_layer(
            &conn,
            ADDRESSES_MVT_SQL,
            duckdb::params![
                min_lon, min_lat, max_lon, max_lat, // bbox CTE
                min_lon, min_lat, max_lon, max_lat, // prg_unmatched filter
            ],
        )?;
        let buildings = query_mvt_layer(
            &conn,
            BUILDINGS_MVT_SQL,
            duckdb::params![
                min_lon, min_lat, max_lon, max_lat, // bbox CTE
                min_lon, min_lat, max_lon, max_lat, // bdot10k_unmatched filter
                min_lon, min_lat, max_lon, max_lat, // egib_unmatched filter
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
    use std::sync::Arc;

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use duckdb::Connection;
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
    #[test]
    fn mvt_bbox_filter_uses_the_rtree_index() {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let conn = crate::db::init_db(std::path::Path::new(":memory:"), &init, None).unwrap();
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
                 SELECT ST_Point(20.0 + i*0.0001, 52.0), 'p' || i, '1', NULL, NULL, NULL, NULL, 0, 0, now()
                 FROM range(20000) t(i);
             CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             CREATE INDEX bdot10k_buildings_geom_idx ON bdot10k_buildings USING RTREE (geom);
             INSERT INTO bdot10k_buildings
                 SELECT 'b' || i, ST_MakeEnvelope(20.0 + i*0.0001, 52.0, 20.0 + i*0.0001 + 0.00005, 52.00005)
                 FROM range(20000) t(i);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);
             CREATE INDEX egib_buildings_geom_idx ON egib_buildings USING RTREE (geom);
             INSERT INTO egib_buildings
                 SELECT 'e' || i, ST_MakeEnvelope(20.0 + i*0.0001, 52.0, 20.0 + i*0.0001 + 0.00005, 52.00005)
                 FROM range(20000) t(i);
             CREATE TABLE prg_addresses (lokalny_id VARCHAR, numer_porzadkowy VARCHAR, miejscowosc VARCHAR, geom GEOMETRY);
             CREATE INDEX prg_addresses_geom_idx ON prg_addresses USING RTREE (geom);
             INSERT INTO prg_addresses
                 SELECT 'p' || i, '1', NULL, ST_Point(20.0 + i*0.0001, 52.0)
                 FROM range(20000) t(i);",
        )
        .unwrap();

        let plan_of = |sql: &str, params: &[f64]| -> String {
            let mut stmt = conn.prepare(&format!("EXPLAIN {sql}")).unwrap();
            let owned: Vec<&dyn duckdb::ToSql> =
                params.iter().map(|v| v as &dyn duckdb::ToSql).collect();
            let mut rows = stmt.query(owned.as_slice()).unwrap();
            let mut out = String::new();
            while let Some(row) = rows.next().unwrap() {
                out.push_str(&row.get::<_, String>(1).unwrap_or_default());
            }
            out
        };

        let b = [20.5_f64, 52.0, 20.6, 52.1];
        let addr_params: Vec<f64> = b.iter().chain(b.iter()).copied().collect();
        let bldg_params: Vec<f64> = b.iter().chain(b.iter()).chain(b.iter()).copied().collect();

        let addr_plan = plan_of(ADDRESSES_MVT_SQL, &addr_params);
        assert!(
            addr_plan.contains("RTREE_INDEX_SCAN"),
            "addresses MVT query must use the RTREE index, got plan:\n{addr_plan}"
        );

        let bldg_plan = plan_of(BUILDINGS_MVT_SQL, &bldg_params);
        assert!(
            bldg_plan.contains("RTREE_INDEX_SCAN"),
            "buildings MVT query must use the RTREE index, got plan:\n{bldg_plan}"
        );

        let all_addr_plan = plan_of(ALL_ADDRESSES_MVT_SQL, &addr_params);
        assert!(
            all_addr_plan.contains("RTREE_INDEX_SCAN"),
            "all-addresses MVT query must use the RTREE index, got plan:\n{all_addr_plan}"
        );

        let all_bldg_plan = plan_of(ALL_BUILDINGS_MVT_SQL, &bldg_params);
        assert!(
            all_bldg_plan.contains("RTREE_INDEX_SCAN"),
            "all-buildings MVT query must use the RTREE index, got plan:\n{all_bldg_plan}"
        );
    }

    /// In-memory DB with the government/OSM tables `/tiles` queries touch,
    /// optionally seeded with rows via `seed_sql`.
    fn make_state(seed_sql: &str) -> AppState {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("INSTALL spatial; LOAD spatial; SET GLOBAL geometry_always_xy = true;")
            .unwrap();
        conn.execute_batch(
            "CREATE TABLE prg_unmatched (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, miejscowosc VARCHAR, geom GEOMETRY,
                 cell_x INTEGER, cell_y INTEGER, computed_at TIMESTAMP WITH TIME ZONE);
             CREATE TABLE bdot10k_unmatched (
                 LOKALNYID VARCHAR, geom GEOMETRY,
                 cell_x INTEGER, cell_y INTEGER, computed_at TIMESTAMP WITH TIME ZONE);
             CREATE TABLE egib_unmatched (
                 id_budynku VARCHAR, geom GEOMETRY,
                 cell_x INTEGER, cell_y INTEGER, computed_at TIMESTAMP WITH TIME ZONE);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, miejscowosc VARCHAR, geom GEOMETRY);
             CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);",
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

    #[tokio::test]
    async fn non_z14_returns_no_content() {
        let state = make_state("");
        let response = tiles_app(state)
            .oneshot(
                Request::builder()
                    .uri("/tiles/10/1/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
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
        let response = tiles_app(state)
            .oneshot(
                Request::builder()
                    .uri("/tiles/14/8000/4900")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
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
            "INSERT INTO prg_unmatched VALUES
                 ('a1', '12', 'Warszawa', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());
             INSERT INTO bdot10k_unmatched VALUES
                 ('b1', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());
             INSERT INTO egib_unmatched VALUES
                 ('e1', ST_Point({mid_lon}, {mid_lat}), 8000, 4900, now());
             INSERT INTO prg_addresses VALUES
                 ('a1', '12', 'Warszawa', ST_Point({mid_lon}, {mid_lat}));
             INSERT INTO bdot10k_buildings VALUES
                 ('b1', ST_Point({mid_lon}, {mid_lat}));
             INSERT INTO egib_buildings VALUES
                 ('e1', ST_Point({mid_lon}, {mid_lat}));"
        );
        let state = make_state(&seed);
        let response = tiles_app(state)
            .oneshot(
                Request::builder()
                    .uri("/tiles/14/8000/4900")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
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
        let seed = "INSERT INTO prg_unmatched VALUES
            ('a1', '12', 'Warszawa', ST_Point(0.0, 0.0), 0, 0, now());";
        let state = make_state(seed);
        let response = tiles_app(state)
            .oneshot(
                Request::builder()
                    .uri("/tiles/14/8000/4900")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8_lossy(&bytes);
        assert!(
            !body.contains("Warszawa"),
            "out-of-tile address must not appear"
        );
    }
}
