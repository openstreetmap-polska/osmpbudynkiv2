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
const ADDRESSES_MVT_SQL: &str = "
    WITH bbox AS (SELECT ST_Extent(ST_MakeEnvelope(?, ?, ?, ?)) AS geom)
    SELECT ST_AsMVT(t, 'addresses', 4096, 'geom') AS mvt
    FROM (
        SELECT ST_AsMVTGeom(a.geom, bbox.geom, 4096, 256, true) AS geom,
               a.lokalny_id,
               a.numer_porzadkowy AS housenumber,
               a.miejscowosc AS city
        FROM prg_addresses a, bbox
        WHERE a.geom && bbox.geom
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
            SELECT bdot10k_buildings.geom, LOKALNYID AS id, 'bdot10k' AS source
            FROM bdot10k_buildings, bbox
            WHERE bdot10k_buildings.geom && bbox.geom
            UNION ALL
            SELECT egib_buildings.geom, id_budynku AS id, 'egib' AS source
            FROM egib_buildings, bbox
            WHERE egib_buildings.geom && bbox.geom
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
        let bbox = duckdb::params![min_lon, min_lat, max_lon, max_lat];
        let addresses = query_mvt_layer(&conn, ADDRESSES_MVT_SQL, bbox)?;
        let buildings = query_mvt_layer(&conn, BUILDINGS_MVT_SQL, bbox)?;
        Ok::<Vec<u8>, anyhow::Error>([addresses, buildings].concat())
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

    /// In-memory DB with the government/OSM tables `/tiles` queries touch,
    /// optionally seeded with rows via `seed_sql`.
    fn make_state(seed_sql: &str) -> AppState {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("INSTALL spatial; LOAD spatial; SET GLOBAL geometry_always_xy = true;")
            .unwrap();
        conn.execute_batch(
            "CREATE TABLE prg_addresses (
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
            "INSERT INTO prg_addresses VALUES
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
        assert!(body.contains("bdot10k"), "missing bdot10k source tag");
        assert!(body.contains("egib"), "missing egib source tag");
    }

    #[tokio::test]
    async fn tile_with_no_nearby_data_returns_ok_with_no_matching_features() {
        // Data exists, but nowhere near the requested tile.
        let seed = "INSERT INTO prg_addresses VALUES
            ('a1', '12', 'Warszawa', ST_Point(0.0, 0.0));";
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
