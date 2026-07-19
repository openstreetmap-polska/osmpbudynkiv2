use std::f64::consts::PI;

use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use anyhow::Context;

use super::AppState;

const ADDRESSES_MVT_SQL: &str = "
    WITH bbox AS (SELECT ST_MakeEnvelope(?, ?, ?, ?) AS geom)
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
    WITH bbox AS (SELECT ST_MakeEnvelope(?, ?, ?, ?) AS geom)
    SELECT ST_AsMVT(t, 'buildings', 4096, 'geom') AS mvt
    FROM (
        SELECT ST_AsMVTGeom(raw.geom, bbox.geom, 4096, 256, true) AS geom,
               raw.id, raw.source
        FROM (
            SELECT geom, LOKALNYID AS id, 'bdot10k' AS source
            FROM bdot10k_buildings, bbox
            WHERE geom && bbox.geom
            UNION ALL
            SELECT geom, id_budynku AS id, 'egib' AS source
            FROM egib_buildings, bbox
            WHERE geom && bbox.geom
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
            .read_pool
            .get()
            .context("Failed to acquire read connection")?;
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

fn tile_to_bbox(z: u32, x: u32, y: u32) -> (f64, f64, f64, f64) {
    let n = 2f64.powi(z as i32);
    let min_lon = x as f64 / n * 360.0 - 180.0;
    let max_lon = (x + 1) as f64 / n * 360.0 - 180.0;
    let max_lat = (PI * (1.0 - 2.0 * y as f64 / n)).sinh().atan() * 180.0 / PI;
    let min_lat = (PI * (1.0 - 2.0 * (y + 1) as f64 / n)).sinh().atan() * 180.0 / PI;
    (min_lon, min_lat, max_lon, max_lat)
}
