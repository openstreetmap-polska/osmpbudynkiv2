use std::sync::Arc;

use anyhow::{Context, Result};
use duckdb::Connection;
use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use duckdb::vtab::arrow::WritableVector;

use super::encoding;
use super::kvstore::{self, RocksDB};

/// Shared state passed to the UDF at registration time.
#[derive(Clone)]
pub struct KvState {
    pub kv: Arc<RocksDB>,
}

/// Scalar UDF: `resolve_node_coords(refs LIST<BIGINT>) -> BLOB`
///
/// Takes a list of OSM node IDs, looks up each coordinate in RocksDB,
/// and returns a WKB LineString. Returns NULL if any node is missing
/// or the list has fewer than 2 nodes.
pub struct ResolveNodeCoords;

impl VScalar for ResolveNodeCoords {
    type State = KvState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let num_rows = input.len();
        let input_nulls = input.flat_vector(0);
        let list_vec = input.list_vector(0);
        let mut out = output.flat_vector();

        for i in 0..num_rows {
            if input_nulls.row_is_null(i as u64) {
                out.set_null(i);
                continue;
            }

            let (offset, length) = list_vec.get_entry(i);

            if length < 2 {
                out.set_null(i);
                continue;
            }

            let child = list_vec.child(offset + length);
            let refs = child.as_slice::<i64>();

            let mut raw_coords: Vec<u8> = Vec::with_capacity(length * encoding::NODE_BYTE_LEN);
            let mut all_found = true;

            for j in 0..length {
                match kvstore::get_node_raw(&state.kv, refs[offset + j]) {
                    Ok(Some(bytes)) => raw_coords.extend_from_slice(&bytes),
                    Ok(None) => {
                        all_found = false;
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            if !all_found {
                out.set_null(i);
                continue;
            }

            let wkb = encode_wkb_linestring_raw(length, &raw_coords);
            out.insert(i, &wkb);
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::list(&LogicalTypeHandle::from(
                LogicalTypeId::Bigint,
            ))],
            LogicalTypeHandle::from(LogicalTypeId::Blob),
        )]
    }
}

/// Scalar UDF: `resolve_way_coords(way_id BIGINT) -> BLOB`
///
/// Takes an OSM way ID, looks up its node refs in RocksDB, resolves each
/// node's coordinates, and returns a WKB LineString.
/// Returns NULL if the way is not found, has <2 nodes, or any node is missing.
pub struct ResolveWayCoords;

impl VScalar for ResolveWayCoords {
    type State = KvState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let num_rows = input.len();
        let way_ids_vec = input.flat_vector(0);
        let way_ids = way_ids_vec.as_slice::<i64>();
        let mut out = output.flat_vector();

        for i in 0..num_rows {
            if way_ids_vec.row_is_null(i as u64) {
                out.set_null(i);
                continue;
            }

            let node_ids = match kvstore::get_way(&state.kv, way_ids[i]) {
                Ok(Some(ids)) => ids,
                Ok(None) => {
                    out.set_null(i);
                    continue;
                }
                Err(e) => return Err(e.into()),
            };

            if node_ids.len() < 2 {
                out.set_null(i);
                continue;
            }

            let mut raw_coords: Vec<u8> =
                Vec::with_capacity(node_ids.len() * encoding::NODE_BYTE_LEN);
            let mut all_found = true;

            for &nid in &node_ids {
                match kvstore::get_node_raw(&state.kv, nid) {
                    Ok(Some(bytes)) => raw_coords.extend_from_slice(&bytes),
                    Ok(None) => {
                        all_found = false;
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            if !all_found {
                out.set_null(i);
                continue;
            }

            let wkb = encode_wkb_linestring_raw(node_ids.len(), &raw_coords);
            out.insert(i, &wkb);
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Bigint)],
            LogicalTypeHandle::from(LogicalTypeId::Blob),
        )]
    }
}

/// Register UDFs on the given connection. Call once after opening the DB.
pub fn register_udfs(conn: &Connection, kv: Arc<RocksDB>) -> Result<()> {
    let state = KvState { kv };
    conn.register_scalar_function_with_state::<ResolveNodeCoords>("resolve_node_coords", &state)
        .context("Failed to register resolve_node_coords UDF")?;
    conn.register_scalar_function_with_state::<ResolveWayCoords>("resolve_way_coords", &state)
        .context("Failed to register resolve_way_coords UDF")?;
    Ok(())
}

/// Encode raw node coordinate bytes as a WKB LineString (little-endian, 2D).
/// `raw_coords` is a flat buffer of N * 16 bytes (each 16 bytes = lon LE f64 || lat LE f64).
/// WKB LE LineString layout is identical: header + sequence of (f64 x, f64 y) pairs.
/// So we can copy the raw bytes directly without any float conversion.
fn encode_wkb_linestring_raw(num_points: usize, raw_coords: &[u8]) -> Vec<u8> {
    debug_assert_eq!(raw_coords.len(), num_points * encoding::NODE_BYTE_LEN);
    let mut buf = Vec::with_capacity(9 + raw_coords.len());
    buf.push(0x01); // little-endian
    buf.extend_from_slice(&2u32.to_le_bytes()); // wkbLineString = 2
    buf.extend_from_slice(&(num_points as u32).to_le_bytes());
    buf.extend_from_slice(raw_coords);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Connection, Arc<RocksDB>) {
        let tmp = TempDir::new().unwrap();
        let kv = Arc::new(kvstore::open(tmp.path(), 32, 4).unwrap());

        // Insert some test nodes (a simple square)
        kvstore::put_node(&kv, 1, 20.0, 50.0).unwrap();
        kvstore::put_node(&kv, 2, 21.0, 50.0).unwrap();
        kvstore::put_node(&kv, 3, 21.0, 51.0).unwrap();
        kvstore::put_node(&kv, 4, 20.0, 51.0).unwrap();
        kvstore::put_node(&kv, 5, 20.0, 50.0).unwrap(); // closing node

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("INSTALL spatial; LOAD spatial;")
            .unwrap();
        register_udfs(&conn, kv.clone()).unwrap();

        (tmp, conn, kv)
    }

    #[test]
    fn test_resolve_node_coords_returns_wkb() {
        let (_tmp, conn, _kv) = setup();

        let wkt: String = conn
            .query_row(
                "SELECT ST_AsText(ST_GeomFromWKB(resolve_node_coords([1, 2, 3, 4, 5]::BIGINT[])))",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(wkt, "LINESTRING (20 50, 21 50, 21 51, 20 51, 20 50)");
    }

    #[test]
    fn test_resolve_node_coords_missing_node_returns_null() {
        let (_tmp, conn, _kv) = setup();

        let result: Option<Vec<u8>> = conn
            .query_row(
                "SELECT resolve_node_coords([1, 2, 999]::BIGINT[])",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_node_coords_too_few_refs_returns_null() {
        let (_tmp, conn, _kv) = setup();

        let result: Option<Vec<u8>> = conn
            .query_row("SELECT resolve_node_coords([1]::BIGINT[])", [], |row| {
                row.get(0)
            })
            .unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_node_coords_in_query() {
        let (_tmp, conn, _kv) = setup();

        conn.execute_batch(
            "CREATE TABLE test_ways (id BIGINT, refs BIGINT[]);
             INSERT INTO test_ways VALUES (100, [1, 2, 3, 4, 5]);
             INSERT INTO test_ways VALUES (101, [1, 2, 3]);",
        )
        .unwrap();

        let mut stmt = conn
            .prepare(
                "SELECT id, ST_AsText(ST_GeomFromWKB(resolve_node_coords(refs))) AS geom_wkt
                 FROM test_ways ORDER BY id",
            )
            .unwrap();
        let rows: Vec<(i64, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 100);
        assert_eq!(rows[0].1, "LINESTRING (20 50, 21 50, 21 51, 20 51, 20 50)");
        assert_eq!(rows[1].0, 101);
        assert_eq!(rows[1].1, "LINESTRING (20 50, 21 50, 21 51)");
    }

    #[test]
    fn test_udf_with_large_batch() {
        let tmp = TempDir::new().unwrap();
        let kv = Arc::new(kvstore::open(tmp.path(), 32, 4).unwrap());

        for i in 0..5000i64 {
            kvstore::put_node(&kv, i, 20.0 + (i as f64) * 0.001, 50.0).unwrap();
        }

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("INSTALL spatial; LOAD spatial;")
            .unwrap();
        register_udfs(&conn, kv.clone()).unwrap();

        // Create 3000 ways, each referencing 10 nodes
        // This exceeds the 2048 STANDARD_VECTOR_SIZE that caused the arrow vtab panic
        conn.execute_batch(
            "CREATE TABLE big_ways AS
             SELECT i AS id, list(j) AS refs
             FROM (SELECT i, (i * 3 + s)::BIGINT AS j FROM range(3000) t(i), range(10) ts(s))
             GROUP BY i",
        )
        .unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT count(*)
                 FROM big_ways
                 WHERE resolve_node_coords(refs) IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();

        // Nodes 0..4999, ways reference i*3..i*3+9, max ref = 2999*3+9 = 9006 > 5000
        // So some will be NULL. But many should succeed.
        assert!(count > 0, "expected some resolved ways, got {count}");
    }

    #[test]
    fn test_resolve_way_coords() {
        let (_tmp, conn, kv) = setup();
        kvstore::put_way(&kv, 100, &[1, 2, 3, 4, 5]).unwrap();

        let wkt: String = conn
            .query_row(
                "SELECT ST_AsText(ST_GeomFromWKB(resolve_way_coords(100::BIGINT)))",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(wkt, "LINESTRING (20 50, 21 50, 21 51, 20 51, 20 50)");
    }

    #[test]
    fn test_resolve_way_coords_missing_way_returns_null() {
        let (_tmp, conn, _kv) = setup();

        let result: Option<Vec<u8>> = conn
            .query_row("SELECT resolve_way_coords(999::BIGINT)", [], |row| {
                row.get(0)
            })
            .unwrap();

        assert!(result.is_none());
    }
}
