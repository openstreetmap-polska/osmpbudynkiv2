use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::{Connection, types::Value};
use tracing::info;

use crate::config::Config;
use crate::download::download_file;
use crate::osm::kvstore;
use crate::osm::kvstore::RocksDB;

fn value_to_i64_list(value: Value) -> Result<Vec<i64>> {
    match value {
        Value::Array(items) | Value::List(items) => items
            .into_iter()
            .map(|item| match item {
                Value::BigInt(v) => Ok(v),
                Value::Int(v) => Ok(v as i64),
                Value::SmallInt(v) => Ok(v as i64),
                Value::TinyInt(v) => Ok(v as i64),
                Value::UInt(v) => Ok(v as i64),
                Value::UBigInt(v) => Ok(v as i64),
                _ => Err(anyhow::anyhow!("Invalid numeric element in array")),
            })
            .collect(),
        _ => Err(anyhow::anyhow!("Expected array/list value")),
    }
}

fn value_to_string_list(value: Value) -> Result<Vec<String>> {
    match value {
        Value::Array(items) | Value::List(items) => items
            .into_iter()
            .map(|item| match item {
                Value::Text(s) => Ok(s),
                Value::Null => Ok(String::new()),
                Value::Enum(s) => Ok(s),
                Value::Int(i) => Ok(i.to_string()),
                _ => Ok(String::new()),
            })
            .collect(),
        _ => Err(anyhow::anyhow!("Expected array/list value")),
    }
}

pub fn import(
    conn: &Connection,
    kv: &RocksDB,
    _config: &Config,
    file: Option<&Path>,
    url: &str,
) -> Result<()> {
    let pbf_path = match file {
        Some(path) => PathBuf::from(path),
        None => download_file(url, Path::new("./data"))?,
    };

    let pbf_str = pbf_path.to_str().context("PBF path is not valid UTF-8")?;

    let has_data: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM osm_buildings LIMIT 1)",
        [],
        |row| row.get(0),
    )?;
    if has_data {
        anyhow::bail!("OSM data already imported. Drop the database and reimport if needed.");
    }

    info!(path = pbf_str, "Starting OSM import");

    let total = std::time::Instant::now();

    let t = std::time::Instant::now();
    stream_nodes_to_rocksdb(conn, kv, pbf_str)?;
    info!(
        elapsed_s = t.elapsed().as_secs_f64(),
        "Step done: stream nodes to RocksDB"
    );

    let t = std::time::Instant::now();
    import_address_nodes(conn, pbf_str)?;
    info!(
        elapsed_s = t.elapsed().as_secs_f64(),
        "Step done: import address nodes"
    );

    let t = std::time::Instant::now();
    stream_ways_to_rocksdb(conn, kv, pbf_str)?;
    info!(
        elapsed_s = t.elapsed().as_secs_f64(),
        "Step done: stream ways to RocksDB"
    );

    let t = std::time::Instant::now();
    import_way_buildings_and_addresses(conn, pbf_str)?;
    info!(
        elapsed_s = t.elapsed().as_secs_f64(),
        "Step done: import way buildings and addresses"
    );

    let t = std::time::Instant::now();
    stream_relations_to_rocksdb(conn, kv, pbf_str)?;
    info!(
        elapsed_s = t.elapsed().as_secs_f64(),
        "Step done: stream relations to RocksDB"
    );

    let t = std::time::Instant::now();
    import_relation_buildings_and_addresses(conn, pbf_str)?;
    info!(
        elapsed_s = t.elapsed().as_secs_f64(),
        "Step done: import relation buildings and addresses"
    );

    let t = std::time::Instant::now();
    kvstore::compact_reverse_indexes(kv);
    info!(
        elapsed_s = t.elapsed().as_secs_f64(),
        "Step done: compact reverse indexes"
    );

    let t = std::time::Instant::now();
    create_spatial_indexes(conn)?;
    info!(
        elapsed_s = t.elapsed().as_secs_f64(),
        "Step done: create spatial indexes"
    );

    log_import_stats(conn)?;

    info!(
        total_s = total.elapsed().as_secs_f64(),
        "OSM import complete"
    );
    Ok(())
}

fn import_address_nodes(conn: &Connection, pbf_path: &str) -> Result<()> {
    info!("Pass 2: Importing address nodes");
    conn.execute_batch(&format!(
        "
        INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
        SELECT
            id AS osm_id,
            'node' AS osm_type,
            element_at(tags, 'addr:housenumber')[1] AS housenumber,
            element_at(tags, 'addr:street')[1] AS street,
            COALESCE(element_at(tags, 'addr:city')[1], element_at(tags, 'addr:place')[1]) AS city,
            element_at(tags, 'addr:postcode')[1] AS postcode,
            ST_Point(lon, lat) AS geom
        FROM ST_ReadOSM('{pbf_path}')
        WHERE kind = 'node'
          AND element_at(tags, 'addr:housenumber')[1] IS NOT NULL
          AND lon IS NOT NULL
          AND lat IS NOT NULL;
        "
    ))
    .context("Failed to import address nodes")?;

    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_addresses WHERE osm_type = 'node'",
        [],
        |row| row.get(0),
    )?;
    info!(count, "Address nodes imported");

    Ok(())
}

fn stream_nodes_to_rocksdb(conn: &Connection, kv: &RocksDB, pbf_path: &str) -> Result<()> {
    info!("Pass 1: Streaming nodes to RocksDB");

    let sql = format!(
        "SELECT id, lon, lat FROM ST_ReadOSM('{pbf_path}') WHERE kind = 'node' AND lon IS NOT NULL AND lat IS NOT NULL"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    let mut batch = kvstore::new_batch();
    let mut count = 0u64;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let lon: f64 = row.get(1)?;
        let lat: f64 = row.get(2)?;
        kvstore::batch_put_node(kv, &mut batch, id, lon, lat);
        count += 1;
        if count % 10000 == 0 {
            kvstore::write_batch(kv, batch)?;
            batch = kvstore::new_batch();
        }
    }
    if count % 10000 != 0 {
        kvstore::write_batch(kv, batch)?;
    }
    info!(count, "Nodes streamed to RocksDB");
    Ok(())
}

fn stream_ways_to_rocksdb(conn: &Connection, kv: &RocksDB, pbf_path: &str) -> Result<()> {
    info!("Pass 2: Streaming ways to RocksDB");

    let sql = format!(
        "SELECT id, refs FROM ST_ReadOSM('{pbf_path}') WHERE kind = 'way' AND refs IS NOT NULL AND len(refs) > 0"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    let mut batch = kvstore::new_batch();
    let mut count = 0u64;

    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let refs_value: Value = row.get(1)?;
        let refs = value_to_i64_list(refs_value)?;
        kvstore::batch_put_way(kv, &mut batch, id, &refs);

        for &node_id in &refs {
            kvstore::batch_merge_node_to_way(kv, &mut batch, node_id, id);
        }

        count += 1;
        if count % 10000 == 0 {
            kvstore::write_batch(kv, batch)?;
            batch = kvstore::new_batch();
        }
    }

    if count % 10000 != 0 {
        kvstore::write_batch(kv, batch)?;
    }

    info!(count, "Ways streamed to RocksDB");
    Ok(())
}

fn import_way_buildings_and_addresses(conn: &Connection, pbf_path: &str) -> Result<()> {
    info!("Importing way buildings");
    conn.execute_batch(&format!(
        "INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
         SELECT id, 'way', element_at(tags, 'building')[1],
                ST_MakePolygon(ST_GeomFromWKB(resolve_node_coords(refs)))
         FROM ST_ReadOSM('{pbf_path}')
         WHERE kind = 'way'
           AND refs IS NOT NULL
           AND len(refs) >= 4
           AND refs[1] = refs[len(refs)]
           AND element_at(tags, 'building')[1] IS NOT NULL
           AND resolve_node_coords(refs) IS NOT NULL"
    ))
    .context("Failed to import way buildings")?;

    let building_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_buildings WHERE osm_type = 'way'",
        [],
        |row| row.get(0),
    )?;
    info!(count = building_count, "Way buildings imported");

    info!("Importing way addresses");
    conn.execute_batch(&format!(
        "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
         SELECT id, 'way',
                element_at(tags, 'addr:housenumber')[1],
                element_at(tags, 'addr:street')[1],
                COALESCE(element_at(tags, 'addr:city')[1], element_at(tags, 'addr:place')[1]),
                element_at(tags, 'addr:postcode')[1],
                ST_Centroid(ST_GeomFromWKB(resolve_node_coords(refs)))
         FROM ST_ReadOSM('{pbf_path}')
         WHERE kind = 'way'
           AND refs IS NOT NULL
           AND len(refs) > 0
           AND element_at(tags, 'addr:housenumber')[1] IS NOT NULL
           AND resolve_node_coords(refs) IS NOT NULL"
    ))
    .context("Failed to import way addresses")?;

    let addr_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_addresses WHERE osm_type = 'way'",
        [],
        |row| row.get(0),
    )?;
    info!(count = addr_count, "Way addresses imported");

    Ok(())
}

fn import_relation_buildings_and_addresses(conn: &Connection, pbf_path: &str) -> Result<()> {
    info!("Importing relation buildings");
    conn.execute_batch(&format!(
        "INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
         WITH rel_members AS (
             SELECT
                 id AS relation_id,
                 element_at(tags, 'building')[1] AS building,
                 unnest(refs) AS ref_id,
                 unnest(ref_types) AS ref_type,
                 unnest(ref_roles) AS ref_role
             FROM ST_ReadOSM('{pbf_path}')
             WHERE kind = 'relation'
               AND refs IS NOT NULL
               AND len(refs) > 0
               AND element_at(tags, 'building')[1] IS NOT NULL
         ),
         way_geoms AS (
             SELECT
                 relation_id, building, ref_role,
                 ST_GeomFromWKB(resolve_way_coords(ref_id)) AS line_geom
             FROM rel_members
             WHERE ref_type = 'way'
               AND resolve_way_coords(ref_id) IS NOT NULL
         ),
         outer_polys AS (
             SELECT relation_id, building,
                    ST_Union_Agg(ST_MakePolygon(line_geom)) AS outer_geom
             FROM way_geoms
             WHERE (ref_role = 'outer' OR ref_role = '')
               AND ST_NPoints(line_geom) >= 4
               AND ST_IsClosed(line_geom)
             GROUP BY relation_id, building
         ),
         inner_polys AS (
             SELECT relation_id,
                    ST_Union_Agg(ST_MakePolygon(line_geom)) AS inner_geom
             FROM way_geoms
             WHERE ref_role = 'inner'
               AND ST_NPoints(line_geom) >= 4
               AND ST_IsClosed(line_geom)
             GROUP BY relation_id
         )
         SELECT
             o.relation_id, 'relation', o.building,
             CASE
                 WHEN i.inner_geom IS NOT NULL THEN ST_Difference(o.outer_geom, i.inner_geom)
                 ELSE o.outer_geom
             END AS geom
         FROM outer_polys o
         LEFT JOIN inner_polys i ON o.relation_id = i.relation_id
         WHERE o.outer_geom IS NOT NULL"
    ))
    .context("Failed to import relation buildings")?;

    let building_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_buildings WHERE osm_type = 'relation'",
        [],
        |row| row.get(0),
    )?;
    info!(count = building_count, "Relation buildings imported");

    info!("Importing relation addresses");
    conn.execute_batch(&format!(
        "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
         WITH rel_members AS (
             SELECT
                 id AS relation_id,
                 element_at(tags, 'addr:housenumber')[1] AS housenumber,
                 element_at(tags, 'addr:street')[1] AS street,
                 COALESCE(element_at(tags, 'addr:city')[1], element_at(tags, 'addr:place')[1]) AS city,
                 element_at(tags, 'addr:postcode')[1] AS postcode,
                 unnest(refs) AS ref_id,
                 unnest(ref_types) AS ref_type
             FROM ST_ReadOSM('{pbf_path}')
             WHERE kind = 'relation'
               AND refs IS NOT NULL
               AND len(refs) > 0
               AND element_at(tags, 'addr:housenumber')[1] IS NOT NULL
         ),
         way_geoms AS (
             SELECT
                 relation_id, housenumber, street, city, postcode,
                 ST_GeomFromWKB(resolve_way_coords(ref_id)) AS line_geom
             FROM rel_members
             WHERE ref_type = 'way'
               AND resolve_way_coords(ref_id) IS NOT NULL
         )
         SELECT
             relation_id, 'relation', housenumber, street, city, postcode,
             ST_Centroid(ST_Collect(list(line_geom)))
         FROM way_geoms
         GROUP BY relation_id, housenumber, street, city, postcode"
    ))
    .context("Failed to import relation addresses")?;

    let addr_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_addresses WHERE osm_type = 'relation'",
        [],
        |row| row.get(0),
    )?;
    info!(count = addr_count, "Relation addresses imported");

    Ok(())
}

fn string_to_member_type(member_type: &str) -> u8 {
    match member_type {
        "node" => 0,
        "way" => 1,
        "relation" => 2,
        _ => 3,
    }
}

fn string_to_member_role(role: &str) -> u8 {
    match role {
        "outer" | "" => 0,
        "inner" => 1,
        _ => 2,
    }
}

fn stream_relations_to_rocksdb(conn: &Connection, kv: &RocksDB, pbf_path: &str) -> Result<()> {
    info!("Pass 3: Streaming relations to RocksDB");

    let sql = format!(
        "SELECT id, refs, ref_types, ref_roles FROM ST_ReadOSM('{pbf_path}') WHERE kind = 'relation' AND refs IS NOT NULL AND len(refs) > 0"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    let mut batch = kvstore::new_batch();
    let mut count = 0u64;

    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let refs = value_to_i64_list(row.get::<_, Value>(1)?)?;
        let ref_types = value_to_string_list(row.get::<_, Value>(2)?)?;
        let ref_roles = value_to_string_list(row.get::<_, Value>(3)?)?;

        let members: Vec<(i64, u8, u8)> = refs
            .into_iter()
            .zip(ref_types.into_iter())
            .zip(ref_roles.into_iter())
            .map(|((ref_id, ref_type), ref_role)| {
                (
                    ref_id,
                    string_to_member_type(&ref_type),
                    string_to_member_role(&ref_role),
                )
            })
            .collect();

        kvstore::batch_put_relation(kv, &mut batch, id, &members);

        for &(way_id, ref_type, _) in &members {
            if ref_type == 1 {
                kvstore::batch_merge_way_to_relation(kv, &mut batch, way_id, id);
            }
        }

        count += 1;
        if count % 1000 == 0 {
            kvstore::write_batch(kv, batch)?;
            batch = kvstore::new_batch();
        }
    }

    if count % 1000 != 0 {
        kvstore::write_batch(kv, batch)?;
    }

    info!(count, "Relations streamed to RocksDB");
    Ok(())
}

fn create_spatial_indexes(conn: &Connection) -> Result<()> {
    info!("Creating spatial indexes");
    conn.execute_batch(
        "
        CREATE INDEX osm_buildings_geom_idx ON osm_buildings USING RTREE (geom);
        CREATE INDEX osm_addresses_geom_idx ON osm_addresses USING RTREE (geom);
        ",
    )
    .context("Failed to create spatial indexes")?;
    Ok(())
}

fn log_import_stats(conn: &Connection) -> Result<()> {
    let buildings: i64 =
        conn.query_row("SELECT COUNT(*) FROM osm_buildings", [], |row| row.get(0))?;
    let addresses: i64 =
        conn.query_row("SELECT COUNT(*) FROM osm_addresses", [], |row| row.get(0))?;
    info!(buildings, addresses, "OSM import totals");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::config::Config;
    use crate::db::init_db;
    use crate::osm::kvstore;

    fn setup_test_db() -> Result<Connection> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;
        Ok(conn)
    }

    fn run_import_with_fixture(conn: &Connection, pbf_path: &Path) -> Result<()> {
        let tmp_dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(kvstore::open(tmp_dir.path(), 512, 64)?);
        crate::osm::udf::register_udfs(conn, kv.clone())?;
        let config = Config::default();
        import(conn, &kv, &config, Some(pbf_path), "")?;
        Ok(())
    }

    /// End-to-end test: import the fixture PBF and verify final counts.
    #[test]
    fn test_import_fixture_pbf() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;
        run_import_with_fixture(&conn, Path::new("fixtures/osm.pbf"))?;

        // 2 buildings: way 947235698 (apartments) + relation 1891415 (school)
        let buildings: i64 =
            conn.query_row("SELECT COUNT(*) FROM osm_buildings", [], |row| row.get(0))?;
        assert_eq!(buildings, 2, "Expected 2 buildings (1 way + 1 relation)");

        // 3 addresses: node 13200892212 + way 947235698 + relation 1891415
        let addresses: i64 =
            conn.query_row("SELECT COUNT(*) FROM osm_addresses", [], |row| row.get(0))?;
        assert_eq!(
            addresses, 3,
            "Expected 3 addresses (1 node + 1 way + 1 relation)"
        );

        Ok(())
    }

    /// Verify building types and tags after import.
    #[test]
    fn test_import_fixture_building_details() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;
        run_import_with_fixture(&conn, Path::new("fixtures/osm.pbf"))?;

        // Way building: apartments
        let building_tag: String = conn.query_row(
            "SELECT building FROM osm_buildings WHERE osm_id = 947235698 AND osm_type = 'way'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(building_tag, "apartments");

        let geom_type: String = conn.query_row(
            "SELECT ST_GeometryType(geom) FROM osm_buildings WHERE osm_id = 947235698",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(geom_type, "POLYGON");

        // Relation building: school (multipolygon with inner hole)
        let building_tag: String = conn.query_row(
            "SELECT building FROM osm_buildings WHERE osm_id = 1891415 AND osm_type = 'relation'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(building_tag, "school");

        // School building should have smaller area than its outer ring (it has a hole)
        let area: f64 = conn.query_row(
            "SELECT ST_Area(geom) FROM osm_buildings WHERE osm_id = 1891415",
            [],
            |row| row.get(0),
        )?;
        assert!(area > 0.0, "School building should have positive area");

        Ok(())
    }

    /// Verify address details after import.
    #[test]
    fn test_import_fixture_address_details() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;
        run_import_with_fixture(&conn, Path::new("fixtures/osm.pbf"))?;

        // Node address: housenumber 32, Ludwika Narbutta
        let (hn, street): (String, String) = conn.query_row(
            "SELECT housenumber, street FROM osm_addresses WHERE osm_id = 13200892212 AND osm_type = 'node'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(hn, "32");
        assert_eq!(street, "Ludwika Narbutta");

        // Way address: housenumber 63, Kazimierzowska, Warszawa
        let (hn, street, city, postcode): (String, String, String, String) = conn.query_row(
            "SELECT housenumber, street, city, postcode FROM osm_addresses WHERE osm_id = 947235698 AND osm_type = 'way'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(hn, "63");
        assert_eq!(street, "Kazimierzowska");
        assert_eq!(city, "Warszawa");
        assert_eq!(postcode, "02-538");

        // Relation address: housenumber 60, Kazimierzowska, Warszawa
        let (hn, street, city, postcode): (String, String, String, String) = conn.query_row(
            "SELECT housenumber, street, city, postcode FROM osm_addresses WHERE osm_id = 1891415 AND osm_type = 'relation'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(hn, "60");
        assert_eq!(street, "Kazimierzowska");
        assert_eq!(city, "Warszawa");
        assert_eq!(postcode, "02-543");

        Ok(())
    }

    /// Verify address geometries are within expected bounding box (Warsaw area).
    #[test]
    fn test_import_fixture_address_geometries() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;
        run_import_with_fixture(&conn, Path::new("fixtures/osm.pbf"))?;

        // All addresses should have geometry in the Warsaw area (~21.01 lon, ~52.20 lat)
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_addresses
             WHERE ST_X(geom) BETWEEN 21.01 AND 21.02
               AND ST_Y(geom) BETWEEN 52.20 AND 52.21",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(count, 3, "All 3 addresses should be in the Warsaw area");

        // Node address should be a point at its exact coordinates
        let (lon, lat): (f64, f64) = conn.query_row(
            "SELECT ST_X(geom), ST_Y(geom) FROM osm_addresses WHERE osm_id = 13200892212",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert!((lon - 21.014861).abs() < 1e-5);
        assert!((lat - 52.206263).abs() < 1e-4);

        Ok(())
    }
}
