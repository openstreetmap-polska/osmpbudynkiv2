use std::sync::Arc;

use anyhow::{Context, Result};
use duckdb::Connection;
use duckdb::arrow::array::{
    Array, Float64Array, Int64Array, ListArray, StringArray, StringBuilder,
};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::vtab::arrow::arrow_recordbatch_to_query_params;
use tracing::info;

use super::kvstore::{self, RocksDB};

/// Build way geometries (buildings and addresses) by reading tagged ways from PBF,
/// resolving node coordinates from RocksDB, and inserting into DuckDB via Arrow batches.
pub fn build_way_geometries(conn: &Connection, kv: &RocksDB, pbf_path: &str) -> Result<()> {
    info!("Building building geometries from ways (batched)");

    let mut stmt = conn.prepare(&format!(
        "SELECT id, refs, element_at(tags, 'building')[1] AS building,
                element_at(tags, 'addr:housenumber')[1] AS housenumber,
                element_at(tags, 'addr:street')[1] AS street,
                COALESCE(element_at(tags, 'addr:city')[1], element_at(tags, 'addr:place')[1]) AS city,
                element_at(tags, 'addr:postcode')[1] AS postcode
         FROM ST_ReadOSM('{pbf_path}')
         WHERE kind = 'way'
           AND refs IS NOT NULL
           AND len(refs) > 0
           AND (element_at(tags, 'building')[1] IS NOT NULL
                OR element_at(tags, 'addr:housenumber')[1] IS NOT NULL)"
    ))?;
    let batches = stmt.query_arrow([])?;

    let mut building_count: u64 = 0;
    let mut address_count: u64 = 0;

    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("id")?;
        let refs_list = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .context("refs")?;

        let building_col = batch.column(2);
        let hn_col = batch.column(3);
        let street_col = batch.column(4);
        let city_col = batch.column(5);
        let postcode_col = batch.column(6);

        // Flat: one row per (way, node) pair
        let mut flat_way_ids: Vec<i64> = Vec::new();
        let mut flat_buildings: Vec<Option<String>> = Vec::new();
        let mut flat_housenumbers: Vec<Option<String>> = Vec::new();
        let mut flat_streets: Vec<Option<String>> = Vec::new();
        let mut flat_cities: Vec<Option<String>> = Vec::new();
        let mut flat_postcodes: Vec<Option<String>> = Vec::new();
        let mut flat_lons: Vec<f64> = Vec::new();
        let mut flat_lats: Vec<f64> = Vec::new();

        for i in 0..batch.num_rows() {
            let way_id = ids.value(i);
            let refs_arr = refs_list.value(i);
            let refs = refs_arr.as_any().downcast_ref::<Int64Array>().unwrap();

            let mut lons = Vec::with_capacity(refs.len());
            let mut lats = Vec::with_capacity(refs.len());
            let mut all_found = true;

            for j in 0..refs.len() {
                match kvstore::get_node(kv, refs.value(j))? {
                    Some((lon, lat)) => {
                        lons.push(lon);
                        lats.push(lat);
                    }
                    None => {
                        all_found = false;
                        break;
                    }
                }
            }

            if !all_found || lons.is_empty() {
                continue;
            }

            let building = nullable_string(building_col, i);
            let housenumber = nullable_string(hn_col, i);
            let street = nullable_string(street_col, i);
            let city = nullable_string(city_col, i);
            let postcode = nullable_string(postcode_col, i);

            for k in 0..lons.len() {
                flat_way_ids.push(way_id);
                flat_buildings.push(building.clone());
                flat_housenumbers.push(housenumber.clone());
                flat_streets.push(street.clone());
                flat_cities.push(city.clone());
                flat_postcodes.push(postcode.clone());
                flat_lons.push(lons[k]);
                flat_lats.push(lats[k]);
            }
        }

        if flat_way_ids.is_empty() {
            continue;
        }

        let rb = build_way_record_batch(
            &flat_way_ids,
            &flat_buildings,
            &flat_housenumbers,
            &flat_streets,
            &flat_cities,
            &flat_postcodes,
            &flat_lons,
            &flat_lats,
        )?;

        building_count += insert_way_buildings(conn, &rb)?;
        address_count += insert_way_addresses(conn, &rb)?;
    }

    info!(count = building_count, "Way buildings imported");
    info!(count = address_count, "Way addresses imported");
    Ok(())
}

fn nullable_string(col: &dyn Array, i: usize) -> Option<String> {
    if col.is_null(i) {
        return None;
    }
    use duckdb::arrow::array::LargeStringArray;
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        Some(arr.value(i).to_string())
    } else if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
        Some(arr.value(i).to_string())
    } else {
        None
    }
}

/// Flat schema: one row per node coordinate.
/// Columns: way_id, building, housenumber, street, city, postcode, lon, lat
fn build_way_record_batch(
    way_ids: &[i64],
    buildings: &[Option<String>],
    housenumbers: &[Option<String>],
    streets: &[Option<String>],
    cities: &[Option<String>],
    postcodes: &[Option<String>],
    lons: &[f64],
    lats: &[f64],
) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("way_id", DataType::Int64, false),
        Field::new("building", DataType::Utf8, true),
        Field::new("housenumber", DataType::Utf8, true),
        Field::new("street", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("postcode", DataType::Utf8, true),
        Field::new("lon", DataType::Float64, false),
        Field::new("lat", DataType::Float64, false),
    ]));

    let mut b_builder = StringBuilder::new();
    let mut hn_builder = StringBuilder::new();
    let mut st_builder = StringBuilder::new();
    let mut ci_builder = StringBuilder::new();
    let mut pc_builder = StringBuilder::new();
    for i in 0..way_ids.len() {
        match &buildings[i] {
            Some(v) => b_builder.append_value(v),
            None => b_builder.append_null(),
        }
        match &housenumbers[i] {
            Some(v) => hn_builder.append_value(v),
            None => hn_builder.append_null(),
        }
        match &streets[i] {
            Some(v) => st_builder.append_value(v),
            None => st_builder.append_null(),
        }
        match &cities[i] {
            Some(v) => ci_builder.append_value(v),
            None => ci_builder.append_null(),
        }
        match &postcodes[i] {
            Some(v) => pc_builder.append_value(v),
            None => pc_builder.append_null(),
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(way_ids.to_vec())),
            Arc::new(b_builder.finish()),
            Arc::new(hn_builder.finish()),
            Arc::new(st_builder.finish()),
            Arc::new(ci_builder.finish()),
            Arc::new(pc_builder.finish()),
            Arc::new(Float64Array::from(lons.to_vec())),
            Arc::new(Float64Array::from(lats.to_vec())),
        ],
    )
    .context("Failed to build way RecordBatch")
}

fn insert_way_buildings(conn: &Connection, rb: &RecordBatch) -> Result<u64> {
    let params = arrow_recordbatch_to_query_params(rb.clone());
    let changed = conn.execute(
        "INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
         WITH way_lines AS (
             SELECT way_id, building,
                    ST_MakeLine(list(ST_Point(lon, lat))) AS line_geom
             FROM arrow(?, ?)
             WHERE building IS NOT NULL
             GROUP BY way_id, building
             HAVING COUNT(*) >= 4
         )
         SELECT way_id AS osm_id, 'way' AS osm_type, building,
                ST_MakePolygon(line_geom) AS geom
         FROM way_lines
         WHERE ST_NPoints(line_geom) >= 4",
        params,
    )?;
    Ok(changed as u64)
}

fn insert_way_addresses(conn: &Connection, rb: &RecordBatch) -> Result<u64> {
    let params = arrow_recordbatch_to_query_params(rb.clone());
    let changed = conn.execute(
        "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
         SELECT
             way_id AS osm_id, 'way' AS osm_type,
             housenumber, street, city, postcode,
             ST_Point(AVG(lon), AVG(lat)) AS geom
         FROM arrow(?, ?)
         WHERE housenumber IS NOT NULL
         GROUP BY way_id, housenumber, street, city, postcode",
        params,
    )?;
    Ok(changed as u64)
}

/// Build relation geometries (multipolygon buildings and addresses) by reading tagged relations
/// from PBF, resolving member way coordinates from RocksDB, and inserting into DuckDB.
pub fn build_relation_geometries(conn: &Connection, kv: &RocksDB, pbf_path: &str) -> Result<()> {
    info!("Building building geometries from relations (batched)");

    let mut stmt = conn.prepare(&format!(
        "SELECT id, refs, ref_types::VARCHAR[], ref_roles, tags
         FROM ST_ReadOSM('{pbf_path}')
         WHERE kind = 'relation'
           AND refs IS NOT NULL
           AND len(refs) > 0
           AND (element_at(tags, 'building')[1] IS NOT NULL
                OR element_at(tags, 'addr:housenumber')[1] IS NOT NULL)"
    ))?;
    let batches = stmt.query_arrow([])?;

    let mut building_count: u64 = 0;
    let mut address_count: u64 = 0;

    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("id")?;
        let refs_list = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .context("refs")?;
        let types_list = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .context("types")?;
        let roles_list = batch
            .column(3)
            .as_any()
            .downcast_ref::<ListArray>()
            .context("roles")?;
        let tags_col = batch.column(4);

        for i in 0..batch.num_rows() {
            let relation_id = ids.value(i);

            let refs_arr = refs_list.value(i);
            let refs = refs_arr.as_any().downcast_ref::<Int64Array>().unwrap();

            let types_arr = types_list.value(i);
            let types = types_arr.as_any().downcast_ref::<StringArray>().unwrap();

            let roles_arr = roles_list.value(i);
            let roles = roles_arr.as_any().downcast_ref::<StringArray>().unwrap();

            let building = extract_map_value(tags_col, i, "building");
            let housenumber = extract_map_value(tags_col, i, "addr:housenumber");
            let street = extract_map_value(tags_col, i, "addr:street");
            let city = extract_map_value(tags_col, i, "addr:city")
                .or_else(|| extract_map_value(tags_col, i, "addr:place"));
            let postcode = extract_map_value(tags_col, i, "addr:postcode");

            // Flat: one row per (way, node) pair
            let mut flat_relation_ids: Vec<i64> = Vec::new();
            let mut flat_way_ids: Vec<i64> = Vec::new();
            let mut flat_roles: Vec<String> = Vec::new();
            let mut flat_lons: Vec<f64> = Vec::new();
            let mut flat_lats: Vec<f64> = Vec::new();

            for j in 0..refs.len() {
                if types.value(j) != "way" {
                    continue;
                }
                let way_ref = refs.value(j);
                let role = roles.value(j).to_string();

                let node_ids = match kvstore::get_way(kv, way_ref)? {
                    Some(ids) => ids,
                    None => continue,
                };

                let mut lons = Vec::with_capacity(node_ids.len());
                let mut lats = Vec::with_capacity(node_ids.len());
                let mut all_found = true;
                for &nid in &node_ids {
                    match kvstore::get_node(kv, nid)? {
                        Some((lon, lat)) => {
                            lons.push(lon);
                            lats.push(lat);
                        }
                        None => {
                            all_found = false;
                            break;
                        }
                    }
                }

                if !all_found || lons.len() < 2 {
                    continue;
                }

                for k in 0..lons.len() {
                    flat_relation_ids.push(relation_id);
                    flat_way_ids.push(way_ref);
                    flat_roles.push(role.clone());
                    flat_lons.push(lons[k]);
                    flat_lats.push(lats[k]);
                }
            }

            if flat_way_ids.is_empty() {
                continue;
            }

            let rb = build_relation_member_batch(
                &flat_relation_ids,
                &flat_way_ids,
                &flat_roles,
                &flat_lons,
                &flat_lats,
            )?;

            if building.is_some() {
                building_count += insert_relation_building(
                    conn,
                    &rb,
                    relation_id,
                    building.as_deref().unwrap_or("yes"),
                )?;
            }

            if housenumber.is_some() {
                address_count += insert_relation_address(
                    conn,
                    &rb,
                    relation_id,
                    housenumber.as_deref(),
                    street.as_deref(),
                    city.as_deref(),
                    postcode.as_deref(),
                )?;
            }
        }
    }

    info!(count = building_count, "Relation buildings imported");
    info!(count = address_count, "Relation addresses imported");
    Ok(())
}

/// Extract a value from a DuckDB MAP column at row i for a given key.
fn extract_map_value(col: &dyn Array, row: usize, key: &str) -> Option<String> {
    use duckdb::arrow::array::{LargeStringArray, MapArray};
    let map = col.as_any().downcast_ref::<MapArray>()?;
    if map.is_null(row) {
        return None;
    }
    let entry = map.value(row);
    let keys = entry.column(0);
    let values = entry.column(1);

    for j in 0..keys.len() {
        let k = if let Some(arr) = keys.as_any().downcast_ref::<StringArray>() {
            arr.value(j).to_string()
        } else if let Some(arr) = keys.as_any().downcast_ref::<LargeStringArray>() {
            arr.value(j).to_string()
        } else {
            continue;
        };

        if k == key {
            if values.is_null(j) {
                return None;
            }
            if let Some(arr) = values.as_any().downcast_ref::<StringArray>() {
                return Some(arr.value(j).to_string());
            } else if let Some(arr) = values.as_any().downcast_ref::<LargeStringArray>() {
                return Some(arr.value(j).to_string());
            }
        }
    }
    None
}

/// Flat schema: one row per node coordinate within a relation member way.
/// Columns: relation_id, way_id, member_role, lon, lat
fn build_relation_member_batch(
    flat_relation_ids: &[i64],
    flat_way_ids: &[i64],
    flat_roles: &[String],
    flat_lons: &[f64],
    flat_lats: &[f64],
) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("relation_id", DataType::Int64, false),
        Field::new("way_id", DataType::Int64, false),
        Field::new("member_role", DataType::Utf8, false),
        Field::new("lon", DataType::Float64, false),
        Field::new("lat", DataType::Float64, false),
    ]));

    let mut role_builder = StringBuilder::new();
    for r in flat_roles {
        role_builder.append_value(r);
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(flat_relation_ids.to_vec())),
            Arc::new(Int64Array::from(flat_way_ids.to_vec())),
            Arc::new(role_builder.finish()),
            Arc::new(Float64Array::from(flat_lons.to_vec())),
            Arc::new(Float64Array::from(flat_lats.to_vec())),
        ],
    )
    .context("Failed to build relation member RecordBatch")
}

fn insert_relation_building(
    conn: &Connection,
    rb: &RecordBatch,
    relation_id: i64,
    building_tag: &str,
) -> Result<u64> {
    let params = arrow_recordbatch_to_query_params(rb.clone());
    let building_escaped = building_tag.replace('\'', "''");
    let changed = conn.execute(
        &format!(
            "INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
             WITH way_lines AS (
                 SELECT way_id, member_role,
                        ST_MakeLine(list(ST_Point(lon, lat))) AS line_geom
                 FROM arrow(?, ?)
                 GROUP BY way_id, member_role
                 HAVING COUNT(*) >= 2
             ),
             outer_polys AS (
                 SELECT ST_Union_Agg(ST_MakePolygon(line_geom)) AS outer_geom
                 FROM way_lines
                 WHERE (member_role = 'outer' OR member_role = '')
                   AND ST_NPoints(line_geom) >= 4
             ),
             inner_polys AS (
                 SELECT ST_Union_Agg(ST_MakePolygon(line_geom)) AS inner_geom
                 FROM way_lines
                 WHERE member_role = 'inner'
                   AND ST_NPoints(line_geom) >= 4
             )
             SELECT
                 {relation_id} AS osm_id,
                 'relation' AS osm_type,
                 '{building_escaped}' AS building,
                 CASE
                     WHEN i.inner_geom IS NOT NULL THEN ST_Difference(o.outer_geom, i.inner_geom)
                     ELSE o.outer_geom
                 END AS geom
             FROM outer_polys o
             LEFT JOIN inner_polys i ON true
             WHERE o.outer_geom IS NOT NULL"
        ),
        params,
    )?;
    Ok(changed as u64)
}

fn insert_relation_address(
    conn: &Connection,
    rb: &RecordBatch,
    relation_id: i64,
    housenumber: Option<&str>,
    street: Option<&str>,
    city: Option<&str>,
    postcode: Option<&str>,
) -> Result<u64> {
    let params = arrow_recordbatch_to_query_params(rb.clone());
    let hn_sql = match housenumber {
        Some(v) => format!("'{}'", v.replace('\'', "''")),
        None => "NULL".to_string(),
    };
    let street_sql = match street {
        Some(v) => format!("'{}'", v.replace('\'', "''")),
        None => "NULL".to_string(),
    };
    let city_sql = match city {
        Some(v) => format!("'{}'", v.replace('\'', "''")),
        None => "NULL".to_string(),
    };
    let postcode_sql = match postcode {
        Some(v) => format!("'{}'", v.replace('\'', "''")),
        None => "NULL".to_string(),
    };

    let changed = conn.execute(
        &format!(
            "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
             WITH all_coords AS (
                 SELECT lon, lat FROM arrow(?, ?)
             )
             SELECT
                 {relation_id} AS osm_id,
                 'relation' AS osm_type,
                 {hn_sql} AS housenumber,
                 {street_sql} AS street,
                 {city_sql} AS city,
                 {postcode_sql} AS postcode,
                 ST_Point(AVG(lon), AVG(lat)) AS geom
             FROM all_coords"
        ),
        params,
    )?;
    Ok(changed as u64)
}
