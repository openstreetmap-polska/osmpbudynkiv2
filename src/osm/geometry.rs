use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

/// Build way geometries (buildings and addresses) from osm_ways + osm_nodes.
/// Must be called after nodes and ways have been imported.
pub fn build_way_geometries(conn: &Connection) -> Result<()> {
    // Materialize UNNEST into a temp table so DuckDB can spill to disk,
    // rather than holding the entire UNNEST + JOIN in memory.
    info!("Building building geometries from ways");
    conn.execute_batch(
        "
        CREATE OR REPLACE TEMP TABLE building_way_nodes AS
        SELECT
            w.way_id,
            element_at(w.tags, 'building')[1] AS building,
            UNNEST(w.node_ids) AS node_id,
            UNNEST(generate_series(1, len(w.node_ids))) AS position
        FROM osm_ways w
        WHERE element_at(w.tags, 'building')[1] IS NOT NULL;

        INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
        SELECT
            wn.way_id AS osm_id,
            'way' AS osm_type,
            wn.building,
            ST_MakePolygon(
                ST_MakeLine(
                    list(ST_Point(n.lon, n.lat) ORDER BY wn.position)
                )
            ) AS geom
        FROM building_way_nodes wn
        JOIN osm_nodes n ON wn.node_id = n.node_id
        GROUP BY wn.way_id, wn.building
        HAVING COUNT(*) >= 4;

        DROP TABLE IF EXISTS building_way_nodes;
        ",
    )
    .context("Failed to build building geometries from ways")?;

    let building_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_buildings WHERE osm_type = 'way'",
        [],
        |row| row.get(0),
    )?;
    info!(count = building_count, "Way buildings imported");

    info!("Building address geometries from ways");
    conn.execute_batch(
        "
        CREATE OR REPLACE TEMP TABLE address_way_nodes AS
        SELECT
            w.way_id,
            element_at(w.tags, 'addr:housenumber')[1] AS housenumber,
            element_at(w.tags, 'addr:street')[1] AS street,
            COALESCE(element_at(w.tags, 'addr:city')[1], element_at(w.tags, 'addr:place')[1]) AS city,
            element_at(w.tags, 'addr:postcode')[1] AS postcode,
            UNNEST(w.node_ids) AS node_id
        FROM osm_ways w
        WHERE element_at(w.tags, 'addr:housenumber')[1] IS NOT NULL;

        INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
        SELECT
            wn.way_id AS osm_id,
            'way' AS osm_type,
            wn.housenumber,
            wn.street,
            wn.city,
            wn.postcode,
            ST_Point(AVG(n.lon), AVG(n.lat)) AS geom
        FROM address_way_nodes wn
        JOIN osm_nodes n ON wn.node_id = n.node_id
        GROUP BY wn.way_id, wn.housenumber, wn.street, wn.city, wn.postcode;

        DROP TABLE IF EXISTS address_way_nodes;
        ",
    )
    .context("Failed to build address geometries from ways")?;

    let addr_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_addresses WHERE osm_type = 'way'",
        [],
        |row| row.get(0),
    )?;
    info!(count = addr_count, "Way addresses imported");

    Ok(())
}

/// Build relation geometries (multipolygon buildings and addresses).
/// Must be called after nodes, ways, and relations have been imported.
pub fn build_relation_geometries(conn: &Connection) -> Result<()> {
    // Materialize UNNEST steps into temp tables to avoid OOM on large datasets.
    info!("Building building geometries from relations");
    conn.execute_batch(
        "
        CREATE OR REPLACE TEMP TABLE rel_member_way_nodes AS
        WITH rel_members AS (
            SELECT
                r.relation_id,
                UNNEST(r.member_refs) AS member_id,
                UNNEST(r.member_types) AS member_type,
                UNNEST(r.member_roles) AS member_role
            FROM osm_relations r
            WHERE element_at(r.tags, 'building')[1] IS NOT NULL
        )
        SELECT
            rm.relation_id,
            rm.member_id AS way_id,
            rm.member_role,
            UNNEST(w.node_ids) AS node_id,
            UNNEST(generate_series(1, len(w.node_ids))) AS position
        FROM rel_members rm
        JOIN osm_ways w ON rm.member_id = w.way_id
        WHERE rm.member_type = 'way';

        CREATE OR REPLACE TEMP TABLE rel_way_lines AS
        SELECT
            mwn.relation_id,
            mwn.way_id,
            mwn.member_role,
            ST_MakeLine(list(ST_Point(n.lon, n.lat) ORDER BY mwn.position)) AS line_geom
        FROM rel_member_way_nodes mwn
        JOIN osm_nodes n ON mwn.node_id = n.node_id
        GROUP BY mwn.relation_id, mwn.way_id, mwn.member_role
        HAVING COUNT(*) >= 2;

        DROP TABLE IF EXISTS rel_member_way_nodes;

        -- Build outer polygons per relation
        CREATE OR REPLACE TEMP TABLE rel_outer_polys AS
        SELECT
            relation_id,
            ST_Union_Agg(ST_MakePolygon(line_geom)) AS outer_geom
        FROM rel_way_lines
        WHERE (member_role = 'outer' OR member_role = '')
          AND ST_NPoints(line_geom) >= 4
        GROUP BY relation_id;

        -- Build inner polygons (holes) per relation
        CREATE OR REPLACE TEMP TABLE rel_inner_polys AS
        SELECT
            relation_id,
            ST_Union_Agg(ST_MakePolygon(line_geom)) AS inner_geom
        FROM rel_way_lines
        WHERE member_role = 'inner'
          AND ST_NPoints(line_geom) >= 4
        GROUP BY relation_id;

        -- Combine: outer minus inner holes
        INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
        SELECT
            o.relation_id AS osm_id,
            'relation' AS osm_type,
            element_at(r.tags, 'building')[1] AS building,
            CASE
                WHEN i.inner_geom IS NOT NULL THEN ST_Difference(o.outer_geom, i.inner_geom)
                ELSE o.outer_geom
            END AS geom
        FROM rel_outer_polys o
        JOIN osm_relations r ON o.relation_id = r.relation_id
        LEFT JOIN rel_inner_polys i ON o.relation_id = i.relation_id
        WHERE o.outer_geom IS NOT NULL;

        DROP TABLE IF EXISTS rel_way_lines;
        DROP TABLE IF EXISTS rel_outer_polys;
        DROP TABLE IF EXISTS rel_inner_polys;
        ",
    )
    .context("Failed to build building geometries from relations")?;

    let building_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_buildings WHERE osm_type = 'relation'",
        [],
        |row| row.get(0),
    )?;
    info!(count = building_count, "Relation buildings imported");

    info!("Building address geometries from relations");
    conn.execute_batch(
        "
        CREATE OR REPLACE TEMP TABLE rel_addr_member_nodes AS
        WITH rel_members AS (
            SELECT
                r.relation_id,
                element_at(r.tags, 'addr:housenumber')[1] AS housenumber,
                element_at(r.tags, 'addr:street')[1] AS street,
                COALESCE(element_at(r.tags, 'addr:city')[1], element_at(r.tags, 'addr:place')[1]) AS city,
                element_at(r.tags, 'addr:postcode')[1] AS postcode,
                UNNEST(r.member_refs) AS member_id,
                UNNEST(r.member_types) AS member_type
            FROM osm_relations r
            WHERE element_at(r.tags, 'addr:housenumber')[1] IS NOT NULL
        )
        SELECT
            rm.relation_id,
            rm.housenumber,
            rm.street,
            rm.city,
            rm.postcode,
            UNNEST(w.node_ids) AS node_id
        FROM rel_members rm
        JOIN osm_ways w ON rm.member_id = w.way_id
        WHERE rm.member_type = 'way';

        INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
        SELECT
            mn.relation_id AS osm_id,
            'relation' AS osm_type,
            mn.housenumber,
            mn.street,
            mn.city,
            mn.postcode,
            ST_Point(AVG(n.lon), AVG(n.lat)) AS geom
        FROM rel_addr_member_nodes mn
        JOIN osm_nodes n ON mn.node_id = n.node_id
        GROUP BY mn.relation_id, mn.housenumber, mn.street, mn.city, mn.postcode;

        DROP TABLE IF EXISTS rel_addr_member_nodes;
        ",
    )
    .context("Failed to build address geometries from relations")?;

    let addr_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_addresses WHERE osm_type = 'relation'",
        [],
        |row| row.get(0),
    )?;
    info!(count = addr_count, "Relation addresses imported");

    Ok(())
}
