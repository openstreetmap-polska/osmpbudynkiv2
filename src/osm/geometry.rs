use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

/// Build way geometries (buildings and addresses) from osm_way_nodes + osm_nodes.
/// Must be called after nodes and way_nodes have been imported.
pub fn build_way_geometries(conn: &Connection) -> Result<()> {
    info!("Building building geometries from ways");
    conn.execute_batch(
        "
        INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
        SELECT
            w.way_id AS osm_id,
            'way' AS osm_type,
            w.building,
            ST_MakePolygon(
                ST_MakeLine(
                    list(ST_Point(n.lon, n.lat) ORDER BY wn.position)
                )
            ) AS geom
        FROM (
            SELECT DISTINCT way_id, element_at(tags, 'building')[1] AS building
            FROM osm_way_tags
        ) w
        JOIN osm_way_nodes wn ON w.way_id = wn.way_id
        JOIN osm_nodes n ON wn.node_id = n.node_id
        WHERE w.building IS NOT NULL
        GROUP BY w.way_id, w.building
        HAVING COUNT(*) >= 4;
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
        INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
        SELECT
            w.way_id AS osm_id,
            'way' AS osm_type,
            w.housenumber,
            w.street,
            w.city,
            w.postcode,
            ST_Point(AVG(n.lon), AVG(n.lat)) AS geom
        FROM (
            SELECT DISTINCT
                way_id,
                element_at(tags, 'addr:housenumber')[1] AS housenumber,
                element_at(tags, 'addr:street')[1] AS street,
                element_at(tags, 'addr:city')[1] AS city,
                element_at(tags, 'addr:postcode')[1] AS postcode
            FROM osm_way_tags
        ) w
        JOIN osm_way_nodes wn ON w.way_id = wn.way_id
        JOIN osm_nodes n ON wn.node_id = n.node_id
        WHERE w.housenumber IS NOT NULL
        GROUP BY w.way_id, w.housenumber, w.street, w.city, w.postcode;
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
/// Must be called after nodes, way_nodes, and relations have been imported.
pub fn build_relation_geometries(conn: &Connection) -> Result<()> {
    // For multipolygon building relations, we construct geometry by:
    // 1. Resolving each member way's nodes into coordinates
    // 2. Building linestrings per member way
    // 3. For outer rings: making polygons, for inner rings: cutting holes
    // This is complex in pure SQL — we use a simplified approach where we
    // take the union of all outer way polygons and subtract inner way polygons.

    info!("Building building geometries from relations");
    conn.execute_batch(
        "
        -- First, build linestrings for each member way of building relations
        CREATE OR REPLACE TEMP TABLE rel_way_lines AS
        SELECT
            r.relation_id,
            r.member_id AS way_id,
            r.member_role,
            ST_MakeLine(list(ST_Point(n.lon, n.lat) ORDER BY wn.position)) AS line_geom
        FROM osm_relations r
        JOIN osm_relation_tags rt ON r.relation_id = rt.relation_id
        JOIN osm_way_nodes wn ON r.member_id = wn.way_id
        JOIN osm_nodes n ON wn.node_id = n.node_id
        WHERE r.member_type = 'way'
          AND rt.building IS NOT NULL
        GROUP BY r.relation_id, r.member_id, r.member_role
        HAVING COUNT(*) >= 2;

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
            rt.building,
            CASE
                WHEN i.inner_geom IS NOT NULL THEN ST_Difference(o.outer_geom, i.inner_geom)
                ELSE o.outer_geom
            END AS geom
        FROM rel_outer_polys o
        JOIN osm_relation_tags rt ON o.relation_id = rt.relation_id
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
        INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
        SELECT
            rt.relation_id AS osm_id,
            'relation' AS osm_type,
            rt.housenumber,
            rt.street,
            rt.city,
            rt.postcode,
            ST_Point(AVG(n.lon), AVG(n.lat)) AS geom
        FROM osm_relation_tags rt
        JOIN osm_relations r ON rt.relation_id = r.relation_id
        JOIN osm_way_nodes wn ON r.member_id = wn.way_id AND r.member_type = 'way'
        JOIN osm_nodes n ON wn.node_id = n.node_id
        WHERE rt.housenumber IS NOT NULL
        GROUP BY rt.relation_id, rt.housenumber, rt.street, rt.city, rt.postcode;
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
