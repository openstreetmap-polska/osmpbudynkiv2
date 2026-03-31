# Denormalize OSM Schema Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace normalized per-member-row tables with denormalized tables using DuckDB lists/maps, and remove all primary key constraints.

**Architecture:** Four tables (`osm_way_nodes`, `osm_relations`, `osm_way_tags`, `osm_relation_tags`) are replaced by two (`osm_ways`, `osm_relations`) using BIGINT[] and MAP(VARCHAR, VARCHAR) column types. PKs removed from all tables. Import becomes simpler (no UNNEST), geometry building UNNESTs at query time instead.

**Tech Stack:** Rust, DuckDB (bundled), DuckDB spatial extension

**Spec:** `docs/superpowers/specs/2026-03-31-denormalize-osm-schema-design.md`

---

### Task 1: Update schema in db.rs

**Files:**
- Modify: `src/db.rs:20-71` (create_schema function and tests)

- [ ] **Step 1: Update create_schema DDL**

Replace the schema in `create_schema()`:

```rust
fn create_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS metadata (
            key VARCHAR,
            value VARCHAR
        );

        -- OSM raw data
        CREATE TABLE IF NOT EXISTS osm_nodes (
            node_id BIGINT,
            lon DOUBLE,
            lat DOUBLE
        );

        CREATE TABLE IF NOT EXISTS osm_ways (
            way_id BIGINT,
            node_ids BIGINT[],
            tags MAP(VARCHAR, VARCHAR)
        );

        CREATE TABLE IF NOT EXISTS osm_relations (
            relation_id BIGINT,
            member_refs BIGINT[],
            member_types VARCHAR[],
            member_roles VARCHAR[],
            tags MAP(VARCHAR, VARCHAR)
        );

        -- Processed OSM data with geometry
        CREATE TABLE IF NOT EXISTS osm_addresses (
            osm_id BIGINT,
            osm_type VARCHAR,
            housenumber VARCHAR,
            street VARCHAR,
            city VARCHAR,
            postcode VARCHAR,
            geom GEOMETRY
        );

        CREATE TABLE IF NOT EXISTS osm_buildings (
            osm_id BIGINT,
            osm_type VARCHAR,
            building VARCHAR,
            geom GEOMETRY
        );
        ",
    )
    .context("Failed to create schema")?;

    Ok(())
}
```

Key changes: `osm_nodes` loses `PRIMARY KEY`, `metadata` loses `PRIMARY KEY`, `osm_way_nodes` becomes `osm_ways` with `node_ids BIGINT[]` and `tags MAP(VARCHAR, VARCHAR)`, `osm_relations` changes from normalized (one row per member) to denormalized (parallel arrays + tags map).

- [ ] **Step 2: Update test table list**

In `test_init_db_creates_tables`, change the table list:

```rust
let tables = [
    "metadata",
    "osm_nodes",
    "osm_ways",
    "osm_relations",
    "osm_addresses",
    "osm_buildings",
];
```

- [ ] **Step 3: Run db.rs tests**

Run: `cargo test db::tests -v`
Expected: both `test_init_db_creates_tables` and `test_init_db_is_idempotent` PASS.

- [ ] **Step 4: Commit**

```bash
git add src/db.rs
git commit -m "schema: replace osm_way_nodes/osm_relations with denormalized osm_ways/osm_relations, remove PKs"
```

---

### Task 2: Rewrite import functions and update import test helpers

**Files:**
- Modify: `src/import/osm.rs:84-175` (import_ways, import_relations functions)
- Modify: `src/import/osm.rs:198-226` (setup_test_db, test_import_fixture_node_counts)

- [ ] **Step 1: Rewrite import_ways**

Replace the `import_ways` function (lines 84-124):

```rust
fn import_ways(conn: &Connection, pbf_path: &str) -> Result<()> {
    info!("Pass 3: Importing ways");

    conn.execute_batch(&format!(
        "
        INSERT INTO osm_ways (way_id, node_ids, tags)
        SELECT id, refs, tags
        FROM ST_ReadOSM('{pbf_path}')
        WHERE kind = 'way' AND refs IS NOT NULL AND len(refs) > 0;
        "
    ))
    .context("Failed to import ways")?;

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM osm_ways", [], |row| row.get(0))?;
    info!(count, "Ways imported");

    Ok(())
}
```

- [ ] **Step 2: Rewrite import_relations**

Replace the `import_relations` function (lines 126-175):

```rust
fn import_relations(conn: &Connection, pbf_path: &str) -> Result<()> {
    info!("Pass 4: Importing relations");

    conn.execute_batch(&format!(
        "
        INSERT INTO osm_relations (relation_id, member_refs, member_types, member_roles, tags)
        SELECT id, refs, ref_types::VARCHAR[], ref_roles, tags
        FROM ST_ReadOSM('{pbf_path}')
        WHERE kind = 'relation' AND refs IS NOT NULL AND len(refs) > 0;
        "
    ))
    .context("Failed to import relations")?;

    let count: i64 =
        conn.query_row("SELECT COUNT(*) FROM osm_relations", [], |row| row.get(0))?;
    info!(count, "Relations imported");

    Ok(())
}
```

- [ ] **Step 3: Add duplicate import guard**

At the top of the `import()` function (line 10), after the `pbf_path` is resolved but before import starts, add:

```rust
let has_data: bool = conn.query_row(
    "SELECT EXISTS (SELECT 1 FROM osm_nodes LIMIT 1)",
    [],
    |row| row.get(0),
)?;
if has_data {
    anyhow::bail!("OSM data already imported. Drop the database and reimport if needed.");
}
```

Add `use anyhow::bail` or use fully qualified `anyhow::bail!` (check existing imports — `bail` is not currently imported in this file, so use `anyhow::bail!`).

- [ ] **Step 4: Simplify setup_test_db**

Replace `setup_test_db` in the test module (lines 203-226):

```rust
fn setup_test_db() -> Result<Connection> {
    let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
    let conn = init_db(Path::new(":memory:"), &init_commands)?;
    Ok(conn)
}
```

No extra table creation needed — `osm_ways` and `osm_relations` are now part of the schema.

- [ ] **Step 5: Update test_import_fixture_node_counts**

Replace `test_import_fixture_node_counts` assertions for way-nodes and relations (lines 525-536):

```rust
// Ways: 3 ways in the fixture
let way_count: i64 =
    conn.query_row("SELECT COUNT(*) FROM osm_ways", [], |row| row.get(0))?;
assert_eq!(way_count, 3, "Expected 3 ways");

// Relations: 1 relation in the fixture
let rel_count: i64 =
    conn.query_row("SELECT COUNT(*) FROM osm_relations", [], |row| row.get(0))?;
assert_eq!(rel_count, 1, "Expected 1 relation");
```

- [ ] **Step 6: Commit**

```bash
git add src/import/osm.rs
git commit -m "import: rewrite import_ways/import_relations to use denormalized tables, add duplicate guard"
```

---

### Task 3: Rewrite way geometry building

**Files:**
- Modify: `src/osm/geometry.rs:7-78` (build_way_geometries)
- Modify: `src/import/osm.rs` (test_way_building_geometry, test_way_address_geometry test data)

- [ ] **Step 1: Update test_way_building_geometry test data**

Replace the test data inserts in `test_way_building_geometry` (lines 233-247):

```rust
conn.execute_batch(
    "
    INSERT INTO osm_nodes VALUES (1, 20.0, 50.0);
    INSERT INTO osm_nodes VALUES (2, 20.001, 50.0);
    INSERT INTO osm_nodes VALUES (3, 20.001, 50.001);
    INSERT INTO osm_nodes VALUES (4, 20.0, 50.001);

    INSERT INTO osm_ways VALUES (100, [1, 2, 3, 4, 1], MAP {'building': 'yes'});
    ",
)?;
```

- [ ] **Step 2: Update test_way_address_geometry test data**

Replace the test data inserts in `test_way_address_geometry` (lines 273-289):

```rust
conn.execute_batch(
    "
    INSERT INTO osm_nodes VALUES (1, 20.0, 50.0);
    INSERT INTO osm_nodes VALUES (2, 20.002, 50.0);

    INSERT INTO osm_ways VALUES (200, [1, 2], MAP {
        'addr:housenumber': '42',
        'addr:street': 'ul. Testowa',
        'addr:city': 'Warszawa',
        'addr:postcode': '00-001'
    });
    ",
)?;
```

- [ ] **Step 3: Rewrite build_way_geometries**

Replace the entire `build_way_geometries` function in `src/osm/geometry.rs`:

```rust
pub fn build_way_geometries(conn: &Connection) -> Result<()> {
    info!("Building building geometries from ways");
    conn.execute_batch(
        "
        INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
        WITH way_nodes AS (
            SELECT
                w.way_id,
                element_at(w.tags, 'building')[1] AS building,
                UNNEST(w.node_ids) AS node_id,
                UNNEST(generate_series(1, len(w.node_ids))) AS position
            FROM osm_ways w
            WHERE element_at(w.tags, 'building')[1] IS NOT NULL
        )
        SELECT
            wn.way_id AS osm_id,
            'way' AS osm_type,
            wn.building,
            ST_MakePolygon(
                ST_MakeLine(
                    list(ST_Point(n.lon, n.lat) ORDER BY wn.position)
                )
            ) AS geom
        FROM way_nodes wn
        JOIN osm_nodes n ON wn.node_id = n.node_id
        GROUP BY wn.way_id, wn.building
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
        WITH way_nodes AS (
            SELECT
                w.way_id,
                element_at(w.tags, 'addr:housenumber')[1] AS housenumber,
                element_at(w.tags, 'addr:street')[1] AS street,
                element_at(w.tags, 'addr:city')[1] AS city,
                element_at(w.tags, 'addr:postcode')[1] AS postcode,
                UNNEST(w.node_ids) AS node_id
            FROM osm_ways w
            WHERE element_at(w.tags, 'addr:housenumber')[1] IS NOT NULL
        )
        SELECT
            wn.way_id AS osm_id,
            'way' AS osm_type,
            wn.housenumber,
            wn.street,
            wn.city,
            wn.postcode,
            ST_Point(AVG(n.lon), AVG(n.lat)) AS geom
        FROM way_nodes wn
        JOIN osm_nodes n ON wn.node_id = n.node_id
        GROUP BY wn.way_id, wn.housenumber, wn.street, wn.city, wn.postcode;
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
```

- [ ] **Step 4: Run way geometry tests**

Run: `cargo test test_way_building_geometry test_way_address_geometry -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/osm/geometry.rs src/import/osm.rs
git commit -m "geometry: rewrite build_way_geometries to UNNEST from osm_ways"
```

---

### Task 4: Rewrite relation geometry building

**Files:**
- Modify: `src/osm/geometry.rs:80-188` (build_relation_geometries)
- Modify: `src/import/osm.rs` (test_relation_building_geometry test data)

- [ ] **Step 1: Update test_relation_building_geometry test data**

Replace the test data inserts in `test_relation_building_geometry` (lines 316-348):

```rust
conn.execute_batch(
    "
    INSERT INTO osm_nodes VALUES (1, 20.0, 50.0);
    INSERT INTO osm_nodes VALUES (2, 20.01, 50.0);
    INSERT INTO osm_nodes VALUES (3, 20.01, 50.01);
    INSERT INTO osm_nodes VALUES (4, 20.0, 50.01);

    -- Inner ring (hole): a smaller square
    INSERT INTO osm_nodes VALUES (5, 20.003, 50.003);
    INSERT INTO osm_nodes VALUES (6, 20.007, 50.003);
    INSERT INTO osm_nodes VALUES (7, 20.007, 50.007);
    INSERT INTO osm_nodes VALUES (8, 20.003, 50.007);

    -- Outer way (way_id=10) and inner way (way_id=11)
    INSERT INTO osm_ways VALUES (10, [1, 2, 3, 4, 1], NULL);
    INSERT INTO osm_ways VALUES (11, [5, 6, 7, 8, 5], NULL);

    -- Relation 300 references both ways
    INSERT INTO osm_relations VALUES (300, [10, 11], ['way', 'way'], ['outer', 'inner'], MAP {'building': 'yes'});
    ",
)?;
```

- [ ] **Step 2: Rewrite build_relation_geometries**

Replace the entire `build_relation_geometries` function in `src/osm/geometry.rs`:

```rust
pub fn build_relation_geometries(conn: &Connection) -> Result<()> {
    info!("Building building geometries from relations");
    conn.execute_batch(
        "
        CREATE OR REPLACE TEMP TABLE rel_way_lines AS
        WITH rel_members AS (
            SELECT
                r.relation_id,
                UNNEST(r.member_refs) AS member_id,
                UNNEST(r.member_types) AS member_type,
                UNNEST(r.member_roles) AS member_role
            FROM osm_relations r
            WHERE element_at(r.tags, 'building')[1] IS NOT NULL
        ),
        member_way_nodes AS (
            SELECT
                rm.relation_id,
                rm.member_id AS way_id,
                rm.member_role,
                UNNEST(w.node_ids) AS node_id,
                UNNEST(generate_series(1, len(w.node_ids))) AS position
            FROM rel_members rm
            JOIN osm_ways w ON rm.member_id = w.way_id
            WHERE rm.member_type = 'way'
        )
        SELECT
            mwn.relation_id,
            mwn.way_id,
            mwn.member_role,
            ST_MakeLine(list(ST_Point(n.lon, n.lat) ORDER BY mwn.position)) AS line_geom
        FROM member_way_nodes mwn
        JOIN osm_nodes n ON mwn.node_id = n.node_id
        GROUP BY mwn.relation_id, mwn.way_id, mwn.member_role
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
        INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
        WITH rel_members AS (
            SELECT
                r.relation_id,
                element_at(r.tags, 'addr:housenumber')[1] AS housenumber,
                element_at(r.tags, 'addr:street')[1] AS street,
                element_at(r.tags, 'addr:city')[1] AS city,
                element_at(r.tags, 'addr:postcode')[1] AS postcode,
                UNNEST(r.member_refs) AS member_id,
                UNNEST(r.member_types) AS member_type
            FROM osm_relations r
            WHERE element_at(r.tags, 'addr:housenumber')[1] IS NOT NULL
        ),
        member_nodes AS (
            SELECT
                rm.relation_id,
                rm.housenumber,
                rm.street,
                rm.city,
                rm.postcode,
                UNNEST(w.node_ids) AS node_id
            FROM rel_members rm
            JOIN osm_ways w ON rm.member_id = w.way_id
            WHERE rm.member_type = 'way'
        )
        SELECT
            mn.relation_id AS osm_id,
            'relation' AS osm_type,
            mn.housenumber,
            mn.street,
            mn.city,
            mn.postcode,
            ST_Point(AVG(n.lon), AVG(n.lat)) AS geom
        FROM member_nodes mn
        JOIN osm_nodes n ON mn.node_id = n.node_id
        GROUP BY mn.relation_id, mn.housenumber, mn.street, mn.city, mn.postcode;
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
```

- [ ] **Step 3: Run all import and geometry tests**

Run: `cargo test import::osm::tests -v`
Expected: all 9 tests PASS (way_building, way_address, relation_building, fixture_pbf, fixture_building_details, fixture_address_details, fixture_address_geometries, fixture_node_counts — and the e2e import tests that run the full pipeline).

- [ ] **Step 4: Commit**

```bash
git add src/osm/geometry.rs src/import/osm.rs
git commit -m "geometry: rewrite build_relation_geometries to UNNEST from osm_relations + osm_ways"
```

---

### Task 5: Rewrite update — node changes, metadata, and test setup

**Files:**
- Modify: `src/update/osm.rs:70-89` (apply_sequence metadata upsert)
- Modify: `src/update/osm.rs:108-150` (apply_node_changes)
- Modify: `src/update/osm.rs:283-295` (update_ways_referencing_node)
- Modify: `src/update/osm.rs:473-515` (setup_test_db)

- [ ] **Step 1: Update setup_test_db in update tests**

Replace `setup_test_db` in update/osm.rs test module:

```rust
fn setup_test_db() -> Result<Connection> {
    let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
    let conn = init_db(Path::new(":memory:"), &init_commands)?;
    conn.execute_batch(
        "
        -- Seed with some initial data
        INSERT INTO osm_nodes VALUES (1, 20.0, 50.0);
        INSERT INTO osm_nodes VALUES (2, 20.001, 50.0);
        INSERT INTO osm_nodes VALUES (3, 20.001, 50.001);
        INSERT INTO osm_nodes VALUES (4, 20.0, 50.001);

        INSERT INTO osm_ways VALUES (100, [1, 2, 3, 4, 1], MAP {'building': 'yes'});

        INSERT INTO osm_buildings VALUES (100, 'way', 'yes', ST_MakePolygon(ST_MakeLine(
            list_value(ST_Point(20.0, 50.0), ST_Point(20.001, 50.0),
                       ST_Point(20.001, 50.001), ST_Point(20.0, 50.001),
                       ST_Point(20.0, 50.0))
        )));

        INSERT INTO metadata VALUES ('osm_replication_sequence', '1000');
        ",
    )?;
    Ok(conn)
}
```

- [ ] **Step 2: Rewrite apply_node_changes — use DELETE + INSERT**

In `apply_node_changes`, replace the `ChangeAction::Create | ChangeAction::Modify` arm (lines 118-148):

```rust
ChangeAction::Create | ChangeAction::Modify => {
    // DELETE + INSERT (no PK for INSERT OR REPLACE)
    conn.execute("DELETE FROM osm_nodes WHERE node_id = ?", [node.id])?;
    conn.execute(
        "INSERT INTO osm_nodes (node_id, lon, lat) VALUES (?, ?, ?)",
        duckdb::params![node.id, node.lon, node.lat],
    )?;

    // Remove old address entry if any
    conn.execute(
        "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'node'",
        [node.id],
    )?;

    // If this node has an address, insert it
    let housenumber = node.tags.iter().find(|(k, _)| k == "addr:housenumber");
    if let Some((_, hn)) = housenumber {
        let street = tag_value(&node.tags, "addr:street");
        let city = tag_value(&node.tags, "addr:city");
        let postcode = tag_value(&node.tags, "addr:postcode");
        conn.execute(
            "INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
             VALUES (?, 'node', ?, ?, ?, ?, ST_Point(?, ?))",
            duckdb::params![node.id, hn, street, city, postcode, node.lon, node.lat],
        )?;
    }

    // Update geometries of ways that reference this node
    update_ways_referencing_node(conn, node.id)?;
}
```

- [ ] **Step 3: Rewrite update_ways_referencing_node**

Replace `update_ways_referencing_node` (lines 283-295):

```rust
fn update_ways_referencing_node(conn: &Connection, node_id: i64) -> Result<()> {
    let mut stmt = conn.prepare("SELECT way_id FROM osm_ways WHERE list_contains(node_ids, ?)")?;
    let way_ids: Vec<i64> = stmt
        .query_map([node_id], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    for way_id in way_ids {
        rebuild_way_geometry(conn, way_id)?;
    }

    Ok(())
}
```

- [ ] **Step 4: Rewrite metadata upsert in apply_sequence**

In `apply_sequence` (line 83-86), replace the `INSERT OR REPLACE` with DELETE + INSERT:

```rust
conn.execute(
    "DELETE FROM metadata WHERE key = 'osm_replication_sequence'",
    [],
)?;
conn.execute(
    "INSERT INTO metadata (key, value) VALUES ('osm_replication_sequence', ?)",
    [&seq.to_string()],
)?;
```

- [ ] **Step 5: Run node update tests**

Run: `cargo test update::osm::tests::test_apply_node -v`
Expected: `test_apply_node_create`, `test_apply_node_delete`, `test_apply_node_modify_cascades_to_way` all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/update/osm.rs
git commit -m "update: rewrite node changes to DELETE+INSERT, use list_contains for way lookup"
```

---

### Task 6: Rewrite update — way changes

**Files:**
- Modify: `src/update/osm.rs:152-375` (apply_way_changes, rebuild_way_geometry)

- [ ] **Step 1: Rewrite apply_way_changes**

Replace `apply_way_changes` (lines 152-214):

```rust
fn apply_way_changes(conn: &Connection, ways: &[WayChange]) -> Result<()> {
    for way in ways {
        match way.action {
            ChangeAction::Delete => {
                conn.execute("DELETE FROM osm_ways WHERE way_id = ?", [way.id])?;
                conn.execute(
                    "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way'",
                    [way.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'way'",
                    [way.id],
                )?;
            }
            ChangeAction::Create | ChangeAction::Modify => {
                conn.execute("DELETE FROM osm_ways WHERE way_id = ?", [way.id])?;

                let node_ids_literal = format!(
                    "[{}]",
                    way.node_refs
                        .iter()
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let tag_pairs: Vec<String> = way
                    .tags
                    .iter()
                    .map(|(k, v)| format!("'{}': '{}'", k.replace('\'', "''"), v.replace('\'', "''")))
                    .collect();
                let map_literal = if tag_pairs.is_empty() {
                    "MAP([]::VARCHAR[], []::VARCHAR[])".to_string()
                } else {
                    format!("MAP {{{}}}", tag_pairs.join(", "))
                };
                conn.execute_batch(&format!(
                    "INSERT INTO osm_ways (way_id, node_ids, tags) VALUES ({}, {}, {})",
                    way.id, node_ids_literal, map_literal
                ))?;

                // Clean old geometry entries
                conn.execute(
                    "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way'",
                    [way.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'way'",
                    [way.id],
                )?;

                // Rebuild geometry for this way
                rebuild_way_geometry(conn, way.id)?;
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 2: Rewrite rebuild_way_geometry**

Replace `rebuild_way_geometry` (lines 297-375):

```rust
fn rebuild_way_geometry(conn: &Connection, way_id: i64) -> Result<()> {
    let has_tags: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM osm_ways WHERE way_id = ?
             AND (element_at(tags, 'building')[1] IS NOT NULL
                  OR element_at(tags, 'addr:housenumber')[1] IS NOT NULL)",
            [way_id],
            |row| row.get(0),
        )
        .unwrap_or(false);

    if !has_tags {
        return Ok(());
    }

    conn.execute(
        "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'way'",
        [way_id],
    )?;
    conn.execute(
        "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'way'",
        [way_id],
    )?;

    conn.execute(
        &format!(
            "
            INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
            WITH way_nodes AS (
                SELECT
                    w.way_id,
                    element_at(w.tags, 'building')[1] AS building,
                    UNNEST(w.node_ids) AS node_id,
                    UNNEST(generate_series(1, len(w.node_ids))) AS position
                FROM osm_ways w
                WHERE w.way_id = {way_id}
                  AND element_at(w.tags, 'building')[1] IS NOT NULL
            )
            SELECT
                wn.way_id AS osm_id,
                'way' AS osm_type,
                wn.building,
                ST_MakePolygon(
                    ST_MakeLine(list(ST_Point(n.lon, n.lat) ORDER BY wn.position))
                ) AS geom
            FROM way_nodes wn
            JOIN osm_nodes n ON wn.node_id = n.node_id
            GROUP BY wn.way_id, wn.building
            HAVING COUNT(*) >= 4
            "
        ),
        [],
    )?;

    conn.execute(
        &format!(
            "
            INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
            WITH way_nodes AS (
                SELECT
                    w.way_id,
                    element_at(w.tags, 'addr:housenumber')[1] AS housenumber,
                    element_at(w.tags, 'addr:street')[1] AS street,
                    element_at(w.tags, 'addr:city')[1] AS city,
                    element_at(w.tags, 'addr:postcode')[1] AS postcode,
                    UNNEST(w.node_ids) AS node_id
                FROM osm_ways w
                WHERE w.way_id = {way_id}
                  AND element_at(w.tags, 'addr:housenumber')[1] IS NOT NULL
            )
            SELECT
                wn.way_id AS osm_id,
                'way' AS osm_type,
                wn.housenumber,
                wn.street,
                wn.city,
                wn.postcode,
                ST_Point(AVG(n.lon), AVG(n.lat)) AS geom
            FROM way_nodes wn
            JOIN osm_nodes n ON wn.node_id = n.node_id
            GROUP BY wn.way_id, wn.housenumber, wn.street, wn.city, wn.postcode
            "
        ),
        [],
    )?;

    Ok(())
}
```

- [ ] **Step 3: Update test_apply_way_delete assertions**

In `test_apply_way_delete`, replace the `osm_way_nodes` check (lines 664-669):

```rust
let way_count: i64 = conn.query_row(
    "SELECT COUNT(*) FROM osm_ways WHERE way_id = 100",
    [],
    |row| row.get(0),
)?;
assert_eq!(way_count, 0);
```

- [ ] **Step 4: Run way update tests**

Run: `cargo test update::osm::tests::test_apply_way -v`
Expected: `test_apply_way_delete` PASS.

- [ ] **Step 5: Commit**

```bash
git add src/update/osm.rs
git commit -m "update: rewrite way changes to use whole-row operations on osm_ways"
```

---

### Task 7: Rewrite update — relation changes

**Files:**
- Modify: `src/update/osm.rs:216-466` (apply_relation_changes, rebuild_relation_geometry)

- [ ] **Step 1: Rewrite apply_relation_changes**

Replace `apply_relation_changes` (lines 216-277):

```rust
fn apply_relation_changes(conn: &Connection, relations: &[RelationChange]) -> Result<()> {
    for rel in relations {
        match rel.action {
            ChangeAction::Delete => {
                conn.execute(
                    "DELETE FROM osm_relations WHERE relation_id = ?",
                    [rel.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'relation'",
                    [rel.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'relation'",
                    [rel.id],
                )?;
            }
            ChangeAction::Create | ChangeAction::Modify => {
                conn.execute(
                    "DELETE FROM osm_relations WHERE relation_id = ?",
                    [rel.id],
                )?;

                let refs_literal = format!(
                    "[{}]",
                    rel.members
                        .iter()
                        .map(|m| m.member_ref.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let types_literal = format!(
                    "[{}]",
                    rel.members
                        .iter()
                        .map(|m| format!("'{}'", m.member_type))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let roles_literal = format!(
                    "[{}]",
                    rel.members
                        .iter()
                        .map(|m| format!("'{}'", m.role.replace('\'', "''")))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let tag_pairs: Vec<String> = rel
                    .tags
                    .iter()
                    .map(|(k, v)| {
                        format!(
                            "'{}': '{}'",
                            k.replace('\'', "''"),
                            v.replace('\'', "''")
                        )
                    })
                    .collect();
                let map_literal = if tag_pairs.is_empty() {
                    "MAP([]::VARCHAR[], []::VARCHAR[])".to_string()
                } else {
                    format!("MAP {{{}}}", tag_pairs.join(", "))
                };

                conn.execute_batch(&format!(
                    "INSERT INTO osm_relations (relation_id, member_refs, member_types, member_roles, tags) VALUES ({}, {}, {}, {}, {})",
                    rel.id, refs_literal, types_literal, roles_literal, map_literal
                ))?;

                // Clean old geometry entries
                conn.execute(
                    "DELETE FROM osm_buildings WHERE osm_id = ? AND osm_type = 'relation'",
                    [rel.id],
                )?;
                conn.execute(
                    "DELETE FROM osm_addresses WHERE osm_id = ? AND osm_type = 'relation'",
                    [rel.id],
                )?;

                // Rebuild geometry
                rebuild_relation_geometry(conn, rel.id)?;
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 2: Rewrite rebuild_relation_geometry**

Replace `rebuild_relation_geometry` (lines 377-466):

```rust
fn rebuild_relation_geometry(conn: &Connection, relation_id: i64) -> Result<()> {
    let has_tags: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM osm_relations WHERE relation_id = ?
             AND (element_at(tags, 'building')[1] IS NOT NULL
                  OR element_at(tags, 'addr:housenumber')[1] IS NOT NULL)",
            [relation_id],
            |row| row.get(0),
        )
        .unwrap_or(false);

    if !has_tags {
        return Ok(());
    }

    conn.execute(
        &format!(
            "
            INSERT INTO osm_buildings (osm_id, osm_type, building, geom)
            WITH rel_members AS (
                SELECT
                    r.relation_id,
                    UNNEST(r.member_refs) AS member_id,
                    UNNEST(r.member_types) AS member_type,
                    UNNEST(r.member_roles) AS member_role
                FROM osm_relations r
                WHERE r.relation_id = {relation_id}
            ),
            member_way_nodes AS (
                SELECT
                    rm.relation_id,
                    rm.member_id AS way_id,
                    rm.member_role,
                    UNNEST(w.node_ids) AS node_id,
                    UNNEST(generate_series(1, len(w.node_ids))) AS position
                FROM rel_members rm
                JOIN osm_ways w ON rm.member_id = w.way_id
                WHERE rm.member_type = 'way'
            ),
            rel_way_lines AS (
                SELECT
                    mwn.relation_id,
                    mwn.member_role,
                    ST_MakeLine(list(ST_Point(n.lon, n.lat) ORDER BY mwn.position)) AS line_geom
                FROM member_way_nodes mwn
                JOIN osm_nodes n ON mwn.node_id = n.node_id
                GROUP BY mwn.relation_id, mwn.way_id, mwn.member_role
                HAVING COUNT(*) >= 2
            ),
            outer_polys AS (
                SELECT relation_id, ST_Union_Agg(ST_MakePolygon(line_geom)) AS outer_geom
                FROM rel_way_lines
                WHERE (member_role = 'outer' OR member_role = '')
                  AND ST_NPoints(line_geom) >= 4
                GROUP BY relation_id
            ),
            inner_polys AS (
                SELECT relation_id, ST_Union_Agg(ST_MakePolygon(line_geom)) AS inner_geom
                FROM rel_way_lines
                WHERE member_role = 'inner'
                  AND ST_NPoints(line_geom) >= 4
                GROUP BY relation_id
            )
            SELECT
                o.relation_id AS osm_id,
                'relation' AS osm_type,
                element_at(r.tags, 'building')[1] AS building,
                CASE
                    WHEN i.inner_geom IS NOT NULL THEN ST_Difference(o.outer_geom, i.inner_geom)
                    ELSE o.outer_geom
                END AS geom
            FROM outer_polys o
            JOIN osm_relations r ON o.relation_id = r.relation_id
            LEFT JOIN inner_polys i ON o.relation_id = i.relation_id
            WHERE element_at(r.tags, 'building')[1] IS NOT NULL AND o.outer_geom IS NOT NULL
            "
        ),
        [],
    )?;

    conn.execute(
        &format!(
            "
            INSERT INTO osm_addresses (osm_id, osm_type, housenumber, street, city, postcode, geom)
            WITH rel_members AS (
                SELECT
                    r.relation_id,
                    element_at(r.tags, 'addr:housenumber')[1] AS housenumber,
                    element_at(r.tags, 'addr:street')[1] AS street,
                    element_at(r.tags, 'addr:city')[1] AS city,
                    element_at(r.tags, 'addr:postcode')[1] AS postcode,
                    UNNEST(r.member_refs) AS member_id,
                    UNNEST(r.member_types) AS member_type
                FROM osm_relations r
                WHERE r.relation_id = {relation_id}
                  AND element_at(r.tags, 'addr:housenumber')[1] IS NOT NULL
            ),
            member_nodes AS (
                SELECT
                    rm.relation_id,
                    rm.housenumber,
                    rm.street,
                    rm.city,
                    rm.postcode,
                    UNNEST(w.node_ids) AS node_id
                FROM rel_members rm
                JOIN osm_ways w ON rm.member_id = w.way_id
                WHERE rm.member_type = 'way'
            )
            SELECT
                mn.relation_id AS osm_id,
                'relation' AS osm_type,
                mn.housenumber,
                mn.street,
                mn.city,
                mn.postcode,
                ST_Point(AVG(n.lon), AVG(n.lat)) AS geom
            FROM member_nodes mn
            JOIN osm_nodes n ON mn.node_id = n.node_id
            GROUP BY mn.relation_id, mn.housenumber, mn.street, mn.city, mn.postcode
            "
        ),
        [],
    )?;

    Ok(())
}
```

- [ ] **Step 3: Run all update tests**

Run: `cargo test update::osm::tests -v`
Expected: all 4 update tests PASS.

- [ ] **Step 4: Commit**

```bash
git add src/update/osm.rs
git commit -m "update: rewrite relation changes to use whole-row operations on osm_relations"
```

---

### Task 8: Fix integration tests

**Files:**
- Modify: `tests/cli_import_osm.rs:55-95` (test_import_osm_twice_fails_on_duplicates)

- [ ] **Step 1: Update duplicate import test**

Replace `test_import_osm_twice_fails_on_duplicates` (lines 55-95):

```rust
#[test]
fn test_import_osm_twice_fails_on_duplicates() {
    // This test needs a persistent database between two CLI invocations
    let db_path = "target/test_cli_import_twice.duckdb";
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}.wal"));

    let mut cfg_file = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    write!(cfg_file, "db_path = \"{db_path}\"\n").unwrap();
    let cfg_path = cfg_file.path().to_str().unwrap().to_string();

    // First import succeeds
    cmd()
        .args([
            "--config",
            &cfg_path,
            "import",
            "osm",
            "--file",
            "fixtures/osm.pbf",
        ])
        .assert()
        .success();

    // Second import fails — data already exists
    cmd()
        .args([
            "--config",
            &cfg_path,
            "import",
            "osm",
            "--file",
            "fixtures/osm.pbf",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("already imported"));

    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}.wal"));
}
```

- [ ] **Step 2: Run full test suite**

Run: `cargo test`
Expected: all 32 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/cli_import_osm.rs
git commit -m "test: update duplicate import test for PK-less schema"
```
