# Denormalize OSM Schema

## Goal

Replace normalized per-member row tables (`osm_way_nodes`, `osm_relations`) and separate tag tables (`osm_way_tags`, `osm_relation_tags`) with denormalized tables that use DuckDB's list and map types. Remove all primary key constraints to avoid in-memory ART index builds on large datasets.

## Schema

### Tables removed

- `osm_way_nodes (way_id, node_id, position)` — one row per way-node reference
- `osm_relations (relation_id, member_id, member_type, member_role, position)` — one row per relation member

### Tables no longer created during import

- `osm_way_tags (way_id, tags)` — created via `CREATE OR REPLACE TABLE`
- `osm_relation_tags (relation_id, building, housenumber, street, city, postcode)` — created via `CREATE OR REPLACE TABLE`

### New tables

```sql
CREATE TABLE osm_ways (
    way_id BIGINT,
    node_ids BIGINT[],
    tags MAP(VARCHAR, VARCHAR)
);

CREATE TABLE osm_relations (
    relation_id BIGINT,
    member_refs BIGINT[],
    member_types VARCHAR[],
    member_roles VARCHAR[],
    tags MAP(VARCHAR, VARCHAR)
);
```

Relations use parallel arrays (not `STRUCT[]`) because `ST_ReadOSM` returns `refs`, `ref_types`, `ref_roles` as separate lists — no transformation needed at import time.

All ways and relations from the PBF are stored, not just building/address-tagged ones, because relation geometry resolution requires member ways that may lack relevant tags.

### Modified tables — remove PRIMARY KEY

```sql
CREATE TABLE osm_nodes (
    node_id BIGINT,  -- was PRIMARY KEY
    lon DOUBLE,
    lat DOUBLE
);

CREATE TABLE metadata (
    key VARCHAR,  -- was PRIMARY KEY
    value VARCHAR
);
```

`osm_addresses` and `osm_buildings` had no PKs and remain unchanged.

## Import changes

### import_ways

Single query, no UNNEST, no separate tag table:

```sql
INSERT INTO osm_ways (way_id, node_ids, tags)
SELECT id, refs, tags
FROM ST_ReadOSM('{pbf_path}')
WHERE kind = 'way' AND refs IS NOT NULL AND len(refs) > 0;
```

### import_relations

Single query, no UNNEST, no separate tag table:

```sql
INSERT INTO osm_relations (relation_id, member_refs, member_types, member_roles, tags)
SELECT id, refs, ref_types::VARCHAR[], ref_roles, tags
FROM ST_ReadOSM('{pbf_path}')
WHERE kind = 'relation' AND refs IS NOT NULL AND len(refs) > 0;
```

## Geometry building changes

UNNEST moves from import time to geometry query time.

### Way buildings

```sql
WITH way_nodes AS (
    SELECT w.way_id,
           element_at(w.tags, 'building')[1] AS building,
           UNNEST(w.node_ids) AS node_id,
           UNNEST(generate_series(1, len(w.node_ids))) AS position
    FROM osm_ways w
    WHERE element_at(w.tags, 'building')[1] IS NOT NULL
)
SELECT wn.way_id AS osm_id, 'way' AS osm_type, wn.building,
    ST_MakePolygon(ST_MakeLine(list(ST_Point(n.lon, n.lat) ORDER BY wn.position))) AS geom
FROM way_nodes wn
JOIN osm_nodes n ON wn.node_id = n.node_id
GROUP BY wn.way_id, wn.building
HAVING COUNT(*) >= 4;
```

### Way addresses

Same pattern — UNNEST node_ids, JOIN osm_nodes, compute centroid via `AVG(lon), AVG(lat)`.

### Relation buildings

UNNEST relation members, JOIN `osm_ways` for node_ids, UNNEST those, JOIN `osm_nodes` for coordinates. Build linestrings per member way, then outer/inner polygon logic (same as current approach, just different source tables).

### Relation addresses

UNNEST relation members, JOIN osm_ways + osm_nodes, compute centroid.

## Update changes

### Node create/modify

`INSERT OR REPLACE` no longer works without PK. Use explicit `DELETE` + `INSERT`:

```rust
conn.execute("DELETE FROM osm_nodes WHERE node_id = ?", [node.id])?;
conn.execute("INSERT INTO osm_nodes VALUES (?, ?, ?)", params![node.id, node.lon, node.lat])?;
```

### Way create/modify

Replace the whole row in `osm_ways`:

```rust
conn.execute("DELETE FROM osm_ways WHERE way_id = ?", [way.id])?;
// Insert with list/map literals constructed in Rust
conn.execute_batch(&format!(
    "INSERT INTO osm_ways VALUES ({}, {}, {})", way.id, node_ids_literal, map_literal
))?;
```

No more row-per-node-ref insert loop.

### Relation create/modify

Same pattern — delete + insert whole row with list literals.

### Find ways referencing a node

```sql
SELECT way_id FROM osm_ways WHERE list_contains(node_ids, ?)
```

Full scan (no index), but only used during incremental updates for single nodes — acceptable.

### Metadata

`INSERT OR REPLACE` becomes `DELETE` + `INSERT` (same PK removal reason).

## Files affected

- `src/db.rs` — schema changes (new table definitions, remove PKs)
- `src/import/osm.rs` — simplify import_ways and import_relations, remove tag table creation
- `src/osm/geometry.rs` — rewrite JOINs to UNNEST from new tables
- `src/update/osm.rs` — replace row-per-member logic with whole-row operations, replace INSERT OR REPLACE with DELETE+INSERT
- Tests in all above files
