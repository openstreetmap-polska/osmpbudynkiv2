//! An OSM relation's area, assembled from the RocksDB store, for
//! `GET /package?relation=<id>`.
//!
//! Everything needed is already stored and kept current by `update osm`:
//! `import osm` writes the members of *every* relation and the node refs of
//! *every* way, with no tag filter. What the store does not have is tags, so
//! nothing here can tell a boundary from any other relation -- and nothing
//! needs to: any relation whose ways close into rings is a legitimate request
//! area. The real guard against a huge request (the national boundary) is the
//! size limit the caller applies, not the relation's type.
//!
//! Three rules, each with a reason:
//!
//! 1. **The area is all-or-nothing.** A missing way, a missing node or a ring
//!    that does not close is an error naming what is missing, never a polygon
//!    built from the members that did resolve. A partial boundary is a wrong
//!    package, and silently so. This is the rule `update osm` follows for
//!    objects at the extract's edge.
//! 2. **Closure is checked on node ids, before any geometry exists.** Every
//!    way's two endpoints are counted, and the member ways form closed rings
//!    exactly when every endpoint node is counted an even number of times
//!    (an Euler argument: interior nodes contribute two each implicitly).
//!    `ST_BuildArea` would otherwise drop a dangling way without a word and
//!    return the area of whatever rings remained.
//! 3. **Roles are not trusted; nesting decides.** `ST_BuildArea` polygonizes
//!    the member ways and assigns holes by even-odd nesting, so an island in a
//!    lake in a boundary comes out right, and a ring tagged with the wrong
//!    role does not punch a hole where none belongs. Only way members with
//!    the `outer`/`inner`/empty roles take part (`encoding::encode_member_role`
//!    maps every other role to one value); node and sub-relation members are
//!    ignored.

use std::collections::{BTreeSet, HashMap};

use anyhow::{Context, Result};
use duckdb::{Connection, OptionalExt};

use super::encoding;
use super::kvstore::{self, RocksDB};

/// At most this many ids are named in an error message; the count of the rest
/// follows. A relation clipped by the extract's edge can miss thousands of
/// nodes, and the message is shown to a person.
const MAX_NAMED_IDS: usize = 10;

/// Why a relation's area cannot be built. Every variant except [`NotFound`]
/// is a 400 for `/package`: the relation exists, it just does not describe an
/// area this server can assemble.
///
/// [`NotFound`]: RelationAreaError::NotFound
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationAreaError {
    NotFound,
    /// No way member with an area role.
    NoWayMembers,
    /// Way members the store does not have -- typically outside the extract.
    MissingWays(Vec<i64>),
    /// Endpoint nodes where a ring stays open.
    OpenRings(Vec<i64>),
    /// Nodes referenced by member ways that the store does not have.
    MissingNodes(Vec<i64>),
    /// The rings closed, but enclose no area (e.g. a degenerate ring).
    NoArea,
}

impl RelationAreaError {
    /// The message shown to the client. Names the relation, because the
    /// frontend shows it verbatim and the user may have typed the id.
    pub fn message(&self, relation_id: i64) -> String {
        match self {
            Self::NotFound => format!("relation {relation_id} not found"),
            Self::NoWayMembers => {
                format!("relation {relation_id} has no way members to build an area from")
            }
            Self::MissingWays(ids) => format!(
                "relation {relation_id} cannot be built: missing {}",
                name_ids("way", ids)
            ),
            Self::OpenRings(ids) => format!(
                "relation {relation_id} cannot be built: its ways do not close into rings (open at {})",
                name_ids("node", ids)
            ),
            Self::MissingNodes(ids) => format!(
                "relation {relation_id} cannot be built: missing {}",
                name_ids("node", ids)
            ),
            Self::NoArea => format!("relation {relation_id} encloses no area"),
        }
    }
}

/// `way/1, way/2, ... (and 15 more)`.
fn name_ids(kind: &str, ids: &[i64]) -> String {
    let mut s = ids
        .iter()
        .take(MAX_NAMED_IDS)
        .map(|id| format!("{kind}/{id}"))
        .collect::<Vec<_>>()
        .join(", ");
    if ids.len() > MAX_NAMED_IDS {
        s.push_str(&format!(" (and {} more)", ids.len() - MAX_NAMED_IDS));
    }
    s
}

/// A relation's assembled area: a GeoJSON Polygon/MultiPolygon and its
/// bounding box.
#[derive(Debug, Clone)]
pub struct RelationArea {
    pub geojson: String,
    pub min_lon: f64,
    pub min_lat: f64,
    pub max_lon: f64,
    pub max_lat: f64,
}

/// Assemble relation `relation_id`'s area. The outer `Result` is an
/// infrastructure failure (RocksDB, DuckDB); the inner one is a property of
/// the relation, which the caller turns into a 4xx.
pub fn relation_area(
    conn: &Connection,
    kv: &RocksDB,
    relation_id: i64,
) -> Result<std::result::Result<RelationArea, RelationAreaError>> {
    let lines = match relation_lines(kv, relation_id)? {
        Ok(lines) => lines,
        Err(e) => return Ok(Err(e)),
    };
    build_area(conn, &lines)
}

/// The member ways' lines as a GeoJSON MultiLineString, after every check
/// that can be made on ids alone.
fn relation_lines(
    kv: &RocksDB,
    relation_id: i64,
) -> Result<std::result::Result<String, RelationAreaError>> {
    let Some(members) = kvstore::get_relation(kv, relation_id)? else {
        return Ok(Err(RelationAreaError::NotFound));
    };
    let way = encoding::encode_member_type("way");
    let area_roles = [
        encoding::encode_member_role("outer"),
        encoding::encode_member_role("inner"),
    ];
    // Deduplicated: a way listed twice would be drawn twice, and its
    // endpoints counted twice -- which the parity check would wave through.
    let way_ids: BTreeSet<i64> = members
        .iter()
        .filter(|(_, t, role)| *t == way && area_roles.contains(role))
        .map(|(id, _, _)| *id)
        .collect();
    if way_ids.is_empty() {
        return Ok(Err(RelationAreaError::NoWayMembers));
    }

    let mut refs_by_way: Vec<Vec<i64>> = Vec::with_capacity(way_ids.len());
    let mut missing_ways = Vec::new();
    for &way_id in &way_ids {
        match kvstore::get_way(kv, way_id)? {
            // A one-node way draws nothing and closes nothing; GeoJSON
            // cannot even express it as a LineString.
            Some(refs) if refs.len() >= 2 => refs_by_way.push(refs),
            Some(_) => {}
            None => missing_ways.push(way_id),
        }
    }
    if !missing_ways.is_empty() {
        return Ok(Err(RelationAreaError::MissingWays(missing_ways)));
    }

    let open = open_ends(&refs_by_way);
    if !open.is_empty() {
        return Ok(Err(RelationAreaError::OpenRings(open)));
    }

    let mut lines: Vec<Vec<[f64; 2]>> = Vec::with_capacity(refs_by_way.len());
    let mut missing_nodes = BTreeSet::new();
    for refs in &refs_by_way {
        let coords = kvstore::multi_get_nodes(kv, refs)?;
        let mut line = Vec::with_capacity(refs.len());
        for (node_id, c) in refs.iter().zip(coords) {
            match c {
                Some((lon, lat)) => line.push([
                    encoding::decimicro_to_f64(lon),
                    encoding::decimicro_to_f64(lat),
                ]),
                None => {
                    missing_nodes.insert(*node_id);
                }
            }
        }
        lines.push(line);
    }
    if !missing_nodes.is_empty() {
        return Ok(Err(RelationAreaError::MissingNodes(
            missing_nodes.into_iter().collect(),
        )));
    }

    let geojson = serde_json::json!({ "type": "MultiLineString", "coordinates": lines });
    Ok(Ok(geojson.to_string()))
}

/// Endpoint nodes counted an odd number of times, sorted. Empty exactly when
/// the ways decompose into closed rings.
fn open_ends(refs_by_way: &[Vec<i64>]) -> Vec<i64> {
    let mut count: HashMap<i64, u32> = HashMap::new();
    for refs in refs_by_way {
        // A closed way counts its one endpoint twice, which is even: closed.
        *count.entry(refs[0]).or_default() += 1;
        *count.entry(refs[refs.len() - 1]).or_default() += 1;
    }
    let mut open: Vec<i64> = count
        .into_iter()
        .filter(|(_, n)| n % 2 == 1)
        .map(|(id, _)| id)
        .collect();
    open.sort_unstable();
    open
}

/// `ST_BuildArea` over the closed rings: polygonize, then holes by even-odd
/// nesting (see the module doc).
fn build_area(
    conn: &Connection,
    lines_geojson: &str,
) -> Result<std::result::Result<RelationArea, RelationAreaError>> {
    // No row at all when the rings enclose nothing: `ST_BuildArea` returns
    // an empty collection then, never an error.
    let area = conn
        .query_row(
            "WITH a AS (SELECT ST_BuildArea(ST_GeomFromGeoJSON(?)) AS g)
             SELECT ST_AsGeoJSON(g)::VARCHAR, ST_XMin(g), ST_YMin(g), ST_XMax(g), ST_YMax(g)
             FROM a
             WHERE ST_GeometryType(g)::VARCHAR IN ('POLYGON', 'MULTIPOLYGON')
               AND ST_Area(g) > 0",
            [lines_geojson],
            |r| {
                Ok(RelationArea {
                    geojson: r.get(0)?,
                    min_lon: r.get(1)?,
                    min_lat: r.get(2)?,
                    max_lon: r.get(3)?,
                    max_lat: r.get(4)?,
                })
            },
        )
        .optional()
        .context("Failed to build the relation's area")?;
    Ok(area.ok_or(RelationAreaError::NoArea))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::path::Path;

    use super::*;
    use crate::db::init_db;

    fn conn() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    pub(crate) fn temp_kv() -> (tempfile::TempDir, RocksDB) {
        let dir = tempfile::tempdir().unwrap();
        let kv = kvstore::open(dir.path(), 8, 4, 8).unwrap();
        (dir, kv)
    }

    /// Store a way and its nodes. Node ids are given with their coordinates
    /// so a closed way repeats its first entry, exactly as OSM does.
    pub(crate) fn put_way(kv: &RocksDB, way_id: i64, nodes: &[(i64, f64, f64)]) {
        for &(id, lon, lat) in nodes {
            kvstore::put_node(
                kv,
                id,
                encoding::f64_to_decimicro(lon),
                encoding::f64_to_decimicro(lat),
            )
            .unwrap();
        }
        let refs: Vec<i64> = nodes.iter().map(|n| n.0).collect();
        kvstore::put_way(kv, way_id, &refs).unwrap();
    }

    pub(crate) fn put_relation(kv: &RocksDB, relation_id: i64, members: &[(&str, i64, &str)]) {
        let members: Vec<(i64, u8, u8)> = members
            .iter()
            .map(|&(t, id, role)| {
                (
                    id,
                    encoding::encode_member_type(t),
                    encoding::encode_member_role(role),
                )
            })
            .collect();
        kvstore::put_relation(kv, relation_id, &members).unwrap();
    }

    /// A 1° square split into two ways meeting at nodes 1 and 3 -- the shape
    /// of a real boundary, which is never one closed way.
    fn put_square_in_two_ways(kv: &RocksDB, first_way: i64) {
        put_way(
            kv,
            first_way,
            &[(1, 0.0, 0.0), (2, 1.0, 0.0), (3, 1.0, 1.0)],
        );
        put_way(
            kv,
            first_way + 1,
            &[(3, 1.0, 1.0), (4, 0.0, 1.0), (1, 0.0, 0.0)],
        );
    }

    fn area_of(conn: &Connection, a: &RelationArea) -> f64 {
        conn.query_row(
            "SELECT ST_Area(ST_GeomFromGeoJSON(?))",
            [a.geojson.as_str()],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn covers(conn: &Connection, a: &RelationArea, lon: f64, lat: f64) -> bool {
        conn.query_row(
            "SELECT ST_Intersects(ST_GeomFromGeoJSON(?), ST_Point(?, ?))",
            duckdb::params![a.geojson.as_str(), lon, lat],
            |r| r.get(0),
        )
        .unwrap()
    }

    #[test]
    fn rings_split_across_ways_are_stitched_and_the_inner_ring_is_a_hole() {
        let (_dir, kv) = temp_kv();
        let conn = conn();
        put_square_in_two_ways(&kv, 10);
        put_way(
            &kv,
            20,
            &[
                (11, 0.25, 0.25),
                (12, 0.75, 0.25),
                (13, 0.75, 0.75),
                (14, 0.25, 0.75),
                (11, 0.25, 0.25),
            ],
        );
        put_relation(
            &kv,
            100,
            &[
                ("way", 10, "outer"),
                ("way", 11, "outer"),
                ("way", 20, "inner"),
            ],
        );

        let a = relation_area(&conn, &kv, 100).unwrap().unwrap();
        assert_eq!(
            (a.min_lon, a.min_lat, a.max_lon, a.max_lat),
            (0.0, 0.0, 1.0, 1.0)
        );
        assert_eq!(area_of(&conn, &a), 0.75);
        assert!(covers(&conn, &a, 0.125, 0.125));
        assert!(!covers(&conn, &a, 0.5, 0.5));
    }

    /// Nesting decides, not roles: an island inside the hole is area again,
    /// and a ring wrongly tagged `outer` inside an outer ring still becomes
    /// a hole rather than being unioned away.
    #[test]
    fn holes_follow_nesting_not_roles() {
        let (_dir, kv) = temp_kv();
        let conn = conn();
        put_square_in_two_ways(&kv, 10);
        put_way(
            &kv,
            20,
            &[
                (11, 0.25, 0.25),
                (12, 0.75, 0.25),
                (13, 0.75, 0.75),
                (14, 0.25, 0.75),
                (11, 0.25, 0.25),
            ],
        );
        put_way(
            &kv,
            30,
            &[
                (21, 0.375, 0.375),
                (22, 0.625, 0.375),
                (23, 0.625, 0.625),
                (24, 0.375, 0.625),
                (21, 0.375, 0.375),
            ],
        );
        // The hole is mistagged `outer`; the island correctly `outer`.
        put_relation(
            &kv,
            100,
            &[
                ("way", 10, ""),
                ("way", 11, "outer"),
                ("way", 20, "outer"),
                ("way", 30, "outer"),
            ],
        );

        let a = relation_area(&conn, &kv, 100).unwrap().unwrap();
        assert_eq!(area_of(&conn, &a), 0.75 + 0.0625);
        assert!(covers(&conn, &a, 0.125, 0.125));
        assert!(!covers(&conn, &a, 0.3125, 0.3125));
        assert!(covers(&conn, &a, 0.5, 0.5));
    }

    #[test]
    fn a_way_listed_twice_is_drawn_once() {
        let (_dir, kv) = temp_kv();
        let conn = conn();
        put_square_in_two_ways(&kv, 10);
        put_relation(
            &kv,
            100,
            &[
                ("way", 10, "outer"),
                ("way", 11, "outer"),
                ("way", 10, "outer"),
            ],
        );
        let a = relation_area(&conn, &kv, 100).unwrap().unwrap();
        assert_eq!(area_of(&conn, &a), 1.0);
    }

    #[test]
    fn node_sub_relation_and_other_role_members_are_ignored() {
        let (_dir, kv) = temp_kv();
        let conn = conn();
        put_square_in_two_ways(&kv, 10);
        // An open way with a non-area role would fail the closure check if
        // it took part.
        put_way(&kv, 40, &[(31, 2.0, 2.0), (32, 3.0, 3.0)]);
        put_relation(
            &kv,
            100,
            &[
                ("way", 10, "outer"),
                ("way", 11, "outer"),
                ("node", 1, "admin_centre"),
                ("relation", 7, "subarea"),
                ("way", 40, "street"),
            ],
        );
        let a = relation_area(&conn, &kv, 100).unwrap().unwrap();
        assert_eq!(area_of(&conn, &a), 1.0);
    }

    #[test]
    fn an_unknown_relation_is_not_found() {
        let (_dir, kv) = temp_kv();
        assert_eq!(
            relation_area(&conn(), &kv, 100).unwrap().unwrap_err(),
            RelationAreaError::NotFound
        );
    }

    #[test]
    fn a_relation_without_area_way_members_has_none() {
        let (_dir, kv) = temp_kv();
        put_way(&kv, 40, &[(31, 2.0, 2.0), (32, 3.0, 3.0)]);
        put_relation(
            &kv,
            100,
            &[
                ("node", 1, ""),
                ("relation", 7, "subarea"),
                ("way", 40, "street"),
            ],
        );
        assert_eq!(
            relation_area(&conn(), &kv, 100).unwrap().unwrap_err(),
            RelationAreaError::NoWayMembers
        );
    }

    /// The extract's edge: never build from the members that did resolve.
    #[test]
    fn a_missing_way_fails_the_whole_relation() {
        let (_dir, kv) = temp_kv();
        put_square_in_two_ways(&kv, 10);
        put_relation(
            &kv,
            100,
            &[
                ("way", 10, "outer"),
                ("way", 11, "outer"),
                ("way", 99, "inner"),
            ],
        );
        assert_eq!(
            relation_area(&conn(), &kv, 100).unwrap().unwrap_err(),
            RelationAreaError::MissingWays(vec![99])
        );
    }

    #[test]
    fn a_missing_node_fails_the_whole_relation() {
        let (_dir, kv) = temp_kv();
        put_square_in_two_ways(&kv, 10);
        kvstore::delete_node(&kv, 2).unwrap();
        kvstore::delete_node(&kv, 4).unwrap();
        put_relation(&kv, 100, &[("way", 10, "outer"), ("way", 11, "outer")]);
        assert_eq!(
            relation_area(&conn(), &kv, 100).unwrap().unwrap_err(),
            RelationAreaError::MissingNodes(vec![2, 4])
        );
    }

    /// `ST_BuildArea` alone would drop the dangling way and return the
    /// closed ring's area as if nothing were wrong.
    #[test]
    fn a_ring_that_does_not_close_is_refused_even_beside_one_that_does() {
        let (_dir, kv) = temp_kv();
        put_square_in_two_ways(&kv, 10);
        put_way(&kv, 40, &[(31, 2.0, 2.0), (32, 3.0, 2.0), (33, 3.0, 3.0)]);
        put_relation(
            &kv,
            100,
            &[
                ("way", 10, "outer"),
                ("way", 11, "outer"),
                ("way", 40, "outer"),
            ],
        );
        assert_eq!(
            relation_area(&conn(), &kv, 100).unwrap().unwrap_err(),
            RelationAreaError::OpenRings(vec![31, 33])
        );
    }

    /// Exact binary fractions, so the ring is genuinely collinear (see
    /// CLAUDE.md's fixture gotcha).
    #[test]
    fn a_closed_ring_with_no_interior_encloses_no_area() {
        let (_dir, kv) = temp_kv();
        put_way(
            &kv,
            10,
            &[
                (1, 21.0, 52.0),
                (2, 21.0625, 52.0),
                (3, 21.125, 52.0),
                (1, 21.0, 52.0),
            ],
        );
        put_relation(&kv, 100, &[("way", 10, "outer")]);
        assert_eq!(
            relation_area(&conn(), &kv, 100).unwrap().unwrap_err(),
            RelationAreaError::NoArea
        );
    }

    #[test]
    fn error_messages_name_at_most_ten_ids() {
        let ids: Vec<i64> = (1..=12).collect();
        assert_eq!(
            RelationAreaError::MissingWays(ids).message(5),
            "relation 5 cannot be built: missing way/1, way/2, way/3, way/4, way/5, way/6, \
             way/7, way/8, way/9, way/10 (and 2 more)"
        );
        assert_eq!(
            RelationAreaError::OpenRings(vec![31, 33]).message(5),
            "relation 5 cannot be built: its ways do not close into rings (open at node/31, node/33)"
        );
    }
}
