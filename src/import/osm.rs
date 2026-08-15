use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use duckdb::Connection;
use osmpbf::{BlobDecode, BlobReader, Element, RelMemberType};
use tracing::info;

use crate::config::Config;
use crate::download::download_file;
use crate::osm::encoding;
use crate::osm::geometry;
use crate::osm::kvstore;
use crate::osm::kvstore::RocksDB;
use crate::osm::lifecycle;
use crate::osm::pbf_header::read_replication_info;
use crate::utils::format_duration;

fn reset_osm_tables(conn: &Connection, kv: &RocksDB) -> Result<()> {
    conn.execute_batch(
        "
        DROP TABLE IF EXISTS osm_addresses;
        DROP TABLE IF EXISTS osm_buildings;
        DROP TABLE IF EXISTS osm_former_buildings;
        CREATE TABLE osm_addresses (
            osm_id BIGINT,
            osm_type VARCHAR,
            housenumber VARCHAR,
            street VARCHAR,
            city VARCHAR,
            postcode VARCHAR,
            geom GEOMETRY
        );
        CREATE TABLE osm_buildings (
            osm_id BIGINT,
            osm_type VARCHAR,
            building VARCHAR,
            geom GEOMETRY
        );
        -- OSM ways/relations tagged with a lifecycle-prefixed building key
        -- (demolished:building, ruins:building, ...). Not buildings -- the OSM
        -- record that a building here is gone. Read only by compare::rule's
        -- suppression veto. Kept in sync with db::create_schema, which
        -- declares this table too (as CREATE TABLE IF NOT EXISTS, for a
        -- database that has never run `import osm`).
        CREATE TABLE osm_former_buildings (
            osm_id BIGINT,
            osm_type VARCHAR,
            lifecycle_key VARCHAR,
            lifecycle_value VARCHAR,
            geom GEOMETRY
        );
        ",
    )
    .context("Failed to reset OSM tables")?;
    kvstore::clear(kv).context("Failed to clear RocksDB")?;
    Ok(())
}

pub fn import(
    conn: &Connection,
    kv: &RocksDB,
    config: &Config,
    file: Option<&Path>,
    url: &str,
) -> Result<()> {
    // The whole import lives in this closure so that every exit path --
    // success, any `?` propagated from a step, and a `check_shutdown()` bail
    // in particular -- funnels through the single `job_run_log` self-report
    // below. Self-reported here rather than at import/mod.rs's call sites
    // because `ImportSource::Full` also calls this function, and
    // self-reporting keeps "record import:osm's outcome" a single site
    // regardless of which CLI arm got here (mirrors bdot10k::import,
    // egib::import and update::dataset::refresh's outcome/match shape).
    let outcome = (|| -> Result<String> {
        let (pbf_path, was_downloaded) = match file {
            Some(path) => (PathBuf::from(path), false),
            None => (download_file(url, &config.download_dir())?, true),
        };

        let pbf_str = pbf_path.to_str().context("PBF path is not valid UTF-8")?;

        // Read the header now -- an unreadable or malformed PBF should fail
        // fast, before hours of work -- but hold the parsed value rather than
        // writing it to `metadata` yet. It gets written only after every
        // data-loading step below has succeeded; see the comment beside that
        // write for why the split is load-bearing.
        let replication_info = read_replication_info(&pbf_path)?;
        if replication_info.is_none() {
            tracing::warn!("PBF header has no replication metadata — update start point unknown");
        }

        reset_osm_tables(conn, kv)?;

        info!(path = pbf_str, "Starting OSM import");

        let total = std::time::Instant::now();

        let check_shutdown = || -> Result<()> {
            crate::shutdown::check_requested()?;
            Ok(())
        };

        // One pass writes the nodes, ways and relations key spaces together;
        // see `stream_pbf_to_rocksdb` for why that is safe and why it replaced
        // three separate scans.
        let t = std::time::Instant::now();
        stream_pbf_to_rocksdb(kv, &pbf_path)?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: stream PBF to RocksDB"
        );
        check_shutdown()?;

        let t = std::time::Instant::now();
        import_address_nodes(conn, pbf_str)?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: import address nodes"
        );
        check_shutdown()?;

        let t = std::time::Instant::now();
        import_way_buildings_and_addresses(conn, pbf_str)?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: import way buildings and addresses"
        );
        check_shutdown()?;

        let t = std::time::Instant::now();
        import_way_former_buildings(conn, pbf_str)?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: import way former buildings"
        );
        check_shutdown()?;

        let t = std::time::Instant::now();
        import_relation_buildings_and_addresses(conn, pbf_str)?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: import relation buildings and addresses"
        );
        check_shutdown()?;

        let t = std::time::Instant::now();
        import_relation_former_buildings(conn, pbf_str)?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: import relation former buildings"
        );
        check_shutdown()?;

        // After every insert pass, before the RTREE indexes below, so they are
        // built over final geometry and never have to be maintained through
        // the repair UPDATE. Covers all four polygon insert passes above from
        // two call sites -- see `osm::geometry::repair_invalid_geometry` for
        // why the import path scans while `update::osm` wraps inline instead.
        // `osm_addresses` is deliberately absent: every row there is a point
        // (`ST_Centroid`), and a point cannot be invalid.
        let t = std::time::Instant::now();
        let repair = geometry::repair_invalid_geometry(conn, "osm_buildings")?.merge(
            geometry::repair_invalid_geometry(conn, "osm_former_buildings")?,
        );
        info!(
            repaired = repair.repaired,
            dropped_degenerate = repair.dropped_degenerate,
            elapsed = %format_duration(t.elapsed()),
            "Step done: repair invalid geometry"
        );
        check_shutdown()?;

        let t = std::time::Instant::now();
        kvstore::compact_reverse_indexes(kv);
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: compact reverse indexes"
        );
        check_shutdown()?;

        let t = std::time::Instant::now();
        create_spatial_indexes(conn)?;
        info!(
            elapsed = %format_duration(t.elapsed()),
            "Step done: create spatial indexes"
        );

        // Stamp the replication metadata LAST -- only now that every
        // data-loading step above has actually landed. `update osm`'s
        // catch-up loop resumes from this stamp, so it must never be visible
        // for an import that did not finish. Stamping it up front (the
        // previous behaviour) meant a Ctrl+C at, say, step 3 left the
        // sequence recorded while `osm_addresses` was half full,
        // `osm_buildings` was empty and RocksDB was partial -- and a later
        // `update osm` or `run` would proceed against that state with no
        // indication the import never completed. A PBF with no replication
        // metadata in its header (warned about above, at the top) writes
        // nothing here either way.
        if let Some((seq, timestamp)) = replication_info {
            conn.execute_batch(&format!(
                "DELETE FROM metadata WHERE key IN ('osm_replication_sequence', 'osm_replication_timestamp');
                 INSERT INTO metadata VALUES ('osm_replication_sequence', '{seq}');
                 INSERT INTO metadata VALUES ('osm_replication_timestamp', '{timestamp}');"
            ))
            .context("Failed to store replication metadata")?;
            info!(
                sequence = seq,
                timestamp, "OSM replication metadata from PBF header"
            );
        }

        let (buildings, addresses, former_buildings) = log_import_stats(conn)?;

        if was_downloaded && config.cleanup_downloaded_files {
            info!(path = %pbf_path.display(), "Cleaning up downloaded file");
            let _ = std::fs::remove_file(&pbf_path);
        }

        let elapsed = total.elapsed();
        info!(
            elapsed = %format_duration(elapsed),
            "OSM import complete"
        );

        // The repair clause is appended only when the pass actually did
        // something, so the usual message stays exactly as it was. Unlike the
        // three government sources, whose clauses report rows *skipped*, this
        // one reports rows fixed in place -- OSM data is repaired, not dropped
        // (see `osm::geometry`'s module doc for the asymmetry).
        let repair_clause = repair
            .summary_clause()
            .map(|c| format!(" {c}"))
            .unwrap_or_default();
        Ok(format!(
            "buildings={buildings} addresses={addresses} former_buildings={former_buildings} \
             elapsed={}{repair_clause}",
            format_duration(elapsed)
        ))
    })();

    // bdot10k/egib/prg all self-report via job_run_log (see their `import`
    // functions and `update::dataset::refresh`); OSM previously did not, so
    // `/status` showed nothing for it either way an import went. A failure to
    // write the log must never fail the job itself -- see
    // `job_log::record`'s own doc comment, which is why this is `let _ =`.
    match &outcome {
        Ok(msg) => {
            let _ = crate::job_log::record(conn, "import:osm", "Success", Some(msg));
        }
        Err(e) => {
            let _ = crate::job_log::record(conn, "import:osm", "Error", Some(&format!("{e:#}")));
        }
    }
    outcome.map(|_| ())
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

/// Counts reported by [`stream_pbf_to_rocksdb`], purely for logging.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PbfCounts {
    nodes: u64,
    ways: u64,
    relations: u64,
}

/// Stream every node, way and relation into RocksDB in a single parallel pass
/// over the PBF.
///
/// # Why one pass and not three
///
/// This replaces what used to be three separate `ST_ReadOSM` scans (nodes,
/// then ways, then relations), each of which decompressed the entire 2.2 GB
/// file. Collapsing them is safe because all three are **pure writes** — none
/// reads anything back out of RocksDB — so they have no ordering dependency on
/// each other, and in particular this does *not* rely on the PBF being sorted
/// by element type. The passes that do read (`resolve_node_coords` needs the
/// nodes CF, `resolve_way_coords` the ways CF) all run after this returns.
///
/// # Why `BlobReader` rather than `IndexedReader`
///
/// `IndexedReader::create_index` only reads blob *headers* and skips the
/// bodies, so it knows each blob's offset but not its contents; the per-blob
/// id ranges that would let it skip anything are filled in lazily, only once a
/// blob has actually been decompressed. Skipping therefore never helps a first
/// pass. Reading every blob exactly once beats any amount of skipping across
/// repeated passes.
///
/// # Why this loop is sequential
///
/// An earlier version ran the blob loop through rayon's `par_bridge`, one
/// `WriteBatch` per blob across all 12 cores. That is *correct* — every write
/// here is a blind put or a commutative merge, never a read-modify-write, so
/// blobs may be decoded and committed in any order — but it was measured and
/// dropped: it bought 41s on a 5m 00s pass (see `docs/kv_encoding_measured.md`)
/// because the pass is bound by RocksDB write throughput and the sequential
/// blob read, not by decode CPU. Almost all of the win over the old
/// three-scan version comes from decompressing the file once instead of three
/// times, not from threading. A `rayon` dependency and nondeterministic
/// `node_to_ways` ordering were not worth 7% of one import step.
///
/// The same measurement covers the decompression backend: forcing
/// `osmpbf`'s `zlib-ng` feature moved the pass to 4m 46s. Note the default
/// build does *not* use miniz_oxide — `zip` (via `prg_convert`) already
/// enables `flate2/zlib-rs`, which outranks `flate2`'s `rust_backend`, so the
/// baseline is already a fast zlib. Adding `zlib-ng` on top trades a cmake +
/// C build for ~5% of one step.
fn stream_pbf_to_rocksdb(kv: &RocksDB, pbf_path: &Path) -> Result<PbfCounts> {
    info!("Pass 1: Streaming nodes, ways and relations to RocksDB");

    let reader = BlobReader::from_path(pbf_path)
        .with_context(|| format!("Failed to open PBF at {}", pbf_path.display()))?;

    let nodes = AtomicU64::new(0);
    let ways = AtomicU64::new(0);
    let relations = AtomicU64::new(0);

    for blob in reader {
        // Polled per blob rather than per element: a blob is ~8k elements, so
        // this is frequent enough to stay responsive to Ctrl+C without
        // checking an atomic millions of times.
        crate::shutdown::check_requested()?;

        let block = match blob.context("Failed to read PBF blob")?.decode()? {
            BlobDecode::OsmData(block) => block,
            BlobDecode::OsmHeader(_) | BlobDecode::Unknown(_) => continue,
        };

        let mut batch = kvstore::new_batch();
        let (mut n, mut w, mut r) = (0u64, 0u64, 0u64);

        for element in block.elements() {
            match element {
                Element::Node(node) => {
                    kvstore::batch_put_node(
                        kv,
                        &mut batch,
                        node.id(),
                        node.decimicro_lon(),
                        node.decimicro_lat(),
                    );
                    n += 1;
                }
                Element::DenseNode(node) => {
                    kvstore::batch_put_node(
                        kv,
                        &mut batch,
                        node.id(),
                        node.decimicro_lon(),
                        node.decimicro_lat(),
                    );
                    n += 1;
                }
                Element::Way(way) => {
                    // `refs()`, not `raw_refs()` -- the latter returns the
                    // delta-coded values straight out of the protobuf.
                    let refs: Vec<i64> = way.refs().collect();
                    if refs.is_empty() {
                        continue;
                    }
                    kvstore::batch_put_way(kv, &mut batch, way.id(), &refs);
                    for &node_id in &refs {
                        kvstore::batch_merge_node_to_way(kv, &mut batch, node_id, way.id());
                    }
                    w += 1;
                }
                Element::Relation(rel) => {
                    let members: Vec<(i64, u8, u8)> = rel
                        .members()
                        .map(|m| {
                            let type_str = match m.member_type {
                                RelMemberType::Node => "node",
                                RelMemberType::Way => "way",
                                RelMemberType::Relation => "relation",
                            };
                            (
                                m.member_id,
                                encoding::encode_member_type(type_str),
                                encoding::encode_member_role(m.role().unwrap_or("")),
                            )
                        })
                        .collect();
                    if members.is_empty() {
                        continue;
                    }
                    kvstore::batch_put_relation(kv, &mut batch, rel.id(), &members);
                    for &(way_id, member_type, _) in &members {
                        if member_type == encoding::encode_member_type("way") {
                            kvstore::batch_merge_way_to_relation(kv, &mut batch, way_id, rel.id());
                        }
                    }
                    r += 1;
                }
            }
        }

        kvstore::write_batch(kv, batch)?;
        nodes.fetch_add(n, Ordering::Relaxed);
        ways.fetch_add(w, Ordering::Relaxed);
        relations.fetch_add(r, Ordering::Relaxed);
    }

    let counts = PbfCounts {
        nodes: nodes.load(Ordering::Relaxed),
        ways: ways.load(Ordering::Relaxed),
        relations: relations.load(Ordering::Relaxed),
    };
    info!(
        nodes = counts.nodes,
        ways = counts.ways,
        relations = counts.relations,
        "PBF streamed to RocksDB"
    );
    Ok(counts)
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

/// Ways carrying a lifecycle-prefixed building key (`demolished:building`,
/// `ruins:building`, ...) — the OSM record that a building here is gone, not
/// a building itself. Kept disjoint from `osm_buildings` (see the
/// disjointness decision in the plan): an object also carrying a live
/// `building` key stays a normal `osm_buildings` row.
fn import_way_former_buildings(conn: &Connection, pbf_path: &str) -> Result<()> {
    info!("Importing way former buildings");
    conn.execute_batch(&format!(
        "INSERT INTO osm_former_buildings (osm_id, osm_type, lifecycle_key, lifecycle_value, geom)
         SELECT id, 'way',
                {matched_key},
                element_at(tags, {matched_key})[1],
                ST_MakePolygon(ST_GeomFromWKB(resolve_node_coords(refs)))
         FROM ST_ReadOSM('{pbf_path}')
         WHERE kind = 'way'
           AND refs IS NOT NULL
           AND len(refs) >= 4
           AND refs[1] = refs[len(refs)]
           AND {is_former_building}
           AND resolve_node_coords(refs) IS NOT NULL",
        matched_key = lifecycle::matched_key_sql("tags"),
        is_former_building = lifecycle::is_former_building_sql("tags"),
    ))
    .context("Failed to import way former buildings")?;

    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_former_buildings WHERE osm_type = 'way'",
        [],
        |row| row.get(0),
    )?;
    info!(count, "Way former buildings imported");

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

/// Relations carrying a lifecycle-prefixed building key. Same CTE chain as
/// `import_relation_buildings_and_addresses`'s building half, with the
/// `building` tag replaced by the lifecycle key/value pair, carried through
/// `way_geoms` and `outer_polys`' `GROUP BY` the same way `building` is.
fn import_relation_former_buildings(conn: &Connection, pbf_path: &str) -> Result<()> {
    info!("Importing relation former buildings");
    conn.execute_batch(&format!(
        "INSERT INTO osm_former_buildings (osm_id, osm_type, lifecycle_key, lifecycle_value, geom)
         WITH rel_members AS (
             SELECT
                 id AS relation_id,
                 {matched_key} AS lifecycle_key,
                 element_at(tags, {matched_key})[1] AS lifecycle_value,
                 unnest(refs) AS ref_id,
                 unnest(ref_types) AS ref_type,
                 unnest(ref_roles) AS ref_role
             FROM ST_ReadOSM('{pbf_path}')
             WHERE kind = 'relation'
               AND refs IS NOT NULL
               AND len(refs) > 0
               AND {is_former_building}
         ),
         way_geoms AS (
             SELECT
                 relation_id, lifecycle_key, lifecycle_value, ref_role,
                 ST_GeomFromWKB(resolve_way_coords(ref_id)) AS line_geom
             FROM rel_members
             WHERE ref_type = 'way'
               AND resolve_way_coords(ref_id) IS NOT NULL
         ),
         outer_polys AS (
             SELECT relation_id, lifecycle_key, lifecycle_value,
                    ST_Union_Agg(ST_MakePolygon(line_geom)) AS outer_geom
             FROM way_geoms
             WHERE (ref_role = 'outer' OR ref_role = '')
               AND ST_NPoints(line_geom) >= 4
               AND ST_IsClosed(line_geom)
             GROUP BY relation_id, lifecycle_key, lifecycle_value
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
             o.relation_id, 'relation', o.lifecycle_key, o.lifecycle_value,
             CASE
                 WHEN i.inner_geom IS NOT NULL THEN ST_Difference(o.outer_geom, i.inner_geom)
                 ELSE o.outer_geom
             END AS geom
         FROM outer_polys o
         LEFT JOIN inner_polys i ON o.relation_id = i.relation_id
         WHERE o.outer_geom IS NOT NULL",
        matched_key = lifecycle::matched_key_sql("tags"),
        is_former_building = lifecycle::is_former_building_sql("tags"),
    ))
    .context("Failed to import relation former buildings")?;

    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM osm_former_buildings WHERE osm_type = 'relation'",
        [],
        |row| row.get(0),
    )?;
    info!(count, "Relation former buildings imported");

    Ok(())
}

fn create_spatial_indexes(conn: &Connection) -> Result<()> {
    info!("Creating spatial indexes");
    conn.execute_batch(
        "
        CREATE INDEX osm_buildings_geom_idx ON osm_buildings USING RTREE (geom);
        CREATE INDEX osm_addresses_geom_idx ON osm_addresses USING RTREE (geom);
        CREATE INDEX osm_former_buildings_geom_idx ON osm_former_buildings USING RTREE (geom);
        ",
    )
    .context("Failed to create spatial indexes")?;
    Ok(())
}

fn log_import_stats(conn: &Connection) -> Result<(i64, i64, i64)> {
    let buildings: i64 =
        conn.query_row("SELECT COUNT(*) FROM osm_buildings", [], |row| row.get(0))?;
    let addresses: i64 =
        conn.query_row("SELECT COUNT(*) FROM osm_addresses", [], |row| row.get(0))?;
    let former_buildings: i64 =
        conn.query_row("SELECT COUNT(*) FROM osm_former_buildings", [], |row| {
            row.get(0)
        })?;
    info!(buildings, addresses, former_buildings, "OSM import totals");
    Ok((buildings, addresses, former_buildings))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::config::Config;
    use crate::db::init_db;
    use crate::osm::kvstore;

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

    /// Verify the fixture's lifecycle-tagged way (w664679941,
    /// `demolished:building=yes`, its only tag) lands in
    /// `osm_former_buildings` and nowhere near `osm_buildings` — the
    /// disjointness invariant from the plan's Step 3.
    #[test]
    fn test_import_fixture_former_building_details() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;
        run_import_with_fixture(&conn, Path::new("fixtures/osm.pbf"))?;

        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM osm_former_buildings", [], |row| {
                row.get(0)
            })?;
        assert_eq!(count, 1, "Expected 1 former building (way 664679941)");

        let (osm_type, lifecycle_key, lifecycle_value, geom_type): (
            String,
            String,
            String,
            String,
        ) = conn.query_row(
            "SELECT osm_type, lifecycle_key, lifecycle_value, ST_GeometryType(geom)
             FROM osm_former_buildings WHERE osm_id = 664679941",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(osm_type, "way");
        assert_eq!(lifecycle_key, "demolished:building");
        assert_eq!(lifecycle_value, "yes");
        assert_eq!(geom_type, "POLYGON");

        let geom_valid: bool = conn.query_row(
            "SELECT ST_IsValid(geom) FROM osm_former_buildings WHERE osm_id = 664679941",
            [],
            |row| row.get(0),
        )?;
        assert!(geom_valid, "Former building geometry should be valid");

        // Disjointness: osm_buildings still has exactly the 2 real buildings,
        // and the former-building way must not have leaked into it.
        let buildings: i64 =
            conn.query_row("SELECT COUNT(*) FROM osm_buildings", [], |row| row.get(0))?;
        assert_eq!(buildings, 2, "osm_buildings should be unaffected");

        let leaked: i64 = conn.query_row(
            "SELECT COUNT(*) FROM osm_buildings WHERE osm_id = 664679941",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            leaked, 0,
            "Former building must not appear in osm_buildings"
        );

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

    /// A completed import must both stamp the replication metadata (the
    /// fixture PBF's header has genuine replication info -- see
    /// `pbf_header::tests::test_read_replication_info_from_fixture`) and
    /// record a `job_run_log` success row, which OSM did not do before this
    /// change.
    #[test]
    fn successful_import_stamps_replication_metadata_and_records_job_log_success() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;
        run_import_with_fixture(&conn, Path::new("fixtures/osm.pbf"))?;

        let stamped: String = conn.query_row(
            "SELECT value FROM metadata WHERE key = 'osm_replication_sequence'",
            [],
            |row| row.get(0),
        )?;
        assert!(
            !stamped.is_empty(),
            "a completed import must stamp the replication sequence"
        );

        let log = crate::job_log::read_all(&conn)?;
        let entry = log.get("import:osm").expect("entry must be present");
        assert_eq!(entry.outcome, "Success");
        let msg = entry.message.as_deref().unwrap();
        assert!(msg.contains("buildings=2"), "got: {msg}");
        assert!(msg.contains("addresses=3"), "got: {msg}");
        assert!(msg.contains("former_buildings=1"), "got: {msg}");

        Ok(())
    }

    /// The load-bearing ordering change: the replication stamp must not
    /// exist for an import that failed partway through, even though the PBF
    /// header itself was read successfully and has valid replication info.
    /// This is the Gap 3 hazard -- stamping up front used to leave a
    /// half-imported database indistinguishable from a complete one to a
    /// later `update osm` or `run`.
    ///
    /// There is no way to reach into the process-wide shutdown flag from a
    /// unit test (it has no public setter, by design -- see
    /// `shutdown::is_requested`), so this simulates "fails after the header
    /// read but before the import finishes" a different way: it pre-creates
    /// an index with the exact name `create_spatial_indexes` (the last
    /// data-loading step, immediately before the metadata write) will try to
    /// create, so that step -- and therefore the whole import -- fails.
    /// `check_shutdown()`'s bail is just one of many ways a late step can
    /// fail; this test exercises the ordering guarantee itself, which
    /// protects against all of them alike.
    #[test]
    fn failed_import_does_not_stamp_replication_metadata() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;
        conn.execute_batch(
            "CREATE TABLE zzz_scratch (geom GEOMETRY);
             CREATE INDEX osm_buildings_geom_idx ON zzz_scratch USING RTREE (geom);",
        )?;

        let result = run_import_with_fixture(&conn, Path::new("fixtures/osm.pbf"));
        assert!(
            result.is_err(),
            "expected the forced index-name collision to fail the import"
        );

        use duckdb::OptionalExt;
        let stamped: Option<String> = conn
            .query_row(
                "SELECT value FROM metadata WHERE key = 'osm_replication_sequence'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        assert_eq!(
            stamped, None,
            "a failed import must not stamp replication metadata, even though \
             the header itself was read successfully"
        );

        let log = crate::job_log::read_all(&conn)?;
        let entry = log
            .get("import:osm")
            .expect("entry must exist even on failure");
        assert_eq!(entry.outcome, "Error");
        assert!(entry.message.is_some());

        Ok(())
    }

    /// Mirrors `bdot10k`/`egib`'s `import_records_error_in_job_run_log_on_failure`:
    /// even a failure before any table is touched at all (here, the PBF file
    /// doesn't exist, so `read_replication_info` errors immediately) must
    /// still leave an `import:osm` row in `job_run_log`.
    #[test]
    fn import_records_error_in_job_run_log_on_missing_file() {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None).unwrap();
        let tmp_dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(kvstore::open(tmp_dir.path(), 512, 64).unwrap());
        let config = Config::default();

        let result = import(
            &conn,
            &kv,
            &config,
            Some(Path::new("nonexistent.pbf")),
            "unused",
        );
        assert!(result.is_err());

        let log = crate::job_log::read_all(&conn).unwrap();
        let entry = log
            .get("import:osm")
            .expect("entry must exist even on failure");
        assert_eq!(entry.outcome, "Error");
        assert!(entry.message.is_some());
    }
}
