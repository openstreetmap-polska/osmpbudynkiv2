use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use duckdb::{Config, Connection};

use crate::osm::kvstore::RocksDB;
use crate::osm::udf;

pub fn init_db(
    path: &Path,
    init_commands: &[String],
    kv: Option<Arc<RocksDB>>,
) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        Config::default()
            .with("storage_compatibility_version", "latest")
            .unwrap(),
    )
    .with_context(|| format!("Failed to open database at {path:?}"))?;

    if let Some(kv) = kv {
        udf::register_udfs(&conn, kv)?;
    }

    for cmd in init_commands {
        conn.execute_batch(cmd)
            .with_context(|| format!("Failed to execute DuckDB init command: {cmd}"))?;
    }

    create_schema(&conn)?;
    bind_rtree_indexes(&conn);
    warn_about_outdated_rtree_indexes(&conn);

    Ok(conn)
}

/// Binds every RTREE index on this connection, so each one holds the
/// `ClientContext` of a connection that outlives every writer.
///
/// **Workaround for <https://github.com/duckdb/duckdb-spatial/issues/915>;
/// delete this function and its call in `init_db` once a spatial release
/// with the fix is in use.** That release can also undo the pool settings in
/// `server::build_pool`.
///
/// The bug: an RTREE index keeps an `ExpressionExecutor` built from the
/// `ClientContext` of the connection that created it or, after the database
/// is reopened, the first one whose query bound it. Every later
/// insert into or delete from the table runs through that executor at
/// COMMIT, whichever connection commits. Once the binding connection is
/// closed, that is a read of freed memory. On DuckDB 2.0 it fails the commit
/// with `INTERNAL Error: Attempted to dereference shared_ptr that is NULL!`,
/// which invalidates the database. In production the first `/tiles` query
/// bound the indexes on a pooled connection, the pool closed it, and the next
/// `update osm` commit took the database down: five times on 2026-11-09.
///
/// Binding is lazy, so it has to be forced. Spatial's own
/// `pragma_rtree_index_info()` binds every RTREE index in the database and
/// fails if any stays unbound. `CREATE INDEX IF NOT EXISTS` does not bind
/// one. Measured on the national database: all 11 indexes in 0.03 s, and
/// 7 MiB of buffer pool.
///
/// It runs on the connection `init_db` returns, which the CLI uses throughout
/// and `run` keeps as its pool's base connection for the server's lifetime.
/// The indexes `import` creates later are created on that same connection.
/// **An RTREE index created on a pooled connection (none is, today) would
/// reintroduce the bug.**
///
/// Warns instead of failing, like `create_serving_indexes`. A failure leaves
/// the server exposed to the crash, which is no worse than before this
/// existed, rather than not starting at all.
fn bind_rtree_indexes(conn: &Connection) -> Vec<String> {
    let bound = conn
        .prepare("SELECT index_name FROM pragma_rtree_index_info() ORDER BY index_name")
        .and_then(|mut stmt| {
            stmt.query_map([], |row| row.get::<_, String>(0))?
                .collect::<duckdb::Result<Vec<_>>>()
        });
    match bound {
        Ok(names) => {
            tracing::debug!(
                count = names.len(),
                "bound RTREE indexes on the base connection"
            );
            names
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "could not bind RTREE indexes on the base connection; a write \
                 to an RTREE-indexed table may invalidate the database once a \
                 pooled connection that bound an index is closed \
                 (https://github.com/duckdb/duckdb-spatial/issues/915)"
            );
            Vec::new()
        }
    }
}

fn create_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS metadata (
            key VARCHAR,
            value VARCHAR
        );

        -- Last-run outcome per job/command, keyed by job_name (e.g.
        -- 'import:bdot10k', 'update:egib'). Delete-then-insert on every run
        -- (see job_log::record), so only the most recent run survives --
        -- this is a status snapshot, not a history. Read by /status.
        CREATE TABLE IF NOT EXISTS job_run_log (
            job_name VARCHAR,
            ran_at TIMESTAMP WITH TIME ZONE,
            outcome VARCHAR,
            message VARCHAR
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

        -- OSM ways/relations tagged with a lifecycle-prefixed building key
        -- (demolished:building, ruins:building, ...). Not buildings -- the OSM
        -- record that a building here is gone. Read only by compare::rule's
        -- suppression veto. Kept in sync with import::osm::reset_osm_tables,
        -- which DROPs and re-CREATEs it.
        --
        -- Note the index asymmetry: this creates the table without an RTREE
        -- index -- create_serving_indexes below covers only the three
        -- `*_unmatched` tables. The index only appears when `import osm` runs
        -- create_spatial_indexes. Empty + unindexed is harmless (there is
        -- nothing to scan), but on an in-place upgrade it also means the veto
        -- silently never fires until a re-import, which looks identical to
        -- 'no former buildings nearby'.
        CREATE TABLE IF NOT EXISTS osm_former_buildings (
            osm_id BIGINT,
            osm_type VARCHAR,
            lifecycle_key VARCHAR,
            lifecycle_value VARCHAR,
            geom GEOMETRY
        );

        -- Export log for the /package endpoint (see GET /updates). Requires
        -- the spatial extension to already be loaded (via duckdb_init_commands)
        -- before this runs, since GEOMETRY('epsg:4326') needs spatial to
        -- resolve the CRS string -- unlike the bare GEOMETRY columns above.
        CREATE TABLE IF NOT EXISTS package_exports (
            exported_at TIMESTAMP WITH TIME ZONE,
            area GEOMETRY('epsg:4326'),
            datasets VARCHAR[],
            address_count INTEGER,
            building_count INTEGER
        );

        -- One row per dataset refresh attempt, including no-ops. Owns snapshot_id,
        -- which is assigned inside the apply transaction as MAX(snapshot_id) + 1.
        CREATE TABLE IF NOT EXISTS dataset_refreshes (
            snapshot_id BIGINT PRIMARY KEY,
            source VARCHAR,
            started_at TIMESTAMP WITH TIME ZONE,
            finished_at TIMESTAMP WITH TIME ZONE,
            source_etag VARCHAR,
            added INTEGER,
            modified INTEGER,
            removed INTEGER
        );

        -- Aggregated change counts per XYZ tile (z = tile_math::CHANGE_CELL_ZOOM).
        -- Both the old and the new geometry of a changed object contribute, so an
        -- object that moves marks the cell it left and the cell it entered.
        CREATE TABLE IF NOT EXISTS dataset_change_areas (
            snapshot_id BIGINT,
            source VARCHAR,
            cell_z INTEGER,
            cell_x INTEGER,
            cell_y INTEGER,
            added INTEGER,
            modified INTEGER,
            removed INTEGER,
            detected_at TIMESTAMP WITH TIME ZONE
        );

        -- Precomputed unmatched government objects served by /tiles and /package.
        -- Only unmatched rows are stored, tagged with the z14 cell of their
        -- representative point and the time that cell was last recomputed.
        --
        -- funkcja_szczegolowa/funkcja_ogolna/liczba_kondygnacji,
        -- rodzaj_kod/kondygnacje_nadziemne, and everything below them in each
        -- CREATE TABLE are 'carried columns' -- raw government fields copied
        -- over at compare time (not computed/classified) so both
        -- server::package::building_tags (tag resolution) and /tiles (raw
        -- attribute display) can read them at serve time without joining back
        -- to the live table -- see
        -- docs/superpowers/specs/2026-08-03-building-type-mappings-design.md
        -- and docs/vector_tile_attributes.md. The column list is positional in
        -- `compare::columns::classification_columns` for bdot10k/egib (append
        -- there, both compare paths pick it up automatically) and hand-edited
        -- in `compare::addresses`/`compare::incremental` for prg (no shared
        -- mechanism exists for a single column). egib_unmatched.rodzaj_kod is
        -- populated once `import egib` computes `egib_buildings.rodzaj_kod`;
        -- until then it stays NULL, same as any column added ahead of the
        -- compare that first populates it. No migration path exists for a
        -- database predating a given column -- see the CLAUDE.md gotcha.
        CREATE TABLE IF NOT EXISTS bdot10k_unmatched (
            LOKALNYID VARCHAR,
            geom GEOMETRY,
            cell_x INTEGER,
            cell_y INTEGER,
            computed_at TIMESTAMP WITH TIME ZONE,
            -- Not a display attribute: the other half of BDOT10k's composite
            -- record key (PRZESTRZENNAZW, LOKALNYID). LOKALNYID alone is not
            -- unique, so POST /report cannot identify a building without it.
            -- See compare::columns::classification_columns.
            PRZESTRZENNAZW VARCHAR,
            funkcja_szczegolowa VARCHAR,
            funkcja_ogolna VARCHAR,
            liczba_kondygnacji SMALLINT,
            KATEGORIAISTNIENIA VARCHAR,
            NAZWA VARCHAR,
            FSBUD VARCHAR,
            INFORMACJADODATKOWA VARCHAR,
            KODKST TINYINT,
            ZRODLODANYCHGEOMETRYCZNYCH VARCHAR
        );
        CREATE TABLE IF NOT EXISTS egib_unmatched (
            id_budynku VARCHAR,
            geom GEOMETRY,
            cell_x INTEGER,
            cell_y INTEGER,
            computed_at TIMESTAMP WITH TIME ZONE,
            rodzaj_kod VARCHAR,
            kondygnacje_nadziemne INTEGER,
            kondygnacje_podziemne INTEGER,
            rodzaj VARCHAR
        );
        CREATE TABLE IF NOT EXISTS prg_unmatched (
            geom GEOMETRY,
            lokalny_id VARCHAR,
            numer_porzadkowy VARCHAR,
            ulica VARCHAR,
            miejscowosc VARCHAR,
            kod_pocztowy VARCHAR,
            teryt_miejscowosc VARCHAR,
            wazny_od_lub_data_nadania DATE,
            teryt_gmina VARCHAR,
            gmina VARCHAR,
            cell_x INTEGER,
            cell_y INTEGER,
            computed_at TIMESTAMP WITH TIME ZONE
        );

        -- The TERC catalogue: every wojewodztwo (2-digit code), powiat (4) and
        -- gmina (7; types 1-3 only, the ones PRG uses) with its name. Replaced wholesale by every PRG load
        -- from the TERC map that load fetched, and empty until the first one.
        -- Join PRG to it by the 2-/4-digit prefix of teryt_gmina. See
        -- import/teryt.rs.
        CREATE TABLE IF NOT EXISTS teryt_units (
            code VARCHAR,
            level VARCHAR,
            name VARCHAR
        );

        -- Curated PRG -> OSM street-name expansions, applied by
        -- server/package.rs when building addr:street. A row with a NULL
        -- teryt_simc_code is a global rule; a non-NULL one scopes the rule to
        -- that settlement and takes priority. Populated from
        -- mappings/street_names_mappings.csv; an empty table is a valid state
        -- and simply means names are served exactly as PRG publishes them.
        CREATE TABLE IF NOT EXISTS street_name_mappings (
            teryt_simc_code VARCHAR,
            prg_street_name VARCHAR,
            osm_street_name VARCHAR
        );

        -- Curated BDOT10k/EGIB classification -> OSM building tag mappings,
        -- applied at serve time by server/package.rs. An empty table is a
        -- valid state and degrades to a plain `building=yes`. tier is 1 for
        -- the source's primary key column, 2 for BDOT10k's fallback KŚT
        -- category (EGIB has no tier 2). key is stored lower(trim(...)).
        -- min_levels/max_levels/max_neighbours are inclusive constraints,
        -- NULL meaning unconstrained; tags is ';'-separated k=v pairs. See
        -- docs/superpowers/specs/2026-08-03-building-type-mappings-design.md.
        CREATE TABLE IF NOT EXISTS bdot10k_building_types (
            tier INTEGER,
            key VARCHAR,
            min_levels INTEGER,
            max_levels INTEGER,
            max_neighbours INTEGER,
            tags VARCHAR
        );
        CREATE TABLE IF NOT EXISTS egib_building_types (
            tier INTEGER,
            key VARCHAR,
            min_levels INTEGER,
            max_levels INTEGER,
            max_neighbours INTEGER,
            tags VARCHAR
        );

        -- Dirty-cell queue drained by the match_refresh job. Duplicates allowed
        -- (deduped on drain). source is 'bdot10k'|'egib'|'prg'; an OSM building
        -- edit enqueues bdot10k+egib, an OSM address edit enqueues prg, and a
        -- street_name_mappings reload enqueues prg for the addresses whose
        -- resolved street name changed (the mapping is a match input, not just
        -- a serve-time lookup -- see compare::rule's rule B).
        -- The cells a producer enqueues are the exact reach of the match
        -- rule's OSM read, not a neighbourhood margin: an edited object's
        -- bbox cells, widened by update::dirty_cells::layer_buffer_deg (0 for
        -- buildings, OSM_MATCH_BUFFER_DEG for addresses). See that function
        -- for why, and note it must widen if either rule's OSM read ever does.
        -- cell_z is informational only: every producer writes CHANGE_CELL_ZOOM
        -- and the drain neither selects nor filters on it (recompute_cell_in_txn
        -- hardcodes CHANGE_CELL_ZOOM). If CHANGE_CELL_ZOOM ever changes, queue
        -- rows written at the old zoom are silently reinterpreted at the new
        -- one — drain the queue before changing it, then `queue reconcile`.
        CREATE TABLE IF NOT EXISTS match_dirty_cells (
            source VARCHAR,
            cell_z INTEGER,
            cell_x INTEGER,
            cell_y INTEGER,
            enqueued_at TIMESTAMP WITH TIME ZONE
        );

        -- Tiles whose rendered bytes are stale, drained by jobs::tile_refresh.
        --
        -- Deliberately NOT keyed like match_dirty_cells: no `source` (a tile
        -- is source-agnostic -- it renders all three) and no `cell_z` (always
        -- CHANGE_CELL_ZOOM, since that is the granularity every producer
        -- already computes). One row per changed cell; the expansion into the
        -- tiles that render it is server::tile_dirty::tiles_for_cell's job,
        -- which keeps the queue an eleventh the size a pre-expanded one would
        -- be and keeps the ring/parent asymmetry in one place.
        CREATE TABLE IF NOT EXISTS tile_dirty_cells (
            cell_x INTEGER,
            cell_y INTEGER,
            enqueued_at TIMESTAMP WITH TIME ZONE
        );

        -- Denominator for the low-zoom completeness ratio: how many government
        -- objects a z14 cell holds *in total*, against which <source>_unmatched
        -- is the numerator. Written by exactly the two paths that write the
        -- unmatched rows themselves and always in the same transaction as them
        -- (compare::totals) -- see that module for why the ratio needs its own
        -- table at all rather than being counted at serve time.
        --
        -- source is 'bdot10k'|'egib'|'prg', spelled identically to
        -- match_dirty_cells.source. A cell with government objects but nothing
        -- unmatched has a row here and none in <source>_unmatched: that pair is
        -- what lets /tiles tell a fully-imported area apart from one with no
        -- government data at all, which the unmatched tables alone cannot
        -- express -- both are simply absent from them.
        CREATE TABLE IF NOT EXISTS cell_totals (
            source VARCHAR,
            cell_x INTEGER,
            cell_y INTEGER,
            total INTEGER
        );

        -- User-submitted reports that a government object should not be
        -- proposed for import (bad source data, or OSM already maps it in a way
        -- compare::rule cannot see). An active row vetoes its object out of
        -- <source>_unmatched via compare::rule::reported_sql -- the same shape
        -- as the osm_former_buildings suppression veto, and like it the object
        -- stays in cell_totals (it is comparable; someone has decided how to
        -- handle it), unlike a BDOT10K_EKSPLOATOWANY_FILTER-excluded row.
        --
        -- This is the ONLY table in this database holding data that cannot be
        -- reconstructed from an external source. Everything else is derived
        -- from OSM/PRG/BDOT10k/EGIB or the mapping CSVs and a lost database is
        -- a re-import away; these are not. `reports export`/`reports import`
        -- is the backup path.
        --
        -- source is 'bdot10k'|'egib'|'prg', always read off DatasetSpec.name
        -- and never typed as a literal, spelled identically to
        -- match_dirty_cells.source. record_key holds the key column values in
        -- DatasetSpec.key_columns order -- BDOT10k's key is the composite
        -- (PRZESTRZENNAZW, LOKALNYID), which is why this is a list and not a
        -- single column.
        --
        -- No UNIQUE constraint, deliberately: duplicate reports on one object
        -- are harmless *because* the veto is phrased NOT EXISTS rather than a
        -- LEFT JOIN. Rewriting it as a join would make a duplicate report
        -- duplicate rows in <source>_unmatched -- the exact fan-out failure
        -- documented for street_name_mappings.
        --
        -- `signature` is DatasetSpec::content_signature_sql at report time; a
        -- report lapses (status='expired') once it no longer matches the live
        -- record, which is what makes a corrected record importable again.
        -- See reports::reconcile_source.
        --
        -- Nothing identifying the submitter is stored -- no address, no hash of
        -- one, no user agent. Cleanup after an abusive burst is time-scoped
        -- (`reports revoke --since`), not actor-scoped, by design.
        CREATE TABLE IF NOT EXISTS object_reports (
            report_id BIGINT,
            source VARCHAR,
            record_key VARCHAR[],
            signature VARCHAR,
            reported_at TIMESTAMP WITH TIME ZONE,
            cell_x INTEGER,
            cell_y INTEGER,
            status VARCHAR,
            resolved_at TIMESTAMP WITH TIME ZONE
        );

        ",
    )
    .context("Failed to create schema")?;

    create_serving_indexes(conn);

    Ok(())
}

/// `min_node_capacity` every RTREE index in this database is created with.
///
/// DuckDB's default is 50 (40% of the 128-entry node), and it makes a delete
/// of *spatially clustered* rows pathological. When a delete leaves a node
/// below the minimum, spatial dissolves it: every remaining entry is orphaned
/// and re-inserted from the root (`RTree::LeafDelete`/`BranchDelete`). A split
/// leaves ~64 entries a leaf, so modifying a region's rows -- new entries in,
/// old entries out, which is what a dataset refresh and every drained cell do
/// -- halves each leaf to ~32, under the minimum, and the whole region's
/// leaves dissolve into each other again and again. Scattered rows never
/// trigger it, which is why a uniform benchmark does not see it.
///
/// Measured on the national `egib_buildings` (17.6M rows, two indexes, both
/// built by `CREATE INDEX`), capped like production:
///
/// | | default (50) | 16 |
/// |---|---|---|
/// | COMMIT of 699,946 clustered rows, deleted and re-inserted | 132.5-145.8 s | 4.5 s |
/// | `update egib`, real 08-15 -> 11-09 delta (2.54M rows): apply | 27m 58s | 27.6 s |
///
/// **Never 0.** A split only rebalances a side that fell below the minimum,
/// so at 0 it can leave one node still full and the insert retries without
/// end -- the same test did not finish in ten minutes. The minimum must stay
/// positive, and at most half the node (spatial rejects more).
///
/// The option is part of the index, so it is fixed when the index is built:
/// [`reindex`] is how an existing database picks it up.
pub const RTREE_MIN_NODE_CAPACITY: u32 = 16;

/// Every RTREE index in the schema, as `(index, table, column)`.
///
/// The one home for the set: the loaders build their table's entries through
/// [`create_rtree_indexes`], `create_schema` the serving tables', and
/// [`reindex`] walks all of it. A new index goes here, not into a loader.
pub const RTREE_INDEXES: &[(&str, &str, &str)] = &[
    ("bdot10k_buildings_geom_idx", "bdot10k_buildings", "geom"),
    (
        "bdot10k_buildings_centroid_idx",
        "bdot10k_buildings",
        "centroid",
    ),
    ("egib_buildings_geom_idx", "egib_buildings", "geom"),
    ("egib_buildings_centroid_idx", "egib_buildings", "centroid"),
    ("prg_addresses_geom_idx", "prg_addresses", "geom"),
    ("osm_buildings_geom_idx", "osm_buildings", "geom"),
    ("osm_addresses_geom_idx", "osm_addresses", "geom"),
    (
        "osm_former_buildings_geom_idx",
        "osm_former_buildings",
        "geom",
    ),
    ("bdot10k_unmatched_geom_idx", "bdot10k_unmatched", "geom"),
    ("egib_unmatched_geom_idx", "egib_unmatched", "geom"),
    ("prg_unmatched_geom_idx", "prg_unmatched", "geom"),
];

/// The `CREATE INDEX` statement for one RTREE index -- the only place the
/// options are spelled, so no index can be built without
/// [`RTREE_MIN_NODE_CAPACITY`].
fn rtree_index_sql(name: &str, table: &str, column: &str, if_not_exists: bool) -> String {
    let guard = if if_not_exists { "IF NOT EXISTS " } else { "" };
    format!(
        "CREATE INDEX {guard}{name} ON {table} USING RTREE ({column}) \
         WITH (min_node_capacity = {RTREE_MIN_NODE_CAPACITY});"
    )
}

/// Build every index [`RTREE_INDEXES`] declares for `table`, which must exist
/// and must not have them yet. Called by each loader once its table is full,
/// so the index is bulk-built rather than grown row by row.
///
/// Must run on the connection `init_db` returned, never a pooled one -- see
/// [`bind_rtree_indexes`].
pub fn create_rtree_indexes(conn: &Connection, table: &str) -> Result<()> {
    let declared: Vec<_> = RTREE_INDEXES
        .iter()
        .filter(|(_, t, _)| *t == table)
        .collect();
    anyhow::ensure!(
        !declared.is_empty(),
        "no RTREE index is declared for table {table}"
    );
    for (name, table, column) in declared {
        conn.execute_batch(&rtree_index_sql(name, table, column, false))
            .with_context(|| format!("Failed to create spatial index {name}"))?;
    }
    Ok(())
}

/// Drop and rebuild every RTREE index (`db reindex`).
///
/// Two reasons to run it, both on a database that already exists:
///
/// - **To apply [`RTREE_MIN_NODE_CAPACITY`]**, which an index only takes when
///   it is built. Nothing else changes an existing index: `import` rebuilds one
///   source's, and `db copy` replays each index's stored DDL, old options
///   included.
/// - **To undo growth.** An index that has been written to for weeks is
///   larger than a freshly built one (production held 5.1 GiB of index memory
///   against ~2.4 GiB for the same indexes just built), and DuckDB never
///   evicts index memory (`docs/duckdb_index_memory.md`).
///
/// It walks [`RTREE_INDEXES`] rather than the catalog, so a run interrupted
/// between a `DROP` and its `CREATE` is repaired by running it again. A table
/// that does not exist yet (a source never imported) is skipped. Building is
/// a bulk load -- seconds per index on the national tables -- and it happens
/// on the CLI's own connection, which is what [`bind_rtree_indexes`] needs.
///
/// Changes no row, so nothing a tile renders moves.
pub fn reindex(conn: &Connection) -> Result<()> {
    for (name, table, column) in RTREE_INDEXES {
        crate::shutdown::check_requested()?;
        let exists: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM duckdb_tables()
             WHERE database_name = current_database() AND schema_name = 'main'
               AND table_name = ?",
            duckdb::params![table],
            |row| row.get(0),
        )?;
        if !exists {
            tracing::info!(index = name, table, "Skipped index: table does not exist");
            continue;
        }
        let t = std::time::Instant::now();
        conn.execute_batch(&format!("DROP INDEX IF EXISTS {name};"))
            .with_context(|| format!("Failed to drop index {name}"))?;
        conn.execute_batch(&rtree_index_sql(name, table, column, false))
            .with_context(|| {
                format!("Failed to rebuild index {name}; run `db reindex` again to restore it")
            })?;
        tracing::info!(
            index = name,
            elapsed_s = t.elapsed().as_secs_f64(),
            "Rebuilt index"
        );
    }
    conn.execute_batch("CHECKPOINT")
        .context("Failed to checkpoint after rebuilding indexes")?;
    Ok(())
}

/// Names of the RTREE indexes that were not built with
/// [`RTREE_MIN_NODE_CAPACITY`] -- a database from before it existed, until
/// `db reindex` runs. Read off the DDL DuckDB stores for each index.
fn rtree_indexes_with_other_capacity(conn: &Connection) -> duckdb::Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT index_name FROM duckdb_indexes()
         WHERE database_name = current_database() AND sql ILIKE '%USING RTREE%'
           AND NOT regexp_matches(sql, ?)
         ORDER BY index_name",
    )?;
    let pattern = format!(r"min_node_capacity\s*=\s*{RTREE_MIN_NODE_CAPACITY}\b");
    stmt.query_map(duckdb::params![pattern], |row| row.get::<_, String>(0))?
        .collect()
}

/// Says so, once per start, when the database still carries indexes built
/// with another `min_node_capacity`. Not an error: they answer queries
/// correctly, they are only slow to delete from.
fn warn_about_outdated_rtree_indexes(conn: &Connection) {
    match rtree_indexes_with_other_capacity(conn) {
        Ok(names) if !names.is_empty() => tracing::warn!(
            indexes = %names.join(", "),
            "RTREE indexes built without min_node_capacity = {RTREE_MIN_NODE_CAPACITY}: \
             dataset refreshes and the match drain delete from them slowly. \
             Stop the service and run `db reindex` to rebuild them (seconds each)"
        ),
        Ok(_) => {}
        Err(e) => tracing::debug!(error = %e, "could not inspect RTREE index options"),
    }
}

/// Read-path indexes for `/tiles` and `/package`, which scan the serving tables
/// on a bbox predicate. Both callers must phrase that predicate as
/// `ST_Intersects(geom, <constant>)` for these to be used at all: an RTREE index
/// scan only fires against a constant argument, so the `geom && bbox.geom` form
/// (bbox joined in as a one-row CTE) plans as a full sequential scan even with
/// the index present. Measured on the Poland dataset the paired index+predicate
/// is 3-60x on `/tiles`, and the cost is one-sided: per-cell recompute churn
/// measured 1.89ms unindexed vs 1.91ms indexed, with no read degradation after
/// 15k cell rewrites. See `docs/followups_precomputed_unmatched_serving.md`.
///
/// **Warns instead of failing, deliberately.** `CREATE INDEX` forces a DuckDB
/// checkpoint, and a database that cannot checkpoint therefore turns index
/// creation into a fatal error. That happened for real on the Poland database
/// (`docs/duckdb_checkpoint_failure.md`): with these statements inside the
/// schema batch, `create_schema` failed and the server would not boot at all —
/// converting "queries are slower than they could be" into "the service is
/// down". Serving unindexed is strictly better than not serving, so a failure
/// here is logged and startup continues.
fn create_serving_indexes(conn: &Connection) {
    for (name, table, column) in RTREE_INDEXES
        .iter()
        .filter(|(_, table, _)| table.ends_with("_unmatched"))
    {
        let sql = rtree_index_sql(name, table, column, true);
        if let Err(e) = conn.execute_batch(&sql) {
            tracing::warn!(
                index = name,
                table = table,
                error = %e,
                "could not create serving-table index; /tiles and /package will \
                 fall back to sequential scans. This is usually a database that \
                 cannot checkpoint -- see docs/duckdb_checkpoint_failure.md"
            );
        }
    }
}

/// `1, 2, 3` for an `IN (...)` list. The ids are integers, so interpolating
/// them is safe, and it keeps a 1,000-id list out of the parameter count.
pub(crate) fn id_list_sql(ids: &[i64]) -> String {
    ids.iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// `(?::BIGINT, ?::VARCHAR), (?::BIGINT, ?::VARCHAR)` for `rows` rows of the
/// given column types: the placeholder text of a multi-row `VALUES` list,
/// which is how a set of rows goes into one INSERT statement instead of one
/// statement per row (`docs/duckdb_per_statement_insert_memory.md`). Every
/// placeholder is cast, so a column that is NULL in every row of a chunk
/// still has a type.
pub(crate) fn values_sql(rows: usize, types: &[&str]) -> String {
    let row = format!(
        "({})",
        types
            .iter()
            .map(|t| format!("?::{t}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    vec![row; rows].join(", ")
}

/// How text `EXPLAIN` labels an RTREE index scan, for the tests that assert a
/// query still reaches its index. DuckDB 2.0's renderer prints the full name
/// in each box's header; 1.5 printed `RTREE_INDEX_SCAN` and truncated it in a
/// wide plan, which is why those tests used to match the prefix `RTREE_IN`.
/// `EXPLAIN (FORMAT JSON)` still says `RTREE_INDEX_SCAN` and also names the
/// index, so prefer it where the index name matters.
#[cfg(test)]
pub(crate) const EXPLAIN_RTREE_SCAN: &str = "Rtree Index Scan";

#[cfg(test)]
mod tests {
    use super::*;

    /// Guards the version stamp the `bundled-cmake` build depends on.
    ///
    /// DuckDB derives its version from `git describe` inside `duckdb-sources`,
    /// which cargo checks out without tags — so it falls back to the dummy
    /// `v0.0.1` unless the build passes `DUCKDB_EXPLICIT_VERSION`, which
    /// libduckdb-sys 1.20000.1+ derives from its own crate version. That version string *is* the extension
    /// repository path, so a wrong one makes every `INSTALL <extension>` 404 and
    /// hides locally installed extensions. Without this test that surfaces as
    /// several hundred unrelated-looking failures across the whole suite; with
    /// it, one named test says exactly what broke.
    ///
    /// Asserts the shape rather than an exact string so a routine version bump
    /// doesn't need a test edit — only the "no version at all" failure is pinned.
    #[test]
    fn duckdb_reports_a_real_version_so_extensions_resolve() -> Result<()> {
        // Uses the real init commands rather than an empty list: `INSTALL spatial`
        // is exactly what a bad version stamp breaks, and `create_schema` needs
        // the GEOMETRY type it provides.
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;
        let version: String = conn.query_row("SELECT version()", [], |row| row.get(0))?;

        assert_ne!(
            version, "v0.0.1",
            "DuckDB built with its dummy version: `git describe` failed in duckdb-sources \
             and no DUCKDB_EXPLICIT_VERSION reached the CMake build. Extension installs \
             will 404. libduckdb-sys 1.20000.1+ passes it itself; check the build \
             script's `bundled-cmake DuckDB version` warning."
        );
        let major_minor_patch = version.trim_start_matches('v');
        assert_eq!(
            major_minor_patch.split('.').count(),
            3,
            "unexpected DuckDB version shape: {version}"
        );
        Ok(())
    }

    #[test]
    fn test_init_db_creates_tables() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;

        // Verify all tables exist by querying them
        let tables = [
            "metadata",
            "osm_addresses",
            "osm_buildings",
            "osm_former_buildings",
            "package_exports",
            "job_run_log",
        ];
        for table in tables {
            let count: i64 =
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })?;
            assert_eq!(count, 0, "Table {table} should be empty initially");
        }

        Ok(())
    }

    /// Spatial accepts a minimum from 0 to half the node, and 0 hangs the
    /// first unbalanced split (see `RTREE_MIN_NODE_CAPACITY`). 128 is the
    /// extension's default node size, which no index here overrides.
    #[test]
    fn the_rtree_min_node_capacity_is_positive_and_at_most_half_a_node() {
        const { assert!(RTREE_MIN_NODE_CAPACITY >= 1 && RTREE_MIN_NODE_CAPACITY <= 128 / 2) };
    }

    fn spatial_db() -> Result<Connection> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        init_db(Path::new(":memory:"), &init_commands, None)
    }

    /// The indexes `create_schema` builds carry the shared option -- the check
    /// that would catch a `CREATE INDEX` spelled by hand somewhere again.
    #[test]
    fn the_serving_indexes_are_built_with_the_shared_min_node_capacity() -> Result<()> {
        let conn = spatial_db()?;
        assert_eq!(
            rtree_indexes_with_other_capacity(&conn)?,
            Vec::<String>::new()
        );
        Ok(())
    }

    /// A loader's indexes come from the same list, with the same option.
    #[test]
    fn create_rtree_indexes_builds_every_index_declared_for_the_table() -> Result<()> {
        let conn = spatial_db()?;
        conn.execute_batch(
            "CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY);",
        )?;
        create_rtree_indexes(&conn, "egib_buildings")?;

        let names: Vec<String> = conn
            .prepare(
                "SELECT index_name FROM duckdb_indexes()
                 WHERE table_name = 'egib_buildings' ORDER BY index_name",
            )?
            .query_map([], |row| row.get(0))?
            .collect::<duckdb::Result<_>>()?;
        assert_eq!(
            names,
            ["egib_buildings_centroid_idx", "egib_buildings_geom_idx"]
        );
        assert!(rtree_indexes_with_other_capacity(&conn)?.is_empty());

        assert!(
            create_rtree_indexes(&conn, "no_such_table").is_err(),
            "a table with no declared index is a caller's mistake, not a no-op"
        );
        Ok(())
    }

    /// `db reindex` on a database from before the option existed: the old
    /// index is reported, rebuilt with the option, still there and still
    /// usable -- and tables that were never imported are skipped, not an
    /// error.
    #[test]
    fn reindex_rebuilds_an_index_built_with_other_options() -> Result<()> {
        let conn = spatial_db()?;
        // Only prg_addresses of the source tables exists, as on a database
        // with one source imported. Its index is built the old way.
        conn.execute_batch(
            "CREATE TABLE prg_addresses AS
                 SELECT i::VARCHAR AS lokalny_id, ST_Point(19 + i / 1000.0, 52.0) AS geom
                 FROM range(500) t(i);
             CREATE INDEX prg_addresses_geom_idx ON prg_addresses USING RTREE (geom);",
        )?;
        assert_eq!(
            rtree_indexes_with_other_capacity(&conn)?,
            ["prg_addresses_geom_idx"]
        );

        reindex(&conn)?;

        assert!(rtree_indexes_with_other_capacity(&conn)?.is_empty());
        let indexed: Vec<String> = conn
            .prepare(
                "SELECT table_name FROM duckdb_indexes()
                 WHERE sql ILIKE '%USING RTREE%' ORDER BY table_name",
            )?
            .query_map([], |row| row.get(0))?
            .collect::<duckdb::Result<_>>()?;
        assert!(indexed.contains(&"prg_addresses".to_string()));
        assert!(
            !indexed
                .iter()
                .any(|t| t == "egib_buildings" || t == "bdot10k_buildings"),
            "a table that does not exist must be skipped: {indexed:?}"
        );
        let hits: i64 = conn.query_row(
            "SELECT COUNT(*) FROM prg_addresses
             WHERE ST_Intersects(geom, ST_MakeEnvelope(19.0995, 51.9, 19.2005, 52.1))",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(hits, 101, "the rebuilt index must answer correctly");
        Ok(())
    }

    /// A run that died between a DROP and its CREATE left no index behind;
    /// running it again must put it back, which is why `reindex` walks the
    /// declared list rather than the catalog.
    #[test]
    fn reindex_restores_an_index_that_is_missing() -> Result<()> {
        let conn = spatial_db()?;
        conn.execute_batch("DROP INDEX egib_unmatched_geom_idx;")?;

        reindex(&conn)?;

        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM duckdb_indexes() WHERE index_name = 'egib_unmatched_geom_idx'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(n, 1);
        Ok(())
    }

    /// The serving tables carry RTREE indexes, and /tiles + /package phrase
    /// their bbox filter so those indexes are actually usable. The index half
    /// is pinned here; `server::tiles::tests::mvt_bbox_filter_uses_the_rtree_index`
    /// pins the query half. Either one alone is a silent no-op.
    #[test]
    fn test_init_db_creates_serving_table_rtree_indexes() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;

        for table in ["bdot10k_unmatched", "egib_unmatched", "prg_unmatched"] {
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM duckdb_indexes()
                 WHERE table_name = ? AND sql ILIKE '%USING RTREE%'",
                duckdb::params![table],
                |row| row.get(0),
            )?;
            assert_eq!(n, 1, "{table} must have an RTREE index on geom");
        }

        Ok(())
    }

    #[test]
    fn test_init_db_is_idempotent() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;
        // Re-run schema creation — should not fail
        create_schema(&conn)?;
        Ok(())
    }

    /// duckdb-spatial#861's reproduction: one row appended to an
    /// RTREE-indexed table, then a close that skips the checkpoint, as any
    /// crash or kill does. Up to DuckDB v1.5.5 the reopened database failed
    /// the DELETE below with `INTERNAL Error: Operation requires a flat
    /// vector`, and in the server that left every later query on the table
    /// spinning (docs/duckdb_checkpoint_failure.md, 2026-10-31). Fixed by
    /// duckdb-spatial#862, first shipped with v1.5.6.
    #[test]
    fn rtree_indexes_survive_an_unclean_shutdown() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("unclean.duckdb");
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        {
            let conn = init_db(&path, &init_commands, None)?;
            conn.execute_batch(
                "CREATE INDEX osm_addresses_geom_idx ON osm_addresses USING RTREE (geom);
                 PRAGMA disable_checkpoint_on_shutdown;
                 INSERT INTO osm_addresses (osm_id, osm_type, geom)
                     VALUES (1, 'node', ST_Point(21, 52));",
            )?;
        }
        let wal = std::fs::metadata(dir.path().join("unclean.duckdb.wal"))?;
        assert!(wal.len() > 0, "the close must leave the insert in the WAL");

        let conn = init_db(&path, &init_commands, None)?;
        conn.execute_batch("DELETE FROM osm_addresses WHERE osm_type = 'node' AND osm_id IN (2)")?;
        let hits: i64 = conn.query_row(
            "SELECT count(*) FROM osm_addresses
             WHERE ST_Intersects(geom, ST_MakeEnvelope(20, 51, 22, 53))",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(hits, 1, "the replayed row must be found through the index");
        Ok(())
    }

    /// `bind_rtree_indexes` must reach every RTREE index of a reopened
    /// database, whose indexes all start unbound, including those on empty
    /// tables. If spatial renames or drops `pragma_rtree_index_info`, the
    /// function only warns, so this test is what notices.
    /// Workaround for <https://github.com/duckdb/duckdb-spatial/issues/915>:
    /// delete this test together with the function.
    #[test]
    fn every_rtree_index_is_bound_on_the_base_connection() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("bind.duckdb");
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        {
            let conn = init_db(&path, &init_commands, None)?;
            conn.execute_batch(
                "INSERT INTO osm_buildings (osm_id, osm_type, geom)
                     VALUES (1, 'way', ST_Point(21, 52));
                 CREATE INDEX osm_buildings_geom_idx ON osm_buildings USING RTREE (geom);
                 CREATE INDEX osm_addresses_geom_idx ON osm_addresses USING RTREE (geom);",
            )?;
        }

        let conn = init_db(&path, &init_commands, None)?;
        let mut expected: Vec<String> = conn
            .prepare("SELECT index_name FROM duckdb_indexes() WHERE sql ILIKE '%USING RTREE%'")?
            .query_map([], |row| row.get(0))?
            .collect::<duckdb::Result<_>>()?;
        expected.sort();
        assert_eq!(
            expected.len(),
            5,
            "three serving indexes plus the two above"
        );
        assert_eq!(bind_rtree_indexes(&conn), expected);
        Ok(())
    }

    #[test]
    fn test_package_exports_column_types_round_trip() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;

        conn.execute(
            "INSERT INTO package_exports (exported_at, area, datasets, address_count, building_count)
             VALUES (now(), ST_Point(21.0, 52.0), ['prg', 'bdot10k'], 3, 5)",
            [],
        )?;

        let (geojson, datasets_json, address_count, building_count): (String, String, i32, i32) =
            conn.query_row(
                "SELECT ST_AsGeoJSON(area), to_json(datasets), address_count, building_count
                 FROM package_exports",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;

        assert!(geojson.contains("\"Point\""));
        assert_eq!(datasets_json, r#"["prg","bdot10k"]"#);
        assert_eq!(address_count, 3);
        assert_eq!(building_count, 5);

        Ok(())
    }

    #[test]
    fn test_init_db_creates_changeset_tables() -> Result<()> {
        let init_commands = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;

        for table in ["dataset_refreshes", "dataset_change_areas"] {
            let count: i64 =
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })?;
            assert_eq!(count, 0, "Table {table} should be empty initially");
        }
        Ok(())
    }

    #[test]
    fn test_changeset_tables_round_trip() -> Result<()> {
        let init_commands = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
        ];
        let conn = init_db(Path::new(":memory:"), &init_commands, None)?;

        conn.execute_batch(
            "INSERT INTO dataset_refreshes
                 VALUES (1, 'bdot10k', now(), now(), 'etag-abc', 10, 20, 5);
             INSERT INTO dataset_change_areas
                 VALUES (1, 'bdot10k', 14, 9147, 5411, 10, 20, 5, now());",
        )?;

        let (source, added, modified, removed): (String, i32, i32, i32) = conn.query_row(
            "SELECT source, added, modified, removed FROM dataset_refreshes",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(
            (source.as_str(), added, modified, removed),
            ("bdot10k", 10, 20, 5)
        );

        let (z, x, y): (i32, i32, i32) = conn.query_row(
            "SELECT cell_z, cell_x, cell_y FROM dataset_change_areas",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!((z, x, y), (14, 9147, 5411));

        Ok(())
    }

    #[test]
    fn test_init_db_creates_serving_and_queue_tables() -> Result<()> {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        let conn = init_db(Path::new(":memory:"), &init, None)?;
        for table in [
            "bdot10k_unmatched",
            "egib_unmatched",
            "prg_unmatched",
            "match_dirty_cells",
        ] {
            let n: i64 =
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?;
            assert_eq!(n, 0, "table {table} should exist and be empty");
        }
        // prg_unmatched must carry the serving + cell columns.
        conn.execute_batch(
            "INSERT INTO prg_unmatched
             (geom, lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
              teryt_miejscowosc, cell_x, cell_y, computed_at)
             VALUES (ST_Point(21.0,52.0),'id1','5','Main','Town','00-001','0918123',
                     9147, 5411, now());",
        )?;
        let (hn, cx): (String, i32) = conn.query_row(
            "SELECT numer_porzadkowy, cell_x FROM prg_unmatched",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert_eq!((hn.as_str(), cx), ("5", 9147));
        Ok(())
    }
}
