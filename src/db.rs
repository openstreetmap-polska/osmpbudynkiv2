use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use duckdb::vtab::arrow::ArrowVTab;
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

    conn.register_table_function::<ArrowVTab>("arrow")
        .context("Failed to register arrow vtab")?;

    if let Some(kv) = kv {
        udf::register_udfs(&conn, kv)?;
    }

    for cmd in init_commands {
        conn.execute_batch(cmd)
            .with_context(|| format!("Failed to execute DuckDB init command: {cmd}"))?;
    }

    create_schema(&conn)?;

    Ok(conn)
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
            reason VARCHAR,
            note VARCHAR,
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
    for (name, table) in [
        ("bdot10k_unmatched_geom_idx", "bdot10k_unmatched"),
        ("egib_unmatched_geom_idx", "egib_unmatched"),
        ("prg_unmatched_geom_idx", "prg_unmatched"),
    ] {
        let sql = format!("CREATE INDEX IF NOT EXISTS {name} ON {table} USING RTREE (geom);");
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Guards the version stamp the `bundled-cmake` build depends on.
    ///
    /// DuckDB derives its version from `git describe` inside `duckdb-sources`,
    /// which cargo checks out without tags — so it falls back to the dummy
    /// `v0.0.1` unless `cmake/duckdb_version.cmake` forces `OVERRIDE_GIT_DESCRIBE`
    /// (wired up by `.cargo/config.toml`). That version string *is* the extension
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
             and OVERRIDE_GIT_DESCRIBE was not applied. Extension installs will 404. \
             Check that .cargo/config.toml's CMAKE_TOOLCHAIN_FILE reaches the build \
             (it is not covered by rerun-if-env-changed — `cargo clean -p libduckdb-sys` \
             after changing it)."
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
