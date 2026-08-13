//! Loading the curated PRG -> OSM street-name mapping file into DuckDB.
//!
//! Parsing goes through DuckDB's `read_csv` rather than a Rust CSV crate: it
//! already handles the `""`-escaped quoting the file uses for nicknames, and
//! it keeps the dependency list unchanged.
//!
//! Validation is all-or-nothing. A bad file leaves the previous table intact
//! rather than half-replacing it, because serving a slightly stale mapping is
//! strictly better than serving a partial one.

use std::path::Path;

use anyhow::{Context, Result, bail};
use duckdb::Connection;
use tracing::{info, warn};

#[allow(dead_code)]
pub const MAPPINGS_TABLE: &str = "street_name_mappings";

#[allow(dead_code)]
const STAGING_TABLE: &str = "street_name_mappings__staging";

/// Outcome of one successful load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadStats {
    pub rows_loaded: usize,
    /// Rows whose `prg_street_name` appears nowhere in `prg_addresses.ulica`.
    /// Not an error -- the database may simply not have PRG imported yet --
    /// but a large count against a populated database means the file has
    /// drifted from what PRG currently publishes.
    pub rows_absent_from_prg: i64,
}

/// Replace the contents of `street_name_mappings` with the rows in `path`.
pub fn load_from_path(conn: &Connection, path: &Path) -> Result<LoadStats> {
    let path_str = path
        .to_str()
        .with_context(|| format!("mapping path is not valid UTF-8: {path:?}"))?;

    conn.execute_batch(&format!("DROP TABLE IF EXISTS {STAGING_TABLE}"))
        .context("Failed to drop stale mapping staging table")?;

    // read_csv accepts the path as a bound parameter, so no escaping needed.
    conn.execute(
        &format!(
            "CREATE TABLE {STAGING_TABLE} AS
             SELECT NULLIF(trim(teryt_simc_code), '') AS teryt_simc_code,
                    trim(prg_street_name) AS prg_street_name,
                    trim(osm_street_name) AS osm_street_name
             FROM read_csv(?, header = true, all_varchar = true)"
        ),
        duckdb::params![path_str],
    )
    .with_context(|| format!("Failed to read mapping CSV at {path_str}"))?;

    let result = validate_and_swap(conn, path_str);
    let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {STAGING_TABLE}"));
    result
}

#[allow(dead_code)]
fn validate_and_swap(conn: &Connection, path_str: &str) -> Result<LoadStats> {
    let empty: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM {STAGING_TABLE}
                 WHERE prg_street_name IS NULL OR prg_street_name = ''
                    OR osm_street_name IS NULL OR osm_street_name = ''"
            ),
            [],
            |r| r.get(0),
        )
        .context("Failed to check for empty names")?;
    if empty > 0 {
        bail!("{path_str}: {empty} row(s) have an empty street name; refusing to load");
    }

    let dupes: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM (
                     SELECT 1 FROM {STAGING_TABLE}
                     GROUP BY lower(prg_street_name), teryt_simc_code
                     HAVING COUNT(*) > 1)"
            ),
            [],
            |r| r.get(0),
        )
        .context("Failed to check for duplicate keys")?;
    if dupes > 0 {
        bail!(
            "{path_str}: {dupes} duplicate (street name, settlement) key(s); refusing to load. \
             Lookup is case-insensitive, so two rows differing only in case collide."
        );
    }

    let untrimmed: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM {STAGING_TABLE} s
                 JOIN read_csv(?, header = true, all_varchar = true) r
                   ON r.prg_street_name = s.prg_street_name
                 WHERE r.prg_street_name <> trim(r.prg_street_name)
                    OR r.osm_street_name <> trim(r.osm_street_name)"
            ),
            duckdb::params![path_str],
            |r| r.get(0),
        )
        .unwrap_or(0);
    if untrimmed > 0 {
        warn!(
            file = path_str,
            rows = untrimmed,
            "Mapping rows had surrounding whitespace; trimmed on load"
        );
    }

    let rows_loaded: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {STAGING_TABLE}"), [], |r| {
            r.get(0)
        })
        .context("Failed to count staged mapping rows")?;

    // `prg_addresses` is created by the PRG import, not by create_schema, so
    // it is legitimately absent on a database where `import street-mappings`
    // ran first. Treat the query failing as "no staleness signal available"
    // rather than as a load failure.
    let rows_absent_from_prg: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM {STAGING_TABLE} s
                 WHERE NOT EXISTS (
                     SELECT 1 FROM prg_addresses p
                     WHERE lower(trim(p.ulica)) = lower(s.prg_street_name))"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    conn.execute_batch("BEGIN TRANSACTION")
        .context("Failed to begin mapping swap")?;
    // `/tiles` and `/package` apply this mapping at serve time with no dirty
    // cell and no recompute (see CLAUDE.md's street-name gotcha and
    // `serving_version`'s module doc), so a landed swap is exactly the "no
    // per-cell version tracks this" case the global epoch exists for.
    // Chained via `.context().and_then(...)` (converting the duckdb::Error
    // into anyhow::Error first, so the two calls' error types line up) so
    // the bump is folded into the same fallible value the swap already is
    // and lands or rolls back with it below -- this is the one home for the
    // load (both call sites go through here), so bumping anywhere else would
    // risk a second, divergent copy.
    let swap = conn
        .execute_batch(&format!(
            "DELETE FROM {MAPPINGS_TABLE};
         INSERT INTO {MAPPINGS_TABLE} (teryt_simc_code, prg_street_name, osm_street_name)
         SELECT teryt_simc_code, prg_street_name, osm_street_name FROM {STAGING_TABLE};"
        ))
        .context("Failed to apply mapping swap")
        .and_then(|()| crate::serving_version::bump_serving_epoch(conn));
    match swap {
        Ok(()) => conn
            .execute_batch("COMMIT")
            .context("Failed to commit mapping swap")?,
        Err(e) => {
            if let Err(rb) = conn.execute_batch("ROLLBACK") {
                warn!(error = %rb, "Failed to roll back mapping swap");
            }
            return Err(e).context("Failed to replace mapping table contents");
        }
    }

    info!(
        rows = rows_loaded,
        absent_from_prg = rows_absent_from_prg,
        "Loaded street name mappings"
    );
    Ok(LoadStats {
        rows_loaded: rows_loaded as usize,
        rows_absent_from_prg,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::io::Write;
    use std::path::Path;

    fn setup_db() -> duckdb::Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    fn write_csv(body: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "teryt_simc_code,prg_street_name,osm_street_name\n{body}").unwrap();
        f.flush().unwrap();
        f
    }

    fn loaded(conn: &duckdb::Connection) -> Vec<(Option<String>, String, String)> {
        let mut stmt = conn
            .prepare(
                "SELECT teryt_simc_code, prg_street_name, osm_street_name
                 FROM street_name_mappings ORDER BY prg_street_name, teryt_simc_code",
            )
            .unwrap();
        let rows = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap();
        rows.map(|r| r.unwrap()).collect()
    }

    #[test]
    fn loads_global_and_settlement_rows() {
        let conn = setup_db();
        let f =
            write_csv(",gen. Kruka,Generała Kruka\n0956069,gen. Kruka,Generała Michała Kruka\n");
        let stats = load_from_path(&conn, f.path()).unwrap();
        assert_eq!(stats.rows_loaded, 2);
        let rows = loaded(&conn);
        assert_eq!(rows.len(), 2);
        // Empty SIMC must land as NULL, so the `IS NULL` join in package.rs matches it.
        assert!(rows.iter().any(|r| r.0.is_none()));
        assert!(rows.iter().any(|r| r.0.as_deref() == Some("0956069")));
    }

    #[test]
    fn quoted_fields_survive_parsing() {
        let conn = setup_db();
        let f = write_csv(",gen. Fieldorfa,\"Generała Emila Fieldorfa \"\"Nila\"\"\"\n");
        load_from_path(&conn, f.path()).unwrap();
        assert_eq!(loaded(&conn)[0].2, "Generała Emila Fieldorfa \"Nila\"");
    }

    #[test]
    fn reload_replaces_previous_contents() {
        let conn = setup_db();
        let first = write_csv(",A,Aaa\n");
        load_from_path(&conn, first.path()).unwrap();
        let second = write_csv(",B,Bbb\n");
        let stats = load_from_path(&conn, second.path()).unwrap();
        assert_eq!(stats.rows_loaded, 1);
        let rows = loaded(&conn);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, "B");
    }

    #[test]
    fn duplicate_key_rejects_the_load_and_leaves_the_table_untouched() {
        let conn = setup_db();
        let good = write_csv(",A,Aaa\n");
        load_from_path(&conn, good.path()).unwrap();
        // Same key differing only by case is still a duplicate.
        let bad = write_csv(",B,Bbb\n,b,Bbb2\n");
        let err = load_from_path(&conn, bad.path()).unwrap_err();
        assert!(
            format!("{err:#}").contains("duplicate"),
            "unexpected error: {err:#}"
        );
        let rows = loaded(&conn);
        assert_eq!(
            rows.len(),
            1,
            "previous contents must survive a failed load"
        );
        assert_eq!(rows[0].1, "A");
    }

    #[test]
    fn empty_name_rejects_the_load() {
        let conn = setup_db();
        let bad = write_csv(",,Aaa\n");
        let err = load_from_path(&conn, bad.path()).unwrap_err();
        assert!(format!("{err:#}").contains("empty"), "unexpected: {err:#}");
    }

    #[test]
    fn values_are_trimmed_on_load() {
        let conn = setup_db();
        let f = write_csv(",\"  gen. Kruka \",\" Generała Kruka \"\n");
        load_from_path(&conn, f.path()).unwrap();
        assert_eq!(loaded(&conn)[0].1, "gen. Kruka");
        assert_eq!(loaded(&conn)[0].2, "Generała Kruka");
    }

    /// `prg_addresses` is created by the PRG import, NOT by `create_schema`,
    /// so the test has to build it and the loader has to cope without it.
    #[test]
    fn counts_rows_whose_prg_name_is_absent_from_prg_addresses() {
        let conn = setup_db();
        conn.execute_batch(
            "CREATE TABLE prg_addresses (lokalny_id VARCHAR, ulica VARCHAR);
             INSERT INTO prg_addresses VALUES ('1', 'gen. Kruka');",
        )
        .unwrap();
        let f = write_csv(",gen. Kruka,Generała Kruka\n,gone. Street,Whatever Street\n");
        let stats = load_from_path(&conn, f.path()).unwrap();
        assert_eq!(stats.rows_loaded, 2);
        assert_eq!(stats.rows_absent_from_prg, 1);
    }

    /// Loading into a database that has never had PRG imported must succeed --
    /// `import street-mappings` may legitimately run first.
    #[test]
    fn load_succeeds_when_prg_addresses_does_not_exist() {
        let conn = setup_db();
        let f = write_csv(",gen. Kruka,Generała Kruka\n");
        let stats = load_from_path(&conn, f.path()).unwrap();
        assert_eq!(stats.rows_loaded, 1);
        assert_eq!(stats.rows_absent_from_prg, 0);
    }

    /// This mapping changes what `/tiles` renders (`addr:street`) with no
    /// dirty cell and no recompute, so a landed load must bump the global
    /// serving epoch -- see `serving_version`'s module doc.
    #[test]
    fn successful_load_bumps_the_serving_epoch() {
        let conn = setup_db();
        assert_eq!(
            crate::serving_version::read_serving_epoch(&conn).unwrap(),
            0
        );
        let f = write_csv(",gen. Kruka,Generała Kruka\n");
        load_from_path(&conn, f.path()).unwrap();
        assert_eq!(
            crate::serving_version::read_serving_epoch(&conn).unwrap(),
            1
        );
    }

    /// Mirror of `duplicate_key_rejects_the_load_and_leaves_the_table_untouched`:
    /// a rejected load must not claim the serving state moved when nothing
    /// was actually swapped in.
    #[test]
    fn failed_load_does_not_bump_the_serving_epoch() {
        let conn = setup_db();
        let bad = write_csv(",B,Bbb\n,b,Bbb2\n");
        load_from_path(&conn, bad.path()).unwrap_err();
        assert_eq!(
            crate::serving_version::read_serving_epoch(&conn).unwrap(),
            0
        );
    }
}
