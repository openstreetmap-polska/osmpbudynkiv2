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

use crate::tile_math::{CHANGE_CELL_ZOOM, cell_x_sql, cell_y_sql};

pub const MAPPINGS_TABLE: &str = "street_name_mappings";

const STAGING_TABLE: &str = "street_name_mappings__staging";

/// The two LEFT JOINs that resolve `{alias}.ulica` through
/// `street_name_mappings`: the settlement-scoped row (`loc`) wins over the
/// global row (`gl`). Emitted as bare join text so a caller can splice it into
/// whatever FROM clause it already has; pair it with
/// [`resolved_street_expr_sql`], which names the result.
///
/// **This cannot fan out -- but only because the loader says so.** At most one
/// `loc` row and one `gl` row can match, because [`validate_and_swap`] rejects
/// duplicate `(lower(prg_street_name), teryt_simc_code)` keys before the swap.
/// The table itself carries no UNIQUE constraint (see `db::create_schema`), so
/// a hand-INSERTed duplicate duplicates rows in every consumer.
pub fn resolved_street_join_sql(alias: &str) -> String {
    format!(
        "LEFT JOIN street_name_mappings loc
                ON lower(trim(loc.prg_street_name)) = lower(trim({alias}.ulica))
               AND loc.teryt_simc_code = {alias}.teryt_miejscowosc
         LEFT JOIN street_name_mappings gl
                ON lower(trim(gl.prg_street_name)) = lower(trim({alias}.ulica))
               AND gl.teryt_simc_code IS NULL"
    )
}

/// The COALESCE naming [`resolved_street_join_sql`]'s output: settlement row,
/// then global row, then the raw PRG name -- so an empty mapping table
/// degrades to serving PRG names verbatim rather than erroring.
///
/// Deliberately *unnormalized*: consumers normalize differently (`/package`
/// serves it raw, `/tiles` wraps it in `NULLIF(trim(...), '')`, the match rule
/// lowercases it), so normalization belongs at the call site.
pub fn resolved_street_expr_sql(alias: &str) -> String {
    format!("COALESCE(loc.osm_street_name, gl.osm_street_name, {alias}.ulica)")
}

/// Outcome of one successful load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadStats {
    pub rows_loaded: usize,
    /// Rows whose `prg_street_name` appears nowhere in `prg_addresses.ulica`.
    /// Not an error -- the database may simply not have PRG imported yet --
    /// but a large count against a populated database means the file has
    /// drifted from what PRG currently publishes.
    pub rows_absent_from_prg: i64,
    /// z14 `match_dirty_cells` rows enqueued for the drain because this load
    /// changed the (settlement, PRG name) -> OSM name triple for at least one
    /// PRG address's street. See `enqueue_mapping_delta_cells`.
    pub cells_enqueued: i64,
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

/// Enqueue the z14 `match_dirty_cells` rows a mapping swap invalidates.
///
/// **This table is a match input, not merely a serving-time lookup.** The
/// PRG<->OSM address match rule has a branch comparing PRG's street name
/// *resolved through `street_name_mappings`* against OSM's `addr:street` at up
/// to 150 m, so a mapping edit can flip an address between matched and
/// unmatched, and the drain needs to know which cells to recompute.
///
/// Must be called before the caller deletes `{MAPPINGS_TABLE}`'s contents:
/// this reads the live table's pre-swap rows, which are half the symmetric
/// difference against `{STAGING_TABLE}` and gone one statement later. (Same
/// read-before-write ordering as `update::changeset::insert_change_areas`.)
///
/// No drain race, though it looks like there is one: DuckDB's `now()` is
/// transaction-start-scoped, so the rows this inserts can be stamped
/// *earlier* than a concurrent `compare::drain::drain_batch`'s
/// `batch_start`, whose paired delete is `enqueued_at <= batch_start`. That
/// delete only ever sees rows in its own snapshot, though: either the drain
/// starts after this swap commits (it sees the new mapping *and* this queue
/// row -> correct recompute, then delete), or before it (it sees neither ->
/// the row survives untouched to the next tick). There is no interleaving
/// where the delete fires without the corresponding recompute having run.
fn enqueue_mapping_delta_cells(conn: &Connection) -> Result<i64> {
    // `prg_addresses` is created by `import prg`, not by `db::create_schema`,
    // so it is legitimately absent when `import street-mappings` runs first
    // on a fresh database. Probe the catalog explicitly instead of
    // swallowing a query error the way `rows_absent_from_prg` above does
    // (`.unwrap_or(0)`) -- a swallowed error here would silently skip the
    // enqueue forever instead of just once on a fresh database.
    let prg_addresses_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'prg_addresses'",
            [],
            |r| r.get(0),
        )
        .context("Failed to probe for prg_addresses")?;
    if prg_addresses_exists == 0 {
        return Ok(0);
    }

    let z = CHANGE_CELL_ZOOM;
    let cx = cell_x_sql("p.geom");
    let cy = cell_y_sql("p.geom");
    // `execute` (not `execute_batch`), same reason as `enqueue_all`'s
    // comment: the return value must be the number of rows *this* INSERT
    // added, not the queue's total depth for 'prg' afterwards.
    let n = conn
        .execute(
            &format!(
                "INSERT INTO match_dirty_cells (source, cell_z, cell_x, cell_y, enqueued_at)
                 WITH triples_live AS (
                     SELECT teryt_simc_code, lower(prg_street_name) AS n, osm_street_name
                     FROM {MAPPINGS_TABLE}
                 ),
                 triples_new AS (
                     SELECT teryt_simc_code, lower(prg_street_name) AS n, osm_street_name
                     FROM {STAGING_TABLE}
                 ),
                 -- Each EXCEPT below must be parenthesized: EXCEPT and UNION
                 -- share precedence and are left-associative, so the
                 -- unparenthesized form silently parses as
                 -- ((A EXCEPT B) UNION B) EXCEPT A, not the symmetric
                 -- difference this needs. Correctness, not style.
                 changed AS (
                     (SELECT * FROM triples_live EXCEPT SELECT * FROM triples_new)
                     UNION
                     (SELECT * FROM triples_new EXCEPT SELECT * FROM triples_live)
                 )
                 -- EXCEPT compares NULLs as equal (verified in DuckDB 1.5.5).
                 -- That is what keeps an unchanged *global* row (NULL
                 -- teryt_simc_code) out of `changed`; rewriting this as
                 -- `NOT EXISTS ... AND teryt_simc_code = ...` would put every
                 -- global row into the delta on every reload.
                 --
                 -- The full triple is compared above but only the name is
                 -- projected below -- deliberately over-broad: a
                 -- settlement-scoped edit dirties that name's addresses
                 -- nationally. It's cheap, and it removes a whole class of
                 -- \"which addresses could this row have applied to\"
                 -- reasoning.
                 SELECT DISTINCT 'prg', {z}, {cx}, {cy}, now()
                 FROM prg_addresses p
                 WHERE p.geom IS NOT NULL
                   AND lower(trim(p.ulica)) IN (SELECT DISTINCT n FROM changed)"
            ),
            [],
        )
        .context("Failed to enqueue dirty cells for changed mappings")?;
    Ok(n as i64)
}

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
    // The mapping is a match input, not a serving-time lookup: the PRG<->OSM
    // address match rule compares PRG's street name resolved through
    // street_name_mappings against OSM's addr:street, so a mapping edit can
    // flip an address between matched and unmatched (see CLAUDE.md's
    // street-name gotcha). enqueue_mapping_delta_cells must run first, before
    // the DELETE below discards the live rows it needs to diff against.
    // Chained via `.context().and_then(...)` (converting the duckdb::Error
    // into anyhow::Error first, so the calls' error types line up) so the
    // enqueue and the bump are folded into the same fallible value the swap
    // already is and land or roll back with it below -- this is the one home
    // for the load (both call sites go through here), so doing either step
    // anywhere else would risk a second, divergent copy.
    let swap = enqueue_mapping_delta_cells(conn)
        .and_then(|cells_enqueued| {
            conn.execute_batch(&format!(
                "DELETE FROM {MAPPINGS_TABLE};
             INSERT INTO {MAPPINGS_TABLE} (teryt_simc_code, prg_street_name, osm_street_name)
             SELECT teryt_simc_code, prg_street_name, osm_street_name FROM {STAGING_TABLE};"
            ))
            .context("Failed to apply mapping swap")
            .map(|()| cells_enqueued)
        })
        // Even with the per-cell enqueue above, an undrained cell keeps
        // serving the old match decision alongside the new serve-time
        // addr:street until the drain catches up, and the addresses_all
        // legend layer plus z5-z13 tiles are epoch-only (see
        // `serving_version`'s module doc) -- so this bump is still required
        // on top of the enqueue, not replaced by it.
        .and_then(|cells_enqueued| {
            crate::serving_version::bump_serving_epoch(conn).map(|()| cells_enqueued)
        });
    let cells_enqueued = match swap {
        Ok(cells_enqueued) => {
            conn.execute_batch("COMMIT")
                .context("Failed to commit mapping swap")?;
            cells_enqueued
        }
        Err(e) => {
            if let Err(rb) = conn.execute_batch("ROLLBACK") {
                warn!(error = %rb, "Failed to roll back mapping swap");
            }
            return Err(e).context("Failed to replace mapping table contents");
        }
    };

    info!(
        rows = rows_loaded,
        absent_from_prg = rows_absent_from_prg,
        cells_enqueued,
        "Loaded street name mappings"
    );
    Ok(LoadStats {
        rows_loaded: rows_loaded as usize,
        rows_absent_from_prg,
        cells_enqueued,
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

    /// The builders exist so the settlement-beats-global-beats-raw priority
    /// has one home. This pins that order end to end in DuckDB rather than
    /// asserting on the SQL text, which would pass for a chain that resolves
    /// in the wrong order.
    #[test]
    fn resolved_street_builders_produce_a_working_priority_chain() {
        let conn = setup_db();
        conn.execute_batch(
            "CREATE TABLE a (ulica VARCHAR, teryt_miejscowosc VARCHAR);
             INSERT INTO a VALUES
                 ('gen. Kruka', '0956069'),   -- has both a settlement and a global row
                 ('gen. Kruka', '0000001'),   -- only the global row applies
                 ('Polna',      '0956069');   -- no mapping at all
             INSERT INTO street_name_mappings VALUES
                 ('0956069', 'gen. Kruka', 'Generała Michała Kruka'),
                 (NULL,      'gen. Kruka', 'Generała Kruka');",
        )
        .unwrap();
        let sql = format!(
            "SELECT {} FROM a {} ORDER BY 1",
            resolved_street_expr_sql("a"),
            resolved_street_join_sql("a"),
        );
        let mut stmt = conn.prepare(&sql).unwrap();
        let got: Vec<String> = stmt
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            got,
            vec![
                "Generała Kruka".to_string(),         // global row
                "Generała Michała Kruka".to_string(), // settlement row wins
                "Polna".to_string(),                  // falls through to the raw name
            ],
            "settlement row must beat the global row, which must beat the raw PRG name"
        );
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
            "CREATE TABLE prg_addresses (lokalny_id VARCHAR, ulica VARCHAR, geom GEOMETRY);
             INSERT INTO prg_addresses VALUES ('1', 'gen. Kruka', ST_Point(21.0, 52.0));",
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
        assert_eq!(
            stats.cells_enqueued, 0,
            "no prg_addresses table means nothing to enqueue"
        );
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

    /// Creates `prg_addresses` with the shape `enqueue_mapping_delta_cells`
    /// reads (`ulica` + `geom`), so the tests below can seed real addresses.
    fn setup_db_with_prg_addresses(rows_sql: &str) -> duckdb::Connection {
        let conn = setup_db();
        conn.execute_batch(&format!(
            "CREATE TABLE prg_addresses (lokalny_id VARCHAR, ulica VARCHAR, geom GEOMETRY);
             {rows_sql}"
        ))
        .unwrap();
        conn
    }

    fn dirty_prg_cell_count(conn: &duckdb::Connection) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM match_dirty_cells WHERE source = 'prg'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// Scale claim from the design doc: a reload whose contents are byte-for-
    /// byte identical to what's already loaded must enqueue nothing, even
    /// though matching addresses exist and would enqueue on a real change.
    #[test]
    fn reload_with_identical_contents_enqueues_no_cells() {
        let conn = setup_db_with_prg_addresses(
            "INSERT INTO prg_addresses VALUES ('1', 'gen. Kruka', ST_Point(21.0, 52.0));",
        );
        let f = write_csv(",gen. Kruka,Generała Kruka\n");
        load_from_path(&conn, f.path()).unwrap();

        let f2 = write_csv(",gen. Kruka,Generała Kruka\n");
        let stats = load_from_path(&conn, f2.path()).unwrap();
        assert_eq!(
            stats.cells_enqueued, 0,
            "identical mapping contents must not enqueue anything"
        );
    }

    #[test]
    fn changing_a_mapping_target_enqueues_the_cells_of_addresses_with_that_prg_name() {
        let conn = setup_db_with_prg_addresses(
            "INSERT INTO prg_addresses VALUES ('1', 'gen. Kruka', ST_Point(21.0, 52.0));",
        );
        let first = write_csv(",gen. Kruka,Generała Kruka\n");
        load_from_path(&conn, first.path()).unwrap();

        let second = write_csv(",gen. Kruka,Generała Michała Kruka\n");
        let stats = load_from_path(&conn, second.path()).unwrap();
        assert_eq!(
            stats.cells_enqueued, 1,
            "the one cell holding the address whose mapped target changed"
        );
    }

    #[test]
    fn adding_a_mapping_enqueues_cells() {
        let conn = setup_db_with_prg_addresses(
            "INSERT INTO prg_addresses VALUES
                 ('1', 'Polna', ST_Point(21.0, 52.0)),
                 ('2', 'gen. Kruka', ST_Point(19.0, 50.0));",
        );
        let first = write_csv(",Polna,Polna Ulica\n");
        load_from_path(&conn, first.path()).unwrap();

        let second = write_csv(",Polna,Polna Ulica\n,gen. Kruka,Generała Kruka\n");
        let stats = load_from_path(&conn, second.path()).unwrap();
        assert_eq!(
            stats.cells_enqueued, 1,
            "only the newly-added row's addresses, not the unchanged Polna row's"
        );
    }

    #[test]
    fn removing_a_mapping_enqueues_cells() {
        let conn = setup_db_with_prg_addresses(
            "INSERT INTO prg_addresses VALUES
                 ('1', 'Polna', ST_Point(21.0, 52.0)),
                 ('2', 'gen. Kruka', ST_Point(19.0, 50.0));",
        );
        let first = write_csv(",Polna,Polna Ulica\n,gen. Kruka,Generała Kruka\n");
        load_from_path(&conn, first.path()).unwrap();

        let second = write_csv(",Polna,Polna Ulica\n");
        let stats = load_from_path(&conn, second.path()).unwrap();
        assert_eq!(
            stats.cells_enqueued, 1,
            "the removed row's addresses, not the still-present Polna row's"
        );
    }

    /// Pins that the *full triple* is compared, not just the (name, target)
    /// pair: the (name, target) pair below is byte-identical across the two
    /// loads, only `teryt_simc_code` changes, and that alone must enqueue.
    #[test]
    fn changing_only_the_simc_scope_enqueues() {
        let conn = setup_db_with_prg_addresses(
            "INSERT INTO prg_addresses VALUES ('1', 'gen. Kruka', ST_Point(21.0, 52.0));",
        );
        let first = write_csv(",gen. Kruka,Generała Kruka\n");
        load_from_path(&conn, first.path()).unwrap();

        let second = write_csv("0956069,gen. Kruka,Generała Kruka\n");
        let stats = load_from_path(&conn, second.path()).unwrap();
        assert_eq!(
            stats.cells_enqueued, 1,
            "the (name, target) pair is unchanged but the scope differs, so the full \
             triple must still be flagged as changed"
        );
    }

    /// Pins `EXCEPT`'s NULL-as-equal behaviour: a reload where only a
    /// *settlement*-scoped row changes must not also enqueue the untouched
    /// global row's (NULL `teryt_simc_code`) addresses.
    #[test]
    fn an_unchanged_global_row_is_not_in_the_symmetric_difference() {
        let conn = setup_db_with_prg_addresses(
            "INSERT INTO prg_addresses VALUES
                 ('1', 'gen. Kruka', ST_Point(21.0, 52.0)),
                 ('2', 'Polna', ST_Point(19.0, 50.0));",
        );
        let first = write_csv(",gen. Kruka,Generała Kruka\n0956069,Polna,Polna Ulica\n");
        load_from_path(&conn, first.path()).unwrap();

        // Only the settlement-scoped Polna row's target changes.
        let second = write_csv(",gen. Kruka,Generała Kruka\n0956069,Polna,Nowa Polna\n");
        let stats = load_from_path(&conn, second.path()).unwrap();
        assert_eq!(
            stats.cells_enqueued, 1,
            "the untouched global row (NULL teryt_simc_code) must not appear in the delta"
        );
    }

    #[test]
    fn addresses_with_other_street_names_are_not_enqueued() {
        let conn = setup_db_with_prg_addresses(
            "INSERT INTO prg_addresses VALUES
                 ('1', 'gen. Kruka', ST_Point(21.0, 52.0)),
                 ('2', 'Polna', ST_Point(19.0, 50.0));",
        );
        let first = write_csv(",gen. Kruka,Generała Kruka\n");
        load_from_path(&conn, first.path()).unwrap();

        let second = write_csv(",gen. Kruka,Generała Michała Kruka\n");
        let stats = load_from_path(&conn, second.path()).unwrap();
        assert_eq!(
            stats.cells_enqueued, 1,
            "only gen. Kruka's cell, not Polna's -- Polna's mapping never changed"
        );
    }

    #[test]
    fn enqueue_matches_ulica_case_and_whitespace_insensitively() {
        let conn = setup_db_with_prg_addresses(
            "INSERT INTO prg_addresses VALUES ('1', '  GEN. KRUKA  ', ST_Point(21.0, 52.0));",
        );
        let first = write_csv(",gen. Kruka,Generała Kruka\n");
        load_from_path(&conn, first.path()).unwrap();

        let second = write_csv(",gen. Kruka,Generała Michała Kruka\n");
        let stats = load_from_path(&conn, second.path()).unwrap();
        assert_eq!(
            stats.cells_enqueued, 1,
            "case/whitespace differences between ulica and prg_street_name must still match"
        );
    }

    #[test]
    fn enqueue_skips_addresses_with_null_ulica_and_null_geom() {
        let conn = setup_db_with_prg_addresses(
            "INSERT INTO prg_addresses VALUES
                 ('1', NULL, ST_Point(21.0, 52.0)),
                 ('2', 'gen. Kruka', NULL),
                 ('3', 'gen. Kruka', ST_Point(19.0, 50.0));",
        );
        let first = write_csv(",gen. Kruka,Generała Kruka\n");
        load_from_path(&conn, first.path()).unwrap();

        let second = write_csv(",gen. Kruka,Generała Michała Kruka\n");
        let stats = load_from_path(&conn, second.path()).unwrap();
        assert_eq!(
            stats.cells_enqueued, 1,
            "the NULL-ulica and NULL-geom rows must be skipped, not crash the query \
             or inflate the count"
        );
    }

    /// Mirror of `failed_load_does_not_bump_the_serving_epoch`: a rejected
    /// load must not enqueue dirty cells for a mapping swap that never
    /// happened.
    #[test]
    fn failed_load_enqueues_no_cells() {
        let conn = setup_db_with_prg_addresses(
            "INSERT INTO prg_addresses VALUES ('1', 'gen. Kruka', ST_Point(21.0, 52.0));",
        );
        let bad = write_csv(",B,Bbb\n,b,Bbb2\n");
        load_from_path(&conn, bad.path()).unwrap_err();
        assert_eq!(dirty_prg_cell_count(&conn), 0);
    }
}
