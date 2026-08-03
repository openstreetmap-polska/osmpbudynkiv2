//! Loading the curated BDOT10k/EGIB building-classification mapping files
//! into DuckDB.
//!
//! Mirrors `street_names::load_from_path`: parse into a `__staging` table via
//! `read_csv`, validate all-or-nothing, then `DELETE` + `INSERT` in one
//! transaction so a bad file leaves the previous mapping intact. See
//! `docs/superpowers/specs/2026-08-03-building-type-mappings-design.md` for
//! the design this implements.

use std::path::Path;

use anyhow::{Context, Result, bail};
use duckdb::Connection;
use tracing::info;

/// Static description of one source's mapping: which table it loads into,
/// which live table/columns it is checked for drift against, and which tier-1
/// keys may carry a `max_neighbours` constraint (the adjacency class -- see
/// the design doc's Adjacency section).
pub struct BuildingTypeSource {
    pub name: &'static str,
    pub mapping_table: &'static str,
    pub staging_table: &'static str,
    /// Live table this source's classification columns are checked against
    /// for drift. May legitimately not exist yet (`import building-types` can
    /// run before the first `import bdot10k`/`import egib`).
    pub source_table: &'static str,
    pub tier1_source_column: &'static str,
    /// `None` for EGIB, which has no second cascade tier.
    pub tier2_source_column: Option<&'static str>,
    /// Tier-1 keys allowed to carry a `max_neighbours` constraint -- the
    /// class the serve-time adjacency CTE actually restricts its neighbour
    /// read to (see `compare::columns` and the design doc's Adjacency
    /// section). A `max_neighbours` value on any other key would be
    /// referencing a count the serve query never computes.
    pub adjacency_keys: &'static [&'static str],
}

pub const BDOT10K: BuildingTypeSource = BuildingTypeSource {
    name: "bdot10k",
    mapping_table: "bdot10k_building_types",
    staging_table: "bdot10k_building_types__staging",
    source_table: "bdot10k_buildings",
    tier1_source_column: "PRZEWAZAJACAFUNKCJABUDYNKU",
    tier2_source_column: Some("FUNKCJAOGOLNABUDYNKU"),
    adjacency_keys: &["budynek jednorodzinny"],
};

pub const EGIB: BuildingTypeSource = BuildingTypeSource {
    name: "egib",
    mapping_table: "egib_building_types",
    staging_table: "egib_building_types__staging",
    // rodzaj_kod does not exist on egib_buildings until the source loader is
    // taught to precompute it (design doc step 5); until then every drift
    // query against it fails and the `.unwrap_or` fallbacks below report 0,
    // the same "no signal available yet" treatment a missing table gets.
    source_table: "egib_buildings",
    tier1_source_column: "rodzaj_kod",
    tier2_source_column: None,
    adjacency_keys: &["m"],
};

/// Outcome of one successful load.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BuildingTypeStats {
    pub rows_loaded: usize,
    /// Distinct (tier, key) pairs in the file matching no row in the source
    /// live table -- probably a typo or a removed schema value.
    pub keys_absent_from_source: i64,
    /// Distinct source key values (tier 1, or tier 2 when tier 1 and tier 2
    /// both miss) that no file row covers.
    pub source_keys_uncovered: i64,
    /// Row count in the live source table behind `source_keys_uncovered`.
    pub source_rows_uncovered: i64,
}

/// Parse a `;`-separated `k=v` tags string into ordered pairs. Every mapping
/// row must have at least one pair and must include a `building` key -- see
/// "Every entry must include a `building` key" in
/// `docs/building_type_mappings.md`: `osm_buildings` is populated solely from
/// the presence of that tag, so a row without it would never leave the
/// package.
pub fn parse_tags(s: &str) -> std::result::Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    for part in s.split(';') {
        let part = part.trim();
        if part.is_empty() {
            return Err(format!("empty tag segment in '{s}'"));
        }
        let (k, v) = part
            .split_once('=')
            .ok_or_else(|| format!("tag '{part}' in '{s}' is not in k=v form"))?;
        let (k, v) = (k.trim(), v.trim());
        if k.is_empty() || v.is_empty() {
            return Err(format!("tag '{part}' in '{s}' has an empty key or value"));
        }
        out.push((k.to_string(), v.to_string()));
    }
    if out.is_empty() {
        return Err("tags value is empty".to_string());
    }
    Ok(out)
}

/// Render tags back to the `;`-separated `k=v` form, in whatever order
/// `parse_tags` returned them -- used by tests that round-trip a value.
#[cfg(test)]
fn render_tags(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(";")
}

/// Replace the contents of `source.mapping_table` with the rows in `path`.
pub fn load_from_path(
    conn: &Connection,
    source: &BuildingTypeSource,
    path: &Path,
) -> Result<BuildingTypeStats> {
    let path_str = path
        .to_str()
        .with_context(|| format!("mapping path is not valid UTF-8: {path:?}"))?;
    let staging = source.staging_table;

    conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging}"))
        .context("Failed to drop stale building-type staging table")?;

    conn.execute(
        &format!(
            "CREATE TABLE {staging} AS SELECT * FROM read_csv(?, header = true, all_varchar = true)"
        ),
        duckdb::params![path_str],
    )
    .with_context(|| format!("Failed to read mapping CSV at {path_str}"))?;

    let result = validate_and_swap(conn, source, path_str);
    let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging}"));
    result
}

/// Column names actually present in `table`, in file order -- used both to
/// assert the raw column count (6 or 7) and to check the required names are
/// among them. Read from `information_schema` rather than trusting the CSV
/// sniffer, since a stray unquoted comma has silently collapsed a file to one
/// column before (see `docs/building_type_mappings.md`).
fn staged_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT column_name FROM information_schema.columns
         WHERE table_name = ? ORDER BY ordinal_position",
    )?;
    let rows = stmt.query_map(duckdb::params![table], |r| r.get::<_, String>(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn validate_and_swap(
    conn: &Connection,
    source: &BuildingTypeSource,
    path_str: &str,
) -> Result<BuildingTypeStats> {
    let staging = source.staging_table;

    let cols = staged_columns(conn, staging)?;
    if cols.len() != 6 && cols.len() != 7 {
        bail!(
            "{path_str}: expected 6 or 7 columns, got {} ({:?}); refusing to load. \
             A raw string edit inserting a stray comma into an unquoted field has \
             silently collapsed this file to one column before -- check the header.",
            cols.len(),
            cols
        );
    }
    let required = [
        "tier",
        "key",
        "min_levels",
        "max_levels",
        "max_neighbours",
        "tags",
    ];
    for req in required {
        if !cols.iter().any(|c| c == req) {
            bail!("{path_str}: missing required column '{req}'; got {cols:?}");
        }
    }
    if cols.len() == 7 && !cols.iter().any(|c| c == "note") {
        bail!("{path_str}: 7 columns present but none is 'note'; got {cols:?}");
    }

    // Normalize into typed columns. A non-numeric tier/min_levels/max_levels/
    // max_neighbours value surfaces here as a plain DuckDB cast error --
    // acceptable, since the row-level validation below needs typed values
    // anyway and a cast failure already names the offending column.
    let clean = format!("{staging}__clean");
    conn.execute_batch(&format!("DROP TABLE IF EXISTS {clean}"))?;
    conn.execute_batch(&format!(
        "CREATE TABLE {clean} AS
         SELECT CAST(tier AS INTEGER) AS tier,
                lower(trim(key)) AS key,
                CAST(NULLIF(trim(min_levels), '') AS INTEGER) AS min_levels,
                CAST(NULLIF(trim(max_levels), '') AS INTEGER) AS max_levels,
                CAST(NULLIF(trim(max_neighbours), '') AS INTEGER) AS max_neighbours,
                trim(tags) AS tags
         FROM {staging}"
    ))
    .with_context(|| format!("{path_str}: failed to normalize staged columns"))?;

    let rows_loaded: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {clean}"), [], |r| r.get(0))
        .context("Failed to count staged mapping rows")?;

    // Row-level checks that need Rust logic (tags parsing) or the
    // adjacency-key allowlist, which isn't expressible as a single SQL
    // predicate against a &[&str] constant.
    {
        let mut stmt = conn.prepare(&format!(
            "SELECT tier, key, min_levels, max_levels, max_neighbours, tags FROM {clean}"
        ))?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, Option<i64>>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<i64>>(2)?,
                r.get::<_, Option<i64>>(3)?,
                r.get::<_, Option<i64>>(4)?,
                r.get::<_, String>(5)?,
            ))
        })?;
        for row in rows {
            let (tier, key, min_levels, max_levels, max_neighbours, tags) = row?;
            let max_tier = if source.tier2_source_column.is_some() {
                2
            } else {
                1
            };
            match tier {
                Some(t) if (1..=max_tier).contains(&t) => {}
                other => bail!(
                    "{path_str}: row for key '{key}' has tier {other:?}, must be 1..={max_tier} for {}",
                    source.name
                ),
            }
            if key.is_empty() {
                bail!("{path_str}: a row has an empty key; refusing to load");
            }
            let pairs = parse_tags(&tags)
                .map_err(|e| anyhow::anyhow!("{path_str}: row for key '{key}': {e}"))?;
            if !pairs.iter().any(|(k, _)| k == "building") {
                bail!(
                    "{path_str}: row for key '{key}' has tags '{tags}' with no 'building' \
                     key; every row must assert one (see CLAUDE.md/building_type_mappings.md)"
                );
            }
            if let (Some(min), Some(max)) = (min_levels, max_levels)
                && min > max
            {
                bail!("{path_str}: row for key '{key}' has min_levels {min} > max_levels {max}");
            }
            if let Some(n) = max_neighbours {
                if n < 0 {
                    bail!("{path_str}: row for key '{key}' has negative max_neighbours {n}");
                }
                if tier != Some(1) || !source.adjacency_keys.contains(&key.as_str()) {
                    bail!(
                        "{path_str}: row for key '{key}' carries max_neighbours but is not in \
                         the code-level adjacency set {:?} for {}; the serve query never \
                         computes a neighbour count for this key",
                        source.adjacency_keys,
                        source.name
                    );
                }
            }
        }
    }

    // Duplicate (tier, key, specificity): two rows that would tie in the
    // serve query's `ORDER BY tier, specificity DESC LIMIT 1` precedence rule.
    let dupes: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM (
                 SELECT tier, key,
                        (min_levels IS NOT NULL)::INT
                      + (max_levels IS NOT NULL)::INT
                      + (max_neighbours IS NOT NULL)::INT AS specificity
                 FROM {clean}
                 GROUP BY tier, key, specificity
                 HAVING COUNT(*) > 1
             )"
        ),
        [],
        |r| r.get(0),
    )?;
    if dupes > 0 {
        bail!(
            "{path_str}: {dupes} (tier, key) group(s) have two rows tying on specificity; \
             refusing to load. The serve query picks the most specific matching row and \
             cannot break a tie."
        );
    }

    // Drift, in both directions. Tolerates the source table (or, ahead of
    // step 5, egib_buildings.rodzaj_kod) not existing yet -- `import
    // building-types` may legitimately run before the first buildings import.
    let keys_absent_from_source: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM (SELECT DISTINCT tier, key FROM {clean}) f
                 WHERE (f.tier = 1 AND NOT EXISTS (
                            SELECT 1 FROM {src} WHERE lower(trim({t1})) = f.key))
                    OR (f.tier = 2 AND NOT EXISTS (
                            SELECT 1 FROM {src} WHERE lower(trim({t2})) = f.key))",
                src = source.source_table,
                t1 = source.tier1_source_column,
                t2 = source
                    .tier2_source_column
                    .unwrap_or(source.tier1_source_column),
            ),
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    let (source_keys_uncovered, source_rows_uncovered): (i64, i64) =
        match source.tier2_source_column {
            Some(t2) => conn
                .query_row(
                    &format!(
                        "WITH src AS (
                         SELECT lower(trim({t1})) AS t1, lower(trim({t2})) AS t2
                         FROM {src}
                     ), uncovered AS (
                         SELECT * FROM src
                         WHERE (t1 IS NULL OR t1 NOT IN (SELECT key FROM {clean} WHERE tier = 1))
                           AND (t2 IS NULL OR t2 NOT IN (SELECT key FROM {clean} WHERE tier = 2))
                     )
                     SELECT COUNT(DISTINCT t1), COUNT(*) FROM uncovered",
                        src = source.source_table,
                        t1 = source.tier1_source_column,
                    ),
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap_or((0, 0)),
            None => conn
                .query_row(
                    &format!(
                        "WITH src AS (SELECT lower(trim({t1})) AS t1 FROM {src}),
                          uncovered AS (
                              SELECT * FROM src
                              WHERE t1 IS NULL
                                 OR t1 NOT IN (SELECT key FROM {clean} WHERE tier = 1)
                          )
                     SELECT COUNT(DISTINCT t1), COUNT(*) FROM uncovered",
                        src = source.source_table,
                        t1 = source.tier1_source_column,
                    ),
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap_or((0, 0)),
        };

    conn.execute_batch("BEGIN TRANSACTION")
        .context("Failed to begin building-type mapping swap")?;
    let swap = conn.execute_batch(&format!(
        "DELETE FROM {dest};
         INSERT INTO {dest} (tier, key, min_levels, max_levels, max_neighbours, tags)
         SELECT tier, key, min_levels, max_levels, max_neighbours, tags FROM {clean};",
        dest = source.mapping_table
    ));
    match swap {
        Ok(()) => conn
            .execute_batch("COMMIT")
            .context("Failed to commit building-type mapping swap")?,
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e).context("Failed to replace building-type mapping table contents");
        }
    }
    let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {clean}"));

    info!(
        source = source.name,
        rows = rows_loaded,
        keys_absent_from_source,
        source_keys_uncovered,
        source_rows_uncovered,
        "Loaded building-type mappings"
    );
    Ok(BuildingTypeStats {
        rows_loaded: rows_loaded as usize,
        keys_absent_from_source,
        source_keys_uncovered,
        source_rows_uncovered,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::io::Write;
    use std::path::Path;

    fn setup_db() -> Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    fn write_csv(header: &str, body: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "{header}\n{body}").unwrap();
        f.flush().unwrap();
        f
    }

    const BDOT_HEADER: &str = "tier,key,min_levels,max_levels,max_neighbours,tags";
    const EGIB_HEADER: &str = "tier,key,min_levels,max_levels,max_neighbours,tags,note";

    fn mapping_rows(conn: &Connection, table: &str) -> Vec<(i64, String, String)> {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT tier, key, tags FROM {table} ORDER BY tier, key"
            ))
            .unwrap();
        let rows = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap();
        rows.map(|r| r.unwrap()).collect()
    }

    #[test]
    fn parse_tags_splits_multiple_pairs() {
        let pairs = parse_tags("building=silo;man_made=silo").unwrap();
        assert_eq!(
            pairs,
            vec![
                ("building".to_string(), "silo".to_string()),
                ("man_made".to_string(), "silo".to_string())
            ]
        );
        assert_eq!(render_tags(&pairs), "building=silo;man_made=silo");
    }

    #[test]
    fn parse_tags_rejects_missing_equals() {
        assert!(parse_tags("building").is_err());
    }

    #[test]
    fn loads_a_simple_bdot10k_file() {
        let conn = setup_db();
        let f = write_csv(
            BDOT_HEADER,
            "1,budynek gospodarczy,,,,building=outbuilding\n\
             2,budynki transportu i łączności,,,,building=garage\n",
        );
        let stats = load_from_path(&conn, &BDOT10K, f.path()).unwrap();
        assert_eq!(stats.rows_loaded, 2);
        let rows = mapping_rows(&conn, "bdot10k_building_types");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1, "budynek gospodarczy");
    }

    #[test]
    fn tolerates_the_optional_note_column() {
        let conn = setup_db();
        let f = write_csv(EGIB_HEADER, "1,m,,,,building=residential,some note\n");
        let stats = load_from_path(&conn, &EGIB, f.path()).unwrap();
        assert_eq!(stats.rows_loaded, 1);
        let rows = mapping_rows(&conn, "egib_building_types");
        assert_eq!(rows[0].1, "m");
    }

    #[test]
    fn reload_replaces_previous_contents() {
        let conn = setup_db();
        let first = write_csv(BDOT_HEADER, "1,a,,,,building=yes\n");
        load_from_path(&conn, &BDOT10K, first.path()).unwrap();
        let second = write_csv(BDOT_HEADER, "1,b,,,,building=yes\n");
        let stats = load_from_path(&conn, &BDOT10K, second.path()).unwrap();
        assert_eq!(stats.rows_loaded, 1);
        let rows = mapping_rows(&conn, "bdot10k_building_types");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, "b");
    }

    #[test]
    fn wrong_column_count_rejects_the_load() {
        let conn = setup_db();
        let f = write_csv("tier,key,tags", "1,a,building=yes\n");
        let err = load_from_path(&conn, &BDOT10K, f.path()).unwrap_err();
        assert!(
            format!("{err:#}").contains("6 or 7 columns"),
            "unexpected: {err:#}"
        );
    }

    #[test]
    fn missing_building_key_rejects_the_load() {
        let conn = setup_db();
        let f = write_csv(BDOT_HEADER, "1,a,,,,man_made=silo\n");
        let err = load_from_path(&conn, &BDOT10K, f.path()).unwrap_err();
        assert!(
            format!("{err:#}").contains("no 'building' key"),
            "unexpected: {err:#}"
        );
    }

    #[test]
    fn duplicate_specificity_rejects_the_load() {
        let conn = setup_db();
        let f = write_csv(
            BDOT_HEADER,
            "1,budynek jednorodzinny,,,,building=detached\n\
             1,budynek jednorodzinny,,,,building=house\n",
        );
        let err = load_from_path(&conn, &BDOT10K, f.path()).unwrap_err();
        assert!(
            format!("{err:#}").contains("tying on specificity"),
            "unexpected: {err:#}"
        );
    }

    #[test]
    fn differing_specificity_is_allowed_for_the_same_key() {
        let conn = setup_db();
        let f = write_csv(
            BDOT_HEADER,
            "1,budynek jednorodzinny,,,0,building=detached\n\
             1,budynek jednorodzinny,,,,building=house\n",
        );
        let stats = load_from_path(&conn, &BDOT10K, f.path()).unwrap();
        assert_eq!(stats.rows_loaded, 2);
    }

    #[test]
    fn min_levels_greater_than_max_levels_rejects_the_load() {
        let conn = setup_db();
        let f = write_csv(BDOT_HEADER, "1,a,5,3,,building=yes\n");
        let err = load_from_path(&conn, &BDOT10K, f.path()).unwrap_err();
        assert!(
            format!("{err:#}").contains("min_levels"),
            "unexpected: {err:#}"
        );
    }

    #[test]
    fn negative_max_neighbours_rejects_the_load() {
        let conn = setup_db();
        let f = write_csv(
            BDOT_HEADER,
            "1,budynek jednorodzinny,,,-1,building=detached\n",
        );
        let err = load_from_path(&conn, &BDOT10K, f.path()).unwrap_err();
        assert!(
            format!("{err:#}").contains("negative max_neighbours"),
            "unexpected: {err:#}"
        );
    }

    #[test]
    fn max_neighbours_on_a_non_adjacency_key_rejects_the_load() {
        let conn = setup_db();
        let f = write_csv(BDOT_HEADER, "1,garaż,,,0,building=garage\n");
        let err = load_from_path(&conn, &BDOT10K, f.path()).unwrap_err();
        assert!(
            format!("{err:#}").contains("adjacency set"),
            "unexpected: {err:#}"
        );
    }

    #[test]
    fn tier_2_rejected_for_egib_which_has_no_second_tier() {
        let conn = setup_db();
        let f = write_csv(EGIB_HEADER, "2,m,,,,building=residential,\n");
        let err = load_from_path(&conn, &EGIB, f.path()).unwrap_err();
        assert!(format!("{err:#}").contains("tier"), "unexpected: {err:#}");
    }

    #[test]
    fn drift_reports_keys_absent_from_source_and_uncovered_source_keys() {
        let conn = setup_db();
        conn.execute_batch(
            "CREATE TABLE bdot10k_buildings (
                 LOKALNYID VARCHAR, geom GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR);
             INSERT INTO bdot10k_buildings VALUES
                 ('a', NULL, 'budynek gospodarczy', 'budynki mieszkalne'),
                 ('b', NULL, 'garaż', 'budynki mieszkalne'),
                 ('c', NULL, 'nieznana funkcja', 'nieznana kategoria');",
        )
        .unwrap();
        // Covers 'budynek gospodarczy' (tier 1) and 'budynki mieszkalne' via
        // tier 2 as a fallback for the rows tier 1 doesn't cover ('garaż' has
        // no tier-1 row here, but tier 2 covers it; 'nieznana funkcja' has
        // neither, so it stays uncovered) -- also includes one file key
        // ('nie istnieje') absent from the source entirely.
        let f = write_csv(
            BDOT_HEADER,
            "1,budynek gospodarczy,,,,building=outbuilding\n\
             2,budynki mieszkalne,,,,building=residential\n\
             1,nie istnieje,,,,building=yes\n",
        );
        let stats = load_from_path(&conn, &BDOT10K, f.path()).unwrap();
        assert_eq!(stats.rows_loaded, 3);
        assert_eq!(
            stats.keys_absent_from_source, 1,
            "'nie istnieje' has no source row"
        );
        assert_eq!(
            stats.source_keys_uncovered, 1,
            "'nieznana funkcja' is covered by neither tier"
        );
        assert_eq!(stats.source_rows_uncovered, 1);
    }

    #[test]
    fn drift_queries_tolerate_a_missing_source_table() {
        let conn = setup_db();
        // No bdot10k_buildings table at all -- `import building-types` may
        // legitimately run before the first `import bdot10k`.
        let f = write_csv(
            BDOT_HEADER,
            "1,budynek gospodarczy,,,,building=outbuilding\n",
        );
        let stats = load_from_path(&conn, &BDOT10K, f.path()).unwrap();
        assert_eq!(stats.rows_loaded, 1);
        assert_eq!(stats.keys_absent_from_source, 0);
        assert_eq!(stats.source_keys_uncovered, 0);
        assert_eq!(stats.source_rows_uncovered, 0);
    }

    #[test]
    fn a_bad_load_leaves_the_previous_table_untouched() {
        let conn = setup_db();
        let good = write_csv(BDOT_HEADER, "1,a,,,,building=yes\n");
        load_from_path(&conn, &BDOT10K, good.path()).unwrap();
        let bad = write_csv(BDOT_HEADER, "1,b,,,,man_made=silo\n");
        assert!(load_from_path(&conn, &BDOT10K, bad.path()).is_err());
        let rows = mapping_rows(&conn, "bdot10k_building_types");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, "a");
    }

    #[test]
    fn key_is_lowercased_and_trimmed_on_load() {
        let conn = setup_db();
        let f = write_csv(
            BDOT_HEADER,
            "1, Budynek Gospodarczy ,,,,building=outbuilding\n",
        );
        load_from_path(&conn, &BDOT10K, f.path()).unwrap();
        let rows = mapping_rows(&conn, "bdot10k_building_types");
        assert_eq!(rows[0].1, "budynek gospodarczy");
    }
}
