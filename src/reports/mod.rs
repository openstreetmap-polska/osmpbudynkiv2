//! User-submitted reports that a government object should not be proposed for
//! import — "bad source data" or "OSM already maps this in a way the match rule
//! cannot see", the roadmap item at `docs/project_ideas.md:27`.
//!
//! **Where the veto actually lives is `compare::rule`, not here.** This module
//! owns the *table*: inserting a report, expiring one whose record has changed,
//! revoking one by hand. The predicate that removes a reported object from
//! `<source>_unmatched` is `compare::rule::reported_sql`, so it stays in the
//! one home the match rule already has and both compare paths inherit it.
//!
//! Three properties are load-bearing and none of them is obvious from the
//! schema:
//!
//! 1. **A report is stored only if its key resolves to a live record.** The
//!    insert is an `INSERT ... SELECT` *from the source table*, so an unknown
//!    key simply produces no row and is reported back to the caller as
//!    rejected. That is not merely input validation: resolving against the live
//!    row is also what yields the content signature and the z14 cell, so there
//!    is no cheaper path that skips it. It is also what guarantees a stored
//!    report can never carry a NULL key — see `DatasetSpec::key_list_sql` for
//!    why that matters (DuckDB's list `=` treats NULL as equal to NULL).
//! 2. **Expiry is signature-based, and the signature comes from
//!    `compared_columns`.** `reconcile_source` expires a report once the live
//!    record's `DatasetSpec::content_signature_sql` no longer matches the one
//!    stored at report time, or the record is gone. Reusing the diff's own
//!    notion of "content" is what makes "a new version of the object becomes
//!    importable again" true without a second definition of change — and it
//!    inherits each source's measured choices for free (PRG ignores its
//!    version-churn columns, BDOT10k includes `WERSJA` but not geometry).
//! 3. **Every state change enqueues the object's z14 cell.** Insert, expire and
//!    revoke all move whether the object belongs in `<source>_unmatched`, and
//!    the serving tables are only rebuilt by the drain — so a state change that
//!    forgot to enqueue would be invisible until the next `queue reconcile`.

use anyhow::{Context, Result, bail};
use duckdb::Connection;

use crate::compare::rule::REPORT_ACTIVE;
use crate::dataset::{ALL_SPECS, DatasetSpec};
use crate::tile_math::CHANGE_CELL_ZOOM;

/// Report lifecycle states. `active` is the only one the match-rule veto reads
/// (`compare::rule::REPORT_ACTIVE`); the other two exist so a report is
/// *retired* rather than deleted, keeping the audit trail intact and letting
/// `reports list` explain why an object came back.
pub const STATUS_ACTIVE: &str = REPORT_ACTIVE;
/// The live record changed or disappeared, so the report no longer describes
/// it. Set only by [`reconcile_source`].
pub const STATUS_EXPIRED: &str = "expired";
/// Retracted by an operator via `reports revoke`. Distinct from `expired`
/// because it says something different: the record did not change, a human
/// decided the report was wrong.
pub const STATUS_REVOKED: &str = "revoked";

/// One object a caller wants reported, already resolved to a spec.
#[derive(Debug, Clone)]
pub struct ReportTarget {
    pub spec: &'static DatasetSpec,
    /// Key column values in `spec.key_columns` order. Length is guaranteed to
    /// equal `spec.key_columns.len()` by whoever built this (see
    /// `server::reports::parse_report_body`).
    pub key: Vec<String>,
}

/// Outcome for one submitted object.
#[derive(Debug, Clone)]
pub enum Outcome {
    Accepted {
        report_id: i64,
    },
    /// The key matched no live record. Not an error for the request as a whole
    /// — one stale id in a batch must not discard the rest.
    UnknownRecord,
}

#[derive(Debug, Clone, Default)]
pub struct InsertStats {
    pub accepted: usize,
    pub rejected: usize,
    pub cells_enqueued: i64,
}

/// Store `targets` as active reports and enqueue each object's z14 cell.
///
/// Runs in one transaction: a partially-applied batch would leave reports whose
/// cells were never enqueued, i.e. objects that are reported but still served.
///
/// Returns one [`Outcome`] per target, positionally, so a caller can tell which
/// of a batch's ids were stale without re-querying.
pub fn insert(
    conn: &Connection,
    targets: &[ReportTarget],
    note: Option<&str>,
) -> Result<(Vec<Outcome>, InsertStats)> {
    if targets.is_empty() {
        return Ok((Vec::new(), InsertStats::default()));
    }

    conn.execute_batch("BEGIN TRANSACTION")
        .context("Failed to begin report insert")?;

    let applied = (|| -> Result<(Vec<Outcome>, InsertStats)> {
        // Allocated inside the transaction, the same way
        // `update::dataset::refresh` allocates `snapshot_id`: a concurrent
        // insert that began first blocks this read until it commits or rolls
        // back, so two overlapping batches cannot hand out the same id.
        let mut next_id: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(report_id), 0) + 1 FROM object_reports",
                [],
                |r| r.get(0),
            )
            .context("Failed to allocate report_id")?;

        let mut outcomes = Vec::with_capacity(targets.len());
        let mut stats = InsertStats::default();

        for target in targets {
            let inserted = insert_one(conn, target, next_id, note)?;
            if inserted {
                outcomes.push(Outcome::Accepted { report_id: next_id });
                stats.accepted += 1;
                next_id += 1;
            } else {
                outcomes.push(Outcome::UnknownRecord);
                stats.rejected += 1;
            }
        }

        stats.cells_enqueued = enqueue_cells_for_reports(conn, next_id - stats.accepted as i64)?;
        Ok((outcomes, stats))
    })();

    match applied {
        Ok(result) => {
            conn.execute_batch("COMMIT")
                .context("Failed to commit report insert")?;
            Ok(result)
        }
        Err(e) => {
            if let Err(rb) = conn.execute_batch("ROLLBACK") {
                tracing::warn!(error = %rb, "failed to roll back report insert");
            }
            Err(e)
        }
    }
}

/// Insert one report by resolving its key against the live source table.
/// Returns whether a row was written — `false` means the key matched nothing.
fn insert_one(
    conn: &Connection,
    target: &ReportTarget,
    report_id: i64,
    note: Option<&str>,
) -> Result<bool> {
    let spec = target.spec;
    let point = spec.representative_point_sql("b");
    let cx = crate::tile_math::cell_x_sql(&point);
    let cy = crate::tile_math::cell_y_sql(&point);
    let signature = spec.content_signature_sql("b");
    let key_list = spec.key_list_sql("b");

    // The `?` placeholders carry the *submitted* key, which is why the key
    // comparison is `key_list = list_value(?, ?)` rather than string
    // interpolation: these values come from an HTTP request body.
    let placeholders = vec!["?"; spec.key_columns.len()].join(", ");

    let sql = format!(
        "INSERT INTO object_reports
             (report_id, source, record_key, signature, note,
              reported_at, cell_x, cell_y, status, resolved_at)
         SELECT {report_id}, '{source}', {key_list}, {signature}, ?,
                now(), {cx}, {cy}, '{STATUS_ACTIVE}', NULL
         FROM {table} b
         WHERE {key_list} = list_value({placeholders})
           AND b.geom IS NOT NULL",
        source = spec.name,
        table = spec.table,
    );

    let mut params: Vec<Box<dyn duckdb::ToSql>> = Vec::new();
    params.push(Box::new(note.map(str::to_string)));
    for value in &target.key {
        params.push(Box::new(value.clone()));
    }
    let param_refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    let n = conn
        .execute(&sql, param_refs.as_slice())
        .with_context(|| format!("Failed to insert report for {}", spec.name))?;
    Ok(n > 0)
}

/// Enqueue the z14 cell of every report with `report_id >= from_id`, so the
/// drain rebuilds the cells whose contents these reports just changed.
fn enqueue_cells_for_reports(conn: &Connection, from_id: i64) -> Result<i64> {
    let n = conn
        .execute(
            &format!(
                "INSERT INTO match_dirty_cells
                 SELECT DISTINCT source, {CHANGE_CELL_ZOOM}, cell_x, cell_y, now()
                 FROM object_reports
                 WHERE report_id >= ? AND cell_x IS NOT NULL AND cell_y IS NOT NULL"
            ),
            duckdb::params![from_id],
        )
        .context("Failed to enqueue dirty cells for reports")?;
    Ok(n as i64)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileStats {
    /// Reports retired because their record's content signature moved.
    pub expired_changed: i64,
    /// Reports retired because their record is no longer in the source table.
    pub expired_removed: i64,
    pub cells_enqueued: i64,
}

impl ReconcileStats {
    pub fn total_expired(&self) -> i64 {
        self.expired_changed + self.expired_removed
    }
}

/// Retire every active report for `spec` whose live record has changed or gone,
/// and enqueue the affected cells so the objects reappear in the serving table.
///
/// Cost is `O(active reports for this source)`, not `O(source table)` — the
/// signature is only ever evaluated for rows that carry a report. That is what
/// makes it cheap enough to run unconditionally after every refresh and every
/// import rather than on a schedule.
///
/// Assumes it may be called inside a caller's transaction (the government
/// refresh calls it inside the apply transaction) and so opens none of its own.
pub fn reconcile_source(conn: &Connection, spec: &DatasetSpec) -> Result<ReconcileStats> {
    let key_list = spec.key_list_sql("b");
    let signature = spec.content_signature_sql("b");

    // The "is this report stale" test, written once and used twice: to enqueue
    // the cells *before* anything is updated, and then to do the updating.
    let record_missing = format!(
        "NOT EXISTS (SELECT 1 FROM {table} b WHERE {key_list} = object_reports.record_key)",
        table = spec.table,
    );
    let record_changed = format!(
        "EXISTS (
             SELECT 1 FROM {table} b
             WHERE {key_list} = object_reports.record_key
               AND {signature} IS DISTINCT FROM object_reports.signature
         )",
        table = spec.table,
    );

    // Cells are enqueued BEFORE the status changes, not after, for the same
    // read-before-write reason `update::changeset::insert_change_areas` runs
    // before its delta: once `status` moves off 'active' these rows no longer
    // satisfy the predicate that identifies them, so the set has to be read
    // while it still exists. Deriving it afterwards from `resolved_at` would
    // work only by accident -- `now()` is transaction-start-scoped in DuckDB,
    // so every row retired anywhere in the enclosing transaction shares one
    // timestamp, including rows this call did not touch.
    let cells_enqueued = conn
        .execute(
            &format!(
                "INSERT INTO match_dirty_cells
                 SELECT DISTINCT source, {CHANGE_CELL_ZOOM}, cell_x, cell_y, now()
                 FROM object_reports
                 WHERE status = '{STATUS_ACTIVE}' AND source = '{source}'
                   AND cell_x IS NOT NULL AND cell_y IS NOT NULL
                   AND ({record_missing} OR {record_changed})",
                source = spec.name,
            ),
            [],
        )
        .with_context(|| format!("Failed to enqueue expired-report cells for {}", spec.name))?
        as i64;

    // Split into "changed" and "removed" rather than one combined UPDATE
    // because the two mean different things to an operator reading the log: a
    // changed record is upstream fixing something, a removed one is upstream
    // withdrawing it. Both retire the report; only the counts differ.
    //
    // `removed` runs first so the two are disjoint: a report whose record is
    // gone cannot also be counted as changed, because the second statement
    // only looks at rows still 'active'.
    //
    // Note the `NOT EXISTS` phrasing rather than `record_key NOT IN (...)`:
    // the latter drags in SQL's NULL-in-`NOT IN` trap for no benefit.
    let removed = conn
        .execute(
            &format!(
                "UPDATE object_reports SET status = '{STATUS_EXPIRED}', resolved_at = now()
                 WHERE status = '{STATUS_ACTIVE}' AND source = '{source}'
                   AND {record_missing}",
                source = spec.name,
            ),
            [],
        )
        .with_context(|| format!("Failed to expire removed-record reports for {}", spec.name))?
        as i64;

    let changed = conn
        .execute(
            &format!(
                "UPDATE object_reports SET status = '{STATUS_EXPIRED}', resolved_at = now()
                 WHERE status = '{STATUS_ACTIVE}' AND source = '{source}'
                   AND {record_changed}",
                source = spec.name,
            ),
            [],
        )
        .with_context(|| format!("Failed to expire changed-record reports for {}", spec.name))?
        as i64;

    Ok(ReconcileStats {
        expired_changed: changed,
        expired_removed: removed,
        cells_enqueued,
    })
}

/// `reconcile_source` across every source, for `reports reconcile` with no
/// `--source` and for the background safety-net job.
pub fn reconcile_all(conn: &Connection) -> Result<ReconcileStats> {
    let mut total = ReconcileStats::default();
    for spec in ALL_SPECS {
        crate::shutdown::check_requested()?;
        let s = reconcile_source(conn, spec)?;
        total.expired_changed += s.expired_changed;
        total.expired_removed += s.expired_removed;
        total.cells_enqueued += s.cells_enqueued;
    }
    Ok(total)
}

/// How many reports a revoke should affect.
pub enum RevokeSelector<'a> {
    /// One report, by id.
    Id(i64),
    /// Every active report submitted at or after `since`, optionally narrowed
    /// to one source.
    ///
    /// This is the *only* bulk-undo path, and it exists because nothing about
    /// the submitter is stored: with no identity column there is no way to
    /// revoke "everything from that person", so an abusive burst has to be
    /// unwound by the window it arrived in. See the reports table's comment in
    /// `db::create_schema`.
    Since {
        since: &'a str,
        source: Option<&'a str>,
    },
}

/// Retire reports by operator decision, and enqueue their cells so the objects
/// return to `<source>_unmatched` on the next drain.
///
/// Distinct from expiry: `revoked` records that a human judged the report
/// wrong, where `expired` records that the underlying data moved on.
pub fn revoke(conn: &Connection, selector: RevokeSelector<'_>) -> Result<(i64, i64)> {
    let (predicate, params): (String, Vec<Box<dyn duckdb::ToSql>>) = match selector {
        RevokeSelector::Id(id) => ("report_id = ?".to_string(), vec![Box::new(id)]),
        RevokeSelector::Since { since, source } => match source {
            Some(s) => (
                "reported_at >= ?::TIMESTAMPTZ AND source = ?".to_string(),
                vec![Box::new(since.to_string()), Box::new(s.to_string())],
            ),
            None => (
                "reported_at >= ?::TIMESTAMPTZ".to_string(),
                vec![Box::new(since.to_string())],
            ),
        },
    };
    let refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    conn.execute_batch("BEGIN TRANSACTION")
        .context("Failed to begin revoke")?;
    let applied = (|| -> Result<(i64, i64)> {
        // Same read-before-write ordering as `reconcile_source`: the rows stop
        // matching the predicate once their status moves.
        let cells = conn
            .execute(
                &format!(
                    "INSERT INTO match_dirty_cells
                     SELECT DISTINCT source, {CHANGE_CELL_ZOOM}, cell_x, cell_y, now()
                     FROM object_reports
                     WHERE status = '{STATUS_ACTIVE}' AND {predicate}
                       AND cell_x IS NOT NULL AND cell_y IS NOT NULL"
                ),
                refs.as_slice(),
            )
            .context("Failed to enqueue revoked-report cells")? as i64;
        let revoked = conn
            .execute(
                &format!(
                    "UPDATE object_reports
                     SET status = '{STATUS_REVOKED}', resolved_at = now()
                     WHERE status = '{STATUS_ACTIVE}' AND {predicate}"
                ),
                refs.as_slice(),
            )
            .context("Failed to revoke reports")? as i64;
        Ok((revoked, cells))
    })();

    match applied {
        Ok(result) => {
            conn.execute_batch("COMMIT")
                .context("Failed to commit revoke")?;
            Ok(result)
        }
        Err(e) => {
            if let Err(rb) = conn.execute_batch("ROLLBACK") {
                tracing::warn!(error = %rb, "failed to roll back revoke");
            }
            Err(e)
        }
    }
}

/// Per-status counts for one source, for `/status` and `reports list`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct ReportCounts {
    pub active: i64,
    pub expired: i64,
    pub revoked: i64,
}

pub fn counts(conn: &Connection) -> Result<ReportCounts> {
    conn.query_row(
        &format!(
            "SELECT
                 COUNT(*) FILTER (status = '{STATUS_ACTIVE}'),
                 COUNT(*) FILTER (status = '{STATUS_EXPIRED}'),
                 COUNT(*) FILTER (status = '{STATUS_REVOKED}')
             FROM object_reports"
        ),
        [],
        |r| {
            Ok(ReportCounts {
                active: r.get(0)?,
                expired: r.get(1)?,
                revoked: r.get(2)?,
            })
        },
    )
    .context("Failed to count object_reports")
}

/// One report as `reports list` prints it and `reports export` serialises it.
///
/// `record_key` stays a list rather than being flattened into named columns
/// because the arity is per-source; the `source` field is what tells a reader
/// which `DatasetSpec.key_columns` the values line up with.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ReportRow {
    pub report_id: i64,
    pub source: String,
    pub record_key: Vec<String>,
    pub signature: Option<String>,
    pub note: Option<String>,
    pub reported_at: String,
    pub cell_x: Option<i32>,
    pub cell_y: Option<i32>,
    pub status: String,
    pub resolved_at: Option<String>,
}

/// Read reports newest-first, optionally narrowed.
pub fn list(
    conn: &Connection,
    source: Option<&str>,
    status: Option<&str>,
    since: Option<&str>,
    limit: Option<usize>,
) -> Result<Vec<ReportRow>> {
    let mut wheres: Vec<String> = Vec::new();
    let mut params: Vec<Box<dyn duckdb::ToSql>> = Vec::new();
    if let Some(s) = source {
        if crate::dataset::spec_by_name(s).is_none() {
            bail!("unknown source '{s}'; expected bdot10k, egib or prg");
        }
        wheres.push("source = ?".to_string());
        params.push(Box::new(s.to_string()));
    }
    if let Some(s) = status {
        if ![STATUS_ACTIVE, STATUS_EXPIRED, STATUS_REVOKED].contains(&s) {
            bail!(
                "unknown status '{s}'; expected {STATUS_ACTIVE}, {STATUS_EXPIRED} or {STATUS_REVOKED}"
            );
        }
        wheres.push("status = ?".to_string());
        params.push(Box::new(s.to_string()));
    }
    if let Some(s) = since {
        wheres.push("reported_at >= ?::TIMESTAMPTZ".to_string());
        params.push(Box::new(s.to_string()));
    }
    let filter = if wheres.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", wheres.join(" AND "))
    };
    let refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    // Two deliberate casts in the projection, both so the row decodes with the
    // types duckdb-rs actually implements:
    //
    // - `record_key` is a `VARCHAR[]`, which duckdb-rs has no `FromSql` for, so
    //   it goes over as JSON text and is parsed here. `to_json` / `CAST(... AS
    //   VARCHAR[])` round-trip exactly (verified), which is what makes
    //   `reports export | reports import` faithful.
    // - Timestamps become VARCHAR in SQL rather than being read as a typed
    //   value and formatted in Rust, so the import side can hand them straight
    //   back through `?::TIMESTAMPTZ` with no format to keep in sync.
    // `None` omits the clause outright rather than standing in a huge literal:
    // `usize::MAX` is wider than the INT64 DuckDB casts a LIMIT to, so it does
    // not merely look silly, it fails the statement ("Type INT128 with value
    // 18446744073709551615 can't be cast"). `reports export` is the caller that
    // wants every row.
    let limit_sql = match limit {
        Some(n) => format!(" LIMIT {n}"),
        None => String::new(),
    };
    let sql = format!(
        "SELECT report_id, source, to_json(record_key)::VARCHAR, signature, note,
                reported_at::VARCHAR, cell_x, cell_y, status, resolved_at::VARCHAR
         FROM object_reports {filter}
         ORDER BY report_id DESC{limit_sql}"
    );
    let mut stmt = conn
        .prepare(&sql)
        .context("Failed to prepare report list")?;
    let rows = stmt
        .query_map(refs.as_slice(), |r| {
            Ok(ReportRow {
                report_id: r.get(0)?,
                source: r.get(1)?,
                record_key: {
                    let json: String = r.get(2)?;
                    serde_json::from_str(&json).map_err(|e| {
                        duckdb::Error::FromSqlConversionFailure(
                            2,
                            duckdb::types::Type::Any,
                            Box::new(e),
                        )
                    })?
                },
                signature: r.get(3)?,
                note: r.get(4)?,
                reported_at: r.get(5)?,
                cell_x: r.get(6)?,
                cell_y: r.get(7)?,
                status: r.get(8)?,
                resolved_at: r.get(9)?,
            })
        })
        .context("Failed to read object_reports")?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.context("Failed to decode a report row")?);
    }
    Ok(out)
}

/// Re-insert reports from a `reports export` dump.
///
/// Deliberately *not* an upsert and deliberately not id-preserving: ids are
/// reallocated from the current maximum so importing into a database that
/// already has reports cannot collide or silently overwrite. A round trip into
/// an empty database is therefore faithful in content but not in id, which is
/// the property that matters -- `record_key` is what identifies the object.
pub fn import_rows(conn: &Connection, rows: &[ReportRow]) -> Result<i64> {
    if rows.is_empty() {
        return Ok(0);
    }
    conn.execute_batch("BEGIN TRANSACTION")
        .context("Failed to begin report import")?;
    let applied = (|| -> Result<i64> {
        let mut next_id: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(report_id), 0) + 1 FROM object_reports",
                [],
                |r| r.get(0),
            )
            .context("Failed to allocate report_id")?;
        let mut n = 0i64;
        for row in rows {
            conn.execute(
                "INSERT INTO object_reports
                     (report_id, source, record_key, signature, note,
                      reported_at, cell_x, cell_y, status, resolved_at)
                 VALUES (?, ?, CAST(? AS VARCHAR[]), ?, ?, ?::TIMESTAMPTZ, ?, ?, ?,
                         ?::TIMESTAMPTZ)",
                duckdb::params![
                    next_id,
                    row.source,
                    serde_json::to_string(&row.record_key)?,
                    row.signature,
                    row.note,
                    row.reported_at,
                    row.cell_x,
                    row.cell_y,
                    row.status,
                    row.resolved_at,
                ],
            )
            .context("Failed to insert an imported report")?;
            next_id += 1;
            n += 1;
        }
        // Every imported active report may suppress an object, so their cells
        // have to be rebuilt -- same reasoning as `insert`.
        conn.execute(
            &format!(
                "INSERT INTO match_dirty_cells
                 SELECT DISTINCT source, {CHANGE_CELL_ZOOM}, cell_x, cell_y, now()
                 FROM object_reports
                 WHERE report_id >= ? AND status = '{STATUS_ACTIVE}'
                   AND cell_x IS NOT NULL AND cell_y IS NOT NULL"
            ),
            duckdb::params![next_id - n],
        )
        .context("Failed to enqueue imported-report cells")?;
        Ok(n)
    })();
    match applied {
        Ok(n) => {
            conn.execute_batch("COMMIT")
                .context("Failed to commit report import")?;
            Ok(n)
        }
        Err(e) => {
            if let Err(rb) = conn.execute_batch("ROLLBACK") {
                tracing::warn!(error = %rb, "failed to roll back report import");
            }
            Err(e)
        }
    }
}

/// `reports <action>` dispatch.
pub fn run(conn: &Connection, action: crate::cli::ReportsAction) -> Result<()> {
    use crate::cli::ReportsAction;
    match action {
        ReportsAction::List {
            source,
            status,
            since,
            limit,
        } => {
            let rows = list(
                conn,
                source.as_deref(),
                status.as_deref(),
                since.as_deref(),
                Some(limit),
            )?;
            if rows.is_empty() {
                println!("no reports");
            }
            for r in &rows {
                println!(
                    "{:>6}  {:<8} {:<9} {}  key={:?}{}",
                    r.report_id,
                    r.source,
                    r.status,
                    r.reported_at,
                    r.record_key,
                    r.note
                        .as_deref()
                        .map(|n| format!("  note={n}"))
                        .unwrap_or_default(),
                );
            }
            Ok(())
        }
        ReportsAction::Revoke { id, since, source } => {
            let selector = match (id, since.as_deref()) {
                (Some(id), None) => RevokeSelector::Id(id),
                (None, Some(since)) => RevokeSelector::Since {
                    since,
                    source: source.as_deref(),
                },
                // clap's `conflicts_with` rules out "both"; this is "neither",
                // which it cannot express for a positional plus a flag.
                _ => bail!("specify either a report id or --since <timestamp>"),
            };
            let (revoked, cells) = revoke(conn, selector)?;
            tracing::info!(
                revoked,
                cells_enqueued = cells,
                "revoked reports; run `queue drain` (or let the server's match_refresh job run) for the objects to reappear"
            );
            Ok(())
        }
        ReportsAction::Reconcile { source } => {
            let stats = match source.as_deref() {
                Some(name) => {
                    let spec = crate::dataset::spec_by_name(name)
                        .ok_or_else(|| anyhow::anyhow!("unknown source '{name}'"))?;
                    reconcile_source(conn, spec)?
                }
                None => reconcile_all(conn)?,
            };
            tracing::info!(
                expired_changed = stats.expired_changed,
                expired_removed = stats.expired_removed,
                cells_enqueued = stats.cells_enqueued,
                "report reconcile complete"
            );
            Ok(())
        }
        ReportsAction::Export { file } => {
            // `None` = no LIMIT clause: an export that silently stopped at some
            // default would be a backup missing rows, which is worse than no
            // backup for being indistinguishable from a complete one.
            let rows = list(conn, None, None, None, None)?;
            let mut out = String::new();
            for row in &rows {
                out.push_str(&serde_json::to_string(row)?);
                out.push('\n');
            }
            if file.as_os_str() == "-" {
                print!("{out}");
            } else {
                std::fs::write(&file, out)
                    .with_context(|| format!("Failed to write {}", file.display()))?;
            }
            tracing::info!(rows = rows.len(), file = %file.display(), "exported reports");
            Ok(())
        }
        ReportsAction::Import { file } => {
            let text = std::fs::read_to_string(&file)
                .with_context(|| format!("Failed to read {}", file.display()))?;
            let mut rows = Vec::new();
            for (i, line) in text.lines().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }
                rows.push(
                    serde_json::from_str::<ReportRow>(line).with_context(|| {
                        format!("{}:{}: malformed report", file.display(), i + 1)
                    })?,
                );
            }
            let n = import_rows(conn, &rows)?;
            tracing::info!(rows = n, "imported reports");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compare::incremental::recompute_cell;
    use crate::dataset::{BDOT10K, PRG};
    use crate::db::init_db;
    use crate::tile_math::lonlat_to_tile;
    use std::path::Path;

    /// Mirrors `compare::incremental`'s fixture: `bdot10k_buildings` and
    /// `prg_addresses` are built by their importers rather than by
    /// `create_schema`, so a test has to declare them itself.
    fn conn() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR,
                 geom GEOMETRY, centroid GEOMETRY, WERSJA VARCHAR,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR,
                 LICZBAKONDYGNACJI SMALLINT,
                 KATEGORIAISTNIENIA VARCHAR DEFAULT 'eksploatowany',
                 NAZWA VARCHAR, FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 wazny_od_lub_data_nadania DATE, teryt_gmina VARCHAR, gmina VARCHAR,
                 geom GEOMETRY);",
        )
        .unwrap();
        c
    }

    /// One unmatched BDOT10k building at a known location.
    fn seed_building(c: &Connection) -> (i32, i32) {
        c.execute_batch(
            "INSERT INTO bdot10k_buildings (PRZESTRZENNAZW, LOKALNYID, geom, WERSJA) VALUES
                 ('04', 'bud-1', ST_MakeEnvelope(21.0,52.0,21.001,52.001), 'v1');
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();
        let (x, y) = lonlat_to_tile(21.0005, 52.0005, CHANGE_CELL_ZOOM);
        (x as i32, y as i32)
    }

    fn bdot_target(key: &[&str]) -> ReportTarget {
        ReportTarget {
            spec: &BDOT10K,
            key: key.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn count(c: &Connection, sql: &str) -> i64 {
        c.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    #[test]
    fn reporting_a_building_removes_it_from_the_serving_table_on_the_next_recompute() {
        let c = conn();
        let (cx, cy) = seed_building(&c);

        recompute_cell(&c, "bdot10k", cx, cy).unwrap();
        assert_eq!(
            count(&c, "SELECT COUNT(*) FROM bdot10k_unmatched"),
            1,
            "precondition: the building is unmatched before anyone reports it"
        );

        let (outcomes, stats) = insert(&c, &[bdot_target(&["04", "bud-1"])], None).unwrap();
        assert!(matches!(outcomes[0], Outcome::Accepted { .. }));
        assert_eq!(stats.accepted, 1);
        assert_eq!(
            stats.cells_enqueued, 1,
            "the object's cell must be enqueued"
        );

        recompute_cell(&c, "bdot10k", cx, cy).unwrap();
        assert_eq!(
            count(&c, "SELECT COUNT(*) FROM bdot10k_unmatched"),
            0,
            "an actively reported building must not be served"
        );
        // The denominator is deliberately untouched: a reported object is
        // still a comparable government object, exactly like a
        // former-building-suppressed one. See compare::totals' module doc.
        assert_eq!(
            count(&c, "SELECT COALESCE(SUM(total),0) FROM cell_totals"),
            1,
            "reporting must not change cell_totals"
        );
    }

    #[test]
    fn a_key_that_matches_no_live_record_is_rejected_and_stores_nothing() {
        let c = conn();
        seed_building(&c);
        let (outcomes, stats) = insert(
            &c,
            &[
                bdot_target(&["04", "bud-1"]),
                bdot_target(&["04", "does-not-exist"]),
                // Right LOKALNYID, wrong namespace half of the composite key:
                // this is the case that would silently succeed if reports were
                // keyed on LOKALNYID alone.
                bdot_target(&["99", "bud-1"]),
            ],
            None,
        )
        .unwrap();
        assert!(matches!(outcomes[0], Outcome::Accepted { .. }));
        assert!(matches!(outcomes[1], Outcome::UnknownRecord));
        assert!(matches!(outcomes[2], Outcome::UnknownRecord));
        assert_eq!(stats.accepted, 1);
        assert_eq!(stats.rejected, 2);
        assert_eq!(count(&c, "SELECT COUNT(*) FROM object_reports"), 1);
    }

    /// `object_reports` has no UNIQUE constraint, so the same object can carry
    /// several reports. The veto is phrased `NOT EXISTS` precisely so that
    /// cannot fan out into duplicate serving rows -- rewriting it as a join
    /// would reintroduce the `street_name_mappings` duplicate-row failure.
    #[test]
    fn two_reports_on_one_object_still_suppress_exactly_one_row() {
        let c = conn();
        let (cx, cy) = seed_building(&c);
        insert(&c, &[bdot_target(&["04", "bud-1"])], None).unwrap();
        insert(&c, &[bdot_target(&["04", "bud-1"])], None).unwrap();
        assert_eq!(count(&c, "SELECT COUNT(*) FROM object_reports"), 2);

        recompute_cell(&c, "bdot10k", cx, cy).unwrap();
        assert_eq!(
            count(&c, "SELECT COUNT(*) FROM bdot10k_unmatched"),
            0,
            "two reports must suppress the one row, not produce two rows"
        );

        // And with the reports revoked, exactly one row comes back -- the
        // other half of the same guarantee.
        revoke(&c, RevokeSelector::Id(1)).unwrap();
        revoke(&c, RevokeSelector::Id(2)).unwrap();
        recompute_cell(&c, "bdot10k", cx, cy).unwrap();
        assert_eq!(count(&c, "SELECT COUNT(*) FROM bdot10k_unmatched"), 1);
    }

    #[test]
    fn an_expired_or_revoked_report_does_not_suppress() {
        let c = conn();
        let (cx, cy) = seed_building(&c);
        insert(&c, &[bdot_target(&["04", "bud-1"])], None).unwrap();
        for status in [STATUS_EXPIRED, STATUS_REVOKED] {
            c.execute(
                "UPDATE object_reports SET status = ?",
                duckdb::params![status],
            )
            .unwrap();
            recompute_cell(&c, "bdot10k", cx, cy).unwrap();
            assert_eq!(
                count(&c, "SELECT COUNT(*) FROM bdot10k_unmatched"),
                1,
                "a {status} report must not suppress anything"
            );
        }
    }

    #[test]
    fn reconcile_expires_a_report_whose_record_changed_and_enqueues_its_cell() {
        let c = conn();
        let (cx, cy) = seed_building(&c);
        insert(&c, &[bdot_target(&["04", "bud-1"])], None).unwrap();
        c.execute_batch("DELETE FROM match_dirty_cells;").unwrap();

        // WERSJA is in BDOT10k's compared_columns, so bumping it is exactly
        // what "a new version of this object was published" means.
        c.execute_batch("UPDATE bdot10k_buildings SET WERSJA = 'v2';")
            .unwrap();
        let stats = reconcile_source(&c, &BDOT10K).unwrap();
        assert_eq!(stats.expired_changed, 1);
        assert_eq!(stats.expired_removed, 0);
        assert_eq!(stats.cells_enqueued, 1);
        assert_eq!(
            count(
                &c,
                &format!("SELECT COUNT(*) FROM object_reports WHERE status = '{STATUS_EXPIRED}'")
            ),
            1
        );

        recompute_cell(&c, "bdot10k", cx, cy).unwrap();
        assert_eq!(
            count(&c, "SELECT COUNT(*) FROM bdot10k_unmatched"),
            1,
            "a republished record must become importable again"
        );
    }

    #[test]
    fn reconcile_expires_a_report_whose_record_was_removed() {
        let c = conn();
        seed_building(&c);
        insert(&c, &[bdot_target(&["04", "bud-1"])], None).unwrap();
        c.execute_batch("DELETE FROM bdot10k_buildings;").unwrap();
        let stats = reconcile_source(&c, &BDOT10K).unwrap();
        assert_eq!(stats.expired_removed, 1);
        assert_eq!(stats.expired_changed, 0);
    }

    #[test]
    fn reconcile_leaves_an_unchanged_report_alone() {
        let c = conn();
        seed_building(&c);
        insert(&c, &[bdot_target(&["04", "bud-1"])], None).unwrap();
        c.execute_batch("DELETE FROM match_dirty_cells;").unwrap();
        let stats = reconcile_source(&c, &BDOT10K).unwrap();
        assert_eq!(stats, ReconcileStats::default(), "nothing changed upstream");
        assert_eq!(
            count(&c, "SELECT COUNT(*) FROM match_dirty_cells"),
            0,
            "a no-op reconcile must not enqueue work"
        );
    }

    /// The measured reason PRG's version columns are outside
    /// `compared_columns`: it bulk-republishes by gmina, moving `wersja_id` on
    /// records whose content is byte-identical. Because the report signature is
    /// built from the same column set as the refresh diff, that churn cannot
    /// expire reports either -- a property inherited rather than re-decided.
    #[test]
    fn a_prg_version_only_bump_does_not_expire_a_report() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO prg_addresses
                 (lokalny_id, numer_porzadkowy, ulica, miejscowosc, kod_pocztowy,
                  teryt_miejscowosc, geom)
             VALUES ('adr-1','5','Polna','Wies','00-001','0918123', ST_Point(21.0,52.0));",
        )
        .unwrap();
        insert(
            &c,
            &[ReportTarget {
                spec: &PRG,
                key: vec!["adr-1".to_string()],
            }],
            None,
        )
        .unwrap();

        // `wazny_od_lub_data_nadania` is carried but deliberately NOT compared,
        // standing in here for the version-metadata columns PRG excludes.
        c.execute_batch("UPDATE prg_addresses SET wazny_od_lub_data_nadania = DATE '2020-01-01';")
            .unwrap();
        assert_eq!(
            reconcile_source(&c, &PRG).unwrap(),
            ReconcileStats::default(),
            "a change outside compared_columns must not expire the report"
        );

        // ...whereas a change to a compared column must.
        c.execute_batch("UPDATE prg_addresses SET numer_porzadkowy = '7';")
            .unwrap();
        assert_eq!(reconcile_source(&c, &PRG).unwrap().expired_changed, 1);
    }

    #[test]
    fn revoke_since_unwinds_a_burst_and_restores_the_objects() {
        let c = conn();
        let (cx, cy) = seed_building(&c);
        insert(&c, &[bdot_target(&["04", "bud-1"])], None).unwrap();
        recompute_cell(&c, "bdot10k", cx, cy).unwrap();
        assert_eq!(count(&c, "SELECT COUNT(*) FROM bdot10k_unmatched"), 0);

        let (revoked, cells) = revoke(
            &c,
            RevokeSelector::Since {
                since: "2000-01-01",
                source: Some("bdot10k"),
            },
        )
        .unwrap();
        assert_eq!(revoked, 1);
        assert_eq!(cells, 1, "the revoke must enqueue the object's cell");

        recompute_cell(&c, "bdot10k", cx, cy).unwrap();
        assert_eq!(count(&c, "SELECT COUNT(*) FROM bdot10k_unmatched"), 1);
    }

    #[test]
    fn counts_reports_by_status() {
        let c = conn();
        seed_building(&c);
        insert(&c, &[bdot_target(&["04", "bud-1"])], None).unwrap();
        assert_eq!(
            counts(&c).unwrap(),
            ReportCounts {
                active: 1,
                expired: 0,
                revoked: 0
            }
        );
        revoke(&c, RevokeSelector::Id(1)).unwrap();
        assert_eq!(
            counts(&c).unwrap(),
            ReportCounts {
                active: 0,
                expired: 0,
                revoked: 1
            }
        );
    }

    /// `reports export` asks for every row, which is `limit: None`. That path
    /// used to stand in `usize::MAX` and fail the statement outright ("Type
    /// INT128 with value 18446744073709551615 can't be cast"), because DuckDB
    /// casts a LIMIT to INT64 — an unlimited list is the one shape no other
    /// caller exercises, and the whole suite passed with export broken.
    #[test]
    fn listing_without_a_limit_returns_every_report() {
        let c = conn();
        seed_building(&c);
        insert(&c, &[bdot_target(&["04", "bud-1"])], None).unwrap();

        let all = list(&c, None, None, None, None).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(
            all[0].record_key,
            vec!["04".to_string(), "bud-1".to_string()]
        );

        // A limit still narrows, so dropping the clause did not drop the option.
        assert_eq!(list(&c, None, None, None, Some(0)).unwrap().len(), 0);
    }
}
