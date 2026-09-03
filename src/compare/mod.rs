pub mod addresses;
pub mod buildings;
pub mod columns;
pub mod drain;
pub mod incremental;
pub mod reconcile;
pub mod rule;
pub mod totals;

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::info;

use crate::cli::{AddressesSource, BuildingsSource, CompareTarget, QueueAction};

/// `shutdown::check_requested()` is called between sub-compares below, never
/// mid-compare: each of `compare_bdot10k`/`compare_egib`/`compare_prg` is
/// already independently atomic (`in_transaction`), so bailing between them
/// only ever skips a sub-compare that hasn't started -- it can never discard
/// one that already committed. Mid-compare cancellation is a separate seam
/// with a different rationale, inside `compare_buildings`'s grid loop.
///
/// It bails with `Err` rather than swallowing the cancellation as `Ok(())`,
/// for the same reason that grid-loop check does (see its comment): `compare`
/// is invoked by hand, and a non-zero exit after "Shutdown requested" on
/// stderr is the correct signal -- not a silent partial run that looks
/// identical to a completed one.
pub fn run(conn: &Connection, target: CompareTarget) -> Result<()> {
    match target {
        CompareTarget::Buildings { source } => match source {
            None | Some(BuildingsSource::All) => {
                buildings::compare_bdot10k(conn)?;
                crate::shutdown::check_requested()?;
                buildings::compare_egib(conn)?;
            }
            Some(BuildingsSource::Bdot10k) => buildings::compare_bdot10k(conn)?,
            Some(BuildingsSource::Egib) => buildings::compare_egib(conn)?,
        },
        CompareTarget::Addresses { source } => match source {
            None | Some(AddressesSource::All) => addresses::compare_prg(conn)?,
            Some(AddressesSource::Prg) => addresses::compare_prg(conn)?,
        },
        CompareTarget::Full => {
            buildings::compare_bdot10k(conn)?;
            crate::shutdown::check_requested()?;
            buildings::compare_egib(conn)?;
            crate::shutdown::check_requested()?;
            addresses::compare_prg(conn)?;
            // Pure insurance, not a normal invalidation path: every row this
            // rewrote already moved that cell's own per-cell version (see
            // `serving_version`'s module doc), so this bump covers only the
            // case where `compare full` is run as an offline rebuild (e.g.
            // restoring the DB from a snapshot) that never went through a
            // dirty cell at all -- without it, a client's cached ETag for an
            // unrelated tile could in principle survive such a rebuild.
            // Deliberately NOT inside `full`'s per-source transactions
            // (`in_transaction`, above): each source's rebuild is already
            // independently atomic, and this is a single global stamp for
            // the whole `Full` run, not per source.
            crate::serving_version::bump_serving_epoch(conn)?;
        }
    }
    Ok(())
}

/// Handles the `queue` CLI command: `reconcile` re-enqueues, `drain` runs
/// `drain::drain_batch` in a loop until the queue is empty (a batch that
/// drains zero cells means either nothing is left, or everything left is
/// failing and re-selecting it won't help -- either way, further looping
/// can't make progress) or the run is interrupted. Checked between batches
/// rather than passed only into `drain_batch`'s per-cell `is_cancelled`, so a
/// Ctrl+C between batches bails loudly with `Err` instead of quietly
/// stopping and reporting success, matching `run` above.
pub fn run_queue(conn: &Connection, action: QueueAction) -> Result<()> {
    match action {
        QueueAction::Reconcile => {
            let enqueued = reconcile::enqueue_all(conn)?;
            info!(enqueued, "reconcile sweep complete");
        }
        QueueAction::Drain { batch_size } => {
            // A one-shot estimate of the work ahead, logged as the denominator
            // of the per-batch progress line below. Only an estimate: cells
            // re-dirtied mid-drain push the real total up, and a batch that
            // drains only failing cells ends the loop early -- so
            // `total_drained` can finish under, over, or (usually) at this
            // number. `drain_batch` selects the same distinct
            // (source, cell_x, cell_y) grouping.
            let queued_cells: u64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM (
                         SELECT 1 FROM match_dirty_cells GROUP BY source, cell_x, cell_y
                     )",
                    [],
                    |r| r.get::<_, i64>(0).map(|n| n as u64),
                )
                .context("drain: count queued cells")?;
            info!(cells = queued_cells, "drain starting");

            let mut total_drained = 0u64;
            let mut total_failed = 0u64;
            loop {
                crate::shutdown::check_requested()?;
                let stats = drain::drain_batch(conn, batch_size, &crate::shutdown::is_requested)?;
                total_drained += stats.cells;
                total_failed += stats.failed;
                if stats.cells == 0 {
                    break;
                }
                info!(
                    progress = format!("{total_drained}/{queued_cells}"),
                    failed = total_failed,
                    "drain progress"
                );
            }
            if total_failed > 0 {
                tracing::warn!(
                    drained = total_drained,
                    failed = total_failed,
                    "drain complete with some cells left queued for retry"
                );
            } else {
                info!(drained = total_drained, "drain complete, queue empty");
            }
        }
    }
    Ok(())
}

/// Run `f` inside a DuckDB transaction, committing on success and rolling back
/// on error.
///
/// **Why the full compare needs this.** Every full comparison is a
/// clear-then-repopulate: `DELETE FROM <source>_unmatched`, then insert the
/// current unmatched set. Without a transaction the DELETE commits on its own,
/// so *any* later failure — a stale serving-table schema whose columns the
/// INSERT names, a source-extent fault, a disk error mid-grid — leaves the
/// serving table empty rather than leaving the previous contents in place.
/// That is a silent outage: `/tiles` and `/package` then answer every request
/// with zero features, and nothing in `/status` reports it, because `compare`
/// writes no `job_run_log` entry. It happened for real on the Poland database
/// (`bdot10k_unmatched` emptied by a compare against a serving table predating
/// the carried classification columns). Wrapping the clear and the repopulate
/// together makes a failed compare a no-op instead.
///
/// Scope is one source's rebuild, not the whole `compare full` run: a failure
/// comparing egib should not discard a bdot10k rebuild that already succeeded,
/// and the three sources share no invariant that would require them to move
/// together. The per-cell incremental path already had this property — see
/// `drain::drain_batch`, which pairs `incremental::recompute_cell_in_txn` with
/// its queue delete in one transaction — so this closes the gap between the
/// full and incremental paths rather than introducing a new idea.
///
/// A rollback that itself fails is logged and the original error returned:
/// the original error is what explains the failure, and shadowing it with a
/// cleanup error would lose that.
pub fn in_transaction<T>(
    conn: &Connection,
    label: &str,
    f: impl FnOnce() -> Result<T>,
) -> Result<T> {
    conn.execute_batch("BEGIN TRANSACTION")
        .with_context(|| format!("{label}: failed to begin transaction"))?;
    match f() {
        Ok(value) => {
            conn.execute_batch("COMMIT")
                .with_context(|| format!("{label}: failed to commit"))?;
            Ok(value)
        }
        Err(e) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK") {
                tracing::warn!(
                    error = %rollback_err,
                    label,
                    "rollback failed after a compare error; the original error follows"
                );
            }
            Err(e)
        }
    }
}

/// The design's central correctness invariant (see
/// docs/superpowers/specs/2026-07-24-precomputed-unmatched-serving-design.md,
/// "Testing", and line 236: "Its output must be row-identical to draining an
/// enqueue-all through the incremental path"): a full `compare` and an
/// enqueue-all-then-drain through the incremental path must produce
/// row-identical `*_unmatched` sets, for each source. The plan's own test
/// suite only pinned the address grid-key fast path against the shared
/// per-cell rule (`addresses::tests::full_and_per_cell_paths_agree`) -- a
/// different pair of paths -- so this is the actual round-trip the design
/// specifies, spanning `compare::buildings`, `compare::addresses`,
/// `compare::reconcile` and `compare::drain` together.
#[cfg(test)]
mod full_vs_incremental_equivalence {
    use std::collections::BTreeSet;
    use std::path::Path;

    use duckdb::Connection;

    use crate::compare::addresses::compare_prg;
    use crate::compare::buildings::compare_bdot10k;
    use crate::compare::drain::drain_batch;
    use crate::compare::reconcile::enqueue_all;
    use crate::db::init_db;

    fn conn() -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        // enqueue_all touches all three government tables unconditionally,
        // so all three must exist even if a given test only seeds one.
        c.execute_batch(
            "CREATE TABLE bdot10k_buildings (PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR, LICZBAKONDYGNACJI SMALLINT,
                 KATEGORIAISTNIENIA VARCHAR DEFAULT 'eksploatowany',
                 NAZWA VARCHAR, FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 kondygnacje_nadziemne INTEGER, kondygnacje_podziemne INTEGER, rodzaj VARCHAR);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 wazny_od_lub_data_nadania DATE, teryt_gmina VARCHAR, gmina VARCHAR, geom GEOMETRY);",
        )
        .unwrap();
        c
    }

    /// Drain the whole queue, not just one batch.
    fn drain_all(c: &Connection) {
        loop {
            let s = drain_batch(c, 1000, &|| false).unwrap();
            if s.cells == 0 {
                break;
            }
        }
    }

    /// One comparable string per row: id, geometry-as-WKT and cell tags, plus
    /// `extra_col` when given, which pins one carried column between the
    /// full-compare and incremental paths and closes the blind spot this
    /// function otherwise has for anything beyond id/geom/cell tags.
    /// `computed_at` is deliberately excluded -- the two recompute paths run at
    /// different wall-clock times, so that column is expected to differ.
    fn snapshot(
        c: &Connection,
        table: &str,
        id_col: &str,
        extra_col: Option<&str>,
    ) -> BTreeSet<String> {
        let extra = extra_col
            .map(|col| format!(" || '|' || COALESCE({col}::VARCHAR, '')"))
            .unwrap_or_default();
        let sql = format!(
            "SELECT {id_col} || '|' || ST_AsText(geom) || '|' ||
                    CAST(cell_x AS VARCHAR) || '|' || CAST(cell_y AS VARCHAR){extra}
             FROM {table}"
        );
        let mut stmt = c.prepare(&sql).unwrap();
        stmt.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    /// `cell_totals` for one source, as comparable strings. The ratio served
    /// at low zoom is `<source>_unmatched ÷ cell_totals`, so the denominator
    /// has to survive a path switch just as the numerator does — and it is
    /// written by genuinely different SQL on each path (a whole-table GROUP BY
    /// in the full compare, an envelope-filtered count per cell in the drain).
    fn totals_snapshot(c: &Connection, source: &str) -> BTreeSet<String> {
        let mut stmt = c
            .prepare(
                "SELECT CAST(cell_x AS VARCHAR) || '|' || CAST(cell_y AS VARCHAR)
                        || '|' || CAST(total AS VARCHAR)
                 FROM cell_totals WHERE source = ?",
            )
            .unwrap();
        stmt.query_map(duckdb::params![source], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    #[test]
    fn full_compare_and_reconcile_drain_agree_on_bdot10k() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES
                 (1, 'way', NULL, ST_MakeEnvelope(20.0, 52.0, 20.001, 52.001));
             INSERT INTO osm_former_buildings VALUES
                 (2, 'way', 'demolished:building', 'yes',
                  ST_MakeEnvelope(22.9999,53.9999,23.0011,54.0011));
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('inside', ST_MakeEnvelope(20.0002,52.0002,20.0008,52.0008)),
                 ('lonely', ST_MakeEnvelope(21.0,52.2,21.001,52.201)),
                 -- Outside the old hardcoded (14,49,25,55) compare_buildings
                 -- bbox: the extent-divergence scenario the extent fix
                 -- exists to close, and the scenario this test must be able
                 -- to catch a regression of.
                 ('stray', ST_MakeEnvelope(30.0,60.0,30.001,60.001)),
                 -- Suppressed by the former-building veto: covered by
                 -- osm_former_buildings above, not by osm_buildings, so it
                 -- must be neither matched nor unmatched under either path.
                 ('former', ST_MakeEnvelope(23.0,54.0,23.001,54.001)),
                 -- Vetoed by a user report. Identical to 'lonely' in every
                 -- respect the match rule inspects, so the report is the only
                 -- thing separating them -- a path that dropped the veto puts
                 -- this back in the unmatched set and fails the count below.
                 ('reported', ST_MakeEnvelope(24.0,54.5,24.001,54.501));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);
             INSERT INTO object_reports
                 (report_id, source, record_key, signature,
                  reported_at, cell_x, cell_y, status, resolved_at)
             VALUES (1, 'bdot10k', [NULL, 'reported'], 'sig',
                     now(), NULL, NULL, 'active', NULL);",
        )
        .unwrap();

        compare_bdot10k(&c).unwrap();
        let full = snapshot(&c, "bdot10k_unmatched", "LOKALNYID", None);
        let full_totals = totals_snapshot(&c, "bdot10k");
        assert_eq!(
            full.len(),
            2,
            "sanity: 'inside' matched, 'former' suppressed, 'reported' vetoed, \
             'lonely' and 'stray' unmatched"
        );
        assert_eq!(
            full_totals.len(),
            5,
            "sanity: all five buildings count towards a denominator -- a reported \
             building stays comparable, exactly like a suppressed one, so \
             `cell_totals` must not shrink when someone files a report"
        );

        c.execute_batch("DELETE FROM bdot10k_unmatched; DELETE FROM cell_totals;")
            .unwrap();
        enqueue_all(&c).unwrap();
        drain_all(&c);
        let incremental = snapshot(&c, "bdot10k_unmatched", "LOKALNYID", None);

        assert_eq!(
            full, incremental,
            "full compare and reconcile+drain must produce row-identical bdot10k_unmatched"
        );
        assert_eq!(
            full_totals,
            totals_snapshot(&c, "bdot10k"),
            "full compare and reconcile+drain must produce identical cell_totals"
        );
    }

    #[test]
    fn full_compare_and_reconcile_drain_agree_on_prg() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO street_name_mappings VALUES
                 (NULL, 'gen. Kruka', 'Generała Kruka');
             INSERT INTO prg_addresses
                 (lokalny_id, numer_porzadkowy, ulica, miejscowosc,
                  wazny_od_lub_data_nadania, geom) VALUES
                 ('matched', '12', NULL, NULL, DATE '2012-04-27', ST_Point(21.010, 52.210)),
                 ('unmatched', '7', NULL, NULL, DATE '2021-03-09', ST_Point(21.050, 52.250)),
                 -- Far outside Poland; PRG's full compare has never had a
                 -- bbox clamp, but this keeps the fixture parallel to the
                 -- bdot10k test above and exercises a far-away point anyway.
                 ('far', '3', NULL, NULL, NULL, ST_Point(30.0, 60.0)),
                 -- The name rules, end to end: each of these is ~133m from its
                 -- OSM neighbour, so rule A cannot decide any of them. Without
                 -- rows that only the name rules can match, reconcile+drain
                 -- would agree with the full compare on a fixture that never
                 -- exercises the branches this test is here to cover.
                 ('by-street', '44', 'Warszawska', NULL, NULL, ST_Point(21.020, 52.2112)),
                 ('wrong-street', '44', 'Polna', NULL, NULL, ST_Point(21.030, 52.2112)),
                 ('by-mapping', '5', 'gen. Kruka', NULL, NULL, ST_Point(21.040, 52.2112)),
                 ('by-place', '7', NULL, 'Rychnowo', NULL, ST_Point(21.070, 52.2112)),
                 ('wrong-place', '7', NULL, 'Inne', NULL, ST_Point(21.080, 52.2112));
             INSERT INTO osm_addresses VALUES
                 (1,'node','12',NULL,NULL,NULL, ST_Point(21.010, 52.2102)),
                 (2,'node','44','Warszawska',NULL,NULL, ST_Point(21.020, 52.210)),
                 (3,'node','44','Warszawska',NULL,NULL, ST_Point(21.030, 52.210)),
                 (4,'node','5','Generała Kruka',NULL,NULL, ST_Point(21.040, 52.210)),
                 (5,'node','7',NULL,'Rychnowo',NULL, ST_Point(21.070, 52.210)),
                 (6,'node','7',NULL,'Rychnowo',NULL, ST_Point(21.080, 52.210));",
        )
        .unwrap();

        compare_prg(&c).unwrap();
        let full = snapshot(
            &c,
            "prg_unmatched",
            "lokalny_id",
            Some("wazny_od_lub_data_nadania"),
        );
        let full_totals = totals_snapshot(&c, "prg");
        assert_eq!(
            full.len(),
            4,
            "sanity: 'unmatched', 'far', 'wrong-street' and 'wrong-place' present; \
             the three name-rule matches and the 50m match excluded"
        );
        // cell_totals is one row per z14 cell, not per address: the eight
        // addresses above fall in six distinct cells (the ones at 21.010/21.020
        // share a cell, as do 21.030/21.040/21.050).
        assert_eq!(
            full_totals.len(),
            6,
            "sanity: every address counts towards its cell's denominator, matched or not"
        );

        c.execute_batch("DELETE FROM prg_unmatched; DELETE FROM cell_totals;")
            .unwrap();
        enqueue_all(&c).unwrap();
        drain_all(&c);
        let incremental = snapshot(
            &c,
            "prg_unmatched",
            "lokalny_id",
            Some("wazny_od_lub_data_nadania"),
        );

        assert_eq!(
            full, incremental,
            "full compare and reconcile+drain must produce row-identical prg_unmatched"
        );
        assert_eq!(
            full_totals,
            totals_snapshot(&c, "prg"),
            "full compare and reconcile+drain must produce identical cell_totals"
        );
    }
}

/// The design lets `match_refresh` run *outside* the dataset-refresh
/// `refresh_lock` (design ~line 230), so the drain and a government refresh can
/// write the same DuckDB instance at the same time. Review traced the overlap
/// by hand -- the only shared table is `match_dirty_cells`, and
/// append-vs-delete-of-different-rows is not a conflict for DuckDB's optimistic
/// CC -- but that was analysis, not evidence. This module is the evidence.
#[cfg(test)]
mod drain_refresh_concurrency {
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use duckdb::Connection;

    use crate::compare::drain::drain_batch;
    use crate::compare::reconcile::enqueue_all;
    use crate::dataset::BDOT10K;
    use crate::db::init_db;
    use crate::update::dataset::refresh;

    /// `n` buildings, one per z14 cell (cells are ~0.022 deg wide at this
    /// latitude, so a 0.03 deg stride guarantees distinct cells).
    ///
    /// `tag` is woven into `wersja`, not chosen arbitrarily: `WERSJA` is one
    /// of `BDOT10K.compared_columns`, and DuckDB identifiers are
    /// case-insensitive, so this lowercase fixture column matches that
    /// uppercase entry. That is what makes a re-stage with a different `tag`
    /// produce a real change under `changed_predicate_sql` on every row --
    /// it works for this specific reason, not incidentally, so renaming this
    /// column would silently turn these tests into no-ops.
    ///
    /// `PRZESTRZENNAZW` is part of BDOT10k's composite key
    /// (`(PRZESTRZENNAZW, LOKALNYID)`) but held constant across all rows,
    /// mirroring production -- the real column has only 16 distinct values
    /// nationally. `LOKALNYID` stays the per-row-varying half of the key.
    fn rows_sql(n: i64, tag: &str) -> String {
        format!(
            "SELECT '04' AS PRZESTRZENNAZW,
                    'b' || i AS LOKALNYID,
                    '{tag}' AS wersja,
                    ST_MakeEnvelope(20.0 + i * 0.03, 52.0,
                                    20.0 + i * 0.03 + 0.002, 52.002) AS geom,
                    NULL::VARCHAR AS PRZEWAZAJACAFUNKCJABUDYNKU,
                    NULL::VARCHAR AS FUNKCJAOGOLNABUDYNKU,
                    NULL::SMALLINT AS LICZBAKONDYGNACJI,
                    'eksploatowany' AS KATEGORIAISTNIENIA,
                    NULL::VARCHAR AS NAZWA,
                    NULL::VARCHAR AS FSBUD,
                    NULL::VARCHAR AS INFORMACJADODATKOWA,
                    NULL::TINYINT AS KODKST,
                    NULL::VARCHAR AS ZRODLODANYCHGEOMETRYCZNYCH
             FROM range({n}) t(i)"
        )
    }

    /// Ids currently served as unmatched, order-independent.
    fn snapshot_ids(c: &Connection) -> std::collections::BTreeSet<String> {
        let mut stmt = c
            .prepare("SELECT LOKALNYID FROM bdot10k_unmatched")
            .unwrap();
        stmt.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    fn conn(n: i64) -> Connection {
        let init = vec![
            "INSTALL spatial".to_string(),
            "LOAD spatial".to_string(),
            "INSTALL icu".to_string(),
            "LOAD icu".to_string(),
            "SET geometry_always_xy = true".to_string(),
        ];
        let c = init_db(Path::new(":memory:"), &init, None).unwrap();
        c.execute_batch(&format!(
            "CREATE TABLE bdot10k_buildings AS {};
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 wazny_od_lub_data_nadania DATE, teryt_gmina VARCHAR, gmina VARCHAR, geom GEOMETRY);",
            BDOT10K.with_centroid_select(&rows_sql(n, "v1"))
        ))
        .unwrap();
        // Match roughly every third building, so the serving table is neither
        // empty nor a copy of the source.
        c.execute_batch(
            "INSERT INTO osm_buildings
             SELECT i, 'way', NULL,
                    ST_MakeEnvelope(20.0 + i * 0.09, 52.0, 20.0 + i * 0.09 + 0.002, 52.002)
             FROM range(100) t(i);",
        )
        .unwrap();
        c
    }

    /// Drive `drain_batch` in a loop on one thread while `refresh()` runs on
    /// another. Asserts: neither side errors, the drain really did overlap
    /// (non-vacuous), and the queue converges once both are done.
    #[test]
    fn drain_and_dataset_refresh_do_not_collide() {
        const N: i64 = 400;
        let conn = conn(N);
        enqueue_all(&conn).unwrap();

        let drain_conn = conn.try_clone().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_drain = stop.clone();
        let drained = Arc::new(AtomicU64::new(0));
        let drained_thread = drained.clone();

        let handle = std::thread::spawn(move || {
            let mut errors: Vec<String> = Vec::new();
            let mut calls: Vec<(u128, u64)> = Vec::new();
            while !stop_drain.load(Ordering::SeqCst) {
                let t = std::time::Instant::now();
                match drain_batch(&drain_conn, 16, &|| false) {
                    Ok(stats) => {
                        calls.push((t.elapsed().as_millis(), stats.cells));
                        drained_thread.fetch_add(stats.cells, Ordering::SeqCst);
                        if stats.failed > 0 {
                            errors.push(format!("{} cells failed to recompute", stats.failed));
                        }
                    }
                    Err(e) => {
                        calls.push((t.elapsed().as_millis(), 0));
                        errors.push(format!("drain_batch errored: {e:#}"));
                    }
                }
            }
            (errors, calls)
        });

        // Several refresh cycles, so the windows genuinely overlap rather than
        // the drain happening to finish first.
        let mut refresh_errors: Vec<String> = Vec::new();
        for tag in [
            "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13",
        ] {
            let rows = rows_sql(N, tag);
            let res = refresh(
                &conn,
                &BDOT10K,
                move |c: &Connection, target: &str| {
                    c.execute_batch(&format!(
                        "CREATE TABLE {target} AS {}",
                        BDOT10K.with_centroid_select(&rows)
                    ))?;
                    Ok(crate::dataset::LoadStats::default())
                },
                None,
                &|| false,
            );
            if let Err(e) = res {
                refresh_errors.push(format!("refresh({tag}) errored: {e:#}"));
            }
        }

        stop.store(true, Ordering::SeqCst);
        let (drain_errors, drain_calls) = handle.join().unwrap();

        assert!(
            refresh_errors.is_empty(),
            "dataset refresh must not abort against a concurrent drain: {refresh_errors:?}"
        );
        assert!(
            drain_errors.is_empty(),
            "drain must not abort against a concurrent dataset refresh: {drain_errors:?}"
        );
        // Non-vacuity: the drain must have completed several batches *during*
        // the refresh window, not squeezed one in at the end. Measured on a
        // dev box this is consistently 6 batches of 16 cells at a steady
        // 276-372 ms each -- no stall, no lock convoy, no starvation -- so a
        // floor of 2 leaves wide margin on a loaded machine while still
        // failing if the drain ever gets serialized behind the refresh.
        let productive = drain_calls.iter().filter(|(_, cells)| *cells > 0).count();
        assert!(
            productive >= 2,
            "drain made {productive} productive batches during {} refreshes -- \
             expected steady progress; calls (ms, cells): {drain_calls:?}",
            12
        );
        assert!(
            drained.load(Ordering::SeqCst) > 0,
            "drain thread never drained a cell -- the test did not exercise the overlap"
        );

        // Whatever interleaving happened, the queue must still converge: drain
        // to empty, then every remaining dirty cell is gone and the serving
        // table holds exactly the unmatched buildings of the final snapshot.
        loop {
            let s = drain_batch(&conn, 1000, &|| false).unwrap();
            assert_eq!(s.failed, 0, "post-run drain reported failed cells");
            if s.cells == 0 {
                break;
            }
        }
        let queued: i64 = conn
            .query_row("SELECT COUNT(*) FROM match_dirty_cells", [], |r| r.get(0))
            .unwrap();
        assert_eq!(queued, 0, "queue must drain to empty");

        let live: i64 = conn
            .query_row("SELECT COUNT(*) FROM bdot10k_buildings", [], |r| r.get(0))
            .unwrap();
        assert_eq!(live, N, "refresh must leave the live table intact");

        let served_from_drain = snapshot_ids(&conn);
        conn.execute_batch("DELETE FROM bdot10k_unmatched;")
            .unwrap();
        crate::compare::buildings::compare_bdot10k(&conn).unwrap();
        let served_from_full = snapshot_ids(&conn);
        assert_eq!(
            served_from_drain, served_from_full,
            "after a concurrent refresh + drain, the serving table must match a full compare"
        );
    }
}

/// A full compare is a clear-then-repopulate, so the failure mode that matters
/// is "the clear committed and the repopulate didn't": the serving table ends
/// up empty and `/tiles` silently answers with zero features. This is not
/// hypothetical -- `bdot10k_unmatched` was emptied exactly this way on the
/// Poland database, by a compare run against a serving table that predated the
/// carried classification columns (`compare::columns`), so the INSERT failed at
/// bind time while the DELETE had already committed.
///
/// Both tests reproduce that shape directly: rebuild the serving table at its
/// pre-carried-columns schema, seed it with a previous comparison's row, and
/// assert the failed compare leaves that row untouched. The error assertion is
/// what pins *where* the failure happened -- a binder error naming the missing
/// column can only come from the INSERT, which is downstream of the DELETE, so
/// a passing test genuinely exercised the rollback rather than bailing early.
#[cfg(test)]
mod clear_and_repopulate_is_atomic {
    use std::path::Path;

    use duckdb::Connection;

    use crate::compare::addresses::compare_prg;
    use crate::compare::buildings::compare_bdot10k;
    use crate::db::init_db;

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
            "CREATE TABLE bdot10k_buildings (PRZESTRZENNAZW VARCHAR, LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR, LICZBAKONDYGNACJI SMALLINT,
                 KATEGORIAISTNIENIA VARCHAR DEFAULT 'eksploatowany',
                 NAZWA VARCHAR, FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 wazny_od_lub_data_nadania DATE, teryt_gmina VARCHAR, gmina VARCHAR, geom GEOMETRY);",
        )
        .unwrap();
        c
    }

    fn count(c: &Connection, table: &str) -> i64 {
        c.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn failed_buildings_compare_leaves_the_previous_contents_intact() {
        let c = conn();
        // The real pre-carried-columns schema: no KATEGORIAISTNIENIA / NAZWA /
        // FSBUD / INFORMACJADODATKOWA / KODKST / ZRODLODANYCHGEOMETRYCZNYCH,
        // which is what `classification_columns` names in its INSERT.
        c.execute_batch(
            "DROP TABLE bdot10k_unmatched;
             CREATE TABLE bdot10k_unmatched (
                 LOKALNYID VARCHAR, geom GEOMETRY, cell_x INTEGER, cell_y INTEGER,
                 computed_at TIMESTAMPTZ, funkcja_szczegolowa VARCHAR,
                 funkcja_ogolna VARCHAR, liczba_kondygnacji SMALLINT);
             INSERT INTO bdot10k_unmatched
                 (LOKALNYID, geom, cell_x, cell_y, computed_at)
             VALUES ('previous_run', ST_Point(21.0, 52.0), 9147, 5411, now());
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('fresh', ST_MakeEnvelope(21.0, 52.0, 21.001, 52.001));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();

        let err = compare_bdot10k(&c).expect_err("compare must fail on the stale serving schema");
        let chain = format!("{err:#}").to_lowercase();
        // DuckDB's binder reports the *first* column of the INSERT list the
        // destination lacks, so this name tracks the head of
        // `classification_columns`'s `dest_names` -- currently PRZESTRZENNAZW,
        // the carried half of BDOT10k's composite key. The specific column is
        // incidental; what the assertion is really pinning is that the failure
        // came from the INSERT rather than from anything earlier, which is what
        // proves the DELETE had already run inside the transaction.
        assert!(
            chain.contains("przestrzennazw"),
            "the failure must come from the INSERT naming a missing carried column \
             (that is what proves the DELETE had already run), got: {chain}"
        );

        assert_eq!(
            count(&c, "bdot10k_unmatched"),
            1,
            "a failed compare must roll back its clear, not leave the serving table empty"
        );
        let surviving: String = c
            .query_row("SELECT LOKALNYID FROM bdot10k_unmatched", [], |r| r.get(0))
            .unwrap();
        assert_eq!(surviving, "previous_run");
    }

    #[test]
    fn failed_address_compare_leaves_the_previous_contents_intact() {
        let c = conn();
        // Same shape, address side: the real stale schema lacked
        // wazny_od_lub_data_nadania, which compare_addresses' INSERT names.
        c.execute_batch(
            "DROP TABLE prg_unmatched;
             CREATE TABLE prg_unmatched (
                 geom GEOMETRY, lokalny_id VARCHAR, numer_porzadkowy VARCHAR,
                 ulica VARCHAR, miejscowosc VARCHAR, kod_pocztowy VARCHAR,
                 teryt_miejscowosc VARCHAR, cell_x INTEGER, cell_y INTEGER,
                 computed_at TIMESTAMPTZ);
             INSERT INTO prg_unmatched
                 (geom, lokalny_id, numer_porzadkowy, cell_x, cell_y, computed_at)
             VALUES (ST_Point(21.0, 52.0), 'previous_run', '1', 9147, 5411, now());
             INSERT INTO prg_addresses (lokalny_id, numer_porzadkowy, geom) VALUES
                 ('fresh', '2', ST_Point(21.0, 52.0));",
        )
        .unwrap();

        let err = compare_prg(&c).expect_err("compare must fail on the stale serving schema");
        let chain = format!("{err:#}").to_lowercase();
        assert!(
            chain.contains("wazny_od_lub_data_nadania"),
            "the failure must come from the INSERT naming the missing column, got: {chain}"
        );

        assert_eq!(
            count(&c, "prg_unmatched"),
            1,
            "a failed compare must roll back its clear, not leave the serving table empty"
        );
        let surviving: String = c
            .query_row("SELECT lokalny_id FROM prg_unmatched", [], |r| r.get(0))
            .unwrap();
        assert_eq!(surviving, "previous_run");
    }
}
