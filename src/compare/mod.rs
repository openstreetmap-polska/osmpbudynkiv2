pub mod addresses;
pub mod buildings;
pub mod columns;
pub mod drain;
pub mod incremental;
pub mod reconcile;
pub mod rule;

use anyhow::Result;
use duckdb::Connection;
use tracing::info;

use crate::cli::{AddressesSource, BuildingsSource, CompareTarget};

pub fn run(conn: &Connection, target: CompareTarget) -> Result<()> {
    match target {
        CompareTarget::Buildings { source } => match source {
            None | Some(BuildingsSource::All) => {
                buildings::compare_bdot10k(conn)?;
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
            buildings::compare_egib(conn)?;
            addresses::compare_prg(conn)?;
        }
        CompareTarget::Reconcile => {
            let enqueued = reconcile::enqueue_all(conn)?;
            info!(enqueued, "reconcile sweep complete");
        }
    }
    Ok(())
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
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 PRZEWAZAJACAFUNKCJABUDYNKU VARCHAR, FUNKCJAOGOLNABUDYNKU VARCHAR, LICZBAKONDYGNACJI SMALLINT,
                 KATEGORIAISTNIENIA VARCHAR DEFAULT 'eksploatowany',
                 NAZWA VARCHAR, FSBUD VARCHAR, INFORMACJADODATKOWA VARCHAR, KODKST TINYINT,
                 ZRODLODANYCHGEOMETRYCZNYCH VARCHAR);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY, centroid GEOMETRY,
                 kondygnacje_nadziemne INTEGER, kondygnacje_podziemne INTEGER, rodzaj VARCHAR);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 wazny_od_lub_data_nadania DATE, geom GEOMETRY);",
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

    /// One comparable string per row (id/geometry-as-WKT/cell tags, plus
    /// `extra_col` if given -- used to also pin a carried column between the
    /// full-compare and incremental paths, closing the blind spot this
    /// function otherwise has for anything beyond id/geom/cell tags),
    /// deliberately excluding `computed_at` -- the two recompute paths run at
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

    #[test]
    fn full_compare_and_reconcile_drain_agree_on_bdot10k() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES
                 (1, 'way', NULL, ST_MakeEnvelope(20.0, 52.0, 20.001, 52.001));
             INSERT INTO bdot10k_buildings (LOKALNYID, geom) VALUES
                 ('inside', ST_MakeEnvelope(20.0002,52.0002,20.0008,52.0008)),
                 ('lonely', ST_MakeEnvelope(21.0,52.2,21.001,52.201)),
                 -- Outside the old hardcoded (14,49,25,55) compare_buildings
                 -- bbox: the extent-divergence scenario the extent fix
                 -- exists to close, and the scenario this test must be able
                 -- to catch a regression of.
                 ('stray', ST_MakeEnvelope(30.0,60.0,30.001,60.001));
             UPDATE bdot10k_buildings SET centroid = ST_Centroid(geom);",
        )
        .unwrap();

        compare_bdot10k(&c).unwrap();
        let full = snapshot(&c, "bdot10k_unmatched", "LOKALNYID", None);
        assert_eq!(
            full.len(),
            2,
            "sanity: 'inside' matched, 'lonely' and 'stray' unmatched"
        );

        c.execute_batch("DELETE FROM bdot10k_unmatched;").unwrap();
        enqueue_all(&c).unwrap();
        drain_all(&c);
        let incremental = snapshot(&c, "bdot10k_unmatched", "LOKALNYID", None);

        assert_eq!(
            full, incremental,
            "full compare and reconcile+drain must produce row-identical bdot10k_unmatched"
        );
    }

    #[test]
    fn full_compare_and_reconcile_drain_agree_on_prg() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO prg_addresses (lokalny_id, numer_porzadkowy, wazny_od_lub_data_nadania, geom) VALUES
                 ('matched', '12', DATE '2012-04-27', ST_Point(21.010, 52.210)),
                 ('unmatched', '7', DATE '2021-03-09', ST_Point(21.050, 52.250)),
                 -- Far outside Poland; PRG's full compare has never had a
                 -- bbox clamp, but this keeps the fixture parallel to the
                 -- bdot10k test above and exercises a far-away point anyway.
                 ('far', '3', NULL, ST_Point(30.0, 60.0));
             INSERT INTO osm_addresses VALUES
                 (1,'node','12',NULL,NULL,NULL, ST_Point(21.010, 52.2102));",
        )
        .unwrap();

        compare_prg(&c).unwrap();
        let full = snapshot(
            &c,
            "prg_unmatched",
            "lokalny_id",
            Some("wazny_od_lub_data_nadania"),
        );
        assert_eq!(
            full.len(),
            2,
            "sanity: 'matched' excluded, 'unmatched' and 'far' present"
        );

        c.execute_batch("DELETE FROM prg_unmatched;").unwrap();
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
    use crate::dataset::{BDOT10K, hashed_select};
    use crate::db::init_db;
    use crate::update::dataset::refresh;

    /// `n` buildings, one per z14 cell (cells are ~0.022 deg wide at this
    /// latitude, so a 0.03 deg stride guarantees distinct cells), with `tag`
    /// woven into the id-independent column so a re-stage produces a
    /// whole-row-hash change on every row.
    fn rows_sql(n: i64, tag: &str) -> String {
        format!(
            "SELECT 'b' || i AS LOKALNYID,
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
                 wazny_od_lub_data_nadania DATE, geom GEOMETRY);",
            BDOT10K.with_centroid_select(&hashed_select(&rows_sql(n, "v1")))
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
                        BDOT10K.with_centroid_select(&hashed_select(&rows))
                    ))?;
                    Ok(crate::dataset::LoadStats::default())
                },
                None,
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
