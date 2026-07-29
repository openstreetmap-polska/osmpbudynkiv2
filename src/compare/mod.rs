pub mod addresses;
pub mod buildings;
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
            "CREATE TABLE bdot10k_buildings (LOKALNYID VARCHAR, geom GEOMETRY);
             CREATE TABLE egib_buildings (id_budynku VARCHAR, geom GEOMETRY);
             CREATE TABLE prg_addresses (
                 lokalny_id VARCHAR, numer_porzadkowy VARCHAR, ulica VARCHAR,
                 miejscowosc VARCHAR, kod_pocztowy VARCHAR, teryt_miejscowosc VARCHAR,
                 geom GEOMETRY);",
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

    /// One comparable string per row (id/geometry-as-WKT/cell tags),
    /// deliberately excluding `computed_at` -- the two recompute paths run at
    /// different wall-clock times, so that column is expected to differ.
    fn snapshot(c: &Connection, table: &str, id_col: &str) -> BTreeSet<String> {
        let sql = format!(
            "SELECT {id_col} || '|' || ST_AsText(geom) || '|' ||
                    CAST(cell_x AS VARCHAR) || '|' || CAST(cell_y AS VARCHAR)
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
             INSERT INTO bdot10k_buildings VALUES
                 ('inside', ST_MakeEnvelope(20.0002,52.0002,20.0008,52.0008)),
                 ('lonely', ST_MakeEnvelope(21.0,52.2,21.001,52.201)),
                 -- Outside the old hardcoded (14,49,25,55) compare_buildings
                 -- bbox: the extent-divergence scenario the extent fix
                 -- exists to close, and the scenario this test must be able
                 -- to catch a regression of.
                 ('stray', ST_MakeEnvelope(30.0,60.0,30.001,60.001));",
        )
        .unwrap();

        compare_bdot10k(&c).unwrap();
        let full = snapshot(&c, "bdot10k_unmatched", "LOKALNYID");
        assert_eq!(
            full.len(),
            2,
            "sanity: 'inside' matched, 'lonely' and 'stray' unmatched"
        );

        c.execute_batch("DELETE FROM bdot10k_unmatched;").unwrap();
        enqueue_all(&c).unwrap();
        drain_all(&c);
        let incremental = snapshot(&c, "bdot10k_unmatched", "LOKALNYID");

        assert_eq!(
            full, incremental,
            "full compare and reconcile+drain must produce row-identical bdot10k_unmatched"
        );
    }

    #[test]
    fn full_compare_and_reconcile_drain_agree_on_prg() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO prg_addresses (lokalny_id, numer_porzadkowy, geom) VALUES
                 ('matched', '12', ST_Point(21.010, 52.210)),
                 ('unmatched', '7', ST_Point(21.050, 52.250)),
                 -- Far outside Poland; PRG's full compare has never had a
                 -- bbox clamp, but this keeps the fixture parallel to the
                 -- bdot10k test above and exercises a far-away point anyway.
                 ('far', '3', ST_Point(30.0, 60.0));
             INSERT INTO osm_addresses VALUES
                 (1,'node','12',NULL,NULL,NULL, ST_Point(21.010, 52.2102));",
        )
        .unwrap();

        compare_prg(&c).unwrap();
        let full = snapshot(&c, "prg_unmatched", "lokalny_id");
        assert_eq!(
            full.len(),
            2,
            "sanity: 'matched' excluded, 'unmatched' and 'far' present"
        );

        c.execute_batch("DELETE FROM prg_unmatched;").unwrap();
        enqueue_all(&c).unwrap();
        drain_all(&c);
        let incremental = snapshot(&c, "prg_unmatched", "lokalny_id");

        assert_eq!(
            full, incremental,
            "full compare and reconcile+drain must produce row-identical prg_unmatched"
        );
    }
}
