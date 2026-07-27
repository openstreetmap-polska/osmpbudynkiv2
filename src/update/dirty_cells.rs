use std::collections::HashSet;

use anyhow::{Context, Result};
use duckdb::Connection;

use crate::tile_math::{CHANGE_CELL_ZOOM, lonlat_to_tile};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Layer {
    Buildings,
    Addresses,
}

#[derive(Default)]
pub struct DirtyCells {
    buildings: HashSet<(i32, i32)>,
    addresses: HashSet<(i32, i32)>,
}

impl DirtyCells {
    pub fn new() -> Self {
        Self::default()
    }

    fn set(&mut self, layer: Layer) -> &mut HashSet<(i32, i32)> {
        match layer {
            Layer::Buildings => &mut self.buildings,
            Layer::Addresses => &mut self.addresses,
        }
    }

    /// Record the cell of a known point (node fast path — no query).
    pub fn note_point(&mut self, layer: Layer, lon: f64, lat: f64) {
        if lon.is_finite() && lat.is_finite() {
            let (x, y) = lonlat_to_tile(lon, lat, CHANGE_CELL_ZOOM);
            self.set(layer).insert((x as i32, y as i32));
        }
    }

    /// Record the cell of a row currently in `table` for (osm_id, osm_type).
    /// A no-op when the row is absent (nothing to leave from).
    pub fn note_existing(
        &mut self,
        conn: &Connection,
        layer: Layer,
        table: &str,
        osm_id: i64,
        osm_type: &str,
    ) -> Result<()> {
        // ST_X/ST_Y only accept POINT geometry; osm_buildings holds polygons,
        // so route through ST_Centroid (a no-op for the points already stored
        // in osm_addresses) to get a single representative point per row —
        // same convention as DatasetSpec::representative_point_sql.
        let cx = crate::tile_math::cell_x_sql("ST_Centroid(geom)");
        let cy = crate::tile_math::cell_y_sql("ST_Centroid(geom)");
        let sql = format!(
            "SELECT {cx}, {cy} FROM {table}
             WHERE osm_id = ? AND osm_type = ? AND geom IS NOT NULL"
        );
        let mut stmt = conn
            .prepare(&sql)
            .with_context(|| format!("note_existing prepare {table}"))?;
        let rows = stmt.query_map(duckdb::params![osm_id, osm_type], |r| {
            Ok((r.get::<_, i32>(0)?, r.get::<_, i32>(1)?))
        })?;
        for row in rows {
            let (x, y) = row?;
            self.set(layer).insert((x, y));
        }
        Ok(())
    }

    /// Insert the 3×3 neighbourhood of every recorded cell into
    /// match_dirty_cells: buildings → bdot10k+egib, addresses → prg.
    pub fn flush(&self, conn: &Connection) -> Result<()> {
        let z = CHANGE_CELL_ZOOM as i32;
        let mut stmt = conn.prepare("INSERT INTO match_dirty_cells VALUES (?, ?, ?, ?, now())")?;
        let mut insert = |source: &str, x: i32, y: i32| -> Result<()> {
            for dx in -1..=1 {
                for dy in -1..=1 {
                    stmt.execute(duckdb::params![source, z, x + dx, y + dy])?;
                }
            }
            Ok(())
        };
        for &(x, y) in &self.buildings {
            insert("bdot10k", x, y)?;
            insert("egib", x, y)?;
        }
        for &(x, y) in &self.addresses {
            insert("prg", x, y)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db;
    use std::path::Path;

    fn conn() -> Connection {
        let init = vec!["INSTALL spatial".to_string(), "LOAD spatial".to_string()];
        init_db(Path::new(":memory:"), &init, None).unwrap()
    }

    #[test]
    fn flush_expands_3x3_and_fans_out_by_layer() {
        let c = conn();
        let mut d = DirtyCells::default();
        d.note_point(Layer::Buildings, 21.0, 52.0);
        d.flush(&c).unwrap();

        // Buildings fan out to bdot10k + egib; 3x3 => 9 cells each.
        let (bx, by) = lonlat_to_tile(21.0, 52.0, CHANGE_CELL_ZOOM);
        let bdot: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source='bdot10k'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let egib: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source='egib'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!((bdot, egib), (9, 9));
        // Center cell present.
        let center: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source='bdot10k' AND cell_x=? AND cell_y=?",
                duckdb::params![bx as i32, by as i32],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(center, 1);
        // Addresses untouched.
        let prg: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source='prg'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(prg, 0);
    }

    #[test]
    fn note_existing_reads_geom_cell_from_table() {
        let c = conn();
        c.execute_batch(
            "INSERT INTO osm_buildings VALUES (5,'way',NULL, ST_MakeEnvelope(21.0,52.0,21.001,52.001));",
        )
        .unwrap();
        let mut d = DirtyCells::default();
        d.note_existing(&c, Layer::Buildings, "osm_buildings", 5, "way")
            .unwrap();
        d.flush(&c).unwrap();
        let (bx, by) = lonlat_to_tile(21.0005, 52.0005, CHANGE_CELL_ZOOM);
        let center: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM match_dirty_cells WHERE source='bdot10k' AND cell_x=? AND cell_y=?",
                duckdb::params![bx as i32, by as i32],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(center, 1);
    }
}
