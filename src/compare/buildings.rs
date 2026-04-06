use anyhow::Result;
use duckdb::Connection;

pub fn compare_bdot10k(conn: &Connection) -> Result<()> {
    let _ = conn;
    anyhow::bail!("BDOT10k comparison not yet implemented")
}

pub fn compare_egib(conn: &Connection) -> Result<()> {
    let _ = conn;
    anyhow::bail!("EGIB comparison not yet implemented")
}
