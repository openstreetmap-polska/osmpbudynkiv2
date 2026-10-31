//! PRG axis order, end to end through the real CLI.
//!
//! GUGiK's 2021-schema files changed on 2026-10-01 from
//! `srsName="EPSG:2180"` with easting first to
//! `srsName="urn:ogc:def:crs:EPSG::2180"` with northing first. Two things are
//! pinned here:
//!
//! 1. Both layouts load onto the very same positions, so moving from one to
//!    the other is not a change. Read wrongly it would be every row modified:
//!    a national rewrite, and every active PRG report expired.
//! 2. A file whose declaration contradicts its numbers fails the load before
//!    anything is replaced, and `--coordinate-order` / `[prg]
//!    coordinate_order` loads it.
//!
//! The checked-in fixture is lubuskie, which mirrors onto Polish territory, so
//! a mirrored copy of *it* passes the extent check by design (see
//! `import::prg_axis`). The failing cases therefore move every point to the
//! east of the country, where a mirrored point lands in the Baltic.
//!
//! Nothing is added to `fixtures/`: the variants are rewritten from
//! `fixtures/prg.zip` into a temp dir per test.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use assert_cmd::Command;

const SRS_SHORT: &str = r#"srsName="EPSG:2180""#;
const SRS_URN: &str = r#"srsName="urn:ogc:def:crs:EPSG::2180""#;

/// Near Hrubieszów (23.9°E, 50.8°N), as EPSG:2180 easting and northing.
const EAST_EASTING: &str = "850000";
const EAST_NORTHING: &str = "330000";

// Mirrors of `import::prg_axis`'s constants: an integration test of a binary
// crate cannot import them.
const BAIL_MESSAGE: &str = "PRG coordinates do not fit Poland's extent";
const EXCHANGED_FITS: &str = "the file was read in the wrong axis order";

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("osmpbudynkiv2").unwrap();
    cmd.env("NO_COLOR", "1");
    cmd
}

struct Env {
    cfg: tempfile::NamedTempFile,
    dir: tempfile::TempDir,
    db_path: PathBuf,
}

/// A file-backed database (import and update are separate processes), with
/// `extra` appended to the config.
fn env_with(extra: &str) -> Env {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("test.duckdb");
    let mut cfg = tempfile::NamedTempFile::new().unwrap();
    write!(
        cfg,
        "db_path = \"{}\"\nrocksdb_path = \"{}\"\n{extra}",
        db_path.display(),
        dir.path().join("test.rocksdb").display()
    )
    .unwrap();
    Env { cfg, dir, db_path }
}

fn env() -> Env {
    env_with("")
}

fn run(env: &Env, command: &str, file: &Path, extra: &[&str]) -> assert_cmd::assert::Assert {
    cmd()
        .args(["--config", env.cfg.path().to_str().unwrap(), command, "prg"])
        .args(["--file", file.to_str().unwrap()])
        .args(["--terc-file", "fixtures/teryt.zip"])
        .args(extra)
        .assert()
}

fn open(db_path: &Path) -> duckdb::Connection {
    let conn = duckdb::Connection::open(db_path).unwrap();
    conn.execute_batch("INSTALL spatial; LOAD spatial;")
        .unwrap();
    conn
}

/// `fixtures/prg.zip` with `rewrite` applied to every `.gml` entry, written
/// to `dir/name`. Other entries are copied as they are.
fn rewritten_zip(dir: &Path, name: &str, rewrite: impl Fn(&str) -> String) -> PathBuf {
    let mut archive =
        zip::ZipArchive::new(std::fs::File::open("fixtures/prg.zip").unwrap()).unwrap();
    let path = dir.join(name);
    let mut writer = zip::ZipWriter::new(std::fs::File::create(&path).unwrap());
    let mut rewrote = 0;
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).unwrap();
        let entry_name = entry.name().unwrap().to_string();
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).unwrap();
        if entry_name.to_ascii_lowercase().ends_with(".gml") {
            bytes = rewrite(std::str::from_utf8(&bytes).unwrap()).into_bytes();
            rewrote += 1;
        }
        writer
            .start_file(entry_name, zip::write::SimpleFileOptions::default())
            .unwrap();
        writer.write_all(&bytes).unwrap();
    }
    writer.finish().unwrap();
    assert_eq!(rewrote, 1, "the fixture has exactly one .gml entry");
    path
}

/// `gml` with the body of every `<gml:pos>` replaced by `f(first, second)`.
fn map_gml_pos(gml: &str, f: impl Fn(&str, &str) -> String) -> String {
    const OPEN: &str = "<gml:pos>";
    let mut out = String::with_capacity(gml.len());
    let mut rest = gml;
    let mut mapped = 0;
    while let Some(start) = rest.find(OPEN) {
        let body_start = start + OPEN.len();
        let body_end = body_start + rest[body_start..].find("</gml:pos>").unwrap();
        out.push_str(&rest[..body_start]);
        let mut numbers = rest[body_start..body_end].split_whitespace();
        out.push_str(&f(numbers.next().unwrap(), numbers.next().unwrap()));
        rest = &rest[body_end..];
        mapped += 1;
    }
    out.push_str(rest);
    assert!(mapped > 0, "no <gml:pos> found");
    out
}

/// The fixture as GUGiK publishes since 2026-10-01: URN `srsName`, the two
/// numbers of every point exchanged. Same places, other layout.
fn october_layout(gml: &str) -> String {
    assert!(
        gml.contains(SRS_SHORT),
        "the fixture is the pre-October layout"
    );
    map_gml_pos(&gml.replace(SRS_SHORT, SRS_URN), |first, second| {
        format!("{second} {first}")
    })
}

/// Every point moved to the east of Poland and written easting first, under
/// the `srsName` given. Easting first is what the short form declares, so
/// with [`SRS_URN`] this is a file that declares one order and uses the other.
fn eastern_easting_first(gml: &str, srs_name: &str) -> String {
    map_gml_pos(&gml.replace(SRS_SHORT, srs_name), |_, _| {
        format!("{EAST_EASTING} {EAST_NORTHING}")
    })
}

/// Every stored address with its position, as bits.
fn positions(db_path: &Path) -> Vec<(String, u64, u64)> {
    let conn = open(db_path);
    let mut stmt = conn
        .prepare("SELECT lokalny_id, ST_X(geom), ST_Y(geom) FROM prg_addresses ORDER BY lokalny_id")
        .unwrap();
    stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, f64>(1)?.to_bits(),
            r.get::<_, f64>(2)?.to_bits(),
        ))
    })
    .unwrap()
    .collect::<duckdb::Result<_>>()
    .unwrap()
}

fn table_names(db_path: &Path) -> Vec<String> {
    let conn = open(db_path);
    let mut stmt = conn
        .prepare("SELECT table_name FROM duckdb_tables() ORDER BY table_name")
        .unwrap();
    stmt.query_map([], |r| r.get(0))
        .unwrap()
        .collect::<duckdb::Result<_>>()
        .unwrap()
}

fn count(db_path: &Path, table: &str) -> i64 {
    open(db_path)
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

#[test]
fn updating_to_the_october_layout_changes_nothing() {
    let env = env();
    let october = rewritten_zip(env.dir.path(), "october.zip", october_layout);
    run(&env, "import", Path::new("fixtures/prg.zip"), &[]).success();
    let before = positions(&env.db_path);
    assert!(!before.is_empty());

    run(&env, "update", &october, &[]).success();

    let (added, modified, removed): (i32, i32, i32) = open(&env.db_path)
        .query_row(
            "SELECT added, modified, removed FROM dataset_refreshes WHERE source = 'prg'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (added, modified, removed),
        (0, 0, 0),
        "the October layout is the same data; a non-zero `modified` means the \
         axis order was read wrongly or the positions were recomputed"
    );
    assert_eq!(positions(&env.db_path), before);
    for table in [
        "dataset_change_areas",
        "match_dirty_cells",
        "tile_dirty_cells",
    ] {
        assert_eq!(count(&env.db_path, table), 0, "{table} must stay empty");
    }
}

#[test]
fn importing_the_october_layout_stores_the_same_positions() {
    let old = env();
    run(&old, "import", Path::new("fixtures/prg.zip"), &[]).success();

    let new = env();
    let october = rewritten_zip(new.dir.path(), "october.zip", october_layout);
    run(&new, "import", &october, &[]).success();

    let expected = positions(&old.db_path);
    assert!(!expected.is_empty());
    assert_eq!(positions(&new.db_path), expected);
}

/// The control for the two failing tests below: the same eastern points under
/// the declaration that matches them load fine, so what fails there is the
/// contradiction and not the rewritten file.
#[test]
fn eastern_points_under_the_right_declaration_load() {
    let env = env();
    let zip = rewritten_zip(env.dir.path(), "east.zip", |gml| {
        eastern_easting_first(gml, SRS_SHORT)
    });
    run(&env, "import", &zip, &[]).success();

    let (lon, lat): (f64, f64) = open(&env.db_path)
        .query_row(
            "SELECT min(ST_X(geom)), min(ST_Y(geom)) FROM prg_addresses",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert!((23.0..24.2).contains(&lon), "lon {lon}");
    assert!((50.0..51.5).contains(&lat), "lat {lat}");
}

#[test]
fn an_update_to_a_misdeclared_file_fails_and_leaves_the_live_table_alone() {
    let env = env();
    let misdeclared = rewritten_zip(env.dir.path(), "misdeclared.zip", |gml| {
        eastern_easting_first(gml, SRS_URN)
    });
    run(&env, "import", Path::new("fixtures/prg.zip"), &[]).success();
    let before = positions(&env.db_path);
    let tables_before = table_names(&env.db_path);

    let stderr = String::from_utf8(
        run(&env, "update", &misdeclared, &[])
            .failure()
            .get_output()
            .stderr
            .clone(),
    )
    .unwrap();
    assert!(stderr.contains(BAIL_MESSAGE), "{stderr}");
    assert!(stderr.contains(EXCHANGED_FITS), "{stderr}");

    assert_eq!(positions(&env.db_path), before);
    assert_eq!(
        table_names(&env.db_path),
        tables_before,
        "no staging or raw table may survive a rejected load"
    );
    assert_eq!(count(&env.db_path, "dataset_refreshes"), 0);

    let (outcome, message): (String, String) = open(&env.db_path)
        .query_row(
            "SELECT outcome, message FROM job_run_log WHERE job_name = 'update:prg'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(outcome, "Error");
    assert!(message.contains(BAIL_MESSAGE), "{message}");
}

#[test]
fn an_import_of_a_misdeclared_file_fails_and_keeps_the_existing_table() {
    let env = env();
    let misdeclared = rewritten_zip(env.dir.path(), "misdeclared.zip", |gml| {
        eastern_easting_first(gml, SRS_URN)
    });
    run(&env, "import", Path::new("fixtures/prg.zip"), &[]).success();
    let before = positions(&env.db_path);
    let tables_before = table_names(&env.db_path);

    let stderr = String::from_utf8(
        run(&env, "import", &misdeclared, &[])
            .failure()
            .get_output()
            .stderr
            .clone(),
    )
    .unwrap();
    assert!(stderr.contains(BAIL_MESSAGE), "{stderr}");

    assert_eq!(positions(&env.db_path), before);
    assert_eq!(table_names(&env.db_path), tables_before);
}

#[test]
fn forcing_the_order_loads_a_misdeclared_file() {
    let east = env();
    let declared_right = rewritten_zip(east.dir.path(), "east.zip", |gml| {
        eastern_easting_first(gml, SRS_SHORT)
    });
    run(&east, "import", &declared_right, &[]).success();
    let expected = positions(&east.db_path);

    // By the CLI flag...
    let by_flag = env();
    let misdeclared = rewritten_zip(by_flag.dir.path(), "misdeclared.zip", |gml| {
        eastern_easting_first(gml, SRS_URN)
    });
    run(
        &by_flag,
        "import",
        &misdeclared,
        &["--coordinate-order", "xy"],
    )
    .success();
    assert_eq!(positions(&by_flag.db_path), expected);

    // ...and by the config, which is all the scheduled `prg_update` job has.
    let by_config = env_with("[prg]\ncoordinate_order = \"xy\"\n");
    run(&by_config, "import", &misdeclared, &[]).success();
    assert_eq!(positions(&by_config.db_path), expected);

    // The flag wins over the config: forcing the declared (wrong) order back
    // fails again.
    run(
        &by_config,
        "update",
        &misdeclared,
        &["--coordinate-order", "yx"],
    )
    .failure();
    run(
        &by_config,
        "update",
        &misdeclared,
        &["--coordinate-order", "auto"],
    )
    .failure();
    run(&by_config, "update", &misdeclared, &[]).success();
}
