# Import: implement `import full`

## Motivation

`ImportSource::Full` already exists in the CLI enum and README (`import full` is
documented as "not yet implemented"), but `import::run` currently `bail!`s on it. Users
must chain four separate commands (`import osm`, `import bdot10k`, `import egib`,
`import prg`) to bootstrap a fresh database. This spec implements the stub.

## Scope

- Implement `ImportSource::Full`: run OSM, BDOT10k, EGIB, PRG imports in sequence against
  the same connection/config, fail-fast on the first error.
- Give `Full` the same optional `--file`-style overrides the individual subcommands have,
  so it can run fully offline (using local files) and be integration-tested without
  network access.
- Add an integration test using the existing fixtures.
- Update `CLAUDE.md` and `README.md` to reflect the implemented command.

Out of scope: any change to the individual `import osm` / `import bdot10k` / `import egib`
/ `import prg` commands, download logic, or the row-hash versioning mechanism itself.

## Design

### `src/cli.rs`

`ImportSource::Full` becomes a struct variant carrying one optional file-path flag per
source, named to avoid collision with the other variants' `--file`:

```rust
/// Run all imports in sequence (OSM, BDOT10k, EGIB, PRG)
Full {
    /// Path to local OSM PBF file (skips download)
    #[arg(long)]
    osm_file: Option<PathBuf>,
    /// Path to local BDOT10k file (skips download)
    #[arg(long)]
    bdot10k_file: Option<PathBuf>,
    /// Path to local EGIB file (skips download)
    #[arg(long)]
    egib_file: Option<PathBuf>,
    /// Path to local PRG file (skips download)
    #[arg(long)]
    prg_file: Option<PathBuf>,
    /// Path to a TERC (TERYT) dictionary file (.zip or .xml), for the PRG import
    #[arg(long)]
    terc_file: Option<PathBuf>,
},
```

Each flag is independent: any subset may be given, and any source without its flag falls
back to downloading via `config.download_urls` exactly as the individual commands do.

### `src/import/mod.rs`

Runs the four imports in order — OSM, BDOT10k, EGIB, PRG (matches the README's existing
example and the enum's declaration order) — propagating errors with `?` so the first
failure stops the remaining sources (mirrors `CompareTarget::Full` in
`src/compare/mod.rs`, which uses the same fail-fast pattern):

```rust
ImportSource::Full {
    osm_file,
    bdot10k_file,
    egib_file,
    prg_file,
    terc_file,
} => {
    osm::import(conn, kv, config, osm_file.as_deref(), &urls.osm_pbf)?;
    bdot10k::import(conn, config, bdot10k_file.as_deref(), &urls.bdot10k)?;
    egib::import(conn, config, egib_file.as_deref(), &urls.egib)?;
    prg::import(
        conn,
        config,
        prg_file.as_deref(),
        terc_file.as_deref(),
        &urls.prg,
    )?;
    stamp_row_hash_version(conn)
}
```

`stamp_row_hash_version` is called once at the end, not after each government-dataset
import. It writes a single global `metadata.row_hash_version` key (see
`dataset::ROW_HASH_VERSION_KEY`), so calling it three times in a row would just overwrite
the same value redundantly. OSM stays exempt, as it is in the individual `Osm` arm.

### `tests/cli_import_full.rs`

New file, modeled on the existing per-source CLI tests (`tests/cli_import_*.rs`):

- `test_import_full_from_fixtures` — runs `import full` with all five flags pointed at
  `fixtures/osm.pbf`, `fixtures/bdot10k.parquet`, `fixtures/egib.parquet`,
  `fixtures/prg.zip`, `fixtures/teryt.zip` against a `:memory:` DB. Asserts success and
  that stdout contains all four sources' existing completion markers ("OSM import
  complete", "BDOT10k import complete", "EGIB import complete", and PRG's own marker —
  checked against `tests/cli_import_prg.rs` for the exact string).
- `test_import_full_stops_on_first_failure` — points `--osm-file` at a nonexistent path
  and asserts the command fails. Since OSM runs first, this also confirms the later
  sources never get a chance to run (nothing in the DB to import into means no ambiguity
  here — the assertion is just command failure, matching how `test_import_osm_missing_file`
  already checks the equivalent case for the individual command).

### Docs

- `CLAUDE.md`: no dedicated `import full` mention exists there today (the CLI command
  list is described narratively); no change needed beyond what's already accurate.
- `README.md`: flip the roadmap line (`- [ ] import full ...`) to `- [x]`, and replace the
  `# Import everything ... (not yet implemented)` example under "CLI commands" → "import"
  with a real example showing the new flags.

## Risks

None meaningful. Additive CLI surface on an existing stub variant; no change to any
already-implemented import path's behavior or signature.
