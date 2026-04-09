# Compare: add `full` and `buildings all` options

## Motivation

The `compare` CLI currently exposes only `compare buildings [bdot10k|egib]`. There is no
single invocation for "run every comparison" — users must remember and chain individual
targets. This asymmetry will worsen as new comparison targets (e.g. addresses from PRG)
are added.

`import` already solves the equivalent problem with `import full`. This spec mirrors that
pattern at the `compare` level, and also adds an explicit `all` sibling under
`compare buildings` so every source choice is spelled out on the CLI.

## Scope

- Add `CompareTarget::Full` — invoked as `compare full`, runs every comparison that exists
  today. Currently fans out to the same work as `compare buildings` (both BDOT10k and EGIB).
- Add `BuildingsSource::All` — invoked as `compare buildings all`, an explicit synonym for
  `compare buildings` (no source). Behavior: run both BDOT10k and EGIB.
- Document both additions in `CLAUDE.md` under the CLI commands section.
- Add an integration test covering `compare full`.

Out of scope: new comparison logic, new target types, any behavioral change to
`compare buildings bdot10k` / `compare buildings egib`.

## Naming

The top-level variant is `Full` (matching `ImportSource::Full`). The buildings-level
variant is `All` (matching the user's request). The two names diverge deliberately: `Full`
is the existing project convention for "run every step at this level," and `All` is what
the user asked for at the buildings level. Documenting both in help text prevents
confusion.

## Design

### `src/cli.rs`

```rust
pub enum CompareTarget {
    /// Compare building datasets against OSM buildings
    Buildings { source: Option<BuildingsSource> },
    /// Run all available comparisons
    Full,
}

pub enum BuildingsSource {
    /// Compare only BDOT10k buildings against OSM
    Bdot10k,
    /// Compare only EGIB buildings against OSM
    Egib,
    /// Compare all building sources against OSM
    All,
}
```

### `src/compare/mod.rs`

`CompareTarget::Full` dispatches to the same work that `compare buildings` (no source)
does today: `compare_bdot10k` + `compare_egib`. `BuildingsSource::All` is handled by
reusing the existing `None` branch — either via fall-through match or by treating
`Some(All)` and `None` identically.

When new comparison targets are added later, `Full` must be updated to fan out to them.
A comment in the match arm should call this out.

### `tests/cli_compare_buildings.rs`

Add `test_compare_full` that runs `compare full` after importing fixtures and asserts
both "BDOT10k comparison complete" and "EGIB comparison complete" appear in stdout.
Also add `test_compare_buildings_all` that runs `compare buildings all` and asserts the
same.

### `CLAUDE.md`

Update the `compare <target>` bullet in the CLI commands section to mention the new
options. One line, no new section.

## Risks

None meaningful. Pure additive CLI surface; existing commands are untouched.
