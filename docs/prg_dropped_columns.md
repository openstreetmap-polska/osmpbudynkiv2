# PRG dropped columns

`import::prg::materialize_into` projects an explicit column list from PRG's raw 2021 GML schema (24
columns, `prg_convert::common::SCHEMA_CSV`) instead of storing everything the publisher ships. This is the
record of what was dropped, why, and what it would take to bring a column back deliberately — see
`docs/superpowers/plans/2026-08-14-column-trimming.md` for the full audit (BDOT10k and EGIB included) and
the storage measurements behind it.

Restoring any column below is a one-line addition to the explicit projection in `materialize_into`
(`src/import/prg.rs`) plus a re-import of PRG (`import prg`) — there is no schema migration path, and none
is needed; `prg_addresses` is rebuilt wholesale by `import`, and `update prg`'s staging load funnels
through the same function.

| column | what it holds | why dropped | restore value |
|---|---|---|---|
| `przestrzen_nazw` | INSPIRE namespace, `PL.PZGIK.200` | **exactly 1 distinct value** in all five 2026 snapshots; not part of PRG's key for that reason | none |
| `wersja_id` | record version id | unread by serving, and useless as a change signal (34–147× more version bumps than real content changes, see the plan) — **consumed at import only as the dedup ordering column**, then dropped via `dataset::drop_ordering_column` immediately after `deduplicate_by_key` runs | low |
| `poczatek_wersji_obiektu` | timestamp this version began | unread; moves in exact lockstep with `wersja_id` (0 disagreements in 8.6M rows), so it is a second version-metadata column, not content | none |
| `wazny_do` | validity end date | **0 non-null rows in all five snapshots** (2026-01-10 … 2026-08-14) | none while empty; **re-check if PRG starts populating it** — see the rider below |
| `status` | record status | **0 non-null rows in all five snapshots** | same as above |
| `teryt_wojewodztwo` | voivodeship TERYT code | unread | low |
| `wojewodztwo` | voivodeship name (TERC-derived) | unread by serving; TERC-mapping coverage moved to `import::prg::tests::stream_gml_into_applies_the_terc_mapping_before_materialize_projects_it_away`, which inspects the raw table `stream_gml_into` produces, before `materialize_into` ever drops this column | low |
| `teryt_powiat` | county TERYT code | unread | low |
| `powiat` | county name (TERC-derived) | unread | low |
| `czesc_miejscowosci` | locality-part name | **0 non-null rows in all five snapshots — the column is entirely empty** | none |
| `teryt_ulica` | street ULIC code | unread, though populated for 64% of records and genuinely changing (3,895 and 9,075 in the two long snapshot pairs) | **highest** — the obvious source for an `addr:street:ulic` tag, and a stabler street join key than the name; see the rider below |
| `x_epsg_2180` | easting, PUWG 1992 | redundant with `geom` | none |
| `y_epsg_2180` | northing, PUWG 1992 | redundant with `geom` | none |
| `dlugosc_geograficzna` | longitude | consumed in the same SELECT to build `geom`, then not projected further | none |
| `szerokosc_geograficzna` | latitude | same | none |

## Riders

- **`teryt_ulica`** — restoring it for an `addr:street:ulic` tag means also adding it to
  `DatasetSpec::PRG.compared_columns` (`src/dataset.rs`) in the same change. A served column that is not
  compared silently serves stale values on a record that changed but wasn't detected as changed.
- **`wazny_do` / `status`** — if PRG starts populating either, that is not merely a compared-column
  question. A record with an end date is an expired address that should not be proposed for import at
  all, which is a change to `compare::addresses`'s match rule, not to change detection.

## Kept despite having no current reader

`teryt_gmina` and `gmina` are retained in `prg_addresses` even though nothing currently reads them — the
same "kept but not compared" shape as `wazny_od_lub_data_nadania`, which *is* read (by `compare::addresses`,
`compare::incremental`, and `/tiles`' `addresses_all` legend layer, `ALL_ADDRESSES_MVT_SQL`). They exist as
the natural per-gmina key/name for future statistics or filtering, so a later reader who wants them does
not have to pay for a re-import to get them back.
