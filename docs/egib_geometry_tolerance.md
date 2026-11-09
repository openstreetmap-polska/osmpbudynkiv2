# EGIB geometry tolerance

From November 2026, an EGIB refresh reported ~3.1M of 17.6M rows as
"modified" every day: 3,096,259 on 2026-11-07 and 3,089,180 on 2026-11-08.
Each apply took 5.5–6.5 h in production, and the match and tile backlog it
left lasted hours longer. Almost none of those rows had changed. This page
covers what was measured, what was built, and what it deliberately leaves
alone.

Code: `src/dataset.rs` (`GeometryCompare`, `EGIB_GEOMETRY_TOLERANCE_M`,
`geometry_moved_sql`, `geometry_grid_digest_sql`) and `src/update/diff.rs`
(`build_sql`'s two-stage form).

## Cause

The churn comes from upstream. GUGiK re-serializes whole powiats at a
different coordinate precision from one export to the next. In one export the
coordinates are rounded to 6 decimals or to the millimetre; in the next they
are unrounded or re-reprojected, ~1e-7 m apart. In August EGIB geometry was
still byte-stable: 10,986 of 17.55M rows changed over nine days
(`superpowers/plans/2026-08-14-key-based-diff.md`).

Our pipeline is deterministic: the same file loaded twice diffs 0/0/0. The
diff compared geometry byte for byte (`ST_AsWKB`), so a sub-millimetre
re-serialization counted as a modification. One building from the
2026-08-15 and 2026-11-09 exports moved 25 nm and got a different hash.

## Measurements

Sample: 30,000 rows from the churning powiats whose geometry bytes changed
between the 2026-08-15 and 2026-11-09 exports. Each geometry was transformed
to EPSG:4326 exactly as `import::egib` does. Each cell counts the rows flagged
as modified. "N dec" means coordinates snapped to a 10^-N degree grid.

| largest vertex shift | rows | exact WKB (before) | 7 dec (~0.7×1.1 cm) | 6 dec (~7×11 cm) | 1 cm grid, EPSG:2180 | tolerance 1 cm | tolerance 5 cm |
|---|---|---|---|---|---|---|---|
| < 1 mm | 28,148 | 28,148 | 2,193 | 259 | 3,075 | 0 | 0 |
| 1 mm – 1 cm | 1,045 | 1,045 | 765 | 196 | 725 | 0 | 0 |
| 1 – 5 cm | 0 | | | | | | |
| 5 – 15 cm | 2 | 2 | 2 | 2 | 2 | 2 | 2 |
| ≥ 15 cm | 762 | 762 | 762 | 762 | 762 | 762 | 762 |
| vertex count changed | 43 | 43 | 43 | 43 | 43 | 43 | 43 |

**Rounding is not the answer.** It flags any vertex that happens to sit either
side of a grid line, however small the move, and a coarser grid only lowers
the odds. 7 decimals still flags ~10% of the noise rows. 6 decimals flags
~1.5%, but it catches real moves under ~11 cm only by chance. A metric grid in
EPSG:2180 is no better, and the live table stores only EPSG:4326 anyway.

A tolerance on the actual vertex shift separates the two populations
cleanly: noise stays under 1 cm, real edits start at 5 cm, and nothing falls
in between.

## Definition

An EGIB geometry is modified iff either of these holds:

- its **structure** changed: geometry type, part count, ring count per part,
  or vertex count per ring;
- some vertex moved at least **T = 2 cm**. Vertices are paired in
  `ST_Dump(ST_Points(g))` order.

Distance is measured from the degree deltas, scaled by the WGS84 parallel and
meridian radii at the pair's mid-latitude, the same way `dataset::area_m2_sql`
scales area. It does not use `ST_Distance_Sphere` or `ST_Distance_Spheroid`:
their axis order follows `geometry_always_xy`, which the server sets and a
bare CLI session does not. On the sample above, this distance agreed with
EPSG:2180 distances within 3 µm for shifts under 1 cm, and within ~0.1 mm for
shifts of 5–15 cm. The ≥ 15 cm rows can't be compared this way, because the
sample's `maxd` pairs vertices differently.

The attribute comparison is unchanged, and an attribute change is a
modification whatever the geometry did. PRG keeps the byte-exact comparison,
and BDOT10k keeps no geometry comparison. Both are pinned byte-identical to
the SQL they generated before.

## How the diff evaluates it

A hash cannot express "moved less than 2 cm", so `content_hash_sql` becomes a
**prefilter** for a tolerance spec, not an equivalent:

1. **Stage 1** keeps the cheap shape: inline `(key, hash)` subqueries joined
   on the key. The hash is a struct of two digests:
   - `attrs` hashes the compared attributes;
   - `geom` hashes the structure plus every vertex snapped to a 1e-7° grid.

   Keys whose struct differs are written to `diff_modified_candidates`, with
   whether `attrs` differed.
2. **Stage 2** handles the two kinds of candidate:
   - Candidates whose attributes changed are modified, with no geometry check.
   - The geometry-only candidates are joined back to both tables, for those
     keys only, and kept iff the definition holds.

**Soundness.** A vertex that moves at least one grid-cell diagonal must land
in a different cell. At 49–55°N, 1e-7° is 0.64–0.73 cm of longitude and
~1.11 cm of latitude, so the diagonal is at most ~1.33 cm. T must be at least
that. `dataset::tests::the_tolerance_covers_the_prefilter_grid_diagonal`
computes the bound from the constants.

## Reports and served geometry

- **`content_signature_sql` did not change**, and must not: it is persisted
  in `object_reports`.
  - `reports::reconcile_source` compares the stored signature against the
    *live* row.
  - The apply rewrites only the rows the diff calls modified.
  - So a sub-tolerance re-serialization leaves the live bytes, and therefore
    the signature, untouched, and no migration was needed.
- **`import egib` still expires reports on noise.** It rebuilds the table from
  raw bytes. That is accepted, since it is a manual full rebuild.
- **Served geometry stays raw.** Nothing is snapped on the way in. An
  unmodified row keeps the previous export's geometry, which is within 2 cm of
  the latest at every vertex.

## Measured on the real data

Method: `import egib` of the 2026-08-15 export into a scratch database, then
`update egib --file` with the 2026-11-09 export, once with the binary before
this change (`52ba6e3`) and once after. Both runs used the release binary in a
`systemd-run` scope with `CPUQuota=400%` and `MemorySwapMax=0`, and with
production's DuckDB settings (`memory_limit = '6.5GB'`, `threads = 3`).
`MemoryMax` was 10G instead of 12G, because another heavy job was running on
the machine. The "churning powiats" are the 44 powiats in the sample (the first
4 characters of `id_budynku`).

| | before (byte-exact) | after (2 cm) |
|---|---|---|
| added / removed | 331,703 / 248,504 | 331,703 / 248,504 |
| modified | 4,069,310 | 1,960,328 |
| of which geometry only | 2,230,079 | 121,097 |
| modified, churning powiats (2,222,351 rows) | 2,222,351, every row | 204,761 |
| of which geometry only | 2,076,469 | 58,879 (2.6%) |
| diff wall time | 6.5 s | 22.1 s |
| peak RSS during the diff | 6.12 GiB | 6.27 GiB |
| apply | not run (5.5–6.5 h in production) | 1 h 04 m |

- **Stage 2** received 333,340 geometry-only candidates out of 2,172,571, and
  confirmed 121,097 of them. The prefilter therefore let through 212,243 of
  the 2,108,982 noise rows (10.1%), as the sample predicted.
- **The same file again**, on the updated database, diffs 0/0/0. The 212,243
  rows still holding their 08-15 geometry reach stage 2 every time, and none is
  confirmed.
- **The noise is not confined to the 44 powiats.** In the other powiats,
  geometry-only modifications fell from 153,610 to 62,218.
- **Most of the 1.84M remaining modifications are real.** The 11-09 export
  recodes `rodzaj` from long names to one-letter codes or NULL. For example,
  `mieszkalny` → `m` on 173k rows, `produkcyjnyUslugowyIGospodarczy` → `g` on
  167k, and `mieszkalny` → NULL on 151k. `mappings::egib::RODZAJ_KOD_CASE_SQL`
  already handles the short codes: its last tier maps `m`, `g` and the other
  one-letter codes to themselves, and a modified row's `rodzaj_kod` is
  recomputed from the staged row.

The per-powiat counts come from comparing each pair of tables directly,
attributes and `ST_AsWKB`, after the apply. Their totals reproduce both runs'
`modified` exactly.
