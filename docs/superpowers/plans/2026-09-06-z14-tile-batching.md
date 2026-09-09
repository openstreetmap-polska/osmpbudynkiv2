# Plan — Batched z14 tile rendering, and prepared statements

**Status: done, 2026-09-09.** All six steps landed, plus the RocksDB block-cache
fix that preceded them (`docs/rocksdb_block_cache_not_applied.md`). Outcome and
the three places reality differed from this plan are in *What actually landed*
at the bottom — read that first; the body below is the plan as written
beforehand and is kept for its measurements and its list of traps.

## Context

z12–z13 already render a batch of tiles per query (`tiles::points_mvt_sql`), and a batch of one costs
what the per-tile query cost, so that tier has exactly one SQL text. **z14 does not batch at all**:
`render_z14_tile` runs four separate `query_mvt_layer` calls per tile, so warming the country is
~4 × 140,000 = 560,000 queries.

Two independent costs sit in that:

| | measured |
|---|---|
| planning alone, four layers, per tile | **~8 ms** (independent of tile density) |
| a z14 tile holding **one** object | 17–18 ms |
| the densest z14 tiles (~9,000 objects) | 270–316 ms |
| full z14 warm (139,801 tiles, the ring-expanded `cell_totals` set) | **~85 CPU-minutes** |

The 139,801 figure is what `cell_totals` expanded through `tile_dirty::tiles_for_cell` actually
covers, across 2,322 z11 blocks. (An earlier design note said 198,314; that was a bbox-based count and
is not the warm set.)

## Goal

1. Stop re-planning the same four queries on every tile.
2. Render z14 a **block of tiles at a time** for the bulk paths (`tiles warm`, `jobs::tile_refresh`),
   leaving the request path on the per-tile query.

Measured outcome of both: full z14 warm **~85 → ~18 CPU-minutes** (4.7–4.9× across a random sample of
8 z11 blocks), with byte-identical tiles.

## What the prototype already established

Do not re-derive these.

**Byte identity.** A reconstruction of the four current layer queries was validated against the
running server first (tile 14/9145/5395: 89,206 B, sha256 `4a7b2055c03851df`, exact match), and the
batched form then matched the per-tile form on every tile of:

| set | comparisons |
|---|---|
| Warsaw z12 block (16 tiles) × 4 layers | 64/64 identical |
| dense-unmatched z12 block × 4 layers | 64/64 identical |
| Baltic block, mostly empty, 8 tiles × 4 layers | 32/32 identical |

Empty tiles come back as the same constant layer headers the per-tile path produces
(228 + 267 + 144 + 238 = 877 B, the featureless z14 tile `empty_tile_returns_ok_not_500` pins).

**The RTREE survives.** `EXPLAIN` on the batched form shows the same index scans as the per-tile form
— `RTREE_IN` ×4 (`buildings`), ×2 (`buildings_all`), ×1 (`addresses`), ×1 (`addresses_all`).

**Speed, four layers, per-tile vs batched:**

| block | tiles | per-tile | batched | |
|---|---|---|---|---|
| Warsaw centre z12 | 16 | 136.7 ms/tile | 27.9 | 4.9× |
| Warsaw centre z11 | 64 | 141.6 | 25.6 | 5.5× |
| densest unmatched z12 | 16 | 114.2 | 50.7 | 2.3× |
| rural z11 | 64 | 16.7 | 1.5 | **11.0×** |
| **random sample, 8 z11 blocks** | **~500** | **36.1** | **7.7** | **4.7×** |

**Result-set size.** A dense 64-tile block returns **9.2 MB** of MVT in one result, largest single
tile 288 KB. 64 is comfortable; 256 would be ~37 MB.

**Fan-out is negligible.** A row can belong to several tiles (z14 selects by geometry). Measured on a
dense 64-tile block: 102,781 rows → 105,335 (row, tile) pairs, **+2.5%**, as
`dataset::filter_oversized_geometry`'s invariant predicts (≤ 2×2 tiles per row).

## Step 1 — `prepare_cached` (independent, do this first)

`tiles::query_mvt_layer` calls `conn.prepare(sql)` on every layer of every tile, so DuckDB re-plans
four large queries per z14 tile. `duckdb::Connection::prepare_cached` exists (per-connection LRU,
default capacity **16**), so this is nearly a one-line change.

Measured, four layers, plan-every-time vs prepared once:

| tile | re-planned | prepared |
|---|---|---|
| sparse (1 object) | 17.25 ms | **10.96 ms** (−36%) |
| dense | 113.29 | 106.94 |
| densest | 349.67 | 339.90 |

About **−6 ms flat on every tile**, so ~14 CPU-minutes off a warm and ~6 ms off every cold request.
No SQL change, no behavioural change.

**The one trap: only use it where the SQL text is constant.** The cache is keyed on the text.

- ✅ the four z14 layers — `LazyLock<String>`, one text each.
- ✅ `agg_cells_sql(shift, n, max_age_days)` — 7 distinct texts (one per zoom z5..z11).
- ❌ `points_mvt_sql` — interpolates the tile list, so the text differs per batch. Caching it would
  miss every time *and* evict the useful entries. `render_points_tiles` keeps plain `prepare`.

4 + 7 = 11 texts against a capacity of 16 is tight once anything else joins in. Call
`set_prepared_statement_cache_capacity(32)` on each pooled connection.
`ClonedConnectionManager::connect` (`src/server/mod.rs`) is the one place the pool makes a connection
— note that `build_pool`'s doc comment directly below it currently asserts "**no further
per-connection setup is needed**", on the grounds that extension loads and `SET GLOBAL` settings are
instance-wide. A statement-cache capacity is genuinely per-connection and is the first exception, so
that comment has to move with the change rather than be left standing.

`tile_warm`'s workers hold their own `try_clone()`s outside the pool, but after step 3 they run the
batched text, which varies per block — so they neither need nor benefit from this.

**Follow-up worth noting, not doing here:** if the batched tile list were bound as list parameters and
`unnest`ed instead of interpolated, the batched SQL text would be constant too and could also be
cached. Not attempted; the `VALUES` list cannot be a single bound parameter as written.

## Step 2 — give the four layer builders a mode

Pure refactor; the existing tests are the proof. `buildings_sql(projection)` and
`all_buildings_sql(projection)` already take a seam, so this extends the existing pattern:

```rust
enum TileScope<'a> {
    /// One tile, envelope inlined as a constant -- today's text, unchanged.
    Single,
    /// A block of tiles, joined from a `VALUES` list.
    Batched(&'a [(u32, u32)]),
}
```

The projection, the predicates, the constants (`ADJACENCY_READ_BUFFER_DEG`, `BDOT10K_ADJACENCY_KEY`,
`EGIB_ADJACENCY_KEY`, `SOURCE_BUILDING_*`), the tag `LATERAL` and the `reported_sql` splices are
written **once** and shared by both arms. Land this step with `Batched` unimplemented (or emitting
`Single`) and confirm the suite is green before step 3.

This is a weaker "one home" than z12–z13 got, and it is a **measured** decision, not a slip — see
*Why two shapes* below. Record it as such in CLAUDE.md.

## Step 3 — the batched arm

The calculation does not change. Every predicate, projection and constant is identical; the tile stops
being a constant and becomes a join variable:

| `Single` | `Batched` |
|---|---|
| `bbox` — 1-row CTE, one envelope | `env` — N-row CTE from a `VALUES` list |
| `ST_AsMVTGeom(geom, bbox.geom, …)` | `ST_AsMVTGeom(geom, e.box, …)` — same expression |
| source scan `WHERE ST_Intersects(geom, <tile>)` | same predicate, **batch** envelope |
| `FROM src s, bbox` | `FROM src s JOIN env e ON ST_Intersects(s.geom, e.poly)` |
| `SELECT ST_AsMVT(t, …)` | `SELECT e.tx, e.ty, ST_AsMVT(…) … GROUP BY e.tx, e.ty` |

Four things to get right:

1. **The join predicate is the per-tile query's own `WHERE` clause**, evaluated against N tiles instead
   of one. Do **not** add an arithmetic tile-range prefilter — I built one out of
   `tile_math::cell_x_frac_sql`/`cell_y_frac_sql` on the assumption that a plain spatial join would
   degenerate into a nested loop. It does not; DuckDB plans a `SPATIAL_JOIN`, and the plain join is
   *faster at every batch size* (26.6 → 23.3 ms/tile at 64 tiles; 518 → 454 ms at a batch of one).
2. **The envelope comes from `tile_to_bbox`, carried in the `VALUES` list** — one home for tile → bbox,
   rather than a second Web Mercator inverse in SQL. Format the `f64`s with `{:?}` so they round-trip.
3. **Empty tiles need `env LEFT JOIN proj` + `FILTER (WHERE … IS NOT NULL)`**, exactly as
   `points_mvt_sql` does. A bare `GROUP BY` emits no row for a tile holding nothing, and a missing row
   is not an empty tile.
4. **Keep the grouping keys out of the attribute dictionary.** `ST_AsMVT(t, …)` takes the whole row, so
   a row carrying `tx`/`ty` publishes them as feature attributes. Build a struct of exactly the
   attribute columns (`{'geom': …, 'id': …, …}` — the literal syntax takes arbitrary keys, which
   `addr:street` and friends need) and pass that. Order it with
   `ORDER BY ST_YMin(f.geom), ST_XMin(f.geom), f`, the same rule `deterministic_mvt_order_sql` applies.

The four layers stay four queries per batch, concatenated per tile in Rust in the existing order — an
MVT tile is repeated `layers` fields, which is already how `render_z14_tile` assembles one.

New entry point, mirroring `render_points_tiles`:

```rust
pub fn render_z14_tiles(conn: &Connection, tiles: &[(u32, u32)])
    -> anyhow::Result<Vec<RenderedTile>>
```

## Step 4 — batch by ancestor tile, not by sorted run

**A z11 block's batch bbox is exactly the z11 tile's bbox**, so there is no wasted scan and the batch
is compact. Group keys by `(x >> 3, y >> 3)`; 64 z14 tiles per block, and the measured result-set size
says that is the right cap.

`tile_warm::warm_units` currently cuts z12/z13 runs out of the sorted key list, which is looser —
worth switching that tier to ancestor grouping at the same time.

## Step 5 — wire the bulk callers

- `tile_warm::WarmUnit::Z14(TileKey)` becomes `Z14Block { tiles: Vec<(u32, u32)> }`.
- `jobs::tile_refresh` pass 2 groups its resident z14 keys by z11 ancestor, the way it already groups
  z12/z13 by zoom.
- Both already degrade correctly on a failed batch: the tiles stay unwarmed or stale, which is the same
  outcome a per-tile failure had, just coarser. Keep the warning messages carrying the tile count.
- The request path (`serve_persisted_tile` → `render_tile`) is **unchanged**.

## Step 6 — tests

- `a_z14_tile_rendered_in_a_batch_is_byte_identical_to_one_rendered_alone` — the property that lets
  both shapes exist. Seeded fixture, several tiles, all four layers.
- `a_z14_batch_answers_for_every_requested_tile_including_empty_ones` — the `LEFT JOIN` + `FILTER`.
- Fan-out corners: a building straddling a tile edge appears in **both** tiles; a point exactly on a
  boundary appears in both (`ST_Intersects` is inclusive on the boundary).
- An `EXPLAIN` test in the shape of `mvt_bbox_filter_uses_the_rtree_index`, asserting the batched form
  still reaches the RTREE — this is the regression that passes every functional test.
- An `#[ignore]`d benchmark next to `points_batch_size_vs_render_cost`, reporting ms/tile at block
  sizes 1 / 16 / 64 against the real database.

## The open problem: the adjacency count

**This is the part that needs design work, and it is the only part that does.**

`bdot10k_nb`/`egib_nb` read the *full* building tables inside **the tile's own buffered envelope**
(`ADJACENCY_READ_BUFFER_DEG`), so a building's neighbour count is a property of `(building, tile)`,
not of the building — the same building can get a different `max_neighbours` verdict, and therefore a
different `tags` string, in two tiles that both draw it. **Widening that read to the batch looks like
an obvious cleanup and would silently change tags across the country.** Reproducing it exactly is what
keeps the bytes identical.

Reproducing it exactly is also what makes `buildings` the one layer that is slower batched. At a batch
of one, on the densest tile:

| layer | per-tile | batched | |
|---|---|---|---|
| `buildings_all` | 216.1 ms | 167.2 | **0.77×** — faster batched |
| `addresses_all` | 91.1 | 95.1 | 1.04× |
| `addresses` | 6.6 | 8.4 | 1.28× |
| **`buildings`** | **71.9** | **202.2** | **2.81×** |

Two formulations were tried; **neither is free**, and this is where to start tomorrow:

- **Fan the neighbour set out to tiles and join `USING (tx, ty)`.** Byte-identical, but `EXPLAIN` shows
  DuckDB plans it as `HASH_JOIN: tx = tx, ty = ty`. At one tile that key has a single distinct value,
  so it materialises the entire `pkg × nb` cross product before the `ST_Intersects` filter runs. This
  is the 2.81×.
- **Keep the spatial join primary, apply the tile scoping as a filter on surviving pairs**
  (`WHERE ST_Intersects(nb.geom, ST_Expand(p._poly, BUF))`, with `pkg` carrying `e.poly`).
  Byte-identical (32/32 tiles), and *worse* on the densest tile at a batch of one (462 → 613 ms):
  carrying a polygon per row and calling `ST_Expand`/`ST_Intersects` per surviving pair costs more than
  the cross product it avoids. Block-level throughput was unchanged (4.9× → 4.7×).

Ideas not yet tried:
- Compute the count against the **batch-wide** neighbour set and then subtract, or bound it, so the
  spatial join runs once per row rather than once per (row, tile). Needs a proof that the answer is
  unchanged.
- Carry the tile's *box* (four doubles) rather than its polygon, and scope with plain comparisons
  instead of `ST_Expand` + `ST_Intersects` — the neighbour test is against a rectangle, so a bbox
  comparison is exact for the scoping half.
- At a batch of one the scoping is a provable no-op (`GROUP BY tx, ty, rid` ≡ `GROUP BY rid`, and the
  fan-out disappears), which is most of why `Single` stays a separate shape.

## Why two shapes, stated as the measured decision it is

Not because the calculation differs — because a batch of one pays for machinery it does not need.
After removing the arithmetic prefilter:

| tile | per-tile | batch-of-one |
|---|---|---|
| sparse | 19.8 ms | 27.1 (1.37×) |
| dense | 114.6 | 109.7 (0.96×) |
| densest | 356.1 | 461.8 (1.30×) |

Two of four layers are already at parity or better. If the adjacency count can be made free at n=1,
**revisit whether `Single` is needed at all** — a single shape would be the better outcome and is not
ruled out.

## Traps found — do not rediscover these

1. **Do not wrap the per-tile query in a `LATERAL` over the tile list.** It is the obvious way to make
   the two shapes identical, and it is silently wrong: on a real Warsaw tile it produced **277 features
   where both the per-tile query and the explicit join produce 297**. Same first feature, same last
   feature, same order, no error — 20 missing from the middle. Not the `ORDER BY`, not `LEFT JOIN` vs
   `JOIN`, not the `reported` `EXISTS`; all three were tested. Root cause not chased.
2. **Do not drop the batch-envelope filter from the source scans** and let the join drive the index.
   `buildings_all` over 16 tiles: `RTREE_IN` ×2 → ×0, `SEQ_SCAN` ×2 → ×4, **309.6 ms → 26,970.7 ms**
   (87×). The RTREE needs a *constant* bound; a join condition gives `Bounds: deferred (from join
   filter)`, which prunes nothing — the same hazard CLAUDE.md records for `suppressed_buildings_sql`.
3. **Do not join the fanned neighbour set `USING (tx, ty)`** — see the section above.
4. **`ST_Intersects` in the join is not the batch-of-one cost.** Replacing it with `TRUE` at n=1, where
   it is provably redundant, recovers ~2 ms of ~97. It was my first diagnosis and it was wrong.

## Verification protocol

1. `cargo fmt -- --check && cargo clippy --all-targets && cargo test`
2. **Byte identity against the real database**, the one check that matters. Stop any `run` server
   (it holds the DuckDB lock), then compare `render_tile` against `render_z14_tiles` over several
   whole z11 blocks — one dense city, one with many *unmatched* buildings (the adjacency path), and
   one over the Baltic (empty tiles). Every tile must match exactly.
3. `cargo test --release <benchmark> -- --ignored --nocapture` for the block-size numbers.
4. End-to-end, as the z12/z13 work was verified: a scratch config pointing `rocksdb_path` at a temp
   directory and `http_listen_addr` at a spare port, all jobs disabled, then
   `tiles warm --bbox <small>` → `run` → record ETag + body sha per tile → `tiles clear` → re-warm →
   restart → re-record → **diff must be empty**.
5. Browser check per CLAUDE.md (`npx playwright cli open --browser=chromium`), z13 and z17, console
   clean.

**Do not bump `TILE_FORMAT_VERSION`.** Nothing about what a tile contains changes; a stored tile stays
correct. (If any of this ever *does* change tile content — the adjacency scoping is the candidate —
that is a different decision and needs the bump plus a national re-warm.)

## CLAUDE.md updates this will need

- The three-tier gotcha's point 5 currently says "**z14 has no batched form**" and explains why. Rewrite
  it: z14 batches for the bulk paths only, by z11 ancestor, and the request path stays per-tile —
  with the per-layer table above as the reason.
- A new gotcha for the adjacency scoping: the neighbour count is `(building, tile)`-scoped, widening it
  to the batch silently changes tags, and `USING (tx, ty)` is the trap that looks like the way to keep
  it scoped.
- Record `prepare_cached`'s constant-text requirement next to `query_mvt_layer`, so nobody extends it
  to `points_mvt_sql`.


---

# What actually landed

## Outcome

Byte-identical on every tile tried, and faster than this plan predicted.
`z14_batches_match_per_tile_renders_on_the_real_database` (ignored; needs the
real DuckDB file and exclusive access) over whole z11 blocks:

| block | per-tile | batched | |
|---|---|---|---|
| Warsaw centre | 159.3 ms/tile | 20.7 | **7.7×** |
| dense unmatched | 132.9 | 33.8 | 3.9× |
| rural | 15.5 | 0.90 | **17.1×** |
| Baltic (empty) | 13.8 | 0.43 | **31.8×** |

`differing 0` in all four. End-to-end: a 735-tile Warsaw warm, served, cleared,
re-warmed and re-served produced an identical ETag/sha for every probed tile —
and those ETags match what the *per-tile* renderer produced three days earlier,
which is the strongest evidence available that the two paths agree.

## Three things this plan got wrong

### 1. The struct literal was unnecessary — and so was the fan-out arithmetic

The plan said the grouping keys force a hand-written struct beside the column
list. They do not. `CROSS JOIN LATERAL (SELECT <the column list verbatim>) t`
yields a `t` holding exactly the listed columns, so **the projection is written
once and shared by both arms**, and `TileScope` only wraps it. The arithmetic
tile-range prefilter was already known unnecessary (a plain `ST_Intersects` join
is faster) and is not in the landed code either.

### 2. `ST_AsMVTGeom` inside the aggregate's input crashes DuckDB

Not anticipated anywhere in this plan, and the most important thing it missed.
`ST_AsMVT` raises `INTERNAL Error: Buffer overflow` — an assertion failure that
**invalidates the database handle**, so every later query on that connection
returns `FATAL … database has been invalidated` — whenever `ST_AsMVTGeom` is
evaluated in the aggregate's own input expression and a group has no surviving
row. An empty tile in a batch is precisely that group.

The fix is to materialise the projection in a `proj` CTE first, so the aggregate
reads a plain column. `points_mvt_sql` was already safe only by accident of how
it was written. Full repro on `TileScope::body` and in CLAUDE.md.

This is why the empty-tile test earns its place: the failure mode is a crash,
not wrong bytes, so a warm over sea would have taken the process down.

### 3. `prepare_cached` is worth ~3 ms, not ~6

Measured in-process rather than through the DuckDB CLI:
`z14_render_cost_floor_across_densities` moved from 16.7–18.2 ms to 13.9–14.7 ms
on one-object tiles, with dense tiles unchanged inside noise. Still worth having
— it is one line plus a cache-capacity call — but the CLI experiment that
suggested ~6 ms was measuring its own overhead too.

## Also different, by choice

- **Blocks replaced `WarmUnit` entirely.** `tiles::render_blocks` /
  `render_block` group *every* tier's keys into same-zoom 8×8 ancestor-aligned
  blocks, so `tile_warm` and `jobs::tile_refresh` each lost their bespoke
  batching. z12/z13 moved from 256-tile sorted runs to 64-tile ancestor blocks:
  slightly smaller batches, but a batch's bounding box is now exactly its
  ancestor tile's, instead of a tall thin strip.
- **The adjacency count kept the `USING (tx, ty)` shape** this plan flagged as a
  trap. That shape's pathology is specific to a batch of *one*, which
  `Batched` never sees now that `Single` serves the request path — so the open
  design question the plan ended on turned out not to need solving. The two
  alternatives it listed remain unexplored and unnecessary.
- **`TILE_FORMAT_VERSION` was not bumped**, as planned: the bytes are identical,
  so stored tiles stay valid.

## Not done

- The systemd/production rollout, and confirming the ~640 MB RSS rise from the
  block-cache fix against `MemoryCurrent` alongside the `MALLOC_CONF`
  experiment in `docs/memory_growth_investigation.md`.
- A full-country `tiles warm` to re-measure the ~85 → ~18 CPU-minute projection
  end to end. The per-block numbers above are consistent with it and better in
  the rural blocks that dominate the warm set, so the real figure should land
  under 18.
