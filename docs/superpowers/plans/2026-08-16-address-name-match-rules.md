# Widening PRG↔OSM address matching with two name rules

Date: 2026-08-16
Measured against the live national database (11 GB, read-only): 8,608,828
`prg_addresses`, 8,689,260 `osm_addresses`, 558,782 unmatched under the old rule.

## Problem

An address counted as matched on housenumber agreement alone within 50 m
(`rule::MATCH_DISTANCE_METERS`). That threshold is a blunt instrument: PRG
publishes a point on the parcel, OSM usually puts the node on the building, and
on deep plots and corner lots the two legitimately sit further apart than 50 m.

The motivating record, PRG `7077839d-e180-4030-a679-f968741386f6` (Zakroczym,
ul. Warszawska 44), is **51.8 m** from OSM way `733719233`, which carries
`addr:street=Warszawska` + `addr:housenumber=44`. Unambiguously the same
address; unambiguously proposed for import today, 1.8 m outside the cutoff.

The fix is not to raise 50 m globally. It is to raise it *only when a name
agrees*, which is much stronger evidence than proximity alone.

## The rules

```
matched(a) := EXISTS osm o WHERE hn(o) = hn(a) AND (
       dist <= 50                                            -- A proximity
    OR ( dist <= 150 AND (
             a._street = o._street                           -- B street
          OR ( a._street IS NULL AND o._street IS NULL
               AND a._place = o._city ) ) ) )                -- C locality
```

with `normalized_name_sql(x) = NULLIF(lower(trim(x)), '')` applied to every
name; `a._street` is the resolved `COALESCE(loc, gl, a.ulica)` chain and
`a._place` is `a.miejscowosc`.

## Measured effect

| | added matches |
|---|---|
| **B** — mapped street agrees, ≤150 m | **20,980** |
| **C** — both streets absent, locality agrees, ≤150 m | **22,276** |
| combined | **43,256** (7.7% of unmatched → 515,526 remaining) |

Supporting measurements:

- **The name test is doing real work.** Widening to 150 m on housenumber alone
  matches 56,205 — so requiring a name agreement rejects 35,225 pairs that
  agree on nothing but a house number and a neighbourhood.
- **Duplicate cost, accepted deliberately:** 194 (B) and 211 (C) of the gain —
  ~0.9% each — match an OSM node that a *closer* PRG address already matched.
  Samples are genuine PRG registry duplicates (two `Warszawska 10` records in
  Narol, 1.2 m and 50.2 m from the same node). Left unguarded: matching is
  per-row existence, not a bipartite assignment, and a "closest wins" guard
  would make the two compare paths disagree, since the grid-key path has no
  notion of closest. Pinned by
  `rule::tests::an_osm_node_matched_at_50m_can_also_match_a_second_address_via_street`
  so removing it stays a decision rather than an accident.
- **Cross-locality risk is negligible:** only 74 of the 20,980 B matches have a
  contradicting `city`, and most are name variants (`bojszowy nowe` /
  `nowe bojszowy`, `żółkiewka-osada` / `żółkiewka`). No city guard added.
- **`osm_addresses.city` is `COALESCE(addr:city, addr:place)`** at all six
  insert sites. Rule C is about streetless place-addresses, which in Poland
  carry `addr:place`, so that COALESCE is the whole reason C finds anything.

## Rejected alternatives

- **Raw `ulica` instead of the mapped name** — 20,243 matches, 737 fewer, and
  it would leave the mapping a serving-time-only table (see "consequence"
  below). **"Mapped OR raw"** gives 21,134, only +154 over mapped-only: not
  worth a third branch.
- **Tightening rule A to require street agreement.** 187,617 currently-matched
  rows have *every* nearby OSM address contradicting the street, and sampling
  shows they are overwhelmingly name variants (`dwernickiego` vs
  `józefa dwernickiego`, `aleja klonowa` vs `klonowa`). This would manufacture
  ~187k false import candidates. Do not do it.
- **UNION-ed branches keyed on `(_hn, street, _gx, _gy)`** in the full compare.
  Every pair the name rules can match is already a pair the existing
  `(_hn, _gx, _gy)` join produces — the rules relax the *distance*, never the
  key — so a keyed-branch variant runs two extra hash joins over 8.6M/8.7M
  inputs re-deriving pairs the first join already emitted. The name rules are
  extra `OR` branches on the existing join instead, which leaves the fan-out
  bit-for-bit unchanged and the module doc's O(n²) analysis valid verbatim.
- **`IS NOT DISTINCT FROM` for the name comparison.** Would match two addresses
  that merely both lack a locality. `=` is never true for NULL, which is the
  wanted behaviour; `null_locality_never_matches_by_place` pins it.

## Architectural consequence: the mapping became a match input

Rule B reads `street_name_mappings`, which was **serving-time only** — a
mapping reload changed exported tags and nothing else, and several CLAUDE.md
gotchas said so. After this change a mapping reload can flip an address between
matched and unmatched, so `mappings::street_names::validate_and_swap` enqueues
prg dirty cells for the symmetric difference of old and new mapping triples,
inside the swap transaction and before its `DELETE`.

Scale: a no-op reload enqueues **0** cells; a full 3,272-row replacement
enqueues **8,964 of 112,264** prg cells.

## Constants that had to move

- `OSM_MATCH_BUFFER_DEG`: 0.001 → **0.003**. It is coupled to the *widest*
  distance any branch uses. At Poland's northern edge (54.84 °N) 1° of
  longitude is ~64.1 km, so 150 m needs ≥ 0.00234°; 0.003 preserves the same
  1.28× east-west headroom 0.001 gave 50 m. Propagates automatically to
  `update::dirty_cells::layer_buffer_deg`, costing ~45% more enqueued prg cells
  per edited OSM address node (~1.26 → ~1.83 expected).
- `GRID_KEY_DEG` **stays 0.005** (~320 m EW at 54.84 °N), but headroom drops
  6.4× → 2.1×. Both constants now have computed guard tests
  (`osm_match_buffer_covers_the_widest_match_distance`,
  `grid_key_cell_is_wider_than_the_widest_match_distance`) because both failure
  modes are silent: a pair simply stops being compared, with no error.

## Performance

- **Per-cell drain** on Poland's densest z14 cell (9143/5557, 485 unmatched
  rows): **0.048 s → 0.074 s** (1.5×), correctly dropping to 472 rows.
- Both `RTREE_INDEX_SCAN`s survive, verified by `EXPLAIN` against the real
  tables. Worth recording precisely because it corrects an assumption made
  during design: the rule's `addr_candidates`/`addr_resolved` CTE chain is
  **structurally** required (`compare::incremental` used to concatenate its own
  `WITH`, and two `WITH` keywords is a syntax error) — it is *not* a measured
  index fix. `MATERIALIZED` there is insurance against the documented
  `server::tiles` fold-back, nothing more.
- The full compare is a designed full scan and needs no index; wrapping its
  0.5°-grid guard in a CTE measured *worse* in the analogous buildings case
  (0.955 s → 1.097 s) and was not attempted here.
