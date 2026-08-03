# Building type mappings

Status: design, not implemented — no Rust code exists for any of this yet.

This document is meant to be sufficient on its own: the full BDOT10k tier-1
table is in [Appendix A](#appendix-a--the-full-tier-1-table) and the EGIB
`rodzaj` cascade in [Appendix B](#appendix-b--resolving-egib_buildingsrodzaj-to-a-letter),
so an implementer needs nothing beyond this file and the repository.

The BDOT10k tier-1 table is reviewed and settled. The EGIB table is measured
against BDOT10k by geometric pairing; its `m` refinement has been reviewed, the
remaining letters have not (see [Open items](#open-items)).

All figures were measured in August 2026 against the production
`osmpbudynkiv2.duckdb`, not against fixtures.

## Why

Packages currently emit a fixed `building=yes` (`server::package::building_tags`).
That is not neutral — it actively degrades the statistic the tool exists to
improve.

Poland's OSM building stock is 59.99% untyped (`building=yes`). Importing the
771,699 unmatched BDOT10k rows as `building=yes` moves that to 61.6%; adding
the 2,233,840 unmatched EGIB rows moves it to 64.4%. Meanwhile **98.14% of
unmatched BDOT10k rows carry a detailed function that maps to something more
specific.** The information is already in the database; we are throwing it away
at the last step.

## Source columns

### BDOT10k

Two columns, used as a two-tier cascade:

| tier | column | distinct values | row coverage |
|---|---|---|---|
| 1 | `PRZEWAZAJACAFUNKCJABUDYNKU` | 165 (167 in schema) | 97.74% |
| 2 | `FUNKCJAOGOLNABUDYNKU` | 10 | 99.99991% cumulative |

Tier 2 is the 10 KŚT (Klasyfikacja Środków Trwałych) categories. Falling
through both tiers leaves ~15 rows nationally, which get `building=yes`.

The sample mapping in `example_data/mappings/` uses a three-column composite
key; that is over-specified — 617 rows encoding only 163 distinct decisions.
Tier 1 alone reproduces them.

**The vocabulary is closed.** `OT_FunSzczegolowaBudynkuType` in
`example_data/mappings/XSD/BDOT10k_BDOO.xsd` enumerates exactly 167 values, and
every one of the 165 values present in production data is in that enumeration —
zero unknowns. `OT_FunOgolnaBudynkuType` enumerates exactly the 10 KŚT
categories.

This means the table can be **exhaustive and validated**. A value arriving that
is not in the table indicates a BDOT10k schema revision, and should be logged as
an error via `job_log` rather than silently falling through to `building=yes` —
the same drift-detection role `rows_absent_from_prg` plays in the street-name
loader.

An earlier draft of this document took closedness as the reason to keep the
mappings in Rust rather than in a loadable CSV. That does not follow, and the
conclusion has been reversed — see [Packaging the mappings as
CSV](#packaging-the-mappings-as-csv). Closedness constrains the *key* space; the
part of this table that will actually change is the *value* space, the OSM tag
chosen for each key, which is a matter of community convention rather than of
schema.

### EGIB

`egib_buildings.rodzaj` combines a letter code with a KŚT code. The letter
mapping, derived from the data and agreeing with the sample mapping on all ten
entries:

| letter | KŚT | | letter | KŚT |
|---|---|---|---|---|
| p | 101 | | s | 104 |
| t | 102 | | b | 105 |
| h | 103 | | z | 106 |
| k | 107 | | i | 109 |
| g | 108 | | m | 110 |

`rodzaj` has 76 distinct values across **four different encoding conventions**,
apparently contributed by different voivodeship data providers, plus a tail of
typos. A four-tier cascade normalising on `lower(trim(...))` resolves 99.9105%
of non-null rows:

| tier | form | example | distinct | rows | share |
|---|---|---|---|---|---|
| 1 | bare letter code | `m`, `g`, `mj`, `m2`, `b.` | 33 | 15,680,984 | 91.45% |
| 2 | camelCase enum | `produkcyjnyUslugowyIGospodarczy` | 11 | 1,344,997 | 7.84% |
| 3 | full KŚT name | `budynki transportu i łączności` | 10 | 89,550 | 0.52% |
| 4 | combined | `m - budynki mieszkalne (110)` | 10 | 16,061 | 0.09% |
| — | unresolved | `x`, `f`, `c`, `u`, `e`, … | 12 | 15,343 | 0.09% |

Tier 1 must match a **prefix**, not the whole string: variants like `mj`, `md`,
`mt`, `mz`, `it`, `m2`, `mj3`, `m.`, `bud.` all carry the KŚT letter first and a
local suffix after. Taking the first character resolves all 33 of them.

Only 12 values (15,343 rows, 0.09%) resolve to nothing, dominated by `x`
(14,752) — evidently a local "unspecified" marker. These take `building=yes`.

A further 650,901 rows (3.66% of the table) have `rodzaj IS NULL` outright.

### KŚT as a cross-check

The KŚT bill (`example_data/mappings/kst_mapping.csv`) acts as an interlingua
between the two sources and validates both: all 10 BDOT10k
`FUNKCJAOGOLNABUDYNKU` values match the bill verbatim, and only 4,589 rows
(0.028%) carry a `KODKST` that contradicts their stated general function.

### On taginfo

`example_data/mappings/osm-key-building-values.csv` is a **popularity** list,
not a validity whitelist — 13 of 34 candidate values fall outside its top 41 yet
are all in genuine Polish use (`building=sty`, 16,654). Ground tag choices
against `osm_buildings` instead: it is Poland-specific, always current, and
already in the database. Every `PL` figure in this document comes from there.

## The mapping table

Assignment rule: **use the most specific value that is both documented in OSM
and actually used in Poland, and that does not assert more than the source
states.** Where no such value exists, fall back to a generic rather than invent
one. 26 of the 167 entries land on `building=yes`; all are low-volume.

Twelve entries carry 95.6% of the BDOT10k import (737,886 of 771,699 rows):

| function | unmatched | tags |
|---|---|---|
| budynek gospodarczy | 416,851 | `building=outbuilding` |
| budynek jednorodzinny | 208,718 | `building=detached` / `building=house` — see [Adjacency](#adjacency) |
| dom letniskowy | 28,509 | `building=bungalow` |
| garaż | 25,881 | `building=garage` |
| budynek wielorodzinny | 15,839 | `building=apartments` |
| magazyn | 11,439 | `building=warehouse` |
| produkcyjny | 9,841 | `building=industrial` |
| obiekt handlowo-usługowy | 8,717 | `building=retail` |
| siedziba firmy lub firm | 3,890 | `building=office` |
| szkoła podstawowa | 2,843 | `building=school` |
| budynek produkcyjny zwierząt hodowlanych | 2,768 | `building=farm_auxiliary` |
| szklarnia lub cieplarnia | 2,590 | `building=greenhouse` |

### Tier 2 — the KŚT categories

**This table applies to BDOT10k only.** EGIB has its own, measured differently —
see [EGIB](#egib). BDOT10k carries *both* the general and the detailed function,
so each KŚT category's composition is directly measurable **within this source**,
and the categories turn out to be far more concentrated than their names suggest:

| KŚT category | dominant detailed function | share | tags |
|---|---|---|---|
| budynki produkcyjne, usługowe i gospodarcze dla rolnictwa | budynek gospodarczy | **98.9%** | `building=outbuilding` |
| budynki transportu i łączności | garaż | **98.4%** | `building=garage` |
| zbiorniki, silosy i budynki magazynowe | magazyn | **98.0%** | `building=warehouse` |
| budynki mieszkalne | budynek jednorodzinny | 89.6% | `building=residential` |
| budynki handlowo-usługowe | obiekt handlowo-usługowy | 82.6% | `building=retail` |
| budynki biurowe | siedziba firmy lub firm | 70.3% | `building=office` |
| budynki przemysłowe | produkcyjny | 69.9% | `building=industrial` |
| budynki szpitali i inne budynki opieki zdrowotnej | placówka ochrony zdrowia | 53.1% | `building=healthcare` |
| budynki oświaty, nauki i kultury oraz budynki sportowe | szkoła podstawowa | 30.9% | `building=civic` |
| pozostałe budynki niemieszkalne | restauracja | 16.5% | `building=yes` |

Three of these overturn the obvious reading of the category name, and all three
are high-volume:

- **"budynki transportu i łączności" is 98.4% garages.** Read literally it
  suggests `transportation` (stations, depots); in reality it is where the
  registry puts domestic garages.
- **"budynki produkcyjne … dla rolnictwa" is 98.9% `budynek gospodarczy`** —
  ordinary outbuildings, not `farm_auxiliary` production halls.
- **"zbiorniki, silosy i budynki magazynowe" is 98.0% plain warehouses**, so
  `warehouse` is right and the tanks-and-silos reading is a rounding error.

`budynki mieszkalne` keeps the safe `residential` rather than `house` despite
89.6% being single-family: the remaining 10% includes 495,596 apartment
buildings, and `residential` is never wrong. `pozostałe budynki niemieszkalne`
has no dominant member and stays `yes`.

These shares are only valid **inside BDOT10k**, because the general category and
the detailed function come from the same classification act — they agree by
construction. Reusing them for EGIB was a methodological error; see the next
section.

The full 167-entry tier-1 table is in [Appendix A](#appendix-a--the-full-tier-1-table).

### EGIB

EGIB has no equivalent of BDOT10k's detailed function: `rodzaj` resolves only to
a KŚT letter, so EGIB is limited to ten coarse buckets.

**Do not reuse BDOT10k's tier-2 table here.** EGIB's letter is assigned by a
different surveyor under a different workflow, and the two sources demonstrably
disagree about what the same category means. The table below is instead measured
directly: EGIB buildings were paired **geometrically** with BDOT10k buildings
(BDOT10k centroid inside the EGIB polygon) across eight regions — Warszawa,
Kraków, Gdańsk, Poznań, Wrocław, Katowice, Lublin and rural Podlasie — and each
pair's BDOT10k function was mapped through the tier-1 table to the tag BDOT10k
would emit. `agreement` is how often that tag is the one this table assigns;
`range` is the spread of that figure across regions.

| letter | KŚT | unmatched | share | tags | agreement | range |
|---|---|---|---|---|---|---|
| g | 108 | 967,273 | 45.61% | `building=outbuilding` | 88.9% | 61.8–94.2 |
| i | 109 | 658,323 | 31.04% | `building=yes` | — | — |
| m | 110 | 236,571 | 11.16% | `building=residential`, refined by storeys | **96.6%** | stable |
| t | 102 | 162,618 | 7.67% | `building=yes` + `fixme` | — | — |
| s | 104 | 30,870 | 1.46% | `building=warehouse` | 62.6% | 35.8–90.0 |
| p | 101 | 23,386 | 1.10% | `building=yes` + `fixme` | 41.0% | 24.6–51.6 |
| h | 103 | 22,996 | 1.08% | `building=retail` | 58.0% | 51.7–64.6 |
| k | 107 | 8,473 | 0.40% | `building=civic` + `fixme` | generalisation | — |
| b | 105 | 5,536 | 0.26% | `building=office` | 56.7% | 48.9–63.9 |
| z | 106 | 1,641 | 0.08% | `building=healthcare` | 66.2% | — |
| — | unresolved | 3,073 | 0.14% | `building=yes` | — | — |
| — | `rodzaj IS NULL` | 122,965 | — | `building=yes` | — | — |

**Net effect: 1,296,746 of 2,243,725 unmatched EGIB rows (57.8%) receive a
specific building value**; the remaining 42.2% fall back to `building=yes`.
Essentially all of it is `g` → `outbuilding` (45.6%) and `m` → `residential`
(11.2%).

#### Why `t` is `yes` {#egib-t-is-not-portable}

`t` (KŚT 102, "budynki transportu i łączności") is 79.5% garages nationally,
which looks adoptable until it is broken down by region:

| region | pairs | `garaż` | `budynek gospodarczy` |
|---|---|---|---|
| poznań | 5,479 | **97.6%** | 0.3% |
| wrocław | 1,721 | 93.8% | 0.1% |
| warszawa | 3,761 | 91.8% | 6.3% |
| kraków | 641 | 82.8% | 11.9% |
| katowice | 3,279 | 65.4% | 28.9% |
| lublin | 1,448 | 44.6% | 48.7% |
| gdańsk | 1,211 | 34.2% | 45.7% |
| podlasie | 340 | **16.2%** | 76.5% |

`t` means "garage" in Wielkopolska and "shed" in Podlasie — a 6× swing. The
error is **systematic and regional**, which is far worse than a randomly
distributed one: imports are done area by area, so a mapper working Podlasie
would meet ~84% wrong `garage` tags consecutively. `outbuilding` fails
symmetrically (97.6% wrong in Poznań), so no single value works nationally.

What *is* stable is the pair: `garaż` + `budynek gospodarczy` together is 92–98%
in seven of eight regions. A region-conditioned rule could therefore recover
these 162,618 rows, but it needs a voivodeship or TERYT attribution the pipeline
does not currently carry.

#### Other notes

- **`m` → `residential`, not `house`.** EGIB `m` is 55.8% single-family and
  **40.8% multi-family**, where BDOT10k's equivalent category is 89.6%
  single-family. `house` would have mis-tagged four in ten apartment buildings.
  Together the two account for 96.6%, so `residential` is safe as the base
  value. Storey count and adjacency then refine it — see below.
- **`i` is EGIB's dumping ground.** BDOT10k assigns "pozostałe budynki
  niemieszkalne" to 0.76% of its rows; EGIB assigns it to 15.5%. Its modal
  pairing is `outbuilding` at only 50.9% with a 29.9–84.4 regional range, so it
  stays `yes`.
- **`k` is a generalisation, not a measured modal tag.** Its pairings split
  across `school` (23.9%), `university` (20.9%) and `kindergarten` (11.9%) with
  no winner; `civic` is chosen as the umbrella the KŚT category describes.
- **`p` is the weakest adopted value** at 41.0% with a 24.6–51.6 range. Unlike
  `t`, no competing tag wins in any region — the distribution is diffuse rather
  than regionally inverted — so `industrial` is kept, at 1.1% of volume.
- **Sampling bias.** Seven of the eight regions are urban, so the national
  agreement figures are urban-weighted; `g` reached ≥30 pairs in only five
  regions. The regional *range* is the more trustworthy signal than the national
  column. Only 52.7% of EGIB `t` rows found a BDOT10k pair at all, so these
  figures describe the co-located subset.

#### Refining `m` by storey count

`egib_buildings.kondygnacje_nadziemne` splits the residential bucket further.
Validated by pairing 53,945 EGIB `m` buildings with hand-typed OSM buildings in
the same eight regions:

Neighbours here are **residential neighbours only** — another EGIB `m` building
it touches (see [Adjacency](#adjacency) for the exact predicate; these figures
were measured with the ≥3 m variant). A dwelling that abuts nothing but its own garage is a
free-standing house, so counting outbuildings as neighbours would be wrong. This
mirrors BDOT10k, where the adjacency test is likewise restricted to the same
function class.

| levels | res. nbrs | n | detached | house | apartments | semi | terrace |
|---|---|---|---|---|---|---|---|
| 4+ | 0 | 4,260 | 0.3 | 1.0 | **98.6** | 0.0 | 0.1 |
| 4+ | 1+ | 5,034 | 0.1 | 5.7 | **93.1** | 0.2 | 0.8 |
| 3 | 0 | 1,062 | 19.5 | 21.8 | 55.0 | 2.2 | 1.6 |
| 3 | 1+ | 2,210 | 3.0 | 41.6 | 28.6 | 17.0 | 9.9 |
| 1–2 | 0 | 2,605 | 31.9 | 58.7 | 7.4 | 1.1 | 1.0 |
| 1–2 | 1+ | 2,591 | 5.8 | **58.6** | 7.1 | 19.0 | 9.5 |

Adopted rule:

```
levels >= 4                     → building=apartments   (95.6% across 9,294 pairs)
levels 1-2, 0 res. neighbours   → building=detached
levels 1-2, >=1 res. neighbour  → building=house
levels 3 or NULL                → building=residential
```

The `house` branch is directly supported: `house` is modal at 58.6%, and only
5.8% of those buildings were explicitly tagged `detached` by a mapper, so the
rule rarely contradicts one. The 28.5% that are `semidetached_house` or
`terrace` are refinements of `house`, not category errors.

**The `detached` branch is adopted on form, not on modal agreement**, and the
distinction matters. Nationally `house` outpolls `detached` 58.7% to 31.9% in
that bucket, but the split is a regional tagging convention rather than a
disagreement about the buildings:

| region | n | detached | house | either (single-family) |
|---|---|---|---|---|
| Warszawa | 453 | 84.8 | 3.3 | 88.1 |
| Katowice | 231 | 61.0 | 14.7 | 75.8 |
| Poznań | 1,169 | 13.8 | 74.8 | 88.5 |
| Podlasie | 752 | 19.1 | 80.5 | 99.6 |

`detached` and `house` are not competing claims — `detached` is the subtype of
`house` that asserts free-standing form. We measure that form directly, so where
a Poznań mapper wrote `house` for a free-standing building, `detached` is still
correct, just more specific. The share where `detached` is genuinely wrong in
form is the `apartments` + `semidetached_house` + `terrace` residual: **9.5%**.
It also makes EGIB consistent with BDOT10k, which already emits `detached` under
the same geometric test — without it the two sources would tag the same building
differently depending on which one happened to be unmatched.

One caveat worth recording: BDOT10k's ground truth put 0-neighbour agreement
with `detached` at 95.4%, against 31.9% here. Same country, same physical claim
— the gap is sampling, since the two ground-truth sets were drawn differently
and this one only covers the four regions that publish storey counts. The
form-based argument above is what carries the branch; the 95.4% figure should
not be read as corroborating it.

Three storeys is genuinely ambiguous (55.0% / 28.6% apartments across the two
neighbour bands) and stays generic.

Coverage is uneven and provider-dependent: `kondygnacje_nadziemne` is absent for
100% of rows in the Gdańsk, Kraków, Lublin and Wrocław samples but 0% in
Warszawa and 0.8% in Poznań. That affects only how many rows can be refined, not
whether the refinement is right — rows without a storey count simply keep
`residential`. Of the 236,571 unmatched `m` rows, 76,561 fall in the 1–2 band
and 5,906 at 4+, so **82,467 (34.9%) gain a more specific value**. The 1–2 band
splits roughly evenly between `detached` and `house` in this urban-weighted
sample; nationally `detached` should take the larger share, since rural cells
run ~94% zero-neighbour against ~30% in central Warsaw.

#### Methodological note

An earlier draft of this table derived EGIB's tags from BDOT10k's *internal*
general→detailed composition, which produced figures like 98.4% for `t` and
98.0% for `s`. Every one of them was too high — the geometric check came in at
79.5% and 62.6%. The error is structural: within BDOT10k the general category
and the detailed function are assigned by the same act, so they agree by
construction. **Internal consistency within one source says nothing about
agreement across sources.** Cross-source claims need a cross-source measurement.

Note that joining `egib_unmatched` back to `egib_buildings` on `id_budynku`
returns 2,243,725 rows against 2,233,840 actual unmatched rows — `id_budynku` is
not unique, a live illustration of why the serving tables carry columns rather
than id references. The figures above use the join and are therefore ~0.4% high
in absolute terms; the shares are unaffected.

## Adjacency

`building=detached` cannot be read from any attribute; it is a statement about
building form. It can be derived from geometry, and the derivation is
well supported.

Validated against 1,027 single-family buildings that Polish mappers had already
typed by hand:

| BDOT neighbours (≥3 m shared wall) | `detached` | `semidetached_house` | `house` |
|---|---|---|---|
| 0 | **330 (95.4%)** | 10 | 6 |
| 1 | 59 (13.0%) | 264 (58.0%) | 132 (29.0%) |
| 2 | 7 | 1 | **210 (95.5%)** |
| 3+ | 3 | 0 | 5 |

Adopted rule, for `budynek jednorodzinny` only:

```
0 neighbours  → building=detached
≥1 neighbours → building=house
```

The same test runs on EGIB, over residential rows in the 1–2 storey band — see
[Refining `m` by storey count](#refining-m-by-storey-count). Both sources
therefore need a neighbour count, and both restrict the neighbour set to the
same class they are classifying (`budynek jednorodzinny` for BDOT10k, `m` for
EGIB) so that an abutting garage or outbuilding never suppresses `detached`.

Three findings shaped this:

- **0 neighbours → `detached` agrees with mappers 95.4% of the time.** This is
  the win, and it is large: rural cells run ~94% zero-neighbour (Podlasie
  sample), urban ~30% (central Warsaw).
- **1 neighbour → `semidetached_house` was rejected.** It agrees only 58% of the
  time, and 13% of those buildings were explicitly tagged `detached` — a direct
  contradiction of the geometry, not merely a less specific choice.
- **`terrace` is never correct here.** It appeared zero times in the ground
  truth. `building=terrace` denotes the *whole row as one outline*; BDOT10k
  supplies individual units, so each unit is `house`.

The predicate used throughout the analysis in this document was:

```sql
NOT ST_Equals(a.geom, b.geom)
AND ST_Intersects(a.geom, b.geom)
AND ST_Length_Spheroid(ST_Intersection(ST_Boundary(a.geom), ST_Boundary(b.geom))) >= 3.0
```

The ≥3 m shared-boundary threshold was meant to separate real party walls
(clustering at 10–14 m) from digitization noise. **The implementation should
drop it and keep only the first two lines** — see the comparison below. Every
measurement in this document predates that finding and used the ≥3 m form; the
difference is ~1% of buildings, which does not disturb any conclusion drawn
here, but it does mean the tables are not exactly reproducible from the
recommended predicate.

`ST_Equals` rather than an id comparison, because neither `LOKALNYID` nor
`id_budynku` is unique.

#### Is the length test worth it, or would `ST_Intersects` do?

Measured on 36,748 EGIB `m` buildings across Warszawa, Poznań and rural
Podlasie. **The two are close enough that either is defensible**, and the length
test is the weaker of the two on the evidence that isolates what it is for.

*Cost is not the deciding factor.* Of the 4,481,512 candidate pairs the grid
produces, only 25,742 intersect at all, and the length refinement runs only on
those — 2.17 s against 2.01 s, **8%**. The candidate scan dominates either way.

*What the two disagree about.* Intersecting pairs break down as:

| intersection | pairs | share | Podlasie | Warszawa | Poznań |
|---|---|---|---|---|---|
| party wall (≥3 m) | 22,764 | 88.4% | 2,960 | 9,224 | 10,580 |
| overlap | 1,482 | 5.8% | 8 | 2 | **1,472** |
| corner touch (~0 m) | 1,114 | 4.3% | 28 | 134 | **952** |
| short wall (<3 m) | 382 | 1.5% | 20 | 134 | 228 |

The overlaps are **not** duplicate outlines for one building — checked
explicitly, and 1,511 of 1,513 overlap by under 10% of the smaller polygon, with
zero near-identical pairs. They are distinct adjacent buildings digitized so
their outlines clip each other by a few m². Such a pair is physically attached,
but its boundaries *cross* rather than run together, so the boundary
intersection is points with length 0 and the length test rejects it. That is a
miss, not a save.

*Ground truth on the disagreement set.* The 143 buildings where the rules differ
(`≥3 m` says free-standing, `ST_Intersects` says attached):

| region | n | detached | house | apartments | semi/terrace |
|---|---|---|---|---|---|
| Poznań | 127 | 7 | 104 | 1 | **15** |
| Warszawa | 15 | 7 | 2 | 2 | 4 |
| Podlasie | 1 | 0 | 1 | 0 | 0 |

Against a control set of buildings both rules call free-standing (Poznań, 1,042):
154 detached, 770 house, 109 apartments, 9 semi/terrace.

`detached` vs `house` cannot arbitrate this — Poznań mappers write `house` for
free-standing buildings anyway (770 vs 154 in the control). The confound-free
signal is `semidetached_house`/`terrace`, which assert attachment outright:
**11.8% of the disagreement set against 0.9% of the control, a ~13× enrichment.**
The buildings `ST_Intersects` additionally excludes really are attached more
often.

*But the aggregate barely moves*, because the dominant error in this band is
`apartments` contamination, which is a storey-count problem and orthogonal to
adjacency. Among everything each rule labels free-standing:

| region | ≥3 m: n / wrong form | `ST_Intersects`: n / wrong form |
|---|---|---|
| Podlasie | 752 / 3 (0.4%) | 751 / 3 (0.4%) |
| Poznań | 1,169 / 134 (11.5%) | 1,042 / 118 (11.3%) |
| Warszawa | 453 / 54 (11.9%) | 438 / 48 (11.0%) |

**Recommendation: use `ST_Intersects`.** It is slightly better on the metric
that isolates attachment, indistinguishable on the aggregate, marginally
cheaper, and has no tuned constant to justify. A hybrid (`≥3 m OR ≥1 m² overlap`)
was tested and proved identical to the plain length test — the clipping overlaps
are mostly under 1 m² — so it adds a second constant for nothing.

Warszawa's disagreement set leans the other way (7 of 15 genuinely `detached`),
so this is not clear-cut; n=15 is too small to weigh against Poznań's 127.

Two claims in earlier drafts of this document were wrong and are corrected here:
that the overlapping pairs were duplicate geometries, and that the ≥3 m
threshold was therefore load-bearing. The original BDOT10k sample that motivated
the threshold saw only 4 corner-touch pairs, which was too small a basis for the
constant.

### Where it runs

Adjacency compares a government source against itself — BDOT10k against
BDOT10k, EGIB against EGIB. It never reads OSM, so recomputing it in the
OSM-triggered `match_refresh` drain would be pure waste. It belongs at **import
and dataset-refresh time**, recomputed only for z14 cells the refresh actually
changed — which `dataset_change_areas` already tracks.

Note that a row's neighbour count can change because the *adjacent* row changed,
so the recompute set is the changed cells plus their buffer, not just the
changed rows.

**It must be cell-partitioned against the RTREE-indexed base table.** A
bbox-wide self-join staged through a temp table is O(n²) — temp tables carry no
RTREE — and never completed on 425k rows (killed at 590 s). Partitioned against
`bdot10k_buildings` with a constant bbox predicate it costs **0.4–1.4 s per z14
cell**. This is the same index-scan discipline recorded in
`docs/centroid_index_measured.md`.

The neighbour query needs a buffered read, since a neighbour can sit across the
cell edge — the `OSM_MATCH_BUFFER_DEG` pattern in `compare::rule` applies.

Restricting the computation to `budynek jednorodzinny` keeps it to 7.26M rows
rather than the full 16.3M. EGIB's equivalent restriction is `m` rows in the
1–2 storey band, which the storey-count coverage figures cut to a fraction of
the 2.4M residential set.

## Non-`building` tags

A small set of objects (~60,856 rows, 0.37%) are not ordinary buildings and
carry a second tag:

| function | rows | tags |
|---|---|---|
| silos, elewator | 1,590 | `building=silo` + `man_made=silo` |
| zbiornik na ciecz / na gaz | 584 | `building=storage_tank` + `man_made=storage_tank` |
| wiatrak | 163 | `building=yes` + `man_made=windmill` |
| latarnia morska | 17 | `building=lighthouse` + `man_made=lighthouse` |

### Every entry must include a `building` key

This is a hard invariant, and it is load-bearing.

`osm_buildings` is populated solely from the presence of a `building` tag —
`element_at(tags, 'building')` in `import::osm` and `building_tag.is_some()` in
`update::osm`. There is no `man_made` fallback. An object imported into OSM
without `building=*` therefore never enters `osm_buildings`, the match rule
never sees it, and **the tool re-suggests it in every package forever.**

The invariant does not distort tagging, which was verified rather than assumed.
Restricted to ways in Poland — polygons are all we emit — `building=*`
co-occurrence is:

| tag | ways in PL | with `building=*` |
|---|---|---|
| `man_made=silo` | 11,738 | 82.8% |
| `man_made=windmill` | 211 | 85.8% |
| `man_made=water_tower` | 760 | 80.0% |
| `man_made=storage_tank` | 6,958 | 73.3% |
| `man_made=lighthouse` | 14 | 78.6% |

Global taginfo looks far worse (silo 63%, water_tower 13%, lighthouse ~0%)
because these are predominantly mapped as *nodes* worldwide, and a node has no
building tag. The polygon-restricted Polish figure is the applicable one.

The check also improved the choices: dedicated `building=silo` (1,766 PL uses),
`building=storage_tank` (1,427) and `building=lighthouse` values exist, and are
better than the `building=yes` pairs originally proposed.

If a class is ever found where `building=*` would be genuinely wrong, the
correct response is to **exclude those rows from the unmatched serving table**,
not to emit a feature the compare cannot track. That keeps the decision explicit
in the mapping table instead of silently creating a re-suggestion loop.

Use-asserting tags (`amenity=*`, `shop=*`, `tourism=*`) stay out of scope. They
describe occupancy rather than structure, and BDOT10k's function field is
frequently stale about occupancy.

## `fixme` for ambiguous classes

Where a class is genuinely bimodal, `building=yes` throws away information the
registry does have. Emitting the ambiguity instead — `building=yes` plus a
`fixme` naming the two candidates — lets a mapper resolve it in JOSM before
upload, which is exactly where the local knowledge is.

**This needs no new machinery.** The `tags` column is already `;`-separated
`k=v` pairs, so a row reads:

```csv
tier,key,min_levels,max_levels,max_neighbours,tags,note
1,t,,,,building=yes;fixme=EGiB: budynki transportu i łączności,w praktyce garaż lub budynek gospodarczy
```

The `fixme` states what the source recorded and stops there. It is not an
instruction — the mapper knows what to do with a classification they can see is
coarse, and 200k copies of a politely worded request read as noise. The
interpretation (that EGIB `t` means garage or shed in practice) belongs in the
`note` column, which is for whoever maintains the file, not for OSM.

It is also safe against the re-suggestion trap: `osm_buildings` stores only the
`building` value, so a building uploaded carrying a `fixme` still matches
normally and leaves the package. The `fixme` is invisible to the compare.

### Scale is the constraint

Poland currently has **5,517 buildings with a `fixme`**, out of 17,976,041
building ways — 0.031%. (14,835 ways carry one across all feature types.)
Measured from `example_data/OSM/poland-2026-08-01.osm.pbf`.

There is good precedent for import-generated ones: the most common values on
buildings are `sprawdzić/importować adres` (472), `Ten adres jest zdublowany w
oryginalnych danych` (198) and `Duplicate address in import` (139). The Polish
community accepts the pattern. But it accepts it at a scale of hundreds, and the
candidate classes here are much larger:

| class | rows | adopted | is the message informative? |
|---|---|---|---|
| BDOT10k's 26 `yes` functions | 3,353 | **yes** | the exact function is known, only the OSM value is unclear |
| EGIB `t` | 162,618 | **yes** | garage or outbuilding, and the pair is 92–98% stable |
| EGIB `p` | 23,386 | **yes** | `industrial` is right only 41.0%, so the value is dropped to `yes` |
| EGIB `k` | 8,473 | **yes** | a generalisation over a three-way split |
| EGIB `rodzaj IS NULL` | 122,965 | no | there is nothing to say |
| EGIB `i` | 658,323 | no | a dumping ground; the honest message is "could be anything" |
| EGIB unresolved | 3,073 | no | as above |

The test is whether the text tells the mapper something they can act on without
a site visit. For `t` it does: it names two specific alternatives and the
registry is right about the pair 92–98% of the time. For `i` it does not, and
658k uninformative task markers would be 119× the existing national stock — the
kind of thing that gets an import reverted rather than praised.

Note that `t` is the garage/outbuilding case, not `g`: `g` is 88.9%
`budynek gospodarczy` and takes `building=outbuilding` outright.

The adopted set totals **197,830 rows, ~36× the current national stock** of
building `fixme`s. That is a large multiplier and worth stating plainly to the
community before the first package ships; it is defensible because every one of
those rows names concrete alternatives, but it is not a quiet change.

### `p` and `k`: flagging a value we chose

`t` and BDOT10k's 26 decline to choose. `p` and `k` do choose, and the `fixme`
admits the choice is weak — a different act, and the more useful one, since a
confident-looking wrong tag is less likely to be re-examined than a bare
`building=yes`.

```csv
1,p,,,,building=yes;fixme=EGiB: budynki przemysłowe,industrial tylko 41.0%
1,k,,,,"building=civic;fixme=EGiB: budynki oświaty, nauki i kultury oraz budynki sportowe",uogólnienie
```

Note the `k` row contains commas inside the `fixme` value, so the file needs
proper CSV quoting and the loader must not be tempted into a naive `split(',')`.
DuckDB's `read_csv` handles it; the existing street-name file already relies on
the same quoting for nicknames. This bit during authoring: a typo fix applied by
raw string replace inserted a comma into an unquoted `note` and silently
collapsed the file to one column, which the sniffer accepted without complaint.
**The loader should assert the column count rather than trust the sniffer.**

**`p` was dropped from `building=industrial` to `building=yes`.** Asserting
`industrial` while the `fixme` offers `warehouse` and `service` as alternatives
is self-contradictory, and 41.0% does not deserve the confidence a bare
`building=industrial` projects. `k` keeps `civic`, which is a true
generalisation over its three-way split rather than a bet on one outcome —
`school`, `university` and `kindergarten` are all `civic`, so the tag is not
wrong, merely coarse.

This makes `p` the one class where the `fixme` route *reduces* the specificity
we emit. That is the correct trade: 23,386 rows carrying an honest question
beat 23,386 carrying a 59%-wrong answer.

### `fixme` or `note`?

`fixme` enters QA tooling (Osmose, KeepRight, the JOSM validator) and becomes a
work item for the whole community, not just the importer. `note` is
informational and does not. What we are conveying is source ambiguity rather
than a known defect, which argues for `note` — but `fixme` is what actually gets
acted on. This is a call for the Polish community rather than for this document;
the CSV expresses either at no cost, and the choice can be changed by editing a
file.

## Where the mapping is applied

**Carry the raw classification columns into `*_unmatched` at compare time;
apply the mapping at serve time.**

This preserves the property that makes the street-name mapping pleasant to
maintain: editing a mapping requires no `compare`, no reconcile, and no drain —
it changes only what `/package` renders.

Two alternatives were rejected:

- **Apply at import time.** A mapping edit would rewrite 16.3M rows, and putting
  a mapped value inside `hashed_select`'s projection would force a
  `ROW_HASH_VERSION` bump on every mapping change.
- **Apply at serve time by joining back to the source table.** Barred outright
  by the serving-table invariant in `CLAUDE.md`: `*_unmatched` stores rows, not
  id references. BDOT10k's `LOKALNYID` is not unique and DuckDB rowids are not
  stable across the DELETE+INSERT that every recompute performs, so the join
  would go stale silently.

Concretely, this means:

- `bdot10k_unmatched` gains `funkcja_szczegolowa`, `funkcja_ogolna` and a
  precomputed `neighbours` count; `egib_unmatched` gains `rodzaj`,
  `kondygnacje_nadziemne` and its own precomputed `neighbours` count.
- The columns are added the way `centroid` was — outside `hashed_select`'s
  projection (`DatasetSpec::with_centroid_select`) — so they never affect
  `_row_hash` and need no `ROW_HASH_VERSION` bump.
- `server::package::building_tags` becomes a function of those columns instead
  of a constant. Its test `building_tags_are_fixed` will need replacing.
- `make_seeded_state` in `server/package.rs` builds its tables from a local
  `SEED` constant rather than from `create_schema`, so the new columns must be
  added in **both** places.
- `server::tiles::BUILDINGS_MVT_SQL` can surface the type cheaply once the
  column is carried, which makes the tile layer far more useful for review.

As with `centroid`, there is no migration path: databases built before the
change must re-run `import bdot10k` / `import egib` to gain the columns.

## Packaging the mappings as CSV

**Yes — one file per source, loaded exactly like `street_names_mappings.csv`.**

The case for it is stronger than the case for a Rust table, and rests on which
part of the mapping is volatile. The keys are fixed by the BDOT10k XSD and by
EGIB's letter codes. The *tags* are not: they are a judgement about Polish OSM
convention, and this document already contains a live example of one that is
expected to move — the six `urząd` entries use `building=government` over
`building=townhall` purely on current usage counts. Decisions of that kind are
best reviewed by the mappers who will consume the packages, and a CSV diff in a
pull request is reviewable by someone who does not write Rust. Since the mapping
is applied at serve time, a corrected file also takes effect without a
`compare`, a reconcile, a drain, or — unlike a Rust table — a redeploy.

### Files

`mappings/bdot10k_building_types.csv` and `mappings/egib_building_types.csv`,
alongside the existing street-name file, with matching `download_urls` and
`jobs` entries so the same background refresh applies.

```csv
tier,key,min_levels,max_levels,max_neighbours,tags
1,budynek gospodarczy,,,,building=outbuilding
1,silos,,,,building=silo;man_made=silo
1,budynek jednorodzinny,,,0,building=detached
1,budynek jednorodzinny,,,,building=house
2,budynki transportu i łączności,,,,building=garage
```

and for EGIB, where the same three constraint columns express the storey rule:

```csv
tier,key,min_levels,max_levels,max_neighbours,tags,note
1,m,4,,,building=apartments,
1,m,1,2,0,building=detached,
1,m,1,2,,building=house,
1,m,,,,building=residential,3 storeys or unknown
1,g,,,,building=outbuilding,
```

- `tier` — which column the key reads from: 1 is
  `PRZEWAZAJACAFUNKCJABUDYNKU` / `rodzaj`, 2 is `FUNKCJAOGOLNABUDYNKU`. Keeps
  both cascade levels in one file per source, as asked, without a sparse
  two-key-column layout.
- `tags` — `;`-separated `k=v` pairs, the JOSM convention. This is what makes
  the `man_made` pairs expressible at all.
- `min_levels` / `max_levels` / `max_neighbours` — optional, inclusive. Empty
  means unconstrained.
- `note` — free text for whoever maintains the file, never emitted. **Optional
  as a column**: `bdot10k_building_types.csv` omits it entirely, because a
  per-row comment on 178 mechanical rows is filler, while
  `egib_building_types.csv` keeps it to record each letter's measured agreement.
  The loader must therefore tolerate its absence rather than require a fixed
  seven-column shape.
- Precedence: the **most-constrained matching row wins**; two rows matching with
  equal constraint counts is a load error, not a silent pick. Order in the file
  is not significant.

### What must stay in code

The constraint columns *reference* precomputed columns; they never compute
anything. Storey count is loaded from the source, and the neighbour count is the
adjacency self-join described above — both are stored on the `*_unmatched` rows
before the mapping is ever consulted. The CSV chooses tags given those numbers,
which is the whole of its job.

That boundary is the thing to defend. Those three constraint columns are a
closed set: adding `min_area`, `has_address` or similar turns the file into a
rules engine and the loader into an interpreter. A new *kind* of condition
should be a code change and a decision recorded here; only new *rows* should be
a CSV change.

### Validation

Mirroring `mappings::validate_and_swap`, all-or-nothing into a `__staging`
table so a bad file leaves the previous mapping intact:

- every `tags` value parses, and **every row includes a `building` key** — this
  turns the hard invariant from [Every entry must include a `building`
  key](#every-entry-must-include-a-building-key) from prose into something
  enforced at load;
- no duplicate `(tier, key, constraints)`, and no two rows matching with equal
  specificity;
- `min_levels <= max_levels`; `max_neighbours >= 0`;
- **key drift, in both directions** — this is the analogue of
  `rows_absent_from_prg`, and it is more useful here because the key space is
  closed. Keys in the file that appear in no source row are probably typos or a
  removed schema value. Keys in the source that the file does not cover are
  reported with their row counts; for BDOT10k tier 1 that count should be zero,
  and anything else means a schema revision.

Both counts belong in `LoadStats` and in the `job_log` entry, so `/status`
surfaces them the way it does for the street mapping.

### The one real cost

Two sources of truth for the same knowledge: an unmapped key can no longer be a
non-compiling `match` arm. It becomes a runtime warning plus a `building=yes`
fallback. Given that the keys arrive as strings from a Parquet file, that check
could never have been a compile-time one anyway — an exhaustive `match` would
have been over a Rust enum that some parsing step had to produce from those
strings, with the same runtime fallback underneath. The safety being given up is
smaller than it looks.

## Suggested implementation order

Each step is independently shippable and independently verifiable.

1. **Carry the raw columns.** Add `funkcja_szczegolowa` / `funkcja_ogolna` to
   `bdot10k_unmatched` and `rodzaj` / `kondygnacje_nadziemne` to
   `egib_unmatched`, in `create_schema` *and* in `SEED`. No behaviour change yet;
   verify by re-importing and checking the columns populate.
2. **The CSV loader.** Generalise `src/mappings.rs` to load a building-type file
   into a `<source>_building_types` table with the validation above. Nothing
   consumes it yet, so it can be tested in isolation.
3. **BDOT10k tier 1 + tier 2.** Populate `mappings/bdot10k_building_types.csv`
   from [Appendix A](#appendix-a--the-full-tier-1-table) and turn `building_tags`
   into a lookup against it. This alone covers 98.14% of the BDOT10k import and
   is the bulk of the value.
4. **EGIB letters.** [Appendix B](#appendix-b--resolving-egib_buildingsrodzaj-to-a-letter)
   plus the ten-letter table, as a second CSV. Volume is much larger than
   BDOT10k and confidence is lower, so it is worth a separate reviewable change.
   Note the `rodzaj` cascade itself stays in code — it is normalisation of a
   messy input, not a tagging judgement.
5. **`building:levels`.** Independent of everything above and the largest single
   win per line of code — resolve the `LICZBAKONDYGNACJI` question in
   [Open items](#open-items) first.
6. **Adjacency.** Last, because it is the only step that needs a new computed
   column, a cell-partitioned recompute, and a hook into dataset refresh. Until
   it lands, `budynek jednorodzinny` emits `building=house` and EGIB's 1–2 band
   emits `building=residential` — both are safe, merely less specific.

For testing, the existing `building_tags_are_fixed` should be replaced by cases
that pin each decision *class* rather than the whole table: tier-1 hit, tier-2
fallthrough, unmapped value → `yes` plus a logged warning, a `man_made` pair,
both adjacency branches, and each EGIB cascade tier. Pinning all 167 rows would
make the test a copy of the CSV and would fail for any addition rather than for
any regression. The loader's own rejection cases — missing `building` key,
duplicate specificity, `min_levels > max_levels` — are worth pinning
individually, since they are what stands between a bad pull request and a bad
package.

## Open items

- **Region-conditioned EGIB `t`.** 162,618 unmatched rows currently take
  `building=yes` because the category means "garage" in some voivodeships and
  "shed" in others, while the *pair* is 92–98% stable. Recovering them needs a
  voivodeship/TERYT attribution the pipeline does not carry today. Until then,
  the [`fixme`](#fixme-for-ambiguous-classes) route conveys the same ambiguity
  to the mapper without needing that attribution.
- **EGIB `p`, `s`, `k`.** Adopted at 41.0%, 62.6% and "generalisation"
  respectively — the weakest values in the EGIB table, together 2.96% of the
  unmatched set. Worth re-measuring on a rural-weighted sample.
- ~~**`building:levels`.**~~ **Implemented.** Confirmed against GUGiK's
  official BDOT10k/BDOO object catalogue: `liczbaKondygnacji` is defined as
  "liczba nadziemnych kondygnacji budynku" — above-ground storeys only, the
  same thing EGIB's `kondygnacje_nadziemne` already names explicitly and the
  same thing OSM's `building:levels` counts. No unit conversion needed on
  either source. `0` ("budynek nie posiada kondygnacji") and a missing value
  are both treated as nothing to report. See
  `server::package::with_building_levels`.
- **Values chosen on popularity over precision.** The six `urząd`/town-hall
  entries use `building=government` (1,675 PL uses) rather than the semantically
  exact `building=townhall` (50). Reviewed and accepted; noted here because the
  balance may shift as `townhall` usage grows.

## Appendix A — the full tier-1 table

All 167 values of `OT_FunSzczegolowaBudynkuType`, reviewed and settled. `unmatched`
is the row count in `bdot10k_unmatched` at the time of analysis (2026-08); entries
with `0` exist in the schema, and sometimes in the data, but were fully matched
then. The counts are provenance only — the mapping does not depend on them.

Grouping is editorial, to make review tractable. The implementation needs a flat
lookup keyed on the function string.

This appendix is the **seed for `mappings/bdot10k_building_types.csv`** (see
[Packaging the mappings as CSV](#packaging-the-mappings-as-csv)). Once that file
exists it becomes the source of truth and this appendix is historical — prefer
deleting it over letting the two drift.

**Residential**

| function | unmatched | tags |
|---|---|---|
| budynek jednorodzinny | 208718 | `building=detached` (0 nbrs) / `building=house` (≥1) |
| dom letniskowy | 28509 | `building=bungalow` |
| budynek wielorodzinny | 15839 | `building=apartments` |
| domek kempingowy | 1341 | `building=cabin` |
| dom wypoczynkowy | 535 | `building=hotel` |
| dom opieki społecznej | 330 | `building=residential` |
| pensjonat | 209 | `building=hotel` |
| internat lub bursa szkolna | 113 | `building=dormitory` |
| leśniczówka | 77 | `building=house` |
| ośrodek szkoleniowo-wypoczynkowy | 73 | `building=yes` |
| zajazd | 67 | `building=hotel` |
| placówka opiekuńczo-wychowawcza | 95 | `building=residential` |
| dom studencki | 28 | `building=dormitory` |
| bacówka | 28 | `building=hut` |
| dom dziecka | 26 | `building=residential` |
| schronisko turystyczne | 24 | `building=yes` |
| motel | 14 | `building=hotel` |
| dom dla bezdomnych | 14 | `building=residential` |
| hotel robotniczy | 10 | `building=dormitory` |
| hotel | 764 | `building=hotel` |

**Farm / outbuilding**

| function | unmatched | tags |
|---|---|---|
| budynek gospodarczy | 416851 | `building=outbuilding` |
| garaż | 25881 | `building=garage` |
| budynek produkcyjny zwierząt hodowlanych | 2768 | `building=farm_auxiliary` |
| szklarnia lub cieplarnia | 2590 | `building=greenhouse` |
| silos | 300 | `building=silo` + `man_made=silo` |
| stajnia | 149 | `building=stable` |
| młyn | 90 | `building=yes` |
| elewator | 84 | `building=silo` + `man_made=silo` |
| chłodnia | 81 | `building=warehouse` |
| wiatrak | 33 | `building=yes` + `man_made=windmill` |
| ujeżdżalnia | 16 | `building=riding_hall` |

**Commercial / retail**

| function | unmatched | tags |
|---|---|---|
| obiekt handlowo-usługowy | 8717 | `building=retail` |
| siedziba firmy lub firm | 3890 | `building=office` |
| restauracja | 825 | `building=yes` |
| dom towarowy lub handlowy | 682 | `building=retail` |
| stacja paliw | 289 | `building=retail` |
| stacja obsługi pojazdów | 195 | `building=service` |
| dom weselny | 119 | `building=commercial` |
| bank | 112 | `building=commercial` |
| hipermarket lub supermarket | 99 | `building=supermarket` |
| apteka | 97 | `building=retail` |
| placówka operatora pocztowego | 78 | `building=commercial` |
| myjnia samochodowa | 58 | `building=service` |
| klinika weterynaryjna | 55 | `building=commercial` |
| centrum handlowe | 51 | `building=retail` |
| klub, dyskoteka | 17 | `building=commercial` |
| budynek spedycji | 14 | `building=warehouse` |
| centrum konferencyjne | 13 | `building=commercial` |
| kino | 10 | `building=commercial` |
| hala targowa | 7 | `building=retail` |
| centrum informacyjne | 5 | `building=yes` |
| kasyno | 3 | `building=commercial` |

**Industrial / utility**

| function | unmatched | tags |
|---|---|---|
| magazyn | 11439 | `building=warehouse` |
| produkcyjny | 9841 | `building=industrial` |
| warsztat remontowo-naprawczy | 1972 | `building=yes` |
| stacja pomp | 556 | `building=service` |
| stacja transformatorowa | 511 | `building=service` |
| kotłownia | 257 | `building=service` |
| zbiornik na ciecz | 156 | `building=storage_tank` + `man_made=storage_tank` |
| elektrownia | 107 | `building=industrial` |
| elektrociepłownia | 101 | `building=industrial` |
| stacja gazowa | 74 | `building=service` |
| hangar | 69 | `building=hangar` |
| zbiornik na gaz | 9 | `building=storage_tank` + `man_made=storage_tank` |
| rafineria | 4 | `building=industrial` |
| spalarnia śmieci | 2 | `building=industrial` |
| stacja nadawcza radia i telewizji | 1 | `building=service` |
| centrum telekomunikacyjne | 1 | `building=yes` |

**Transport**

| function | unmatched | tags |
|---|---|---|
| budynek kontroli ruchu kolejowego | 155 | `building=transportation` |
| dworzec kolejowy | 70 | `building=train_station` |
| parking wielopoziomowy | 32 | `building=parking` |
| lokomotywownia lub wagonownia | 19 | `building=transportation` |
| stacja kolejki górskiej lub wyciągu krzesełkowego | 18 | `building=transportation` |
| dworzec autobusowy | 17 | `building=transportation` |
| budynek kontroli ruchu powietrznego | 8 | `building=transportation` |
| dworzec lotniczy | 7 | `building=transportation` |
| terminal portowy | 4 | `building=transportation` |
| przejście graniczne | 2 | `building=yes` |
| zajezdnia autobusowa | 1 | `building=transportation` |
| zajezdnia tramwajowa | 1 | `building=transportation` |
| kapitanat lub bosmanat portu | 1 | `building=yes` |
| stacja nautyczna | 0 | `building=yes` |
| zajezdnia trolejbusowa | 0 | `building=transportation` |
| latarnia morska | 3 | `building=lighthouse` + `man_made=lighthouse` |

**Education**

| function | unmatched | tags |
|---|---|---|
| szkoła podstawowa | 2843 | `building=school` |
| szkoła ponadpodstawowa | 923 | `building=school` |
| przedszkole | 689 | `building=kindergarten` |
| szkoła wyższa | 300 | `building=university` |
| biblioteka | 134 | `building=civic` |
| hala sportowa | 125 | `building=sports_hall` |
| sala gimnastyczna | 103 | `building=sports_hall` |
| żłobek | 70 | `building=kindergarten` |
| placówka badawcza | 70 | `building=office` |
| inna placówka edukacyjna | 19 | `building=education` |
| archiwum | 7 | `building=civic` |
| obserwatorium lub planetarium | 4 | `building=yes` |

**Health**

| function | unmatched | tags |
|---|---|---|
| placówka ochrony zdrowia | 561 | `building=healthcare` |
| szpital | 484 | `building=hospital` |
| sanatorium | 79 | `building=hospital` |
| jednostka ratownictwa medycznego | 15 | `building=yes` |
| hospicjum | 15 | `building=hospital` |
| stacja sanitarno-epidemiologiczna | 8 | `building=public` |
| stacja krwiodawstwa | 2 | `building=healthcare` |
| izba wytrzeźwień | 2 | `building=yes` |

**Public administration / civic**

| function | unmatched | tags |
|---|---|---|
| straż pożarna | 487 | `building=fire_station` |
| dom kultury | 654 | `building=civic` |
| muzeum | 451 | `building=museum` |
| inny urząd administracji publicznej | 209 | `building=government` |
| policja | 172 | `building=civic` |
| koszary | 137 | `building=barracks` |
| zabudowania koszarowe | 90 | `building=barracks` |
| urząd miasta | 76 | `building=government` |
| sąd | 70 | `building=government` |
| zakład karny lub poprawczy | 60 | `building=civic` |
| ośrodek pomocy społecznej | 60 | `building=civic` |
| urząd gminy | 59 | `building=government` |
| toaleta publiczna | 48 | `building=toilets` |
| starostwo powiatowe | 38 | `building=government` |
| straż graniczna | 29 | `building=government` |
| zakład karny | 28 | `building=civic` |
| urząd miasta i gminy | 28 | `building=government` |
| urząd marszałkowski | 15 | `building=government` |
| areszt śledczy | 12 | `building=civic` |
| ministerstwo | 12 | `building=government` |
| urząd wojewódzki | 10 | `building=government` |
| prokuratura | 9 | `building=government` |
| urząd celny | 8 | `building=government` |
| placówka dyplomatyczna lub konsularna | 4 | `building=government` |
| rezydencja ambasadora | 0 | `building=yes` |
| zakład poprawczy | 3 | `building=civic` |
| schronisko dla nieletnich | 1 | `building=civic` |
| rezydencja prezydencka | 1 | `building=yes` |

**Religion**

| function | unmatched | tags |
|---|---|---|
| dom parafialny | 244 | `building=presbytery` |
| kaplica | 189 | `building=chapel` |
| klasztor | 181 | `building=monastery` |
| dzwonnica | 148 | `building=bell_tower` |
| kościół | 143 | `building=church` |
| dom zakonny | 28 | `building=monastery` |
| dom rekolekcyjny | 19 | `building=religious` |
| budynki cmentarne | 14 | `building=yes` |
| inny budynek kultu religijnego | 13 | `building=religious` |
| cerkiew | 7 | `building=church` |
| synagoga | 0 | `building=synagogue` |
| rezydencja biskupia | 2 | `building=religious` |
| kuria metropolitalna | 1 | `building=religious` |
| meczet | 1 | `building=religious` |
| dom pogrzebowy | 25 | `building=yes` |
| krematorium | 4 | `building=yes` |

**Sport / leisure / other**

| function | unmatched | tags |
|---|---|---|
| klub sportowy | 310 | `building=sports_centre` |
| zabytek niepełniący żadnej funkcji użytkowej | 152 | `building=yes` |
| schronisko dla zwierząt | 48 | `building=yes` |
| budynek ogrodu zoologicznego lub botanicznego | 45 | `building=yes` |
| basen kąpielowy | 31 | `building=sports_centre` |
| korty tenisowe | 21 | `building=yes` |
| strzelnica | 21 | `building=yes` |
| teatr | 13 | `building=civic` |
| hala widowiskowa | 10 | `building=civic` |
| galeria sztuki | 9 | `building=civic` |
| sztuczne lodowisko | 6 | `building=sports_centre` |
| pawilon ogrodowy lub oranżeria | 5 | `building=yes` |
| hala wystawowa | 4 | `building=civic` |
| filharmonia | 3 | `building=civic` |
| opera | 0 | `building=civic` |
| halowy tor gokartowy | 0 | `building=sports_centre` |
| kręgielnia | 2 | `building=sports_centre` |
| stacja meteorologiczna | 2 | `building=yes` |
| stacja hydrologiczna | 1 | `building=yes` |

## Appendix B — resolving `egib_buildings.rodzaj` to a letter

The four-tier cascade described under [EGIB](#egib), as validated against the
production table. `r` is `lower(trim(rodzaj))`. Order matters: tier 1's prefix
match is last, because it would otherwise swallow tier-3 and tier-4 strings that
also begin with a KŚT letter.

```sql
CASE
  -- tier 4: combined, e.g. 'm - budynki mieszkalne (110)'
  WHEN regexp_matches(r, '^[a-z] - .* \(\d+\)$')          THEN r[1:1]
  -- tier 3: full KŚT category name
  WHEN r LIKE 'budynki mieszkalne%'                       THEN 'm'
  WHEN r LIKE 'budynki biurowe%'                          THEN 'b'
  WHEN r LIKE 'budynki handlowo-usługowe%'                THEN 'h'
  WHEN r LIKE 'budynki przemysłowe%'                      THEN 'p'
  WHEN r LIKE 'budynki transportu%'                       THEN 't'
  WHEN r LIKE 'budynki szpitali%'                         THEN 'z'
  WHEN r LIKE 'budynki oświaty%'                          THEN 'k'
  WHEN r LIKE 'budynki produkcyjne%'                      THEN 'g'
  WHEN r LIKE 'zbiorniki, silosy%'                        THEN 's'
  WHEN r LIKE 'pozostałe budynki niemieszkalne%'          THEN 'i'
  -- tier 2: camelCase enum, lowercased and diacritic-free as stored
  WHEN r IN ('mieszkalny','budynekmieszkalny')            THEN 'm'
  WHEN r = 'biurowy'                                      THEN 'b'
  WHEN r = 'handlowouslugowy'                             THEN 'h'
  WHEN r = 'przemyslowy'                                  THEN 'p'
  WHEN r = 'transportuilacznosci'                         THEN 't'
  WHEN r = 'szpitalaiinnebudynkiopiekizdrowotnej'         THEN 'z'
  WHEN r = 'oswiatynaukiikulturyorazsportu'               THEN 'k'
  WHEN r = 'produkcyjnyuslugowyigospodarczy'              THEN 'g'
  WHEN r = 'zbiorniksilosibudynekmagazynowy'              THEN 's'
  WHEN r = 'budynekniemieszkalny'                         THEN 'i'
  -- tier 1: bare letter code with optional local suffix ('mj', 'm2', 'b.')
  WHEN regexp_matches(r, '^[mgithpskbz][a-z]?[0-9]?\.?$') THEN r[1:1]
  ELSE NULL                                               -- -> building=yes
END
```

Unlike BDOT10k's closed enumeration, this cascade is **empirical** — it was
derived from the 76 distinct values present in the production table, not from a
published schema. A new voivodeship provider can introduce a form it does not
cover. The `ELSE NULL` branch is therefore an expected condition rather than a
schema violation, and should be counted and reported through `job_log` rather
than logged as an error the way an unknown BDOT10k function is.
