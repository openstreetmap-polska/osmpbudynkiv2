# The `ulica` prefix in PRG street names

Investigated 2026-08-09 against the database built from
`PRG-punkty_adresowe_2026-08-01.zip`.

## Symptom

Street names served by `/tiles` and `/package` were widely prefixed with the
word `ulica` — `ulica Wał Miedzeszyński` rather than `Wał Miedzeszyński`. It
looked national in the frontend.

## Verdict: the source data

Not `prg_convert`, and not our comparison or serving code.

The PRG 2021 GML declares the street type *and* embeds it in the name:

```xml
<prgad:nazwaPelna>ulica Wał Miedzeszyński</prgad:nazwaPelna>
<prgad:rodzaj>1</prgad:rodzaj>                                  <!-- 1 = ulica -->
<prgad:TERYTNazwa1>ulica Wał Miedzeszyński</prgad:TERYTNazwa1>
<prgad:identyfikatorULIC>23573</prgad:identyfikatorULIC>
```

under the `PL.ZIPIN.1469.EMUiA_*` namespace — Warsaw's EMUiA operator. TERYT
ULIC for `SYM_UL=23573` carries `CECHA='ul.'` and `NAZWA_1='Wał Miedzeszyński'`
as separate fields, so the registry's own dictionary agrees the name is just
`Wał Miedzeszyński`. PRG duplicated the cecha into the name field.

`prg_convert`'s `model2021::STREET_TYPE` maps `rodzaj=1` to `""` precisely so
that "ulica" is never prepended, and `construct_full_name_from_parts` only ever
*adds* a missing type word — it has no branch that removes one already present
in `TERYTNazwa1`. So it passes the name through verbatim, which is correct
behaviour given self-contradictory input.

`model2012.rs`'s naive `przedrostek1 + przedrostek2 + nazwa` concatenation
*would* produce this shape, but it is not involved: `import::prg` only processes
`NOWE_*.gml` entries and ignores the legacy `.xml` files in the same zip.

Not a regression in the 2026-08-01 distribution either — the same three tags are
byte-identical in `PRG-punkty_adresowe_2026-03-15.zip`.

## Scope

Three forms occur, and only three:

| form | rows | distinct names |
|---|---:|---:|
| `ulica ` | 122,822 | 4,729 |
| `ul. ` | 3 | 1 |
| `Ulica ` | 1 | 1 |
| **total** | **122,826** | **4,731** |

122,822 of those rows are gmina `1465011` (Warszawa) — 97% of Warsaw's 126,573
addresses. Every other major city (Kraków, Łódź, Wrocław, Poznań, Gdańsk …) has
zero. Nationwide there are exactly four strays outside Warsaw:

- `ul. Szkolna` — Jata (×2) and Sójkowa (×1), woj. 18
- `Ulica Szkółka Brzeźnica` — Połomia, gmina `2413082`, woj. 24

It read as national in the frontend because testing happened over Warsaw and
`/tiles`' `addresses_all` legend layer (`ALL_ADDRESSES_MVT_SQL`) serves raw
`prg_addresses.ulica` for every address, matched or not. The comparison-derived
`prg_unmatched` table was much less affected: 1,579 of 556,490 rows.

## Why stripping is the right output

Two independent checks, both against the 4,730 distinct names in the
`ulica `/`Ulica ` set:

- **TERYT ULIC** — 3,275 stripped names match the official dictionary exactly
  (1 matches before stripping). The 1,455 that don't match have no ULIC row at
  all for that `SYM_UL`/`WOJ` pair; none of them is a *name disagreement*.
  (Join on `SYM_UL` + `WOJ`, not on the gmina code: Warsaw's ULIC rows are keyed
  at dzielnica level — `GMI='07'`/`'14'`, `RODZ_GMI=8` — while PRG's
  `teryt_gmina` is the city code `1465011`.)
- **OSM** — 4,658 stripped names appear verbatim in `osm_addresses.street`.
  Only 3 of the raw prefixed forms do, and those look like PRG-copied tagging
  errors in OSM itself.

## Do not generalize the strip

Warsaw spells out other cecha words too, and those are correct. `Aleja`,
`Aleje`, `Trakt` and `Osiedle` account for 70,574 rows nationwide (3,407 of them
in Warsaw) and OSM uses those exact forms — `Aleja Krakowska` (1,128 objects),
`Aleje Jerozolimskie` (770). Only `ulica` and `ul.` may be removed; widening the
pattern to the cecha words in general would corrupt all 70,574.

## The fix

`import::prg::ULICA_PREFIX_STRIP_SQL`, applied in `materialize_into` — the one
funnel both `import prg` and `update prg`'s staging load pass through. It sits
inside `hashed_select`'s projection, so it moved `ROW_HASH_VERSION` from 1 to 2;
see the corresponding gotcha in `CLAUDE.md` for why that placement was chosen
over the outside-the-hash wrapping used by `centroid` and `rodzaj_kod`.

Fixing this required no change to the match rule. `compare::addresses` joins on
`UPPER(TRIM(numer_porzadkowy))` plus a grid key plus `ST_Distance_Sphere`, and
reads `ulica` only in its final projection — street names have never influenced
which addresses count as unmatched. Rejected alternatives: patching
`prg_convert` upstream (correct, but the fix belongs where the data lands), and
adding ~4,729 rows to `mappings/street_names_mappings.csv` (that file is a
curated exception list, not a place for a systematic defect — and it currently
contains zero prefixed keys, which is why the serving-time mapping join keeps
working unchanged after the strip).
