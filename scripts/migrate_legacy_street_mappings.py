#!/usr/bin/env python3
"""One-shot migration of the legacy gugik2osm street-name mapping file.

ALREADY RUN. `mappings/street_names_mappings.csv` is this script's output;
ongoing changes to that file are hand-made PRs, not re-runs of this script.
It is kept because it records ~50 curation decisions together with the
evidence for each, and because it fails loudly if any of them goes stale --
an ambiguous key with no recorded decision, a decision naming a row that is
not in the source, or a correction that matches nothing. See
docs/superpowers/specs/2026-08-01-prg-street-name-mappings-design.md.

What it does: collapses the legacy per-settlement file into global rows
(empty SIMC) wherever every settlement agreed on one OSM name, keeps
settlement-scoped rows only where they earn their place, then re-keys what
survives onto the spelling current PRG actually publishes.

Output columns: teryt_simc_code, prg_street_name, osm_street_name
Lookup is case-insensitive on prg_street_name, so casing in the file is
cosmetic; ordering is stable so the file diffs cleanly.

Usage:
    python3 scripts/migrate_legacy_street_mappings.py \\
        <legacy.csv> <output.csv> <prg_streets.csv> <prg_streets_by_simc.csv>

The two PRG vocabulary files come from the DuckDB database:

    COPY (SELECT DISTINCT ulica FROM prg_addresses
          WHERE ulica IS NOT NULL AND trim(ulica) <> '')
      TO 'prg_streets.csv' (HEADER, FORMAT CSV);
    COPY (SELECT DISTINCT teryt_miejscowosc AS simc, ulica FROM prg_addresses
          WHERE ulica IS NOT NULL AND trim(ulica) <> '')
      TO 'prg_streets_by_simc.csv' (HEADER, FORMAT CSV);
"""

import csv
import sys
from collections import Counter, defaultdict

SRC = sys.argv[1] if len(sys.argv) > 1 else "example_data/mappings/street_names_mappings.csv"
DST = sys.argv[2] if len(sys.argv) > 2 else "mappings/street_names_mappings.csv"


def key(name):
    return name.strip().lower()


# Keys dropped entirely: no global, no settlement rows.
DROP_KEYS = {
    "gen. romualda traugutta",  # file majority contradicted by OSM (435 vs 7756); user chose to drop
}

# (simc, key) source rows dropped as typos, keeping the rest of the key intact.
DROP_ROWS = {
    ("0975285", "bp. ignacego krasickiego"),  # "Bpiskupa" -> typo for Biskupa, 0 in OSM
    # Missing space on both sides ("św.Brata" -> "ŚwiętegoBrata"). Its own key
    # until PRG spellings are normalised, at which point it collides with the
    # correctly-spelled "św. Brata Alberta" rows and contradicts them.
    ("0937280", "św.brata alberta"),
}

# Misspelled OSM targets in the legacy file, corrected on read. Found by
# splitting every target into tokens and keeping those that appear nowhere in
# OSM Poland's or PRG's street vocabulary; each correction below was then
# confirmed against real OSM usage or is a plain grammatical fix (Polish street
# names take the genitive).
FIX_TARGETS = {
    # botched find-and-replace: "Drozdowicza" had its "Dr" expanded mid-word
    "Doktora Michała Doktoraozdowicza": "Doktora Michała Drozdowicza",   # OSM: 27
    "Księdza Kaziemierza Merkleina": "Księdza Kazimierza Merkleina",     # OSM: 36
    "Pułkownika Makszmiliana Chodakowskiego": "Pułkownika Maksymiliana Chodakowskiego",  # OSM: 35
    "Malchiora Wańkowicza": "Melchiora Wańkowicza",                      # OSM: 1419
    "Kapitana Zeglugi Wielkiej Witolda Poinca": "Kapitana Żeglugi Wielkiej Witolda Poinca",
    "62 Pułku Piechoty Wielkpolskiej": "62 Pułku Piechoty Wielkopolskiej",
    "4 Stycznia 1919 rroku": "4 Stycznia 1919 roku",
    "7 czerwca 1991 rroku": "7 czerwca 1991 roku",
    "Harcerzy Rzeczpospolitej Polskiej": "Harcerzy Rzeczypospolitej Polskiej",
    # nominative where the genitive is required
    "Maksymilian Gierymskiego": "Maksymiliana Gierymskiego",             # OSM: 55
    "Porucznika Marian Zaremby": "Porucznika Mariana Zaremby",
    "Księdza Tadeusz Muszyńskiego": "Księdza Tadeusza Muszyńskiego",
    "Pułkownika Zdzisław Orłowskiego": "Pułkownika Zdzisława Orłowskiego",
    "Bogdana i Eleonor Hutten-Czapskich": "Bogdana i Eleonory Hutten-Czapskich",
    "Helena i Szymon Syrkusów": "Heleny i Szymona Syrkusów",
}

# Corrections applied to a surviving settlement row's OSM name.
FIX_ROWS = {
    # Garwolin / Mlawa: word order was right, quotes were missing (OSM: 6 / 21)
    ("0975285", "mjr hubala"): 'Majora Henryka Dobrzańskiego "Hubala"',
    ("0930696", "mjr hubala"): 'Majora Henryka Dobrzańskiego "Hubala"',
}

# Curated global for each ambiguous key. A key absent here (and not in
# DROP_KEYS) stays settlement-only: no global row is emitted.
GLOBALS = {
    # --- dominant keys: file majority, confirmed against OSM ---
    "aleja marsz. józefa piłsudskiego": "Aleja Marszałka Józefa Piłsudskiego",
    "bp. ignacego krasickiego": "Biskupa Ignacego Krasickiego",
    "gen. grota roweckiego": "Generała Stefana Grota-Roweckiego",  # flipped: 7 of 8 towns use hyphen
    "karłowicza": "Mieczysława Karłowicza",
    "kołłątaja": "Hugona Kołłątaja",
    "kościuszki": "Tadeusza Kościuszki",
    "ks. augustyna kordeckiego": "Księdza Augustyna Kordeckiego",
    "ks. ignacego skorupki": "Księdza Ignacego Skorupki",
    "p. skargi": "Piotra Skargi",
    "paderewskiego": "Ignacego Paderewskiego",
    "reymonta": "Władysława Reymonta",
    # --- contested keys resolved on OSM frequency ---
    "aleja ks. kardynała stefana wyszyńskiego": "Aleja Księdza Kardynała Stefana Wyszyńskiego",
    "antoniego hedy ps. szary": "Antoniego Hedy ps. Szary",
    "bończyka": "Księdza Norberta Bończyka",
    "bł. ks. j. popiełuszki": "Błogosławionego Księdza Jerzego Popiełuszki",
    "dr. jordana": "Doktora Henryka Jordana",
    "gen. augusta emila fieldorfa nila": 'Generała Augusta Emila Fieldorfa "Nila"',
    "gen. augusta fieldorfa-nila": "Generała Augusta Fieldorfa-Nila",
    "gen. emila fieldorfa nila": 'Generała Emila Fieldorfa "Nila"',
    "gen. kruka": "Generała Kruka",
    "h. derdowskiego": "Hieronima Derdowskiego",  # surname-stem guard; frequency alone picks Dąbrowskiego
    "krasickiego": "Ignacego Krasickiego",
    "ks. dr bernarda sychty": "Księdza doktora Bernarda Sychty",
    "ks. skłodowskiego": "Księdza Mariana Skłodowskiego",
    "o. adama f. studzińskiego": "Ojca Adama Studzińskiego",
    "osiedle j. dambka": "Osiedle Józefa Dambka",
    "ojca św. jana pawła ii": "Ojca Świętego Jana Pawła II",
    "plac gen. józefa wybickiego": "Plac Generała Józefa Wybickiego",  # no OSM evidence either way
    "plac im. dr zygmunta brodowicza": "Plac Imienia Doktora Zygmunta Brodowicza",
    "plac im. jana pawła ii": "Plac Jana Pawła II",
    "plac powstańców wlkp.": "Plac Powstańców Wielkopolskich",
    "płk. lisa-kuli": "Pułkownika Leopolda Lisa-Kuli",
    "rynek duży im. j. piłsudskiego": "Duży Rynek imienia Józefa Piłsudskiego",
    "rynek im. powstańców 1863": "Rynek Powstańców 1863 roku",
    "sikorskiego": "Generała Władysława Sikorskiego",
    "sobieskiego": "Jana III Sobieskiego",
}

# Settlement rows that survive alongside a curated global, as (simc, key).
# For a key WITH a global this list is exhaustive: every other legacy row for
# that key collapses into the global. Deriving exceptions implicitly instead
# would preserve the losing variant of every typo pair as a per-town rule.
# Each of these was confirmed to have real usage in that town's OSM data.
KEEP_EXCEPTIONS = {
    ("0940335", "aleja marsz. józefa piłsudskiego"),  # Jaworzno
    ("0613903", "gen. grota roweckiego"),             # Żurawica, unhyphenated (31)
    ("0942765", "karłowicza"),                        # Rybnik, Jana (different person)
    ("0939409", "kołłątaja"),                         # Czerwionka-Leszczyny
    ("0212529", "kościuszki"),                        # Dobieszowice
    ("0931678", "ks. augustyna kordeckiego"),         # Kłobuck
    ("0931678", "ks. ignacego skorupki"),             # Kłobuck
    ("0940163", "p. skargi"),                         # Jastrzębie-Zdrój
    ("0140675", "paderewskiego"),                     # Gierałtowice
    ("0214327", "paderewskiego"),                     # Olsztyn
    ("0940890", "reymonta"),                          # Lędziny
    ("0941984", "krasickiego"),                       # Orzesze, Jana (different person)
}

# Keys deliberately left with no global: different people, or no form near a
# majority. Listed so the script can assert it has an opinion on every
# ambiguous key rather than silently defaulting.
SETTLEMENT_ONLY = {
    "dąbrowskiego",      # Jarosława (2807) vs Henryka (1657) -- different people
    "gen. fieldorfa",    # PRG gives surname only; expanding invents three words
    "księdza pojdy",     # Jana (349) vs Adolfa (222) -- different people
    "mjr hubala",        # 774 : 285 : 272, no majority
    "mjra hubala",       # 774 : 285, no majority
}


PRG_STREETS = sys.argv[3] if len(sys.argv) > 3 else "prg_streets.csv"
PRG_STREETS_BY_SIMC = sys.argv[4] if len(sys.argv) > 4 else "prg_streets_by_simc.csv"


def punct_norm(s):
    return " ".join(s.replace(".", " ").lower().split())


def retarget_to_current_prg(out):
    """Re-key rows onto the spelling current PRG actually emits, and drop rows
    that can no longer fire.

    The legacy file was built against an older PRG export whose conventions
    have since changed: leading tokens are now capitalised ("Ks. Płaczka"),
    some abbreviations lost their dot ("płk"), and the street-type prefix is no
    longer concatenated onto the name. A row keyed on a spelling PRG no longer
    publishes is dead weight, so it is either re-keyed or dropped.

    Deliberately NOT re-keyed: rows where PRG dropped the honorific entirely
    (file "dr. Adama Próchnika", PRG now "Adama Próchnika"). Re-keying those
    would build a global rule that prepends a title PRG never mentions, to
    every town in the country.
    """
    stats = Counter()

    with open(PRG_STREETS, encoding="utf-8", newline="") as fh:
        prg_all = [r["ulica"] for r in csv.DictReader(fh)]
    by_simc = defaultdict(set)
    with open(PRG_STREETS_BY_SIMC, encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            by_simc[r["simc"]].add(r["ulica"])

    # Current PRG spellings, indexed by progressively looser keys. Ties are
    # broken by picking the spelling PRG uses most distinctly -- in practice
    # collisions here are casing variants of the same name.
    exact = set(prg_all)
    by_lower, by_punct = {}, {}
    for name in prg_all:
        by_lower.setdefault(name.lower(), name)
        by_punct.setdefault(punct_norm(name), name)

    result = []
    for simc, prg, osm in out:
        if simc:
            # Settlement rows are checked against their own town only.
            local = by_simc.get(simc, set())
            if prg in local:
                result.append((simc, prg, osm))
                stats["kept_settlement"] += 1
            else:
                match = next((c for c in local if c.lower() == prg.lower()), None) \
                    or next((c for c in local if punct_norm(c) == punct_norm(prg)), None)
                if match:
                    result.append((simc, match, osm))
                    stats["rekeyed_settlement"] += 1
                else:
                    stats["dropped_settlement_absent"] += 1
            continue

        current = prg if prg in exact else by_lower.get(prg.lower()) or by_punct.get(punct_norm(prg))
        if current is None:
            stats["dropped_absent_from_prg"] += 1
            continue
        if current == osm:
            stats["dropped_noop"] += 1
            continue
        if current != prg:
            stats["rekeyed_global"] += 1
        else:
            stats["kept_global"] += 1
        result.append((simc, current, osm))

    # Re-keying can collapse two rows onto one key; keep one only if they agree.
    seen, deduped, conflicts = {}, [], []
    for simc, prg, osm in result:
        k = (simc, prg.lower())
        if k in seen:
            if seen[k] != osm:
                conflicts.append((simc, prg, seen[k], osm))
            else:
                stats["deduped_after_rekey"] += 1
            continue
        seen[k] = osm
        deduped.append((simc, prg, osm))

    if conflicts:
        for simc, prg, a, b in conflicts:
            print(f"re-key conflict: [{simc or 'global'}] {prg!r}: {a!r} vs {b!r}", file=sys.stderr)
        sys.exit(f"{len(conflicts)} re-key conflict(s); resolve via DROP_ROWS/FIX_ROWS")

    return deduped, stats


def main():
    rows, dropped_rows, raw_targets = [], 0, set()
    with open(SRC, encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            simc = r["teryt_simc_code"].strip()
            prg = r["teryt_street_name"].strip()
            osm = r["osm_street_name"].strip()
            if not prg or not osm:
                continue
            raw_targets.add(osm)
            osm = FIX_TARGETS.get(osm, osm)
            rows.append((simc, key(prg), prg, osm))

    present = {(simc, k) for simc, k, _, _ in rows}
    dropped_rows = sum(1 for x in DROP_ROWS if x in present)

    # Ambiguity is judged on the source file, before DROP_ROWS is applied: a
    # decision recorded for a key stays valid even once dropping its typo row
    # leaves the key with a single form.
    all_by_key = defaultdict(list)
    for simc, k, prg, osm in rows:
        all_by_key[k].append((simc, prg, osm))
    ambiguous = {k for k, v in all_by_key.items() if len({o for _, _, o in v}) > 1}

    by_key = defaultdict(list)
    for simc, k, prg, osm in rows:
        if (simc, k) not in DROP_ROWS:
            by_key[k].append((simc, prg, osm))

    # Every ambiguous key must have an explicit decision.
    undecided = ambiguous - set(GLOBALS) - SETTLEMENT_ONLY - DROP_KEYS
    if undecided:
        sys.exit(f"undecided ambiguous keys: {sorted(undecided)}")
    stale = (set(GLOBALS) | SETTLEMENT_ONLY | DROP_KEYS) - ambiguous
    if stale:
        sys.exit(f"decisions for keys that are not ambiguous: {sorted(stale)}")

    out = []          # (simc, prg_street_name, osm_street_name)
    stats = Counter()

    for k, entries in by_key.items():
        if k in DROP_KEYS:
            stats["dropped_keys"] += 1
            continue

        # Readable PRG spelling for a global row: the most common casing.
        file_form = Counter(prg for _, prg, _ in entries).most_common(1)[0][0]

        if k not in ambiguous:
            out.append(("", file_form, entries[0][2]))
            stats["global_unambiguous"] += 1
            continue

        global_name = GLOBALS.get(k)
        if global_name:
            out.append(("", file_form, global_name))
            stats["global_curated"] += 1

        # With a global, only the explicitly listed exceptions survive; without
        # one, every row does. Deduped on (simc, key).
        seen = {}
        for simc, prg, osm in entries:
            if global_name and (simc, k) not in KEEP_EXCEPTIONS:
                stats["collapsed_into_global"] += 1
                continue
            osm = FIX_ROWS.get((simc, k), osm)
            if simc in seen and seen[simc][2] != osm:
                sys.exit(f"conflict: SIMC {simc} key {k!r} -> {seen[simc][2]!r} and {osm!r}")
            seen[simc] = (simc, prg, osm)
        for simc, prg, osm in seen.values():
            out.append((simc, prg, osm))
            stats["exception" if global_name else "settlement_only"] += 1

    out, retarget_stats = retarget_to_current_prg(out)
    for k in sorted(retarget_stats):
        stats[k] = retarget_stats[k]

    for stale_ref in (KEEP_EXCEPTIONS | set(DROP_ROWS) | set(FIX_ROWS)) - present:
        sys.exit(f"decision references a row not in the source: {stale_ref}")
    unused_fixes = set(FIX_TARGETS) - raw_targets
    if unused_fixes:
        sys.exit(f"FIX_TARGETS entries not found in the source: {sorted(unused_fixes)}")

    out.sort(key=lambda r: (r[1].lower(), r[0]))

    with open(DST, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["teryt_simc_code", "prg_street_name", "osm_street_name"])
        w.writerows(out)

    stats["dropped_rows"] = dropped_rows
    print(f"source rows: {len(rows) + dropped_rows}")
    for k in sorted(stats):
        print(f"  {k}: {stats[k]}")
    print(f"written: {len(out)} -> {DST}")


if __name__ == "__main__":
    main()
