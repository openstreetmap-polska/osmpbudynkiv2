#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""Generate ``fixtures/prg_v2.zip`` — the "next snapshot" counterpart of
``fixtures/prg.zip`` — for ``tests/cli_update_prg.rs``.

Usage:
    uv run fixtures/scripts/prepare_prg_update_fixture.py

PRG's snapshot is GML inside a zip rather than a parquet file, so it cannot be
derived with the DuckDB one-liners ``prepare_update_fixtures.sh`` uses for
BDOT10k and EGIB. The shape of the delta is deliberately the same as theirs,
so all three ``cli_update_*`` tests assert the same 1/1/1:

  - 1 removed  — the address point with the lexicographically smallest
                 ``numerPorzadkowy`` is dropped outright.
  - 1 modified — the largest one is MOVED ~3 km east, leaving every compared
                 attribute untouched.
  - 1 added    — a copy of the largest one under a fresh ``lokalnyId`` and
                 ``gml:id``, with ``_ADDED`` appended to its housenumber.

**The modification is a pure geometry move, and that is the point.** PRG is
``compare_geometry: true`` (``dataset::PRG``), so a record whose coordinates
changed and whose attributes did not must still report as modified — and it
must dirty the cell it LEFT as well as the one it entered. A fixture that
modified an attribute instead would pass just as happily with geometry
excluded from the comparison, which is the regression worth pinning.

3 km is chosen against the z14 cell width (~1340 m at Polish latitudes), so
the moved point lands in a different cell with room to spare. Coordinates are
EPSG:2180 eastings/northings in metres, so this is a plain addition.

The other two zip entries (the ``.xml`` and the conversion-table ``.pdf``) are
copied through untouched: ``import::prg::collect_gml_indices`` selects entries
by the ``.gml`` extension alone, so their presence is part of what the fixture
exercises.
"""

from __future__ import annotations

import re
import shutil
import zipfile
from pathlib import Path

FIXTURES = Path(__file__).resolve().parent.parent
SRC = FIXTURES / "prg.zip"
DST = FIXTURES / "prg_v2.zip"

# Metres east, in EPSG:2180, to move the modified point. See the module
# docstring for why this is larger than a z14 cell.
MOVE_EAST_METRES = 3000.0

MEMBER_RE = re.compile(r"[ \t]*<gml:featureMember>.*?</gml:featureMember>\n?", re.S)


def housenumber(member: str) -> str | None:
    m = re.search(r"<prgad:numerPorzadkowy>(.*?)</prgad:numerPorzadkowy>", member)
    return m.group(1) if m else None


def move_east(member: str, metres: float) -> str:
    def shift(m: re.Match[str]) -> str:
        easting, northing = m.group(1).split()
        return f"<gml:pos>{float(easting) + metres} {northing}</gml:pos>"

    return re.sub(r"<gml:pos>([^<]+)</gml:pos>", shift, member)


def as_added_copy(member: str) -> str:
    """A second address point derived from `member`, under identities that
    cannot collide with it: a fresh `lokalnyId` (PRG's record key, so this is
    what makes it `added` rather than `modified`) and a fresh `gml:id`."""
    out = re.sub(
        r"<prgad:lokalnyId>.*?</prgad:lokalnyId>",
        "<prgad:lokalnyId>00000000-0000-4000-8000-00000000add0</prgad:lokalnyId>",
        member,
    )
    out = re.sub(r'(gml:id=")([^"]+)(")', r"\1\2_ADDED\3", out)
    hn = housenumber(member)
    return out.replace(
        f"<prgad:numerPorzadkowy>{hn}</prgad:numerPorzadkowy>",
        f"<prgad:numerPorzadkowy>{hn}_ADDED</prgad:numerPorzadkowy>",
    )


def rewrite_gml(text: str) -> str:
    members = MEMBER_RE.findall(text)
    points = [(m, housenumber(m)) for m in members]
    points = [(m, hn) for m, hn in points if hn is not None]
    if len(points) < 2:
        raise SystemExit(f"expected at least 2 address points, found {len(points)}")

    smallest = min(points, key=lambda p: p[1])[0]
    largest = max(points, key=lambda p: p[1])[0]
    if smallest is largest:
        raise SystemExit("the removed and modified points must differ")

    out = text.replace(smallest, "")
    out = out.replace(largest, move_east(largest, MOVE_EAST_METRES) + as_added_copy(largest))
    return out


def main() -> None:
    if not SRC.exists():
        raise SystemExit(f"{SRC} not found")
    tmp = DST.with_suffix(".zip.tmp")
    with zipfile.ZipFile(SRC) as src, zipfile.ZipFile(
        tmp, "w", zipfile.ZIP_DEFLATED
    ) as dst:
        for info in src.infolist():
            data = src.read(info.filename)
            if info.filename.lower().endswith(".gml"):
                data = rewrite_gml(data.decode("utf-8")).encode("utf-8")
            dst.writestr(info, data)
    shutil.move(tmp, DST)
    print(f"Wrote {DST.name}")


if __name__ == "__main__":
    main()
