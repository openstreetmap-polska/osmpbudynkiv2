# Report endpoint — exclude government objects from matching

Implements the roadmap item at `README.md` / `docs/project_ideas.md:27`:
*"Endpoint for reporting records to exclude (bad source data, comparison
mismatches)"*. `POST /report` records a user's claim that a government object
should not be proposed for import; an active report vetoes it out of
`<source>_unmatched` until the underlying record changes.

The narrative rationale lives in CLAUDE.md's *"the user-report veto is a third
layer on the same rule"* gotcha. This document holds the parts that don't belong
there: the decisions and the measurements behind them.

## Decisions

| Axis | Decision | Why not the alternative |
|---|---|---|
| Enforcement | Immediate — active on insert, cell enqueued, gone on the next drain | A moderation queue needs moderators; there are none, and an object that is genuinely wrong keeps being re-proposed to every user meanwhile |
| Expiry | On any content change, by signature over `compared_columns` | Time-based expiry says nothing about the record; "the registry fixed it" is the event that actually matters, and the diff already defines it |
| Ratio | Reported objects stay in `cell_totals` | Same call as former-building suppression: the denominator is "objects that could be imported here", not "objects currently offered" |
| Abuse control | Validation + per-request cap + config kill-switch. **No rate limit, no stored client identity** | See below |
| Un-report | CLI only | With no auth, an anonymous DELETE is a strictly worse vector than the report itself |
| Storage | One table with `source VARCHAR` + `record_key VARCHAR[]` | Three tables would triple every call site; the source string is always read off `DatasetSpec.name`, never typed, which neutralises the "dirty-queue source strings must match everywhere" hazard |

### No rate limiting, no client identity

Decided in review, in two steps: first the per-IP rate limiter was dropped, then
IP storage with it.

The reasoning on the second is worth keeping, because "hash the IP" is the
obvious-looking fix and it does not work. An unkeyed hash of an IP address is
not one-way in any useful sense: IPv4 has ~4.3 billion possible values, which is
minutes of GPU work to enumerate exhaustively and entirely precomputable, so the
hash is a reversible encoding of the address rather than a redaction of it.
Truncating the digest adds collisions without adding preimage resistance. It
remains personal data under EU guidance either way, which means a retention
policy and a lawful basis for something that was only ever meant to group
requests.

A keyed MAC (`md5(secret || ip)`) is genuinely not invertible without the
secret, and was the plan for one revision — but it needs a secret to generate,
store, protect and rotate, and rotating it destroys exactly the grouping it
exists to provide. For a feature whose entire abuse story is "an operator
notices and runs one CLI command", that is a large amount of machinery and a
standing liability. Storing nothing is simpler and strictly safer.

Consequences, all accepted deliberately:

- `server::mod`'s `axum::serve(listener, app)` stays as it is — no
  `into_make_service_with_connect_info`, no `ConnectInfo` extractor, no
  `X-Forwarded-For` parsing, no `trust_forwarded_for` config. The
  reverse-proxy trap disappears with the feature that needed it, and the
  existing `tower::ServiceExt::oneshot` handler tests keep mounting the bare
  `Router` unchanged.
- The cap bounds one request, not a sequence. A client can loop; nothing here
  stops that. Its real job is making a mis-drawn bulk selection cheap to undo.
- Cleanup is time-scoped, not actor-scoped: `reports revoke --since <ts>
  [--source S]` is the only way to unwind a burst, which is why the bulk form
  is in v1 rather than deferred.

## The BDOT10k composite-key problem

`bdot10k_unmatched` carried only `LOKALNYID`. BDOT10k's identity is the
composite `(PRZESTRZENNAZW, LOKALNYID)` (`dataset::BDOT10K.key_columns`), so no
client could express a complete BDOT10k key from anything the server served.
EGIB (`id_budynku`) and PRG (`lokalny_id`) were already fine.

Measured 2026-08-17 on the live national table (16,351,813 rows):

| Query | Result |
|---|---|
| `LOKALNYID` values used more than once | **0** |
| rows sharing a `LOKALNYID` | **0** |
| `(PRZESTRZENNAZW, LOKALNYID)` duplicate groups | **0** (post-`deduplicate_by_key`, as expected) |
| distinct `PRZESTRZENNAZW` values | **16** |

So `LOKALNYID` alone *is* currently unique, which contradicts the flat claim in
CLAUDE.md's serving-table gotcha — that claim was never measured, and has been
corrected in place. **Carry the composite anyway**, for three reasons that the
measurement doesn't touch: nothing in the schema or the published export
guarantees uniqueness, `DatasetSpec::key_columns` already declares the composite
and the diff/dedup machinery keys on it, and the raw pre-`deduplicate_by_key`
export does carry duplicate composite keys (2 groups per snapshot, per the
key-based-diff notes). Keying user reports on a non-key is exactly the
silent-wrong-row failure the key-diff gotcha warns about, and a future export
can start colliding with no notice and no error.

The column is now carried through `compare::columns::classification_columns`,
the `bdot10k_unmatched` schema and `BUILDINGS_MVT_SQL`, with
`TILE_FORMAT_VERSION` 1 → 2.

## Cost of the veto on the full compare

The risk flagged before implementing was `rule.rs`'s documented
`DELIM_JOIN`-materialises-`b.geom` OOM: a second correlated `EXISTS` alongside
the `osm_buildings` anti-join could plan badly enough to spill gigabytes. It
does not, because the correlation here is on two short `VARCHAR`s rather than on
geometry.

Measured against the real national tables (read-only `ATTACH`, `object_reports`
held in an in-memory database), on the full compare's own grid-cell shape — one
0.5° cell over Warsaw, `memory_limit = '4GB'`, `threads = 8`:

| Variant | Time | Unmatched |
|---|---|---|
| Baseline, no veto | 2.757 s | 7,536 |
| Veto, `object_reports` empty | 2.911 s | 7,536 |
| Veto, **50,000** active reports | 3.029 s | 6,017 |

50,000 active reports is far beyond any realistic volume and still costs ~0.27 s
(~10%) on a cell that takes 2.8 s without it; an empty table — the state every
existing database starts in — costs nothing measurable. The cost tracks the
report table, not the 16.35M-row source, which is the property the design
depends on. No spill, no memory pressure, no plan pathology.

Not measured: a full national `compare bdot10k` before/after. That needs
`bdot10k_unmatched` dropped and recreated for the new `PRZESTRZENNAZW` column,
i.e. a destructive change to the live database, so it is left for whoever
performs that migration. The per-cell numbers above are the same predicate the
grid loop runs 264 times.

## Verification

Backend and frontend were exercised end to end against a running server on a
seeded database (`3` bdot10k / `2` egib / `2` prg objects in one z14 cell,
including two bdot10k rows deliberately sharing a `LOKALNYID` under different
`PRZESTRZENNAZW`):

- `POST /report` with the composite key removed exactly one of the two
  same-`LOKALNYID` rows (`/package` 3 → 2), confirming the composite key
  discriminates in practice and not just in the schema.
- Rejections behaved per object, not per request: an unknown key came back in
  `rejected` with the request still `200`; unknown reason, missing key column,
  `other` without a note and an over-cap batch were each `400` with the
  message in `{"error": ...}`.
- `Cache-Control: no-store` present on the response.
- Reported → `reports reconcile` after a `WERSJA` bump → `expired_changed=1`,
  cell enqueued, drain, object back in `bdot10k_unmatched`.
- `reports revoke <id>` → drain → object back.
- `cell_totals` read `3 / 2 / 2` before, during and after all of it — the
  denominator parity the design promises, verified on running code rather than
  argued.
- `reports export` → `import` into an empty database → re-export: byte-identical
  content, ids reallocated (documented behaviour).
- Browser (Chromium via `npx playwright cli`): popup shows "Zgłoś problem" on
  unmatched layers only and not on `buildings-all-fill`; `PRZESTRZENNAZW` is
  absent from the displayed attributes; the modal names the full composite key;
  `other` with an empty note is caught client-side; a successful submit closes
  the popup and reports back; and the button is inert while the area-drawing
  tool is in `drawing` state.

### Bug found by verification, not by tests

`reports export` was broken on every database: `list`'s unlimited path passed
`usize::MAX` into an interpolated `LIMIT`, and DuckDB casts a `LIMIT` to INT64
— `Type INT128 with value 18446744073709551615 can't be cast`. The whole suite
passed, because no test asked for an unlimited list. `limit` is now
`Option<usize>` with `None` omitting the clause, pinned by
`reports::tests::listing_without_a_limit_returns_every_report`.

Worth noting as a pattern: the defect was in the one code path that only the CLI
dispatch reaches, and the CLI dispatch had no test at all.

## Out of scope

Area-based exclusion zones (draw a polygon, exclude everything inside
permanently); cross-source linking ("reported in BDOT10k ⇒ also exclude the
EGIB building here"); a public un-report endpoint; a reports tile layer (it
would make `object_reports` a direct `/tiles` input and force a `serving_epoch`
bump per report); OSM OAuth accountability; bulk selection from a drawn polygon
in the frontend (planned as phase 2 — the server already accepts a batch, the
client just doesn't build one yet).
