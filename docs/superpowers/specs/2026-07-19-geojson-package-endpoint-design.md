# GeoJSON Data Package Endpoint Design

## Summary

Add a `/package` endpoint to the Axum HTTP server that returns a GeoJSON `FeatureCollection` of government-registry records missing from OSM within a requested area — the core JOSM import deliverable (see ADR-003). The comparison against OSM runs live per request (scoped to the requested area), so results always reflect the current OSM replication state and do not depend on the `compare` CLI command having been run. Feature properties are OSM-ready tags so the file is directly usable in JOSM without retagging.

## API surface

### Requests

- `GET /package?bbox=minLon,minLat,maxLon,maxLat&datasets=prg,bdot10k`
  - `bbox` — required, four comma-separated finite numbers in EPSG:4326.
  - `datasets` — optional, comma-separated, case-insensitive subset of `prg`, `bdot10k`, `egib`, plus the alias `all` (expands to all three; union semantics, so combining it with other names is valid and redundant). Default: all three.
- `POST /package?datasets=...`
  - Body: a GeoJSON `Polygon` or `MultiPolygon` geometry object. A `Feature` wrapping such a geometry is accepted and unwrapped leniently. Coordinates are EPSG:4326.
  - `datasets` — same query parameter as GET.

### Responses

- `200` — `Content-Type: application/geo+json`, `Content-Disposition: attachment; filename="package.geojson"`. Body is a single `FeatureCollection` mixing address point features and building polygon features. An area with nothing missing yields an empty `features` array (still `200`).
- `400` — validation failure, JSON body `{"error": "<message>"}`. Causes: malformed `bbox` (wrong count, non-finite numbers, `minLon ≥ maxLon` or `minLat ≥ maxLat`, out of lon/lat range), invalid or non-Polygon POST body, unknown dataset name, or requested area over the cap.
- `500` — query execution failure. Logged with details; response body is a generic `{"error": "internal error"}`.

### Area cap

The bounding-box area of the request geometry (in square degrees) must not exceed `package.max_area_sq_deg` — a new `[package]` TOML config section, default `0.04` (0.2° × 0.2° ≈ 14 × 22 km in Poland). The same bounding-box rule applies to GET and POST so behavior is predictable; a sprawling thin polygon is judged by its envelope. This is the only request-size guardrail (no feature-count cap in v1).

## Per-request comparison queries

Three read-only `SELECT` queries, one per dataset, run on the server's read-only connection pool inside `spawn_blocking` — the same pattern as `tiles.rs`. The existing `compare` module functions cannot be reused directly because they `CREATE TABLE` (impossible on read-only connections) and are optimized for full-Poland runs; instead, each query mirrors the matching semantics of its `compare` counterpart, scoped to the request area where R-tree index scans with constant envelopes keep it fast.

### PRG addresses (mirrors `compare::addresses`)

Select `prg_addresses` rows whose `geom` intersects the request geometry and for which no matching OSM address exists (`NOT EXISTS` anti-join against `osm_addresses`). Match rule, identical to the compare module:

- `UPPER(TRIM(numer_porzadkowy)) = UPPER(TRIM(osm.housenumber))`
- `ST_Distance_Sphere(...) <= 50.0` meters
- NULL housenumbers never match (SQL NULL equality semantics, same as compare).

The inner `osm_addresses` scan is bounded by the request envelope expanded by 0.001° (> 50 m in both axes at Polish latitudes), so a matching OSM address just outside the requested area still suppresses the candidate — the result matches what the full-Poland grid-key comparison would produce for the same rows.

### BDOT10k / EGIB buildings (mirrors `compare::buildings`)

Select buildings whose centroid (`ST_Centroid(geom)`) falls in the request geometry and for which no OSM building contains that centroid (`NOT EXISTS` with `ST_Contains(osm.geom, centroid)`). The inner `osm_buildings` scan is bounded by the unexpanded request envelope — no buffer is needed, because an OSM polygon containing a point inside the request area necessarily intersects the envelope. One query per source table (`bdot10k_buildings`, `egib_buildings`).

### Request geometry plumbing

- GET: the envelope is `ST_MakeEnvelope(?, ?, ?, ?)` with bound bbox parameters.
- POST: the polygon's envelope is computed in Rust for the index-friendly bbox predicates; the exact filter adds `ST_Intersects` against the polygon itself, passed as a GeoJSON string via `ST_GeomFromGeoJSON(?)`.
- Feature geometry is returned from SQL as `ST_AsGeoJSON(geom)` strings alongside raw columns. No JSON assembly happens in SQL.

## OSM tag mapping (Rust, per feature)

### Addresses

| Condition | Tags emitted |
|---|---|
| always | `addr:housenumber` = trimmed `numer_porzadkowy`; `source:addr` = `gugik.gov.pl` |
| `ulica` present | `addr:street` = `ulica`; `addr:city` = `miejscowosc` |
| `ulica` empty/NULL | `addr:place` = `miejscowosc` |
| `kod_pocztowy` present | `addr:postcode` = `kod_pocztowy` |
| `teryt_miejscowosc` present | `addr:city:simc` = `teryt_miejscowosc` |

### Buildings

`building` = `yes`; `source:building` = `geoportal.gov.pl`.

Empty or NULL columns are omitted entirely — never emitted as empty-valued tags. Mapping BDOT10k function codes to specific `building=*` values requires a curated lookup table and is out of scope (new roadmap item, alongside street-name corrections).

## Code structure

- **New `src/server/package.rs`** containing:
  - Axum handlers `get_package` (query extractor) and `post_package` (JSON body).
  - A `RequestArea` type holding the envelope `(min_lon, min_lat, max_lon, max_lat)` and an optional polygon GeoJSON string (present only for POST).
  - Pure, unit-testable parsing/validation functions: bbox parsing, datasets parsing, area-cap check, polygon parsing + envelope computation.
  - The three query functions returning row structs.
  - Tag-mapping functions (address row → properties map, building row → properties map).
  - FeatureCollection assembly with `serde_json`; the `ST_AsGeoJSON` geometry strings are embedded via `serde_json::value::RawValue` rather than parsed and re-serialized.
- **`src/server/mod.rs`**: register `.route("/package", get(package::get_package).post(package::post_package))`.
- **`src/config.rs`**: new optional `[package]` section with `max_area_sq_deg: f64`, default `0.04`, following the existing optional-section pattern (like `[teryt]` / `[jobs]`).
- **`example_config.toml`** and **README**: document the new section and endpoint; tick the roadmap item and add the BDOT10k building-type mapping item.

## Error handling

- All validation happens before any database work; failures return `400` with a specific message.
- Query/pool errors are logged via `tracing::error!` with the request area and return a generic `500` — same convention as `tiles.rs`.
- `spawn_blocking` join errors (panics) are logged and mapped to `500`.

## Testing

- **Unit tests** for the pure functions: bbox parsing (valid, wrong count, non-numeric, min ≥ max, out of range), datasets parsing (default, subsets, `all`, unknown → error, case-insensitivity), area cap boundary, polygon envelope computation (Polygon, MultiPolygon, Feature unwrap, invalid body), tag mapping branches (street vs place, NULL/empty stripping).
- **Handler tests** in `package.rs` using the tower `oneshot` pattern from `server/mod.rs` tests: seed a temp file-backed DuckDB (the read pool requires a real file; spatial extension loaded as in `compare` tests) with small PRG/OSM address and building fixtures, then assert:
  - matched address (same housenumber within 50 m) is excluded; unmatched is included with correct tags;
  - an OSM match lying just outside the bbox still suppresses a candidate inside it (buffer works);
  - building with centroid inside an OSM building is excluded; uncovered building is included;
  - `datasets` filter limits which layers appear;
  - over-cap bbox → `400`; malformed bbox → `400`;
  - POST polygon path returns only features inside the polygon (not its whole envelope);
  - empty area → `200` with empty `features`.

## Out of scope (stays on the roadmap)

Record exclusion/reporting endpoint, BDOT10k building-type → `building=*` mapping, street-name corrections, package/tile caching, feature-count cap.
