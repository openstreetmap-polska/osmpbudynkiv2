// v6 dropped the UMD bundle (`maplibre-gl.js`) that used to expose a
// `maplibregl` global -- only the ESM build (`maplibre-gl.mjs`) is published
// now, so this file has to be a module and import it itself instead of index.html
// loading a separate global-exposing <script> before this one.
// Vendored locally (web/vendor/maplibre-gl/) rather than loaded from unpkg,
// so the app works with no outbound network. The .mjs also needs its sibling
// maplibre-gl-shared.mjs and maplibre-gl-worker.mjs to stay in the same
// directory, both fetched by relative URL at runtime.
import * as maplibregl from "./vendor/maplibre-gl/maplibre-gl.mjs";

(() => {
  const TILE_URL = `${location.origin}/tiles/{z}/{x}/{y}`;
  const DEFAULT_BUILDING_SOURCE = "bdot10k";

  // MapLibre paint properties are its own expression language, not CSS --
  // `var(--x)` is not resolved there, so read the real color values out of
  // the page's computed style up front (keeps style.css as the single
  // source of truth, light/dark included) instead of baking `var(...)`
  // strings into the style spec below.
  const rootStyle = getComputedStyle(document.documentElement);
  const buildingContextColor = rootStyle.getPropertyValue("--building-all").trim();
  const buildingAccentColor = rootStyle.getPropertyValue("--building-unmatched").trim();
  const buildingAllOutlineColor = rootStyle.getPropertyValue("--building-outline-all").trim();
  const addressAllColor = rootStyle.getPropertyValue("--address-all").trim();
  const addressUnmatchedColor = rootStyle.getPropertyValue("--address-unmatched").trim();
  const paperRaisedColor = rootStyle.getPropertyValue("--paper-raised").trim();
  const inkColor = rootStyle.getPropertyValue("--ink").trim();
  // Sequential ramp for the z5-11 aggregate grid (low -> high density), used
  // by the "Liczba" colour mode.
  const rampColors = [1, 2, 3, 4, 5].map((i) => rootStyle.getPropertyValue(`--ramp-${i}`).trim());
  const ratioColors = [0, 1, 2, 3, 4].map((i) => rootStyle.getPropertyValue(`--ratio-${i}`).trim());
  const ratioUnknownColor = rootStyle.getPropertyValue("--ratio-unknown").trim();
  // "Draw an area to download" overlay -- see the draw layers below and the
  // token comment in style.css.
  const drawOutlineColor = rootStyle.getPropertyValue("--draw-outline").trim();
  const drawFillColor = rootStyle.getPropertyValue("--draw-fill").trim();
  const drawInvalidColor = rootStyle.getPropertyValue("--draw-invalid").trim();
  // Recently-downloaded /package export areas, fed by GET /updates -- see the
  // updates-fill/updates-outline layers and pollUpdates below.
  const updatesColor = rootStyle.getPropertyValue("--updates").trim();

  // agg_cells carries integer count attributes, but what a given
  // count *means* changes sharply across z5-11: bz = min(z + 5, 14) caps at
  // z9, so each zoom step roughly quarters the bin's area (and so its typical
  // sum) until z9, where it flattens (z9 and z10 both bin at exactly one z14
  // cell -- identical distribution). A single static count->color/opacity
  // domain badly mismatches one end of that range: tuned for z6 it saturates
  // solid at z9/10, tuned for z9 it's all one pale colour at z6 (a single
  // flat 1..10000 domain was tried first and screenshotted against the live
  // 11GB database -- almost the whole country rendered as one dark-red
  // blob, which is what prompted this).
  //
  // Fix: two domains, anchored at z6 and z9 (the zooms this MVP actually
  // screenshots) and interpolated by zoom in between via MapLibre's
  // "zoom-and-property function" pattern (an `interpolate` on `["zoom"]`
  // whose outputs are themselves `interpolate`s on `["get", attr]`). Stops
  // are the p10/p25/p50/p75/p90 values themselves, pulled from real n_total
  // values in two representative tiles from the live database:
  //   z6  (bz=11, bin=8x8=64 z14 cells): p10=877 p25=1277 p50=1895 p75=3007 p90=4496 p99=8446 max=14281
  //   z9  (bz=14, bin=1 z14 cell):       p10=10  p25=20   p50=38   p75=81   p90=165  p99=664  max=1044
  // z10 and z11 share z9's domain exactly (same bin definition, no approximation).
  // z5/z7/z8 clamp to their nearest anchor -- not exact, but those zooms
  // aren't screenshotted and clamping degrades gracefully rather than
  // extrapolating into nonsense.
  //
  // An earlier version of this domain used round numbers (50/900/2000/4500/
  // 9000) that were merely *near* these percentiles rather than equal to
  // them, with a floor (50) far below p10. Since interpolation is linear in
  // raw value, that floor was never actually reached -- 90% of cells (every
  // one above p10) already fell in the ramp's saturated upper half, and the
  // "Liczba" mode rendered as a near-uniform dark wash country-wide (this is
  // exactly the "one dark-red blob" failure the comment above describes, just
  // reintroduced at a smaller scale by an under-measured floor). Anchoring
  // the 5 stops to the 5 percentiles exactly instead makes each of the 5
  // colours cover a genuinely equal-ish slice of the real distribution
  // (10/15/25/25/15/10 percent bands below-p10/p10-p25/p25-p50/p50-p75/
  // p75-p90/above-p90), so the map actually differentiates instead of
  // clamping almost everywhere.
  const COUNT_DOMAIN_Z6 = [877, 1277, 1895, 3007, 4496];
  const COUNT_DOMAIN_Z9 = [10, 20, 38, 81, 165];

  /** Builds a zoom-and-property `interpolate` expression over `valueExpr`: at
   * z6 use `outputs` keyed to COUNT_DOMAIN_Z6's thresholds, at z9 the same
   * `outputs` keyed to COUNT_DOMAIN_Z9's thresholds, linearly blended in
   * between (and clamped outside). `valueExpr` is a full MapLibre expression
   * rather than always `["get", attr]` so a combined multi-registry sum (see
   * combinedCountValueExpr below) can reuse the same domain/output shape as a
   * single `n_*` attribute. */
  function zoomPropertyRampFromValue(valueExpr, outputs) {
    const propertyExpr = (domain) => {
      const expr = ["interpolate", ["linear"], valueExpr];
      domain.forEach((stop, i) => expr.push(stop, outputs[i]));
      return expr;
    };
    return [
      "interpolate",
      ["linear"],
      ["zoom"],
      6,
      propertyExpr(COUNT_DOMAIN_Z6),
      9,
      propertyExpr(COUNT_DOMAIN_Z9),
    ];
  }

  function zoomPropertyRamp(attr, outputs) {
    return zoomPropertyRampFromValue(["get", attr], outputs);
  }

  // ---- completeness ratio (bivariate encoding) ----
  //
  // The counts above answer "where are there many missing objects", which at
  // country scale is mostly a population-density map -- cities dominate every
  // ramp because cities have more of everything. r_* answers the question the
  // map is actually for: what *share* of the government registry is still
  // missing here. That's scale-free, so unlike the count ramp it needs no
  // zoom-dependent domain at all -- 30% means 30% at every zoom.
  //
  // Ratio alone has the opposite failure, though: a hamlet with 3 buildings
  // and 3 missing is 100% and would out-shout a city missing 40,000. So the
  // two are encoded together -- hue from the ratio, fill opacity from the
  // absolute count -- and the combination is what reads as "worth a trip":
  // big *and* saturated.
  //
  // -1 is the server's "no denominator" sentinel (tiles.rs RATIO_UNKNOWN),
  // which is what every bin returns on a database whose cell_totals has not
  // been built yet. It gets its own off-ramp colour: painting it at the
  // ramp's 0 end would claim the country is fully imported.
  // Quantile-derived, not evenly spread over the 0..1 the ratio could
  // theoretically span. Measured over the whole country after the first
  // `compare full` that populated cell_totals, at the bin resolutions the
  // server actually renders (bin zoom = z+5 capped at 14, so a z5 map bin is
  // a z10 bin and z9 and up bin at raw z14 cells):
  //
  //        p05     p25     p50     p75     p95
  //   z5   0.043   0.068   0.085   0.102   0.127
  //   z6   0.039   0.067   0.086   0.107   0.138
  //   z7   0.034   0.064   0.086   0.111   0.154
  //   z8   0.023   0.058   0.084   0.116   0.182
  //   z9+  0.000   0.048   0.081   0.125   0.238
  //
  // Two things follow. First, the distribution barely moves with zoom -- the
  // median is ~0.085 at every tier -- so one fixed set of stops serves all of
  // them and the legend can stay zoom-independent. Second, the bulk of the
  // data lives in roughly 0.03..0.20, so stops must too: an 0..1 ramp puts
  // ~99% of bins in its first segment, and even 0..0.4 spent its whole upper
  // half on a tail holding well under 1% of bins. That is what made z5-z7
  // look flat -- aggregation pulls every bin toward the national mean, so at
  // those tiers the ramp has to resolve a *narrow* range, not a wide one.
  //
  // These sit near the pooled p05/p25/p50/p75/p95, so each band holds a real
  // share of bins at every zoom and regional differences separate. Anything
  // above the last stop clamps to --ratio-4; at z5-z7 that is ~5% of bins,
  // rising to ~9% at z9+, which is the intended "look here first" set.
  //
  // Consequence for the legend: the deep end means ">= 20% missing", not
  // "nothing in OSM", so #ratio-legend is labelled with numbers.
  const RATIO_STOPS = [0.03, 0.06, 0.09, 0.13, 0.2];

  function ratioColorExprFromValue(valueExpr) {
    const ramp = ["interpolate", ["linear"], valueExpr];
    RATIO_STOPS.forEach((stop, i) => ramp.push(stop, ratioColors[i]));
    return ["case", ["<", valueExpr, 0], ratioUnknownColor, ramp];
  }

  function ratioColorExpr(attr) {
    return ratioColorExprFromValue(["get", attr]);
  }

  /** fill-opacity from the absolute count -- the second channel of the
   * bivariate pair. Never reaches 0: a fully-matched bin should read as
   * quiet, not vanish, since "nothing left to do here" is a real answer.
   *
   * Deliberately a narrow band, not 0..1. --ratio-0.._4 is a single hue
   * varying in lightness, so alpha over a light basemap pushes a cell along
   * the *same* visual axis the ramp already uses: a pale bin at high alpha
   * and a deep bin at low alpha land in the same place, and the two channels
   * cancel. A wide range (this was 0.25..0.9) turned the regional pattern
   * into speckle. Kept wide enough to still rank bins by weight, narrow
   * enough that hue decides what a cell reads as. */
  const COUNT_OPACITY_STOPS = [0.55, 0.68, 0.78, 0.87, 0.95];

  function countOpacityExpr(attr) {
    return zoomPropertyRamp(attr, COUNT_OPACITY_STOPS);
  }

  // ---- combined grid "Dane" (buildings + addresses toggles drive the grid) ----
  //
  // The grid used to have its own independent 4-way "Dane" picker
  // (Wszystko/BDOT10k/EGIB/Adresy PRG). That picker is gone -- the grid now
  // mirrors whichever registries the Budynki/Adresy legend toggles have
  // turned on, so there's one set of controls instead of two that could
  // silently disagree. "Wszystko" doesn't survive this: the building toggle
  // only ever selects one of bdot10k/egib (never both), so "all three at
  // once" can no longer be expressed and isn't offered.
  //
  // The combination is a literal sum of the *same* underlying quantities
  // src/server/tiles.rs already sums for r_total/n_total (see ratio_sql and
  // its r_total call in tiles.rs) -- just over whichever subset of
  // {bdot10k, egib, prg} is active instead of always all three. For counts
  // that's a plain sum. For the ratio it's numerator-sum over
  // denominator-sum (n_a + n_b) / (t_a + t_b), not an average of the two
  // ratios -- summing n_x/t_x directly would double- or under-weight a
  // registry relative to its actual share of the bin, exactly like r_total
  // itself. All of this is ordinary MapLibre expression arithmetic over
  // attributes the tile already carries (n_bdot10k/t_bdot10k/n_egib/t_egib/
  // n_prg/t_prg) -- no server change needed.
  function combinedCountValueExpr(sources) {
    return sources.length === 1
      ? ["get", `n_${sources[0]}`]
      : ["+", ...sources.map((s) => ["get", `n_${s}`])];
  }

  function combinedRatioValueExpr(sources) {
    const numerator = combinedCountValueExpr(sources);
    const denominator =
      sources.length === 1 ? ["get", `t_${sources[0]}`] : ["+", ...sources.map((s) => ["get", `t_${s}`])];
    // Same RATIO_UNKNOWN convention as ratio_sql in tiles.rs: no denominator
    // means "unknown", not "0% missing".
    return ["case", [">", denominator, 0], ["min", ["/", numerator, denominator], 1], -1];
  }

  const sourceFilter = (source) => ["==", ["get", "source"], source];

  // Not per-source: color depends on match status, not registry, so one pair
  // of layers per status covers both registries — the source toggle just
  // swaps their filter instead of picking between two color sets. "all" gets
  // a black outline (registry footprint, not a status signal) and a faint
  // grey wash; "unmatched" gets a solid red fill and red outline, since that
  // status *is* the thing being signalled. "all" is listed first so
  // "unmatched" draws on top once both are visible.
  // minzoom: 14 on every layer below: the vector source now starts at z5
  // (Tiers A/B added for the low-zoom MVP), but buildings_all/buildings/
  // addresses_all/addresses are still z14-only MVT layers (see
  // src/server/tiles.rs) -- without this, MapLibre would just find those
  // source-layers absent from every tile below z14 and draw nothing anyway,
  // but the explicit bound documents the real cutoff instead of relying on
  // that incidentally.
  const buildingLayers = [
    {
      id: "buildings-all-fill",
      type: "fill",
      source: "unmatched",
      "source-layer": "buildings_all",
      minzoom: 14,
      filter: sourceFilter(DEFAULT_BUILDING_SOURCE),
      layout: { visibility: "none" },
      paint: { "fill-color": buildingContextColor, "fill-opacity": 0.3 },
    },
    {
      id: "buildings-all-outline",
      type: "line",
      source: "unmatched",
      "source-layer": "buildings_all",
      minzoom: 14,
      filter: sourceFilter(DEFAULT_BUILDING_SOURCE),
      layout: { visibility: "none" },
      paint: { "line-color": buildingAllOutlineColor, "line-width": 1, "line-opacity": 1 },
    },
    {
      id: "buildings-unmatched-fill",
      type: "fill",
      source: "unmatched",
      "source-layer": "buildings",
      minzoom: 14,
      filter: sourceFilter(DEFAULT_BUILDING_SOURCE),
      layout: { visibility: "none" },
      paint: { "fill-color": buildingAccentColor, "fill-opacity": 0.9 },
    },
    {
      id: "buildings-unmatched-outline",
      type: "line",
      source: "unmatched",
      "source-layer": "buildings",
      minzoom: 14,
      filter: sourceFilter(DEFAULT_BUILDING_SOURCE),
      layout: { visibility: "none" },
      paint: { "line-color": buildingAccentColor, "line-width": 1.4 },
    },
  ];

  // Tiers A (z5..11, aggregated bins) and B (z12..13, individual points) --
  // see src/server/tiles.rs. Both source-layers carry n_bdot10k/n_egib/
  // n_prg/n_total. Tier A also emits an agg_points source-layer (point per
  // bin, same aggregate as agg_cells); it fed circle and heatmap styles that
  // this frontend no longer offers, and nothing here reads it now.
  //
  // The A/B handoff sits at z11/z12, not z10/z11: at z11 individual unmatched
  // points are still packed close enough (bz caps at 14, so an 8x8-bin z11
  // tile is still binning real z14 cells together) that Tier B's dots render
  // as an unreadable speckle -- the grid stays legible one zoom level further
  // in before points take over.
  const aggLayers = [
    {
      id: "agg-grid-fill",
      type: "fill",
      source: "unmatched",
      "source-layer": "agg_cells",
      minzoom: 5,
      maxzoom: 12,
      paint: {
        // Bivariate by default (hue = ratio, opacity = count); the "Kolor"
        // toggle rewrites both together via applyAggMetric.
        "fill-color": ratioColorExpr("r_total"),
        "fill-opacity": countOpacityExpr("n_total"),
        "fill-outline-color": paperRaisedColor,
      },
    },
    {
      id: "points-dots",
      type: "circle",
      source: "unmatched",
      "source-layer": "points",
      minzoom: 12,
      maxzoom: 14,
      paint: {
        "circle-radius": ["interpolate", ["linear"], ["zoom"], 12, 3, 13, 4],
        "circle-color": [
          "match",
          ["get", "source"],
          "bdot10k",
          buildingAccentColor,
          "egib",
          buildingAccentColor,
          "prg",
          addressUnmatchedColor,
          /* other */ buildingContextColor,
        ],
        "circle-opacity": 0.85,
      },
    },
  ];

  // ---- recently-downloaded /package export areas ----
  //
  // One plain GeoJSON source (like "draw" below), refreshed wholesale on a
  // timer by pollUpdates rather than tied to the vector tile scheme -- an
  // export area is unrelated to z14 cells and needs to render the same way
  // at every zoom, not just z14+. No minzoom for the same reason.
  // fill-opacity is kept low so overlapping export areas stay legible as a
  // wash rather than stacking into solid colour, and so buildings/addresses
  // drawn on top of it (see the layers array below) aren't tinted away.
  const updatesLayers = [
    {
      id: "updates-fill",
      type: "fill",
      source: "updates",
      paint: { "fill-color": updatesColor, "fill-opacity": 0.12 },
    },
    {
      id: "updates-outline",
      type: "line",
      source: "updates",
      paint: { "line-color": updatesColor, "line-width": 1.6 },
    },
  ];

  // ---- "draw an area to download" overlay ----
  //
  // One plain GeoJSON source holding whatever the current drawing/selection
  // needs to render, swapped wholesale via .setData() on every geometry
  // change (see setDrawSourceData in the package-download section below) --
  // there's never more than a handful of features, so there's no case here
  // for the vector-tile-style "filter a big source" approach the rest of
  // this file uses. Features carry a `kind` property (polygon/vertex/
  // rubberband) and, for polygons, `phase` (drawing/committed) and `valid`
  // (false once the envelope exceeds MAX_AREA_SQ_DEG) -- separate layers key
  // off those instead of feature-state, since the whole source is rebuilt on
  // every change anyway.
  //
  // line-dasharray is a camera-only MapLibre property (not data-driven), so
  // "dashed while drawing, solid once committed" needs two line layers, not
  // one with a `["case", ["get","phase"], ...]` expression -- draw-outline-
  // drawing and draw-outline-committed each filter to their own phase.
  const drawLayers = [
    {
      id: "draw-fill",
      type: "fill",
      source: "draw",
      filter: ["==", ["get", "kind"], "polygon"],
      paint: {
        "fill-color": ["case", ["==", ["get", "valid"], false], drawInvalidColor, drawFillColor],
        "fill-opacity": 0.18,
      },
    },
    {
      id: "draw-outline-drawing",
      type: "line",
      source: "draw",
      filter: ["all", ["==", ["get", "kind"], "polygon"], ["==", ["get", "phase"], "drawing"]],
      paint: {
        "line-color": ["case", ["==", ["get", "valid"], false], drawInvalidColor, drawOutlineColor],
        "line-width": 2,
        "line-dasharray": [2, 2],
      },
    },
    {
      id: "draw-outline-committed",
      type: "line",
      source: "draw",
      filter: ["all", ["==", ["get", "kind"], "polygon"], ["==", ["get", "phase"], "committed"]],
      paint: {
        "line-color": ["case", ["==", ["get", "valid"], false], drawInvalidColor, drawOutlineColor],
        "line-width": 2.4,
      },
    },
    {
      // Two things reuse this layer: the last-vertex-to-cursor segment in
      // "Punkty" mode, and the open in-progress path in "Odręcznie" mode
      // before it has the 3 points needed to close into a polygon -- both
      // are "not a polygon yet" line previews, so one dashed-line layer
      // covers both instead of two near-identical ones.
      id: "draw-rubberband",
      type: "line",
      source: "draw",
      filter: ["==", ["get", "kind"], "rubberband"],
      paint: {
        "line-color": drawOutlineColor,
        "line-width": 1.4,
        "line-dasharray": [1, 1.5],
      },
    },
    {
      id: "draw-vertices",
      type: "circle",
      source: "draw",
      filter: ["==", ["get", "kind"], "vertex"],
      paint: {
        "circle-radius": 4,
        "circle-color": paperRaisedColor,
        "circle-stroke-color": drawOutlineColor,
        "circle-stroke-width": 2,
      },
    },
  ];

  const map = new maplibregl.Map({
    container: "map",
    style: {
      version: 8,
      // Self-hosted under web/fonts, served by the same static ServeDir as
      // app.js -- keeps the deploy self-contained rather than depending on a
      // third-party glyph host. Only the housenumber label layer below needs
      // this; every other layer stays glyph-free.
      glyphs: "/fonts/{fontstack}/{range}.pbf",
      sources: {
        osm: {
          type: "raster",
          tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
          tileSize: 256,
          maxzoom: 19,
          attribution: "podkład mapowy &copy; współautorzy OpenStreetMap",
        },
        unmatched: {
          type: "vector",
          tiles: [TILE_URL],
          minzoom: 5,
          maxzoom: 14,
        },
        draw: {
          type: "geojson",
          data: { type: "FeatureCollection", features: [] },
        },
        updates: {
          type: "geojson",
          data: { type: "FeatureCollection", features: [] },
        },
      },
      layers: [
        { id: "osm", type: "raster", source: "osm" },
        // Before buildingLayers/addresses so those features' own colours
        // stay crisp on top of the translucent export-area wash instead of
        // it tinting them.
        ...updatesLayers,
        ...buildingLayers,
        {
          id: "addresses-all-circle",
          type: "circle",
          source: "unmatched",
          "source-layer": "addresses_all",
          minzoom: 14,
          layout: { visibility: "none" },
          paint: {
            "circle-color": addressAllColor,
            "circle-radius": ["interpolate", ["linear"], ["zoom"], 14, 2, 18, 4.5],
          },
        },
        {
          id: "addresses-unmatched-circle",
          type: "circle",
          source: "unmatched",
          "source-layer": "addresses",
          minzoom: 14,
          layout: { visibility: "none" },
          paint: {
            "circle-color": addressUnmatchedColor,
            "circle-radius": ["interpolate", ["linear"], ["zoom"], 14, 3, 18, 7],
            "circle-stroke-color": "#fffdf8",
            "circle-stroke-width": 1.2,
          },
        },
        {
          // Housenumber label for unmatched PRG addresses only -- addresses_all
          // (the "every PRG address" legend layer) never gets tag/label
          // preview treatment, same reasoning as ALL_ADDRESSES_MVT_SQL not
          // applying the street-name mapping (see CLAUDE.md). Gated on z17+:
          // below that the circles are too close together for text to stay
          // legible (ADDRESSES_MVT_SQL has no per-zoom decimation, so density
          // near z14 would be all overlapping labels).
          id: "addresses-unmatched-label",
          type: "symbol",
          source: "unmatched",
          "source-layer": "addresses",
          minzoom: 17,
          layout: {
            visibility: "none",
            "text-field": ["get", "addr:housenumber"],
            "text-font": ["Klokantech Noto Sans Regular"],
            "text-size": 11,
            "text-anchor": "top",
            "text-offset": [0, 0.6],
            "text-optional": true,
          },
          paint: {
            "text-color": inkColor,
            "text-halo-color": paperRaisedColor,
            "text-halo-width": 1.2,
          },
        },
        ...aggLayers,
        // Last, so the draw overlay always renders on top of every data
        // layer -- it's a UI affordance the user is actively interacting
        // with, not another data series to blend with the rest.
        ...drawLayers,
      ],
    },
    center: [19.4, 52.0],
    zoom: 6,
    minZoom: 5,
    maxZoom: 19,
    hash: "map",
    // Suppresses MapLibre's default (compact, auto-collapsing) attribution
    // control so the explicit non-compact one added below is the only one.
    attributionControl: false,
  });
  map.addControl(new maplibregl.NavigationControl(), "top-right");
  // `compact: false` keeps the attribution text always visible instead of
  // MapLibre's default behaviour of collapsing it behind a click-to-reveal
  // "i" button once the map narrows -- the default hides exactly the credit
  // (OpenStreetMap contributors) this deployment is obligated to show.
  map.addControl(new maplibregl.AttributionControl({ compact: false }), "bottom-right");

  // Mouse-wheel zoom speed. MapLibre's default wheelZoomRate (1/450) maps a
  // typical notch (~100 deltaY) to ~0.15 zoom levels -- roughly 6-7 notches
  // per level. The mapping is an asymptotic sigmoid
  // (ScrollZoomHandler#_computeZoom in maplibre-gl), so exactly 1 level/notch
  // is unreachable, but this rate lands close to it (~0.9 levels/notch at a
  // 100-deltaY notch).
  map.scrollZoom.setWheelZoomRate(1 / 38);

  // ---- feature popups ----

  const CLICKABLE_LAYERS = [
    "buildings-all-fill",
    "buildings-unmatched-fill",
    "addresses-all-circle",
    "addresses-unmatched-circle",
    "updates-fill",
  ];

  function escapeHtml(value) {
    // Defers to the browser's own text-node serialization instead of a
    // hand-maintained character map, so it can't miss a special character.
    const el = document.createElement("span");
    el.textContent = String(value);
    return el.innerHTML;
  }

  // Every key any of the four MVT selects in src/server/tiles.rs can produce
  // (ADDRESSES_MVT_SQL, BUILDINGS_MVT_SQL, ALL_ADDRESSES_MVT_SQL,
  // ALL_BUILDINGS_MVT_SQL), spelled out by hand instead of derived from the
  // column name -- a translated label doesn't have to agree with whatever
  // shape the government schema happens to use (snake_case here, glued
  // uppercase there), and a missing key is a visible untranslated row instead
  // of a plausible-looking guess.
  const ATTRIBUTE_LABELS = {
    // shared identity/meta
    id: "ID",
    lokalny_id: "ID",
    source: "Zbiór danych",
    tags: "Tagi",

    // addresses -- prg_unmatched / prg_addresses
    numer_porzadkowy: "Numer porządkowy",
    ulica: "Ulica",
    miejscowosc: "Miejscowość",
    kod_pocztowy: "Kod pocztowy",
    wazny_od_lub_data_nadania: "Ważny od / data nadania",
    teryt_gmina: "TERYT gminy",
    gmina: "Gmina",

    // addresses -- OSM tag preview. Literal tag keys, shown unchanged: this
    // is naming the tag /package would write, not a column, so translating
    // it would stop it naming the tag it names.
    "addr:housenumber": "addr:housenumber",
    "addr:street": "addr:street",
    "addr:city": "addr:city",
    "addr:place": "addr:place",
    "addr:postcode": "addr:postcode",
    "addr:city:simc": "addr:city:simc",
    "source:addr": "source:addr",

    // buildings -- classification columns carried by bdot10k_unmatched /
    // egib_unmatched (see CLAUDE.md's building-type-mapping gotcha)
    funkcja_szczegolowa: "Przeważająca funkcja budynku",
    funkcja_ogolna: "Funkcja ogólna budynku",
    levels_above_ground: "Kondygnacje nadziemne",
    kondygnacje_podziemne: "Kondygnacje podziemne",
    rodzaj: "Rodzaj budynku",

    // buildings -- raw BDOT10k columns (also used by the buildings_all legend layer)
    PRZEWAZAJACAFUNKCJABUDYNKU: "Przeważająca funkcja budynku",
    FUNKCJAOGOLNABUDYNKU: "Funkcja ogólna budynku",
    KATEGORIAISTNIENIA: "Kategoria istnienia",
    NAZWA: "Nazwa",
    FSBUD: "Funkcje szczegółowe budynku",
    INFORMACJADODATKOWA: "Informacja dodatkowa",
    KODKST: "Kod KST",
    ZRODLODANYCHGEOMETRYCZNYCH: "Źródło geometrii",
  };

  function attributeLabel(key) {
    // Falls through to the raw key for anything not listed above, so a new
    // tile attribute appears in the popup by itself under its raw name
    // rather than silently going missing.
    return key in ATTRIBUTE_LABELS ? ATTRIBUTE_LABELS[key] : key;
  }

  // ADDRESSES_MVT_SQL has no single `tags` column like BUILDINGS_MVT_SQL --
  // it projects the OSM tag preview as separate addr:*/source:addr columns
  // directly on the feature (see the "addresses -- OSM tag preview" group in
  // ATTRIBUTE_LABELS above). describeFeature routes these into the same OSM
  // tags section as buildings' `tags` column, keyed on this set.
  const ADDRESS_TAG_KEYS = new Set([
    "addr:housenumber",
    "addr:street",
    "addr:city",
    "addr:place",
    "addr:postcode",
    "addr:city:simc",
    "source:addr",
  ]);

  // GUS Klasyfikacja Środków Trwałych (KŚT) group-1 names for the codes
  // BDOT10k's KODKST carries, copied from example_data/mappings/kst_mapping.csv.
  const KST_NAMES = {
    101: "Budynki przemysłowe",
    102: "Budynki transportu i łączności",
    103: "Budynki handlowo-usługowe",
    104: "Zbiorniki, silosy i budynki magazynowe",
    105: "Budynki biurowe",
    106: "Budynki szpitali i inne budynki opieki zdrowotnej",
    107: "Budynki oświaty, nauki i kultury oraz budynki sportowe",
    108: "Budynki produkcyjne, usługowe i gospodarcze dla rolnictwa",
    109: "Pozostałe budynki niemieszkalne",
    110: "Budynki mieszkalne",
  };

  // Hand-drawn in place of relying on a Unicode glyph (previously "ⓘ"/"⚠")
  // -- emoji-block characters like U+26A0 render at wildly different sizes
  // across fonts/platforms depending on whether a colour-emoji font is
  // available, which made the two hints' hoverable areas visually
  // inconsistent even though their CSS `cursor: help` was identical. Both
  // icons share the same 16x16 viewBox/14px box so they line up regardless
  // of glyph. `currentColor` picks up each span's `color` (see style.css),
  // so no fill/stroke colour is hardcoded here.
  const INFO_ICON_SVG =
    '<svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true" focusable="false">' +
    '<circle cx="8" cy="8" r="6.3" fill="none" stroke="currentColor" stroke-width="1.3"/>' +
    '<line x1="8" y1="7.1" x2="8" y2="11.3" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>' +
    '<circle cx="8" cy="4.7" r="0.9" fill="currentColor"/>' +
    "</svg>";
  const WARNING_ICON_SVG =
    '<svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true" focusable="false">' +
    '<polygon points="8,2 14.5,13.3 1.5,13.3" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linejoin="round"/>' +
    '<line x1="8" y1="6.3" x2="8" y2="9.6" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>' +
    '<circle cx="8" cy="11.6" r="0.9" fill="currentColor"/>' +
    "</svg>";

  // A hoverable info icon appended after KODKST's value, decoding the
  // numeric code to its KŚT category name. KST_NAMES is a fixed table baked
  // into this file, not tile data, so unlike escapeHtml's inputs it needs no
  // escaping.
  function kstHint(key, value) {
    const name = KST_NAMES[String(value).trim()];
    if (key !== "KODKST" || !name) return "";
    return ` <span class="kst-hint" title="${name}">${INFO_ICON_SVG}</span>`;
  }

  // BDOT10K_EKSPLOATOWANY_FILTER in src/compare/rule.rs -- the one value of
  // KATEGORIAISTNIENIA the match rule accepts. Any other value (w budowie,
  // nieczynny, zniszczony) is filtered out of the comparison entirely, so the
  // building can never appear as matched *or* unmatched, no matter what's on
  // the ground in OSM. Kept in sync by hand since this is the frontend's only
  // link to that Rust constant.
  const BDOT10K_EKSPLOATOWANY = "eksploatowany";

  // A hoverable warning icon appended after KATEGORIAISTNIENIA's value on
  // the buildings_all legend layer, flagging that this building is excluded
  // from the OSM comparison outright (see the CLAUDE.md gotcha on this
  // filter). Only bdot10k carries this column -- egib rows have it NULL --
  // and only a non-"eksploatowany" value is exclusion-worthy, so an in-use
  // building shows no hint at all.
  function exclusionHint(key, value) {
    if (key !== "KATEGORIAISTNIENIA") return "";
    const v = String(value).trim();
    if (!v || v === BDOT10K_EKSPLOATOWANY) return "";
    return (
      ` <span class="exclusion-hint" title="Budynek pominięty w porównaniu z OSM ` +
      `(kategoria inna niż „eksploatowany”)">${WARNING_ICON_SVG}</span>`
    );
  }

  function formatValue(key, value) {
    if (value === null || value === undefined || value === "") return "—";
    return String(value);
  }

  // `tags` is the resolved k=v;k=v string the building-type mapping produced
  // -- the tags /package would export for this object. Split into individual
  // [key, value] pairs so the OSM-tags section can list one tag per row
  // instead of one blob cell, matching the attributes section's layout.
  function parseTags(value) {
    if (value === null || value === undefined || value === "") return [];
    return String(value)
      .split(";")
      .filter((pair) => pair !== "")
      .map((pair) => {
        const i = pair.indexOf("=");
        return i === -1 ? [pair, ""] : [pair.slice(0, i), pair.slice(i + 1)];
      });
  }

  // updates-fill's properties (exported_at/datasets/address_count/
  // building_count, see UpdateProperties in src/server/updates.rs) don't
  // follow the tile attribute shape describeFeature below parses -- datasets
  // is a real array (a plain GeoJSON source, not MVT, carries JSON types
  // as-is) and there's no OSM-tags preview to split out -- so this builds
  // the popup shape directly instead of routing through that parser.
  // DATASET_LABELS is declared further down (the package-download section)
  // but already initialized by the time any click can fire.
  function describeUpdateFeature(props) {
    const datasets = Array.isArray(props.datasets) ? props.datasets : [];
    return {
      title: "Pobrany obszar",
      status: "Eksport",
      attributes: [
        ["exported_at", "Data eksportu", fmtTimestamp(props.exported_at)],
        ["datasets", "Warstwy", datasets.map((d) => DATASET_LABELS[d] || d).join(", ") || "—"],
        ["address_count", "Adresy", String(props.address_count)],
        ["building_count", "Budynki", String(props.building_count)],
      ],
      tags: [],
    };
  }

  function describeFeature(layerId, props) {
    if (layerId === "updates-fill") return describeUpdateFeature(props);
    const attributes = [];
    let tags = [];
    // Insertion order is the tile's attribute order, which follows the
    // SELECT in src/server/tiles.rs (identity, then classification, then
    // the resolved tags) -- more useful than sorting alphabetically.
    // ST_AsMVT drops NULL attributes, so this is exactly the set that
    // feature actually has: an EGIB building carries `rodzaj`, a BDOT10k
    // one carries FSBUD/KODKST/..., and neither shows the other's blanks.
    // The raw key rides along (not shown) so popupHtml can special-case
    // KODKST for the KST hover hint below.
    for (const key of Object.keys(props)) {
      if (key === "tags") {
        tags = parseTags(props[key]);
        continue;
      }
      if (ADDRESS_TAG_KEYS.has(key)) {
        tags.push([key, formatValue(key, props[key])]);
        continue;
      }
      attributes.push([key, attributeLabel(key), formatValue(key, props[key])]);
    }
    return {
      title: layerId.startsWith("buildings-") ? "Budynek" : "Adres",
      // Not an attribute -- it comes from which layer was clicked, not from
      // the data -- so it sits in the header rather than among the rows.
      status: layerId.includes("unmatched") ? "Niedopasowany" : "W rejestrze",
      attributes,
      tags,
    };
  }

  function popupSection(heading, rows) {
    if (!rows.length) return "";
    const rowsHtml = rows
      .map(
        ([key, label, value]) =>
          `<dt>${escapeHtml(label)}</dt><dd>${escapeHtml(value)}${kstHint(key, value)}${exclusionHint(key, value)}</dd>`
      )
      .join("");
    return `<h4 class="feature-popup-section">${escapeHtml(heading)}</h4><dl>${rowsHtml}</dl>`;
  }

  function popupHtml({ title, status, attributes, tags }) {
    // Each present only when it has rows -- a feature with no OSM-tag preview
    // (or, in principle, no plain attributes) shows a single section rather
    // than an empty one with nothing under its heading.
    const attributesHtml = popupSection("Atrybuty", attributes);
    const tagsHtml = popupSection(
      "Tagi OSM",
      tags.map(([key, value]) => [key, key, value === "" ? "—" : value])
    );
    return (
      `<div class="feature-popup"><h3>${escapeHtml(title)}` +
      `<span class="feature-status">${escapeHtml(status)}</span></h3>` +
      `<div class="feature-popup-body">${attributesHtml}${tagsHtml}</div></div>`
    );
  }

  // closeOnClick is deliberately off. MapLibre registers its close-on-click
  // listener when the popup is *added*, i.e. after this module's own click
  // handler, so with it on, clicking feature B while A's popup was open ran
  // the two in the order "re-add popup", "close popup" and the popup simply
  // vanished -- you had to click twice to see anything. Closing it below
  // instead, and only when the click missed every clickable layer, doesn't
  // depend on handler registration order.
  const popup = new maplibregl.Popup({ closeButton: true, closeOnClick: false, maxWidth: "320px" });

  // Guarded on appDrawState rather than wired up/down per drawing mode (see
  // setupModeHandlers/teardownDrawMode further down): these two listeners are
  // registered once at startup and every drawing mode -- even rectangle and
  // freehand, whose drags don't normally emit a `click` -- can still produce
  // one on a small or jittery gesture. Without the guard, a "Punkty" vertex
  // click landing on a building opens a feature popup that then follows every
  // subsequent vertex click, covering the area being drawn. appDrawState is
  // declared later in this file but that's fine: these callbacks only run in
  // response to a later user click, by which point the whole module has
  // finished its initial pass and the `let` is live.
  map.on("click", CLICKABLE_LAYERS, (e) => {
    if (appDrawState === "drawing") return;
    const feature = e.features[0];
    popup
      .setLngLat(e.lngLat)
      .setHTML(popupHtml(describeFeature(feature.layer.id, feature.properties)))
      .addTo(map);
  });
  map.on("click", (e) => {
    if (appDrawState === "drawing") return;
    if (!map.queryRenderedFeatures(e.point, { layers: CLICKABLE_LAYERS }).length) {
      popup.remove();
    }
  });
  // Same guard, and for the same reason the cursor is handled here rather
  // than in setupModeHandlers/teardownDrawMode: while drawing, enterDrawingMode
  // sets a crosshair cursor as a "click places a vertex" affordance, and
  // hovering a building would otherwise flip it to "pointer" (reading as
  // "click for info") via this pair -- mouseleave would then hand back ""
  // instead of the crosshair it stomped. Leaving the cursor alone here while
  // drawing keeps the crosshair set by enterDrawingMode in place untouched.
  map.on("mouseenter", CLICKABLE_LAYERS, () => {
    if (appDrawState === "drawing") return;
    map.getCanvas().style.cursor = "pointer";
  });
  map.on("mouseleave", CLICKABLE_LAYERS, () => {
    if (appDrawState === "drawing") return;
    map.getCanvas().style.cursor = "";
  });

  // ---- state <-> URL hash ----
  //
  // MapLibre owns the `map=` hash entry (`hash: "map"` above). Its own Hash
  // class only ever touches its own key and leaves every other entry
  // untouched -- so a second entry here coexists safely as long as our
  // reads/writes follow that same "touch only our own key" discipline, which
  // readStateHash/writeStateHash below do. (As of MapLibre v6 the Hash class
  // round-trips the whole fragment through `URLSearchParams` internally
  // rather than splitting the raw string on `&` as it did pre-v6, but it
  // percent-decodes the result before writing it back, which is lossless for
  // our comma-separated values -- verified against v6's hash.ts source, not
  // merely assumed.)
  const STATE_HASH_KEY = "layers";
  const VALID_BUILDING_SOURCES = ["off", "bdot10k", "egib"];
  const VALID_ADDRESS_SOURCES = ["off", "prg"];
  const VALID_AGG_COLORS = ["ratio", "count"];

  function readStateHash() {
    const raw = location.hash.replace(/^#/, "");
    const entry = raw.split("&").find((part) => part.split("=")[0] === STATE_HASH_KEY);
    const value = entry ? entry.slice(entry.indexOf("=") + 1) : "";
    const [buildingSource, addressSource, aggColor] = value.split(",");
    return {
      buildingSource: VALID_BUILDING_SOURCES.includes(buildingSource) ? buildingSource : null,
      addressSource: VALID_ADDRESS_SOURCES.includes(addressSource) ? addressSource : null,
      aggColor: VALID_AGG_COLORS.includes(aggColor) ? aggColor : null,
    };
  }

  // Called after every state-changing click. Rewrites just the STATE_HASH_KEY
  // entry, preserving `map=...` (and anything else already in the hash)
  // exactly as MapLibre's own getHashString does for its own entry.
  function writeStateHash() {
    const value = `${STATE_HASH_KEY}=${buildingSource},${addressSource},${aggColor}`;
    const parts = location.hash.replace(/^#/, "").split("&").filter(Boolean);
    let found = false;
    const next = parts.map((part) => {
      if (part.split("=")[0] === STATE_HASH_KEY) {
        found = true;
        return value;
      }
      return part;
    });
    if (!found) next.push(value);
    history.replaceState(history.state, "", `${location.pathname}${location.search}#${next.join("&")}`);
  }

  // Read once at startup -- opening a URL that already carries a `layers=`
  // entry should restore that state instead of always falling back to the
  // HTML defaults (bdot10k/prg/ratio) below.
  const restoredState = readStateHash();

  // ---- legend ----

  const legendBuildingsGroup = document.querySelector(".legend-group[data-building-source]");
  const legendAddressesGroup = document.querySelector(".legend-group[data-address-source]");
  const legendUpdatesGroup = document.querySelector(".legend-group[data-updates-source]");
  const buildingSourceButtons = legendBuildingsGroup.querySelectorAll(".source-btn");
  const addressSourceButtons = legendAddressesGroup.querySelectorAll(".source-btn");
  const updatesSourceButtons = legendUpdatesGroup.querySelectorAll(".source-btn");

  // "off"/"bdot10k"/"egib" and "off"/"prg": one mutually-exclusive control per
  // category replaces the old pair of "all"/"unmatched" checkboxes -- picking
  // a registry shows both its all+unmatched layers together (previously the
  // default, and only tested, checkbox state anyway); "Wyłącz" hides both.
  let buildingSource = restoredState.buildingSource || legendBuildingsGroup.dataset.buildingSource;
  let addressSource = restoredState.addressSource || legendAddressesGroup.dataset.addressSource;
  legendBuildingsGroup.dataset.buildingSource = buildingSource;
  legendAddressesGroup.dataset.addressSource = addressSource;
  // Buttons default to bdot10k/prg pressed in the HTML; if the hash restored
  // a different value, the buttons must catch up so they don't lie about
  // which state is actually active.
  for (const b of buildingSourceButtons) {
    b.setAttribute("aria-pressed", String(b.dataset.source === buildingSource));
  }
  for (const b of addressSourceButtons) {
    b.setAttribute("aria-pressed", String(b.dataset.addressSource === addressSource));
  }

  // "on"/"off": recently-downloaded export areas are their own single
  // registry-less overlay (see pollUpdates below), so there's no third state
  // to pick between the way buildingSource/addressSource choose a registry --
  // just visible or not. Not restored from the URL hash: unlike the other
  // three toggles it's not a *view* of one map layer's data, and adding a
  // fourth hash field for a low-stakes default-on overlay isn't worth it.
  let updatesVisible = legendUpdatesGroup.dataset.updatesSource !== "off";
  for (const b of updatesSourceButtons) {
    b.setAttribute("aria-pressed", String(b.dataset.updatesSource === (updatesVisible ? "on" : "off")));
  }

  function setLayerVisible(layerId, visible) {
    map.setLayoutProperty(layerId, "visibility", visible ? "visible" : "none");
  }

  function applyBuildingVisibility() {
    const visible = buildingSource !== "off";
    if (visible) {
      map.setFilter("buildings-all-fill", sourceFilter(buildingSource));
      map.setFilter("buildings-all-outline", sourceFilter(buildingSource));
      map.setFilter("buildings-unmatched-fill", sourceFilter(buildingSource));
      map.setFilter("buildings-unmatched-outline", sourceFilter(buildingSource));
    }
    setLayerVisible("buildings-all-fill", visible);
    setLayerVisible("buildings-all-outline", visible);
    setLayerVisible("buildings-unmatched-fill", visible);
    setLayerVisible("buildings-unmatched-outline", visible);
  }

  function applyAddressVisibility() {
    // No filter needed: addresses/addresses_all source-layers are PRG-only
    // (see src/server/tiles.rs) -- "Wyłącz" is the only other state.
    const visible = addressSource !== "off";
    setLayerVisible("addresses-all-circle", visible);
    setLayerVisible("addresses-unmatched-circle", visible);
    setLayerVisible("addresses-unmatched-label", visible);
  }

  function applyUpdatesVisibility() {
    setLayerVisible("updates-fill", updatesVisible);
    setLayerVisible("updates-outline", updatesVisible);
  }

  function wireLegend() {
    for (const btn of buildingSourceButtons) {
      btn.addEventListener("click", () => {
        buildingSource = btn.dataset.source;
        legendBuildingsGroup.dataset.buildingSource = buildingSource;
        for (const b of buildingSourceButtons) {
          b.setAttribute("aria-pressed", String(b === btn));
        }
        applyBuildingVisibility();
        // The grid (agg-grid-fill) mirrors this toggle too -- see
        // activeAggSources/applyAggMetric below.
        applyAggMetric();
        writeStateHash();
      });
    }
    for (const btn of addressSourceButtons) {
      btn.addEventListener("click", () => {
        addressSource = btn.dataset.addressSource;
        legendAddressesGroup.dataset.addressSource = addressSource;
        for (const b of addressSourceButtons) {
          b.setAttribute("aria-pressed", String(b === btn));
        }
        applyAddressVisibility();
        applyAggMetric();
        writeStateHash();
      });
    }
    for (const btn of updatesSourceButtons) {
      btn.addEventListener("click", () => {
        updatesVisible = btn.dataset.updatesSource === "on";
        legendUpdatesGroup.dataset.updatesSource = btn.dataset.updatesSource;
        for (const b of updatesSourceButtons) {
          b.setAttribute("aria-pressed", String(b === btn));
        }
        applyUpdatesVisibility();
      });
    }
    // Layer visibility starts as "none" in the style spec above; derive the
    // real initial state from the buttons' own aria-pressed default here
    // instead of duplicating it. updates-fill/-outline have no such default
    // (they start visible, see the layers array above), but applying it here
    // too keeps this one call site the single source of "what's on at
    // startup" instead of splitting it across two places.
    applyBuildingVisibility();
    applyAddressVisibility();
    applyUpdatesVisibility();
  }

  // ---- low-zoom aggregate controls (z5-13) ----

  // The z5-11 tier is a single visualisation (the grid); a "Styl" picker that
  // also offered circles and a heatmap was tried and removed. The grid no
  // longer has its own "Dane" picker either -- see the "combined grid Dane"
  // comment near combinedCountValueExpr above -- it now mirrors whichever
  // registries the Budynki/Adresy toggles above have turned on. Only "Kolor"
  // remains its own control, since it's orthogonal (how to draw the data, not
  // which data). points-dots (z12-13) has no style/Dane hook of its own --
  // it's always dots colored by source -- but it does mirror the same active
  // Budynki/Adresy sources as the grid (applyAggMetric below sets its filter
  // too), so switching to e.g. BDOT10k hides EGiB's dots the same way it
  // hides EGiB's grid contribution.
  const aggColorButtons = document.querySelectorAll("#agg-color-toggle .metric-btn");
  const ratioLegend = document.getElementById("ratio-legend");
  const countLegend = document.getElementById("count-legend");

  let aggColor = restoredState.aggColor || "ratio";
  for (const b of aggColorButtons) {
    b.setAttribute("aria-pressed", String(b.dataset.color === aggColor));
  }

  function activeAggSources() {
    const sources = [];
    if (buildingSource !== "off") sources.push(buildingSource);
    if (addressSource !== "off") sources.push("prg");
    return sources;
  }

  function applyAggMetric() {
    const sources = activeAggSources();
    const visible = sources.length > 0;
    setLayerVisible("agg-grid-fill", visible);
    // points-dots carries all three sources unfiltered from the tile (see
    // POINTS_MVT_SQL in src/server/tiles.rs) -- restrict it to whichever
    // registries are actually toggled on, same set the grid uses.
    setLayerVisible("points-dots", visible);
    if (visible) {
      map.setFilter("points-dots", ["in", ["get", "source"], ["literal", sources]]);
    }
    if (ratioLegend) ratioLegend.hidden = !visible || aggColor !== "ratio";
    if (countLegend) countLegend.hidden = !visible || aggColor !== "count";
    if (!visible) return;

    const ratioMode = aggColor === "ratio";
    const countValue = combinedCountValueExpr(sources);

    // Grid: in ratio mode hue and opacity carry different variables; in count
    // mode hue carries the count and opacity goes flat, so the two modes stay
    // visually comparable rather than one being a dimmer version of the other.
    map.setPaintProperty(
      "agg-grid-fill",
      "fill-color",
      ratioMode ? ratioColorExprFromValue(combinedRatioValueExpr(sources)) : zoomPropertyRampFromValue(countValue, rampColors),
    );
    map.setPaintProperty(
      "agg-grid-fill",
      "fill-opacity",
      ratioMode ? zoomPropertyRampFromValue(countValue, COUNT_OPACITY_STOPS) : 0.75,
    );
  }

  function wireAggLegend() {
    for (const btn of aggColorButtons) {
      btn.addEventListener("click", () => {
        aggColor = btn.dataset.color;
        for (const b of aggColorButtons) {
          b.setAttribute("aria-pressed", String(b === btn));
        }
        applyAggMetric();
        writeStateHash();
      });
    }
    // Same reasoning as wireLegend above: layer paint starts from the style
    // spec's defaults (r_total); derive the real initial state from the
    // buttons' own aria-pressed default instead of trusting the two to stay
    // in sync by hand.
    applyAggMetric();
  }

  if (map.isStyleLoaded()) {
    wireLegend();
    wireAggLegend();
    pollUpdates();
  } else {
    map.once("load", () => {
      wireLegend();
      wireAggLegend();
      pollUpdates();
    });
  }

  // ---- status panel ----

  const statusToggle = document.getElementById("status-toggle");
  const statusBody = document.getElementById("status-body");
  const statusDot = document.getElementById("status-dot");
  const statusTableBody = document.querySelector("#status-table tbody");
  const stalenessHint = document.getElementById("staleness-hint");
  const osmReplicationHint = document.getElementById("osm-replication-hint");
  const jobDetailsModal = document.getElementById("job-details-modal");
  const jobDetailsTitle = document.getElementById("job-details-title");
  const jobDetailsSummary = document.getElementById("job-details-summary");
  const jobDetailsLogs = document.getElementById("job-details-logs");
  const jobDetailsClose = document.getElementById("job-details-close");

  const JOB_STATE_LABELS = {
    idle: "bezczynne",
    running: "w trakcie",
    disabled: "wyłączone",
  };

  // The header dot answers one question only -- "can this page currently
  // reach the server" -- so it has just two states, set from `pollStatus`'s
  // own try/catch rather than derived from job data. Per-job dots answer a
  // different question ("what is THIS job doing / what did it last do") and
  // get their own state->title mapping below (`jobDotTitle`); the two must
  // not be merged into one shared table again, or the header silently goes
  // back to meaning "any job errored" instead of "server reachable".
  const SERVER_DOT_TITLES = {
    idle: "Serwer działa",
    error: "Błąd połączenia z serwerem",
  };

  const OUTCOME_LABELS = {
    Success: "sukces",
    Error: "błąd",
    TimedOut: "przekroczono limit czasu",
  };

  // Shared fold/unfold behaviour for every collapsible panel (status,
  // legend, download) -- a toggle button and the [hidden] body it controls,
  // wired identically so aria-expanded and the chevron in
  // .panel-toggle-icon (see style.css) never drift out of sync with the
  // actual visibility.
  function wirePanelToggle(toggle, body) {
    toggle.addEventListener("click", () => {
      const expanded = toggle.getAttribute("aria-expanded") === "true";
      toggle.setAttribute("aria-expanded", String(!expanded));
      body.hidden = expanded;
    });
  }

  wirePanelToggle(statusToggle, statusBody);
  wirePanelToggle(document.getElementById("legend-toggle"), document.getElementById("legend-body"));
  wirePanelToggle(document.getElementById("download-toggle"), document.getElementById("download-body"));

  // /status carries timestamps in two shapes: the in-memory job registry
  // emits plain RFC3339 UTC ("...Z"), while job_run_log's ran_at and
  // match_dirty_cells' enqueued_at come back from a DuckDB
  // `TIMESTAMP WITH TIME ZONE`::VARCHAR cast -- "YYYY-MM-DD HH:MM:SS.ffffff+HH",
  // a space instead of "T" and a bare hour offset some engines refuse to
  // parse. Normalizing both to a shape every `Date` parser accepts (rather
  // than trusting `new Date(raw)` directly) is what lets every timestamp
  // in this panel render in the browser's own timezone instead of quietly
  // keeping whichever timezone the server happened to format it in.
  function parseTimestamp(raw) {
    if (!raw) return null;
    let s = raw.trim().replace(" ", "T");
    s = s.replace(/([+-]\d{2})$/, "$1:00");
    const d = new Date(s);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  const TIMESTAMP_FORMATTER = new Intl.DateTimeFormat("pl-PL", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  // No explicit `timeZone` option -- Intl defaults to the browser's own,
  // which is the point: the same raw value now renders differently for a
  // viewer in a different timezone than the server, instead of always UTC.
  function fmtTimestamp(raw) {
    const d = parseTimestamp(raw);
    return d ? TIMESTAMP_FORMATTER.format(d) : "—";
  }

  function fmtDuration(ms) {
    if (ms == null) return "—";
    if (ms < 1000) return `${ms} ms`;
    const totalSeconds = ms / 1000;
    if (totalSeconds < 60) return `${totalSeconds.toFixed(1)} s`;
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = Math.round(totalSeconds % 60);
    return `${minutes} min ${seconds} s`;
  }

  function fmtIntervalSeconds(sec) {
    if (sec == null) return "—";
    if (sec < 60) return `${sec} s`;
    if (sec < 3600) return `${Math.round(sec / 60)} min`;
    if (sec < 86400) {
      const hours = sec / 3600;
      return `${Number.isInteger(hours) ? hours : hours.toFixed(1)} h`;
    }
    const days = sec / 86400;
    return `${Number.isInteger(days) ? days : days.toFixed(1)} d`;
  }

  function fmtOutcome(outcome) {
    if (!outcome) return { label: "—", detail: "" };
    return { label: OUTCOME_LABELS[outcome.kind] || outcome.kind, detail: outcome.message || "" };
  }

  // A job's dot has exactly one job: show what it's doing right now, or --
  // once it's idle -- what its last run actually resulted in. "idle" no
  // longer means "green because nothing's happening"; it means "green
  // because the last run succeeded" (and red if it didn't), so the color
  // itself carries the last-execution-result signal the row's text alone
  // doesn't.
  function jobDotState(job) {
    if (job.state === "running") return "running";
    if (job.state === "disabled") return "";
    if (job.last_outcome && job.last_outcome.kind !== "Success") return "error";
    return "idle";
  }

  function jobDotTitle(job, dotState) {
    if (dotState === "running") return "W trakcie";
    if (dotState === "") return "Wyłączone";
    if (!job.last_outcome) return "Bezczynne — brak danych o poprzednim uruchomieniu";
    const outcome = fmtOutcome(job.last_outcome);
    return outcome.detail
      ? `Bezczynne — ${outcome.label}: ${outcome.detail}`
      : `Bezczynne — ${outcome.label}`;
  }

  function detailRow(term, valueHtml) {
    return `<div><dt>${escapeHtml(term)}</dt><dd>${valueHtml}</dd></div>`;
  }

  // Keyed by job_run_log's own keys (e.g. "update:bdot10k"), not the job
  // registry's names -- see `Job::log_keys` on the server, which each
  // job's row in `jobs[]` echoes back as `log_keys` so this panel can join
  // the two without hardcoding the mapping itself.
  let latestJobRunLog = {};

  function renderStatus(data) {
    const jobs = data.jobs || [];
    latestJobRunLog = data.job_run_log || {};

    statusTableBody.innerHTML = "";
    for (const job of jobs) {
      const row = document.createElement("tr");
      const stateLabel = JOB_STATE_LABELS[job.state] || job.state;
      const dotState = jobDotState(job);
      const dotTitle = jobDotTitle(job, dotState);
      const stateCell = document.createElement("td");
      stateCell.innerHTML = `<span class="job-state"><span class="status-dot" data-state="${dotState}" title="${dotTitle}"></span>${stateLabel}</span>`;
      row.innerHTML = `<td>${job.name}</td>`;
      row.appendChild(stateCell);

      const lastRun = document.createElement("td");
      lastRun.textContent = fmtTimestamp(job.last_finished_at);
      row.appendChild(lastRun);

      const duration = document.createElement("td");
      duration.textContent = fmtDuration(job.last_duration_ms);
      row.appendChild(duration);

      const nextRun = document.createElement("td");
      nextRun.textContent = fmtTimestamp(job.next_run_at);
      row.appendChild(nextRun);

      const detailsCell = document.createElement("td");
      const detailsBtn = document.createElement("button");
      detailsBtn.type = "button";
      detailsBtn.className = "job-details-btn";
      detailsBtn.textContent = "i";
      detailsBtn.title = "Szczegóły";
      detailsBtn.setAttribute("aria-label", `Szczegóły: ${job.name}`);
      detailsBtn.addEventListener("click", () => openJobDetails(job));
      detailsCell.appendChild(detailsBtn);
      row.appendChild(detailsCell);

      statusTableBody.appendChild(row);
    }

    const staleness = data.match_staleness;
    if (staleness && staleness.pending_total > 0) {
      stalenessHint.textContent = `Kolejka: ${staleness.pending_total} komórek oczekujących na odświeżenie.`;
    } else {
      stalenessHint.textContent = "Kolejka: 0 komórek oczekujących na odświeżenie.";
    }

    const osmReplication = data.osm_replication;
    if (osmReplication && osmReplication.sequence_number != null) {
      osmReplicationHint.textContent = `Stan zapisanych danych OSM: numer sekwencji ${osmReplication.sequence_number} (${fmtTimestamp(
        osmReplication.timestamp
      )}).`;
    } else {
      osmReplicationHint.textContent = "Stan zapisanych danych OSM: brak danych o sekwencji replikacji.";
    }
  }

  function openJobDetails(job) {
    jobDetailsTitle.textContent = job.name;

    const outcome = fmtOutcome(job.last_outcome);
    jobDetailsSummary.innerHTML = [
      detailRow("Włączone", job.enabled ? "tak" : "nie"),
      detailRow("Interwał", fmtIntervalSeconds(job.interval_seconds)),
      detailRow("Limit czasu", fmtIntervalSeconds(job.timeout_seconds)),
      detailRow("Stan", escapeHtml(JOB_STATE_LABELS[job.state] || job.state)),
      detailRow("Ostatnie uruchomienie", fmtTimestamp(job.last_started_at)),
      detailRow("Ostatnie zakończenie", fmtTimestamp(job.last_finished_at)),
      detailRow("Czas trwania", fmtDuration(job.last_duration_ms)),
      detailRow("Następne uruchomienie", fmtTimestamp(job.next_run_at)),
      detailRow("Liczba uruchomień", String(job.run_count)),
      detailRow(
        "Ostatni wynik",
        outcome.detail
          ? `${escapeHtml(outcome.label)}: ${escapeHtml(outcome.detail)}`
          : escapeHtml(outcome.label)
      ),
    ].join("");

    const logKeys = job.log_keys || [];
    if (logKeys.length === 0) {
      jobDetailsLogs.innerHTML = `<p class="hint">Brak dodatkowego logu dla tego zadania.</p>`;
    } else {
      jobDetailsLogs.innerHTML = logKeys
        .map((key) => {
          const entry = latestJobRunLog[key];
          if (!entry) {
            return (
              `<div class="job-log-entry"><h3>${escapeHtml(key)}</h3>` +
              `<p class="hint">Brak zapisanego przebiegu.</p></div>`
            );
          }
          return (
            `<div class="job-log-entry"><h3>${escapeHtml(key)}</h3>` +
            `<dl class="feedback-modal-details">` +
            detailRow("Czas", fmtTimestamp(entry.ran_at)) +
            detailRow("Wynik", escapeHtml(entry.outcome)) +
            detailRow("Wiadomość", entry.message ? escapeHtml(entry.message) : "—") +
            `</dl></div>`
          );
        })
        .join("");
    }

    if (!jobDetailsModal.open) jobDetailsModal.showModal();
  }

  jobDetailsClose.addEventListener("click", () => jobDetailsModal.close());
  // Same backdrop-click-to-dismiss handling as #download-feedback below.
  jobDetailsModal.addEventListener("click", (e) => {
    if (e.target === jobDetailsModal) jobDetailsModal.close();
  });

  async function pollStatus() {
    try {
      const res = await fetch("/status");
      if (!res.ok) throw new Error(`status ${res.status}`);
      const data = await res.json();
      statusDot.dataset.state = "idle";
      statusDot.title = SERVER_DOT_TITLES.idle;
      renderStatus(data);
    } catch (err) {
      statusDot.dataset.state = "error";
      statusDot.title = SERVER_DOT_TITLES.error;
      console.error("failed to load /status", err);
    }
  }

  pollStatus();
  setInterval(pollStatus, 30000);

  // ---- recently-downloaded /package export areas ----
  //
  // GET /updates with no ?minutes= uses the server's own default window
  // (config [updates].default_minutes, 60 by default -- see
  // src/server/updates.rs) rather than picking a separate value here, so a
  // deployment that changes the window doesn't need this file touched too.
  // Polled on the response's own Cache-Control max-age (60s by default,
  // [cache].updates_max_age_seconds) -- polling faster would just re-fetch
  // the same cached body.
  async function pollUpdates() {
    try {
      const res = await fetch("/updates");
      if (!res.ok) throw new Error(`updates ${res.status}`);
      const data = await res.json();
      map.getSource("updates").setData(data);
    } catch (err) {
      console.error("failed to load /updates", err);
    }
  }

  setInterval(pollUpdates, 60000);

  // ---- package download ----

  const downloadBtn = document.getElementById("download-btn");
  const drawBtn = document.getElementById("draw-btn");
  const drawModePicker = document.getElementById("draw-mode-picker");
  const drawModeButtons = drawModePicker.querySelectorAll(".source-btn");
  const drawCancelBtn = document.getElementById("draw-cancel-btn");
  const drawClearBtn = document.getElementById("draw-clear-btn");
  const drawHint = document.getElementById("draw-hint");
  const downloadZoomHint = document.getElementById("download-zoom-hint");
  const downloadFeedback = document.getElementById("download-feedback");
  const downloadFeedbackBbox = document.getElementById("download-feedback-bbox");
  const downloadFeedbackLayers = document.getElementById("download-feedback-layers");
  const downloadFeedbackText = document.getElementById("download-feedback-text");
  const downloadFeedbackClose = document.getElementById("download-feedback-close");

  // /package rejects any request whose bbox (GET) or polygon envelope (POST)
  // exceeds config.package.max_area_sq_deg (0.04 by default -- see check_area
  // in src/server/package.rs). At z0-11 the visible viewport is already far
  // larger than that, so "Pobierz widoczny obszar" would just 400 -- that
  // button stays gated on zoom below.
  const MIN_DOWNLOAD_ZOOM = 12;
  let downloadInFlight = false;

  // Mirrors config.package.max_area_sq_deg's default (0.04, see
  // check_area/RequestArea::bbox_area_sq_deg in src/server/package.rs) the
  // same way MIN_DOWNLOAD_ZOOM above mirrors a server constraint -- this only
  // avoids sending a request the server is guaranteed to reject; the server
  // stays authoritative (a deployment with a different configured limit still
  // 400s and that surfaces through the existing feedback modal, same as any
  // other request error). Server-side the limit is the *envelope* area, not
  // the polygon's true area (bbox_area_sq_deg), so that's what's mirrored
  // here too -- a diagonal sliver polygon can have a huge envelope and a tiny
  // true area, and the server would still reject it.
  const MAX_AREA_SQ_DEG = 0.04;

  // ---- draw-an-area-to-download state machine ----
  //
  // Three states -- idle (no polygon), drawing (one of the three modes is
  // live), selected (a polygon has been committed) -- see the state table in
  // the design doc this shipped from. One primary button (#download-btn)
  // covers idle+selected (its label and click behaviour change); #draw-btn
  // covers idle ("Narysuj obszar") and selected ("Narysuj ponownie"); the
  // mode picker + Anuluj only show while drawing; Usuń zaznaczenie only shows
  // once selected. applyDownloadPanelState (below) is the single place that
  // reconciles all of that against {appDrawState, map zoom, downloadInFlight}
  // -- every state-changing function in this section ends by calling it
  // rather than poking DOM visibility itself, so the panel can never drift
  // out of sync with the state variables.
  let appDrawState = "idle"; // 'idle' | 'drawing' | 'selected'

  // Last-used mode, remembered only for this page session (module-level, not
  // localStorage -- this frontend has no persisted-preference pattern
  // elsewhere and adding one just for this would be scope creep). Rectangle
  // is the default first mode since it needs no explanation.
  let drawMode = "rectangle";

  // In-progress geometry, one set of fields per mode -- only the fields for
  // the active mode are ever populated; resetInProgressGeometry (called on
  // every entry into "drawing" and on every commit) clears all of them.
  let rectStartLngLat = null; // [lng, lat] of the rectangle's anchor corner
  let rectStartPoint = null; // screen Point of the same corner, for the drag-threshold check
  let clickVertices = []; // "Punkty": committed [lng, lat] vertices so far
  let clickVertexPoints = []; // parallel screen Points, for the double-click dedupe below
  let freehandPoints = []; // "Odręcznie": thinned [lng, lat] samples so far
  let lastFreehandScreenPoint = null; // screen Point of the last kept sample, for thinning
  let isMouseDown = false;
  let currentDrawAreaSqDeg = null; // envelope area of the in-progress polygon, or null before one exists
  let currentDrawValid = true;

  // The committed polygon, once selected -- a plain GeoJSON Polygon geometry
  // (the exact object POSTed to /package), plus the bits the UI needs to
  // describe it without recomputing them on every render.
  let committedPolygon = null;
  let committedEnvelope = null; // {minLng, minLat, maxLng, maxLat}
  let committedAreaSqDeg = 0;
  let committedVertexCount = 0;
  let committedValid = true;

  // Handlers currently attached for the active drawing mode, so teardownDrawMode
  // can remove exactly those (and only those) on every exit path.
  let activeModeHandlers = null; // { handlers: [[event, fn], ...] }

  const MIN_DRAG_PX = 3; // "Prostokąt": below this, treat mousedown+mouseup as a stray click, not a rectangle
  const CLOSE_DEDUPE_PX = 6; // "Punkty": see pointsDblClick's comment
  const FREEHAND_MIN_PX = 4; // "Odręcznie": see freehandMouseMove's comment

  function pointDistance(a, b) {
    return Math.hypot(a.x - b.x, a.y - b.y);
  }

  function envelopeOf(positions) {
    let minLng = Infinity;
    let minLat = Infinity;
    let maxLng = -Infinity;
    let maxLat = -Infinity;
    for (const [lng, lat] of positions) {
      minLng = Math.min(minLng, lng);
      maxLng = Math.max(maxLng, lng);
      minLat = Math.min(minLat, lat);
      maxLat = Math.max(maxLat, lat);
    }
    return { minLng, minLat, maxLng, maxLat };
  }

  function envelopeAreaSqDeg(env) {
    return (env.maxLng - env.minLng) * (env.maxLat - env.minLat);
  }

  function closeRing(positions) {
    return [...positions, positions[0]];
  }

  // Two opposite corners -> a 5-position closed ring. Always axis-aligned and
  // always valid (even a zero-size drag just yields a degenerate ring the
  // mouseup handler below discards before it's ever committed).
  function rectRing(a, b) {
    const minLng = Math.min(a[0], b[0]);
    const maxLng = Math.max(a[0], b[0]);
    const minLat = Math.min(a[1], b[1]);
    const maxLat = Math.max(a[1], b[1]);
    return [
      [minLng, minLat],
      [maxLng, minLat],
      [maxLng, maxLat],
      [minLng, maxLat],
      [minLng, minLat],
    ];
  }

  function polygonFeature(ring, phase, valid) {
    return {
      type: "Feature",
      properties: { kind: "polygon", phase, valid },
      geometry: { type: "Polygon", coordinates: [ring] },
    };
  }

  function vertexFeature(position) {
    return { type: "Feature", properties: { kind: "vertex" }, geometry: { type: "Point", coordinates: position } };
  }

  function rubberbandFeature(positions) {
    return {
      type: "Feature",
      properties: { kind: "rubberband" },
      geometry: { type: "LineString", coordinates: positions },
    };
  }

  function setDrawSourceData(features) {
    map.getSource("draw").setData({ type: "FeatureCollection", features });
  }

  function renderCommittedPolygon() {
    setDrawSourceData(
      committedPolygon ? [polygonFeature(committedPolygon.coordinates[0], "committed", committedValid)] : [],
    );
  }

  function resetInProgressGeometry() {
    rectStartLngLat = null;
    rectStartPoint = null;
    clickVertices = [];
    clickVertexPoints = [];
    freehandPoints = [];
    lastFreehandScreenPoint = null;
    isMouseDown = false;
    currentDrawAreaSqDeg = null;
    currentDrawValid = true;
  }

  // ---- rectangle mode ----

  function refreshRectanglePreview(corner) {
    if (!rectStartLngLat) {
      setDrawSourceData([]);
      currentDrawAreaSqDeg = null;
      currentDrawValid = true;
      updateDrawHint();
      return;
    }
    const ring = rectRing(rectStartLngLat, corner);
    const env = envelopeOf(ring);
    const area = envelopeAreaSqDeg(env);
    const valid = area <= MAX_AREA_SQ_DEG;
    setDrawSourceData([polygonFeature(ring, "drawing", valid)]);
    currentDrawAreaSqDeg = area;
    currentDrawValid = valid;
    updateDrawHint();
  }

  function rectMouseDown(e) {
    isMouseDown = true;
    rectStartLngLat = [e.lngLat.lng, e.lngLat.lat];
    rectStartPoint = e.point;
    refreshRectanglePreview(rectStartLngLat);
  }

  function rectMouseMove(e) {
    if (!isMouseDown || !rectStartLngLat) return;
    refreshRectanglePreview([e.lngLat.lng, e.lngLat.lat]);
  }

  function rectMouseUp(e) {
    if (!isMouseDown || !rectStartLngLat) return;
    isMouseDown = false;
    if (pointDistance(rectStartPoint, e.point) < MIN_DRAG_PX) {
      // A drag too small to be deliberate (or a stray click) -- discard and
      // stay in drawing mode rather than committing a sliver.
      rectStartLngLat = null;
      rectStartPoint = null;
      setDrawSourceData([]);
      currentDrawAreaSqDeg = null;
      updateDrawHint();
      return;
    }
    commitDrawnPolygon(rectRing(rectStartLngLat, [e.lngLat.lng, e.lngLat.lat]), 4);
  }

  // ---- points (click-polygon) mode ----

  function refreshPointsPreview(cursorLngLat) {
    const features = clickVertices.map(vertexFeature);
    if (clickVertices.length >= 1 && cursorLngLat) {
      const last = clickVertices[clickVertices.length - 1];
      features.push(rubberbandFeature([last, [cursorLngLat.lng, cursorLngLat.lat]]));
    }
    if (clickVertices.length >= 3) {
      const ring = closeRing(clickVertices);
      const env = envelopeOf(ring);
      const area = envelopeAreaSqDeg(env);
      const valid = area <= MAX_AREA_SQ_DEG;
      features.push(polygonFeature(ring, "drawing", valid));
      currentDrawAreaSqDeg = area;
      currentDrawValid = valid;
    } else {
      currentDrawAreaSqDeg = null;
      currentDrawValid = true;
    }
    setDrawSourceData(features);
    updateDrawHint();
  }

  function pointsClick(e) {
    clickVertices.push([e.lngLat.lng, e.lngLat.lat]);
    clickVertexPoints.push(e.point);
    refreshPointsPreview(e.lngLat);
  }

  function pointsMouseMove(e) {
    refreshPointsPreview(e.lngLat);
  }

  // MapLibre fires `click` twice (at essentially the same point) before the
  // `dblclick` that's meant to close the polygon, so pointsClick above has
  // already appended two near-duplicate vertices by the time this runs. Pop
  // trailing vertices while each is within CLOSE_DEDUPE_PX of the one before
  // it, so the closing gesture leaves the ring ending where the user actually
  // double-clicked instead of with a spurious near-duplicate point.
  function dedupeTrailingClickVertices() {
    while (clickVertexPoints.length >= 2) {
      const last = clickVertexPoints[clickVertexPoints.length - 1];
      const prev = clickVertexPoints[clickVertexPoints.length - 2];
      if (pointDistance(last, prev) >= CLOSE_DEDUPE_PX) break;
      clickVertexPoints.pop();
      clickVertices.pop();
    }
  }

  function tryClosePointsPolygon() {
    if (clickVertices.length < 3) return; // not enough vertices to form a polygon yet
    commitDrawnPolygon(closeRing(clickVertices), clickVertices.length);
  }

  function pointsDblClick(e) {
    e.preventDefault();
    dedupeTrailingClickVertices();
    tryClosePointsPolygon();
  }

  // ---- freehand mode ----

  function refreshFreehandPreview() {
    if (freehandPoints.length >= 3) {
      const ring = closeRing(freehandPoints);
      const env = envelopeOf(ring);
      const area = envelopeAreaSqDeg(env);
      const valid = area <= MAX_AREA_SQ_DEG;
      setDrawSourceData([polygonFeature(ring, "drawing", valid)]);
      currentDrawAreaSqDeg = area;
      currentDrawValid = valid;
    } else if (freehandPoints.length === 2) {
      // Not enough points for a polygon yet -- reuse the rubberband line
      // layer for the open in-progress path (see its comment near drawLayers).
      setDrawSourceData([rubberbandFeature(freehandPoints)]);
      currentDrawAreaSqDeg = null;
      currentDrawValid = true;
    } else {
      setDrawSourceData([]);
      currentDrawAreaSqDeg = null;
      currentDrawValid = true;
    }
    updateDrawHint();
  }

  function freehandMouseDown(e) {
    isMouseDown = true;
    freehandPoints = [[e.lngLat.lng, e.lngLat.lat]];
    lastFreehandScreenPoint = e.point;
    refreshFreehandPreview();
  }

  function freehandMouseMove(e) {
    if (!isMouseDown) return;
    // Thinning has to happen here, at collection time -- not as a post-pass
    // after mouseup -- because a single drag gesture fires one mousemove per
    // pixel of travel; without this a short gesture yields thousands of
    // vertices, most of them well under a pixel apart.
    if (lastFreehandScreenPoint && pointDistance(lastFreehandScreenPoint, e.point) < FREEHAND_MIN_PX) return;
    freehandPoints.push([e.lngLat.lng, e.lngLat.lat]);
    lastFreehandScreenPoint = e.point;
    refreshFreehandPreview();
  }

  function freehandMouseUp() {
    if (!isMouseDown) return;
    isMouseDown = false;
    if (freehandPoints.length < 3) {
      freehandPoints = [];
      lastFreehandScreenPoint = null;
      setDrawSourceData([]);
      currentDrawAreaSqDeg = null;
      updateDrawHint();
      return;
    }
    commitDrawnPolygon(closeRing(freehandPoints), freehandPoints.length);
  }

  // ---- mode setup/teardown ----

  function setupModeHandlers(mode) {
    const handlers = [];
    if (mode === "rectangle") {
      // Otherwise the drag pans the map instead of drawing a rectangle.
      map.dragPan.disable();
      handlers.push(["mousedown", rectMouseDown], ["mousemove", rectMouseMove], ["mouseup", rectMouseUp]);
    } else if (mode === "points") {
      // Otherwise the closing double-click also zooms the map.
      map.doubleClickZoom.disable();
      handlers.push(["click", pointsClick], ["dblclick", pointsDblClick], ["mousemove", pointsMouseMove]);
    } else if (mode === "freehand") {
      map.dragPan.disable();
      handlers.push(["mousedown", freehandMouseDown], ["mousemove", freehandMouseMove], ["mouseup", freehandMouseUp]);
    }
    for (const [evt, fn] of handlers) map.on(evt, fn);
    activeModeHandlers = handlers;
    document.addEventListener("keydown", onDrawKeyDown);
  }

  // Called on every exit from "drawing" -- commit, cancel, Escape, or a mode
  // switch mid-draw -- so no path can leave the map stuck with dragPan or
  // doubleClickZoom disabled, or a stale listener attached. Re-enabling both
  // unconditionally (rather than only the one the active mode disabled) is
  // deliberate: it's a no-op if already enabled, and it means this function
  // never has to know which mode was active to be correct.
  function teardownDrawMode() {
    if (activeModeHandlers) {
      for (const [evt, fn] of activeModeHandlers) map.off(evt, fn);
      activeModeHandlers = null;
    }
    document.removeEventListener("keydown", onDrawKeyDown);
    map.dragPan.enable();
    map.doubleClickZoom.enable();
    isMouseDown = false;
    // Undo enterDrawingMode's crosshair the same unconditional way as
    // dragPan/doubleClickZoom above -- every exit path (commit, cancel,
    // Escape, mode switch) runs through here, so restoring to "" rather than
    // whatever the CLICKABLE_LAYERS hover pair would have set is correct: if
    // the cursor is still over a clickable feature, that pair's own next
    // mouseenter/mousemove will put "pointer" back.
    map.getCanvas().style.cursor = "";
  }

  function onDrawKeyDown(e) {
    if (appDrawState !== "drawing") return;
    if (e.key === "Escape") {
      e.preventDefault();
      cancelDrawing();
    } else if (drawMode === "points" && e.key === "Enter") {
      e.preventDefault();
      tryClosePointsPolygon();
    } else if (drawMode === "points" && e.key === "Backspace") {
      e.preventDefault();
      if (clickVertices.length > 0) {
        clickVertices.pop();
        clickVertexPoints.pop();
        refreshPointsPreview(null);
      }
    }
  }

  function updateModeButtonsPressed() {
    for (const btn of drawModeButtons) {
      btn.setAttribute("aria-pressed", String(btn.dataset.drawMode === drawMode));
    }
  }

  // Mouse-only: rectangle and freehand both need drag, which touch has no
  // equivalent for in MapLibre's event model. "Punkty" works on touch too --
  // MapLibre's `click` still fires on tap -- but building an actual touch
  // gesture set (long-press, pinch-safe dragging, ...) for the other two
  // modes is out of scope here.
  function enterDrawingMode(mode) {
    teardownDrawMode(); // in case a different mode's handlers are still attached
    popup.remove(); // a popup left open from before drawing started would cover the area being drawn
    drawMode = mode;
    resetInProgressGeometry();
    appDrawState = "drawing";
    updateModeButtonsPressed();
    setupModeHandlers(mode);
    setDrawSourceData([]); // hide any previous committed polygon while drawing
    // "Click to place a vertex", not "click for info" -- see the comment on
    // the CLICKABLE_LAYERS mouseenter/mouseleave pair above for why those
    // don't fight this. teardownDrawMode restores it on every exit path.
    map.getCanvas().style.cursor = "crosshair";
    applyDownloadPanelState();
  }

  // Cancelling restores the previous committed polygon if one existed --
  // "Narysuj ponownie" doesn't discard the old selection until a new one is
  // actually committed, so backing out of a re-draw isn't destructive.
  function cancelDrawing() {
    teardownDrawMode();
    resetInProgressGeometry();
    appDrawState = committedPolygon ? "selected" : "idle";
    renderCommittedPolygon();
    applyDownloadPanelState();
  }

  function commitDrawnPolygon(ring, vertexCount) {
    teardownDrawMode();
    const env = envelopeOf(ring);
    const area = envelopeAreaSqDeg(env);
    committedPolygon = { type: "Polygon", coordinates: [ring] };
    committedEnvelope = env;
    committedAreaSqDeg = area;
    committedVertexCount = vertexCount;
    committedValid = area <= MAX_AREA_SQ_DEG;
    resetInProgressGeometry();
    appDrawState = "selected";
    renderCommittedPolygon();
    applyDownloadPanelState();
  }

  function clearSelection() {
    committedPolygon = null;
    committedEnvelope = null;
    committedAreaSqDeg = 0;
    committedVertexCount = 0;
    committedValid = true;
    appDrawState = "idle";
    renderCommittedPolygon();
    applyDownloadPanelState();
  }

  // ---- panel chrome ----

  function areaText(sqDeg) {
    return `${sqDeg.toFixed(4)} °²`;
  }

  function modeInstructions(mode) {
    switch (mode) {
      case "rectangle":
        return "Przeciągnij, aby narysować prostokąt.";
      case "points":
        return "Klikaj, aby dodawać punkty. Podwójne kliknięcie lub Enter zamyka obszar, Backspace usuwa ostatni punkt, Escape anuluje.";
      case "freehand":
        return "Przytrzymaj przycisk myszy i przeciągnij, aby narysować obszar odręcznie.";
      default:
        return "";
    }
  }

  function updateDrawHint() {
    if (appDrawState !== "drawing") return;
    let text = modeInstructions(drawMode);
    if (currentDrawAreaSqDeg !== null) {
      text += ` Powierzchnia obwiedni: ${areaText(currentDrawAreaSqDeg)} (limit ${areaText(MAX_AREA_SQ_DEG)}).`;
      if (!currentDrawValid) text += " Przekroczono limit powierzchni — zmniejsz obszar.";
    }
    drawHint.textContent = text;
  }

  function formatEnvelope(env) {
    const fmt = (n) => n.toFixed(4);
    return `${fmt(env.minLng)}, ${fmt(env.minLat)} — ${fmt(env.maxLng)}, ${fmt(env.maxLat)}`;
  }

  function selectedHintText() {
    let text = `Powierzchnia obwiedni: ${areaText(committedAreaSqDeg)} (limit ${areaText(MAX_AREA_SQ_DEG)}).`;
    if (!committedValid) text += " Przekroczono limit powierzchni — narysuj mniejszy obszar, aby pobrać.";
    return text;
  }

  // The single place that reconciles the whole download panel (both buttons,
  // the mode picker, Anuluj/Usuń zaznaczenie, both hints) against
  // {appDrawState, map zoom, downloadInFlight} -- see the state-machine
  // comment above appDrawState's declaration. Renamed from the original
  // applyDownloadZoomGating, which only ever toggled zoom-based disabling:
  // that gate is real but now applies to the viewport path alone --
  // drawing/selecting a polygon is legal at any zoom (a small polygon framed
  // at z10 has a perfectly legal envelope), so it would be wrong to keep
  // disabling the draw button below z12 the way the old function did.
  function applyDownloadPanelState() {
    const tooFarOut = map.getZoom() < MIN_DOWNLOAD_ZOOM;
    const idle = appDrawState === "idle";
    const drawing = appDrawState === "drawing";
    const selected = appDrawState === "selected";

    downloadBtn.hidden = drawing;
    if (idle) {
      downloadBtn.textContent = "Pobierz widoczny obszar";
      downloadBtn.disabled = tooFarOut || downloadInFlight;
    } else if (selected) {
      downloadBtn.textContent = "Pobierz zaznaczony obszar";
      // Gated on the polygon's own envelope, never on zoom -- see the
      // function comment above.
      downloadBtn.disabled = !committedValid || downloadInFlight;
    }
    // Only the idle/viewport path is zoom-gated -- see the function comment.
    downloadZoomHint.hidden = !(idle && tooFarOut);

    drawBtn.hidden = drawing;
    drawBtn.textContent = selected ? "Narysuj ponownie" : "Narysuj obszar";
    drawBtn.disabled = downloadInFlight;

    drawModePicker.hidden = !drawing;
    drawCancelBtn.hidden = !drawing;
    drawClearBtn.hidden = !selected;
    drawClearBtn.disabled = downloadInFlight;

    drawHint.hidden = idle;
    if (drawing) updateDrawHint();
    else if (selected) drawHint.textContent = selectedHintText();
  }

  map.on("zoom", applyDownloadPanelState);

  drawBtn.addEventListener("click", () => enterDrawingMode(drawMode));
  drawCancelBtn.addEventListener("click", cancelDrawing);
  drawClearBtn.addEventListener("click", clearSelection);
  for (const btn of drawModeButtons) {
    btn.addEventListener("click", () => enterDrawingMode(btn.dataset.drawMode));
  }

  applyDownloadPanelState();

  // Keys match the `datasets` values /package accepts (src/server/package.rs
  // parse_datasets) and what activeAggSources() above returns.
  const DATASET_LABELS = {
    bdot10k: "Budynki (BDOT10k)",
    egib: "Budynki (EGiB)",
    prg: "Adresy (PRG)",
  };

  function formatLayers(datasets) {
    if (datasets.length === 0) return "Brak wybranych warstw";
    return `${datasets.map((d) => DATASET_LABELS[d]).join(", ")} — tylko niedopasowane`;
  }

  function setFeedback(message, state) {
    downloadFeedbackText.textContent = message;
    if (state) {
      downloadFeedback.dataset.state = state;
    } else {
      delete downloadFeedback.dataset.state;
    }
    if (message) {
      if (!downloadFeedback.open) downloadFeedback.showModal();
    } else {
      downloadFeedback.close();
    }
  }

  downloadFeedbackClose.addEventListener("click", () => downloadFeedback.close());
  // Native <dialog> only light-dismisses via ::backdrop clicks if we handle it ourselves:
  // a click landing on the dialog element itself but outside its content box is a backdrop click.
  downloadFeedback.addEventListener("click", (e) => {
    if (e.target === downloadFeedback) downloadFeedback.close();
  });

  function filenameFromDisposition(header) {
    if (!header) return "package.geojson";
    const match = /filename="?([^"]+)"?/.exec(header);
    return match ? match[1] : "package.geojson";
  }

  function formatBbox(b) {
    return formatEnvelope({ minLng: b.getWest(), minLat: b.getSouth(), maxLng: b.getEast(), maxLat: b.getNorth() });
  }

  // Shared by both the GET-bbox (viewport) and POST-polygon (drawn area)
  // paths -- everything past "issue the request" (in-flight disabling, blob/
  // filename handling, the error-body text-then-JSON fallback, the feedback
  // modal's rows) is identical between them, so `request` is the only thing
  // that varies: a plain URL for GET, or {url, init} with a POST body for the
  // drawn-polygon path.
  async function runDownload(request, datasets, areaLabel) {
    downloadFeedbackBbox.textContent = areaLabel;
    downloadFeedbackLayers.textContent = formatLayers(datasets);

    if (datasets.length === 0) {
      setFeedback("Włącz co najmniej jedną warstwę (Budynki lub Adresy) w legendzie, aby pobrać paczkę.", "error");
      return;
    }

    downloadInFlight = true;
    applyDownloadPanelState();
    setFeedback("Pobieranie…", null);
    try {
      const res = await fetch(request.url, request.init);
      if (!res.ok) {
        // Read the body as text first -- res.json() consumes the same stream,
        // so a failed .json() parse would leave no way to fall back to raw text.
        let detail = null;
        try {
          const bodyText = (await res.text()).trim();
          try {
            detail = JSON.parse(bodyText)?.error || bodyText || null;
          } catch {
            detail = bodyText || null;
          }
        } catch {
          detail = null;
        }
        const statusPart = `${res.status}${res.statusText ? ` ${res.statusText}` : ""}`;
        setFeedback(detail ? `Błąd ${statusPart}: ${detail}` : `Żądanie nie powiodło się (${statusPart})`, "error");
        return;
      }
      // Read as text (not blob) so the feature count can be reported alongside
      // the download -- the same text becomes the downloaded file's content below,
      // so this adds no extra network cost.
      const text = await res.text();
      let featureCount = null;
      try {
        const geo = JSON.parse(text);
        if (Array.isArray(geo.features)) featureCount = geo.features.length;
      } catch {
        // Not parseable as JSON -- still downloadable, just no count to report.
      }
      const filename = filenameFromDisposition(res.headers.get("content-disposition"));
      const blob = new Blob([text], { type: res.headers.get("content-type") || "application/geo+json" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
      const sizeKb = (blob.size / 1024).toFixed(1);
      const countPart = featureCount !== null ? `${featureCount} obiektów, ` : "";
      setFeedback(`Pobrano ${filename} (${countPart}${sizeKb} KB)`, "ok");
    } catch (err) {
      setFeedback(`Błąd sieci: ${err.message || err}`, "error");
      console.error("package download failed", err);
    } finally {
      downloadInFlight = false;
      applyDownloadPanelState();
    }
  }

  downloadBtn.addEventListener("click", () => {
    // Download only whatever the legend currently has switched on, same set
    // activeAggSources() derives for the z5-13 aggregate layers -- otherwise
    // the package could silently include a registry the user just turned off.
    const datasets = activeAggSources();
    if (appDrawState === "selected" && committedPolygon) {
      runDownload(
        {
          url: `/package?datasets=${datasets.join(",")}`,
          init: {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(committedPolygon),
          },
        },
        datasets,
        `${formatEnvelope(committedEnvelope)} (narysowany, ${committedVertexCount} pkt.)`,
      );
    } else {
      const b = map.getBounds();
      const bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()].join(",");
      runDownload(
        { url: `/package?bbox=${encodeURIComponent(bbox)}&datasets=${datasets.join(",")}` },
        datasets,
        formatBbox(b),
      );
    }
  });
})();
