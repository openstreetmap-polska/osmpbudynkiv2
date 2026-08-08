// v6 dropped the UMD bundle (`maplibre-gl.js`) that used to expose a
// `maplibregl` global -- only the ESM build (`maplibre-gl.mjs`) is published
// now, so this file has to be a module and import it itself instead of index.html
// loading a separate global-exposing <script> before this one.
import * as maplibregl from "https://unpkg.com/maplibre-gl@6/dist/maplibre-gl.mjs";

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
  // Sequential ramp for the z5-11 aggregate grid (low -> high density), used
  // by the "Liczba" colour mode.
  const rampColors = [1, 2, 3, 4, 5].map((i) => rootStyle.getPropertyValue(`--ramp-${i}`).trim());
  const ratioColors = [0, 1, 2, 3, 4].map((i) => rootStyle.getPropertyValue(`--ratio-${i}`).trim());
  const ratioUnknownColor = rootStyle.getPropertyValue("--ratio-unknown").trim();

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

  const map = new maplibregl.Map({
    container: "map",
    style: {
      version: 8,
      // No `glyphs` endpoint: the style has no symbol layer since the circle
      // style (and its per-bin percentage labels) was dropped. Adding any
      // text-field layer back means restoring it -- MapLibre can't fetch
      // glyph PBFs without one.
      sources: {
        osm: {
          type: "raster",
          tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
          tileSize: 256,
          attribution: "&copy; OpenStreetMap contributors",
        },
        unmatched: {
          type: "vector",
          tiles: [TILE_URL],
          minzoom: 5,
          maxzoom: 14,
        },
      },
      layers: [
        { id: "osm", type: "raster", source: "osm" },
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
        ...aggLayers,
      ],
    },
    center: [19.4, 52.0],
    zoom: 6,
    minZoom: 5,
    maxZoom: 19,
    hash: "map"
  });
  map.addControl(new maplibregl.NavigationControl(), "top-right");

  // ---- feature popups ----

  const CLICKABLE_LAYERS = [
    "buildings-all-fill",
    "buildings-unmatched-fill",
    "addresses-all-circle",
    "addresses-unmatched-circle",
  ];

  function escapeHtml(value) {
    return String(value).replace(
      /[&<>"']/g,
      (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]
    );
  }

  // Popup labels are derived from the tile attribute's *own* name -- re-cased
  // and spaced, never swapped for a different word -- so what the popup calls
  // a field is what the government dataset calls it, and a value can be traced
  // back to its column without a glossary.
  //
  // Two shapes need help. snake_case (`numer_porzadkowy`,
  // `levels_above_ground`) splits on "_" generically and needs no table.
  // BDOT10k's glued uppercase column names carry no separator at all, so where
  // the word boundaries fall is knowledge only a table has; the entries below
  // therefore only insert spaces and lower letters. Acronyms (FSBUD, KST) stay
  // upper, since lowering them would be changing the word.
  //
  // Anything not listed falls through to the generic transform, so a new tile
  // attribute appears in the popup by itself under its raw name rather than
  // silently going missing -- which is the failure this replaced (the popup
  // used to render a hardcoded 3-4 rows out of the 13-14 attributes the tile
  // carries; see src/server/tiles.rs's MVT selects for the full set).
  const ATTRIBUTE_LABELS = {
    id: "ID",
    lokalny_id: "Lokalny ID",
    NAZWA: "Nazwa",
    KATEGORIAISTNIENIA: "Kategoria istnienia",
    INFORMACJADODATKOWA: "Informacja dodatkowa",
    KODKST: "Kod KST",
    ZRODLODANYCHGEOMETRYCZNYCH: "Zrodlo danych geometrycznych",
    PRZEWAZAJACAFUNKCJABUDYNKU: "Przewazajaca funkcja budynku",
    FUNKCJAOGOLNABUDYNKU: "Funkcja ogolna budynku",
  };

  function attributeLabel(key) {
    if (key in ATTRIBUTE_LABELS) return ATTRIBUTE_LABELS[key];
    // addr:street, addr:city:simc, source:addr -- literal OSM tag keys, not
    // column names. Re-casing one would stop it naming the tag it names, so
    // anything with a colon is shown exactly as the tile spells it.
    if (key.includes(":")) return key;
    const spaced = key.replace(/_/g, " ");
    return spaced.charAt(0).toUpperCase() + spaced.slice(1);
  }

  function formatValue(key, value) {
    if (value === null || value === undefined || value === "") return "—";
    // `tags` is the resolved k=v;k=v string the building-type mapping
    // produced -- the tags /package would export for this object. One per
    // line beats a run-on cell; .feature-popup dd is `white-space: pre-line`.
    if (key === "tags") return String(value).split(";").join("\n");
    return String(value);
  }

  function describeFeature(layerId, props) {
    return {
      title: layerId.startsWith("buildings-") ? "Budynek" : "Adres",
      // Not an attribute -- it comes from which layer was clicked, not from
      // the data -- so it sits in the header rather than among the rows.
      status: layerId.includes("unmatched") ? "Niedopasowany" : "W rejestrze",
      // Insertion order is the tile's attribute order, which follows the
      // SELECT in src/server/tiles.rs (identity, then classification, then
      // the resolved tags) -- more useful than sorting alphabetically.
      // ST_AsMVT drops NULL attributes, so this is exactly the set that
      // feature actually has: an EGIB building carries `rodzaj`, a BDOT10k
      // one carries FSBUD/KODKST/..., and neither shows the other's blanks.
      rows: Object.keys(props).map((key) => [attributeLabel(key), formatValue(key, props[key])]),
    };
  }

  function popupHtml({ title, status, rows }) {
    const rowsHtml = rows
      .map(([label, value]) => `<dt>${escapeHtml(label)}</dt><dd>${escapeHtml(value)}</dd>`)
      .join("");
    return (
      `<div class="feature-popup"><h3>${escapeHtml(title)}` +
      `<span class="feature-status">${escapeHtml(status)}</span></h3>` +
      `<dl>${rowsHtml}</dl></div>`
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

  map.on("click", CLICKABLE_LAYERS, (e) => {
    const feature = e.features[0];
    popup
      .setLngLat(e.lngLat)
      .setHTML(popupHtml(describeFeature(feature.layer.id, feature.properties)))
      .addTo(map);
  });
  map.on("click", (e) => {
    if (!map.queryRenderedFeatures(e.point, { layers: CLICKABLE_LAYERS }).length) {
      popup.remove();
    }
  });
  map.on("mouseenter", CLICKABLE_LAYERS, () => {
    map.getCanvas().style.cursor = "pointer";
  });
  map.on("mouseleave", CLICKABLE_LAYERS, () => {
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
  const buildingSourceButtons = legendBuildingsGroup.querySelectorAll(".source-btn");
  const addressSourceButtons = legendAddressesGroup.querySelectorAll(".source-btn");

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
    // Layer visibility starts as "none" in the style spec above; derive the
    // real initial state from the buttons' own aria-pressed default here
    // instead of duplicating it.
    applyBuildingVisibility();
    applyAddressVisibility();
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
  } else {
    map.once("load", () => {
      wireLegend();
      wireAggLegend();
    });
  }

  // ---- status panel ----

  const statusToggle = document.getElementById("status-toggle");
  const statusBody = document.getElementById("status-body");
  const statusDot = document.getElementById("status-dot");
  const statusTableBody = document.querySelector("#status-table tbody");
  const stalenessHint = document.getElementById("staleness-hint");

  const JOB_STATE_LABELS = {
    idle: "bezczynne",
    running: "w trakcie",
    disabled: "wyłączone",
  };

  statusToggle.addEventListener("click", () => {
    const expanded = statusToggle.getAttribute("aria-expanded") === "true";
    statusToggle.setAttribute("aria-expanded", String(!expanded));
    statusBody.hidden = expanded;
  });

  function fmtTimestamp(iso) {
    if (!iso) return "—";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    return d.toISOString().replace("T", " ").slice(0, 19) + "Z";
  }

  function renderStatus(data) {
    const jobs = data.jobs || [];
    const anyError = jobs.some((j) => j.last_outcome && j.last_outcome.kind === "Error");
    const anyRunning = jobs.some((j) => j.state === "running");
    statusDot.dataset.state = anyError ? "error" : anyRunning ? "running" : "idle";

    statusTableBody.innerHTML = "";
    for (const job of jobs) {
      const row = document.createElement("tr");
      const stateLabel = JOB_STATE_LABELS[job.state] || job.state;
      const stateCell = document.createElement("td");
      stateCell.innerHTML = `<span class="job-state"><span class="status-dot" data-state="${
        job.state === "running" ? "running" : job.enabled ? "idle" : ""
      }"></span>${stateLabel}</span>`;
      row.innerHTML = `<td>${job.name}</td>`;
      row.appendChild(stateCell);
      const lastRun = document.createElement("td");
      lastRun.textContent = fmtTimestamp(job.last_finished_at);
      row.appendChild(lastRun);
      const nextRun = document.createElement("td");
      nextRun.textContent = fmtTimestamp(job.next_run_at);
      row.appendChild(nextRun);
      statusTableBody.appendChild(row);
    }

    const staleness = data.match_staleness;
    if (staleness && staleness.pending_total > 0) {
      stalenessHint.textContent = `${staleness.pending_total} komórek oczekuje na odświeżenie dopasowań.`;
    } else {
      stalenessHint.textContent = "Tabele danych są aktualne.";
    }
  }

  async function pollStatus() {
    try {
      const res = await fetch("/status");
      if (!res.ok) throw new Error(`status ${res.status}`);
      renderStatus(await res.json());
    } catch (err) {
      statusDot.dataset.state = "error";
      console.error("failed to load /status", err);
    }
  }

  pollStatus();
  setInterval(pollStatus, 30000);

  // ---- package download ----

  const downloadBtn = document.getElementById("download-btn");
  const drawBtn = document.getElementById("draw-btn");
  const downloadZoomHint = document.getElementById("download-zoom-hint");
  const downloadFeedback = document.getElementById("download-feedback");
  const downloadFeedbackBbox = document.getElementById("download-feedback-bbox");
  const downloadFeedbackLayers = document.getElementById("download-feedback-layers");
  const downloadFeedbackText = document.getElementById("download-feedback-text");
  const downloadFeedbackClose = document.getElementById("download-feedback-close");

  // /package rejects any request whose bbox exceeds config.package.max_area_sq_deg
  // (0.04 by default -- see check_area in src/server/package.rs). At z0-11 the
  // visible viewport is already far larger than that, so "Pobierz widoczny
  // obszar" would just 400. Gate both download buttons on zoom instead of
  // letting that request go out and fail.
  const MIN_DOWNLOAD_ZOOM = 12;
  let downloadInFlight = false;

  function applyDownloadZoomGating() {
    const tooFarOut = map.getZoom() < MIN_DOWNLOAD_ZOOM;
    downloadBtn.disabled = tooFarOut || downloadInFlight;
    drawBtn.disabled = tooFarOut;
    downloadZoomHint.hidden = !tooFarOut;
  }

  map.on("zoom", applyDownloadZoomGating);
  applyDownloadZoomGating();

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
    const fmt = (n) => n.toFixed(4);
    return `${fmt(b.getWest())}, ${fmt(b.getSouth())} — ${fmt(b.getEast())}, ${fmt(b.getNorth())}`;
  }

  downloadBtn.addEventListener("click", async () => {
    const b = map.getBounds();
    const bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()].join(",");
    // Download only whatever the legend currently has switched on, same set
    // activeAggSources() derives for the z5-13 aggregate layers -- otherwise
    // the package could silently include a registry the user just turned off.
    const datasets = activeAggSources();

    downloadFeedbackBbox.textContent = formatBbox(b);
    downloadFeedbackLayers.textContent = formatLayers(datasets);

    if (datasets.length === 0) {
      setFeedback("Włącz co najmniej jedną warstwę (Budynki lub Adresy) w legendzie, aby pobrać paczkę.", "error");
      return;
    }

    downloadInFlight = true;
    applyDownloadZoomGating();
    setFeedback("Pobieranie…", null);
    try {
      const res = await fetch(`/package?bbox=${encodeURIComponent(bbox)}&datasets=${datasets.join(",")}`);
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
      applyDownloadZoomGating();
    }
  });
})();
