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
  const addressAllColor = rootStyle.getPropertyValue("--address-all").trim();
  const addressUnmatchedColor = rootStyle.getPropertyValue("--address-unmatched").trim();
  const egibAccentColor = rootStyle.getPropertyValue("--egib-accent").trim();
  const paperRaisedColor = rootStyle.getPropertyValue("--paper-raised").trim();
  const inkColor = rootStyle.getPropertyValue("--ink").trim();
  // Sequential ramp for the z5-13 aggregate layers (low -> high density) plus
  // the single fixed hue used by the "Kółka" circle style -- see style.css's
  // comment on --ramp-1.._5/--agg-accent for why these are separate.
  const rampColors = [1, 2, 3, 4, 5].map((i) => rootStyle.getPropertyValue(`--ramp-${i}`).trim());
  const aggAccentColor = rootStyle.getPropertyValue("--agg-accent").trim();

  // agg_cells/agg_points carry integer count attributes, but what a given
  // count *means* changes sharply across z5-10: bz = min(z + 5, 14) caps at
  // z9, so each zoom step roughly quarters the bin's area (and so its typical
  // sum) until z9, where it flattens (z9 and z10 both bin at exactly one z14
  // cell -- identical distribution). A single static count->color/radius
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
  // are quantile-informed, not round log-decade numbers, pulled from real
  // n_total values in two representative tiles from the live database:
  //   z6  (bz=11, bin=8x8=64 z14 cells): p10=877 p25=1277 p50=1895 p75=3007 p90=4496 p99=8446 max=14281
  //   z9  (bz=14, bin=1 z14 cell):       p10=10  p25=20   p50=38   p75=81   p90=165  p99=664  max=1044
  // z10 shares z9's domain exactly (same bin definition, no approximation).
  // z5/z7/z8 clamp to their nearest anchor -- not exact, but those zooms
  // aren't screenshotted and clamping degrades gracefully rather than
  // extrapolating into nonsense.
  const COUNT_DOMAIN_Z6 = [50, 900, 2000, 4500, 9000];
  const COUNT_DOMAIN_Z9 = [1, 12, 35, 150, 500];

  /** Builds a zoom-and-property `interpolate` expression: at z6 use
   * `outputs` keyed to COUNT_DOMAIN_Z6's thresholds, at z9 the same
   * `outputs` keyed to COUNT_DOMAIN_Z9's thresholds, linearly blended
   * in between (and clamped outside). */
  function zoomPropertyRamp(attr, outputs) {
    const propertyExpr = (domain) => {
      const expr = ["interpolate", ["linear"], ["get", attr]];
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

  /** `fill-color`/`heatmap-color`-shaped ramp over one of the
   * n_total/n_bdot10k/n_egib/n_prg attributes -- rewritten via
   * setPaintProperty whenever the "Dane" selector changes. */
  function countColorExpr(attr) {
    return zoomPropertyRamp(attr, rampColors);
  }

  /** circle-radius over the selected attribute, sqrt-ish (each step less
   * than doubles the radius) so a handful of huge bins don't blow every
   * other circle down to a dot. Radius outputs stay fixed across zoom --
   * only the count thresholds that reach them shift -- so "biggest circle"
   * keeps a consistent visual meaning at any zoom within this tier. */
  function countRadiusExpr(attr) {
    return zoomPropertyRamp(attr, [3, 6, 11, 20, 34]);
  }

  /** heatmap-weight over the selected attribute, normalised so the top of
   * each zoom anchor's domain reaches full weight. */
  function heatmapWeightExpr(attr) {
    return zoomPropertyRamp(attr, [0.05, 0.2, 0.4, 0.7, 1]);
  }

  /** Label filter threshold: roughly the top 15-20% of bins at each zoom
   * anchor, so labels highlight the genuinely dense cells instead of
   * papering the whole tile in numbers. */
  function labelThresholdExpr() {
    return ["interpolate", ["linear"], ["zoom"], 6, COUNT_DOMAIN_Z6[3], 9, COUNT_DOMAIN_Z9[3]];
  }

  const sourceFilter = (source) => ["==", ["get", "source"], source];

  // Not per-source: color depends on match status, not registry, so one pair
  // of layers per status covers both registries — the source toggle just
  // swaps their filter instead of picking between two color sets. Every
  // building gets a red accent outline so registry footprints read clearly
  // against the basemap; fill opacity is what actually signals status (faint
  // grey wash for "all", solid red for "unmatched"). "all" is listed first so
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
      paint: { "line-color": buildingAccentColor, "line-width": 1, "line-opacity": 1 },
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

  // Tiers A (z5..10, aggregated bins) and B (z11..13, individual points) --
  // see src/server/tiles.rs. Both source-layers carry n_bdot10k/n_egib/
  // n_prg/n_total; Tier A emits agg_cells (polygon per bin) and agg_points
  // (point per bin) from the same aggregate, so "Styl" below can switch
  // between them with a pure visibility toggle and no backend change.
  const aggLayers = [
    {
      id: "agg-grid-fill",
      type: "fill",
      source: "unmatched",
      "source-layer": "agg_cells",
      minzoom: 5,
      maxzoom: 11,
      paint: {
        "fill-color": countColorExpr("n_total"),
        "fill-opacity": 0.75,
        "fill-outline-color": paperRaisedColor,
      },
    },
    {
      id: "agg-heatmap",
      type: "heatmap",
      source: "unmatched",
      "source-layer": "agg_points",
      minzoom: 5,
      maxzoom: 11,
      layout: { visibility: "none" },
      paint: {
        "heatmap-weight": heatmapWeightExpr("n_total"),
        // Increasing intensity/radius with zoom is the standard heatmap
        // recipe (e.g. Mapbox's earthquake example) for a *fixed* point
        // dataset, where the same points simply spread across more screen
        // pixels as you zoom in. That assumption doesn't hold here: bz caps
        // at 14, so points get *finer and more numerous* from z5 to z9 (a
        // z9 bin is one z14 cell; a z6 bin folds 64 of them together) while
        // staying at roughly the same on-screen spacing. The standard
        // increasing curve compounds with that growing point count instead
        // of compensating for it -- first tried with the standard
        // increasing shape, it saturated almost the entire z9 viewport
        // solid maroon. Decreasing intensity (and keeping radius nearly
        // flat) counters the higher point density at higher zoom instead.
        "heatmap-intensity": ["interpolate", ["linear"], ["zoom"], 5, 1.35, 9, 1.4, 10, 1.4],
        "heatmap-radius": ["interpolate", ["linear"], ["zoom"], 5, 16, 9, 16, 10, 17],
        "heatmap-opacity": 0.85,
        "heatmap-color": [
          "interpolate",
          ["linear"],
          ["heatmap-density"],
          0,
          "rgba(0, 0, 0, 0)",
          0.2,
          rampColors[0],
          0.4,
          rampColors[1],
          0.6,
          rampColors[2],
          0.8,
          rampColors[3],
          1,
          rampColors[4],
        ],
      },
    },
    {
      id: "agg-circles",
      type: "circle",
      source: "unmatched",
      "source-layer": "agg_points",
      minzoom: 5,
      maxzoom: 11,
      layout: {
        visibility: "none",
        // circle-sort-key is a *layout* property, not paint (unlike most of
        // circle's other properties) -- MapLibre rejects the whole layer at
        // style-load time if it's placed under paint instead, verified via
        // the browser console (`unknown property "circle-sort-key"`).
        // Bigger bins first (bottom), smaller bins last (top) so a huge
        // circle never fully hides a small neighbour.
        "circle-sort-key": ["*", -1, ["get", "n_total"]],
      },
      paint: {
        "circle-radius": countRadiusExpr("n_total"),
        "circle-color": aggAccentColor,
        "circle-opacity": 0.6,
        "circle-stroke-color": paperRaisedColor,
        "circle-stroke-width": 1,
      },
    },
    {
      id: "agg-circle-labels",
      type: "symbol",
      source: "unmatched",
      "source-layer": "agg_points",
      minzoom: 5,
      maxzoom: 11,
      layout: {
        visibility: "none",
        "text-field": ["get", "n_total"],
        "text-font": ["Noto Sans Regular"],
        "text-size": 11,
      },
      filter: [">", ["get", "n_total"], labelThresholdExpr()],
      paint: {
        "text-color": inkColor,
        "text-halo-color": paperRaisedColor,
        "text-halo-width": 1.4,
      },
    },
    {
      id: "points-dots",
      type: "circle",
      source: "unmatched",
      "source-layer": "points",
      minzoom: 11,
      maxzoom: 14,
      paint: {
        "circle-radius": ["interpolate", ["linear"], ["zoom"], 11, 1.4, 13, 3],
        "circle-color": [
          "match",
          ["get", "source"],
          "bdot10k",
          buildingAccentColor,
          "egib",
          egibAccentColor,
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
      // Only agg-circle-labels (a symbol layer) needs this, for its
      // text-field glyphs. If this endpoint is unreachable, MapLibre fails
      // to fetch that layer's glyph PBFs and simply doesn't render its text
      // -- it does not take down the rest of the style, verified in-browser.
      glyphs: "https://fonts.openmaptiles.org/{fontstack}/{range}.pbf",
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
    minZoom: 4,
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

  function describeFeature(layerId, props) {
    const status = layerId.includes("unmatched") ? "Niedopasowany" : "W rejestrze";
    if (layerId.startsWith("buildings-")) {
      return {
        title: "Budynek",
        rows: [
          ["Rejestr", props.source === "egib" ? "EGIB" : "BDOT10k"],
          ["Status", status],
          ["ID", props.id],
        ],
      };
    }
    return {
      title: "Adres",
      rows: [
        ["Status", status],
        ["Miejscowość", props.miejscowosc || "—"],
        ["Numer", props.numer_porzadkowy || "—"],
        ["ID PRG", props.lokalny_id],
      ],
    };
  }

  function popupHtml({ title, rows }) {
    const rowsHtml = rows
      .map(([label, value]) => `<dt>${escapeHtml(label)}</dt><dd>${escapeHtml(value)}</dd>`)
      .join("");
    return `<div class="feature-popup"><h3>${escapeHtml(title)}</h3><dl>${rowsHtml}</dl></div>`;
  }

  const popup = new maplibregl.Popup({ closeButton: true, closeOnClick: true, maxWidth: "260px" });

  map.on("click", CLICKABLE_LAYERS, (e) => {
    const feature = e.features[0];
    popup
      .setLngLat(e.lngLat)
      .setHTML(popupHtml(describeFeature(feature.layer.id, feature.properties)))
      .addTo(map);
  });
  map.on("mouseenter", CLICKABLE_LAYERS, () => {
    map.getCanvas().style.cursor = "pointer";
  });
  map.on("mouseleave", CLICKABLE_LAYERS, () => {
    map.getCanvas().style.cursor = "";
  });

  // ---- legend ----

  const legendBuildingsGroup = document.querySelector(".legend-group[data-building-source]");
  const sourceButtons = document.querySelectorAll(".source-btn");
  const buildingsAllCheckbox = document.getElementById("buildings-all-checkbox");
  const buildingsUnmatchedCheckbox = document.getElementById("buildings-unmatched-checkbox");
  const addressesAllCheckbox = document.getElementById("addresses-all-checkbox");
  const addressesUnmatchedCheckbox = document.getElementById("addresses-unmatched-checkbox");

  let buildingSource = legendBuildingsGroup.dataset.buildingSource;

  function setLayerVisible(layerId, visible) {
    map.setLayoutProperty(layerId, "visibility", visible ? "visible" : "none");
  }

  function applyBuildingVisibility() {
    map.setFilter("buildings-all-fill", sourceFilter(buildingSource));
    map.setFilter("buildings-all-outline", sourceFilter(buildingSource));
    map.setFilter("buildings-unmatched-fill", sourceFilter(buildingSource));
    map.setFilter("buildings-unmatched-outline", sourceFilter(buildingSource));
    setLayerVisible("buildings-all-fill", buildingsAllCheckbox.checked);
    setLayerVisible("buildings-all-outline", buildingsAllCheckbox.checked);
    setLayerVisible("buildings-unmatched-fill", buildingsUnmatchedCheckbox.checked);
    setLayerVisible("buildings-unmatched-outline", buildingsUnmatchedCheckbox.checked);
  }

  function applyAddressVisibility() {
    setLayerVisible("addresses-all-circle", addressesAllCheckbox.checked);
    setLayerVisible("addresses-unmatched-circle", addressesUnmatchedCheckbox.checked);
  }

  function wireLegend() {
    for (const btn of sourceButtons) {
      btn.addEventListener("click", () => {
        buildingSource = btn.dataset.source;
        legendBuildingsGroup.dataset.buildingSource = buildingSource;
        for (const b of sourceButtons) {
          b.setAttribute("aria-pressed", String(b === btn));
        }
        applyBuildingVisibility();
      });
    }
    buildingsAllCheckbox.addEventListener("change", applyBuildingVisibility);
    buildingsUnmatchedCheckbox.addEventListener("change", applyBuildingVisibility);
    addressesAllCheckbox.addEventListener("change", applyAddressVisibility);
    addressesUnmatchedCheckbox.addEventListener("change", applyAddressVisibility);
    // Layer visibility starts as "none" in the style spec above; derive the
    // real initial state from the checkboxes here instead of duplicating it.
    applyBuildingVisibility();
    applyAddressVisibility();
  }

  // ---- low-zoom aggregate controls (z5-13) ----

  // "Styl" picks which of the three z5-10 visualisations is visible;
  // agg-circle-labels rides along with "circles" since it's a label on top
  // of that same agg_points layer. points-dots (z11-13) has no style choice
  // -- it's always dots, only "Dane" affects it indirectly by not existing
  // (points-dots colours by source, not by count, since a source-colored
  // dot is already the finest-grained view -- see the report for why this
  // reads fine without a "Dane" hook of its own).
  const AGG_STYLE_LAYERS = {
    grid: ["agg-grid-fill"],
    circles: ["agg-circles", "agg-circle-labels"],
    heatmap: ["agg-heatmap"],
  };
  const aggStyleButtons = document.querySelectorAll("#agg-style-toggle .metric-btn");
  const aggMetricButtons = document.querySelectorAll("#agg-metric-toggle .metric-btn");

  let aggStyle = "grid";
  let aggMetric = "n_total";

  function applyAggStyleVisibility() {
    for (const [style, layerIds] of Object.entries(AGG_STYLE_LAYERS)) {
      for (const id of layerIds) {
        // agg-circle-labels is skipped here, not absent -- guarded in case a
        // future change drops it conditionally (e.g. an unreachable glyphs
        // endpoint); see the style construction above.
        if (!map.getLayer(id)) continue;
        setLayerVisible(id, style === aggStyle);
      }
    }
  }

  function applyAggMetric() {
    map.setPaintProperty("agg-grid-fill", "fill-color", countColorExpr(aggMetric));
    map.setPaintProperty("agg-circles", "circle-radius", countRadiusExpr(aggMetric));
    map.setLayoutProperty("agg-circles", "circle-sort-key", ["*", -1, ["get", aggMetric]]);
    map.setPaintProperty("agg-heatmap", "heatmap-weight", heatmapWeightExpr(aggMetric));
    if (map.getLayer("agg-circle-labels")) {
      map.setLayoutProperty("agg-circle-labels", "text-field", ["get", aggMetric]);
      map.setFilter("agg-circle-labels", [">", ["get", aggMetric], labelThresholdExpr()]);
    }
  }

  function wireAggLegend() {
    for (const btn of aggStyleButtons) {
      btn.addEventListener("click", () => {
        aggStyle = btn.dataset.style;
        for (const b of aggStyleButtons) {
          b.setAttribute("aria-pressed", String(b === btn));
        }
        applyAggStyleVisibility();
      });
    }
    for (const btn of aggMetricButtons) {
      btn.addEventListener("click", () => {
        aggMetric = btn.dataset.metric;
        for (const b of aggMetricButtons) {
          b.setAttribute("aria-pressed", String(b === btn));
        }
        applyAggMetric();
      });
    }
    // Same reasoning as wireLegend above: layer paint/layout starts from the
    // style spec's defaults (attribute "n_total", style "grid"); derive the
    // real initial state from the buttons' own aria-pressed default instead
    // of trusting the two to stay in sync by hand.
    applyAggStyleVisibility();
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
  const downloadFeedback = document.getElementById("download-feedback");

  function setFeedback(message, state) {
    downloadFeedback.textContent = message;
    if (state) {
      downloadFeedback.dataset.state = state;
    } else {
      delete downloadFeedback.dataset.state;
    }
  }

  function filenameFromDisposition(header) {
    if (!header) return "package.geojson";
    const match = /filename="?([^"]+)"?/.exec(header);
    return match ? match[1] : "package.geojson";
  }

  downloadBtn.addEventListener("click", async () => {
    const b = map.getBounds();
    const bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()].join(",");

    downloadBtn.disabled = true;
    setFeedback("Pobieranie…", null);
    try {
      const res = await fetch(`/package?bbox=${encodeURIComponent(bbox)}&datasets=all`);
      if (!res.ok) {
        const body = await res.json().catch(() => null);
        setFeedback(body?.error || `Żądanie nie powiodło się (${res.status})`, "error");
        return;
      }
      const blob = await res.blob();
      const filename = filenameFromDisposition(res.headers.get("content-disposition"));
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
      setFeedback(`Pobrano ${filename}`, "ok");
    } catch (err) {
      setFeedback("Błąd sieci — sprawdź konsolę", "error");
      console.error("package download failed", err);
    } finally {
      downloadBtn.disabled = false;
    }
  });
})();
