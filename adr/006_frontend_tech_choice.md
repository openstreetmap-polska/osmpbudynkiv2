# Decision
The web map frontend (browsing data status, viewing unmatched buildings/addresses, downloading packages) will use MapLibre GL JS, plain JavaScript with no framework, and be served from a directory on disk (`Config::web_dir`, `tower_http::services::ServeDir` mounted as an axum fallback route) rather than embedded into the binary at compile time.

# Rationale
`/tiles/{z}/{x}/{y}` already emits vector tiles (`ST_AsMVTGeom`), so the map library needs to be one that consumes MVT natively. MapLibre GL JS does this directly.

The page itself is a single map view plus a couple of JSON-backed panels (`/status`, and later `/updates`) updated via `fetch`, not a form-heavy or multi-page app. That fits plain JavaScript driving the DOM directly; htmx's model of swapping server-rendered HTML fragments would require adding an HTML templating layer to the Rust server for a page that doesn't otherwise need one. No build step is used either, since there's no framework/JSX to compile — the files under `web/` are served as-is.

For deployment, the alternative considered was embedding the static assets into the binary at compile time (`rust-embed`/`include_dir!`), matching the project's overall goal of an easy-to-deploy single binary (ADR-002). That was turned down: a deployment already ships the binary, a config TOML, and possibly in the future a unit file as separate files, so one more directory doesn't meaningfully add deployment complexity, and GitHub Actions can package all of them into one release archive regardless. In exchange, the frontend can be updated (or, e.g., hot-edited locally) without rebuilding the Rust binary, and a missing/misconfigured `web_dir` just 404s API-adjacent routes rather than failing the whole build.
