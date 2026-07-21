# Healthcheck Endpoint Design

## Summary

Add a `GET /health` liveness endpoint to the Axum HTTP server. It returns `200 OK` immediately, confirming the process is alive and the HTTP server is responding. No database check is performed.

## Architecture

The handler is an inline async closure registered directly in `src/server/mod.rs` alongside the existing tile route. No new module or file is needed.

```
GET /health → 200 OK (empty body)
```

## Implementation

In `src/server/mod.rs`, add one route to the existing `Router`:

```rust
.route("/health", axum::routing::get(|| async { StatusCode::OK }))
```

The `StatusCode` type is already imported via the `axum` dependency. The handler takes no state.

## Error Handling

None required — the handler has no fallible operations.

## Testing

Add an integration test in `tests/` that starts the server and asserts `GET /health` returns `200 OK`.
