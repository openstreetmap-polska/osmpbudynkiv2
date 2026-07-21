# Healthcheck Endpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `GET /health` to the Axum HTTP server that returns `200 OK` immediately, with no database interaction.

**Architecture:** One inline route handler (async closure) registered in `src/server/mod.rs`. The handler requires no state. A unit test verifies the handler using `tower::ServiceExt::oneshot` to exercise the router in-process.

**Tech Stack:** Rust, axum 0.8, tower (via axum), tokio

---

### Task 1: Add `tower` dev-dependency

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Add dev-dependency**

In `Cargo.toml`, under `[dev-dependencies]`, add:

```toml
tower = "0.5"
```

- [ ] **Step 2: Verify it compiles**

```bash
cargo build
```

Expected: success.

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "deps: add tower as dev-dependency for server tests"
```

---

### Task 2: Implement `GET /health`

**Files:**
- Modify: `src/server/mod.rs`

- [ ] **Step 1: Add the route to the router**

In `src/server/mod.rs`, find the `Router::new()` block inside `pub async fn run(...)` and add the health route first:

```rust
let app = Router::new()
    .route("/health", axum::routing::get(|| async { StatusCode::OK }))
    .route("/tiles/{z}/{x}/{y}", axum::routing::get(tiles::serve_tile))
    .with_state(state);
```

Also extend the existing `axum` import at the top of the file to include `StatusCode`:

```rust
use axum::{Router, http::StatusCode};
```

- [ ] **Step 2: Verify it compiles**

```bash
cargo build
```

Expected: success.

---

### Task 3: Test `GET /health`

**Files:**
- Modify: `src/server/mod.rs`

The test builds a minimal router with only the health route — no `AppState` needed since the handler is stateless.

- [ ] **Step 1: Add a unit test**

At the bottom of `src/server/mod.rs`, add:

```rust
#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_returns_200() {
        let app = Router::new()
            .route("/health", axum::routing::get(|| async { StatusCode::OK }));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
```

- [ ] **Step 2: Run the test**

```bash
cargo test health_returns_200
```

Expected: PASS.

- [ ] **Step 3: Run the full test suite**

```bash
cargo test
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/server/mod.rs
git commit -m "feat: add GET /health liveness endpoint"
```
