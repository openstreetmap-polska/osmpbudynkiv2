use axum::Json;
use axum::extract::State;
use serde::Serialize;

use crate::server::AppState;
use crate::server::jobs::JobStatus;

#[derive(Serialize)]
pub struct StatusResponse {
    pub jobs: Vec<JobStatus>,
}

pub async fn get_status(State(state): State<AppState>) -> Json<StatusResponse> {
    Json(StatusResponse {
        jobs: state.registry.snapshot(),
    })
}
