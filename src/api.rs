use axum::{
    extract::State,
    http::StatusCode,
    routing::get, 
    Json, 
    Router,
};

use rustyraft::raft::state::ServerId;
use serde::Serialize;
use tokio::sync::watch;

#[derive(Clone)]
pub struct ApiState {
    leader_status_rx: watch::Receiver<Option<ServerId>>,
}

#[derive(Serialize)]
struct LeaderResponse {
    leader_id: Option<u64>,
    known: bool,
}

async fn leader(
    State(state): State<ApiState>
) -> Result<Json<LeaderResponse>, StatusCode> {
    let leader_id = *state.leader_status_rx.borrow();

    Ok(Json(LeaderResponse { 
        leader_id: leader_id.map(|id| id.value()),
        known: leader_id.is_some(),
     }))
}

pub fn create_router(
    leader_status_rx: watch::Receiver<Option<ServerId>>,
) -> Router {
    let state = ApiState {
        leader_status_rx,
    };

    Router::new()
        .route("/leader", get(leader))
        .with_state(state)
}