
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

async fn hello() -> &'static str {
    "Hello from RustyRaft!"
}

async fn status() -> &'static str {
    "RustyRaft API is running"
}

async fn leader(
    State(state): State<ApiState>,
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
        .route("/hello", get(hello))
        .route("/status", get(status))
        .route("/leader", get(leader))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn leader_endpoint_returns_unknown_when_no_leader_is_known() {
        let (_tx, rx) =
            watch::channel::<Option<ServerId>>(None);

        let app = create_router(rx);

        let request: Request<Body> = Request::builder()
            .uri("/leader")
            .body(Body::empty())
            .unwrap();

        let response = app
            .oneshot(request)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(
            response.into_body(),
            usize::MAX,
        )
        .await
        .unwrap();

        let json: serde_json::Value =
            serde_json::from_slice(&body).unwrap();

        assert_eq!(json["leader_id"], serde_json::Value::Null);
        assert_eq!(json["known"], false);
    }

    #[tokio::test]
    async fn leader_endpoint_returns_published_leader_id() {
        let (tx, rx) =
            watch::channel::<Option<ServerId>>(None);

        tx.send_replace(Some(ServerId::new(2)));

        let app = create_router(rx);

        let request: Request<Body> = Request::builder()
            .uri("/leader")
            .body(Body::empty())
            .unwrap();

        let response = app
            .oneshot(request)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(
            response.into_body(),
            usize::MAX,
        )
        .await
        .unwrap();

        let json: serde_json::Value =
            serde_json::from_slice(&body).unwrap();

        assert_eq!(json["leader_id"], 2);
        assert_eq!(json["known"], true);
    }
}
