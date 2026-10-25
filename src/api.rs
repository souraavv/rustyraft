use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json,
    Router,
};

use rustyraft::raft::client::{
    ClientCommandError,
    ClientHandle,
    CommandStatus,
};
use rustyraft::raft::state::ServerId;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

#[derive(Clone)]
pub struct ApiState {
    leader_status_rx: watch::Receiver<Option<ServerId>>,
    client_handle: ClientHandle<String>,
}

#[derive(Serialize)]
struct LeaderResponse {
    leader_id: Option<u64>,
    known: bool,
}

#[derive(Debug, Deserialize)]
struct SubmitCommandRequest {
    request_id: String,
    command: String,
}

#[derive(Serialize)]
struct ApiError {
    error: &'static str,
    known_leader: Option<u64>,
}

async fn leader(
    State(state): State<ApiState>,
) -> Json<LeaderResponse> {
    let leader_id = *state.leader_status_rx.borrow();

    Json(LeaderResponse {
        leader_id: leader_id.map(|id| id.value()),
        known: leader_id.is_some(),
    })
}

/// Submits a command through the Raft runtime.
async fn submit_command(
    State(state): State<ApiState>,
    Json(request): Json<SubmitCommandRequest>,
) -> Response {
    if request.request_id.trim().is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_id",
            None,
        );
    }

    tracing::debug!(
        request_id = %request.request_id,
        "Received client command request"
    );

    match state
        .client_handle
        .submit(
            request.request_id,
            request.command,
        )
        .await
    {
        Ok(snapshot) => {
            let status_code = match snapshot.status {
                CommandStatus::Pending => {
                    StatusCode::ACCEPTED
                }
                CommandStatus::Committed => {
                    StatusCode::OK
                }
                CommandStatus::Aborted => {
                    StatusCode::CONFLICT
                }
            };

            (status_code, Json(snapshot)).into_response()
        }

        Err(error) => client_error_response(error),
    }
}

/// Returns the local status of a previously submitted command.
async fn command_status(
    State(state): State<ApiState>,
    Path(request_id): Path<String>,
) -> Response {
    match state
        .client_handle
        .get_status(request_id)
        .await
    {
        Ok(snapshot) => {
            (StatusCode::OK, Json(snapshot)).into_response()
        }

        Err(error) => client_error_response(error),
    }
}

fn client_error_response(
    error: ClientCommandError,
) -> Response {
    match error {
        ClientCommandError::NotLeader {
            known_leader,
        } => {
            error_response(
                StatusCode::CONFLICT,
                "not_leader",
                known_leader.map(|id| id.value()),
            )
        }

        ClientCommandError::RequestIdConflict => {
            error_response(
                StatusCode::CONFLICT,
                "request_id_conflict",
                None,
            )
        }

        ClientCommandError::RequestNotFound => {
            error_response(
                StatusCode::NOT_FOUND,
                "request_not_found",
                None,
            )
        }

        ClientCommandError::StorageFailure => {
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage_failure",
                None,
            )
        }

        ClientCommandError::Unavailable => {
            error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "runtime_unavailable",
                None,
            )
        }
    }
}

fn error_response(
    status_code: StatusCode,
    error: &'static str,
    known_leader: Option<u64>,
) -> Response {
    (
        status_code,
        Json(ApiError {
            error,
            known_leader,
        }),
    )
        .into_response()
}

pub fn create_router(
    leader_status_rx: watch::Receiver<Option<ServerId>>,
    client_handle: ClientHandle<String>,
) -> Router {
    let state = ApiState {
        leader_status_rx,
        client_handle,
    };

    Router::new()
        .route("/leader", get(leader))
        .route("/commands", post(submit_command))
        .route(
            "/commands/{request_id}",
            get(command_status),
        )
        .with_state(state)
}


// --- unit test case ---
#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{
        body::{to_bytes, Body},
        http::Request,
        response::Response,
    };

    use rustyraft::raft::client::{ClientRequest, CommandStatusSnapshot};
    use tower::ServiceExt;

    fn snapshot(
        status: CommandStatus,
    ) -> CommandStatusSnapshot {
        CommandStatusSnapshot {
            request_id: "request-1".to_string(),
            log_index: 1,
            status,
        }
    }

    fn test_router(
        result: Result<
            CommandStatusSnapshot,
            ClientCommandError,
        >,
        request_count: Option<Arc<AtomicUsize>>,
    ) -> Router {
        let (client, mut receiver) =
            ClientHandle::<String>::channel(8);

        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {
                if let Some(counter) = &request_count {
                    counter.fetch_add(1, Ordering::SeqCst);
                }

                let reply_to = match request {
                    ClientRequest::Submit {
                        reply_to,
                        ..
                    } => reply_to,

                    ClientRequest::GetStatus {
                        reply_to,
                        ..
                    } => reply_to,
                };

                let _ = reply_to.send(result.clone());
            }
        });

        let (_leader_tx, leader_status_rx) =
            watch::channel::<Option<ServerId>>(None);

        create_router(leader_status_rx, client)
    }

    fn post_command(body: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/commands")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("request should build")
    }

    async fn json_body(
        response: Response,
    ) -> serde_json::Value {
        let body = to_bytes(
            response.into_body(),
            usize::MAX,
        )
        .await
        .expect("body should be readable");

        serde_json::from_slice(&body)
            .expect("body should contain JSON")
    }

    #[tokio::test]
    async fn pending_command_returns_202() {
        let app = test_router(
            Ok(snapshot(CommandStatus::Pending)),
            None,
        );

        let response = app
            .oneshot(post_command(
                r#"{"request_id":"request-1","command":"SET x=1"}"#,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let body = json_body(response).await;
        assert_eq!(body["request_id"], "request-1");
        assert_eq!(body["log_index"], 1);
        assert_eq!(body["status"], "pending");
    }

    #[tokio::test]
    async fn committed_command_returns_200() {
        let app = test_router(
            Ok(snapshot(CommandStatus::Committed)),
            None,
        );

        let response = app
            .oneshot(post_command(
                r#"{"request_id":"request-1","command":"SET x=1"}"#,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = json_body(response).await;
        assert_eq!(body["status"], "committed");
    }

    #[tokio::test]
    async fn not_leader_response_includes_known_leader() {
        let app = test_router(
            Err(ClientCommandError::NotLeader {
                known_leader: Some(ServerId::new(2)),
            }),
            None,
        );

        let response = app
            .oneshot(post_command(
                r#"{"request_id":"request-1","command":"SET x=1"}"#,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);

        let body = json_body(response).await;
        assert_eq!(body["error"], "not_leader");
        assert_eq!(body["known_leader"], 2);
    }

    #[tokio::test]
    async fn empty_request_id_is_rejected_before_dispatch() {
        let count = Arc::new(AtomicUsize::new(0));

        let app = test_router(
            Ok(snapshot(CommandStatus::Pending)),
            Some(count.clone()),
        );

        let response = app
            .oneshot(post_command(
                r#"{"request_id":"   ","command":"SET x=1"}"#,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn malformed_json_is_rejected() {
        let app = test_router(
            Ok(snapshot(CommandStatus::Pending)),
            None,
        );

        let response = app
            .oneshot(post_command("{"))
            .await
            .unwrap();

        assert!(response.status().is_client_error());
    }

    #[tokio::test]
    async fn missing_command_field_is_rejected() {
        let app = test_router(
            Ok(snapshot(CommandStatus::Pending)),
            None,
        );

        let response = app
            .oneshot(post_command(
                r#"{"request_id":"request-1"}"#,
            ))
            .await
            .unwrap();

        assert!(response.status().is_client_error());
    }

    #[tokio::test]
    async fn unknown_request_status_returns_404() {
        let app = test_router(
            Err(ClientCommandError::RequestNotFound),
            None,
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/commands/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn client_errors_map_to_expected_http_statuses() {
        let cases = [
            (
                ClientCommandError::RequestIdConflict,
                StatusCode::CONFLICT,
            ),
            (
                ClientCommandError::StorageFailure,
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            (
                ClientCommandError::Unavailable,
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ];

        for (error, expected_status) in cases {
            let app = test_router(Err(error), None);

            let response = app
                .oneshot(post_command(
                    r#"{"request_id":"request-1","command":"SET x=1"}"#,
                ))
                .await
                .unwrap();

            assert_eq!(response.status(), expected_status);
        }
    }
}