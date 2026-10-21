use axum::{routing::get, Router};

async fn hello() -> &'static str {
    "Hello from RustyRaft!"
}

async fn status() -> &'static str {
    "RustyRaft API is running"
}

pub fn create_router() -> Router::<()> {
    let app = 
        Router::new()
            .route("/hello", get(hello))
            .route("/status", get(status));
    app
}
