use anyhow::Result;
use axum::{http::HeaderMap, response::Html};
use axum::{response::IntoResponse, routing::get, Router};
use std::time::Duration;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tower_http::LatencyUnit;
use tower_http::{timeout::TimeoutLayer, trace::DefaultOnFailure};
use tracing::Level;

pub fn create_router() -> Result<Router> {
    let tracing_layer = TraceLayer::new_for_http()
        .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
        .on_response(
            DefaultOnResponse::new()
                .level(Level::INFO)
                .latency_unit(LatencyUnit::Millis),
        )
        .on_failure(DefaultOnFailure::new().level(Level::ERROR));

    Ok(Router::new()
        .route("/", get(serve_root_page))
        .layer(tracing_layer)
        .layer(TimeoutLayer::new(Duration::from_secs(1))))
}

async fn serve_root_page(_headers: HeaderMap) -> impl IntoResponse {
    let body = "
    <html>
        <head>
            <title>Nos</title>
        </head>
        <body>
            <h1>Healthy</h1>
        </body>
        ";

    Html(body)
}
