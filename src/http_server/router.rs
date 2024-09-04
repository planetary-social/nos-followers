use crate::metrics::setup_metrics;
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

    let metrics_handle = setup_metrics()?;
    Ok(Router::new()
        .route("/", get(serve_root_page))
        .layer(tracing_layer)
        .layer(TimeoutLayer::new(Duration::from_secs(1)))
        .route("/metrics", get(|| async move { metrics_handle.render() })))
}

async fn serve_root_page(_headers: HeaderMap) -> impl IntoResponse {
    // TODO: Some stats or useful info about the server here?
    let body = r#"
        <html>
            <head>
                <title>Nos Followers Server</title>
            </head>
            <body>
                <h1>Healthy</h1>
            </body>
        </html>
    "#;

    Html(body)
}
