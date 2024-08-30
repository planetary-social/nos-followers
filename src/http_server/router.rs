use anyhow::Result;
use axum::{http::HeaderMap, response::Html};
use axum::{response::IntoResponse, routing::get, Router};
use metrics::describe_counter;
use metrics_exporter_prometheus::PrometheusBuilder;
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

fn setup_metrics() -> Result<metrics_exporter_prometheus::PrometheusHandle, anyhow::Error> {
    describe_counter!(
        "pubsub_messages",
        "Number of messages published to google pubsub"
    );
    describe_counter!(
        "contact_lists_processed",
        "Number of contact lists processed"
    );
    describe_counter!("follows", "Number of follows");
    describe_counter!("unfollows", "Number of unfollows");
    describe_counter!(
        "worker_lagged",
        "Number of times a worker lagged behind and missed messaged, consider increasing worker pool size or channel buffer size"
    );
    describe_counter!("worker_closed", "Number of times a worker channel closed");
    describe_counter!(
        "worker_failures",
        "Number of times a worker failed to process an item"
    );
    describe_counter!("worker_timeouts", "Number of times a worker timedout");
    describe_counter!("verified_nip05", "Number of verified NIP05 ids fetched");

    let prometheus_builder = PrometheusBuilder::new();
    let prometheus_handle = prometheus_builder.install_recorder()?;
    Ok(prometheus_handle)
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
