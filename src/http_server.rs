mod router;
use anyhow::{Context, Result};
use axum::Router;
use router::create_router;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::timeout;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info};

pub struct HttpServer;
impl HttpServer {
    pub fn start(
        task_tracker: TaskTracker,
        http_port: u16,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        let router = create_router()?;
        start_http_server(task_tracker, http_port, router, cancellation_token);

        Ok(())
    }
}

fn start_http_server(
    task_tracker: TaskTracker,
    http_port: u16,
    router: Router,
    cancellation_token: CancellationToken,
) {
    task_tracker.spawn(async move {
        let addr = SocketAddr::from(([0, 0, 0, 0], http_port));
        let Ok(listener) = tokio::net::TcpListener::bind(addr).await else {
            error!("Failed to bind to address: {}", addr);
            cancellation_token.cancel();
            return;
        };
        let token_clone = cancellation_token.clone();
        let server_future = tokio::spawn(async {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown_hook(token_clone))
                .await
                .context("Failed to start HTTP server")
        });

        await_shutdown(cancellation_token, server_future).await;
    });
}

async fn await_shutdown(
    cancellation_token: CancellationToken,
    server_future: tokio::task::JoinHandle<Result<()>>,
) {
    cancellation_token.cancelled().await;
    info!("Shutdown signal received.");
    match timeout(Duration::from_secs(5), server_future).await {
        Ok(_) => info!("HTTP service exited successfully."),
        Err(e) => info!("HTTP service exited after timeout: {}", e),
    }
}

async fn shutdown_hook(cancellation_token: CancellationToken) {
    cancellation_token.cancelled().await;
    info!("Exiting the process");
}
