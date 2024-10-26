use crate::router::handlers::{get_mmr_latest, get_mmr_proof};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{sync::Arc, time::Duration};

use anyhow::Result;
use axum::{routing::get, Router};

use tracing::info;

use tokio::{net::TcpListener, time::sleep};

mod handlers;

pub async fn initialize_router(should_terminate: Arc<AtomicBool>) -> Result<()> {
    let app = Router::new()
        .route("/", get(|| async { "Healthy" }))
        .route("/mmr", get(get_mmr_latest))
        .route("/mmr/:blocknumber", get(get_mmr_proof));

    let listener: TcpListener =
        TcpListener::bind(dotenvy::var("ROUTER_ENDPOINT").expect("ROUTER_ENDPOINT must be set"))
            .await?;

    info!("->> LISTENING on {}\n", listener.local_addr().unwrap());
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal(should_terminate.clone()))
        .await?;

    Ok(())
}

async fn shutdown_signal(should_terminate: Arc<AtomicBool>) {
    while !should_terminate.load(Ordering::SeqCst) {
        sleep(Duration::from_secs(10)).await;
    }
    info!("Shutdown signal received, shutting down router");
}
