pub mod backend;
pub mod config;
pub mod error;
pub mod protocol;
pub mod proxy;

use backend::Backend;
use config::Config;
use error::ProxyError;
use proxy::handle_connection;
use std::sync::Arc;
use tokio::{net::TcpListener, sync::watch, task::JoinSet};

pub async fn run() -> Result<(), ProxyError> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "wakemc.toml".into());
    let config = Config::load(&path).await?;
    let listener = TcpListener::bind(&config.listen).await?;
    println!("wakemc listening on {}", config.listen);

    let backend = Arc::new(Backend::new(config.clone(), false));
    if backend.probe().await {
        backend.mark_online().await;
    }
    let (shutdown, shutdown_receiver) = watch::channel(false);
    let mut tasks = JoinSet::new();
    let monitor_backend = Arc::clone(&backend);
    tasks.spawn(async move { monitor_backend.monitor(shutdown_receiver).await });
    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, peer) = result?;
                let backend = Arc::clone(&backend);
                let shutdown = shutdown.subscribe();
                tasks.spawn(async move {
                    if let Err(error) = handle_connection(stream, backend, shutdown).await {
                        eprintln!("{peer}: {error}");
                    }
                });
            }
            result = &mut shutdown_signal => {
                result?;
                println!("shutdown signal received, closing connections");
                let _ = shutdown.send(true);
                break;
            }
        }
    }
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            eprintln!("proxy task failed during shutdown: {error}");
        }
    }
    Ok(())
}
