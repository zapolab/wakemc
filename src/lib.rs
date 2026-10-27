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
use tokio::{
    net::TcpListener,
    sync::watch,
    task::JoinSet,
    time::{Duration, timeout},
};

pub async fn run() -> Result<(), ProxyError> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "wakemc.toml".into());
    let config = Config::load(&path).await?;
    let listener = TcpListener::bind(&config.listen).await?;
    println!("wakemc listening on {}", config.listen);

    let backend = Arc::new(Backend::new(config));
    let (shutdown, _) = watch::channel(false);
    let mut tasks = JoinSet::new();
    let shutdown_signal = wait_for_shutdown();
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
    let join_result = timeout(Duration::from_secs(5), async {
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result {
                eprintln!("proxy task failed during shutdown: {error}");
            }
        }
    })
    .await;
    if join_result.is_err() {
        eprintln!("shutdown timed out, aborting remaining proxy tasks");
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    Ok(())
}

async fn wait_for_shutdown() -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = sigterm.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}
