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
use tokio::net::TcpListener;

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
    let monitor_backend = Arc::clone(&backend);
    tokio::spawn(async move { monitor_backend.monitor().await });
    loop {
        let (stream, peer) = listener.accept().await?;
        let backend = Arc::clone(&backend);
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, backend).await {
                eprintln!("{peer}: {error}");
            }
        });
    }
}
