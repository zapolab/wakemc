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
    sync::{Mutex, mpsc, watch},
    task::{AbortHandle, JoinHandle, JoinSet},
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
    let (task_sender, task_receiver) = mpsc::unbounded_channel();
    let task_abort_handles = Arc::new(Mutex::new(Vec::new()));
    let task_supervisor = tokio::spawn(supervise_tasks(task_receiver));
    let shutdown_signal = wait_for_shutdown();
    tokio::pin!(shutdown_signal);
    let result = loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, peer) = match result {
                    Ok(connection) => connection,
                    Err(error) => break Err(error.into()),
                };
                let backend = Arc::clone(&backend);
                let shutdown = shutdown.subscribe();
                let task = tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, backend, shutdown).await
                        && !error.is_expected_disconnect()
                    {
                        eprintln!("{peer}: {error}");
                    }
                });
                task_abort_handles.lock().await.push(task.abort_handle());
                if task_sender.send(task).is_err() {
                    break Err(ProxyError::Protocol(
                        "proxy task supervisor stopped unexpectedly".into(),
                    ));
                }
            }
            result = &mut shutdown_signal => {
                if let Err(error) = result {
                    break Err(error.into());
                }
                println!("shutdown signal received, closing connections");
                break Ok(());
            }
        }
    };
    let _ = shutdown.send(true);
    drop(task_sender);
    shutdown_tasks(task_supervisor, task_abort_handles, Duration::from_secs(5)).await;
    result
}

async fn shutdown_tasks(
    mut task_supervisor: JoinHandle<()>,
    task_abort_handles: Arc<Mutex<Vec<AbortHandle>>>,
    wait: Duration,
) {
    if timeout(wait, &mut task_supervisor).await.is_err() {
        eprintln!("shutdown timed out, aborting remaining proxy tasks");
        for task in task_abort_handles.lock().await.iter() {
            task.abort();
        }
        task_supervisor.abort();
        let _ = task_supervisor.await;
    }
}

async fn supervise_tasks(mut receiver: mpsc::UnboundedReceiver<JoinHandle<()>>) {
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            task = receiver.recv() => {
                match task {
                    Some(task) => {
                        tasks.spawn(task);
                    }
                    None => break,
                }
            }
            result = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    eprintln!("proxy task failed: {error}");
                }
            }
        }
    }
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            eprintln!("proxy task failed during shutdown: {error}");
        }
    }
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

#[cfg(test)]
mod tests {
    use super::{shutdown_tasks, supervise_tasks};
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn shutdown_aborts_supervisor_with_stuck_task() {
        let (sender, receiver) = mpsc::unbounded_channel();
        let supervisor = tokio::spawn(supervise_tasks(receiver));
        let stuck_task = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        let stuck_task_abort = stuck_task.abort_handle();
        sender.send(stuck_task).unwrap();
        drop(sender);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        shutdown_tasks(
            supervisor,
            Arc::new(Mutex::new(vec![stuck_task_abort.clone()])),
            std::time::Duration::from_millis(1),
        )
        .await;

        tokio::task::yield_now().await;
        assert!(stuck_task_abort.is_finished());
    }
}
