use crate::config::Config;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    net::TcpSocket,
    process::Command,
    sync::{Mutex, RwLock},
    time::{sleep, timeout},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendState {
    Offline,
    Starting,
    Online,
}

pub struct Backend {
    pub config: Config,
    state: RwLock<BackendState>,
    startup_lock: Mutex<()>,
    probe_cache: Mutex<Option<(Instant, bool)>>,
}

impl Backend {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            state: RwLock::new(BackendState::Offline),
            startup_lock: Mutex::new(()),
            probe_cache: Mutex::new(None),
        }
    }

    pub async fn state(&self) -> BackendState {
        *self.state.read().await
    }

    pub async fn probe(&self) -> bool {
        let cache_duration = Duration::from_millis(self.config.backend.probe_interval_ms);
        if let Some((checked_at, result)) = *self.probe_cache.lock().await
            && checked_at.elapsed() < cache_duration
        {
            return result;
        }
        let result = timeout(
            Duration::from_millis(self.config.backend.probe_interval_ms),
            async {
                let mut addresses = tokio::net::lookup_host(self.config.backend.address())
                    .await
                    .ok()?;
                let address = addresses.next()?;
                let socket = if address.is_ipv4() {
                    TcpSocket::new_v4().ok()?
                } else {
                    TcpSocket::new_v6().ok()?
                };
                socket.connect(address).await.ok()
            },
        )
        .await
        .is_ok_and(|result| result.is_some());
        *self.probe_cache.lock().await = Some((Instant::now(), result));
        result
    }

    pub async fn ensure_starting(self: &Arc<Self>) {
        let _guard = self.startup_lock.lock().await;
        if self.state().await != BackendState::Offline {
            return;
        }
        *self.state.write().await = BackendState::Starting;
        if !self.start_backend().await {
            *self.state.write().await = BackendState::Offline;
        }
    }

    pub async fn mark_offline(&self) {
        *self.state.write().await = BackendState::Offline;
        *self.probe_cache.lock().await = None;
    }

    pub async fn mark_online(&self) {
        *self.state.write().await = BackendState::Online;
    }

    async fn start_backend(&self) -> bool {
        let mut command = Command::new("bash");
        command.arg("-c").arg(&self.config.startup.command);
        #[cfg(unix)]
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                eprintln!("failed to start backend command: {error}");
                return false;
            }
        };
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(self.config.startup.timeout_seconds);
        loop {
            if self.probe().await {
                self.mark_online().await;
                tokio::spawn(async move {
                    let _ = child.wait().await;
                });
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                eprintln!("backend did not become available before startup timeout");
                Self::terminate_process_group(&mut child).await;
                return false;
            }
            sleep(Duration::from_millis(self.config.backend.probe_interval_ms)).await;
        }
    }

    async fn terminate_process_group(child: &mut tokio::process::Child) {
        let running = match child.try_wait() {
            Ok(Some(_)) => false,
            Ok(None) => true,
            Err(error) => {
                eprintln!("failed to inspect startup process: {error}");
                true
            }
        };
        if !running {
            if let Err(error) = child.wait().await {
                eprintln!("failed to reap startup process: {error}");
            }
            return;
        }
        #[cfg(unix)]
        if let Some(pid) = child.id() {
            let result = unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
            if result != 0 {
                eprintln!(
                    "failed to terminate startup process group {pid}: {}",
                    std::io::Error::last_os_error()
                );
            }
        }
        #[cfg(not(unix))]
        {
            if let Err(error) = child.kill().await {
                eprintln!("failed to terminate startup process: {error}");
            }
        }
        if let Err(error) = child.wait().await {
            eprintln!("failed to reap startup process: {error}");
        }
    }
}
