use crate::config::Config;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    net::TcpStream,
    process::Command,
    sync::{Mutex, Notify, RwLock},
    time::{sleep, timeout},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendState {
    Offline,
    Starting,
    Online,
}

struct ProbeCache {
    value: Option<(Instant, bool)>,
    refreshing: bool,
    generation: u64,
}

pub struct Backend {
    pub config: Config,
    backend_address: String,
    state: RwLock<BackendState>,
    startup_lock: Mutex<()>,
    probe_cache: Mutex<ProbeCache>,
    probe_notify: Notify,
}

impl Backend {
    pub fn new(config: Config) -> Self {
        let backend_address = config.backend.address();
        Self {
            config,
            backend_address,
            state: RwLock::new(BackendState::Offline),
            startup_lock: Mutex::new(()),
            probe_cache: Mutex::new(ProbeCache {
                value: None,
                refreshing: false,
                generation: 0,
            }),
            probe_notify: Notify::new(),
        }
    }

    pub fn address(&self) -> &str {
        &self.backend_address
    }

    pub async fn state(&self) -> BackendState {
        *self.state.read().await
    }

    pub async fn probe(&self) -> bool {
        let cache_duration = Duration::from_millis(self.config.backend.probe_interval_ms);
        loop {
            let notified = self.probe_notify.notified();
            let refresh_generation = {
                let mut cache = self.probe_cache.lock().await;
                if let Some((checked_at, result)) = cache.value
                    && checked_at.elapsed() < cache_duration
                {
                    return result;
                }
                if cache.refreshing {
                    None
                } else {
                    cache.refreshing = true;
                    Some(cache.generation)
                }
            };

            let Some(refresh_generation) = refresh_generation else {
                notified.await;
                continue;
            };

            let result = timeout(
                Duration::from_millis(self.config.backend.probe_interval_ms),
                TcpStream::connect(self.address()),
            )
            .await
            .is_ok_and(|result| result.is_ok());
            let backend_is_offline = self.state().await == BackendState::Offline;
            let mut cache = self.probe_cache.lock().await;
            if cache.generation == refresh_generation && backend_is_offline {
                cache.value = Some((Instant::now(), result));
            }
            cache.refreshing = false;
            drop(cache);
            self.probe_notify.notify_waiters();
            return result;
        }
    }

    pub async fn ensure_starting(self: &Arc<Self>) {
        let _guard = self.startup_lock.lock().await;
        if !self.claim_starting().await {
            return;
        }
        self.start_backend_and_reset().await;
    }

    pub async fn request_starting(self: &Arc<Self>) {
        let _guard = self.startup_lock.lock().await;
        if !self.claim_starting().await {
            return;
        }
        let backend = Arc::clone(self);
        tokio::spawn(async move {
            backend.start_backend_and_reset().await;
        });
    }

    async fn claim_starting(&self) -> bool {
        if self.state().await != BackendState::Offline {
            return false;
        }
        *self.state.write().await = BackendState::Starting;
        true
    }

    pub async fn mark_offline(&self) {
        let mut cache = self.probe_cache.lock().await;
        *self.state.write().await = BackendState::Offline;
        cache.value = None;
        cache.generation = cache.generation.wrapping_add(1);
    }

    pub async fn mark_online(&self) {
        *self.state.write().await = BackendState::Online;
    }

    async fn start_backend_and_reset(self: &Arc<Self>) {
        if !self.start_backend().await {
            self.mark_offline().await;
        }
    }

    async fn start_backend(self: &Arc<Self>) -> bool {
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
                let backend = Arc::clone(self);
                tokio::spawn(async move {
                    match child.wait().await {
                        Ok(status) => {
                            eprintln!("backend process exited with status {status}");
                            if backend.state().await == BackendState::Online {
                                backend.mark_offline().await;
                            }
                        }
                        Err(error) => {
                            eprintln!("failed to wait for backend process: {error}");
                            backend.mark_offline().await;
                        }
                    }
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
