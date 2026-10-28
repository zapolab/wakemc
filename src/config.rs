use crate::error::ProxyError;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: String,
    pub backend: BackendConfig,
    pub startup: StartupConfig,
    pub messages: MessagesConfig,
}

#[derive(Debug, Deserialize)]
pub struct BackendConfig {
    pub host: String,
    pub port: u16,
    #[serde(default = "default_probe_interval")]
    pub probe_interval_ms: u64,
}

impl BackendConfig {
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Deserialize)]
pub struct StartupConfig {
    pub command: String,
    #[serde(default = "default_startup_timeout")]
    pub timeout_seconds: u64,
}

#[derive(Debug, Deserialize)]
pub struct MessagesConfig {
    #[serde(default = "default_offline_motd")]
    pub offline_motd: String,
    #[serde(default = "default_starting_motd")]
    pub starting_motd: String,
    #[serde(default = "default_kick")]
    pub kick: String,
}

impl Config {
    pub async fn load(path: &str) -> Result<Self, ProxyError> {
        let text = tokio::fs::read_to_string(path).await?;
        let config: Self =
            toml::from_str(&text).map_err(|error| ProxyError::Config(error.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ProxyError> {
        if self.backend.host.trim().is_empty() || self.backend.port == 0 {
            return Err(ProxyError::Config(
                "backend host and port are required".into(),
            ));
        }
        if self.backend.probe_interval_ms == 0 || self.startup.timeout_seconds == 0 {
            return Err(ProxyError::Config(
                "probe interval and startup timeout must be positive".into(),
            ));
        }
        if self.startup.command.trim().is_empty() {
            return Err(ProxyError::Config(
                "startup command must not be empty".into(),
            ));
        }
        Ok(())
    }
}

fn default_listen() -> String {
    "0.0.0.0:25565".into()
}
fn default_probe_interval() -> u64 {
    5_000
}
fn default_startup_timeout() -> u64 {
    120
}
fn default_offline_motd() -> String {
    "Server offline - reconnect to wake it".into()
}
fn default_starting_motd() -> String {
    "§2Server is starting - please reconnect shortly".into()
}
fn default_kick() -> String {
    "§2Server is starting, please reconnect shortly".into()
}
