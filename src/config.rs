use std::{
    collections::BTreeMap,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

fn default_upstream() -> String {
    "https://chatgpt.com/backend-api/codex".into()
}
fn default_switch() -> u8 {
    80
}
fn default_inflight() -> usize {
    64
}
fn default_upgrades() -> usize {
    32
}
fn default_bridge_sessions() -> usize {
    256
}
fn default_bridge_idle_seconds() -> u64 {
    15 * 60
}
fn default_bridge_admission_timeout_millis() -> u64 {
    2_000
}
fn default_replay_memory() -> usize {
    256 * 1024
}
fn default_request_limit() -> usize {
    128 * 1024 * 1024
}
fn default_global_spool() -> usize {
    512 * 1024 * 1024
}
fn default_affinity_entries() -> usize {
    100_000
}
fn default_affinity_bytes() -> usize {
    16 * 1024 * 1024
}
fn default_affinity_days() -> u64 {
    180
}
fn default_flush_seconds() -> u64 {
    5
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponsesWebsocketMode {
    Raw,
    #[default]
    HttpBridge,
    Direct,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub proxy: ProxyConfig,
    pub listeners: BTreeMap<String, ListenerConfig>,
    pub pools: BTreeMap<String, PoolConfig>,
    pub accounts: BTreeMap<String, AccountConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    #[serde(default = "default_upstream")]
    pub upstream: String,
    #[serde(default = "default_switch")]
    pub switch_at: u8,
    #[serde(default = "default_inflight")]
    pub max_inflight: usize,
    #[serde(default = "default_upgrades")]
    pub max_upgrades: usize,
    #[serde(default = "default_bridge_sessions")]
    pub max_bridge_sessions: usize,
    #[serde(default = "default_bridge_idle_seconds")]
    pub bridge_idle_seconds: u64,
    #[serde(default = "default_bridge_admission_timeout_millis")]
    pub bridge_admission_timeout_millis: u64,
    #[serde(default)]
    pub responses_websocket_mode: ResponsesWebsocketMode,
    #[serde(default = "default_replay_memory")]
    pub replay_memory_bytes: usize,
    #[serde(default = "default_request_limit")]
    pub max_request_bytes: usize,
    #[serde(default = "default_global_spool")]
    pub max_spool_bytes: usize,
    #[serde(default = "default_affinity_entries")]
    pub max_affinity_entries: usize,
    #[serde(default = "default_affinity_bytes")]
    pub max_affinity_bytes: usize,
    #[serde(default = "default_affinity_days")]
    pub affinity_idle_days: u64,
    #[serde(default = "default_flush_seconds")]
    pub snapshot_interval_seconds: u64,
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
    #[serde(default)]
    pub installation_secret: String,
    #[serde(default)]
    pub affinity_key: String,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            upstream: default_upstream(),
            switch_at: default_switch(),
            max_inflight: default_inflight(),
            max_upgrades: default_upgrades(),
            max_bridge_sessions: default_bridge_sessions(),
            bridge_idle_seconds: default_bridge_idle_seconds(),
            bridge_admission_timeout_millis: default_bridge_admission_timeout_millis(),
            responses_websocket_mode: ResponsesWebsocketMode::HttpBridge,
            replay_memory_bytes: default_replay_memory(),
            max_request_bytes: default_request_limit(),
            max_spool_bytes: default_global_spool(),
            max_affinity_entries: default_affinity_entries(),
            max_affinity_bytes: default_affinity_bytes(),
            affinity_idle_days: default_affinity_days(),
            snapshot_interval_seconds: default_flush_seconds(),
            state_dir: None,
            installation_secret: String::new(),
            affinity_key: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerConfig {
    pub address: SocketAddr,
    pub pool: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    pub members: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AccountConfig {
    Inbound,
    CodexHome { path: PathBuf },
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let config_path =
            fs::canonicalize(path).with_context(|| format!("resolve config {}", path.display()))?;
        let text = fs::read_to_string(&config_path)
            .with_context(|| format!("read {}", config_path.display()))?;
        let mut value: Self = toml::from_str(&text).context("parse config")?;
        let config_dir = config_path.parent().unwrap_or(Path::new("."));
        let state_dir = value
            .proxy
            .state_dir
            .take()
            .unwrap_or_else(|| PathBuf::from("state"));
        value.proxy.state_dir = Some(if state_dir.is_absolute() {
            state_dir
        } else {
            config_dir.join(state_dir)
        });
        for account in value.accounts.values_mut() {
            if let AccountConfig::CodexHome { path } = account {
                if path.as_os_str().is_empty() {
                    bail!("codex_home account path must not be empty")
                }
                if !path.is_absolute() {
                    *path = config_dir.join(&*path);
                }
            }
        }
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<()> {
        if self.proxy.installation_secret.len() < 16 {
            bail!("proxy.installation_secret must be at least 16 characters")
        }
        if self.proxy.affinity_key.len() < 32 {
            bail!("proxy.affinity_key must be at least 32 characters")
        }
        if !(1..=100).contains(&self.proxy.switch_at) {
            bail!("proxy.switch_at must be 1..=100")
        }
        if self.proxy.max_bridge_sessions == 0 {
            bail!("proxy.max_bridge_sessions must be greater than zero")
        }
        if self.proxy.bridge_idle_seconds == 0 {
            bail!("proxy.bridge_idle_seconds must be greater than zero")
        }
        if self.proxy.bridge_admission_timeout_millis == 0 {
            bail!("proxy.bridge_admission_timeout_millis must be greater than zero")
        }
        if self.accounts.len() > 512 {
            bail!("at most 512 accounts are supported")
        }
        for (name, account) in &self.accounts {
            if name.len() > 256 {
                bail!("account name exceeds the 256-byte limit")
            }
            if let AccountConfig::CodexHome { path } = account {
                if path.as_os_str().is_empty() {
                    bail!("account {name} has an empty codex_home path")
                }
                if !path.is_absolute() {
                    bail!("account {name} codex_home path must be absolute after loading")
                }
            }
        }
        if self.listeners.is_empty() {
            bail!("at least one listener is required")
        }
        for (name, listener) in &self.listeners {
            let pool = self.pools.get(&listener.pool).with_context(|| {
                format!("listener {name} references missing pool {}", listener.pool)
            })?;
            if pool.members.is_empty() {
                bail!("pool {} is empty", listener.pool)
            }
            for member in &pool.members {
                if !self.accounts.contains_key(member) {
                    bail!("pool {} references missing account {member}", listener.pool)
                }
            }
        }
        Ok(())
    }

    pub fn snapshot_interval(&self) -> Duration {
        Duration::from_secs(self.proxy.snapshot_interval_seconds.max(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_text(state_dir: Option<&str>) -> String {
        let state_dir = state_dir
            .map(|path| format!("state_dir = {path:?}\n"))
            .unwrap_or_default();
        format!(
            r#"[proxy]
installation_secret = "0123456789abcdef"
affinity_key = "0123456789abcdef0123456789abcdef"
{state_dir}
[listeners.default]
address = "127.0.0.1:10100"
pool = "default"

[pools.default]
members = ["caller"]

[accounts.caller]
kind = "inbound"
"#
        )
    }

    #[test]
    fn http_bridge_is_the_default_websocket_mode() {
        assert_eq!(
            ProxyConfig::default().responses_websocket_mode,
            ResponsesWebsocketMode::HttpBridge
        );
        assert_eq!(ProxyConfig::default().max_bridge_sessions, 256);
        assert_eq!(ProxyConfig::default().bridge_idle_seconds, 900);
        assert_eq!(
            ProxyConfig::default().bridge_admission_timeout_millis,
            2_000
        );
    }

    #[test]
    fn relative_state_dir_is_resolved_from_config_directory() {
        let dir = tempfile::tempdir().unwrap();
        let config_dir = dir.path().join("nested");
        fs::create_dir_all(&config_dir).unwrap();
        let path = config_dir.join("comradex.toml");
        fs::write(&path, config_text(Some("state/custom"))).unwrap();

        let config = Config::load(&path).unwrap();
        assert_eq!(
            config.proxy.state_dir.unwrap(),
            fs::canonicalize(&config_dir).unwrap().join("state/custom")
        );
    }

    #[test]
    fn default_state_dir_is_resolved_from_config_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("comradex.toml");
        fs::write(&path, config_text(None)).unwrap();

        let config = Config::load(&path).unwrap();
        assert_eq!(
            config.proxy.state_dir.unwrap(),
            fs::canonicalize(dir.path()).unwrap().join("state")
        );
    }

    #[test]
    fn relative_account_home_is_resolved_from_config_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("comradex.toml");
        let text = config_text(None)
            .replace("members = [\"caller\"]", "members = [\"managed\"]")
            .replace(
                "[accounts.caller]\nkind = \"inbound\"",
                "[accounts.managed]\nkind = \"codex_home\"\npath = \"accounts/managed\"",
            );
        fs::write(&path, text).unwrap();

        let config = Config::load(&path).unwrap();
        let AccountConfig::CodexHome { path: account_home } = &config.accounts["managed"] else {
            panic!("managed account kind changed")
        };
        assert_eq!(
            account_home,
            &fs::canonicalize(dir.path())
                .unwrap()
                .join("accounts/managed")
        );
    }

    #[test]
    fn empty_account_home_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("comradex.toml");
        let text = config_text(None)
            .replace("members = [\"caller\"]", "members = [\"managed\"]")
            .replace(
                "[accounts.caller]\nkind = \"inbound\"",
                "[accounts.managed]\nkind = \"codex_home\"\npath = \"\"",
            );
        fs::write(&path, text).unwrap();

        let error = Config::load(&path).unwrap_err();
        assert!(error.to_string().contains("must not be empty"));
    }
}
