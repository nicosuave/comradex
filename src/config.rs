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
        let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let mut value: Self = toml::from_str(&text).context("parse config")?;
        value.validate()?;
        if value.proxy.state_dir.is_none() {
            value.proxy.state_dir = Some(path.parent().unwrap_or(Path::new(".")).join("state"));
        }
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
