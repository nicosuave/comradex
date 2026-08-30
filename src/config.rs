use std::{
    collections::BTreeMap,
    fs,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub const CANONICAL_UPSTREAM: &str = "https://chatgpt.com/backend-api/codex";

fn default_upstream() -> String {
    CANONICAL_UPSTREAM.into()
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
    /// Account preferred for fresh, unbound work. Sticky conversations and hard ownership
    /// always take precedence, and an unhealthy or quota-limited account is skipped.
    #[serde(default)]
    pub preferred: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AccountConfig {
    Inbound,
    CodexHome { path: PathBuf },
}

/// Return the stable identity used for a managed CODEX_HOME. This removes
/// lexical aliases and resolves every existing ancestor, so homes that do not
/// exist yet still inherit symlink resolution from their parent directories.
pub fn normalize_codex_home(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        bail!("codex_home path must be absolute")
    }
    let mut lexical = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => lexical.push(prefix.as_os_str()),
            Component::RootDir => lexical.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !lexical.pop() {
                    bail!("codex_home path escapes its filesystem root")
                }
            }
            Component::Normal(part) => lexical.push(part),
        }
    }

    let mut ancestor = lexical.as_path();
    while !ancestor.exists() {
        ancestor = ancestor
            .parent()
            .context("codex_home path has no existing ancestor")?;
    }
    let canonical = fs::canonicalize(ancestor)
        .with_context(|| format!("resolve codex_home ancestor {}", ancestor.display()))?;
    let suffix = lexical
        .strip_prefix(ancestor)
        .expect("ancestor was obtained from the same path");
    Ok(canonical.join(suffix))
}

/// Most macOS installations use a case-insensitive filesystem. A nonexistent
/// suffix cannot be canonicalized yet, so conservatively fold it for collision
/// checks. Linux keeps its native case-sensitive path semantics.
fn managed_home_comparison_identity(path: &Path) -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        PathBuf::from(path.to_string_lossy().to_lowercase())
    }
    #[cfg(not(target_os = "macos"))]
    {
        path.to_owned()
    }
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
                *path = normalize_codex_home(path)?;
            }
        }
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<()> {
        if self.proxy.upstream != CANONICAL_UPSTREAM {
            bail!(
                "proxy.upstream must be exactly {CANONICAL_UPSTREAM}; custom upstreams cannot safely receive account credentials"
            )
        }
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
        let mut managed_homes: Vec<(&str, PathBuf, PathBuf)> = Vec::new();
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
                let identity = normalize_codex_home(path)
                    .with_context(|| format!("normalize account {name} codex_home"))?;
                let comparison_identity = managed_home_comparison_identity(&identity);
                for (other_name, other, other_comparison) in &managed_homes {
                    if comparison_identity == *other_comparison {
                        bail!(
                            "accounts {other_name} and {name} use the same codex_home {}",
                            identity.display()
                        )
                    }
                    if comparison_identity.starts_with(other_comparison)
                        || other_comparison.starts_with(&comparison_identity)
                    {
                        bail!(
                            "accounts {other_name} and {name} have overlapping codex_home paths ({} and {})",
                            other.display(),
                            identity.display()
                        )
                    }
                }
                managed_homes.push((name, identity, comparison_identity));
            }
        }
        if self.listeners.is_empty() {
            bail!("at least one listener is required")
        }
        for (pool_name, pool) in &self.pools {
            if let Some(preferred) = &pool.preferred
                && !pool.members.contains(preferred)
            {
                bail!(
                    "pool {pool_name} prefers account {preferred}, which is not one of its members"
                )
            }
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

/// Atomically replace a configuration file only after the full loader accepts the candidate.
/// The temporary file lives beside the target so relative paths resolve from the same directory.
pub fn write_validated(path: &Path, text: &str) -> Result<()> {
    use std::io::Write;

    let parent = path.parent().unwrap_or(Path::new("."));
    let mut temp = tempfile::Builder::new()
        .suffix(".toml")
        .tempfile_in(parent)?;
    temp.write_all(text.as_bytes())?;
    temp.as_file().sync_all()?;
    Config::load(temp.path()).context("validate updated configuration")?;
    if let Ok(metadata) = fs::metadata(path) {
        temp.as_file().set_permissions(metadata.permissions())?;
    }
    temp.persist(path).map_err(|error| error.error)?;
    Ok(())
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
    fn preferred_account_must_be_a_pool_member() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("comradex.toml");
        let text = config_text(None).replace(
            "members = [\"caller\"]",
            "members = [\"caller\"]\npreferred = \"missing\"",
        );
        fs::write(&path, text).unwrap();
        let error = Config::load(&path).unwrap_err();
        assert!(error.to_string().contains("not one of its members"));
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

    fn two_managed_accounts_text(first: &Path, second: &Path) -> String {
        config_text(None)
            .replace(
                "members = [\"caller\"]",
                "members = [\"first\", \"second\"]",
            )
            .replace(
                "[accounts.caller]\nkind = \"inbound\"",
                &format!(
                    "[accounts.first]\nkind = \"codex_home\"\npath = {:?}\n\n[accounts.second]\nkind = \"codex_home\"\npath = {:?}",
                    first, second
                ),
            )
    }

    #[test]
    fn lexical_account_home_alias_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("comradex.toml");
        let first = dir.path().join("accounts/one");
        let second = dir.path().join("accounts/other/../one");
        fs::write(&path, two_managed_accounts_text(&first, &second)).unwrap();

        let error = Config::load(&path).unwrap_err();
        assert!(error.to_string().contains("same codex_home"));
        assert!(error.to_string().contains("first"));
        assert!(error.to_string().contains("second"));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_account_home_alias_is_rejected() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let real_parent = dir.path().join("real");
        fs::create_dir_all(real_parent.join("account")).unwrap();
        let alias_parent = dir.path().join("alias");
        symlink(&real_parent, &alias_parent).unwrap();
        let path = dir.path().join("comradex.toml");
        fs::write(
            &path,
            two_managed_accounts_text(&real_parent.join("account"), &alias_parent.join("account")),
        )
        .unwrap();

        let error = Config::load(&path).unwrap_err();
        assert!(error.to_string().contains("same codex_home"));
    }

    #[test]
    fn nested_account_homes_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("comradex.toml");
        let first = dir.path().join("accounts/one");
        let second = first.join("nested");
        fs::write(&path, two_managed_accounts_text(&first, &second)).unwrap();

        let error = Config::load(&path).unwrap_err();
        assert!(error.to_string().contains("overlapping codex_home"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn nonexistent_case_folded_account_home_alias_is_rejected_on_macos() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("comradex.toml");
        let first = dir.path().join("accounts/Profile");
        let second = dir.path().join("accounts/profile");
        fs::write(&path, two_managed_accounts_text(&first, &second)).unwrap();

        let error = Config::load(&path).unwrap_err();
        assert!(error.to_string().contains("same codex_home"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn nonexistent_case_distinct_account_homes_remain_valid_on_linux() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("comradex.toml");
        let first = dir.path().join("accounts/Profile");
        let second = dir.path().join("accounts/profile");
        fs::write(&path, two_managed_accounts_text(&first, &second)).unwrap();

        Config::load(&path).unwrap();
    }

    #[test]
    fn arbitrary_upstream_host_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("comradex.toml");
        let text = config_text(None).replace(
            "[proxy]\n",
            "[proxy]\nupstream = \"https://example.com/backend-api/codex\"\n",
        );
        fs::write(&path, text).unwrap();

        let error = Config::load(&path).unwrap_err();
        assert!(error.to_string().contains(CANONICAL_UPSTREAM));
        assert!(error.to_string().contains("account credentials"));
    }

    #[test]
    fn cleartext_canonical_upstream_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("comradex.toml");
        let text = config_text(None).replace(
            "[proxy]\n",
            "[proxy]\nupstream = \"http://chatgpt.com/backend-api/codex\"\n",
        );
        fs::write(&path, text).unwrap();

        let error = Config::load(&path).unwrap_err();
        assert!(error.to_string().contains(CANONICAL_UPSTREAM));
        assert!(error.to_string().contains("account credentials"));
    }
}
