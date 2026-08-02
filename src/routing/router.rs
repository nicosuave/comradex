use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::sync::Mutex;

use crate::{
    config::{Config, PoolConfig},
    routing::{AffinityStore, ThreadKey},
};

#[derive(Debug, Clone)]
pub struct Selection {
    pub account_id: String,
    pub bound: bool,
    pub thread: Option<ThreadKey>,
}

#[derive(Debug, Default)]
struct AccountRuntime {
    usage: Option<u8>,
    inflight: u64,
    last_assigned: u64,
    generation: u64,
    needs_login: bool,
    quota_until: Option<Instant>,
    avoid_until: Option<Instant>,
}

pub struct Router {
    pub affinity: Arc<AffinityStore>,
    accounts: Mutex<HashMap<String, AccountRuntime>>,
    active: Mutex<HashMap<String, String>>,
    sequence: AtomicU64,
    switch_at: u8,
}

impl Router {
    pub fn new(config: &Config, affinity: Arc<AffinityStore>) -> Self {
        Self {
            affinity,
            accounts: Mutex::new(
                config
                    .accounts
                    .keys()
                    .map(|k| (k.clone(), AccountRuntime::default()))
                    .collect(),
            ),
            active: Mutex::new(HashMap::new()),
            sequence: AtomicU64::new(1),
            switch_at: config.proxy.switch_at,
        }
    }

    pub async fn select(
        &self,
        pool_name: &str,
        pool: &PoolConfig,
        thread: Option<ThreadKey>,
        exclude: Option<&str>,
    ) -> Option<Selection> {
        let now = Instant::now();
        let binding = match &thread {
            Some(key) => self.affinity.get(key).await,
            None => None,
        };
        let mut accounts = self.accounts.lock().await;
        for runtime in accounts.values_mut() {
            if runtime.needs_login && runtime.avoid_until.is_some_and(|until| until <= now) {
                runtime.needs_login = false;
            }
        }
        if let Some(binding) = binding {
            let eligible = exclude != Some(binding.account_id.as_str())
                && pool.members.contains(&binding.account_id)
                && accounts.get(&binding.account_id).is_some_and(|a| {
                    a.generation == binding.account_generation
                        && !a.needs_login
                        && a.quota_until.is_none_or(|v| v <= now)
                });
            if eligible {
                return Some(Selection {
                    account_id: binding.account_id,
                    bound: true,
                    thread,
                });
            }
        }
        let active_id = self.active.lock().await.get(pool_name).cloned();
        let eligible = |id: &&String| {
            exclude != Some(id.as_str())
                && accounts.get(*id).is_some_and(|a| {
                    !a.needs_login
                        && a.quota_until.is_none_or(|v| v <= now)
                        && a.avoid_until.is_none_or(|v| v <= now)
                })
        };
        let selected = active_id
            .filter(|id| {
                pool.members.contains(id)
                    && eligible(&id)
                    && accounts
                        .get(id)
                        .and_then(|a| a.usage)
                        .is_none_or(|u| u < self.switch_at)
            })
            .or_else(|| {
                pool.members
                    .iter()
                    .filter(eligible)
                    .min_by_key(|id| {
                        let a = &accounts[*id];
                        let (tier, usage) = match a.usage {
                            Some(usage) if usage < self.switch_at => (0u8, usage),
                            None => (1u8, 0),
                            Some(usage) => (2u8, usage),
                        };
                        (tier, usage, a.inflight, a.last_assigned)
                    })
                    .cloned()
            })?;
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
        let generation = {
            let a = accounts.get_mut(&selected)?;
            a.last_assigned = seq;
            a.generation
        };
        self.active
            .lock()
            .await
            .insert(pool_name.to_owned(), selected.clone());
        if let Some(key) = thread.clone() {
            self.affinity.put(key, selected.clone(), generation).await;
        }
        Some(Selection {
            account_id: selected,
            bound: false,
            thread,
        })
    }

    pub async fn begin(&self, account: &str) {
        if let Some(a) = self.accounts.lock().await.get_mut(account) {
            a.inflight += 1;
        }
    }

    pub async fn bind(&self, key: ThreadKey, account: &str) -> bool {
        let generation = self
            .accounts
            .lock()
            .await
            .get(account)
            .map(|runtime| runtime.generation);
        let Some(generation) = generation else {
            return false;
        };
        self.affinity.put(key, account.to_owned(), generation).await;
        true
    }

    pub async fn select_exact(&self, pool: &PoolConfig, account: &str) -> Option<Selection> {
        let now = Instant::now();
        let accounts = self.accounts.lock().await;
        let eligible = pool.members.iter().any(|v| v == account)
            && accounts.get(account).is_some_and(|a| {
                !a.needs_login
                    && a.quota_until.is_none_or(|v| v <= now)
                    && a.avoid_until.is_none_or(|v| v <= now)
            });
        eligible.then(|| Selection {
            account_id: account.to_owned(),
            bound: true,
            thread: None,
        })
    }
    pub async fn end(&self, account: &str) {
        if let Some(a) = self.accounts.lock().await.get_mut(account) {
            a.inflight = a.inflight.saturating_sub(1);
        }
    }
    pub async fn quota_failure(&self, account: &str, headers: &hyper::HeaderMap) {
        let delay = quota_delay(headers);
        if let Some(a) = self.accounts.lock().await.get_mut(account) {
            a.quota_until = Some(Instant::now() + delay);
        }
    }
    pub async fn soft_failure(&self, account: &str) {
        if let Some(a) = self.accounts.lock().await.get_mut(account) {
            a.avoid_until = Some(Instant::now() + Duration::from_secs(5));
        }
    }
    pub async fn auth_failure(&self, account: &str) {
        if let Some(a) = self.accounts.lock().await.get_mut(account) {
            a.needs_login = true;
            a.avoid_until = Some(Instant::now() + Duration::from_secs(60));
            a.generation += 1;
        }
        self.affinity.invalidate_account(account).await;
    }
    pub async fn observe_headers(&self, account: &str, headers: &hyper::HeaderMap) {
        let candidates = [
            "x-codex-primary-used-percent",
            "x-ratelimit-primary-used-percent",
            "x-codex-usage-percent",
            "x-codex-secondary-used-percent",
            "x-ratelimit-secondary-used-percent",
        ];
        let usage = candidates
            .iter()
            .filter_map(|name| headers.get(*name)?.to_str().ok()?.parse::<f32>().ok())
            .max_by(|left, right| left.total_cmp(right))
            .map(|v| v.clamp(0.0, 100.0) as u8);
        if let (Some(usage), Some(a)) = (usage, self.accounts.lock().await.get_mut(account)) {
            a.usage = Some(usage);
        }
    }
    pub async fn record_count(&self) -> usize {
        self.accounts.lock().await.len()
    }
}

fn quota_delay(headers: &hyper::HeaderMap) -> Duration {
    let seconds = [
        "retry-after",
        "x-codex-primary-reset-after-seconds",
        "x-codex-secondary-reset-after-seconds",
        "x-ratelimit-reset-after-seconds",
    ]
    .iter()
    .filter_map(|name| {
        headers
            .get(*name)?
            .to_str()
            .ok()?
            .trim()
            .parse::<u64>()
            .ok()
    })
    .max()
    .unwrap_or(60)
    .clamp(1, 24 * 60 * 60);
    Duration::from_secs(seconds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AccountConfig, ListenerConfig, ProxyConfig};
    use hyper::{HeaderMap, header::HeaderValue};
    use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf};

    fn config(dir: &std::path::Path) -> Config {
        Config {
            proxy: ProxyConfig {
                installation_secret: "0123456789abcdef".into(),
                affinity_key: "0123456789abcdef0123456789abcdef".into(),
                state_dir: Some(dir.to_owned()),
                ..ProxyConfig::default()
            },
            listeners: BTreeMap::from([(
                "default".into(),
                ListenerConfig {
                    address: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
                    pool: "default".into(),
                },
            )]),
            pools: BTreeMap::from([(
                "default".into(),
                PoolConfig {
                    members: vec!["a".into(), "b".into()],
                },
            )]),
            accounts: BTreeMap::from([
                ("a".into(), AccountConfig::Inbound),
                ("b".into(), AccountConfig::Inbound),
            ]),
        }
    }

    #[tokio::test]
    async fn crossing_threshold_only_moves_new_threads() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let affinity = Arc::new(
            AffinityStore::load(
                PathBuf::from(dir.path()).join("a.json"),
                &cfg.proxy.affinity_key,
                100,
                100_000,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Router::new(&cfg, affinity.clone());
        let pool = &cfg.pools["default"];
        let first_key = affinity.key("thread-one");
        let first = router
            .select("default", pool, Some(first_key.clone()), None)
            .await
            .unwrap();
        assert_eq!(first.account_id, "a");
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-codex-primary-used-percent",
            HeaderValue::from_static("81"),
        );
        router.observe_headers("a", &headers).await;
        let bound = router
            .select("default", pool, Some(first_key), None)
            .await
            .unwrap();
        assert_eq!(bound.account_id, "a");
        assert!(bound.bound);
        let fresh = router
            .select("default", pool, Some(affinity.key("thread-two")), None)
            .await
            .unwrap();
        assert_eq!(fresh.account_id, "b");
    }

    #[tokio::test]
    async fn quota_failure_rebinds_to_one_alternate() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let affinity = Arc::new(
            AffinityStore::load(
                dir.path().join("a.json"),
                &cfg.proxy.affinity_key,
                100,
                100_000,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Router::new(&cfg, affinity.clone());
        let pool = &cfg.pools["default"];
        let key = affinity.key("thread");
        let first = router
            .select("default", pool, Some(key.clone()), None)
            .await
            .unwrap();
        router
            .quota_failure(&first.account_id, &HeaderMap::new())
            .await;
        let second = router
            .select("default", pool, Some(key.clone()), Some(&first.account_id))
            .await
            .unwrap();
        assert_ne!(first.account_id, second.account_id);
        assert_eq!(
            router
                .select("default", pool, Some(key), None)
                .await
                .unwrap()
                .account_id,
            second.account_id
        );
    }
}
