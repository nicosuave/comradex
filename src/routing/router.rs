use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::error;

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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingSnapshot {
    #[serde(default)]
    pub preferred_accounts: BTreeMap<String, String>,
    #[serde(default)]
    pub active_accounts: BTreeMap<String, String>,
    #[serde(default)]
    pub account_states: BTreeMap<String, AccountRoutingStatus>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountRoutingStatus {
    pub available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_at_unix: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_percent: Option<u8>,
    pub inflight: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub quota_windows: BTreeMap<String, QuotaWindowStatus>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaWindowStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used_percent: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_at_unix: Option<i64>,
}

#[derive(Debug, Default)]
struct AccountRuntime {
    usage: Option<u8>,
    inflight: u64,
    last_assigned: u64,
    needs_login: bool,
    needs_login_retry_at: Option<Instant>,
    login_in_progress: bool,
    quota_until: Option<Instant>,
    quota_reset_at: Option<DateTime<Utc>>,
    quota_evidence: Option<QuotaEvidence>,
    avoid_until: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum QuotaWindow {
    Primary,
    Secondary,
    Tertiary,
}

#[derive(Debug, Clone, Copy)]
struct WindowEvidence {
    used_percent: Option<f32>,
    reset_at: Option<DateTime<Utc>>,
}

type QuotaEvidence = HashMap<QuotaWindow, WindowEvidence>;

pub struct Router {
    pub affinity: Arc<AffinityStore>,
    accounts: Mutex<HashMap<String, AccountRuntime>>,
    preferred: Mutex<HashMap<String, String>>,
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
            preferred: Mutex::new(
                config
                    .pools
                    .iter()
                    .filter_map(|(pool, config)| {
                        config
                            .preferred
                            .as_ref()
                            .map(|account| (pool.clone(), account.clone()))
                    })
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
        self.select_with_preference(pool_name, pool, thread, exclude, None)
            .await
    }

    /// Selects an account for fresh work while preferring an existing transport account only
    /// while it remains below the normal rotation threshold.
    pub async fn select_preferred(
        &self,
        pool_name: &str,
        pool: &PoolConfig,
        preferred: &str,
    ) -> Option<Selection> {
        self.select_with_preference(pool_name, pool, None, None, Some(preferred))
            .await
    }

    async fn select_with_preference(
        &self,
        pool_name: &str,
        pool: &PoolConfig,
        thread: Option<ThreadKey>,
        exclude: Option<&str>,
        preferred: Option<&str>,
    ) -> Option<Selection> {
        let now = Instant::now();
        let wall_now = Utc::now();
        let binding = match &thread {
            Some(key) => self.affinity.get(key).await,
            None => None,
        };
        let binding_epoch = match &binding {
            Some(binding) => Some(self.affinity.account_epoch(&binding.account_id).await),
            None => None,
        };
        let mut accounts = self.accounts.lock().await;
        for runtime in accounts.values_mut() {
            if runtime.needs_login
                && runtime
                    .needs_login_retry_at
                    .is_some_and(|until| until <= now)
            {
                runtime.needs_login = false;
                runtime.needs_login_retry_at = None;
            }
            reconcile_expired_quota(runtime, now, wall_now);
        }
        if let Some(binding) = binding {
            let eligible = exclude != Some(binding.account_id.as_str())
                && pool.members.contains(&binding.account_id)
                && accounts.get(&binding.account_id).is_some_and(|a| {
                    binding_epoch == Some(binding.account_generation)
                        && !a.needs_login
                        && !a.login_in_progress
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
        let configured_preferred = self.preferred.lock().await.get(pool_name).cloned();
        let active_id = self.active.lock().await.get(pool_name).cloned();
        let eligible = |id: &str| {
            exclude != Some(id)
                && accounts.get(id).is_some_and(|a| {
                    !a.needs_login
                        && !a.login_in_progress
                        && a.quota_until.is_none_or(|v| v <= now)
                        && a.avoid_until.is_none_or(|v| v <= now)
                })
        };
        let below_switch_at = |id: &str| {
            accounts
                .get(id)
                .and_then(|account| account.usage)
                .is_none_or(|usage| usage < self.switch_at)
        };
        let selected = configured_preferred
            .filter(|id| pool.members.contains(id) && eligible(id) && below_switch_at(id))
            .or_else(|| {
                preferred
                    .filter(|id| {
                        pool.members.iter().any(|member| member == *id)
                            && eligible(id)
                            && below_switch_at(id)
                    })
                    .map(str::to_owned)
            })
            .or_else(|| {
                active_id
                    .filter(|id| pool.members.contains(id) && eligible(id) && below_switch_at(id))
            })
            .or_else(|| {
                pool.members
                    .iter()
                    .filter(|id| eligible(id))
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
        {
            let a = accounts.get_mut(&selected)?;
            a.last_assigned = seq;
        }
        drop(accounts);
        self.active
            .lock()
            .await
            .insert(pool_name.to_owned(), selected.clone());
        if let Some(key) = thread.clone() {
            let generation = self.affinity.account_epoch(&selected).await;
            if !self.affinity.put(key, selected.clone(), generation).await {
                return None;
            }
        }
        Some(Selection {
            account_id: selected,
            bound: false,
            thread,
        })
    }

    /// Change the preferred account used for new work without disturbing bindings or in-flight
    /// requests. The caller validates pool membership before invoking this method.
    pub async fn set_preferred(&self, pool: &str, account: Option<String>) {
        let mut preferred = self.preferred.lock().await;
        match account {
            Some(account) => {
                preferred.insert(pool.to_owned(), account);
            }
            None => {
                preferred.remove(pool);
            }
        }
    }

    pub async fn routing_snapshot(&self) -> RoutingSnapshot {
        let now = Instant::now();
        let wall_now = Utc::now();
        let mut accounts = self.accounts.lock().await;
        for runtime in accounts.values_mut() {
            reconcile_runtime(runtime, now, wall_now);
        }
        let account_states = accounts
            .iter()
            .map(|(name, runtime)| (name.clone(), account_routing_status(runtime, now, wall_now)))
            .collect();
        drop(accounts);
        RoutingSnapshot {
            preferred_accounts: self
                .preferred
                .lock()
                .await
                .iter()
                .map(|(pool, account)| (pool.clone(), account.clone()))
                .collect(),
            active_accounts: self
                .active
                .lock()
                .await
                .iter()
                .map(|(pool, account)| (pool.clone(), account.clone()))
                .collect(),
            account_states,
        }
    }

    /// Accounts the router currently excludes because their credentials need repair.
    pub async fn accounts_needing_login(&self) -> BTreeSet<String> {
        self.accounts
            .lock()
            .await
            .iter()
            .filter(|(_, runtime)| runtime.needs_login)
            .map(|(account, _)| account.clone())
            .collect()
    }

    pub async fn begin(&self, account: &str) {
        if let Some(a) = self.accounts.lock().await.get_mut(account) {
            a.inflight += 1;
        }
    }

    pub async fn bind(&self, key: ThreadKey, account: &str) -> bool {
        if !self.accounts.lock().await.contains_key(account) {
            return false;
        }
        let generation = self.affinity.account_epoch(account).await;
        self.affinity.put(key, account.to_owned(), generation).await
    }

    pub async fn select_exact(&self, pool: &PoolConfig, account: &str) -> Option<Selection> {
        let now = Instant::now();
        let wall_now = Utc::now();
        let mut accounts = self.accounts.lock().await;
        if let Some(runtime) = accounts.get_mut(account) {
            reconcile_expired_quota(runtime, now, wall_now);
        }
        let eligible = pool.members.iter().any(|v| v == account)
            && accounts.get(account).is_some_and(|a| {
                !a.needs_login
                    && !a.login_in_progress
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

    pub async fn clear_inflight(&self) {
        for account in self.accounts.lock().await.values_mut() {
            account.inflight = 0;
        }
    }
    pub async fn quota_failure(&self, account: &str, headers: &hyper::HeaderMap) {
        let now = Utc::now();
        let evidence = blocking_quota_evidence(headers, now);
        let delay = quota_delay_at(headers, now, &evidence);
        if let Some(a) = self.accounts.lock().await.get_mut(account) {
            a.quota_until = Some(Instant::now() + delay);
            a.quota_reset_at = now.checked_add_signed(
                chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::MAX),
            );
            a.quota_evidence = (!evidence.is_empty()).then_some(evidence);
        }
    }
    pub async fn soft_failure(&self, account: &str) {
        if let Some(a) = self.accounts.lock().await.get_mut(account) {
            a.avoid_until = Some(Instant::now() + Duration::from_secs(5));
        }
    }
    pub async fn auth_failure(&self, account: &str) {
        if let Some(a) = self.accounts.lock().await.get_mut(account) {
            if a.needs_login && a.needs_login_retry_at.is_none() {
                return;
            }
            a.needs_login = true;
            let retry_at = Instant::now() + Duration::from_secs(60);
            a.needs_login_retry_at = Some(retry_at);
            a.avoid_until = Some(retry_at);
        }
        if !self.affinity.invalidate_account(account).await {
            error!(account, "failed to invalidate durable affinity bindings");
        }
    }

    /// Keep an account out of routing until its managed credential file is repaired. Unlike a
    /// request-time 401, a refresh-token rejection is not expected to heal after a short delay.
    pub async fn reauth_required(&self, account: &str) {
        if let Some(a) = self.accounts.lock().await.get_mut(account) {
            a.needs_login = true;
            a.needs_login_retry_at = None;
        }
    }

    /// Clear only the durable reauthentication state owned by the proactive refresh scheduler.
    /// Temporary request-time auth backoff remains owned by `auth_failure`.
    pub async fn proactive_auth_ready(&self, account: &str) {
        if let Some(a) = self.accounts.lock().await.get_mut(account)
            && a.needs_login
            && a.needs_login_retry_at.is_none()
        {
            a.needs_login = false;
        }
    }

    /// Reserve an account for exclusive credential maintenance. While set, both fresh and
    /// affinity-bound selections skip the account without changing its quota or auth state.
    pub async fn begin_login(&self, account: &str) -> bool {
        let mut accounts = self.accounts.lock().await;
        let Some(runtime) = accounts.get_mut(account) else {
            return false;
        };
        if runtime.login_in_progress {
            return false;
        }
        runtime.login_in_progress = true;
        true
    }

    /// End credential maintenance. A successful official login clears authentication failure
    /// state and its retry delay, but deliberately preserves quota cooldown evidence.
    pub async fn finish_login(&self, account: &str, succeeded: bool) {
        if let Some(runtime) = self.accounts.lock().await.get_mut(account) {
            runtime.login_in_progress = false;
            if succeeded {
                runtime.needs_login = false;
                runtime.needs_login_retry_at = None;
                runtime.avoid_until = None;
            }
        }
    }
    pub async fn observe_headers(&self, account: &str, headers: &hyper::HeaderMap) {
        let observed_evidence = quota_evidence(headers, Utc::now());
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
        if let Some(a) = self.accounts.lock().await.get_mut(account) {
            if let Some(usage) = usage {
                a.usage = Some(usage);
            }
            if a.quota_evidence
                .as_ref()
                .is_some_and(|blocked| quota_reset_confirmed(blocked, &observed_evidence))
            {
                a.quota_until = None;
                a.quota_reset_at = None;
                a.quota_evidence = None;
            }
        }
    }
    pub async fn record_count(&self) -> usize {
        self.accounts.lock().await.len()
    }
}

fn reconcile_expired_quota(runtime: &mut AccountRuntime, now: Instant, wall_now: DateTime<Utc>) {
    if runtime
        .quota_until
        .is_some_and(|quota_until| quota_until > now)
        && runtime
            .quota_reset_at
            .is_some_and(|reset_at| reset_at <= wall_now)
    {
        runtime.quota_until = None;
        runtime.quota_reset_at = None;
        runtime.quota_evidence = None;
    }
}

fn reconcile_runtime(runtime: &mut AccountRuntime, now: Instant, wall_now: DateTime<Utc>) {
    if runtime.needs_login
        && runtime
            .needs_login_retry_at
            .is_some_and(|retry_at| retry_at <= now)
    {
        runtime.needs_login = false;
        runtime.needs_login_retry_at = None;
    }
    reconcile_expired_quota(runtime, now, wall_now);
}

fn account_routing_status(
    runtime: &AccountRuntime,
    now: Instant,
    wall_now: DateTime<Utc>,
) -> AccountRoutingStatus {
    let (unavailable_reason, retry_at) = if runtime.login_in_progress {
        (Some("login_in_progress".to_owned()), None)
    } else if runtime.needs_login {
        (Some("needs_login".to_owned()), runtime.needs_login_retry_at)
    } else if runtime.quota_until.is_some_and(|until| until > now) {
        (Some("quota".to_owned()), runtime.quota_until)
    } else if runtime.avoid_until.is_some_and(|until| until > now) {
        (Some("temporary_failure".to_owned()), runtime.avoid_until)
    } else {
        (None, None)
    };
    let retry_at_unix = retry_at.map(|deadline| {
        wall_now.timestamp()
            + i64::try_from(deadline.saturating_duration_since(now).as_secs()).unwrap_or(i64::MAX)
    });
    let quota_windows = runtime
        .quota_evidence
        .as_ref()
        .into_iter()
        .flat_map(|evidence| evidence.iter())
        .map(|(window, evidence)| {
            (
                window.name().to_owned(),
                QuotaWindowStatus {
                    used_percent: evidence
                        .used_percent
                        .map(|percent| percent.clamp(0.0, 100.0).round() as u8),
                    reset_at_unix: evidence.reset_at.map(|reset_at| reset_at.timestamp()),
                },
            )
        })
        .collect();
    AccountRoutingStatus {
        available: unavailable_reason.is_none(),
        unavailable_reason,
        retry_at_unix,
        usage_percent: runtime.usage,
        inflight: runtime.inflight,
        quota_windows,
    }
}

fn quota_evidence(headers: &hyper::HeaderMap, now: DateTime<Utc>) -> QuotaEvidence {
    [
        (QuotaWindow::Primary, "primary"),
        (QuotaWindow::Secondary, "secondary"),
        (QuotaWindow::Tertiary, "tertiary"),
    ]
    .into_iter()
    .filter_map(|(window, name)| {
        let used_percent = [
            format!("x-codex-{name}-used-percent"),
            format!("x-ratelimit-{name}-used-percent"),
        ]
        .into_iter()
        .filter_map(|header| {
            headers
                .get(&header)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse::<f32>().ok())
        })
        .max_by(|left, right| left.total_cmp(right));
        let reset_at = ["x-codex", "x-ratelimit"]
            .into_iter()
            .filter_map(|prefix| {
                let absolute = format!("{prefix}-{name}-reset-at");
                let relative = format!("{prefix}-{name}-reset-after-seconds");
                headers
                    .get(&absolute)
                    .and_then(|value| value.to_str().ok())
                    .and_then(parse_reset_at)
                    .or_else(|| {
                        headers
                            .get(&relative)
                            .and_then(|value| value.to_str().ok())
                            .and_then(|value| value.trim().parse::<i64>().ok())
                            .and_then(|seconds| {
                                now.checked_add_signed(chrono::Duration::seconds(seconds))
                            })
                    })
            })
            .max();
        (used_percent.is_some() || reset_at.is_some()).then_some((
            window,
            WindowEvidence {
                used_percent,
                reset_at,
            },
        ))
    })
    .collect()
}

fn blocking_quota_evidence(headers: &hyper::HeaderMap, now: DateTime<Utc>) -> QuotaEvidence {
    let evidence = quota_evidence(headers, now);
    let exhausted = evidence
        .iter()
        .filter(|(_, window)| window.used_percent.is_some_and(|used| used >= 100.0))
        .map(|(window, evidence)| (*window, *evidence))
        .collect::<QuotaEvidence>();
    if exhausted.is_empty() {
        evidence
    } else {
        exhausted
    }
}

fn quota_reset_confirmed(blocked: &QuotaEvidence, observed: &QuotaEvidence) -> bool {
    !blocked.is_empty()
        && blocked.iter().all(|(window, before)| {
            let Some(after) = observed.get(window) else {
                return false;
            };
            let usage_recovered = match (before.used_percent, after.used_percent) {
                (Some(before), Some(after)) => after < before,
                (None, Some(after)) => after < 100.0,
                _ => false,
            };
            let reset_advanced = match (before.reset_at, after.reset_at) {
                (Some(before), Some(after)) => {
                    after.signed_duration_since(before) > chrono::Duration::seconds(60)
                }
                _ => false,
            };
            usage_recovered && reset_advanced
        })
}

fn quota_delay_at(
    headers: &hyper::HeaderMap,
    now: DateTime<Utc>,
    evidence: &QuotaEvidence,
) -> Duration {
    let retry_after = headers.get("retry-after").and_then(|value| {
        let value = value.to_str().ok()?.trim();
        value
            .parse::<u64>()
            .ok()
            .or_else(|| parse_http_date(value).map(|reset_at| seconds_until(reset_at, now)))
    });
    let generic_reset = [
        "x-codex-reset-after-seconds",
        "x-ratelimit-reset-after-seconds",
    ]
    .into_iter()
    .filter_map(|name| headers.get(name)?.to_str().ok()?.trim().parse::<u64>().ok())
    .max();
    let seconds = retry_after
        .into_iter()
        .chain(generic_reset)
        .chain(
            evidence
                .values()
                .filter_map(|window| window.reset_at.map(|reset_at| seconds_until(reset_at, now))),
        )
        .max()
        .unwrap_or(60)
        .clamp(1, 24 * 60 * 60);
    Duration::from_secs(seconds)
}

impl QuotaWindow {
    fn name(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Secondary => "secondary",
            Self::Tertiary => "tertiary",
        }
    }
}

fn parse_reset_at(value: &str) -> Option<DateTime<Utc>> {
    value
        .parse::<i64>()
        .ok()
        .and_then(|timestamp| DateTime::from_timestamp(timestamp, 0))
        .or_else(|| {
            DateTime::parse_from_rfc3339(value)
                .ok()
                .map(|timestamp| timestamp.with_timezone(&Utc))
        })
}

fn parse_http_date(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .or_else(|| {
            NaiveDateTime::parse_from_str(value, "%a, %d %b %Y %H:%M:%S GMT")
                .ok()
                .map(|timestamp| timestamp.and_utc())
        })
}

fn seconds_until(reset_at: DateTime<Utc>, now: DateTime<Utc>) -> u64 {
    reset_at
        .signed_duration_since(now)
        .num_milliseconds()
        .max(0)
        .saturating_add(999)
        .div_euclid(1_000) as u64
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
                    preferred: None,
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
    async fn live_preference_changes_fresh_work_without_rebinding_existing_threads() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let affinity = Arc::new(
            AffinityStore::load(
                dir.path().join("a.json"),
                &cfg.proxy.affinity_key,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Router::new(&cfg, affinity.clone());
        let pool = &cfg.pools["default"];
        let existing_key = affinity.key("existing");
        let existing = router
            .select("default", pool, Some(existing_key.clone()), None)
            .await
            .unwrap();
        assert_eq!(existing.account_id, "a");

        router.set_preferred("default", Some("b".to_owned())).await;

        let bound = router
            .select("default", pool, Some(existing_key), None)
            .await
            .unwrap();
        assert_eq!(bound.account_id, "a");
        assert!(bound.bound);
        let fresh = router
            .select("default", pool, Some(affinity.key("fresh")), None)
            .await
            .unwrap();
        assert_eq!(fresh.account_id, "b");
        assert!(!fresh.bound);
        assert_eq!(
            router.routing_snapshot().await,
            RoutingSnapshot {
                preferred_accounts: BTreeMap::from([("default".into(), "b".into())]),
                active_accounts: BTreeMap::from([("default".into(), "b".into())]),
                account_states: BTreeMap::from([
                    (
                        "a".into(),
                        AccountRoutingStatus {
                            available: true,
                            ..Default::default()
                        }
                    ),
                    (
                        "b".into(),
                        AccountRoutingStatus {
                            available: true,
                            ..Default::default()
                        }
                    ),
                ]),
            }
        );
    }

    #[tokio::test]
    async fn configured_preference_wins_over_reused_transport_preference() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let affinity = Arc::new(
            AffinityStore::load(
                dir.path().join("a.json"),
                &cfg.proxy.affinity_key,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Router::new(&cfg, affinity);
        let pool = &cfg.pools["default"];
        router.set_preferred("default", Some("b".to_owned())).await;

        let selected = router.select_preferred("default", pool, "a").await.unwrap();
        assert_eq!(selected.account_id, "b");
    }

    #[tokio::test]
    async fn exhausted_transport_preference_does_not_pin_fresh_work() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let affinity = Arc::new(
            AffinityStore::load(
                dir.path().join("a.json"),
                &cfg.proxy.affinity_key,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Router::new(&cfg, affinity);
        let pool = &cfg.pools["default"];
        let first = router.select("default", pool, None, None).await.unwrap();
        assert_eq!(first.account_id, "a");

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-codex-primary-used-percent",
            HeaderValue::from_static("100"),
        );
        router.observe_headers("a", &headers).await;

        let fresh = router.select_preferred("default", pool, "a").await.unwrap();
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

    #[tokio::test]
    async fn login_maintenance_excludes_bound_and_exact_selection_then_clears_auth_state() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let affinity = Arc::new(
            AffinityStore::load(
                dir.path().join("a.json"),
                &cfg.proxy.affinity_key,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Router::new(&cfg, affinity.clone());
        let pool = &cfg.pools["default"];
        let key = affinity.key("bound-login");
        assert!(router.bind(key.clone(), "a").await);
        router.reauth_required("a").await;
        assert!(router.begin_login("a").await);
        assert!(!router.begin_login("a").await);
        assert!(router.select_exact(pool, "a").await.is_none());
        let selection = router
            .select("default", pool, Some(key), None)
            .await
            .unwrap();
        assert_eq!(selection.account_id, "b");

        router.finish_login("a", true).await;
        assert!(router.select_exact(pool, "a").await.is_some());
    }

    #[tokio::test]
    async fn successful_login_maintenance_preserves_quota_cooldown() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let affinity = Arc::new(
            AffinityStore::load(
                dir.path().join("a.json"),
                &cfg.proxy.affinity_key,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Router::new(&cfg, affinity);
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("3600"));
        router.quota_failure("a", &headers).await;
        assert!(router.begin_login("a").await);
        router.finish_login("a", true).await;

        assert!(
            router
                .select_exact(&cfg.pools["default"], "a")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn authoritative_new_quota_window_clears_stale_cooldown() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config(dir.path());
        cfg.pools.get_mut("default").unwrap().members = vec!["a".into()];
        let affinity = Arc::new(
            AffinityStore::load(
                dir.path().join("a.json"),
                &cfg.proxy.affinity_key,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Router::new(&cfg, affinity);
        let pool = &cfg.pools["default"];
        let mut blocked = HeaderMap::new();
        blocked.insert(
            "x-codex-primary-used-percent",
            HeaderValue::from_static("100"),
        );
        blocked.insert(
            "x-codex-primary-reset-at",
            HeaderValue::from_static("2099-01-01T00:00:00Z"),
        );
        router.quota_failure("a", &blocked).await;
        assert!(router.select("default", pool, None, None).await.is_none());

        let mut reset = HeaderMap::new();
        reset.insert(
            "x-codex-primary-used-percent",
            HeaderValue::from_static("3"),
        );
        reset.insert(
            "x-codex-primary-reset-at",
            HeaderValue::from_static("2099-02-01T00:00:00Z"),
        );
        router.observe_headers("a", &reset).await;

        assert!(router.select("default", pool, None, None).await.is_some());
    }

    #[tokio::test]
    async fn elapsed_absolute_reset_clears_stale_quota_cooldown_before_selection() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config(dir.path());
        cfg.pools.get_mut("default").unwrap().members = vec!["a".into()];
        let affinity = Arc::new(
            AffinityStore::load(
                dir.path().join("a.json"),
                &cfg.proxy.affinity_key,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Router::new(&cfg, affinity);
        let pool = &cfg.pools["default"];
        let mut reset = HeaderMap::new();
        reset.insert(
            "x-codex-primary-used-percent",
            HeaderValue::from_static("100"),
        );
        reset.insert(
            "x-codex-primary-reset-at",
            HeaderValue::from_static("2020-01-01T00:00:00Z"),
        );
        let evidence = quota_evidence(&reset, Utc::now());
        {
            let mut accounts = router.accounts.lock().await;
            let account = accounts.get_mut("a").unwrap();
            account.quota_until = Some(Instant::now() + Duration::from_secs(3600));
            account.quota_reset_at = Some(DateTime::from_timestamp(1_577_836_800, 0).unwrap());
            account.quota_evidence = Some(evidence);
        }

        assert_eq!(
            router
                .select("default", pool, None, None)
                .await
                .unwrap()
                .account_id,
            "a"
        );
    }

    #[tokio::test]
    async fn retry_after_deadline_prevents_early_window_reset_reconciliation() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config(dir.path());
        cfg.pools.get_mut("default").unwrap().members = vec!["a".into()];
        let affinity = Arc::new(
            AffinityStore::load(
                dir.path().join("a.json"),
                &cfg.proxy.affinity_key,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Router::new(&cfg, affinity);
        let pool = &cfg.pools["default"];
        let wall_now = Utc::now();
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("120"));
        headers.insert(
            "x-codex-primary-used-percent",
            HeaderValue::from_static("100"),
        );
        headers.insert(
            "x-codex-primary-reset-after-seconds",
            HeaderValue::from_static("45"),
        );
        router.quota_failure("a", &headers).await;

        {
            let mut accounts = router.accounts.lock().await;
            let account = accounts.get_mut("a").unwrap();
            account.quota_until = Some(Instant::now() + Duration::from_secs(3600));
            account.quota_reset_at = Some(wall_now + chrono::Duration::seconds(120));
            reconcile_expired_quota(
                account,
                Instant::now(),
                wall_now + chrono::Duration::seconds(46),
            );
            assert!(account.quota_until.is_some());
            reconcile_expired_quota(
                account,
                Instant::now(),
                wall_now + chrono::Duration::seconds(121),
            );
            assert!(account.quota_until.is_none());
        }
        assert!(router.select("default", pool, None, None).await.is_some());
    }

    #[tokio::test]
    async fn usage_alone_does_not_clear_quota_cooldown() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config(dir.path());
        cfg.pools.get_mut("default").unwrap().members = vec!["a".into()];
        let affinity = Arc::new(
            AffinityStore::load(
                dir.path().join("a.json"),
                &cfg.proxy.affinity_key,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Router::new(&cfg, affinity);
        let pool = &cfg.pools["default"];
        let mut blocked = HeaderMap::new();
        blocked.insert(
            "x-codex-primary-used-percent",
            HeaderValue::from_static("100"),
        );
        blocked.insert(
            "x-codex-primary-reset-at",
            HeaderValue::from_static("2099-01-01T00:00:00Z"),
        );
        router.quota_failure("a", &blocked).await;

        let mut ambiguous = HeaderMap::new();
        ambiguous.insert(
            "x-codex-primary-used-percent",
            HeaderValue::from_static("3"),
        );
        ambiguous.insert(
            "x-codex-primary-reset-at",
            HeaderValue::from_static("2099-01-01T00:00:00Z"),
        );
        router.observe_headers("a", &ambiguous).await;

        assert!(router.select("default", pool, None, None).await.is_none());
    }

    #[tokio::test]
    async fn proactive_reauth_is_durable_without_invalidating_affinity() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let affinity = Arc::new(
            AffinityStore::load(
                dir.path().join("a.json"),
                &cfg.proxy.affinity_key,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Router::new(&cfg, affinity.clone());
        let pool = &cfg.pools["default"];
        let key = affinity.key("owned-thread");
        assert!(router.bind(key.clone(), "a").await);

        router.reauth_required("a").await;
        assert!(router.select_exact(pool, "a").await.is_none());
        assert_eq!(
            router
                .select("default", pool, None, None)
                .await
                .unwrap()
                .account_id,
            "b"
        );

        router.proactive_auth_ready("a").await;
        assert!(router.select_exact(pool, "a").await.is_some());
        assert_eq!(affinity.get(&key).await.unwrap().account_id, "a");
    }

    fn fixed_now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-08-11T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn quota_delay(headers: &HeaderMap, now: DateTime<Utc>) -> Duration {
        let evidence = blocking_quota_evidence(headers, now);
        quota_delay_at(headers, now, &evidence)
    }

    #[test]
    fn quota_delay_accepts_tertiary_only_evidence() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-codex-tertiary-reset-after-seconds",
            HeaderValue::from_static("321"),
        );

        assert_eq!(quota_delay(&headers, fixed_now()), Duration::from_secs(321));
    }

    #[test]
    fn quota_delay_accepts_epoch_and_iso_absolute_resets() {
        let now = fixed_now();
        let mut epoch_headers = HeaderMap::new();
        epoch_headers.insert(
            "x-codex-primary-reset-at",
            HeaderValue::from_str(&(now.timestamp() + 450).to_string()).unwrap(),
        );
        assert_eq!(quota_delay(&epoch_headers, now), Duration::from_secs(450));

        let mut iso_headers = HeaderMap::new();
        iso_headers.insert(
            "x-ratelimit-secondary-reset-at",
            HeaderValue::from_static("2026-08-11T12:07:31Z"),
        );
        assert_eq!(quota_delay(&iso_headers, now), Duration::from_secs(451));
    }

    #[test]
    fn quota_delay_accepts_http_date_retry_after() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "retry-after",
            HeaderValue::from_static("Tue, 11 Aug 2026 12:02:03 GMT"),
        );

        assert_eq!(quota_delay(&headers, fixed_now()), Duration::from_secs(123));
    }

    #[test]
    fn quota_delay_uses_longest_valid_window_and_ignores_malformed_values() {
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("20"));
        headers.insert(
            "x-codex-primary-reset-after-seconds",
            HeaderValue::from_static("malformed"),
        );
        headers.insert(
            "x-codex-secondary-reset-at",
            HeaderValue::from_static("2026-08-11T12:00:45Z"),
        );
        headers.insert(
            "x-ratelimit-tertiary-reset-after-seconds",
            HeaderValue::from_static("90"),
        );

        assert_eq!(quota_delay(&headers, fixed_now()), Duration::from_secs(90));
    }

    #[test]
    fn quota_delay_ignores_non_exhausted_longer_windows() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-codex-primary-used-percent",
            HeaderValue::from_static("100"),
        );
        headers.insert(
            "x-codex-primary-reset-after-seconds",
            HeaderValue::from_static("45"),
        );
        headers.insert(
            "x-codex-secondary-used-percent",
            HeaderValue::from_static("12"),
        );
        headers.insert(
            "x-codex-secondary-reset-after-seconds",
            HeaderValue::from_static("86400"),
        );

        let evidence = blocking_quota_evidence(&headers, fixed_now());
        assert_eq!(evidence.len(), 1);
        assert!(evidence.contains_key(&QuotaWindow::Primary));
        assert_eq!(
            quota_delay_at(&headers, fixed_now(), &evidence),
            Duration::from_secs(45)
        );
    }

    #[test]
    fn quota_reset_confirmation_requires_every_blocking_window_to_recover() {
        let now = fixed_now();
        let mut blocked_headers = HeaderMap::new();
        for (usage, reset) in [
            ("x-codex-primary-used-percent", "x-codex-primary-reset-at"),
            (
                "x-codex-secondary-used-percent",
                "x-codex-secondary-reset-at",
            ),
        ] {
            blocked_headers.insert(usage, HeaderValue::from_static("100"));
            blocked_headers.insert(reset, HeaderValue::from_static("2026-08-11T13:00:00Z"));
        }
        let blocked = blocking_quota_evidence(&blocked_headers, now);
        let mut observed = HeaderMap::new();
        observed.insert(
            "x-codex-primary-used-percent",
            HeaderValue::from_static("1"),
        );
        observed.insert(
            "x-codex-primary-reset-at",
            HeaderValue::from_static("2026-08-12T13:00:00Z"),
        );
        observed.insert(
            "x-codex-secondary-used-percent",
            HeaderValue::from_static("100"),
        );
        observed.insert(
            "x-codex-secondary-reset-at",
            HeaderValue::from_static("2026-08-11T13:00:00Z"),
        );
        assert!(!quota_reset_confirmed(
            &blocked,
            &quota_evidence(&observed, now)
        ));
    }

    #[tokio::test]
    async fn routing_snapshot_exposes_quota_deadline_and_blocking_window() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let affinity = Arc::new(
            AffinityStore::load(
                dir.path().join("a.json"),
                &cfg.proxy.affinity_key,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Router::new(&cfg, affinity);
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("120"));
        headers.insert(
            "x-codex-primary-used-percent",
            HeaderValue::from_static("100"),
        );
        headers.insert(
            "x-codex-primary-reset-after-seconds",
            HeaderValue::from_static("120"),
        );
        router.quota_failure("a", &headers).await;

        let snapshot = router.routing_snapshot().await;
        let account = &snapshot.account_states["a"];
        assert!(!account.available);
        assert_eq!(account.unavailable_reason.as_deref(), Some("quota"));
        assert!(account.retry_at_unix.is_some());
        assert_eq!(account.quota_windows["primary"].used_percent, Some(100));
    }
}
