use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
struct Binding {
    account_id: String,
    created: Instant,
    created_unix: u64,
}

pub struct LiveCallStore {
    inner: Mutex<HashMap<String, Binding>>,
    key: [u8; 32],
    max_entries: usize,
    ttl: Duration,
    snapshot_path: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedBinding {
    account_id: String,
    created_unix: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct InvalidCallId;

impl LiveCallStore {
    pub fn load(key_text: &str, max_entries: usize, ttl: Duration, snapshot_path: PathBuf) -> Self {
        let mut material = b"comradex/live/".to_vec();
        material.extend_from_slice(key_text.as_bytes());
        let now_unix = unix_now();
        let persisted: HashMap<String, PersistedBinding> = fs::read(&snapshot_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default();
        let mut entries: Vec<_> = persisted
            .into_iter()
            .filter(|(_, value)| now_unix.saturating_sub(value.created_unix) <= ttl.as_secs())
            .collect();
        entries.sort_by_key(|(_, value)| std::cmp::Reverse(value.created_unix));
        entries.truncate(max_entries);
        let inner = entries
            .into_iter()
            .map(|(digest, value)| {
                let age = Duration::from_secs(now_unix.saturating_sub(value.created_unix));
                (
                    digest,
                    Binding {
                        account_id: value.account_id,
                        created: Instant::now().checked_sub(age).unwrap_or_else(Instant::now),
                        created_unix: value.created_unix,
                    },
                )
            })
            .collect();
        Self {
            inner: Mutex::new(inner),
            key: *blake3::hash(&material).as_bytes(),
            max_entries,
            ttl,
            snapshot_path,
        }
    }

    pub async fn bind(&self, call_id: &str, account_id: String) -> bool {
        if !valid_call_id(call_id) {
            return false;
        }
        let mut inner = self.inner.lock().await;
        self.expire(&mut inner);
        let digest = self.digest(call_id);
        if let Some(existing) = inner.get(&digest) {
            return existing.account_id == account_id;
        }
        inner.insert(
            digest.clone(),
            Binding {
                account_id,
                created: Instant::now(),
                created_unix: unix_now(),
            },
        );
        while inner.len() > self.max_entries {
            let Some(oldest) = inner
                .iter()
                .min_by_key(|(_, v)| v.created)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            inner.remove(&oldest);
        }
        if self.persist(&inner).is_err() {
            inner.remove(&digest);
            return false;
        }
        true
    }

    pub async fn account(&self, call_id: &str) -> Option<String> {
        if !valid_call_id(call_id) {
            return None;
        }
        let mut inner = self.inner.lock().await;
        self.expire(&mut inner);
        inner
            .get(&self.digest(call_id))
            .map(|v| v.account_id.clone())
    }

    fn digest(&self, call_id: &str) -> String {
        blake3::keyed_hash(&self.key, call_id.as_bytes())
            .to_hex()
            .to_string()
    }

    fn expire(&self, inner: &mut HashMap<String, Binding>) {
        inner.retain(|_, v| v.created.elapsed() <= self.ttl);
    }

    fn persist(&self, inner: &HashMap<String, Binding>) -> std::io::Result<()> {
        let persisted: HashMap<_, _> = inner
            .iter()
            .map(|(digest, value)| {
                (
                    digest.clone(),
                    PersistedBinding {
                        account_id: value.account_id.clone(),
                        created_unix: value.created_unix,
                    },
                )
            })
            .collect();
        atomic_write(&self.snapshot_path, &serde_json::to_vec(&persisted)?)
    }
}

pub fn is_call_creation(path: &str) -> bool {
    path.split('?').next() == Some("/realtime/calls")
}

pub fn call_id_from_location(location: &str) -> Option<String> {
    let without_private_query = location.split_once('?').map_or(location, |(path, _)| path);
    let path = if without_private_query.starts_with('/') {
        if without_private_query.contains('#') {
            return None;
        }
        without_private_query
    } else {
        let after_scheme = without_private_query
            .strip_prefix("https://")
            .or_else(|| without_private_query.strip_prefix("http://"))?;
        let slash = after_scheme.find('/')?;
        if slash == 0 || after_scheme.contains('#') {
            return None;
        }
        &after_scheme[slash..]
    };
    let id = path.strip_prefix("/v1/realtime/calls/")?;
    if id.contains('/') {
        return None;
    }
    normalize_call_id(id)
}

pub fn sideband_call_id(path: &str) -> Result<Option<String>, InvalidCallId> {
    let (route, query) = path.split_once('?').unwrap_or((path, ""));
    let query_call_ids: Vec<_> = query
        .split('&')
        .filter_map(|pair| pair.split_once('=').filter(|(key, _)| *key == "call_id"))
        .map(|(_, value)| value)
        .collect();
    let candidate = if let Some(id) = route.strip_prefix("/live/") {
        if !query_call_ids.is_empty() {
            return Err(InvalidCallId);
        }
        Some(id)
    } else if route == "/realtime" {
        if query_call_ids.len() != 1 {
            return Err(InvalidCallId);
        }
        query_call_ids.first().copied()
    } else if route.matches('/').count() == 1 {
        if !query_call_ids.is_empty() {
            return Err(InvalidCallId);
        }
        route
            .strip_prefix('/')
            .filter(|value| value.starts_with("rtc_") || value.len() == 36)
    } else {
        None
    };
    match candidate {
        Some(id) => normalize_call_id(id).map(Some).ok_or(InvalidCallId),
        None => Ok(None),
    }
}

pub fn uses_v1_origin(path: &str) -> bool {
    let route = path.split('?').next().unwrap_or(path);
    route.starts_with("/live/") || route == "/realtime"
}

fn valid_call_id(value: &str) -> bool {
    if let Some(rest) = value.strip_prefix("rtc_") {
        return (1..=252).contains(&rest.len())
            && rest
                .bytes()
                .all(|v| v.is_ascii_alphanumeric() || matches!(v, b'.' | b'_' | b'~' | b'-'));
    }
    value.len() == 36
        && value.bytes().enumerate().all(|(i, v)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                v == b'-'
            } else {
                v.is_ascii_hexdigit()
            }
        })
}

fn normalize_call_id(value: &str) -> Option<String> {
    valid_call_id(value).then(|| {
        if value.starts_with("rtc_") {
            value.to_owned()
        } else {
            value.to_ascii_lowercase()
        }
    })
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    use std::io::Write;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|error| error.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stores_only_digest_and_routes_all_forms() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = dir.path().join("live.json");
        let store = LiveCallStore::load("secret", 2, Duration::from_secs(60), snapshot.clone());
        let id = "rtc_abcdefghijkl";
        assert!(store.bind(id, "account-a".into()).await);
        assert!(!store.bind(id, "account-b".into()).await);
        assert_eq!(store.account(id).await.as_deref(), Some("account-a"));
        assert_eq!(
            sideband_call_id(&format!("/live/{id}")).unwrap().as_deref(),
            Some(id)
        );
        assert_eq!(
            sideband_call_id(&format!("/realtime?call_id={id}"))
                .unwrap()
                .as_deref(),
            Some(id)
        );
        assert_eq!(
            sideband_call_id(&format!("/{id}")).unwrap().as_deref(),
            Some(id)
        );
        assert!(sideband_call_id("/live/nope").is_err());
        assert!(sideband_call_id(&format!("/live/{id}?call_id={id}")).is_err());
        assert!(sideband_call_id(&format!("/realtime?call_id={id}&call_id={id}")).is_err());
        drop(store);
        let reloaded = LiveCallStore::load("secret", 2, Duration::from_secs(60), snapshot);
        assert_eq!(reloaded.account(id).await.as_deref(), Some("account-a"));
    }

    #[test]
    fn extracts_location_without_private_context() {
        assert_eq!(
            call_id_from_location("https://example/v1/realtime/calls/rtc_abcdef?token=private#x")
                .as_deref(),
            Some("rtc_abcdef")
        );
        assert!(call_id_from_location("https://example/anything/rtc_abcdef").is_none());
    }
}
