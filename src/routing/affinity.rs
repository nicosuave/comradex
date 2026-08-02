use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ThreadKey(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Binding {
    pub account_id: String,
    pub last_seen: u64,
    pub account_generation: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Snapshot {
    version: u8,
    bindings: HashMap<String, Binding>,
}

pub struct AffinityStore {
    inner: Mutex<Snapshot>,
    path: PathBuf,
    key: [u8; 32],
    max_entries: usize,
    max_bytes: usize,
    idle: Duration,
}

impl AffinityStore {
    pub fn load(
        path: PathBuf,
        key_text: &str,
        max_entries: usize,
        max_bytes: usize,
        idle: Duration,
    ) -> Result<Self> {
        let key = *blake3::hash(key_text.as_bytes()).as_bytes();
        let mut snapshot = match fs::read(&path) {
            Ok(bytes) if bytes.len() <= max_bytes => {
                serde_json::from_slice(&bytes).context("parse affinity snapshot")?
            }
            Ok(_) => Snapshot::default(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Snapshot::default(),
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        };
        let cutoff = now().saturating_sub(idle.as_secs());
        snapshot
            .bindings
            .retain(|_, value| value.last_seen >= cutoff);
        trim(&mut snapshot, max_entries, max_bytes);
        Ok(Self {
            inner: Mutex::new(snapshot),
            path,
            key,
            max_entries,
            max_bytes,
            idle,
        })
    }

    pub fn key(&self, raw: &str) -> ThreadKey {
        ThreadKey(
            blake3::keyed_hash(&self.key, raw.as_bytes())
                .to_hex()
                .to_string(),
        )
    }

    pub async fn get(&self, key: &ThreadKey) -> Option<Binding> {
        let mut inner = self.inner.lock().await;
        let now = now();
        let expired = inner
            .bindings
            .get(&key.0)
            .is_some_and(|v| now.saturating_sub(v.last_seen) > self.idle.as_secs());
        if expired {
            inner.bindings.remove(&key.0);
            return None;
        }
        let binding = inner.bindings.get_mut(&key.0)?;
        binding.last_seen = now;
        Some(binding.clone())
    }

    pub async fn put(&self, key: ThreadKey, account_id: String, generation: u64) {
        let mut inner = self.inner.lock().await;
        inner.bindings.insert(
            key.0,
            Binding {
                account_id,
                last_seen: now(),
                account_generation: generation,
            },
        );
        trim(&mut inner, self.max_entries, self.max_bytes);
    }

    pub async fn remove(&self, key: &ThreadKey) {
        self.inner.lock().await.bindings.remove(&key.0);
    }

    pub async fn invalidate_account(&self, account: &str) {
        self.inner
            .lock()
            .await
            .bindings
            .retain(|_, v| v.account_id != account);
    }

    pub async fn len_and_bytes(&self) -> (usize, usize) {
        let inner = self.inner.lock().await;
        (
            inner.bindings.len(),
            serde_json::to_vec(&*inner).map_or(0, |v| v.len()),
        )
    }

    pub async fn flush(&self) -> Result<()> {
        let bytes = {
            let mut inner = self.inner.lock().await;
            trim(&mut inner, self.max_entries, self.max_bytes);
            serde_json::to_vec(&*inner)?
        };
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temp = self.path.with_extension("json.tmp");
        fs::write(&temp, bytes)?;
        fs::rename(&temp, &self.path)?;
        Ok(())
    }
}

fn trim(snapshot: &mut Snapshot, max_entries: usize, max_bytes: usize) {
    while snapshot.bindings.len() > max_entries
        || serde_json::to_vec(snapshot).is_ok_and(|v| v.len() > max_bytes)
    {
        let Some(oldest) = snapshot
            .bindings
            .iter()
            .min_by_key(|(_, v)| v.last_seen)
            .map(|(k, _)| k.clone())
        else {
            break;
        };
        snapshot.bindings.remove(&oldest);
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn persists_hashes_only_and_enforces_count_cap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("affinity.json");
        let store = AffinityStore::load(
            path.clone(),
            "0123456789abcdef0123456789abcdef",
            3,
            10_000,
            Duration::from_secs(60),
        )
        .unwrap();
        for raw in ["raw-thread-one", "raw-thread-two", "raw-thread-three"] {
            store.put(store.key(raw), "account".into(), 0).await;
        }
        store.flush().await.unwrap();
        let bytes = fs::read(&path).unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("raw-thread"));
        assert_eq!(store.len_and_bytes().await.0, 3);
        let loaded = AffinityStore::load(
            path,
            "0123456789abcdef0123456789abcdef",
            2,
            10_000,
            Duration::from_secs(60),
        )
        .unwrap();
        assert_eq!(loaded.len_and_bytes().await.0, 2);
    }
}
