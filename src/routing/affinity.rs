use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

const ACCOUNT_EPOCH_STORAGE_BUDGET: usize = 1024 * 1024;
const MAX_TOUCH_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ThreadKey(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Binding {
    pub account_id: String,
    pub last_seen: u64,
    pub account_generation: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Snapshot {
    version: u8,
    bindings: HashMap<String, Binding>,
    #[serde(default)]
    account_epochs: HashMap<String, u64>,
}

#[derive(Debug, Default)]
struct StoreState {
    snapshot: Snapshot,
    binding_bytes: usize,
    generation: u64,
    persisted_generation: u64,
    touch_generation: u64,
    persisted_touch_generation: u64,
    touch_flush_due_at: u64,
}

pub struct AffinityStore {
    inner: Arc<Mutex<StoreState>>,
    // This lock is deliberately owned by the blocking writer. If an async caller is cancelled,
    // the detached blocking task still finishes before a newer generation can replace its file.
    flush_lock: Arc<StdMutex<()>>,
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
        let (mut snapshot, mut dirty) = match fs::read(&path) {
            Ok(bytes) if bytes.len() <= max_bytes.saturating_add(ACCOUNT_EPOCH_STORAGE_BUDGET) => (
                serde_json::from_slice(&bytes).context("parse affinity snapshot")?,
                false,
            ),
            Ok(_) => anyhow::bail!("affinity snapshot exceeds the hard storage limit"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (Snapshot::default(), false),
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        };
        let cutoff = now().saturating_sub(idle.as_secs());
        let before_expiry = snapshot.bindings.len();
        snapshot
            .bindings
            .retain(|_, value| value.last_seen >= cutoff);
        dirty |= snapshot.bindings.len() != before_expiry;
        dirty |= trim_stale_account_epochs(&mut snapshot, "");
        if account_epoch_storage_bytes(&snapshot) > ACCOUNT_EPOCH_STORAGE_BUDGET {
            anyhow::bail!("account epoch snapshot exceeds its hard storage budget")
        }
        let mut binding_bytes = binding_storage_bytes(&snapshot);
        dirty |= trim(&mut snapshot, &mut binding_bytes, max_entries, max_bytes);
        Ok(Self {
            inner: Arc::new(Mutex::new(StoreState {
                snapshot,
                binding_bytes,
                generation: u64::from(dirty),
                persisted_generation: 0,
                touch_generation: 0,
                persisted_touch_generation: 0,
                touch_flush_due_at: 0,
            })),
            flush_lock: Arc::new(StdMutex::new(())),
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
            .snapshot
            .bindings
            .get(&key.0)
            .is_some_and(|v| now.saturating_sub(v.last_seen) > self.idle.as_secs());
        if expired {
            inner.snapshot.bindings.remove(&key.0);
            mark_dirty(&mut inner);
            return None;
        }
        let changed = inner
            .snapshot
            .bindings
            .get(&key.0)
            .is_some_and(|binding| binding.last_seen != now);
        let previous_bytes = inner.snapshot.bindings.get(&key.0).map_or(0, json_len);
        let binding = inner.snapshot.bindings.get_mut(&key.0)?;
        binding.last_seen = now;
        let binding = binding.clone();
        if changed {
            inner.binding_bytes = inner
                .binding_bytes
                .saturating_sub(previous_bytes)
                .saturating_add(json_len(&binding));
            mark_touched(&mut inner, now, touch_checkpoint_interval(self.idle));
        }
        Some(binding)
    }

    pub async fn put(&self, key: ThreadKey, account_id: String, generation: u64) {
        let mut inner = self.inner.lock().await;
        let binding = Binding {
            account_id,
            last_seen: now(),
            account_generation: generation,
        };
        insert_binding(&mut inner, key.0, binding);
        let StoreState {
            snapshot,
            binding_bytes,
            ..
        } = &mut *inner;
        trim(snapshot, binding_bytes, self.max_entries, self.max_bytes);
        mark_dirty(&mut inner);
    }

    pub async fn account_epoch(&self, account: &str) -> u64 {
        self.inner
            .lock()
            .await
            .snapshot
            .account_epochs
            .get(account)
            .copied()
            .unwrap_or(0)
    }

    pub async fn remove(&self, key: &ThreadKey) {
        let mut inner = self.inner.lock().await;
        if remove_binding(&mut inner, &key.0).is_some() {
            mark_dirty(&mut inner);
        }
    }

    pub async fn invalidate_account(&self, account: &str) {
        let mut inner = self.inner.lock().await;
        let removed = inner
            .snapshot
            .bindings
            .iter()
            .filter(|(_, binding)| binding.account_id == account)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in removed {
            remove_binding(&mut inner, &key);
        }
        let epoch = inner
            .snapshot
            .account_epochs
            .entry(account.to_owned())
            .or_default();
        *epoch = epoch.saturating_add(1);
        trim_stale_account_epochs(&mut inner.snapshot, account);
        mark_dirty(&mut inner);
    }

    pub async fn len_and_bytes(&self) -> (usize, usize) {
        let inner = self.inner.lock().await;
        (
            inner.snapshot.bindings.len(),
            snapshot_storage_bytes(&inner.snapshot, inner.binding_bytes),
        )
    }

    pub async fn flush(&self) -> Result<()> {
        let inner = self.inner.clone();
        let flush_lock = self.flush_lock.clone();
        let path = self.path.clone();
        let max_entries = self.max_entries;
        let max_bytes = self.max_bytes;
        let touch_checkpoint_interval = touch_checkpoint_interval(self.idle);
        tokio::task::spawn_blocking(move || -> Result<()> {
            let _flush = flush_lock
                .lock()
                .map_err(|_| anyhow::anyhow!("affinity snapshot writer lock poisoned"))?;
            let current_now = now();
            let (generation, touch_generation, snapshot) = {
                let mut state = inner.blocking_lock();
                let StoreState {
                    snapshot,
                    binding_bytes,
                    ..
                } = &mut *state;
                if trim(snapshot, binding_bytes, max_entries, max_bytes) {
                    mark_dirty(&mut state);
                }
                if account_epoch_storage_bytes(&state.snapshot) > ACCOUNT_EPOCH_STORAGE_BUDGET {
                    anyhow::bail!("account epoch snapshot exceeds its hard storage budget")
                }
                let structural_dirty = state.persisted_generation < state.generation;
                let touches_due = state.persisted_touch_generation < state.touch_generation
                    && current_now >= state.touch_flush_due_at;
                if !structural_dirty && !touches_due {
                    return Ok(());
                }
                if touches_due && !structural_dirty {
                    mark_dirty(&mut state);
                }
                (
                    state.generation,
                    state.touch_generation,
                    state.snapshot.clone(),
                )
            };
            let bytes = serde_json::to_vec(&snapshot)?;
            let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
            fs::create_dir_all(parent)?;
            let mut temp = tempfile::NamedTempFile::new_in(parent)?;
            temp.write_all(&bytes)?;
            temp.as_file().sync_all()?;
            temp.persist(&path).map_err(|error| error.error)?;
            sync_parent_directory(parent)?;

            let mut state = inner.blocking_lock();
            state.persisted_generation = state.persisted_generation.max(generation);
            state.persisted_touch_generation =
                state.persisted_touch_generation.max(touch_generation);
            if state.persisted_touch_generation == state.touch_generation {
                state.touch_flush_due_at = 0;
            } else {
                state.touch_flush_due_at =
                    current_now.saturating_add(touch_checkpoint_interval.as_secs());
            }
            Ok(())
        })
        .await
        .context("join affinity snapshot writer")??;
        Ok(())
    }
}

fn mark_dirty(state: &mut StoreState) {
    state.generation = state.generation.saturating_add(1);
}

fn mark_touched(state: &mut StoreState, now: u64, checkpoint_interval: Duration) {
    let was_clean = state.touch_generation == state.persisted_touch_generation;
    state.touch_generation = state.touch_generation.saturating_add(1);
    if was_clean {
        state.touch_flush_due_at = now.saturating_add(checkpoint_interval.as_secs());
    }
}

fn touch_checkpoint_interval(idle: Duration) -> Duration {
    let quarter_idle = Duration::from_secs(idle.as_secs().saturating_div(4).max(1));
    quarter_idle.min(MAX_TOUCH_CHECKPOINT_INTERVAL)
}

fn sync_parent_directory(path: &std::path::Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

fn trim(
    snapshot: &mut Snapshot,
    binding_bytes: &mut usize,
    max_entries: usize,
    max_bytes: usize,
) -> bool {
    let mut changed = false;
    while snapshot.bindings.len() > max_entries || *binding_bytes > max_bytes {
        let Some(oldest) = snapshot
            .bindings
            .iter()
            .min_by_key(|(_, v)| v.last_seen)
            .map(|(k, _)| k.clone())
        else {
            break;
        };
        remove_snapshot_binding(snapshot, binding_bytes, &oldest);
        changed = true;
    }
    changed
}

fn binding_storage_bytes(snapshot: &Snapshot) -> usize {
    serde_json::to_vec(&snapshot.bindings).map_or(usize::MAX, |bytes| bytes.len())
}

fn snapshot_storage_bytes(snapshot: &Snapshot, binding_bytes: usize) -> usize {
    // Exact serde_json struct overhead around the two serialized maps and the u8 version.
    42usize
        .saturating_add(snapshot.version.to_string().len())
        .saturating_add(binding_bytes)
        .saturating_add(account_epoch_storage_bytes(snapshot))
}

fn json_len(value: &impl Serialize) -> usize {
    serde_json::to_vec(value).map_or(usize::MAX, |bytes| bytes.len())
}

fn binding_entry_bytes(key: &str, binding: &Binding, has_other_entries: bool) -> usize {
    json_len(&key)
        .saturating_add(1)
        .saturating_add(json_len(binding))
        .saturating_add(usize::from(has_other_entries))
}

fn insert_binding(state: &mut StoreState, key: String, binding: Binding) {
    if let Some(previous) = state.snapshot.bindings.insert(key.clone(), binding.clone()) {
        state.binding_bytes = state
            .binding_bytes
            .saturating_sub(json_len(&previous))
            .saturating_add(json_len(&binding));
    } else {
        let has_other_entries = state.snapshot.bindings.len() > 1;
        state.binding_bytes = state.binding_bytes.saturating_add(binding_entry_bytes(
            &key,
            &binding,
            has_other_entries,
        ));
    }
}

fn remove_binding(state: &mut StoreState, key: &str) -> Option<Binding> {
    remove_snapshot_binding(&mut state.snapshot, &mut state.binding_bytes, key)
}

fn remove_snapshot_binding(
    snapshot: &mut Snapshot,
    binding_bytes: &mut usize,
    key: &str,
) -> Option<Binding> {
    let had_other_entries = snapshot.bindings.len() > 1;
    let binding = snapshot.bindings.remove(key)?;
    *binding_bytes =
        binding_bytes.saturating_sub(binding_entry_bytes(key, &binding, had_other_entries));
    Some(binding)
}

fn account_epoch_storage_bytes(snapshot: &Snapshot) -> usize {
    serde_json::to_vec(&snapshot.account_epochs).map_or(usize::MAX, |bytes| bytes.len())
}

fn trim_stale_account_epochs(snapshot: &mut Snapshot, preserve: &str) -> bool {
    let mut changed = false;
    while account_epoch_storage_bytes(snapshot) > ACCOUNT_EPOCH_STORAGE_BUDGET {
        let removable = snapshot
            .account_epochs
            .keys()
            .find(|account| {
                account.as_str() != preserve
                    && !snapshot
                        .bindings
                        .values()
                        .any(|binding| binding.account_id == account.as_str())
            })
            .cloned();
        let Some(removable) = removable else {
            break;
        };
        snapshot.account_epochs.remove(&removable);
        changed = true;
    }
    changed
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
    use std::sync::Arc;

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

    #[tokio::test]
    async fn concurrent_flushes_preserve_the_latest_generation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("affinity.json");
        let store = Arc::new(
            AffinityStore::load(
                path.clone(),
                "0123456789abcdef0123456789abcdef",
                100,
                100_000,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let mut tasks = Vec::new();
        for index in 0..32 {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                let key = store.key(&format!("thread-{index}"));
                store.put(key, format!("account-{index}"), 0).await;
                store.flush().await.unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        store.flush().await.unwrap();

        let loaded = AffinityStore::load(
            path,
            "0123456789abcdef0123456789abcdef",
            100,
            100_000,
            Duration::from_secs(60),
        )
        .unwrap();
        assert_eq!(loaded.len_and_bytes().await.0, 32);
    }

    #[tokio::test]
    async fn mutation_after_flush_remains_dirty_until_next_flush() {
        let dir = tempfile::tempdir().unwrap();
        let store = AffinityStore::load(
            dir.path().join("affinity.json"),
            "0123456789abcdef0123456789abcdef",
            10,
            100_000,
            Duration::from_secs(60),
        )
        .unwrap();
        store.put(store.key("first"), "a".into(), 0).await;
        store.flush().await.unwrap();
        {
            let inner = store.inner.lock().await;
            assert_eq!(inner.persisted_generation, inner.generation);
        }
        store.put(store.key("second"), "b".into(), 0).await;
        {
            let inner = store.inner.lock().await;
            assert!(inner.generation > inner.persisted_generation);
        }
        store.flush().await.unwrap();
        let inner = store.inner.lock().await;
        assert_eq!(inner.persisted_generation, inner.generation);
    }

    #[tokio::test]
    async fn last_seen_churn_is_coalesced_until_the_touch_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("affinity.json");
        let store = AffinityStore::load(
            path.clone(),
            "0123456789abcdef0123456789abcdef",
            10,
            100_000,
            Duration::from_secs(60 * 60),
        )
        .unwrap();
        let key = store.key("hot-thread");
        store.put(key.clone(), "a".into(), 0).await;
        store.flush().await.unwrap();
        let persisted = fs::read(&path).unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(store.get(&key).await.is_some());
        {
            let state = store.inner.lock().await;
            assert_eq!(state.persisted_generation, state.generation);
            assert!(state.touch_generation > state.persisted_touch_generation);
            assert!(state.touch_flush_due_at > now());
        }

        store.flush().await.unwrap();
        assert_eq!(fs::read(&path).unwrap(), persisted);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)] // The test intentionally pins the blocking writer lock across cancellation.
    async fn cancelled_flush_cannot_overwrite_a_newer_generation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("affinity.json");
        let store = Arc::new(
            AffinityStore::load(
                path.clone(),
                "0123456789abcdef0123456789abcdef",
                10,
                100_000,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        store.put(store.key("first"), "a".into(), 0).await;

        let writer_lock = store.flush_lock.clone();
        let guard = writer_lock.lock().unwrap();
        let cancelled_store = store.clone();
        let cancelled = tokio::spawn(async move { cancelled_store.flush().await });
        tokio::task::yield_now().await;
        cancelled.abort();
        store.put(store.key("second"), "b".into(), 0).await;
        drop(guard);

        store.flush().await.unwrap();
        let restored = AffinityStore::load(
            path,
            "0123456789abcdef0123456789abcdef",
            10,
            100_000,
            Duration::from_secs(60),
        )
        .unwrap();
        assert_eq!(restored.len_and_bytes().await.0, 2);
    }

    #[tokio::test]
    async fn atomic_flush_leaves_only_the_complete_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("affinity.json");
        fs::write(&path, b"old contents").unwrap();
        let store = AffinityStore::load(
            dir.path().join("new-affinity.json"),
            "0123456789abcdef0123456789abcdef",
            10,
            100_000,
            Duration::from_secs(60),
        )
        .unwrap();
        store.put(store.key("thread"), "account".into(), 0).await;
        store.flush().await.unwrap();

        let entries = fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert!(entries.contains(&"new-affinity.json".into()));
        assert_eq!(entries.len(), 2);
        let snapshot: Snapshot =
            serde_json::from_slice(&fs::read(dir.path().join("new-affinity.json")).unwrap())
                .unwrap();
        assert_eq!(snapshot.bindings.len(), 1);
    }

    #[tokio::test]
    async fn cached_snapshot_size_stays_exact_across_mutations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("affinity.json");
        let store = AffinityStore::load(
            path.clone(),
            "0123456789abcdef0123456789abcdef",
            10_000,
            10_000_000,
            Duration::from_secs(60),
        )
        .unwrap();
        for index in 0..2_000 {
            store
                .put(
                    store.key(&format!("thread-{index}")),
                    format!("account-{}", index % 3),
                    index % 5,
                )
                .await;
        }
        store.remove(&store.key("thread-10")).await;
        store.invalidate_account("account-2").await;
        store.flush().await.unwrap();

        let (_, cached_bytes) = store.len_and_bytes().await;
        assert_eq!(cached_bytes, fs::read(path).unwrap().len());
    }

    #[tokio::test]
    async fn account_epochs_survive_restart_with_invalidation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("affinity.json");
        let store = AffinityStore::load(
            path.clone(),
            "0123456789abcdef0123456789abcdef",
            100,
            100_000,
            Duration::from_secs(60),
        )
        .unwrap();
        let old = store.key("old");
        let new = store.key("new");
        store.put(old.clone(), "a".into(), 0).await;
        store.flush().await.unwrap();
        store.invalidate_account("a").await;
        assert_eq!(store.account_epoch("a").await, 1);
        store.put(new.clone(), "a".into(), 1).await;
        store.flush().await.unwrap();
        drop(store);

        let restored = AffinityStore::load(
            path,
            "0123456789abcdef0123456789abcdef",
            100,
            100_000,
            Duration::from_secs(60),
        )
        .unwrap();
        assert!(restored.get(&old).await.is_none());
        assert_eq!(restored.account_epoch("a").await, 1);
        assert_eq!(restored.get(&new).await.unwrap().account_generation, 1);
    }

    #[tokio::test]
    async fn epoch_storage_is_outside_the_binding_byte_budget() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("affinity.json");
        let store = AffinityStore::load(
            path.clone(),
            "0123456789abcdef0123456789abcdef",
            1,
            2,
            Duration::from_secs(60),
        )
        .unwrap();
        store.invalidate_account("account-with-an-epoch").await;
        store.flush().await.unwrap();
        drop(store);

        let restored = AffinityStore::load(
            path,
            "0123456789abcdef0123456789abcdef",
            1,
            2,
            Duration::from_secs(60),
        )
        .unwrap();
        assert_eq!(restored.account_epoch("account-with-an-epoch").await, 1);
    }
}
