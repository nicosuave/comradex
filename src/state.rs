use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::Write,
    path::Path,
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::routing::{Router, RoutingSnapshot};

#[derive(Default)]
struct StatsWriterState {
    lock: Mutex<()>,
    next_generation: AtomicU64,
    persisted_generation: AtomicU64,
}

static STATS_WRITERS: OnceLock<Mutex<HashMap<PathBuf, Weak<StatsWriterState>>>> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CapacityStage {
    Http,
    BridgeTurn,
    UpgradeHandshake,
}

impl CapacityStage {
    fn name(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::BridgeTurn => "bridge_turn",
            Self::UpgradeHandshake => "upgrade_handshake",
        }
    }
}

#[derive(Default)]
struct CapacityStages {
    next_id: u64,
    active: HashMap<CapacityStage, BTreeMap<u64, Instant>>,
}

#[derive(Default)]
pub struct Stats {
    pub inflight_http: AtomicUsize,
    pub inflight_regular_http: AtomicUsize,
    pub active_bridge_turns: AtomicUsize,
    pub upgrade_handshakes: AtomicUsize,
    pub open_upgrades: AtomicUsize,
    pub open_bridge_sessions: AtomicUsize,
    pub active_spool_bytes: AtomicUsize,
    pub refresh_inflight: AtomicUsize,
    pub refresh_scheduler_ticks: AtomicU64,
    pub refresh_accounts_checked: AtomicU64,
    pub refresh_successes: AtomicU64,
    pub refresh_failures: AtomicU64,
    pub refresh_reauth_required: AtomicU64,
    pub refresh_last_sweep_unix: AtomicU64,
    pub refresh_last_success_unix: AtomicU64,
    pub http_capacity_limit: AtomicUsize,
    pub bridge_turn_capacity_limit: AtomicUsize,
    pub upgrade_capacity_limit: AtomicUsize,
    pub bridge_session_capacity_limit: AtomicUsize,
    pub http_admission_rejected: AtomicU64,
    pub bridge_turn_admission_rejected: AtomicU64,
    pub upgrade_admission_rejected: AtomicU64,
    pub admission_timed_out: AtomicU64,
    capacity_stages: Mutex<CapacityStages>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CapacitySnapshot {
    pub http_limit: usize,
    pub http_active: usize,
    pub http_available: usize,
    pub bridge_turn_limit: usize,
    pub bridge_turn_active: usize,
    pub bridge_turn_available: usize,
    pub upgrade_limit: usize,
    pub upgrade_active: usize,
    pub upgrade_handshakes: usize,
    pub upgrade_available: usize,
    pub open_upgrades: usize,
    pub bridge_session_limit: usize,
    pub open_bridge_sessions: usize,
    pub bridge_session_available: usize,
    pub saturated: bool,
    pub admission_rejected: u64,
    pub http_admission_rejected: u64,
    pub bridge_turn_admission_rejected: u64,
    pub upgrade_admission_rejected: u64,
    /// Admission is fail-fast, so queue depth is deliberately always zero. Rejection counters
    /// are the durable queue-pressure signal rather than an unbounded waiter count.
    pub admission_queue_depth: usize,
    pub admission_timed_out: u64,
    pub oldest_active_stage: Option<String>,
    pub oldest_active_age_millis: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatsSnapshot {
    pub inflight_http: usize,
    pub open_upgrades: usize,
    #[serde(default)]
    pub open_bridge_sessions: usize,
    pub affinity_entries: usize,
    pub affinity_bytes: usize,
    pub active_spool_bytes: usize,
    pub quota_records: usize,
    pub health_records: usize,
    #[serde(default)]
    pub routing: RoutingSnapshot,
    pub refresh_inflight: usize,
    #[serde(default)]
    pub refresh_scheduler_ticks: u64,
    #[serde(default)]
    pub refresh_accounts_checked: u64,
    #[serde(default)]
    pub refresh_successes: u64,
    #[serde(default)]
    pub refresh_failures: u64,
    #[serde(default)]
    pub refresh_reauth_required: u64,
    #[serde(default)]
    pub refresh_last_sweep_unix: u64,
    #[serde(default)]
    pub refresh_last_success_unix: u64,
    #[serde(default)]
    pub capacity: CapacitySnapshot,
}

impl Stats {
    pub fn configure_capacity(&self, http: usize, bridge_turns: usize, upgrades: usize) {
        self.http_capacity_limit.store(http, Ordering::Release);
        self.bridge_turn_capacity_limit
            .store(bridge_turns, Ordering::Release);
        self.upgrade_capacity_limit
            .store(upgrades, Ordering::Release);
    }

    pub fn configure_bridge_session_capacity(&self, bridge_sessions: usize) {
        self.bridge_session_capacity_limit
            .store(bridge_sessions, Ordering::Release);
    }

    pub fn capacity_stage_started(&self, stage: CapacityStage) -> u64 {
        let mut stages = self.capacity_stages.lock().expect("capacity stages");
        stages.next_id = stages.next_id.wrapping_add(1).max(1);
        let id = stages.next_id;
        stages
            .active
            .entry(stage)
            .or_default()
            .insert(id, Instant::now());
        id
    }

    pub fn capacity_stage_finished(&self, stage: CapacityStage, id: u64) {
        let mut stages = self.capacity_stages.lock().expect("capacity stages");
        if let Some(active) = stages.active.get_mut(&stage) {
            active.remove(&id);
            if active.is_empty() {
                stages.active.remove(&stage);
            }
        }
    }

    pub fn capacity_snapshot(&self) -> CapacitySnapshot {
        let http_limit = self.http_capacity_limit.load(Ordering::Acquire);
        let http_active = self.inflight_regular_http.load(Ordering::Acquire);
        let bridge_turn_limit = self.bridge_turn_capacity_limit.load(Ordering::Acquire);
        let bridge_turn_active = self.active_bridge_turns.load(Ordering::Acquire);
        let upgrade_limit = self.upgrade_capacity_limit.load(Ordering::Acquire);
        let upgrade_handshakes = self.upgrade_handshakes.load(Ordering::Acquire);
        let open_upgrades = self.open_upgrades.load(Ordering::Acquire);
        let upgrade_active = upgrade_handshakes.saturating_add(open_upgrades);
        let bridge_session_limit = self.bridge_session_capacity_limit.load(Ordering::Acquire);
        let open_bridge_sessions = self.open_bridge_sessions.load(Ordering::Acquire);
        let oldest = self
            .capacity_stages
            .lock()
            .expect("capacity stages")
            .active
            .iter()
            .flat_map(|(stage, entries)| entries.values().map(move |started| (*stage, *started)))
            .min_by_key(|(_, started)| *started);
        let http_admission_rejected = self.http_admission_rejected.load(Ordering::Acquire);
        let bridge_turn_admission_rejected =
            self.bridge_turn_admission_rejected.load(Ordering::Acquire);
        let upgrade_admission_rejected = self.upgrade_admission_rejected.load(Ordering::Acquire);
        let rejected =
            http_admission_rejected + bridge_turn_admission_rejected + upgrade_admission_rejected;
        CapacitySnapshot {
            http_limit,
            http_active,
            http_available: http_limit.saturating_sub(http_active),
            bridge_turn_limit,
            bridge_turn_active,
            bridge_turn_available: bridge_turn_limit.saturating_sub(bridge_turn_active),
            upgrade_limit,
            upgrade_active,
            upgrade_handshakes,
            upgrade_available: upgrade_limit.saturating_sub(upgrade_active),
            open_upgrades,
            bridge_session_limit,
            open_bridge_sessions,
            bridge_session_available: bridge_session_limit.saturating_sub(open_bridge_sessions),
            saturated: (http_limit != 0 && http_active >= http_limit)
                || (bridge_turn_limit != 0 && bridge_turn_active >= bridge_turn_limit)
                || (upgrade_limit != 0 && upgrade_active >= upgrade_limit)
                || (bridge_session_limit != 0 && open_bridge_sessions >= bridge_session_limit),
            admission_rejected: rejected,
            http_admission_rejected,
            bridge_turn_admission_rejected,
            upgrade_admission_rejected,
            admission_queue_depth: 0,
            admission_timed_out: self.admission_timed_out.load(Ordering::Acquire),
            oldest_active_stage: oldest.map(|(stage, _)| stage.name().to_owned()),
            oldest_active_age_millis: oldest.map(|(_, started)| {
                u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
            }),
        }
    }

    pub async fn snapshot(&self, router: &Router) -> StatsSnapshot {
        let (affinity_entries, affinity_bytes) = router.affinity.len_and_bytes().await;
        let records = router.record_count().await;
        let routing = router.routing_snapshot().await;
        StatsSnapshot {
            inflight_http: self.inflight_http.load(Ordering::Relaxed),
            open_upgrades: self.open_upgrades.load(Ordering::Relaxed),
            open_bridge_sessions: self.open_bridge_sessions.load(Ordering::Relaxed),
            affinity_entries,
            affinity_bytes,
            active_spool_bytes: self.active_spool_bytes.load(Ordering::Relaxed),
            quota_records: records,
            health_records: records,
            routing,
            refresh_inflight: self.refresh_inflight.load(Ordering::Relaxed),
            refresh_scheduler_ticks: self.refresh_scheduler_ticks.load(Ordering::Relaxed),
            refresh_accounts_checked: self.refresh_accounts_checked.load(Ordering::Relaxed),
            refresh_successes: self.refresh_successes.load(Ordering::Relaxed),
            refresh_failures: self.refresh_failures.load(Ordering::Relaxed),
            refresh_reauth_required: self.refresh_reauth_required.load(Ordering::Relaxed),
            refresh_last_sweep_unix: self.refresh_last_sweep_unix.load(Ordering::Relaxed),
            refresh_last_success_unix: self.refresh_last_success_unix.load(Ordering::Relaxed),
            capacity: self.capacity_snapshot(),
        }
    }

    pub async fn write(&self, path: PathBuf, router: &Arc<Router>) -> Result<()> {
        let snapshot = self.snapshot(router).await;
        let writer = stats_writer(&path)?;
        let generation = writer
            .next_generation
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        tokio::task::spawn_blocking(move || {
            write_snapshot_ordered(&path, &snapshot, generation, &writer)
        })
        .await
        .context("join stats snapshot writer")?
    }
}

fn stats_writer(path: &Path) -> Result<Arc<StatsWriterState>> {
    let mut writers = STATS_WRITERS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| anyhow::anyhow!("stats snapshot writer registry lock poisoned"))?;
    if let Some(writer) = writers.get(path).and_then(Weak::upgrade) {
        return Ok(writer);
    }
    let writer = Arc::new(StatsWriterState::default());
    writers.insert(path.to_path_buf(), Arc::downgrade(&writer));
    Ok(writer)
}

fn write_snapshot_ordered(
    path: &Path,
    snapshot: &StatsSnapshot,
    generation: u64,
    writer: &StatsWriterState,
) -> Result<()> {
    let _writer = writer
        .lock
        .lock()
        .map_err(|_| anyhow::anyhow!("stats snapshot writer lock poisoned"))?;
    if generation <= writer.persisted_generation.load(Ordering::Acquire) {
        return Ok(());
    }
    write_snapshot_atomic(path, snapshot)?;
    writer
        .persisted_generation
        .store(generation, Ordering::Release);
    Ok(())
}

fn write_snapshot_atomic(path: &Path, snapshot: &StatsSnapshot) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let bytes = serde_json::to_vec_pretty(snapshot)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    temp.write_all(&bytes)?;
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|error| error.error)?;
    sync_parent_directory(parent)?;
    Ok(())
}

fn sync_parent_directory(path: &Path) -> std::io::Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(inflight_http: usize) -> StatsSnapshot {
        StatsSnapshot {
            inflight_http,
            open_upgrades: 2,
            open_bridge_sessions: 3,
            affinity_entries: 3,
            affinity_bytes: 4,
            active_spool_bytes: 5,
            quota_records: 6,
            health_records: 6,
            routing: RoutingSnapshot::default(),
            refresh_inflight: 7,
            refresh_scheduler_ticks: 8,
            refresh_accounts_checked: 9,
            refresh_successes: 10,
            refresh_failures: 11,
            refresh_reauth_required: 12,
            refresh_last_sweep_unix: 13,
            refresh_last_success_unix: 14,
            capacity: CapacitySnapshot::default(),
        }
    }

    #[test]
    fn atomic_stats_write_replaces_complete_json_without_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stats.json");
        fs::write(&path, b"stale").unwrap();

        write_snapshot_atomic(&path, &snapshot(42)).unwrap();

        let restored: StatsSnapshot = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(restored.inflight_http, 42);
        let entries = fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(entries, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_stale_stats_writer_cannot_replace_a_newer_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stats.json");
        let writer = Arc::new(StatsWriterState::default());
        let older = writer
            .next_generation
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let newer = writer
            .next_generation
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);

        let (older_started_tx, older_started_rx) = std::sync::mpsc::channel();
        let (release_older_tx, release_older_rx) = std::sync::mpsc::channel();
        let (older_finished_tx, older_finished_rx) = std::sync::mpsc::channel();
        let older_path = path.clone();
        let older_writer = writer.clone();
        let stale_waiter = tokio::task::spawn_blocking(move || {
            older_started_tx.send(()).unwrap();
            release_older_rx.recv().unwrap();
            let result =
                write_snapshot_ordered(&older_path, &snapshot(1), older, older_writer.as_ref());
            older_finished_tx.send(()).unwrap();
            result
        });
        older_started_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        stale_waiter.abort();
        drop(stale_waiter);

        write_snapshot_ordered(&path, &snapshot(2), newer, writer.as_ref()).unwrap();
        release_older_tx.send(()).unwrap();
        older_finished_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();

        let restored: StatsSnapshot = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(restored.inflight_http, 2);
    }

    #[test]
    fn bridge_sessions_are_accounted_against_their_own_capacity() {
        let stats = Stats::default();
        stats.configure_capacity(64, 64, 32);
        stats.configure_bridge_session_capacity(256);
        stats.open_bridge_sessions.store(32, Ordering::Release);

        let snapshot = stats.capacity_snapshot();
        assert_eq!(snapshot.open_bridge_sessions, 32);
        assert_eq!(snapshot.bridge_session_available, 224);
        assert_eq!(snapshot.upgrade_active, 0);
        assert!(!snapshot.saturated);

        stats.open_upgrades.store(32, Ordering::Release);
        assert!(stats.capacity_snapshot().saturated);
        stats.open_upgrades.store(0, Ordering::Release);
        stats.open_bridge_sessions.store(256, Ordering::Release);
        assert!(stats.capacity_snapshot().saturated);
    }
}
