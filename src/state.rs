use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::routing::Router;

#[derive(Default)]
pub struct Stats {
    pub inflight_http: AtomicUsize,
    pub open_upgrades: AtomicUsize,
    pub active_spool_bytes: AtomicUsize,
    pub refresh_inflight: AtomicUsize,
    pub refresh_scheduler_ticks: AtomicU64,
    pub refresh_accounts_checked: AtomicU64,
    pub refresh_successes: AtomicU64,
    pub refresh_failures: AtomicU64,
    pub refresh_reauth_required: AtomicU64,
    pub refresh_last_sweep_unix: AtomicU64,
    pub refresh_last_success_unix: AtomicU64,
}

#[derive(Serialize, Deserialize)]
pub struct StatsSnapshot {
    pub inflight_http: usize,
    pub open_upgrades: usize,
    pub affinity_entries: usize,
    pub affinity_bytes: usize,
    pub active_spool_bytes: usize,
    pub quota_records: usize,
    pub health_records: usize,
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
}

impl Stats {
    pub async fn write(&self, path: PathBuf, router: &Arc<Router>) -> Result<()> {
        let (affinity_entries, affinity_bytes) = router.affinity.len_and_bytes().await;
        let records = router.record_count().await;
        let snapshot = StatsSnapshot {
            inflight_http: self.inflight_http.load(Ordering::Relaxed),
            open_upgrades: self.open_upgrades.load(Ordering::Relaxed),
            affinity_entries,
            affinity_bytes,
            active_spool_bytes: self.active_spool_bytes.load(Ordering::Relaxed),
            quota_records: records,
            health_records: records,
            refresh_inflight: self.refresh_inflight.load(Ordering::Relaxed),
            refresh_scheduler_ticks: self.refresh_scheduler_ticks.load(Ordering::Relaxed),
            refresh_accounts_checked: self.refresh_accounts_checked.load(Ordering::Relaxed),
            refresh_successes: self.refresh_successes.load(Ordering::Relaxed),
            refresh_failures: self.refresh_failures.load(Ordering::Relaxed),
            refresh_reauth_required: self.refresh_reauth_required.load(Ordering::Relaxed),
            refresh_last_sweep_unix: self.refresh_last_sweep_unix.load(Ordering::Relaxed),
            refresh_last_success_unix: self.refresh_last_success_unix.load(Ordering::Relaxed),
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temp = path.with_extension("json.tmp");
        fs::write(&temp, serde_json::to_vec_pretty(&snapshot)?)?;
        fs::rename(temp, path)?;
        Ok(())
    }
}
