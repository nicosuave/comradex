use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
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
            refresh_inflight: 0,
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
