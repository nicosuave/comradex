//! Authoritative Claude quota observations and durable reset-warming reservations.
use crate::{routing::QuotaWindowStatus, usage::UsageSnapshot};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs, io::Write, path::PathBuf};

pub fn parse_usage(bytes: &[u8], now: u64) -> Result<UsageSnapshot> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| anyhow::anyhow!("invalid Claude usage JSON"))?;
    let mut windows = BTreeMap::new();
    for (field, name, seconds) in [("five_hour", "5h", 18000), ("seven_day", "7d", 604800)] {
        let value = value
            .get(field)
            .context("Claude usage omitted a shared window")?;
        let (used, reset) = if value.is_null() {
            (0, None)
        } else {
            let used = value["utilization"]
                .as_f64()
                .filter(|v| v.is_finite() && *v >= 0.0 && *v <= 100.0)
                .context("invalid Claude utilization")?;
            let reset = match value.get("resets_at") {
                Some(serde_json::Value::String(at)) => Some(
                    chrono::DateTime::parse_from_rfc3339(at)
                        .context("invalid Claude reset")?
                        .timestamp(),
                ),
                Some(serde_json::Value::Null) | None => None,
                _ => anyhow::bail!("invalid Claude reset"),
            };
            (used.floor() as u8, reset)
        };
        windows.insert(
            name.into(),
            QuotaWindowStatus {
                used_percent: Some(used),
                reset_at_unix: reset,
                limit_window_seconds: Some(seconds),
            },
        );
    }
    Ok(UsageSnapshot {
        observed_at_unix: now.min(i64::MAX as u64) as i64,
        windows,
    })
}

#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
struct Record {
    resets: BTreeMap<String, i64>,
    completed: BTreeMap<String, i64>,
    pending: BTreeMap<String, i64>,
    attempted_at: Option<i64>,
}

pub struct ActivationLedger {
    path: PathBuf,
    records: BTreeMap<String, Record>,
}
impl ActivationLedger {
    pub fn open(path: PathBuf) -> Result<Self> {
        let records = match fs::read(&path) {
            Ok(bytes) => {
                serde_json::from_slice(&bytes).context("invalid Claude activation ledger")?
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self { path, records })
    }

    /// Save reset observations even while warming is disabled. Only authoritative
    /// shared windows can authorize warming; model/overage counters cannot.
    pub fn observe(
        &mut self,
        key: &str,
        snapshot: &UsageSnapshot,
        now: i64,
    ) -> Result<Option<i64>> {
        let mut record = self.records.get(key).cloned().unwrap_or_default();
        for (name, seconds) in [("5h", 18000), ("7d", 604800)] {
            let window = snapshot
                .windows
                .get(name)
                .context("missing Claude shared window")?;
            let previous = record.resets.get(name).copied();
            let mut due = previous.filter(|at| *at <= now);
            if let Some(reset) = window.reset_at_unix {
                if reset <= now {
                    due = Some(due.unwrap_or(i64::MIN).max(reset));
                }
                if previous.is_none()
                    && window.used_percent == Some(0)
                    && reset.abs_diff(now + seconds) <= 300
                {
                    due = Some(due.unwrap_or(i64::MIN).max(reset - seconds));
                }
                record.resets.insert(name.into(), reset);
            }
            let foreground = window.used_percent.is_some_and(|p| p > 0)
                && window.reset_at_unix.is_none_or(|at| at > now);
            if foreground && window.used_percent.is_some_and(|p| p < 100) {
                if record.pending.remove(name).is_some()
                    || due.is_some()
                    || !record.completed.contains_key(name)
                {
                    record.completed.insert(name.into(), now);
                }
            } else if let Some(cycle) =
                due.filter(|cycle| record.completed.get(name).is_none_or(|done| cycle > done))
            {
                record.pending.insert(name.into(), cycle);
            }
        }
        let blocked = snapshot.windows.values().any(|w| {
            w.used_percent.is_none_or(|p| p >= 100) && w.reset_at_unix.is_none_or(|at| at > now)
        });
        let eligible = record.pending.values().copied().max().filter(|_| {
            !blocked
                && record
                    .attempted_at
                    .is_none_or(|at| now >= at.saturating_add(3600))
        });
        if self.records.get(key) != Some(&record) {
            let mut records = self.records.clone();
            records.insert(key.into(), record);
            self.save(records)?;
        }
        Ok(eligible)
    }
    pub fn reserve(&mut self, key: &str, now: i64) -> Result<()> {
        let mut records = self.records.clone();
        let record = records
            .get_mut(key)
            .context("activation needs observation")?;
        ensure!(
            record
                .attempted_at
                .is_none_or(|at| now >= at.saturating_add(3600)),
            "activation already reserved"
        );
        record.attempted_at = Some(now);
        self.save(records)
    }
    pub fn complete(&mut self, key: &str, now: i64) -> Result<()> {
        let mut records = self.records.clone();
        let record = records
            .get_mut(key)
            .context("activation needs reservation")?;
        for name in ["5h", "7d"] {
            record.completed.insert(name.into(), now);
        }
        record.pending.clear();
        record.attempted_at = None;
        let result = self.save(records.clone());
        self.records = records;
        result
    }
    fn save(&mut self, records: BTreeMap<String, Record>) -> Result<()> {
        let parent = self
            .path
            .parent()
            .context("activation ledger has no parent")?;
        fs::create_dir_all(parent)?;
        let mut file = tempfile::NamedTempFile::new_in(parent)?;
        file.write_all(&serde_json::to_vec(&records)?)?;
        file.as_file().sync_all()?;
        file.persist(&self.path).map_err(|e| e.error)?;
        self.records = records;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn usage(now: i64, reset: i64, used: u8) -> UsageSnapshot {
        UsageSnapshot {
            observed_at_unix: now,
            windows: BTreeMap::from([
                (
                    "5h".into(),
                    QuotaWindowStatus {
                        used_percent: Some(used),
                        reset_at_unix: Some(reset),
                        limit_window_seconds: Some(18000),
                    },
                ),
                (
                    "7d".into(),
                    QuotaWindowStatus {
                        used_percent: Some(10),
                        reset_at_unix: Some(now + 500000),
                        limit_window_seconds: Some(604800),
                    },
                ),
            ]),
        }
    }
    #[test]
    fn claude_usage_validates_units_and_shared_windows() {
        let snapshot=parse_usage(br#"{"five_hour":{"utilization":99.9,"resets_at":"2026-09-25T20:00:00Z"},"seven_day":null,"seven_day_opus":{"utilization":100}}"#,100).unwrap();
        // Routing treats 100 as confirmed exhaustion, so remaining allowance never rounds up to it.
        assert_eq!(snapshot.windows["5h"].used_percent, Some(99));
        assert_eq!(snapshot.windows["7d"].used_percent, Some(0));
        assert_eq!(snapshot.windows.len(), 2);
        for bytes in [
            br#"{}"#.as_slice(),
            br#"{"five_hour":{"utilization":-1},"seven_day":null}"#,
            br#"{"five_hour":{"utilization":1000},"seven_day":null}"#,
        ] {
            assert!(parse_usage(bytes, 100).is_err());
        }
    }
    #[test]
    fn claude_warming_waits_for_reset_and_deduplicates_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("warm.json");
        let mut ledger = ActivationLedger::open(path.clone()).unwrap();
        assert_eq!(
            ledger.observe("grace", &usage(100, 200, 100), 100).unwrap(),
            None
        );
        let due = ledger
            .observe("grace", &usage(200, 200, 100), 200)
            .unwrap()
            .unwrap();
        ledger.reserve("grace", 200).unwrap();
        drop(ledger);
        let mut ledger = ActivationLedger::open(path).unwrap();
        assert_eq!(
            ledger.observe("grace", &usage(201, 200, 100), 201).unwrap(),
            None
        );
        ledger.complete("grace", due).unwrap();
        assert_eq!(
            ledger
                .observe("grace", &usage(4000, 200, 100), 4000)
                .unwrap(),
            None
        );
        assert_eq!(
            ledger
                .observe("grace", &usage(4000, 18000, 10), 4000)
                .unwrap(),
            None
        );
        assert_eq!(
            ledger
                .observe("grace", &usage(18000, 18000, 100), 18000)
                .unwrap(),
            Some(18000)
        );
    }
    #[test]
    fn claude_foreground_use_and_remaining_weekly_limits_suppress_warming() {
        let dir = tempfile::tempdir().unwrap();
        let mut ledger = ActivationLedger::open(dir.path().join("warm.json")).unwrap();
        ledger.observe("ada", &usage(100, 200, 100), 100).unwrap();
        assert_eq!(
            ledger.observe("ada", &usage(201, 18200, 1), 201).unwrap(),
            None
        );
        let mut snapshot = usage(200, 200, 100);
        snapshot.windows.get_mut("7d").unwrap().used_percent = Some(100);
        assert_eq!(ledger.observe("grace", &snapshot, 200).unwrap(), None);
        snapshot.windows.get_mut("7d").unwrap().used_percent = Some(10);
        assert_eq!(ledger.observe("grace", &snapshot, 200).unwrap(), Some(200));
        std::fs::remove_file(dir.path().join("warm.json")).unwrap();
        std::fs::create_dir(dir.path().join("warm.json")).unwrap();
        assert!(ledger.reserve("grace", 200).is_err());
    }
}
