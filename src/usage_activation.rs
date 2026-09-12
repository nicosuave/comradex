//! Weekly-quota activation eligibility and durable, per-owner deduplication.

use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::routing::QuotaWindowStatus;

const WEEK_SECONDS: i64 = 604_800;
const RESET_TOLERANCE_SECONDS: i64 = 300;
const RETRY_SECONDS: i64 = 3_600;

/// Return the reset of the window displayed by the menu, only when it shows
/// 100% remaining and a full weekly allowance. Usage percentages are already
/// rounded by the usage parser; a rounded zero is deliberately eligible.
pub fn activation_reset(windows: &BTreeMap<String, QuotaWindowStatus>, now: i64) -> Option<i64> {
    let window = windows
        .iter()
        .filter(|(_, window)| {
            window.used_percent.is_some() && window.limit_window_seconds != Some(0)
        })
        .min_by_key(|(name, _)| (window_order(name), *name))?
        .1;
    if window.used_percent != Some(0) || window.limit_window_seconds != Some(WEEK_SECONDS as u64) {
        return None;
    }
    let reset = window.reset_at_unix?;
    is_fresh_week(now, reset).then_some(reset)
}

fn window_order(name: &str) -> u8 {
    match name.to_ascii_lowercase().as_str() {
        "primary" => 0,
        "secondary" => 1,
        "tertiary" => 2,
        _ => 3,
    }
}

fn is_fresh_week(now: i64, reset: i64) -> bool {
    now > 0 && reset.abs_diff(now.saturating_add(WEEK_SECONDS)) <= RESET_TOLERANCE_SECONDS as u64
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ActivationRecord {
    attempted_at: i64,
    reset_at: i64,
    completed_at: Option<i64>,
}

/// Owned by the daemon's single activation worker. Keys must identify quota
/// owners, not login slots or credentials. Reservation is saved before a
/// request starts, so crashes and uncertain failures retain the retry cooldown.
pub struct UsageActivationLedger {
    path: PathBuf,
    records: BTreeMap<String, ActivationRecord>,
}

impl UsageActivationLedger {
    /// A malformed or unreadable existing ledger is an error, never permission
    /// to issue duplicate activations.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let records = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).context("parse usage activation ledger")?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(error) => return Err(error).context("read usage activation ledger"),
        };
        Ok(Self { path, records })
    }

    /// Durably reserve one eligible attempt. Call only after `activation_reset`
    /// selects a window; false means a previous attempt or success suppresses it.
    pub fn reserve(&mut self, key: &str, now: i64, reset: i64) -> Result<bool> {
        if !is_fresh_week(now, reset) {
            return Ok(false);
        }
        if let Some(record) = self.records.get(key) {
            if record.completed_at.is_some() {
                // A tiny activation can still round to 100% remaining and the
                // reported reset can move when first use starts the clock.
                // Match the cycle by elapsed time, not exact reset equality.
                if now < record.reset_at.saturating_sub(RESET_TOLERANCE_SECONDS)
                    || reset <= record.reset_at.saturating_add(RESET_TOLERANCE_SECONDS)
                {
                    return Ok(false);
                }
            } else if now < record.attempted_at.saturating_add(RETRY_SECONDS) {
                return Ok(false);
            }
        }
        let mut records = self.records.clone();
        records.insert(
            key.to_owned(),
            ActivationRecord {
                attempted_at: now,
                reset_at: reset,
                completed_at: None,
            },
        );
        self.save(records)?;
        Ok(true)
    }

    /// Record a confirmed successful request. `reset` is the qualifying usage
    /// snapshot's reset; no second usage fetch is required to mark success.
    pub fn complete(&mut self, key: &str, now: i64, reset: i64) -> Result<()> {
        let mut records = self.records.clone();
        let record = records
            .get_mut(key)
            .context("activation completed without reservation")?;
        record.completed_at = Some(now);
        record.reset_at = reset;
        // Failure still propagates, but a daemon that witnessed success must
        // not forget it just because the durable write failed. A restart can
        // only recover the previous reservation and its uncertainty cooldown.
        let result = self.save(records.clone());
        self.records = records;
        result
    }

    fn save(&mut self, records: BTreeMap<String, ActivationRecord>) -> Result<()> {
        let parent = self
            .path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::create_dir_all(parent).context("create usage activation ledger directory")?;
        let mut temp = tempfile::NamedTempFile::new_in(parent)?;
        temp.write_all(&serde_json::to_vec(&records)?)?;
        temp.as_file().sync_all()?;
        temp.persist(&self.path)
            .map_err(|error| error.error)
            .context("persist usage activation ledger")?;
        self.records = records;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_800_000_000;

    fn weekly() -> QuotaWindowStatus {
        QuotaWindowStatus {
            used_percent: Some(0),
            reset_at_unix: Some(NOW + WEEK_SECONDS),
            limit_window_seconds: Some(WEEK_SECONDS as u64),
        }
    }

    #[test]
    fn matches_the_selected_menu_window_including_unknown_names() {
        let mut windows = BTreeMap::new();
        assert_eq!(activation_reset(&windows, NOW), None);
        windows.insert("secondary".into(), weekly());
        assert!(activation_reset(&windows, NOW).is_some());
        windows.insert(
            "PRIMARY".into(),
            QuotaWindowStatus {
                used_percent: Some(1),
                ..weekly()
            },
        );
        assert_eq!(activation_reset(&windows, NOW), None);
        windows.get_mut("PRIMARY").unwrap().used_percent = None;
        assert!(activation_reset(&windows, NOW).is_some());
        windows.get_mut("PRIMARY").unwrap().used_percent = Some(0);
        windows.get_mut("PRIMARY").unwrap().limit_window_seconds = Some(0);
        assert!(activation_reset(&windows, NOW).is_some());
        windows.get_mut("PRIMARY").unwrap().limit_window_seconds = None;
        assert_eq!(activation_reset(&windows, NOW), None);
        windows.clear();
        windows.insert("zeta".into(), weekly());
        windows.insert(
            "alpha".into(),
            QuotaWindowStatus {
                used_percent: Some(10),
                ..weekly()
            },
        );
        assert_eq!(activation_reset(&windows, NOW), None);
        windows.insert("tertiary".into(), weekly());
        assert!(activation_reset(&windows, NOW).is_some());
    }

    #[test]
    fn requires_weekly_duration_reset_and_rounded_full_allowance() {
        for (used, expected) in [(0.49, true), (0.5, false), (1.0, false)] {
            let body = serde_json::to_vec(&serde_json::json!({"rate_limit": {"primary_window": {
                "used_percent": used, "reset_at": NOW + WEEK_SECONDS, "limit_window_seconds": WEEK_SECONDS
            }}})).unwrap();
            let snapshot = crate::usage::parse_usage_response(&body, NOW).unwrap();
            assert_eq!(activation_reset(&snapshot.windows, NOW).is_some(), expected);
        }
        for offset in [-301, -300, 0, 300, 301] {
            let window = QuotaWindowStatus {
                reset_at_unix: Some(NOW + WEEK_SECONDS + offset),
                ..weekly()
            };
            assert_eq!(
                activation_reset(&BTreeMap::from([("primary".into(), window)]), NOW).is_some(),
                offset.abs() <= 300
            );
        }
        for window in [
            QuotaWindowStatus {
                reset_at_unix: None,
                ..weekly()
            },
            QuotaWindowStatus {
                limit_window_seconds: Some(18_000),
                ..weekly()
            },
        ] {
            assert_eq!(
                activation_reset(&BTreeMap::from([("primary".into(), window)]), NOW),
                None
            );
        }
    }

    #[test]
    fn success_survives_restart_and_reset_drift_until_next_week() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("activation.json");
        let mut ledger = UsageActivationLedger::open(&path).unwrap();
        assert!(ledger.reserve("owner", NOW, NOW + WEEK_SECONDS).unwrap());
        ledger
            .complete("owner", NOW + 2, NOW + WEEK_SECONDS)
            .unwrap();
        drop(ledger);
        let mut ledger = UsageActivationLedger::open(&path).unwrap();
        for elapsed in [3, 300, 3_600, 6 * 86_400] {
            assert!(
                !ledger
                    .reserve("owner", NOW + elapsed, NOW + elapsed + WEEK_SECONDS)
                    .unwrap()
            );
        }
        assert!(
            ledger
                .reserve("other-owner", NOW + 3, NOW + 3 + WEEK_SECONDS)
                .unwrap()
        );
        assert!(
            ledger
                .reserve("owner", NOW + WEEK_SECONDS, NOW + 2 * WEEK_SECONDS)
                .unwrap()
        );
        ledger
            .complete("owner", NOW + WEEK_SECONDS, NOW + 2 * WEEK_SECONDS)
            .unwrap();
        assert!(
            !ledger
                .reserve(
                    "owner",
                    NOW + WEEK_SECONDS + 300,
                    NOW + 2 * WEEK_SECONDS + 300
                )
                .unwrap()
        );
    }

    #[test]
    fn uncertain_attempts_back_off_across_restart_even_when_reset_moves() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("activation.json");
        let mut ledger = UsageActivationLedger::open(&path).unwrap();
        assert!(ledger.reserve("owner", NOW, NOW + WEEK_SECONDS).unwrap());
        drop(ledger);
        let mut ledger = UsageActivationLedger::open(&path).unwrap();
        assert!(
            !ledger
                .reserve(
                    "owner",
                    NOW + RETRY_SECONDS - 1,
                    NOW + WEEK_SECONDS + RETRY_SECONDS - 1
                )
                .unwrap()
        );
        assert!(
            ledger
                .reserve(
                    "owner",
                    NOW + RETRY_SECONDS,
                    NOW + WEEK_SECONDS + RETRY_SECONDS
                )
                .unwrap()
        );
    }

    #[test]
    fn persistence_errors_never_authorize_an_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("activation.json");
        let mut ledger = UsageActivationLedger::open(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert!(ledger.reserve("owner", NOW, NOW + WEEK_SECONDS).is_err());
        assert!(ledger.records.is_empty());
        let malformed = dir.path().join("malformed.json");
        fs::write(&malformed, b"invalid json").unwrap();
        assert!(UsageActivationLedger::open(malformed).is_err());
    }

    #[test]
    fn failed_completion_write_keeps_known_success_in_memory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("activation.json");
        let mut ledger = UsageActivationLedger::open(&path).unwrap();
        assert!(ledger.reserve("owner", NOW, NOW + WEEK_SECONDS).unwrap());
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert!(
            ledger
                .complete("owner", NOW + 1, NOW + WEEK_SECONDS)
                .is_err()
        );
        assert!(
            !ledger
                .reserve(
                    "owner",
                    NOW + RETRY_SECONDS,
                    NOW + RETRY_SECONDS + WEEK_SECONDS
                )
                .unwrap()
        );
    }
}
