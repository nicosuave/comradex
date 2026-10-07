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
/// 100% remaining and a current weekly allowance. Usage percentages are already
/// rounded by the usage parser; a rounded zero is deliberately eligible.
pub(crate) fn activation_eligibility(
    windows: &BTreeMap<String, QuotaWindowStatus>,
    now: i64,
) -> std::result::Result<i64, &'static str> {
    let window = windows
        .iter()
        .filter(|(_, window)| {
            window.used_percent.is_some() && window.limit_window_seconds != Some(0)
        })
        .min_by_key(|(name, _)| (window_order(name), *name))
        .ok_or("no_usable_window")?
        .1;
    if window.used_percent != Some(0) {
        return Err("usage_nonzero");
    }
    if window.limit_window_seconds != Some(WEEK_SECONDS as u64) {
        return Err("not_weekly");
    }
    let reset = window.reset_at_unix.ok_or("missing_reset")?;
    if !is_current_week(now, reset) {
        return Err("reset_outside_current_week");
    }
    Ok(reset)
}

fn window_order(name: &str) -> u8 {
    match name.to_ascii_lowercase().as_str() {
        "primary" => 0,
        "secondary" => 1,
        "tertiary" => 2,
        _ => 3,
    }
}

fn is_current_week(now: i64, reset: i64) -> bool {
    now > 0 && reset > now && reset <= now.saturating_add(WEEK_SECONDS + RESET_TOLERANCE_SECONDS)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ActivationRecord {
    pub(crate) attempted_at: i64,
    pub(crate) reset_at: i64,
    pub(crate) completed_at: Option<i64>,
}

/// Owned by the daemon's single activation worker. Keys must identify quota
/// owners, not login slots or credentials. Reservation is saved before a
/// request starts, so crashes and uncertain failures retain the retry cooldown.
pub struct UsageActivationLedger {
    path: PathBuf,
    records: BTreeMap<String, ActivationRecord>,
}

impl UsageActivationLedger {
    pub(crate) fn record(&self, key: &str) -> Option<&ActivationRecord> {
        self.records.get(key)
    }

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
        if self.reservation_skip_reason(key, now, reset).is_some() {
            return Ok(false);
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

    pub(crate) fn reservation_skip_reason(
        &self,
        key: &str,
        now: i64,
        reset: i64,
    ) -> Option<&'static str> {
        if !is_current_week(now, reset) {
            return Some("reset_outside_current_week");
        }
        if let Some(record) = self.records.get(key) {
            // A provider can keep moving an unused window's deadline even after
            // a completed response. Bound retries without requiring the old
            // weekly deadline to elapse.
            if now < record.attempted_at.saturating_add(RETRY_SECONDS) {
                return Some("retry_cooldown");
            }
            if record.completed_at.is_some() {
                // The provider's reset identifies the cycle, even when it changes
                // before the previous deadline. Allow small first-use timestamp drift,
                // but never make the old deadline veto a newly reported cycle.
                if reset <= record.reset_at.saturating_add(RESET_TOLERANCE_SECONDS) {
                    return Some("cycle_already_activated");
                }
            }
        }
        None
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

    fn activation_reset(windows: &BTreeMap<String, QuotaWindowStatus>, now: i64) -> Option<i64> {
        activation_eligibility(windows, now).ok()
    }

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
        for offset in [
            -WEEK_SECONDS,
            -WEEK_SECONDS + 1,
            -3600,
            -301,
            -300,
            0,
            300,
            301,
        ] {
            let window = QuotaWindowStatus {
                reset_at_unix: Some(NOW + WEEK_SECONDS + offset),
                ..weekly()
            };
            assert_eq!(
                activation_reset(&BTreeMap::from([("primary".into(), window)]), NOW).is_some(),
                offset > -WEEK_SECONDS && offset <= 300
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
    fn success_survives_restart_and_small_reset_drift() {
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
                    .reserve("owner", NOW + elapsed, NOW + WEEK_SECONDS + 300)
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
    fn captured_sq_snapshot_overrides_legacy_activation_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("activation.json");
        // Safe timing/usage fields captured on October 7, 2026. The owner key
        // is synthetic. Preserve the installed daemon's legacy ledger format.
        fs::write(&path, br#"{"owner":{"attempted_at":1790975739,"reset_at":1791580539,"completed_at":1790975741}}"#).unwrap();
        let mut ledger = UsageActivationLedger::open(&path).unwrap();
        let observed_at = 1791379753;
        let reset_at = 1791983531;
        let windows = BTreeMap::from([(
            "primary".into(),
            QuotaWindowStatus {
                used_percent: Some(0),
                reset_at_unix: Some(reset_at),
                limit_window_seconds: Some(604800),
            },
        )]);
        assert_eq!(activation_reset(&windows, observed_at), Some(reset_at));
        assert!(ledger.reserve("owner", observed_at, reset_at).unwrap());
        ledger.complete("owner", observed_at + 2, reset_at).unwrap();
        drop(ledger);
        let mut ledger = UsageActivationLedger::open(&path).unwrap();
        assert!(
            !ledger
                .reserve("owner", observed_at + 3600, reset_at)
                .unwrap()
        );
    }

    #[test]
    fn early_reset_uses_current_snapshot_without_prior_usage_observations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("activation.json");
        let mut ledger = UsageActivationLedger::open(&path).unwrap();
        assert!(ledger.reserve("owner", NOW, NOW + WEEK_SECONDS).unwrap());
        ledger
            .complete("owner", NOW + 2, NOW + WEEK_SECONDS)
            .unwrap();
        drop(ledger);

        let mut ledger = UsageActivationLedger::open(&path).unwrap();
        let now = NOW + 4 * 86_400;
        // The menu shows 100% remaining and 6d 23h. The previous recorded
        // deadline is still three days away, and no intervening polls exist.
        let reset = now + WEEK_SECONDS - 3_600;
        let window = QuotaWindowStatus {
            reset_at_unix: Some(reset),
            ..weekly()
        };
        assert_eq!(
            activation_reset(&BTreeMap::from([("primary".into(), window)]), now),
            Some(reset)
        );
        assert!(ledger.reserve("owner", now, reset).unwrap());
        ledger.complete("owner", now + 2, reset).unwrap();
        drop(ledger);
        let mut ledger = UsageActivationLedger::open(&path).unwrap();
        assert!(!ledger.reserve("owner", now + 300, reset).unwrap());
        assert!(
            !ledger
                .reserve("owner", now + 301, NOW + WEEK_SECONDS)
                .unwrap()
        );
    }

    #[test]
    fn moving_deadline_after_success_is_rate_limited_without_blocking_new_cycles() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("activation.json");
        let mut ledger = UsageActivationLedger::open(&path).unwrap();
        assert!(ledger.reserve("owner", NOW, NOW + WEEK_SECONDS).unwrap());
        ledger
            .complete("owner", NOW + 1, NOW + WEEK_SECONDS)
            .unwrap();
        drop(ledger);
        let mut ledger = UsageActivationLedger::open(&path).unwrap();
        for elapsed in [301, RETRY_SECONDS - 1] {
            assert!(
                !ledger
                    .reserve("owner", NOW + elapsed, NOW + WEEK_SECONDS + elapsed)
                    .unwrap()
            );
        }
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
                .reserve("owner", NOW + RETRY_SECONDS, NOW + WEEK_SECONDS)
                .unwrap()
        );
    }
}
