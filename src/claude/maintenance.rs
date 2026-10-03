//! Claude quota observations, reporting metadata, and durable reset-warming reservations.
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
        reset_credits_available: None,
        observed_at_unix: now.min(i64::MAX as u64) as i64,
        windows,
    })
}

/// Fable is reporting metadata, never an account-wide routing or warming limit.
pub fn parse_fable_usage(bytes: &[u8]) -> Option<serde_json::Value> {
    let body: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let window = body.get("seven_day_fable")?;
    let utilization = window.get("utilization")?;
    utilization
        .as_f64()
        .filter(|value| value.is_finite() && *value >= 0.0 && *value <= 100.0)?;
    let reset = match window.get("resets_at") {
        Some(serde_json::Value::String(at)) => {
            chrono::DateTime::parse_from_rfc3339(at).ok()?;
            serde_json::Value::String(at.clone())
        }
        Some(serde_json::Value::Null) | None => serde_json::Value::Null,
        _ => return None,
    };
    Some(serde_json::json!({ "utilization": utilization, "resets_at": reset }))
}

/// Retain provider reporting formats separately from the shared policy snapshot.
pub fn parse_reporting_usage(bytes: &[u8]) -> Option<serde_json::Map<String, serde_json::Value>> {
    let body: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let mut fields = serde_json::Map::new();
    if let Some(fable) = parse_fable_usage(bytes) {
        fields.insert("seven_day_fable".into(), fable);
    }
    if let Some(limits) = body.get("limits").and_then(serde_json::Value::as_array) {
        let limits: Vec<_> = limits
            .iter()
            .filter_map(|limit| {
                let limit: ReportedLimit = serde_json::from_value(limit.clone()).ok()?;
                if limit.kind.is_empty() || limit.group.is_empty() {
                    return None;
                }
                if limit.kind == "weekly_scoped"
                    && limit
                        .scope
                        .as_ref()
                        .is_none_or(|scope| scope.model.is_none() && scope.surface.is_none())
                {
                    return None;
                }
                limit
                    .percent
                    .as_f64()
                    .filter(|value| value.is_finite() && *value >= 0.0 && *value <= 100.0)?;
                if let Some(at) = &limit.resets_at {
                    chrono::DateTime::parse_from_rfc3339(at).ok()?;
                }
                if let Some(scope) = &limit.scope {
                    for label in scope.model.iter().chain(scope.surface.iter()) {
                        if label.display_name.trim().is_empty() {
                            return None;
                        }
                    }
                }
                serde_json::to_value(limit).ok()
            })
            .collect();
        if !limits.is_empty() {
            fields.insert("limits".into(), serde_json::Value::Array(limits));
        }
    }
    (!fields.is_empty()).then_some(fields)
}

#[derive(Deserialize, Serialize)]
struct ReportedLimit {
    kind: String,
    group: String,
    percent: serde_json::Number,
    resets_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    severity: Option<String>,
    scope: Option<ReportedScope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_active: Option<bool>,
}

#[derive(Deserialize, Serialize)]
struct ReportedScope {
    model: Option<ReportedScopeLabel>,
    surface: Option<ReportedScopeLabel>,
}

#[derive(Deserialize, Serialize)]
struct ReportedScopeLabel {
    id: Option<String>,
    display_name: String,
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
            reset_credits_available: None,
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
    fn fable_reporting_preserves_utilization_and_optional_resets() {
        for percent in [0.0, 25.5, 99.9, 100.0] {
            for reset in [
                serde_json::json!("2026-10-03T12:34:56.789-07:00"),
                serde_json::Value::Null,
            ] {
                let payload = serde_json::json!({
                    "seven_day_fable": { "utilization": percent, "resets_at": reset, "extra": "ignored" }
                });
                assert_eq!(
                    parse_fable_usage(payload.to_string().as_bytes()),
                    Some(serde_json::json!({ "utilization": percent, "resets_at": reset }))
                );
            }
        }
        assert_eq!(
            parse_fable_usage(br#"{"seven_day_fable":{"utilization":0}}"#),
            Some(serde_json::json!({ "utilization": 0, "resets_at": null }))
        );
    }

    #[test]
    fn invalid_or_absent_fable_metadata_does_not_discard_shared_usage() {
        for window in [
            serde_json::Value::Null,
            serde_json::json!(false),
            serde_json::json!([]),
            serde_json::json!(50),
            serde_json::json!({}),
            serde_json::json!({ "utilization": null }),
            serde_json::json!({ "utilization": "50" }),
            serde_json::json!({ "utilization": -1 }),
            serde_json::json!({ "utilization": 100.1 }),
            serde_json::json!({ "utilization": 50, "resets_at": false }),
            serde_json::json!({ "utilization": 50, "resets_at": 123 }),
            serde_json::json!({ "utilization": 50, "resets_at": "not-a-date" }),
        ] {
            let bytes = serde_json::to_vec(&serde_json::json!({
                "five_hour": { "utilization": 25 }, "seven_day": null, "seven_day_fable": window
            }))
            .unwrap();
            assert!(parse_fable_usage(&bytes).is_none());
            let snapshot = parse_usage(&bytes, 100).unwrap();
            assert_eq!(snapshot.windows.len(), 2);
            assert_eq!(snapshot.windows["5h"].used_percent, Some(25));
        }
        let bytes = br#"{"five_hour":null,"seven_day":null}"#;
        assert!(parse_fable_usage(bytes).is_none());
        assert!(parse_usage(bytes, 100).is_ok());
        assert!(parse_fable_usage(b"not-json").is_none());
    }

    #[test]
    fn scoped_fable_reporting_retains_valid_entries_and_ignores_bad_metadata() {
        let valid = serde_json::json!({
            "kind":"weekly_scoped","group":"weekly","percent":99.9,
            "resets_at":"2026-10-03T12:34:56.789-07:00","severity":"normal",
            "scope":{"model":{"id":null,"display_name":"Fable"},"surface":null},"is_active":false
        });
        for (field, bad) in [
            ("percent", serde_json::json!("50")),
            ("percent", serde_json::json!(-1)),
            ("percent", serde_json::json!(101)),
            ("resets_at", serde_json::json!(123)),
            ("resets_at", serde_json::json!("invalid")),
            ("kind", serde_json::Value::Null),
            ("group", serde_json::json!(false)),
            ("scope", serde_json::json!({"model":{"display_name":null}})),
            ("scope", serde_json::json!({"model":{"display_name":""}})),
            ("scope", serde_json::Value::Null),
            ("is_active", serde_json::json!("false")),
            ("severity", serde_json::json!(123)),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = bad;
            let mut with_extra = valid.clone();
            with_extra["extra"] = serde_json::json!("must-not-leak");
            with_extra["scope"]["model"]["extra"] = serde_json::json!("must-not-leak");
            let bytes = serde_json::to_vec(&serde_json::json!({
                "five_hour":null,"seven_day":null,"limits":[invalid,with_extra]
            }))
            .unwrap();
            assert_eq!(
                parse_reporting_usage(&bytes).unwrap()["limits"],
                serde_json::json!([valid])
            );
            let shared = parse_usage(&bytes, 100).unwrap();
            assert_eq!(shared.windows.len(), 2);
            assert_eq!(shared.windows["5h"].used_percent, Some(0));
        }
        for limits in [
            serde_json::Value::Null,
            serde_json::json!({}),
            serde_json::json!([null, {}]),
        ] {
            let bytes = serde_json::to_vec(&serde_json::json!({"limits":limits})).unwrap();
            assert!(parse_reporting_usage(&bytes).is_none());
        }
        let mut without_reset = valid.clone();
        without_reset.as_object_mut().unwrap().remove("resets_at");
        let bytes = serde_json::to_vec(&serde_json::json!({"limits":[without_reset]})).unwrap();
        assert!(parse_reporting_usage(&bytes).unwrap()["limits"][0]["resets_at"].is_null());
    }

    #[test]
    fn fable_resets_and_exhaustion_do_not_authorize_or_block_shared_warming() {
        let dir = tempfile::tempdir().unwrap();
        let mut ledger = ActivationLedger::open(dir.path().join("warm.json")).unwrap();
        let fable_reset = parse_usage(
            br#"{
            "five_hour":null,"seven_day":null,
            "seven_day_fable":{"utilization":100,"resets_at":"1970-01-01T00:01:40Z"},
            "limits":[{"kind":"weekly_scoped","group":"weekly","percent":100,"resets_at":"1970-01-01T00:01:40Z","scope":{"model":{"display_name":"Fable"}}}]
        }"#,
            101,
        )
        .unwrap();
        assert_eq!(ledger.observe("grace", &fable_reset, 101).unwrap(), None);
        let shared_reset = parse_usage(
            br#"{
            "five_hour":{"utilization":100,"resets_at":"1970-01-01T00:01:40Z"},"seven_day":null,
            "seven_day_fable":{"utilization":100,"resets_at":"2100-01-01T00:00:00Z"},
            "limits":[{"kind":"weekly_scoped","group":"weekly","percent":100,"resets_at":"2100-01-01T00:00:00Z","scope":{"model":{"display_name":"Fable"}}}]
        }"#,
            101,
        )
        .unwrap();
        assert_eq!(
            ledger.observe("ada", &shared_reset, 101).unwrap(),
            Some(100)
        );
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
