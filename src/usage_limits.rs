//! One typed, labeled list of every usage limit an account reports.
//!
//! Routing reads `usage_windows`, which holds only the shared account-wide windows. Clients
//! that display usage read this list instead, so each number arrives with the limit it
//! belongs to: the 5-hour session, the weekly limit across all models, or a weekly limit
//! scoped to one model or surface (such as Fable). Model-scoped limits never affect routing.
use crate::routing::QuotaWindowStatus;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const WEEK_SECONDS: u64 = 604_800;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageLimit {
    /// Stable identifier, such as `5h`, `7d`, or `7d:model:fable`.
    pub id: String,
    /// Display name, such as "5-hour", "Weekly", or "Fable weekly".
    pub label: String,
    /// Compact display name, such as "5h", "7d", or "Fable".
    pub short_label: String,
    pub scope: UsageLimitScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_seconds: Option<u64>,
    pub used_percent: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_at_unix: Option<i64>,
    /// Whether exhausting this limit blocks the whole account. Only shared windows do.
    pub affects_routing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UsageLimitScope {
    /// Applies to all usage on the account.
    Account,
    /// Applies only to one model, such as Fable.
    Model { name: String },
    /// Applies only to one product surface.
    Surface { name: String },
}

/// Shared windows first, shortest first, then scoped limits in the provider's order.
pub fn build(
    windows: &BTreeMap<String, QuotaWindowStatus>,
    reporting: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Vec<UsageLimit> {
    let mut limits = shared(windows);
    limits.extend(reporting.map(scoped).unwrap_or_default());
    limits
}

fn shared(windows: &BTreeMap<String, QuotaWindowStatus>) -> Vec<UsageLimit> {
    let mut limits: Vec<_> = windows
        .iter()
        // Zero-length windows are placeholders for limits the plan does not have.
        .filter(|(_, window)| window.limit_window_seconds != Some(0))
        .filter_map(|(key, window)| {
            let (label, short_label) = duration_labels(window.limit_window_seconds);
            Some(UsageLimit {
                id: key.clone(),
                label,
                short_label,
                scope: UsageLimitScope::Account,
                window_seconds: window.limit_window_seconds,
                used_percent: window.used_percent?.min(100),
                resets_at_unix: window.reset_at_unix,
                affects_routing: true,
            })
        })
        .collect();
    limits.sort_by(|a, b| {
        a.window_seconds
            .unwrap_or(u64::MAX)
            .cmp(&b.window_seconds.unwrap_or(u64::MAX))
            .then_with(|| a.id.cmp(&b.id))
    });
    limits
}

/// Reads both Claude reporting formats: the `limits` array, and the older
/// `seven_day_fable` field when the array does not already list Fable.
fn scoped(reporting: &serde_json::Map<String, serde_json::Value>) -> Vec<UsageLimit> {
    let mut limits: Vec<UsageLimit> = reporting
        .get("limits")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(scoped_limit)
        .collect();
    let has_fable = limits.iter().any(|limit| {
        matches!(&limit.scope, UsageLimitScope::Model { name } if name.eq_ignore_ascii_case("fable"))
    });
    if !has_fable
        && let Some(fable) = reporting.get("seven_day_fable")
        && let Some(used) = percent(&fable["utilization"])
    {
        limits.push(scoped_entry(
            UsageLimitScope::Model {
                name: "Fable".into(),
            },
            used,
            reset(&fable["resets_at"]),
        ));
    }
    limits
}

fn scoped_limit(limit: &serde_json::Value) -> Option<UsageLimit> {
    // Session and weekly_all entries repeat the shared windows already in `usage_windows`.
    if limit["kind"] != "weekly_scoped" {
        return None;
    }
    let label = |key: &str| {
        limit["scope"][key]["display_name"]
            .as_str()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
    };
    let scope = match (label("model"), label("surface")) {
        (Some(model), _) => UsageLimitScope::Model { name: model },
        (None, Some(surface)) => UsageLimitScope::Surface { name: surface },
        (None, None) => return None,
    };
    Some(scoped_entry(
        scope,
        percent(&limit["percent"])?,
        reset(&limit["resets_at"]),
    ))
}

fn scoped_entry(
    scope: UsageLimitScope,
    used_percent: u8,
    resets_at_unix: Option<i64>,
) -> UsageLimit {
    let (kind, name) = match &scope {
        UsageLimitScope::Model { name } => ("model", name.clone()),
        UsageLimitScope::Surface { name } => ("surface", name.clone()),
        UsageLimitScope::Account => ("account", String::new()),
    };
    UsageLimit {
        id: format!("7d:{kind}:{}", name.to_lowercase().replace(' ', "-")),
        label: format!("{name} weekly"),
        short_label: name,
        scope,
        window_seconds: Some(WEEK_SECONDS),
        used_percent,
        resets_at_unix,
        affects_routing: false,
    }
}

fn percent(value: &serde_json::Value) -> Option<u8> {
    value
        .as_f64()
        .filter(|v| v.is_finite() && (0.0..=100.0).contains(v))
        .map(|v| v.floor() as u8)
}

fn reset(value: &serde_json::Value) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value.as_str()?)
        .ok()
        .map(|at| at.timestamp())
}

fn duration_labels(seconds: Option<u64>) -> (String, String) {
    // Matches the widget's duration titles so labels agree across surfaces.
    let Some(seconds) = seconds.filter(|s| *s > 0) else {
        return ("Usage".into(), String::new());
    };
    let hours = (seconds as f64 / 3_600.0).round() as u64;
    let days = (seconds as f64 / 86_400.0).round() as u64;
    let short = if hours < 48 {
        format!("{hours}h")
    } else {
        format!("{days}d")
    };
    let label = match hours {
        24 => "Daily".into(),
        168 => "Weekly".into(),
        h if h < 48 => format!("{h}-hour"),
        _ => format!("{days}-day"),
    };
    (label, short)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn window(used: u8, seconds: u64) -> QuotaWindowStatus {
        QuotaWindowStatus {
            used_percent: Some(used),
            reset_at_unix: Some(1_000),
            limit_window_seconds: Some(seconds),
        }
    }

    fn reporting(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn shared_windows_are_labeled_and_ordered_shortest_first() {
        let windows = BTreeMap::from([
            ("7d".into(), window(40, 604_800)),
            ("5h".into(), window(18, 18_000)),
        ]);
        let limits = build(&windows, None);
        let ids: Vec<_> = limits
            .iter()
            .map(|l| (l.id.as_str(), l.label.as_str(), l.short_label.as_str()))
            .collect();
        assert_eq!(ids, [("5h", "5-hour", "5h"), ("7d", "Weekly", "7d")]);
        assert!(
            limits
                .iter()
                .all(|l| l.affects_routing && l.scope == UsageLimitScope::Account)
        );
    }

    #[test]
    fn placeholder_and_unknown_windows_are_skipped() {
        let windows = BTreeMap::from([
            ("5h".into(), window(0, 0)),
            ("7d".into(), window(33, 604_800)),
            (
                "primary".into(),
                QuotaWindowStatus {
                    used_percent: None,
                    reset_at_unix: None,
                    limit_window_seconds: Some(18_000),
                },
            ),
        ]);
        let limits = build(&windows, None);
        assert_eq!(limits.len(), 1);
        assert_eq!(limits[0].id, "7d");
    }

    #[test]
    fn scoped_limits_are_separate_buckets_that_never_affect_routing() {
        let windows = BTreeMap::from([
            ("5h".into(), window(25, 18_000)),
            ("7d".into(), window(10, 604_800)),
        ]);
        let reporting = reporting(json!({
            "seven_day_fable": {"utilization": 50, "resets_at": null},
            "limits": [
                {"kind":"session","group":"session","percent":25,"resets_at":null,"scope":null},
                {"kind":"weekly_all","group":"weekly","percent":10,"resets_at":null,"scope":null},
                {"kind":"weekly_scoped","group":"weekly","percent":99.9,"resets_at":"1970-01-01T00:01:40Z",
                 "scope":{"model":{"id":null,"display_name":"Fable"},"surface":null}},
                {"kind":"weekly_scoped","group":"weekly","percent":5,"resets_at":null,
                 "scope":{"model":null,"surface":{"id":null,"display_name":"Claude Code"}}}
            ]
        }));
        let limits = build(&windows, Some(&reporting));
        let ids: Vec<_> = limits.iter().map(|l| l.id.as_str()).collect();
        // The array's Fable entry wins over the legacy field, and shared entries are not repeated.
        assert_eq!(
            ids,
            ["5h", "7d", "7d:model:fable", "7d:surface:claude-code"]
        );
        let fable = &limits[2];
        assert_eq!(
            (fable.label.as_str(), fable.short_label.as_str()),
            ("Fable weekly", "Fable")
        );
        assert_eq!(fable.used_percent, 99);
        assert_eq!(fable.resets_at_unix, Some(100));
        assert!(!fable.affects_routing);
        assert_eq!(
            limits[3].scope,
            UsageLimitScope::Surface {
                name: "Claude Code".into()
            }
        );
    }

    #[test]
    fn legacy_fable_field_is_used_when_the_array_omits_it() {
        let reporting = reporting(
            json!({"seven_day_fable": {"utilization": 25.5, "resets_at": "1970-01-01T00:01:40Z"}}),
        );
        let limits = build(&BTreeMap::new(), Some(&reporting));
        assert_eq!(limits.len(), 1);
        assert_eq!(limits[0].id, "7d:model:fable");
        assert_eq!(limits[0].used_percent, 25);
        assert_eq!(limits[0].resets_at_unix, Some(100));
    }

    #[test]
    fn serialized_shape_is_stable_for_clients() {
        let limit = scoped_entry(
            UsageLimitScope::Model {
                name: "Fable".into(),
            },
            40,
            None,
        );
        assert_eq!(
            serde_json::to_value(&limit).unwrap(),
            json!({
                "id": "7d:model:fable", "label": "Fable weekly", "short_label": "Fable",
                "scope": {"kind": "model", "name": "Fable"}, "window_seconds": 604800,
                "used_percent": 40, "affects_routing": false
            })
        );
    }
}
