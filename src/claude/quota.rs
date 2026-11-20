use crate::{routing::QuotaWindowStatus, usage::UsageSnapshot};
use hyper::HeaderMap;
use std::collections::BTreeMap;

fn header<'a>(headers: &'a HeaderMap, key: &str) -> Option<&'a str> {
    headers.get(key)?.to_str().ok()
}

pub fn is_fable(model: &str) -> bool {
    model == "claude-fable" || model.starts_with("claude-fable-")
}

pub fn fable_exhausted(
    usage: &serde_json::Map<String, serde_json::Value>,
    model: &str,
    now: u64,
) -> bool {
    if !is_fable(model) {
        return false;
    }
    let exhausted = |window: &serde_json::Value, percent: &str| {
        if window.get(percent).and_then(serde_json::Value::as_f64) != Some(100.0) {
            return false;
        }
        match window.get("resets_at") {
            None | Some(serde_json::Value::Null) => true,
            Some(serde_json::Value::String(at)) => chrono::DateTime::parse_from_rfc3339(at)
                .ok()
                .is_some_and(|reset| {
                    let reset_second = reset
                        .timestamp()
                        .saturating_add(i64::from(reset.timestamp_subsec_nanos() != 0));
                    i128::from(reset_second) > i128::from(now)
                }),
            _ => false,
        }
    };
    usage
        .get("seven_day_fable")
        .is_some_and(|window| exhausted(window, "utilization"))
        || usage
            .get("limits")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|limits| {
                limits.iter().any(|limit| {
                    if limit["kind"] != "weekly_scoped"
                        || limit["group"] != "weekly"
                        || limit
                            .get("scope")
                            .and_then(|scope| scope.get("surface"))
                            .is_some_and(|surface| !surface.is_null())
                    {
                        return false;
                    }
                    let scope = &limit["scope"]["model"];
                    let matches = match scope.get("id") {
                        Some(serde_json::Value::String(id)) => id == model,
                        None | Some(serde_json::Value::Null) => scope["display_name"]
                            .as_str()
                            .is_some_and(|name| name.eq_ignore_ascii_case("Fable")),
                        _ => false,
                    };
                    matches && exhausted(limit, "percent")
                })
            })
}

pub fn shared_reset(headers: &HeaderMap, now: u64) -> Option<u64> {
    // Overage/model-specific rejection does not establish whole-account exhaustion.
    ["5h", "7d"]
        .into_iter()
        .filter_map(|window| {
            let prefix = format!("anthropic-ratelimit-unified-{window}");
            if header(headers, &format!("{prefix}-status")) != Some("rejected") {
                return None;
            }
            header(headers, &format!("{prefix}-reset"))?
                .parse::<u64>()
                .ok()
                .filter(|at| *at > now)
        })
        .max()
}

pub fn snapshot(headers: &HeaderMap, now: u64) -> Option<UsageSnapshot> {
    let mut windows = BTreeMap::new();
    for (window, seconds) in [("5h", 18000), ("7d", 604800)] {
        let prefix = format!("anthropic-ratelimit-unified-{window}");
        let status = header(headers, &format!("{prefix}-status"));
        let used = header(headers, &format!("{prefix}-utilization"))
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            // Routing treats 100 as confirmed exhaustion, so remaining allowance never rounds
            // up to it. Matches the JSON parser in `claude::maintenance`.
            .map(|v| (v * 100.0).floor().min(100.0) as u8);
        if used.is_none() && status != Some("rejected") {
            continue;
        }
        windows.insert(
            window.to_owned(),
            QuotaWindowStatus {
                used_percent: if status == Some("rejected") {
                    Some(100)
                } else {
                    used
                },
                reset_at_unix: header(headers, &format!("{prefix}-reset"))
                    .and_then(|v| v.parse().ok()),
                limit_window_seconds: Some(seconds),
            },
        );
    }
    (!windows.is_empty()).then_some(UsageSnapshot {
        reset_credits_available: None,
        observed_at_unix: now as i64,
        windows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fable_usage_is_scoped_to_its_model_and_current_window() {
        let usage = |percent, reset| {
            serde_json::json!({
                "seven_day_fable": {"utilization": percent, "resets_at": reset}
            })
            .as_object()
            .unwrap()
            .clone()
        };
        let future = "1970-01-01T00:03:20Z";
        assert!(fable_exhausted(
            &usage(100.0, Some(future)),
            "claude-fable-5-2",
            100
        ));
        assert!(!fable_exhausted(
            &usage(99.9, Some(future)),
            "claude-fable-5-2",
            100
        ));
        assert!(!fable_exhausted(
            &usage(100.0, Some(future)),
            "claude-fable-5-2",
            200
        ));
        assert!(!fable_exhausted(
            &usage(100.0, Some("invalid")),
            "claude-fable-5-2",
            100
        ));
        assert!(fable_exhausted(
            &usage(100.0, None),
            "claude-fable-5-2",
            100
        ));
        assert!(!fable_exhausted(
            &usage(100.0, None),
            "claude-sonnet-5-5",
            100
        ));
    }

    #[test]
    fn fable_scopes_do_not_apply_to_another_model_or_surface() {
        let scoped = |id, name, surface| {
            serde_json::json!({"limits": [{
            "kind": "weekly_scoped", "group": "weekly", "percent": 100,
            "resets_at": null, "scope": {"model": {"id": id, "display_name": name}, "surface": surface}
        }]}).as_object().unwrap().clone()
        };
        let model = "claude-fable-5-2";
        assert!(fable_exhausted(
            &scoped(None, "Fable", serde_json::Value::Null),
            model,
            100
        ));
        assert!(fable_exhausted(
            &scoped(Some(model), "Fable 5.2", serde_json::Value::Null),
            model,
            100
        ));
        assert!(!fable_exhausted(
            &scoped(Some("claude-fable-5-3"), "Fable", serde_json::Value::Null),
            model,
            100
        ));
        assert!(!fable_exhausted(
            &scoped(None, "Opus", serde_json::Value::Null),
            model,
            100
        ));
        assert!(!fable_exhausted(
            &scoped(
                None,
                "Fable",
                serde_json::json!({"display_name":"Claude Chat"})
            ),
            model,
            100
        ));
        let mut other_kind = scoped(None, "Fable", serde_json::Value::Null);
        other_kind.get_mut("limits").unwrap()[0]["kind"] = "weekly_all".into();
        assert!(!fable_exhausted(&other_kind, model, 100));
        let mut other_group = scoped(None, "Fable", serde_json::Value::Null);
        other_group.get_mut("limits").unwrap()[0]["group"] = "session".into();
        assert!(!fable_exhausted(&other_group, model, 100));
    }

    #[test]
    fn fractional_fable_reset_does_not_expire_early_on_a_whole_second_clock() {
        let usage = serde_json::json!({"seven_day_fable": {
            "utilization": 100, "resets_at": "1970-01-01T00:03:20.5Z"
        }})
        .as_object()
        .unwrap()
        .clone();
        assert!(fable_exhausted(&usage, "claude-fable-5-2", 200));
        assert!(!fable_exhausted(&usage, "claude-fable-5-2", 201));
    }
    #[test]
    fn only_explicit_shared_exhaustion_authorizes_rollover() {
        let mut headers = HeaderMap::new();
        for (key, value) in [
            ("anthropic-ratelimit-unified-status", "rejected"),
            ("anthropic-ratelimit-unified-7d_oi-status", "rejected"),
            ("anthropic-ratelimit-unified-7d_oi-reset", "9999999999"),
            ("retry-after", "120"),
        ] {
            headers.insert(key, value.parse().unwrap());
        }
        assert_eq!(shared_reset(&headers, 100), None);
        headers.insert(
            "anthropic-ratelimit-unified-5h-status",
            "rejected".parse().unwrap(),
        );
        headers.insert(
            "anthropic-ratelimit-unified-5h-reset",
            "200".parse().unwrap(),
        );
        assert_eq!(shared_reset(&headers, 100), Some(200));
        assert_eq!(shared_reset(&headers, 200), None);
        headers.insert(
            "anthropic-ratelimit-unified-5h-reset",
            "NaN".parse().unwrap(),
        );
        assert_eq!(shared_reset(&headers, 100), None);
    }
    #[test]
    fn utilization_is_validated_and_model_windows_do_not_poison_account_usage() {
        for invalid in ["NaN", "inf", "-1", "invalid"] {
            let mut headers = HeaderMap::new();
            headers.insert(
                "anthropic-ratelimit-unified-5h-utilization",
                invalid.parse().unwrap(),
            );
            assert!(snapshot(&headers, 100).is_none());
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            "anthropic-ratelimit-unified-5h-utilization",
            "0.45".parse().unwrap(),
        );
        headers.insert(
            "anthropic-ratelimit-unified-7d_oi-utilization",
            "1".parse().unwrap(),
        );
        let value = snapshot(&headers, 100).unwrap();
        assert_eq!(value.windows["5h"].used_percent, Some(45));
        assert_eq!(value.windows.len(), 1);
    }
    #[test]
    fn remaining_allowance_never_rounds_up_to_confirmed_exhaustion() {
        // Routing treats 100 as confirmed exhaustion and will block the account and
        // permanently reassign portable work, so only a genuine 1.0 may report 100.
        for (utilization, expected) in [("0.996", 99), ("0.991", 99), ("0.9999", 99), ("1", 100)] {
            let mut headers = HeaderMap::new();
            headers.insert(
                "anthropic-ratelimit-unified-5h-utilization",
                utilization.parse().unwrap(),
            );
            let value = snapshot(&headers, 100).unwrap();
            assert_eq!(
                value.windows["5h"].used_percent,
                Some(expected),
                "utilization {utilization}"
            );
        }
    }
}
