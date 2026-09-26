use crate::{routing::QuotaWindowStatus, usage::UsageSnapshot};
use hyper::HeaderMap;
use std::collections::BTreeMap;

fn header<'a>(headers: &'a HeaderMap, key: &str) -> Option<&'a str> {
    headers.get(key)?.to_str().ok()
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
            .map(|v| (v * 100.0).ceil().min(100.0) as u8);
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
        observed_at_unix: now as i64,
        windows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
