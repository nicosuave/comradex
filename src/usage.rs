use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::routing::QuotaWindowStatus;

pub const USAGE_URL: &str = "https://chatgpt.com/backend-api/wham/usage";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageSnapshot {
    pub observed_at_unix: i64,
    pub windows: BTreeMap<String, QuotaWindowStatus>,
}

#[derive(Debug, Deserialize)]
struct WhamUsageResponse {
    rate_limit: Option<WhamRateLimit>,
}

#[derive(Debug, Deserialize)]
struct WhamRateLimit {
    primary_window: Option<WhamWindow>,
    secondary_window: Option<WhamWindow>,
    tertiary_window: Option<WhamWindow>,
}

#[derive(Debug, Deserialize)]
struct WhamWindow {
    used_percent: Option<f32>,
    reset_at: Option<i64>,
    limit_window_seconds: Option<u64>,
}

pub fn parse_usage_response(bytes: &[u8], observed_at_unix: i64) -> Result<UsageSnapshot> {
    let response: WhamUsageResponse =
        serde_json::from_slice(bytes).context("parse Codex usage response")?;
    let limits = response
        .rate_limit
        .context("Codex usage response omitted rate_limit")?;
    let mut windows = BTreeMap::new();
    for (name, window) in [
        ("primary", limits.primary_window),
        ("secondary", limits.secondary_window),
        ("tertiary", limits.tertiary_window),
    ] {
        let Some(window) = window else {
            continue;
        };
        let used_percent = window
            .used_percent
            .filter(|percent| percent.is_finite())
            .map(|percent| percent.clamp(0.0, 100.0).round() as u8);
        let reset_at_unix = window.reset_at.filter(|timestamp| *timestamp > 0);
        let limit_window_seconds = window.limit_window_seconds.filter(|seconds| *seconds > 0);
        if used_percent.is_none() && reset_at_unix.is_none() && limit_window_seconds.is_none() {
            continue;
        }
        windows.insert(
            name.to_owned(),
            QuotaWindowStatus {
                used_percent,
                reset_at_unix,
                limit_window_seconds,
            },
        );
    }
    if windows.is_empty() {
        bail!("Codex usage response contained no usable quota windows")
    }
    Ok(UsageSnapshot {
        observed_at_unix,
        windows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_usage_windows_and_expiration_metadata() {
        let snapshot = parse_usage_response(
            br#"{
                "rate_limit": {
                    "primary_window": {
                        "used_percent": 17.4,
                        "reset_at": 1789000000,
                        "limit_window_seconds": 604800
                    },
                    "secondary_window": {
                        "used_percent": 81.6,
                        "reset_at": 1789100000,
                        "limit_window_seconds": 2628000
                    },
                    "tertiary_window": null
                }
            }"#,
            1788800000,
        )
        .unwrap();

        assert_eq!(snapshot.observed_at_unix, 1788800000);
        assert_eq!(snapshot.windows["primary"].used_percent, Some(17));
        assert_eq!(snapshot.windows["primary"].reset_at_unix, Some(1789000000));
        assert_eq!(
            snapshot.windows["primary"].limit_window_seconds,
            Some(604800)
        );
        assert_eq!(snapshot.windows["secondary"].used_percent, Some(82));
        assert!(!snapshot.windows.contains_key("tertiary"));
    }

    #[test]
    fn rejects_payloads_without_usable_windows() {
        for payload in [
            br#"{}"#.as_slice(),
            br#"{"rate_limit":{}}"#.as_slice(),
            br#"{"rate_limit":{"primary_window":null}}"#.as_slice(),
        ] {
            assert!(parse_usage_response(payload, 1).is_err());
        }
    }

    #[test]
    fn normalizes_untrusted_numeric_fields() {
        let snapshot = parse_usage_response(
            br#"{"rate_limit":{
                "primary_window":{"used_percent":-5,"reset_at":-1,"limit_window_seconds":0},
                "tertiary_window":{"used_percent":120,"reset_at":1789200000}
            }}"#,
            10,
        )
        .unwrap();

        assert_eq!(snapshot.windows["primary"].used_percent, Some(0));
        assert_eq!(snapshot.windows["primary"].reset_at_unix, None);
        assert_eq!(snapshot.windows["primary"].limit_window_seconds, None);
        assert_eq!(snapshot.windows["tertiary"].used_percent, Some(100));
    }
}
