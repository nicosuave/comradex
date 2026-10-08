//! Backend-owned reset credits. Dates stay in RFC3339 form without losing precision.
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::routing::router::{LONG_WINDOW_SECONDS, QuotaWindowStatus};

/// Usage of the fullest long window and the quota a reset would add now. A reset restarts
/// the window, forfeiting the share of it already elapsed.
pub fn redemption_gain(
    windows: &BTreeMap<String, QuotaWindowStatus>,
    now: i64,
) -> Option<(u8, f64)> {
    windows
        .values()
        .filter_map(|window| {
            let used = window.used_percent?;
            let length = window
                .limit_window_seconds
                .filter(|length| *length >= LONG_WINDOW_SECONDS)?;
            let reset = window.reset_at_unix.filter(|reset| *reset > now)?;
            let left = ((reset - now) as f64 / length as f64).min(1.0);
            Some((used, f64::from(used) - 100.0 * (1.0 - left)))
        })
        .max_by_key(|(used, _)| *used)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetCredit {
    pub id: String,
    pub reset_type: String,
    pub status: String,
    pub granted_at: String,
    pub expires_at: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
}

impl ResetCredit {
    pub fn is_available_at(&self, now: DateTime<Utc>) -> bool {
        self.status == "available"
            && self.expires_at.as_ref().is_none_or(|expiry| {
                DateTime::parse_from_rfc3339(expiry).is_ok_and(|expiry| expiry > now)
            })
    }

    pub fn can_redeem_at(&self, now: DateTime<Utc>) -> bool {
        self.reset_type == "codex_rate_limits" && self.is_available_at(now)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetCreditsSnapshot {
    pub available_count: u64,
    pub observed_at_unix: i64,
    /// None means details could not be fetched, not an empty credit balance.
    pub credits: Option<Vec<ResetCredit>>,
    pub error: Option<String>,
}

impl ResetCreditsSnapshot {
    pub fn available_count_at(&self, now: DateTime<Utc>) -> u64 {
        self.credits
            .as_ref()
            .map_or(self.available_count, |credits| {
                credits
                    .iter()
                    .filter(|credit| credit.is_available_at(now))
                    .count() as u64
            })
    }
}

#[derive(Debug, Deserialize)]
pub struct ResetCreditsResponse {
    pub available_count: u64,
    pub credits: Vec<ResetCredit>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResetOutcome {
    Reset,
    NothingToReset,
    NoCredit,
    AlreadyRedeemed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResetResult {
    pub code: ResetOutcome,
    /// Redemption and the subsequent read are distinct; a failed read must not invite a new reset.
    #[serde(default)]
    pub refresh_error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_expiry_and_unknown_types_are_not_redeemable() {
        let mut credit: ResetCredit = serde_json::from_value(serde_json::json!({
            "id": "one", "reset_type": "codex_rate_limits", "status": "available",
            "granted_at": "2026-10-01T00:00:00Z", "expires_at": "2026-10-02T12:34:56.789+02:00"
        }))
        .unwrap();
        let expiry = DateTime::parse_from_rfc3339(credit.expires_at.as_ref().unwrap())
            .unwrap()
            .with_timezone(&Utc);
        assert!(credit.can_redeem_at(expiry - chrono::Duration::milliseconds(1)));
        assert!(!credit.can_redeem_at(expiry));
        credit.expires_at = Some("invalid".into());
        assert!(!credit.can_redeem_at(expiry));
        credit.expires_at = None;
        assert!(credit.can_redeem_at(expiry));
        credit.reset_type = "future_type".into();
        assert!(!credit.can_redeem_at(expiry));
    }

    #[test]
    fn redemption_gains_only_usage_beyond_the_elapsed_window() {
        const NOW: i64 = 1_800_000_000;
        let gain = |windows: &[(&str, u8, i64, u64)]| {
            let windows = windows
                .iter()
                .map(|(name, used, reset_in, length)| {
                    let window = QuotaWindowStatus {
                        used_percent: Some(*used),
                        reset_at_unix: Some(NOW + reset_in),
                        limit_window_seconds: Some(*length),
                    };
                    (name.to_string(), window)
                })
                .collect();
            redemption_gain(&windows, NOW).map(|(used, gain)| (used, gain.round() as i64))
        };
        const DAY: i64 = 24 * 3600;
        const WEEK: u64 = 7 * DAY as u64;
        // Exhausted a day into the week: the reset returns six days of quota.
        assert_eq!(gain(&[("primary", 100, 6 * DAY, WEEK)]), Some((100, 86)));
        // Behind pace, a reset would trade the rest of this week for a later one.
        assert_eq!(gain(&[("primary", 20, DAY, WEEK)]), Some((20, -66)));
        // An hour before the natural reset, near-exhaustion gains nothing.
        assert!(gain(&[("primary", 96, 3600, WEEK)]).unwrap().1 < 0);
        // Short windows refill on their own and do not decide.
        assert_eq!(gain(&[("primary", 100, 3600, 5 * 3600)]), None);
        assert_eq!(
            gain(&[
                ("primary", 100, 3600, 5 * 3600),
                ("secondary", 50, 3 * DAY, WEEK)
            ]),
            Some((50, -7))
        );
    }
}
