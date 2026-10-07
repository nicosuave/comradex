use super::*;
use crate::usage_activation::{ActivationRecord, activation_eligibility};

const ACTIVATION_TIMEOUT: Duration = Duration::from_secs(60);
const ACTIVATION_RESPONSE_LIMIT: usize = 1024 * 1024;

impl App {
    pub(super) async fn activate_weekly_usage(
        &self,
        account: &str,
        snapshot: &usage::UsageSnapshot,
        credentials: &Credentials,
    ) -> Result<()> {
        if let Err(reason) = activation_eligibility(&snapshot.windows, snapshot.observed_at_unix) {
            self.log_weekly_activation(account, Some(snapshot), "skipped", reason, None);
            return Ok(());
        }
        // A rotating bearer and multiple configured aliases can represent the same quota owner.
        // Unknown identities are never activated automatically.
        let identity = credentials.context_identity().map_err(|error| {
            self.log_weekly_activation(
                account,
                Some(snapshot),
                "skipped",
                "unknown_identity",
                None,
            );
            error
        })?;
        let key = blake3::hash(identity.as_bytes()).to_hex().to_string();
        let mut ledger = self.usage_activation.lock().await;
        let ledger = ledger.as_mut().map_err(|_| {
            self.log_weekly_activation(
                account,
                Some(snapshot),
                "skipped",
                "ledger_unavailable",
                None,
            );
            anyhow::anyhow!("usage activation ledger unavailable; automatic activation disabled")
        })?;
        if self.shutting_down.load(Ordering::Acquire) {
            self.log_weekly_activation(
                account,
                Some(snapshot),
                "skipped",
                "shutting_down",
                ledger.record(&key),
            );
            return Ok(());
        }
        // Background requests yield to foreground traffic rather than queueing for capacity.
        let Ok(_slot) = self.http_slots.try_acquire() else {
            self.log_weekly_activation(
                account,
                Some(snapshot),
                "skipped",
                "capacity_unavailable",
                ledger.record(&key),
            );
            return Ok(());
        };
        let mut stage = "preflight_fetch";
        let activated = tokio::time::timeout(ACTIVATION_TIMEOUT, async {
            // A preceding account may have taken time to complete. Recheck actual usage and
            // identity immediately before spending quota; never dispatch on a stale poll.
            let now = chrono::Utc::now().timestamp();
            let (fresh, credentials) = self
                .fetch_managed_usage_account(account, &self.config.accounts[account], now as u64)
                .await?;
            stage = "preflight_identity";
            if credentials.context_identity()? != identity {
                self.log_weekly_activation(
                    account,
                    Some(&fresh),
                    "skipped",
                    "identity_changed",
                    ledger.record(&key),
                );
                return Ok(false);
            }
            let reset = match activation_eligibility(&fresh.windows, now) {
                Ok(reset) => reset,
                Err(reason) => {
                    self.log_weekly_activation(
                        account,
                        Some(&fresh),
                        "preflight_skipped",
                        reason,
                        ledger.record(&key),
                    );
                    return Ok(false);
                }
            };
            let routing = self.router.routing_snapshot().await;
            if !routing
                .account_states
                .get(account)
                .is_some_and(|state| state.available)
            {
                self.log_weekly_activation(
                    account,
                    Some(&fresh),
                    "skipped",
                    "account_unavailable",
                    ledger.record(&key),
                );
                return Ok(false);
            }
            if self.shutting_down.load(Ordering::Acquire) {
                self.log_weekly_activation(
                    account,
                    Some(&fresh),
                    "skipped",
                    "shutting_down",
                    ledger.record(&key),
                );
                return Ok(false);
            }
            // Write the attempt before sending. A timeout, interruption or daemon restart must
            // not immediately repeat an upstream request whose outcome might be unknown.
            if let Some(reason) = ledger.reservation_skip_reason(&key, now, reset) {
                self.log_weekly_activation(
                    account,
                    Some(&fresh),
                    "skipped",
                    reason,
                    ledger.record(&key),
                );
                return Ok(false);
            }
            stage = "reservation_write";
            if !ledger.reserve(&key, now, reset)? {
                return Ok(false);
            }
            self.log_weekly_activation(
                account,
                Some(&fresh),
                "dispatching",
                "eligible",
                ledger.record(&key),
            );
            stage = "activation_request";
            self.send_usage_activation(account, credentials).await?;
            stage = "completion_write";
            ledger.complete(&key, chrono::Utc::now().timestamp(), reset)?;
            self.log_weekly_activation(
                account,
                Some(&fresh),
                "completed",
                "response_completed",
                ledger.record(&key),
            );
            info!(account, "weekly usage activation completed");
            Ok::<_, anyhow::Error>(true)
        })
        .await;
        let activated = match activated {
            Ok(Ok(activated)) => activated,
            Ok(Err(error)) => {
                self.log_weekly_activation(
                    account,
                    Some(snapshot),
                    "failed",
                    stage,
                    ledger.record(&key),
                );
                return Err(error);
            }
            Err(error) => {
                self.log_weekly_activation(
                    account,
                    Some(snapshot),
                    "timed_out",
                    stage,
                    ledger.record(&key),
                );
                return Err(error).context("weekly usage activation timed out");
            }
        };
        // Completion is durable even if the separately bounded follow-up fetch fails.
        if activated {
            let refreshed = tokio::time::timeout(
                USAGE_FETCH_ACCOUNT_TIMEOUT,
                self.fetch_managed_usage_account(
                    account,
                    &self.config.accounts[account],
                    chrono::Utc::now().timestamp() as u64,
                ),
            )
            .await
            .context("usage refresh after weekly activation timed out")
            .and_then(|result| result);
            if let Err(error) = refreshed {
                self.log_weekly_activation(
                    account,
                    Some(snapshot),
                    "failed",
                    "post_activation_refresh",
                    ledger.record(&key),
                );
                warn!(account, %error, "usage refresh after weekly activation failed");
            }
        }
        Ok(())
    }

    pub(super) fn log_weekly_activation(
        &self,
        account: &str,
        snapshot: Option<&usage::UsageSnapshot>,
        outcome: &str,
        reason: &str,
        record: Option<&ActivationRecord>,
    ) {
        if !self.config.proxy.log_weekly_usage_activation {
            return;
        }
        info!(
            target: "comradex::usage_activation_decisions",
            account, outcome, reason,
            observed_at_unix = ?snapshot.map(|snapshot| snapshot.observed_at_unix),
            windows = ?snapshot.map(|snapshot| &snapshot.windows),
            previous_attempted_at_unix = ?record.map(|record| record.attempted_at),
            stored_reset_at_unix = ?record.map(|record| record.reset_at),
            completed_at_unix = ?record.and_then(|record| record.completed_at),
            "weekly usage activation decision"
        );
    }

    async fn send_usage_activation(&self, account: &str, credentials: Credentials) -> Result<()> {
        let headers = hyper::HeaderMap::from_iter([
            (CONTENT_TYPE, "application/json".parse()?),
            (ACCEPT, "text/event-stream".parse()?),
        ]);
        let owner = credentials.quota_owner();
        let response = self
            .send_http(
                account,
                &Method::POST,
                "/responses",
                &headers,
                credentials,
                json_body(serde_json::json!({
                    "model": "gpt-5.6-luna",
                    "reasoning": {"effort": "low"},
                    "instructions": "Reply only OK.",
                    "input": [{"role": "user", "content": [{"type": "input_text", "text": "Reply OK."}]}],
                    "tools": [],
                    "tool_choice": "none",
                    "store": false,
                    "stream": true
                })),
            )
            .await?;
        self.router
            .observe_headers_for_owner(account, response.headers(), &owner)
            .await;
        if !response.status().is_success() {
            bail!(
                "weekly usage activation returned HTTP {}",
                response.status()
            );
        }
        let mut decoder = SseDecoder::new(ACTIVATION_RESPONSE_LIMIT);
        let mut received = 0usize;
        let mut body = response.into_body();
        while let Some(frame) = body.frame().await {
            let frame = frame?;
            let Some(data) = frame.data_ref() else {
                continue;
            };
            received = received.saturating_add(data.len());
            if received > ACTIVATION_RESPONSE_LIMIT {
                bail!("weekly usage activation response exceeded size limit");
            }
            // Do not log payloads: even error events may include sensitive upstream details.
            decoder.push(data)?;
            match decoder.terminal_status() {
                Some(sse::TerminalStatus::Completed) => return Ok(()),
                Some(_) => bail!("weekly usage activation did not complete successfully"),
                None => {}
            }
        }
        bail!("weekly usage activation ended without a completion event")
    }
}
