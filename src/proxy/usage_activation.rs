use super::*;
use crate::usage_activation::activation_reset;

const ACTIVATION_TIMEOUT: Duration = Duration::from_secs(60);
const ACTIVATION_RESPONSE_LIMIT: usize = 1024 * 1024;

impl App {
    pub(super) async fn activate_weekly_usage(
        &self,
        account: &str,
        snapshot: &usage::UsageSnapshot,
        credentials: &Credentials,
    ) -> Result<()> {
        if activation_reset(&snapshot.windows, snapshot.observed_at_unix).is_none() {
            return Ok(());
        }
        // A rotating bearer and multiple configured aliases can represent the same quota owner.
        // Unknown identities are never activated automatically.
        let identity = credentials.context_identity()?;
        let key = blake3::hash(identity.as_bytes()).to_hex().to_string();
        let mut ledger = self.usage_activation.lock().await;
        let ledger = ledger.as_mut().map_err(|_| {
            anyhow::anyhow!("usage activation ledger unavailable; automatic activation disabled")
        })?;
        if self.shutting_down.load(Ordering::Acquire) {
            return Ok(());
        }
        // Background requests yield to foreground traffic rather than queueing for capacity.
        let Ok(_slot) = self.http_slots.try_acquire() else {
            return Ok(());
        };
        let activated = tokio::time::timeout(ACTIVATION_TIMEOUT, async {
            // A preceding account may have taken time to complete. Recheck actual usage and
            // identity immediately before spending quota; never dispatch on a stale poll.
            let now = chrono::Utc::now().timestamp();
            let (fresh, credentials) = self
                .fetch_managed_usage_account(account, &self.config.accounts[account], now as u64)
                .await?;
            if credentials.context_identity()? != identity {
                return Ok(false);
            }
            let Some(reset) = activation_reset(&fresh.windows, now) else {
                return Ok(false);
            };
            let routing = self.router.routing_snapshot().await;
            if !routing
                .account_states
                .get(account)
                .is_some_and(|state| state.available)
            {
                return Ok(false);
            }
            if self.shutting_down.load(Ordering::Acquire) {
                return Ok(false);
            }
            // Write the attempt before sending. A timeout, interruption or daemon restart must
            // not immediately repeat an upstream request whose outcome might be unknown.
            if !ledger.reserve(&key, now, reset)? {
                return Ok(false);
            }
            self.send_usage_activation(account, credentials).await?;
            ledger.complete(&key, chrono::Utc::now().timestamp(), reset)?;
            info!(account, "weekly usage activation completed");
            Ok::<_, anyhow::Error>(true)
        })
        .await
        .context("weekly usage activation timed out")??;
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
                warn!(account, %error, "usage refresh after weekly activation failed");
            }
        }
        Ok(())
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
