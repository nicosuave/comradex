use super::*;
use crate::config::AccountConfig;
use serde::Deserialize;
use serde_json::{Value, json};

pub(super) const CODEX_CREDITS_URL: &str =
    "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits";

/// Usage at which a banked reset is redeemed, so in-flight work rarely reaches the limit first.
const AUTO_REDEEM_AT_PERCENT: u8 = 95;
/// A banked reset this close to expiring is redeemed whenever that adds quota.
const AUTO_REDEEM_BEFORE_EXPIRY_SECONDS: i64 = 24 * 3600;
/// Spacing between declined attempts below exhaustion.
const AUTO_REDEEM_RETRY_SECONDS: i64 = 3600;

#[derive(Default)]
pub(super) struct ResetCredits {
    pub claude: AsyncMutex<HashMap<String, ClaudeCredits>>,
    /// Usage and time of each account's last automatic attempt the provider declined.
    declined: AsyncMutex<HashMap<String, (u8, i64)>>,
}

pub(super) struct ClaudeCredits {
    pub owner: auth::QuotaOwner,
    pub value: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConsumeCredit {
    credit_id: String,
    redeem_request_id: String,
}

impl ConsumeCredit {
    pub fn parse(data: Option<&str>) -> Option<Self> {
        let value: Self = serde_json::from_str(data?).ok()?;
        if value.credit_id.is_empty()
            || !value
                .credit_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || !crate::claude::wire::uuid(&value.redeem_request_id)
        {
            return None;
        }
        Some(value)
    }
}

impl App {
    pub(super) async fn codex_reset_credits(
        &self,
        account_id: &str,
        consume: Option<ConsumeCredit>,
    ) -> Response<ProxyBody> {
        // Redemption bounds each of its own steps. An outer deadline could report a
        // confirmed reset as failed while its follow-up usage refresh is still running.
        let operation = match consume {
            Some(credit) => self.consume_codex_reset_credit(account_id, credit).await,
            None => tokio::time::timeout(
                USAGE_FETCH_ACCOUNT_TIMEOUT,
                self.list_codex_reset_credits(account_id),
            )
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("reset-credit listing timed out"))),
        };
        match operation {
            Ok((status, body)) => super::usage_management::api_response(status, body),
            Err(_) => super::usage_management::api_response(
                StatusCode::BAD_GATEWAY,
                json!({ "error": "reset-credit request could not be completed" }),
            ),
        }
    }

    async fn consume_codex_reset_credit(
        &self,
        account_id: &str,
        credit: ConsumeCredit,
    ) -> Result<(StatusCode, Value)> {
        match self
            .use_reset_credit(account_id, &credit.credit_id, &credit.redeem_request_id)
            .await
        {
            Ok(result) => Ok((StatusCode::OK, serde_json::to_value(result)?)),
            Err(error) => match error.downcast_ref::<ResetProviderError>() {
                Some(provider) => Ok((
                    provider.0,
                    json!({ "error": "provider rejected reset-credit request" }),
                )),
                None => Err(error),
            },
        }
    }

    async fn list_codex_reset_credits(&self, account_id: &str) -> Result<(StatusCode, Value)> {
        let account = &self.config.accounts[account_id];
        anyhow::ensure!(matches!(account, AccountConfig::CodexHome { .. }));
        let url = self.reset_credits_url(false)?;
        let mut credentials = self.auth.resolve(account, &hyper::HeaderMap::new()).await?;
        let mut response = self
            .fetch_account_endpoint(&credentials, Method::GET, url.clone(), empty_body())
            .await?;
        // A rejected read may refresh once. A redemption is never replayed automatically.
        if response.0 == StatusCode::UNAUTHORIZED {
            credentials = self
                .auth
                .force_refresh(account, &credentials)
                .await?
                .context("credentials cannot refresh")?;
            response = self
                .fetch_account_endpoint(&credentials, Method::GET, url, empty_body())
                .await?;
        }
        let (status, bytes) = response;
        if !status.is_success() {
            return Ok((
                status,
                json!({ "error": "provider rejected reset-credit request" }),
            ));
        }
        let body: Value = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(body["credits"].is_array(), "missing reset credits");
        Ok((status, body))
    }

    pub(super) async fn acknowledge_credit_reset(&self, account: &str) -> Response<ProxyBody> {
        if !matches!(
            self.config.accounts.get(account),
            Some(AccountConfig::CodexHome { .. })
        ) {
            return error_response(StatusCode::NOT_FOUND, "not_found", "unknown Codex account");
        }
        let _ = tokio::time::timeout(
            USAGE_FETCH_TIMEOUT,
            self.fetch_managed_usage_account(
                account,
                &self.config.accounts[account],
                chrono::Utc::now().timestamp().max(0) as u64,
            ),
        )
        .await;
        // Redemption clears its own cooldown. This compatibility call cannot clear
        // a block on its own, including a newer rejection after the reset completed.
        let snapshot = self.router.routing_snapshot().await;
        if snapshot
            .account_states
            .get(account)
            .is_some_and(|state| state.unavailable_reason.as_deref() == Some("quota"))
        {
            return error_response(
                StatusCode::CONFLICT,
                "quota_still_active",
                "account still has an active quota cooldown",
            );
        }
        super::usage_management::management_json(json!({ "status": "ok" }))
    }

    pub(super) async fn observe_claude_reset_credits(
        &self,
        account: &str,
        owner: auth::QuotaOwner,
        bytes: &[u8],
    ) {
        let value = serde_json::from_slice::<Value>(bytes)
            .ok()
            .and_then(|mut body| body.as_object_mut()?.remove("cedar_ember"));
        let mut observations = self.reset_credits.claude.lock().await;
        if let Some(value) = value {
            observations.insert(account.into(), ClaudeCredits { owner, value });
        } else {
            observations.remove(account);
        }
    }

    pub(super) async fn claude_reset_credits(&self, account: &str) -> Option<Value> {
        let AccountConfig::ClaudeHome { path } = &self.config.accounts[account] else {
            return None;
        };
        let path = path.clone();
        let current = tokio::task::spawn_blocking(move || crate::claude::auth::read(&path))
            .await
            .ok()?
            .ok()?;
        self.reset_credits
            .claude
            .lock()
            .await
            .get(account)
            .filter(|observation| observation.owner == current.owner())
            .map(|observation| observation.value.clone())
    }
}

use crate::reset_credits::{ResetCreditsResponse, ResetCreditsSnapshot, ResetOutcome, ResetResult};

/// Held under the account's usage lock. An uncertain POST may only be retried
/// with its original identity and target; successful replies are replayed locally.
pub(super) struct ResetAttempt {
    owner: auth::QuotaOwner,
    bearer_fingerprint: blake3::Hash,
    credit_id: String,
    request_id: String,
    quota_generation: u64,
    result: Option<ResetResult>,
}

impl ResetAttempt {
    fn matches_owner(&self, credentials: &Credentials) -> bool {
        self.owner == credentials.quota_owner()
            && (self.owner.is_known()
                || self.bearer_fingerprint == blake3::hash(credentials.authorization.as_bytes()))
    }
}

impl App {
    // Derive both endpoints from the usage origin, including in tests. Tests cannot
    // accidentally fall through to a production reset endpoint.
    fn reset_credits_url(&self, consume: bool) -> Result<Uri> {
        let mut parts = self.usage_url.clone().into_parts();
        let path = self
            .usage_url
            .path()
            .strip_suffix("/usage")
            .context("invalid usage path")?;
        parts.path_and_query = Some(
            format!(
                "{path}/rate-limit-reset-credits{}",
                if consume { "/consume" } else { "" }
            )
            .parse()?,
        );
        Ok(Uri::from_parts(parts)?)
    }

    async fn fetch_reset_credit_details(
        &self,
        credentials: &Credentials,
    ) -> Result<ResetCreditsResponse> {
        let (status, bytes) = tokio::time::timeout(
            USAGE_FETCH_TIMEOUT,
            self.fetch_account_endpoint(
                credentials,
                Method::GET,
                self.reset_credits_url(false)?,
                empty_body(),
            ),
        )
        .await
        .context("reset credit listing timed out")??;
        if !status.is_success() {
            bail!("reset credit listing returned HTTP {status}");
        }
        serde_json::from_slice(&bytes).context("parse reset credit listing")
    }

    pub(super) async fn refresh_reset_credits(
        &self,
        account: &str,
        credentials: &Credentials,
        available: Option<u64>,
    ) {
        let snapshot = match available {
            None => None,
            Some(available_count) => {
                let mut snapshot = ResetCreditsSnapshot {
                    available_count,
                    observed_at_unix: chrono::Utc::now().timestamp(),
                    credits: None,
                    error: None,
                };
                match self.fetch_reset_credit_details(credentials).await {
                    Ok(details) => {
                        snapshot.available_count = details.available_count;
                        snapshot.credits = Some(details.credits);
                    }
                    Err(error) => snapshot.error = Some(format!("{error:#}")),
                }
                Some(snapshot)
            }
        };
        self.router
            .observe_reset_credits_for_owner(account, snapshot, &credentials.quota_owner())
            .await;
    }

    pub async fn read_reset_credits(&self, account_name: &str) -> Result<ResetCreditsSnapshot> {
        let account = self.managed_reset_account(account_name)?;
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        tokio::time::timeout(
            USAGE_FETCH_ACCOUNT_TIMEOUT,
            self.fetch_managed_usage_account(account_name, account, now),
        )
        .await
        .context("reset credit refresh timed out")??;
        self.router
            .routing_snapshot()
            .await
            .account_states
            .get(account_name)
            .and_then(|state| state.reset_credits.clone())
            .context("reset credits are not reported for this account")
    }

    fn managed_reset_account(&self, name: &str) -> Result<&crate::config::AccountConfig> {
        let account = self.config.accounts.get(name).context("unknown account")?;
        if !matches!(account, crate::config::AccountConfig::CodexHome { .. }) {
            bail!("connect this account to a Codex login before using reset credits");
        }
        Ok(account)
    }

    /// Redeems the soonest-expiring banked reset when that adds quota: near the limit, or
    /// before the reset would expire unused.
    pub(super) async fn auto_redeem_reset(&self, account: &str, snapshot: &usage::UsageSnapshot) {
        let now = chrono::Utc::now();
        let now_unix = now.timestamp();
        let Some((used, gain)) = crate::reset_credits::redemption_gain(&snapshot.windows, now_unix)
        else {
            return;
        };
        if gain <= 0.0 {
            return;
        }
        let expiry = |credit: &crate::reset_credits::ResetCredit| {
            credit
                .expires_at
                .as_deref()
                .and_then(|expiry| chrono::DateTime::parse_from_rfc3339(expiry).ok())
                .map_or(i64::MAX, |expiry| expiry.timestamp())
        };
        let Some(credit) = self
            .router
            .routing_snapshot()
            .await
            .account_states
            .get(account)
            .and_then(|state| state.reset_credits.clone()?.credits)
            .into_iter()
            .flatten()
            .filter(|credit| credit.can_redeem_at(now))
            .min_by_key(|credit| expiry(credit))
        else {
            return;
        };
        let expiring = expiry(&credit) - now_unix <= AUTO_REDEEM_BEFORE_EXPIRY_SECONDS;
        if used < AUTO_REDEEM_AT_PERCENT && !expiring {
            return;
        }
        {
            let mut declined = self.reset_credits.declined.lock().await;
            match declined.get(account).copied() {
                // Usage fell, so the window restarted after the provider declined.
                Some((declined_at, _)) if used < declined_at => {
                    declined.remove(account);
                }
                Some((declined_at, at))
                    if used == declined_at
                        || (used < 100 && now_unix - at < AUTO_REDEEM_RETRY_SECONDS) =>
                {
                    return;
                }
                _ => {}
            }
        }
        let request_id = {
            let mut bytes = [0u8; 16];
            rand::RngCore::fill_bytes(&mut rand::rng(), &mut bytes);
            base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, bytes)
        };
        let mut credit_id = credit.id.clone();
        let mut outcome = self
            .use_reset_credit(account, &credit_id, &request_id)
            .await;
        if outcome.is_err() {
            // An earlier uncertain attempt blocks new ones until it is retried as sent.
            let pending = match self.usage_locks.get(account).map(|lock| lock.try_lock()) {
                Some(Ok(attempt)) => attempt
                    .as_ref()
                    .filter(|attempt| attempt.result.is_none())
                    .map(|attempt| (attempt.credit_id.clone(), attempt.request_id.clone())),
                _ => None,
            };
            if let Some((pending_id, request_id)) = pending {
                outcome = self
                    .use_reset_credit(account, &pending_id, &request_id)
                    .await;
                credit_id = pending_id;
            }
        }
        // A retried earlier attempt may target a credit missing from this snapshot.
        let expires_at = if credit_id == credit.id {
            credit.expires_at.as_deref().unwrap_or("never")
        } else {
            "unknown"
        };
        match outcome {
            Ok(result) => {
                info!(
                    account,
                    credit = credit_id.as_str(),
                    expires_at,
                    code = ?result.code,
                    used,
                    "automatic reset redemption finished"
                );
                let mut declined = self.reset_credits.declined.lock().await;
                if result.code == ResetOutcome::NothingToReset {
                    declined.insert(account.into(), (used, now_unix));
                } else {
                    declined.remove(account);
                }
            }
            Err(error) => {
                warn!(
                    account,
                    credit = credit_id.as_str(),
                    expires_at,
                    error = %format!("{error:#}"),
                    "automatic reset redemption failed"
                )
            }
        }
    }

    /// Explicit control requests and opt-in automatic redemption reach this method.
    pub async fn use_reset_credit(
        &self,
        account_name: &str,
        credit_id: &str,
        request_id: &str,
    ) -> Result<ResetResult> {
        if credit_id.is_empty()
            || credit_id.len() > 256
            || request_id.is_empty()
            || request_id.len() > 128
        {
            bail!("a credit ID and a stable request ID are required");
        }
        let account = self.managed_reset_account(account_name)?;
        let mut action = self
            .usage_locks
            .get(account_name)
            .context("unknown account")?
            .try_lock()
            .context("usage refresh or reset already in progress; try again shortly")?;
        let credentials = self.auth.resolve(account, &hyper::HeaderMap::new()).await?;
        let is_retry = match action.as_ref() {
            Some(previous) if previous.request_id == request_id => {
                if previous.credit_id != credit_id || !previous.matches_owner(&credentials) {
                    bail!("reset retry must use the original account identity and credit");
                }
                if let Some(result) = &previous.result {
                    return Ok(result.clone());
                }
                true
            }
            Some(previous) if previous.result.is_none() && previous.matches_owner(&credentials) => {
                bail!(
                    "previous reset outcome is unknown; retry its original credit and request ID"
                );
            }
            _ => false,
        };
        if !is_retry {
            let details = self.fetch_reset_credit_details(&credentials).await?;
            let credit = details
                .credits
                .iter()
                .find(|credit| credit.id == credit_id)
                .context("selected reset credit no longer exists; refresh its status")?;
            if !credit.can_redeem_at(chrono::Utc::now()) {
                bail!(
                    "selected reset credit is expired, unavailable, or unsupported; refresh its status"
                );
            }
        }
        let current = self.auth.resolve(account, &hyper::HeaderMap::new()).await?;
        let owner = credentials.quota_owner();
        if current.quota_owner() != owner
            || (!owner.is_known() && current.authorization != credentials.authorization)
        {
            bail!("account identity changed; refresh before using a reset");
        }
        let request_generation = self
            .router
            .reset_generation_for_owner(account_name, &owner)
            .await
            .context("account identity changed; refresh before using a reset")?;
        if !is_retry {
            *action = Some(ResetAttempt {
                owner: owner.clone(),
                bearer_fingerprint: blake3::hash(credentials.authorization.as_bytes()),
                credit_id: credit_id.into(),
                request_id: request_id.into(),
                quota_generation: request_generation,
                result: None,
            });
        }
        let attempt = action.as_mut().expect("reset attempt was reserved");
        self.router
            .observe_reset_credits_for_owner(account_name, None, &credentials.quota_owner())
            .await;
        let body = json_body(
            serde_json::json!({ "credit_id": credit_id, "redeem_request_id": request_id }),
        );
        // Do not retry a POST or switch identities on failure. The caller retains
        // request_id, and every attempt targets this same credit.
        let (status, bytes) = tokio::time::timeout(
            USAGE_FETCH_TIMEOUT,
            self.fetch_account_endpoint(
                &credentials,
                Method::POST,
                self.reset_credits_url(true)?,
                body,
            ),
        )
        .await
        .context(
            "reset outcome unknown; refresh credits before retrying with the same request ID",
        )??;
        if !status.is_success() {
            return Err(ResetProviderError(status).into());
        }
        let mut result: ResetResult = serde_json::from_slice(&bytes).context(
            "reset outcome unknown; refresh credits before retrying with the same request ID",
        )?;
        if result.code == ResetOutcome::Reset
            || (is_retry && result.code == ResetOutcome::AlreadyRedeemed)
        {
            // A fresh reset happened during this POST. An idempotent replay only
            // confirms the original attempt and cannot erase intervening failures.
            let generation = if result.code == ResetOutcome::Reset {
                request_generation
            } else {
                attempt.quota_generation
            };
            self.router
                .confirm_reset_for_owner(account_name, &owner, generation)
                .await;
        }
        // Retain the confirmed outcome even if the following refresh is interrupted
        // or the local client loses this response. A replay must not clear later quota.
        attempt.result = Some(result.clone());
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        let refreshed = tokio::time::timeout(
            USAGE_FETCH_ACCOUNT_TIMEOUT,
            self.fetch_managed_usage_account_locked(account_name, account, now),
        )
        .await
        .context("usage refresh timed out")
        .and_then(|result| result);
        if let Err(error) = refreshed {
            result.refresh_error = Some(format!("{error:#}"));
        }
        attempt.result = Some(result.clone());
        Ok(result)
    }
}

#[derive(Debug)]
struct ResetProviderError(StatusCode);

impl std::fmt::Display for ResetProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "reset endpoint returned HTTP {}; refresh credits before retrying with the same request ID",
            self.0
        )
    }
}

impl std::error::Error for ResetProviderError {}
