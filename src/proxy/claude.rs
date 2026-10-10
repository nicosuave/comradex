use super::{
    App, DirectAccountLease, HttpClient, error_response, headers,
    replay_body::{ProxyBody, ReplayBody, bytes_body},
};
use crate::{
    claude::{auth, quota, wire},
    config::{AccountConfig, Config, ListenerConfig},
    state::Stats,
};
use anyhow::{Context as _, Result};
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Body, Frame, Incoming, SizeHint},
};
use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit};
mod maintenance;

/// How long a session counts toward its account's load after its last request.
const ACTIVE_SESSION: Duration = Duration::from_secs(30 * 60);

#[cfg(not(test))]
const RESPONSE_BODY_IDLE_TIMEOUT: Duration = super::HTTP_RESPONSE_BODY_IDLE_TIMEOUT;
#[cfg(test)]
const RESPONSE_BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

pub(super) struct Claude {
    auth: auth::Resolver,
    client: HttpClient,
    upstream: String,
    usage_url: String,
    /// Per-account usage poll cooldown: (retry at, current throttle delay).
    usage_backoff: Mutex<HashMap<String, (u64, u64)>>,
    reporting_usage: Mutex<HashMap<String, ReportingUsage>>,
    activation: Mutex<Result<crate::claude::maintenance::ActivationLedger>>,
    sessions: Mutex<HashMap<String, Weak<Session>>>,
    /// Last inference dispatch per (session, account), for balancing new sessions.
    activity: Mutex<HashMap<(String, String), Instant>>,
}
struct ReportingUsage {
    owner: crate::auth::QuotaOwner,
    value: serde_json::Map<String, serde_json::Value>,
}
#[derive(Default)]
struct Session {
    gate: Mutex<()>,
    active: AtomicUsize,
}
impl Claude {
    pub fn new(config: &Config) -> Result<Self> {
        let client =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build(crate::transport::claude_http_connector()?);
        Ok(Self {
            auth: auth::Resolver::new(config),
            client,
            upstream: crate::claude::UPSTREAM.into(),
            usage_url: format!("{}/api/oauth/usage", crate::claude::UPSTREAM),
            usage_backoff: Mutex::new(HashMap::new()),
            reporting_usage: Mutex::new(HashMap::new()),
            activation: Mutex::new(crate::claude::maintenance::ActivationLedger::open(
                config
                    .proxy
                    .state_dir
                    .as_deref()
                    .unwrap_or(std::path::Path::new("."))
                    .join("claude-activation.json"),
            )),
            sessions: Mutex::new(HashMap::new()),
            activity: Mutex::new(HashMap::new()),
        })
    }
    async fn note_activity(&self, session: &str, account: &str) {
        let now = Instant::now();
        let mut activity = self.activity.lock().await;
        activity.retain(|_, at| now.duration_since(*at) < ACTIVE_SESSION);
        activity.insert((session.into(), account.into()), now);
    }
    async fn forget_activity(&self, session: &str, account: &str) {
        self.activity
            .lock()
            .await
            .remove(&(session.to_owned(), account.to_owned()));
    }
    async fn active_sessions(&self) -> HashMap<String, usize> {
        let now = Instant::now();
        let mut counts = HashMap::new();
        for ((_, account), at) in self.activity.lock().await.iter() {
            if now.duration_since(*at) < ACTIVE_SESSION {
                *counts.entry(account.clone()).or_default() += 1;
            }
        }
        counts
    }
    async fn session(&self, key: &str) -> Arc<Session> {
        let mut sessions = self.sessions.lock().await;
        sessions.retain(|_, s| s.strong_count() > 0);
        if let Some(session) = sessions.get(key).and_then(Weak::upgrade) {
            return session;
        }
        let session = Arc::new(Session::default());
        sessions.insert(key.into(), Arc::downgrade(&session));
        session
    }
}

impl App {
    pub(super) async fn observe_claude_reporting_usage(
        &self,
        account: &str,
        owner: crate::auth::QuotaOwner,
        bytes: &[u8],
    ) {
        let value = crate::claude::maintenance::parse_reporting_usage(bytes);
        let mut observations = self.claude.reporting_usage.lock().await;
        if let Some(value) = value {
            observations.insert(account.into(), ReportingUsage { owner, value });
        } else {
            observations.remove(account);
        }
    }

    pub(super) async fn claude_reporting_usage(
        &self,
        account: &str,
    ) -> Option<serde_json::Map<String, serde_json::Value>> {
        let AccountConfig::ClaudeHome { path } = &self.config.accounts[account] else {
            return None;
        };
        let path = path.clone();
        let current = tokio::task::spawn_blocking(move || auth::read(&path))
            .await
            .ok()?
            .ok()?;
        self.claude
            .reporting_usage
            .lock()
            .await
            .get(account)
            .filter(|observation| observation.owner == current.owner())
            .map(|observation| observation.value.clone())
    }

    pub(super) async fn handle_claude(
        &self,
        request: Request<Incoming>,
        listener: &ListenerConfig,
    ) -> Result<Response<ProxyBody>> {
        let root = format!("/{}/", self.config.proxy.installation_secret);
        let Some(path) = request.uri().path().strip_prefix(&root) else {
            return Ok(error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                "unknown proxy path",
            ));
        };
        let path = format!("/{path}");
        let count = path == "/v1/messages/count_tokens";
        let message = path == "/v1/messages";
        let ancillary = (path == "/v1/models"
            && matches!(*request.method(), Method::GET | Method::HEAD))
            || (path == "/api/hello" && *request.method() == Method::HEAD);
        if (!message && !count && !ancillary)
            || ((message || count) && *request.method() != Method::POST)
        {
            return Ok(error_response(
                StatusCode::NOT_FOUND,
                "unsupported_claude_endpoint",
                "unsupported native Claude endpoint",
            ));
        }
        if !wire::native_headers(request.headers())
            || request.headers().get("x-app").and_then(|v| v.to_str().ok()) != Some("cli")
            || request.headers().contains_key("x-api-key")
            || request.headers().get_all("authorization").iter().count() != 1
        {
            return Ok(error_response(
                StatusCode::FORBIDDEN,
                "native_claude_required",
                "only native Claude Code requests are supported",
            ));
        }
        if request
            .headers()
            .get("content-encoding")
            .is_some_and(|v| v.as_bytes() != b"identity")
        {
            return Ok(error_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_encoding",
                "native uncompressed Claude requests required",
            ));
        }
        let permit = match self.http_slots.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                return Ok(error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "at_capacity",
                    "HTTP request limit reached",
                ));
            }
        };
        let admission = Admission::new(self.stats.clone(), permit);
        let (parts, incoming) = request.into_parts();
        let mut replay = match ReplayBody::read_encoded(
            incoming,
            false,
            self.config.proxy.replay_memory_bytes,
            self.config.proxy.max_request_bytes,
            self.config.proxy.max_spool_bytes,
            self.stats.clone(),
        )
        .await
        {
            Ok(body) => body,
            Err(_) => {
                return Ok(error_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "invalid_body",
                    "request body unavailable or exceeds relay capacity",
                ));
            }
        };
        // Retain the reservation while materializing and forwarding the original request.
        let body = replay.body(0)?.collect().await?.to_bytes();
        if ancillary {
            // Discovery/probes retain the caller's credential unless model listing is
            // explicitly pinned. They never cause inference-session placement.
            let Some(_) = parts
                .headers
                .get("authorization")
                .filter(|v| v.to_str().is_ok_and(|v| v.starts_with("Bearer sk-ant-oat")))
            else {
                return Ok(error_response(
                    StatusCode::FORBIDDEN,
                    "native_oauth_required",
                    "native subscription OAuth required",
                ));
            };
            let pool = &self.config.pools[&listener.pool];
            let mut credentials = None;
            let mut lease = None;
            if path == "/v1/models"
                && let Some(account) = &pool.models_account
            {
                let Some(selected) = self.router.select_exact(pool, account).await else {
                    return Ok(error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "claude_account_unavailable",
                        "model-listing account unavailable",
                    ));
                };
                if let AccountConfig::ClaudeHome { path } = &self.config.accounts[account] {
                    credentials = Some(self.claude.auth.resolve(path, None).await?);
                }
                self.router
                    .validate_selection(&selected, &listener.pool, pool)
                    .await
                    .map_err(|_| anyhow::anyhow!("Claude model-listing selection changed"))?;
                self.router.begin(account).await;
                lease = Some(DirectAccountLease::new_for_selection(
                    self.router.clone(),
                    &selected,
                ));
            }
            let response = self
                .claude_send(
                    &parts.method,
                    &path,
                    parts.uri.query(),
                    &parts.headers,
                    body,
                    credentials.as_ref(),
                )
                .await?;
            let (mut parts, incoming) = response.into_parts();
            headers::strip_hop_by_hop(&mut parts.headers);
            return Ok(Response::from_parts(
                parts,
                ClaudeBody::new(incoming, admission, None, lease).boxed(),
            ));
        }
        let native = match wire::inspect(&parts.headers, &body, count) {
            Ok(native) => native,
            Err(error) => {
                // Check names only; the rejected request's headers and body are not logged.
                tracing::warn!(%error, "rejected Claude request that failed native checks");
                return Ok(error_response(
                    StatusCode::FORBIDDEN,
                    "native_claude_required",
                    "unrecognized or inconsistent native Claude Code request",
                ));
            }
        };
        let routing_id = format!("claude:{}:{}", listener.pool, native.session);
        let session = self.claude.session(&routing_id).await;
        let _gate = tokio::time::timeout(Duration::from_secs(30), session.gate.lock())
            .await
            .context("Claude session admission timed out")?;
        let key = self.router.affinity.key(&routing_id);
        let pool = &self.config.pools[&listener.pool];
        let binding = self.router.affinity.get(&key).await;
        // The session binding places portable work. Account-owned context follows the account
        // that served its conversation, which a helper sharing the session cannot move.
        let conversation_key = self.router.affinity.key(&format!(
            "claude-conversation:{routing_id}:{}",
            native.conversation
        ));
        let conversation = self.router.affinity.get(&conversation_key).await;
        let account_bound = native.portability == wire::Portability::Account;
        let hard = native.portability != wire::Portability::Portable
            || session.active.load(Ordering::Acquire) > 0;
        let pinned = pool.model_accounts.get(&native.model).cloned();
        let owner = if hard {
            conversation
                .clone()
                .or(binding.clone())
                .map(|b| b.account_id)
                .or_else(|| {
                    pool.members
                        .iter()
                        .find(|name| match &self.config.accounts[*name] {
                            AccountConfig::ClaudeInbound => true,
                            AccountConfig::ClaudeHome { path } => {
                                auth::read(path).is_ok_and(|c| c.account_uuid == native.account)
                            }
                            _ => false,
                        })
                        .cloned()
                })
        } else {
            None
        };
        if account_bound && owner.is_none() {
            return Ok(error_response(
                StatusCode::CONFLICT,
                "claude_owner_required",
                "account-owned Claude context requires its original account",
            ));
        }
        if owner
            .as_ref()
            .zip(pinned.as_ref())
            .is_some_and(|(a, b)| a != b)
        {
            return Ok(error_response(
                StatusCode::CONFLICT,
                "claude_pin_conflict",
                "model pin conflicts with conversation ownership",
            ));
        }
        // Thinking, compaction, and overlapping generations stay with their owner until it
        // exhausts its included quota, then move instead of stopping. The new account becomes
        // the owner, so the conversation does not return when the old window resets.
        let movable = !account_bound && pinned.is_none();
        let mut exact = owner.or(pinned);
        let mut moved_from = None;
        let mut remaining = pool.clone();
        let mut last_rejection = None;
        let mut attempted_identities = HashSet::new();
        let mut try_binding = true;
        for _ in 0..pool.members.len() {
            if movable
                && let Some(account) = exact.clone()
                && self.router.quota_exhausted(&account).await
            {
                remaining.members.retain(|name| name != &account);
                exact = None;
                moved_from = Some(account);
            }
            let selection = if let Some(account) = &exact {
                self.router.select_exact(&remaining, account).await
            } else {
                let bound = match binding
                    .as_ref()
                    .filter(|_| std::mem::take(&mut try_binding))
                {
                    Some(binding) => {
                        let selected = self
                            .router
                            .select_exact(&remaining, &binding.account_id)
                            .await;
                        if selected.is_none()
                            && movable
                            && self.router.quota_exhausted(&binding.account_id).await
                        {
                            moved_from = Some(binding.account_id.clone());
                        }
                        selected
                    }
                    None => None,
                };
                match bound {
                    Some(selection) => Some(selection),
                    None => {
                        let sessions = self.claude.active_sessions().await;
                        self.router
                            .select_balanced(&listener.pool, &remaining, &sessions)
                            .await
                    }
                }
            };
            let Some(selected) = selection else { break };
            let account = selected.account_id.clone();
            remaining.members.retain(|name| name != &account);
            let credentials = match &self.config.accounts[&account] {
                AccountConfig::ClaudeHome { path } => {
                    match self.claude.auth.resolve(path, None).await {
                        Ok(credentials) => Some(credentials),
                        Err(_) => {
                            if self.claude.auth.needs_login(path).await {
                                self.router.reauth_required(&account).await;
                            } else {
                                self.router.soft_failure(&account).await;
                            }
                            if exact.is_some() {
                                break;
                            }
                            continue;
                        }
                    }
                }
                AccountConfig::ClaudeInbound => None,
                _ => unreachable!("validated provider pool"),
            };
            let identity = credentials
                .as_ref()
                .map(|c| format!("{}:{}", c.organization_uuid, c.account_uuid))
                .unwrap_or_else(|| native.account.clone());
            // Aliases of the same subscription do not provide additional quota.
            let quota_identity = credentials
                .as_ref()
                .map(|c| c.account_uuid.as_str())
                .unwrap_or(&native.account);
            if !attempted_identities.insert(quota_identity.to_owned()) {
                continue;
            }
            let identity_key = self.router.affinity.key(&format!(
                "claude-credential:{routing_id}:{account}:{identity}"
            ));
            if account_bound
                && credentials
                    .as_ref()
                    .is_some_and(|c| binding.is_some() || c.account_uuid != native.account)
                && self.router.affinity.get(&identity_key).await.is_none()
            {
                return Ok(error_response(
                    StatusCode::CONFLICT,
                    "claude_identity_changed",
                    "account-owned context cannot move to a different credential identity",
                ));
            }
            self.router
                .validate_selection(&selected, &listener.pool, pool)
                .await
                .map_err(|_| anyhow::anyhow!("Claude selection changed before dispatch"))?;
            let sent = match &credentials {
                Some(credentials) => {
                    match wire::rewrite(&body, &credentials.account_uuid, &credentials.device_id) {
                        Ok(body) => Bytes::from(body),
                        Err(_) => {
                            return Ok(error_response(
                                StatusCode::UNPROCESSABLE_ENTITY,
                                "claude_identity_rewrite_unsupported",
                                "cannot safely rewrite this native credential/checksum shape",
                            ));
                        }
                    }
                }
                None => body.clone(),
            };
            if message {
                self.claude.note_activity(&routing_id, &account).await;
            }
            self.router.note_wired(&listener.pool, &account).await;
            self.router.begin(&account).await;
            let lease = DirectAccountLease::new_for_selection(self.router.clone(), &selected);
            let mut response = self
                .claude_send(
                    &parts.method,
                    &path,
                    parts.uri.query(),
                    &parts.headers,
                    sent.clone(),
                    credentials.as_ref(),
                )
                .await?;
            if response.status() == StatusCode::UNAUTHORIZED
                && let (Some(previous), AccountConfig::ClaudeHome { path: home }) =
                    (&credentials, &self.config.accounts[&account])
            {
                if let Ok(refreshed) = self
                    .claude
                    .auth
                    .resolve(home, Some(&previous.access_token))
                    .await
                {
                    self.router
                        .validate_selection(&selected, &listener.pool, pool)
                        .await
                        .map_err(|_| anyhow::anyhow!("Claude selection changed during refresh"))?;
                    response = self
                        .claude_send(
                            &parts.method,
                            &path,
                            parts.uri.query(),
                            &parts.headers,
                            sent,
                            Some(&refreshed),
                        )
                        .await?;
                    if response.status() == StatusCode::UNAUTHORIZED {
                        self.claude.auth.require_login(home).await;
                        self.router.reauth_required(&account).await;
                    }
                } else if self.claude.auth.needs_login(home).await {
                    self.router.reauth_required(&account).await;
                }
            }
            let now = auth::now();
            let owner = credentials
                .as_ref()
                .map(auth::Credential::owner)
                .unwrap_or_default();
            if let Some(snapshot) = quota::snapshot(response.headers(), now) {
                self.router
                    .observe_usage_snapshot_for_owner(&account, snapshot, &owner)
                    .await;
            }
            if response.status() == StatusCode::TOO_MANY_REQUESTS
                && let Some(reset) = quota::shared_reset(response.headers(), now)
            {
                self.router
                    .claude_quota_until(&account, reset, &owner)
                    .await;
                if movable {
                    if exact.take().is_some()
                        || binding.as_ref().is_some_and(|b| b.account_id == account)
                    {
                        moved_from = Some(account.clone());
                    }
                    self.claude.forget_activity(&routing_id, &account).await;
                    last_rejection = Some(response);
                    drop(lease);
                    continue;
                }
            }
            // Helpers on another account leave the session home alone. Quota migration from
            // that home updates it even when compaction replaced the first message, otherwise
            // the next compaction would inherit the old account again after its quota resets.
            let leads = binding.as_ref().is_none_or(|b| {
                b.account_id == account
                    || moved_from.as_ref() == Some(&b.account_id)
                    || conversation
                        .as_ref()
                        .is_some_and(|c| c.account_id == b.account_id)
            });
            if response.status().is_success()
                && !count
                && (!self.router.bind(identity_key, &account).await
                    || !self.router.bind(conversation_key.clone(), &account).await
                    || (leads && !self.router.bind(key.clone(), &account).await))
            {
                return Ok(error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "affinity_unavailable",
                    "could not persist Claude account binding",
                ));
            }
            if response.status().is_success()
                && let Some(from) = &moved_from
            {
                tracing::info!(
                    from = from.as_str(),
                    to = account.as_str(),
                    "moved Claude conversation off an account with exhausted quota"
                );
            }
            let (mut parts, incoming) = response.into_parts();
            headers::strip_hop_by_hop(&mut parts.headers);
            return Ok(Response::from_parts(
                parts,
                ClaudeBody::new(incoming, admission, Some(session.clone()), Some(lease)).boxed(),
            ));
        }
        if let Some(response) = last_rejection {
            let (mut parts, incoming) = response.into_parts();
            headers::strip_hop_by_hop(&mut parts.headers);
            return Ok(Response::from_parts(
                parts,
                ClaudeBody::new(incoming, admission, None, None).boxed(),
            ));
        }
        Ok(error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "claude_account_unavailable",
            "no eligible Claude account; check login or wait for quota reset",
        ))
    }

    async fn claude_send(
        &self,
        method: &Method,
        path: &str,
        query: Option<&str>,
        inbound: &hyper::HeaderMap,
        body: Bytes,
        credential: Option<&auth::Credential>,
    ) -> Result<Response<Incoming>> {
        let mut uri = format!("{}{path}", self.claude.upstream);
        if let Some(query) = query {
            uri.push('?');
            uri.push_str(query)
        }
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .body(bytes_body(body))?;
        *request.headers_mut() = inbound.clone();
        headers::strip_hop_by_hop(request.headers_mut());
        request.headers_mut().remove("content-length");
        request.headers_mut().remove("x-api-key");
        if let Some(credential) = credential {
            request.headers_mut().insert(
                "authorization",
                format!("Bearer {}", credential.access_token).parse()?,
            );
        }
        tokio::time::timeout(
            Duration::from_secs(120),
            self.claude.client.request(request),
        )
        .await
        .context("Claude upstream header timeout")?
        .context("Claude upstream request failed")
    }
}

struct Admission {
    stats: Arc<Stats>,
    _permit: OwnedSemaphorePermit,
}
impl Admission {
    fn new(stats: Arc<Stats>, permit: OwnedSemaphorePermit) -> Self {
        stats.inflight_http.fetch_add(1, Ordering::Relaxed);
        Self {
            stats,
            _permit: permit,
        }
    }
}
impl Drop for Admission {
    fn drop(&mut self) {
        self.stats.inflight_http.fetch_sub(1, Ordering::Relaxed);
    }
}
struct ClaudeBody {
    inner: Incoming,
    idle: Pin<Box<tokio::time::Sleep>>,
    _admission: Admission,
    session: Option<Arc<Session>>,
    _lease: Option<DirectAccountLease>,
}
impl ClaudeBody {
    fn new(
        inner: Incoming,
        admission: Admission,
        session: Option<Arc<Session>>,
        lease: Option<DirectAccountLease>,
    ) -> Self {
        if let Some(session) = &session {
            session.active.fetch_add(1, Ordering::AcqRel);
        }
        Self {
            inner,
            idle: Box::pin(tokio::time::sleep(RESPONSE_BODY_IDLE_TIMEOUT)),
            _admission: admission,
            session,
            _lease: lease,
        }
    }
}
impl Body for ClaudeBody {
    type Data = Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        match Pin::new(&mut self.inner).poll_frame(cx) {
            Poll::Ready(frame) => {
                self.idle
                    .as_mut()
                    .reset(tokio::time::Instant::now() + RESPONSE_BODY_IDLE_TIMEOUT);
                Poll::Ready(frame.map(|frame| frame.map_err(std::io::Error::other)))
            }
            Poll::Pending => {
                std::task::ready!(self.idle.as_mut().poll(cx));
                Poll::Ready(Some(Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Claude upstream response idle timeout",
                ))))
            }
        }
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
impl Drop for ClaudeBody {
    fn drop(&mut self) {
        if let Some(session) = &self.session {
            session.active.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::{AffinityStore, Router};
    use http_body_util::Full;
    use hyper::{HeaderMap, service::service_fn};
    use hyper_util::rt::TokioIo;
    use serde_json::json;
    use std::convert::Infallible;
    use tokio::net::TcpListener;

    const SESSION: &str = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
    const ACCOUNT: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    const OTHER: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    const STREAM:&[u8]=b": keepalive\r\nevent: ping\r\ndata: {}\r\n\r\nevent: future_event\ndata: {\"unknown\":true}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
    fn request_body() -> Vec<u8> {
        serde_json::to_vec(&json!({"model":"claude-sonnet-5","max_tokens":32,"stream":true,"metadata":{"user_id":json!({"account_uuid":ACCOUNT,"device_id":"a".repeat(64),"session_id":SESSION}).to_string()},"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.281.127; cc_entrypoint=sdk-cli;"}],"messages":[{"role":"user","content":"Ada Lovelace says hello."}],"future_field":[1,2,3]})).unwrap()
    }
    fn native_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (key, value) in [
            ("authorization", "Bearer sk-ant-oat01-caller"),
            ("user-agent", "claude-cli/2.1.281 (external, sdk-cli)"),
            ("x-app", "cli"),
            ("x-claude-code-session-id", SESSION),
            (
                "anthropic-beta",
                "claude-code-20250219,oauth-2025-04-20,new-future-beta",
            ),
            ("anthropic-version", "2023-06-01"),
            ("content-type", "application/json"),
        ] {
            headers.insert(key, value.parse().unwrap());
        }
        headers
    }
    struct Harness {
        _directory: tempfile::TempDir,
        app: Arc<App>,
        url: String,
        seen: Arc<Mutex<Vec<(HeaderMap, Bytes, String)>>>,
        release_stream: Arc<tokio::sync::Notify>,
        upstream_task: tokio::task::JoinHandle<()>,
        downstream_task: tokio::task::JoinHandle<Result<()>>,
    }
    impl Harness {
        async fn new(managed: bool, status: StatusCode) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let upstream_url = format!("http://{}", upstream.local_addr().unwrap());
            let seen = Arc::new(Mutex::new(Vec::new()));
            let observed = seen.clone();
            let release_stream = Arc::new(tokio::sync::Notify::new());
            let release = release_stream.clone();
            let upstream_task = tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = upstream.accept().await else {
                        break;
                    };
                    let seen = observed.clone();
                    let release = release.clone();
                    tokio::spawn(async move {
                        let service = service_fn(move |req: Request<Incoming>| {
                            let seen = seen.clone();
                            let release = release.clone();
                            async move {
                                let (parts, body) = req.into_parts();
                                let body = body.collect().await.unwrap().to_bytes();
                                let first = parts
                                    .headers
                                    .get("authorization")
                                    .is_some_and(|v| v.as_bytes() == b"Bearer sk-ant-oat01-grace");
                                let hold = parts.headers.contains_key("x-test-hold");
                                let reject = parts.headers.contains_key("x-test-quota");
                                let ambiguous = parts.headers.contains_key("x-test-ambiguous");
                                let usage = parts.uri.path() == "/api/oauth/usage";
                                let token = parts.uri.path() == "/token";
                                seen.lock().await.push((
                                    parts.headers,
                                    body,
                                    parts.uri.to_string(),
                                ));
                                let response = if token {
                                    Response::builder().body(Full::new(Bytes::from_static(br#"{"access_token":"sk-ant-oat01-refreshed","refresh_token":"synthetic-rotated","expires_in":3600}"#)).boxed()).unwrap()
                                } else if usage {
                                    let reset = chrono::DateTime::from_timestamp(
                                        (auth::now() + 3600) as i64,
                                        0,
                                    )
                                    .unwrap()
                                    .to_rfc3339();
                                    let mut data = json!({"five_hour":{"utilization":if first {100}else{25},"resets_at":reset},"seven_day":{"utilization":10,"resets_at":reset},"cedar_ember":{"eligible":true,"next_grant_id":"mock-grant","grants":[{"id":"mock-grant","resets_left":2,"usable_now":true}]}});
                                    if first {
                                        data["seven_day_fable"] =
                                            json!({"utilization":25.5,"resets_at":reset});
                                    } else {
                                        data["limits"] = json!([
                                            {"kind":"session","group":"session","percent":25,"resets_at":reset,"scope":null,"is_active":true,"severity":"normal"},
                                            {"kind":"weekly_all","group":"weekly","percent":10,"resets_at":reset,"scope":null,"is_active":true,"severity":"normal"},
                                            {"kind":"weekly_scoped","group":"weekly","percent":100,"resets_at":reset,"scope":{"model":{"id":null,"display_name":"Fable"},"surface":null},"is_active":false,"severity":"critical"}
                                        ]);
                                    }
                                    Response::builder()
                                        .status(if status == StatusCode::TOO_MANY_REQUESTS {
                                            status
                                        } else {
                                            StatusCode::OK
                                        })
                                        // Anthropic's usage endpoint throttles with `retry-after: 0`.
                                        .header("retry-after", "0")
                                        .body(
                                            Full::new(Bytes::from(
                                                serde_json::to_vec(&data).unwrap(),
                                            ))
                                            .boxed(),
                                        )
                                        .unwrap()
                                } else if ambiguous {
                                    Response::builder()
                                        .status(StatusCode::TOO_MANY_REQUESTS)
                                        .header("retry-after", "10")
                                        .body(
                                            Full::new(Bytes::from_static(b"ambiguous limit"))
                                                .boxed(),
                                        )
                                        .unwrap()
                                } else if first
                                    && (status == StatusCode::TOO_MANY_REQUESTS || reject)
                                {
                                    Response::builder()
                                        .status(StatusCode::TOO_MANY_REQUESTS)
                                        .header("anthropic-ratelimit-unified-5h-status", "rejected")
                                        .header(
                                            "anthropic-ratelimit-unified-5h-reset",
                                            (auth::now() + 3600).to_string(),
                                        )
                                        .body(
                                            Full::new(Bytes::from_static(
                                                b"{\"error\":{\"type\":\"rate_limit_error\"}}",
                                            ))
                                            .boxed(),
                                        )
                                        .unwrap()
                                } else {
                                    let status = if status == StatusCode::TOO_MANY_REQUESTS {
                                        StatusCode::OK
                                    } else {
                                        status
                                    };
                                    let body = if hold {
                                        http_body_util::StreamBody::new(futures_util::stream::once(
                                            async move {
                                                release.notified().await;
                                                Ok::<_, Infallible>(Frame::data(
                                                    Bytes::from_static(STREAM),
                                                ))
                                            },
                                        ))
                                        .boxed()
                                    } else {
                                        Full::new(Bytes::from_static(STREAM)).boxed()
                                    };
                                    Response::builder()
                                        .status(status)
                                        .header("content-type", "text/event-stream")
                                        .header("x-request-id", "native-id")
                                        .body(body)
                                        .unwrap()
                                };
                                Ok::<_, Infallible>(response)
                            }
                        });
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    });
                }
            });
            let mut config: Config = toml::from_str(
                r#"
[proxy]
installation_secret="claude-test-secret"
affinity_key="0123456789abcdef0123456789abcdef"
[listeners.claude]
address="127.0.0.1:0"
pool="claude"
[pools.claude]
members=["caller"]
[accounts.caller]
kind="claude_inbound"
"#,
            )
            .unwrap();
            config.proxy.state_dir = Some(directory.path().to_owned());
            if managed {
                config.accounts.clear();
                config.pools.get_mut("claude").unwrap().members =
                    vec!["grace".into(), "ada".into()];
                config.pools.get_mut("claude").unwrap().preferred = Some("grace".into());
                for (name, account, device) in [("grace", ACCOUNT, 'a'), ("ada", OTHER, 'b')] {
                    let path = directory.path().join(name);
                    std::fs::create_dir_all(&path).unwrap();
                    std::fs::write(path.join("claude-auth.json"),serde_json::to_vec(&json!({"access_token":format!("sk-ant-oat01-{name}"),"refresh_token":"synthetic","expires_at":auth::now()+3600,"account_uuid":account,"organization_uuid":OTHER,"device_id":device.to_string().repeat(64)})).unwrap()).unwrap();
                    config
                        .accounts
                        .insert(name.into(), AccountConfig::ClaudeHome { path });
                }
            }
            let affinity = Arc::new(
                AffinityStore::load(
                    directory.path().join("affinity.json"),
                    &config.proxy.affinity_key,
                    Duration::from_secs(3600),
                )
                .unwrap(),
            );
            let router = Arc::new(Router::new(&config, affinity));
            let listener = config.listeners["claude"].clone();
            let mut app =
                App::new_unvalidated(Arc::new(config), router, Arc::new(Stats::default())).unwrap();
            Arc::get_mut(&mut app).unwrap().claude.usage_url =
                format!("{upstream_url}/api/oauth/usage");
            Arc::get_mut(&mut app)
                .unwrap()
                .claude
                .auth
                .use_test_endpoint(&format!("{upstream_url}/token"));
            Arc::get_mut(&mut app).unwrap().claude.upstream = upstream_url;
            let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/claude-test-secret", tcp.local_addr().unwrap());
            let downstream_task =
                tokio::spawn(app.clone().serve_tcp("claude".into(), listener, tcp));
            Self {
                _directory: directory,
                app,
                url,
                seen,
                release_stream,
                upstream_task,
                downstream_task,
            }
        }
        async fn send(&self, body: Vec<u8>, headers: HeaderMap) -> reqwest::Response {
            reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap()
                .post(format!("{}/v1/messages?beta=true", self.url))
                .headers(headers)
                .body(body)
                .send()
                .await
                .unwrap()
        }
        async fn close(self) {
            self.app.shutdown_connections().await;
            self.upstream_task.abort();
            self.downstream_task.abort();
        }
    }
    #[tokio::test]
    async fn native_bytes_headers_and_sse_are_forwarded_unchanged() {
        let harness = Harness::new(false, StatusCode::OK).await;
        let body = request_body();
        let headers = native_headers();
        let response = harness.send(body.clone(), headers.clone()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-request-id"], "native-id");
        assert_eq!(response.bytes().await.unwrap(), STREAM);
        {
            let seen = harness.seen.lock().await;
            assert_eq!(seen.len(), 1);
            assert_eq!(seen[0].1, body);
            assert_eq!(seen[0].0["anthropic-beta"], headers["anthropic-beta"]);
            assert_eq!(seen[0].0["authorization"], headers["authorization"]);
            assert_eq!(seen[0].2, "/v1/messages?beta=true");
        }
        harness.close().await;
    }
    #[tokio::test]
    async fn shared_quota_rolls_over_once_and_keeps_the_replacement_sticky() {
        let harness = Harness::new(true, StatusCode::TOO_MANY_REQUESTS).await;
        let original = request_body();
        for _ in 0..2 {
            let response = harness.send(original.clone(), native_headers()).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.bytes().await.unwrap(), STREAM);
        }
        {
            let seen = harness.seen.lock().await;
            assert_eq!(seen.len(), 3);
            assert_eq!(seen[0].0["authorization"], "Bearer sk-ant-oat01-grace");
            assert_eq!(seen[1].0["authorization"], "Bearer sk-ant-oat01-ada");
            assert_eq!(seen[2].0["authorization"], "Bearer sk-ant-oat01-ada");
            assert_eq!(
                seen[1].1,
                wire::rewrite(&original, OTHER, &"b".repeat(64)).unwrap()
            );
        }
        harness.close().await;
    }
    #[tokio::test]
    async fn foreign_client_never_reaches_upstream() {
        let harness = Harness::new(false, StatusCode::OK).await;
        let mut headers = native_headers();
        headers.insert("user-agent", "OpenCode/1.0".parse().unwrap());
        assert_eq!(
            harness.send(request_body(), headers).await.status(),
            StatusCode::FORBIDDEN
        );
        assert!(harness.seen.lock().await.is_empty());
        harness.close().await;
    }

    #[tokio::test]
    async fn native_typescript_sdk_request_keeps_its_original_attribution() {
        let harness = Harness::new(false, StatusCode::OK).await;
        let ua = "claude-cli/2.1.281 (external, sdk-ts, agent-sdk/0.3.276)";
        let mut headers = native_headers();
        headers.insert("user-agent", ua.parse().unwrap());
        let body = String::from_utf8(request_body())
            .unwrap()
            .replace("cc_entrypoint=sdk-cli;", "cc_entrypoint=sdk-ts;")
            .into_bytes();
        let response = harness.send(body.clone(), headers).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap(), STREAM);
        let seen = harness.seen.lock().await;
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0["user-agent"], ua);
        assert_eq!(seen[0].1, body);
        drop(seen);
        harness.close().await;
    }

    #[tokio::test]
    async fn claude_background_usage_skips_limited_preferred_account_before_inference() {
        let harness = Harness::new(true, StatusCode::OK).await;
        assert!(
            harness
                .app
                .refresh_claude_usage_at(auth::now(), false)
                .await
        );
        let status = harness.app.router.routing_snapshot().await;
        assert!(!status.account_states["grace"].available);
        assert_eq!(status.account_states["grace"].usage_percent, Some(100));
        assert_eq!(status.account_states["ada"].usage_percent, Some(25));
        let response = harness.send(request_body(), native_headers()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.bytes().await;
        let seen = harness.seen.lock().await;
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[2].0["authorization"], "Bearer sk-ant-oat01-ada");
        assert!(
            seen[..2]
                .iter()
                .all(|r| r.0["user-agent"].as_bytes().starts_with(b"comradex/"))
        );
        drop(seen);
        harness.close().await;
    }

    #[tokio::test]
    async fn claude_background_refresh_and_foreground_share_one_rotating_grant() {
        let harness = Harness::new(true, StatusCode::OK).await;
        let path = harness.app.config.accounts["grace"]
            .home()
            .unwrap()
            .join("claude-auth.json");
        let mut credential: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        credential["expires_at"] = (auth::now() - 1).into();
        std::fs::write(&path, serde_json::to_vec(&credential).unwrap()).unwrap();
        let (_, response) = tokio::join!(
            harness.app.refresh_managed_accounts_at(auth::now()),
            harness.send(request_body(), native_headers())
        );
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.bytes().await;
        let updated = auth::read(path.parent().unwrap()).unwrap();
        assert_eq!(updated.refresh_token, "synthetic-rotated");
        assert_eq!(updated.account_uuid, ACCOUNT);
        assert_eq!(
            harness
                .seen
                .lock()
                .await
                .iter()
                .filter(|r| r.2 == "/token")
                .count(),
            1
        );
        assert_eq!(
            harness
                .app
                .stats
                .refresh_accounts_checked
                .load(Ordering::Relaxed),
            2
        );
        harness.close().await;
    }

    #[tokio::test]
    async fn claude_usage_rate_limit_does_not_spin_or_poison_inference_quota() {
        let harness = Harness::new(true, StatusCode::TOO_MANY_REQUESTS).await;
        assert!(
            !harness
                .app
                .refresh_claude_usage_at(auth::now(), false)
                .await
        );
        assert!(
            harness
                .app
                .refresh_claude_usage_at(auth::now() + 1, false)
                .await
        );
        assert_eq!(harness.seen.lock().await.len(), 2);
        assert!(harness.app.router.routing_snapshot().await.account_states["grace"].available);
        harness.close().await;
    }

    #[tokio::test]
    async fn claude_usage_success_keeps_the_normal_poll_interval() {
        let harness = Harness::new(true, StatusCode::OK).await;
        let now = auth::now();
        assert!(harness.app.refresh_claude_usage_at(now, false).await);
        let polled = harness.seen.lock().await.len();
        // Another account's failure reruns the shared usage loop within a minute.
        assert!(harness.app.refresh_claude_usage_at(now + 60, false).await);
        assert_eq!(harness.seen.lock().await.len(), polled);
        // An explicit refresh, such as the menu bar Refresh action, polls right away.
        assert!(harness.app.refresh_claude_usage_at(now + 60, true).await);
        assert!(harness.seen.lock().await.len() > polled);
        let polled = harness.seen.lock().await.len();
        let next = now + 60 + crate::usage::REFRESH_INTERVAL_SECONDS;
        assert!(harness.app.refresh_claude_usage_at(next, false).await);
        assert!(harness.seen.lock().await.len() > polled);
        harness.close().await;
    }

    #[tokio::test]
    async fn claude_usage_zero_retry_after_backs_off_from_the_normal_poll_interval() {
        let harness = Harness::new(true, StatusCode::TOO_MANY_REQUESTS).await;
        let now = auth::now();
        assert!(!harness.app.refresh_claude_usage_at(now, false).await);
        assert_eq!(harness.seen.lock().await.len(), 2);
        // A zero retry-after must not become a one-minute polling loop.
        assert!(harness.app.refresh_claude_usage_at(now + 61, false).await);
        assert_eq!(harness.seen.lock().await.len(), 2);
        // Provider throttling holds even an explicit refresh.
        assert!(harness.app.refresh_claude_usage_at(now + 61, true).await);
        assert_eq!(harness.seen.lock().await.len(), 2);
        let next = now + crate::usage::REFRESH_INTERVAL_SECONDS;
        assert!(!harness.app.refresh_claude_usage_at(next, false).await);
        assert_eq!(harness.seen.lock().await.len(), 4);
        // Continued throttling doubles the wait.
        let doubled = next + crate::usage::REFRESH_INTERVAL_SECONDS;
        assert!(harness.app.refresh_claude_usage_at(doubled, false).await);
        assert_eq!(harness.seen.lock().await.len(), 4);
        assert!(
            !harness
                .app
                .refresh_claude_usage_at(next + 2 * crate::usage::REFRESH_INTERVAL_SECONDS, false)
                .await
        );
        assert_eq!(harness.seen.lock().await.len(), 6);
        harness.close().await;
    }

    #[tokio::test]
    async fn usage_poll_started_before_a_quota_rejection_keeps_it() {
        let harness = Harness::new(true, StatusCode::OK).await;
        let owner = auth::read(harness.app.config.accounts["ada"].home().unwrap())
            .unwrap()
            .owner();
        let router = &harness.app.router;
        let now = auth::now();
        router.claude_quota_until("ada", now + 3600, &owner).await;
        let usage = br#"{"five_hour":{"utilization":25,"resets_at":null},"seven_day":null}"#;
        let stale = crate::claude::maintenance::parse_usage(usage, now - 5).unwrap();
        router
            .observe_claude_usage_for_owner("ada", stale, &owner)
            .await;
        assert!(!router.routing_snapshot().await.account_states["ada"].available);
        let fresh = crate::claude::maintenance::parse_usage(usage, now + 1).unwrap();
        router
            .observe_claude_usage_for_owner("ada", fresh, &owner)
            .await;
        assert!(router.routing_snapshot().await.account_states["ada"].available);
        harness.close().await;
    }

    #[tokio::test]
    async fn claude_partial_inference_usage_preserves_the_other_shared_window() {
        let harness = Harness::new(true, StatusCode::OK).await;
        assert!(
            harness
                .app
                .refresh_claude_usage_at(auth::now(), false)
                .await
        );
        let credential = auth::read(harness.app.config.accounts["ada"].home().unwrap()).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "anthropic-ratelimit-unified-5h-utilization",
            "0.30".parse().unwrap(),
        );
        headers.insert(
            "anthropic-ratelimit-unified-5h-reset",
            (auth::now() + 3600).to_string().parse().unwrap(),
        );
        harness
            .app
            .router
            .observe_usage_snapshot_for_owner(
                "ada",
                quota::snapshot(&headers, auth::now()).unwrap(),
                &credential.owner(),
            )
            .await;
        let state = harness.app.router.routing_snapshot().await;
        assert_eq!(
            state.account_states["ada"].usage_windows["5h"].used_percent,
            Some(30)
        );
        assert_eq!(
            state.account_states["ada"]
                .usage_windows
                .get("7d")
                .and_then(|w| w.used_percent),
            Some(10)
        );
        harness.close().await;
    }

    #[tokio::test]
    async fn claude_prefer_preserve_changes_apply_to_new_work_and_keep_existing_sessions() {
        let harness = Harness::new(true, StatusCode::OK).await;
        let first = harness.send(request_body(), native_headers()).await;
        assert_eq!(first.status(), StatusCode::OK);
        let _ = first.bytes().await;
        harness
            .app
            .router
            .set_preferred("claude", Some("ada".into()))
            .await;
        harness
            .app
            .router
            .set_preserved("claude", Some("grace".into()))
            .await;
        let same = harness.send(request_body(), native_headers()).await;
        assert_eq!(same.status(), StatusCode::OK);
        let _ = same.bytes().await;
        let fresh_session = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
        let fresh = String::from_utf8(request_body())
            .unwrap()
            .replace(SESSION, fresh_session)
            .into_bytes();
        let mut headers = native_headers();
        headers.insert("x-claude-code-session-id", fresh_session.parse().unwrap());
        let response = harness.send(fresh, headers).await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.bytes().await;
        let seen = harness.seen.lock().await;
        assert_eq!(seen[0].0["authorization"], "Bearer sk-ant-oat01-grace");
        assert_eq!(seen[1].0["authorization"], seen[0].0["authorization"]);
        assert_eq!(seen[2].0["authorization"], "Bearer sk-ant-oat01-ada");
        drop(seen);
        harness.close().await;
    }
    #[tokio::test]
    async fn permission_and_capability_errors_do_not_rotate_accounts() {
        for status in [
            StatusCode::FORBIDDEN,
            StatusCode::BAD_REQUEST,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            let harness = Harness::new(true, status).await;
            let response = harness.send(request_body(), native_headers()).await;
            assert_eq!(response.status(), status);
            assert_eq!(response.bytes().await.unwrap(), STREAM);
            assert_eq!(harness.seen.lock().await.len(), 1);
            harness.close().await;
        }
    }
    /// Reports fresh shared 5-hour and 7-day usage for a managed account.
    async fn report_usage(harness: &Harness, account: &str, utilization: &str) {
        let mut headers = HeaderMap::new();
        for window in ["5h", "7d"] {
            headers.insert(
                hyper::header::HeaderName::try_from(format!(
                    "anthropic-ratelimit-unified-{window}-utilization"
                ))
                .unwrap(),
                utilization.parse().unwrap(),
            );
        }
        let home = harness.app.config.accounts[account].home().unwrap();
        let owner = auth::read(home).unwrap().owner();
        harness
            .app
            .router
            .observe_claude_usage_for_owner(
                account,
                quota::snapshot(&headers, auth::now() + 1).unwrap(),
                &owner,
            )
            .await;
    }
    async fn inference_accounts(harness: &Harness) -> Vec<String> {
        harness
            .seen
            .lock()
            .await
            .iter()
            .filter(|r| r.2.starts_with("/v1/messages"))
            .map(|r| {
                r.0["authorization"]
                    .to_str()
                    .unwrap()
                    .replace("Bearer sk-ant-oat01-", "")
            })
            .collect()
    }
    fn session_request(session: &str) -> (Vec<u8>, HeaderMap) {
        let body = String::from_utf8(request_body())
            .unwrap()
            .replace(SESSION, session)
            .into_bytes();
        let mut headers = native_headers();
        headers.insert("x-claude-code-session-id", session.parse().unwrap());
        (body, headers)
    }
    #[tokio::test]
    async fn helper_on_another_account_does_not_take_over_signed_conversation() {
        let harness = Harness::new(true, StatusCode::OK).await;
        let response = harness.send(request_body(), native_headers()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.bytes().await.unwrap();
        // Maintenance sends the helper elsewhere without touching grace's quota.
        assert!(harness.app.router.begin_login("grace").await);
        let mut helper: serde_json::Value = serde_json::from_slice(&request_body()).unwrap();
        helper["model"] = "claude-haiku-4-5".into();
        helper["messages"] = json!([{"role":"user","content":"Write a short title."}]);
        let response = harness
            .send(serde_json::to_vec(&helper).unwrap(), native_headers())
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.bytes().await.unwrap();
        harness.app.router.finish_login("grace", false).await;
        let mut body: serde_json::Value = serde_json::from_slice(&request_body()).unwrap();
        body["messages"] = json!([{"role":"user","content":"Ada Lovelace says hello."},{"role":"assistant","content":[{"type":"thinking","thinking":"synthetic","signature":"opaque"}]},{"role":"user","content":"continue"}]);
        let response = harness
            .send(serde_json::to_vec(&body).unwrap(), native_headers())
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.bytes().await.unwrap();
        assert_eq!(
            inference_accounts(&harness).await,
            ["grace", "ada", "grace"]
        );
        harness.close().await;
    }
    #[tokio::test]
    async fn signed_conversation_moves_once_when_its_owner_exhausts_quota() {
        let harness = Harness::new(true, StatusCode::OK).await;
        let response = harness.send(request_body(), native_headers()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.bytes().await.unwrap();
        assert!(
            harness
                .app
                .refresh_claude_usage_at(auth::now(), false)
                .await
        );
        // Files still require their account even when the rest of the history can move.
        let mut body: serde_json::Value = serde_json::from_slice(&request_body()).unwrap();
        body["messages"] = json!([{"role":"user","content":[{"type":"document","source":{"type":"file","file_id":"file_1"}}]},{"role":"user","content":"continue"}]);
        let response = harness
            .send(serde_json::to_vec(&body).unwrap(), native_headers())
            .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(inference_accounts(&harness).await, ["grace"]);
        body["messages"] = json!([{"role":"user","content":"Ada Lovelace says hello."},{"role":"assistant","content":[{"type":"thinking","thinking":"synthetic","signature":"opaque"}]},{"role":"user","content":"continue"}]);
        let response = harness
            .send(serde_json::to_vec(&body).unwrap(), native_headers())
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.bytes().await.unwrap();
        // The new owner keeps the conversation after the old account recovers.
        report_usage(&harness, "grace", "0.1").await;
        let response = harness
            .send(serde_json::to_vec(&body).unwrap(), native_headers())
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.bytes().await.unwrap();
        assert_eq!(inference_accounts(&harness).await, ["grace", "ada", "ada"]);
        assert_eq!(
            harness.seen.lock().await.last().unwrap().1,
            wire::rewrite(&serde_json::to_vec(&body).unwrap(), OTHER, &"b".repeat(64)).unwrap()
        );
        let session = harness
            .app
            .router
            .affinity
            .key(&format!("claude:claude:{SESSION}"));
        assert_eq!(
            harness
                .app
                .router
                .affinity
                .get(&session)
                .await
                .unwrap()
                .account_id,
            "ada"
        );
        harness.close().await;
    }
    #[tokio::test]
    async fn compaction_migrates_and_keeps_its_new_home_across_later_compactions() {
        for kind in ["text", "compaction", "signed_compaction"] {
            let messages = |summary: &str| {
                let first = match kind {
                    "text" => json!({"role":"user","content":summary}),
                    "compaction" => {
                        json!({"role":"assistant","content":[{"type":"compaction","content":summary}]})
                    }
                    _ => {
                        json!({"role":"assistant","content":[{"type":"compaction","content":summary,"signature":format!("{summary}-signature") }]})
                    }
                };
                json!([first, {"role":"user","content":"continue"}])
            };
            for upstream_rejection in [false, true] {
                let harness = Harness::new(true, StatusCode::OK).await;
                let response = harness.send(request_body(), native_headers()).await;
                assert_eq!(response.status(), StatusCode::OK);
                let _ = response.bytes().await.unwrap();
                let mut body: serde_json::Value = serde_json::from_slice(&request_body()).unwrap();
                body["messages"] = messages("First summary");
                let response = harness
                    .send(serde_json::to_vec(&body).unwrap(), native_headers())
                    .await;
                assert_eq!(response.status(), StatusCode::OK);
                let _ = response.bytes().await.unwrap();
                assert_eq!(inference_accounts(&harness).await, ["grace", "grace"]);

                // A new summary has no conversation binding yet; it inherits the session home.
                body["messages"] = messages("Second summary");
                let mut headers = native_headers();
                if upstream_rejection {
                    headers.insert("x-test-quota", "true".parse().unwrap());
                } else {
                    report_usage(&harness, "grace", "1.0").await;
                }
                let response = harness
                    .send(serde_json::to_vec(&body).unwrap(), headers)
                    .await;
                assert_eq!(response.status(), StatusCode::OK);
                let _ = response.bytes().await.unwrap();
                assert_eq!(
                    harness.seen.lock().await.last().unwrap().1,
                    wire::rewrite(&serde_json::to_vec(&body).unwrap(), OTHER, &"b".repeat(64))
                        .unwrap()
                );

                report_usage(&harness, "grace", "0.1").await;
                body["messages"] = messages("Third summary");
                let response = harness
                    .send(serde_json::to_vec(&body).unwrap(), native_headers())
                    .await;
                assert_eq!(response.status(), StatusCode::OK);
                let _ = response.bytes().await.unwrap();
                let expected = if upstream_rejection {
                    vec!["grace", "grace", "grace", "ada", "ada"]
                } else {
                    vec!["grace", "grace", "ada", "ada"]
                };
                assert_eq!(inference_accounts(&harness).await, expected);
                harness.close().await;
            }
        }
    }
    #[tokio::test]
    async fn new_sessions_balance_remaining_quota_across_active_sessions() {
        let harness = Harness::new(true, StatusCode::OK).await;
        harness.app.router.set_preferred("claude", None).await;
        let sessions = [
            SESSION,
            "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
            "ffffffff-ffff-4fff-8fff-ffffffffffff",
            "99999999-9999-4999-8999-999999999999",
        ];
        for session in &sessions[..3] {
            let (body, headers) = session_request(session);
            let response = harness.send(body, headers).await;
            assert_eq!(response.status(), StatusCode::OK);
            let _ = response.bytes().await.unwrap();
        }
        assert_eq!(
            inference_accounts(&harness).await,
            ["grace", "ada", "grace"]
        );
        // grace keeps 40% for two sessions; ada keeps 90% for one, then for two.
        report_usage(&harness, "grace", "0.6").await;
        report_usage(&harness, "ada", "0.1").await;
        for session in [sessions[3], sessions[4], sessions[0]] {
            let (body, headers) = session_request(session);
            let response = harness.send(body, headers).await;
            assert_eq!(response.status(), StatusCode::OK);
            let _ = response.bytes().await.unwrap();
        }
        assert_eq!(
            inference_accounts(&harness).await,
            ["grace", "ada", "grace", "ada", "ada", "grace"]
        );
        harness.close().await;
    }
    #[tokio::test]
    async fn token_count_does_not_block_a_signed_resume_on_the_same_account() {
        let harness = Harness::new(true, StatusCode::OK).await;
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("{}/v1/messages/count_tokens?beta=true", harness.url))
            .headers(native_headers())
            .body(
                serde_json::to_vec(&json!({"model":"claude-sonnet-5","messages":[{"role":"user","content":"Ada Lovelace says hello."}]}))
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.bytes().await.unwrap();
        let mut body: serde_json::Value = serde_json::from_slice(&request_body()).unwrap();
        body["messages"] = json!([{"role":"user","content":"Ada Lovelace says hello."},{"role":"assistant","content":[{"type":"thinking","thinking":"synthetic","signature":"opaque"}]},{"role":"user","content":"continue"}]);
        let response = harness
            .send(serde_json::to_vec(&body).unwrap(), native_headers())
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.bytes().await.unwrap();
        harness.close().await;
    }
    #[tokio::test]
    async fn stalled_response_body_releases_its_slot() {
        let harness = Harness::new(true, StatusCode::OK).await;
        let mut headers = native_headers();
        headers.insert("x-test-hold", "true".parse().unwrap());
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("{}/v1/messages?beta=true", harness.url))
            .headers(headers)
            .body(request_body())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.bytes().await.is_err());
        assert_eq!(harness.app.stats.inflight_http.load(Ordering::Relaxed), 0);
        harness.close().await;
    }
    #[tokio::test]
    async fn cache_diagnostics_can_migrate_but_server_threads_cannot() {
        for (field, status, accounts) in [
            (
                json!({"diagnostics":{"previous_message_id":"msg_prior"}}),
                StatusCode::OK,
                &["grace", "ada"][..],
            ),
            (
                json!({"thread":{"type":"continue","previous_message_id":"msg_prior"}}),
                StatusCode::TOO_MANY_REQUESTS,
                &["grace"][..],
            ),
        ] {
            let harness = Harness::new(true, StatusCode::TOO_MANY_REQUESTS).await;
            let mut body: serde_json::Value = serde_json::from_slice(&request_body()).unwrap();
            body.as_object_mut()
                .unwrap()
                .extend(field.as_object().unwrap().clone());
            let response = harness
                .send(serde_json::to_vec(&body).unwrap(), native_headers())
                .await;
            assert_eq!(response.status(), status);
            let _ = response.bytes().await.unwrap();
            assert_eq!(inference_accounts(&harness).await, accounts);
            for (_, bytes, _) in harness.seen.lock().await.iter() {
                let sent: serde_json::Value = serde_json::from_slice(bytes).unwrap();
                for (key, value) in field.as_object().unwrap() {
                    assert_eq!(&sent[key], value);
                }
            }
            harness.close().await;
        }
    }
    #[tokio::test]
    async fn signed_continuation_moves_when_its_owner_rejects_quota() {
        for (content, status, accounts) in [
            (
                json!([{"type":"thinking","thinking":"synthetic","signature":"opaque"}]),
                StatusCode::OK,
                &["grace", "ada"][..],
            ),
            (
                json!([{"type":"compaction","content":"summary","signature":"opaque"}]),
                StatusCode::OK,
                &["grace", "ada"][..],
            ),
            (
                json!([
                    {"type":"server_tool_use","id":"srvtoolu_fetch","name":"web_fetch","input":{"url":"https://example.com"}},
                    {"type":"web_fetch_tool_result","tool_use_id":"srvtoolu_fetch","content":{"type":"web_fetch_result","content":{"type":"document","source":{"type":"text","media_type":"text/plain","data":"Fetched text"}}}}
                ]),
                StatusCode::OK,
                &["grace", "ada"][..],
            ),
            // Claude Code surfaces deferred tools as definitions whose schemas can name
            // container or file_id properties. Declarations are not owned state.
            (
                json!([
                    {"type":"thinking","thinking":"synthetic","signature":"opaque"},
                    {"type":"tool_addition","tool":{"type":"tool_definition","definition":{"name":"bash","description":"Run a command","input_schema":{"type":"object","properties":{"container":{"type":"string"},"file_id":{"type":"string"}}}}}}
                ]),
                StatusCode::OK,
                &["grace", "ada"][..],
            ),
            (
                json!([{"type":"compaction","content":"summary","signature":"opaque"},{"type":"document","source":{"type":"file","file_id":"file_1"}}]),
                StatusCode::TOO_MANY_REQUESTS,
                &["grace"][..],
            ),
            (
                json!([{"type":"thinking","thinking":"synthetic","signature":"opaque"},{"type":"server_tool_use","id":"owned"}]),
                StatusCode::TOO_MANY_REQUESTS,
                &["grace"][..],
            ),
        ] {
            let harness = Harness::new(true, StatusCode::TOO_MANY_REQUESTS).await;
            let mut body: serde_json::Value = serde_json::from_slice(&request_body()).unwrap();
            body["messages"] = json!([{"role":"assistant","content":content},{"role":"user","content":"continue"}]);
            let response = harness
                .send(serde_json::to_vec(&body).unwrap(), native_headers())
                .await;
            assert_eq!(response.status(), status);
            let _ = response.bytes().await.unwrap();
            assert_eq!(inference_accounts(&harness).await, accounts);
            harness.close().await;
        }
    }
    #[tokio::test]
    async fn overlapping_generation_moves_only_when_its_owner_rejects_quota() {
        let harness = Harness::new(true, StatusCode::OK).await;
        let mut headers = native_headers();
        headers.insert("x-test-hold", "true".parse().unwrap());
        let first = harness.send(request_body(), headers).await;
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(harness.app.stats.inflight_http.load(Ordering::Relaxed), 1);
        let second = harness.send(request_body(), native_headers()).await;
        assert_eq!(second.status(), StatusCode::OK);
        let _ = second.bytes().await.unwrap();
        let mut headers = native_headers();
        headers.insert("x-test-quota", "true".parse().unwrap());
        let third = harness.send(request_body(), headers).await;
        assert_eq!(third.status(), StatusCode::OK);
        let _ = third.bytes().await.unwrap();
        assert_eq!(
            inference_accounts(&harness).await,
            ["grace", "grace", "grace", "ada"]
        );
        harness.release_stream.notify_one();
        assert_eq!(first.bytes().await.unwrap(), STREAM);
        harness.close().await;
    }
    #[tokio::test]
    async fn ambiguous_quota_is_returned_without_rotating() {
        let harness = Harness::new(true, StatusCode::OK).await;
        let mut headers = native_headers();
        headers.insert("x-test-ambiguous", "true".parse().unwrap());
        let response = harness.send(request_body(), headers).await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.bytes().await.unwrap(),
            b"ambiguous limit".as_slice()
        );
        assert_eq!(harness.seen.lock().await.len(), 1);
        harness.close().await;
    }
    #[tokio::test]
    async fn signed_state_after_rollover_uses_proven_owner_and_survives_durable_reload() {
        let harness = Harness::new(true, StatusCode::TOO_MANY_REQUESTS).await;
        let response = harness.send(request_body(), native_headers()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.bytes().await.unwrap();
        let mut body: serde_json::Value = serde_json::from_slice(&request_body()).unwrap();
        body["messages"] = json!([{"role":"assistant","content":[{"type":"thinking","thinking":"synthetic","signature":"opaque"}]},{"role":"user","content":"continue"}]);
        let response = harness
            .send(serde_json::to_vec(&body).unwrap(), native_headers())
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.bytes().await.unwrap();
        let durable = AffinityStore::load(
            harness._directory.path().join("affinity.json"),
            &harness.app.config.proxy.affinity_key,
            Duration::from_secs(3600),
        )
        .unwrap();
        assert_eq!(
            durable
                .get(&durable.key(&format!("claude:claude:{SESSION}")))
                .await
                .unwrap()
                .account_id,
            "ada"
        );
        let identity = durable.key(&format!(
            "claude-credential:claude:claude:{SESSION}:ada:{OTHER}:{OTHER}"
        ));
        assert!(durable.get(&identity).await.is_some());
        let path = harness.app.config.accounts["ada"]
            .home()
            .unwrap()
            .join("claude-auth.json");
        let mut changed: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        changed["organization_uuid"] = ACCOUNT.into();
        std::fs::write(path, serde_json::to_vec(&changed).unwrap()).unwrap();
        let mut owned = body.clone();
        owned["messages"][1]["content"] = json!([{"type":"text","text":"continue"},{"type":"document","source":{"type":"file","file_id":"file_1"}}]);
        assert_eq!(
            harness
                .send(serde_json::to_vec(&owned).unwrap(), native_headers())
                .await
                .status(),
            StatusCode::CONFLICT
        );
        assert_eq!(harness.seen.lock().await.len(), 3);
        // Another organization's signatures are dropped upstream rather than rejected.
        let response = harness
            .send(serde_json::to_vec(&body).unwrap(), native_headers())
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.bytes().await.unwrap();
        assert_eq!(harness.seen.lock().await.len(), 4);
        harness.close().await;
    }
    #[tokio::test]
    #[ignore = "requires a synthetic local Claude Code capture; never use real credential captures"]
    async fn installed_native_capture_passes_without_body_or_header_reconstruction() {
        let file = std::env::var("COMRADEX_CLAUDE_CAPTURE").unwrap();
        let captures: serde_json::Value =
            serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
        let harness = Harness::new(false, StatusCode::OK).await;
        for capture in captures.as_array().unwrap() {
            let mut headers = HeaderMap::new();
            for (key, value) in capture["headers"].as_object().unwrap() {
                if ["host", "content-length", "connection"]
                    .contains(&key.to_ascii_lowercase().as_str())
                {
                    continue;
                }
                headers.insert(
                    key.parse::<hyper::header::HeaderName>().unwrap(),
                    value.as_str().unwrap().parse().unwrap(),
                );
            }
            let body = capture["body"].as_str().unwrap().as_bytes().to_vec();
            let response = harness.send(body.clone(), headers).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.bytes().await.unwrap(), STREAM);
            assert_eq!(harness.seen.lock().await.last().unwrap().1, body);
        }
        harness.close().await;
    }
    #[tokio::test]
    async fn background_usage_poll_reports_fable_without_changing_shared_quota() {
        let harness = Harness::new(true, StatusCode::OK).await;
        assert!(
            harness
                .app
                .refresh_claude_usage_at(auth::now(), false)
                .await
        );
        let before = harness.app.router.routing_snapshot().await;
        assert!(!before.account_states["grace"].available);
        assert!(before.account_states["ada"].available);
        assert_eq!(before.account_states["ada"].usage_percent, Some(25));
        for (account, percent) in [("grace", 25.5), ("ada", 100.0)] {
            for url in [
                "https://api.anthropic.com/api/oauth/usage",
                "https://api.anthropic.com/api/oauth/usage?cedar_ember=1&skip_spend=1",
            ] {
                let response = reqwest::Client::new()
                    .post(format!(
                        "{}/v0/management/api-call",
                        harness.url.strip_suffix("/claude-test-secret").unwrap()
                    ))
                    .bearer_auth(&harness.app.config.proxy.installation_secret)
                    .header("content-type", "application/json")
                    .body(json!({ "auth_index": account, "method": "GET", "url": url }).to_string())
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                let envelope: serde_json::Value =
                    serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
                assert_eq!(envelope["status_code"], 200);
                let body: serde_json::Value =
                    serde_json::from_str(envelope["body"].as_str().unwrap()).unwrap();
                let (utilization, reset) = if account == "grace" {
                    (
                        &body["seven_day_fable"]["utilization"],
                        &body["seven_day_fable"]["resets_at"],
                    )
                } else {
                    assert!(body.get("seven_day_fable").is_none());
                    assert_eq!(body["limits"][2]["scope"]["model"]["display_name"], "Fable");
                    (
                        &body["limits"][2]["percent"],
                        &body["limits"][2]["resets_at"],
                    )
                };
                assert_eq!(*utilization, percent);
                assert_eq!(
                    chrono::DateTime::parse_from_rfc3339(reset.as_str().unwrap()).unwrap(),
                    chrono::DateTime::parse_from_rfc3339(
                        body["seven_day"]["resets_at"].as_str().unwrap()
                    )
                    .unwrap()
                );
            }
        }
        assert_eq!(before, harness.app.router.routing_snapshot().await);
        assert_eq!(harness.seen.lock().await.len(), 2);
        let selection = harness
            .app
            .router
            .select("claude", &harness.app.config.pools["claude"], None, None)
            .await
            .unwrap();
        assert_eq!(selection.account_id, "ada");
        harness.close().await;
    }

    #[tokio::test]
    async fn background_usage_poll_collects_claude_reset_data_without_claiming() {
        let harness = Harness::new(true, StatusCode::OK).await;
        assert!(
            harness
                .app
                .refresh_claude_usage_at(auth::now(), false)
                .await
        );
        for account in ["grace", "ada"] {
            let credits = harness.app.claude_reset_credits(account).await.unwrap();
            assert_eq!(credits["grants"][0]["resets_left"], 2);
        }
        let seen = harness.seen.lock().await;
        assert_eq!(seen.len(), 2);
        assert!(seen.iter().all(|request| request.2
            == "/api/oauth/usage?cedar_ember=1&skip_spend=1"
            && request.1.is_empty()));
        drop(seen);
        harness.close().await;
    }
}
