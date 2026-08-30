mod headers;
mod replay_body;
#[allow(dead_code)]
mod sse;
#[allow(dead_code)]
mod websocket_protocol;

use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context as TaskContext, Poll},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use hyper::{
    Method, Request, Response, StatusCode, Uri,
    body::{Body, Frame, Incoming, SizeHint},
    header::{
        AUTHORIZATION, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HOST, LOCATION,
        SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE,
    },
    service::service_fn,
};
use hyper_rustls::HttpsConnector as RustlsHttpsConnector;
use hyper_tls::HttpsConnector as NativeHttpsConnector;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use tokio::{
    net::TcpListener,
    sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::{AbortHandle, JoinSet},
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::Role},
};
use tracing::{error, info, warn};

use crate::{
    auth::{self, Credentials},
    config::{Config, ListenerConfig, PoolConfig, ResponsesWebsocketMode},
    routing::{
        AffinityStore, Router,
        live::{self, LiveCallStore},
        metadata,
    },
    state::Stats,
    transport::{codex_http_connector, codex_websocket_connector},
};
use replay_body::{ProxyBody, ReplayBody, bytes_body, empty_body, incoming_body, json_body};
use sse::{ProtocolEvent, SseDecoder, responses_json_events};
use websocket_protocol::{
    DownstreamEndAction, FailureClassification, FailureKind, ProtocolLimits, ProtocolState,
    ReplayContext, ReplayMode, ReplayTarget, Settlement, TerminalKind, TurnEndDisposition, TurnId,
    UpstreamEnd, analyze_response_create, classify_failure, fresh_replay_without_previous_response,
};

const FILE_CREATE_RESPONSE_LIMIT: usize = 1024 * 1024;
const RESPONSES_JSON_RESPONSE_LIMIT: usize = 16 * 1024 * 1024;
const BRIDGE_SEND_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const HTTP_REQUEST_BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_RESPONSE_BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const WEBSOCKET_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const UNKNOWN_CONTENT_SNIFF_BYTES: usize = 4 * 1024;
const SSE_DECODE_SLICE_BYTES: usize = 64 * 1024;
const MAX_QUEUED_DIRECT_CREATES: usize = 64;
pub(super) const RESPONSES_MISSING_CREATED_TIMEOUT: Duration = Duration::from_secs(240);
pub(super) const RESPONSES_UPSTREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const RESPONSES_DIRECT_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const HTTP_BRIDGE_MAX_MATERIALIZED_ITEMS: usize = 4_096;

fn is_direct_hard_continuity(kind: metadata::AffinityKind) -> bool {
    matches!(
        kind,
        metadata::AffinityKind::TurnState
            | metadata::AffinityKind::PreviousResponse
            | metadata::AffinityKind::File
    )
}

type HttpClient = Client<NativeHttpsConnector<HttpConnector>, ProxyBody>;
type UpgradeHttpClient = Client<RustlsHttpsConnector<HttpConnector>, ProxyBody>;
type UpgradedWebSocket = WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>;

struct WebSocketFrameRoute {
    account_id: String,
    hard_owner: bool,
    non_previous_hard_owner: bool,
    soft_keys: Vec<crate::routing::ThreadKey>,
}

struct DirectTurn {
    route: WebSocketFrameRoute,
    request: Message,
    value: serde_json::Value,
}

struct DirectUpstream {
    socket: UpgradedWebSocket,
    credentials: Credentials,
}

#[derive(Debug, Clone)]
struct HttpBridgeContinuation {
    response_id: String,
    input: Vec<serde_json::Value>,
    output: Vec<serde_json::Value>,
}

fn has_suffix_prefix_overlap<T: PartialEq>(prefix: &[T], incoming: &[T]) -> bool {
    if prefix.is_empty() || incoming.is_empty() {
        return false;
    }

    // KMP keeps the overlap check linear even when histories contain long runs of
    // identical, deeply nested values.
    let mut failure = vec![0; incoming.len()];
    for index in 1..incoming.len() {
        let mut matched = failure[index - 1];
        while matched > 0 && incoming[index] != incoming[matched] {
            matched = failure[matched - 1];
        }
        if incoming[index] == incoming[matched] {
            matched += 1;
        }
        failure[index] = matched;
    }

    let mut matched = 0;
    for item in prefix {
        if matched == incoming.len() {
            matched = failure[matched - 1];
        }
        while matched > 0 && item != &incoming[matched] {
            matched = failure[matched - 1];
        }
        if item == &incoming[matched] {
            matched += 1;
        }
    }
    matched > 0
}

fn materialize_http_bridge_continuation(
    cached: HttpBridgeContinuation,
    incoming: Vec<serde_json::Value>,
) -> std::result::Result<Vec<serde_json::Value>, &'static str> {
    let mut prefix = cached.input;
    prefix.extend(cached.output);
    if prefix.len() > HTTP_BRIDGE_MAX_MATERIALIZED_ITEMS
        || incoming.len() > HTTP_BRIDGE_MAX_MATERIALIZED_ITEMS
    {
        return Err("materialized response.create input exceeds the bridge safety limit");
    }

    let materialized = if incoming.starts_with(&prefix) {
        // Some clients resend their complete local history while retaining the anchor.
        // In that case the cached prefix is already present and must not be duplicated.
        incoming
    } else {
        let shared_prefix = prefix
            .iter()
            .zip(&incoming)
            .take_while(|(left, right)| left == right)
            .count();
        let boundary_overlap = has_suffix_prefix_overlap(&prefix, &incoming);
        if shared_prefix != 0 || boundary_overlap {
            // A partial match can equally represent a truncated resend or a genuinely new,
            // identical item. Guessing either way risks duplicating or dropping conversation
            // history, so require the caller to send an unambiguous full resend or delta.
            return Err("incremental response.create input partially overlaps cached history");
        }
        let item_count = prefix
            .len()
            .checked_add(incoming.len())
            .ok_or("materialized response.create input exceeds the bridge safety limit")?;
        if item_count > HTTP_BRIDGE_MAX_MATERIALIZED_ITEMS {
            return Err("materialized response.create input exceeds the bridge safety limit");
        }
        prefix.extend(incoming);
        prefix
    };

    if materialized.len() > HTTP_BRIDGE_MAX_MATERIALIZED_ITEMS {
        return Err("materialized response.create input exceeds the bridge safety limit");
    }
    Ok(materialized)
}

struct HttpBridgeCapture {
    input: Vec<serde_json::Value>,
    response_id: Option<String>,
    output: Vec<serde_json::Value>,
    delivered_event: bool,
    response_created: bool,
    progress_events: u64,
}

#[derive(Debug)]
struct HttpBridgePumpFailure {
    error: anyhow::Error,
    delivered_event: bool,
    liveness: Option<HttpBridgeLivenessFailure>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HttpBridgeLivenessFailure {
    MissingResponseCreated,
    UpstreamIdle,
}

impl HttpBridgeLivenessFailure {
    fn code(self) -> &'static str {
        match self {
            Self::MissingResponseCreated => "missing_response_created_timeout",
            Self::UpstreamIdle => "upstream_idle_timeout",
        }
    }

    fn message(self) -> &'static str {
        match self {
            Self::MissingResponseCreated => "upstream did not create the response before timeout",
            Self::UpstreamIdle => "upstream response stream became idle before completion",
        }
    }
}

impl HttpBridgeCapture {
    fn observe(&mut self, event: &serde_json::Value) {
        let event_type = event.get("type").and_then(serde_json::Value::as_str);
        if event_type == Some("response.created") {
            self.response_created = true;
        }
        if self.response_created && event_type.is_some() {
            self.progress_events = self.progress_events.saturating_add(1);
        }
        if let Some(response_id) = event
            .get("response")
            .and_then(|response| response.get("id"))
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
        {
            self.response_id = Some(response_id.to_owned());
        }
        if event.get("type").and_then(serde_json::Value::as_str)
            == Some("response.output_item.done")
            && let Some(item) = event.get("item")
        {
            self.output.push(item.clone());
        }
    }
}

struct DirectAccountLease {
    router: Arc<Router>,
    account: Option<String>,
}

struct TrackedTask {
    abort: AbortHandle,
    done: oneshot::Receiver<()>,
}

impl TrackedTask {
    async fn cancel(mut self) {
        self.abort.abort();
        let _ = (&mut self.done).await;
    }

    async fn wait_timeout(mut self, timeout: Duration) {
        if tokio::time::timeout(timeout, &mut self.done).await.is_err() {
            self.abort.abort();
            let _ = (&mut self.done).await;
        }
    }
}

async fn wait_for_optional_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

fn earliest_turn_deadline(
    deadlines: &HashMap<TurnId, tokio::time::Instant>,
) -> Option<(TurnId, tokio::time::Instant)> {
    deadlines
        .iter()
        .min_by_key(|(_, deadline)| **deadline)
        .map(|(turn_id, deadline)| (*turn_id, *deadline))
}

fn refresh_turn_deadlines<'a>(
    deadlines: &mut HashMap<TurnId, tokio::time::Instant>,
    turn_ids: impl IntoIterator<Item = &'a TurnId>,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    for turn_id in turn_ids {
        if let Some(current) = deadlines.get_mut(turn_id) {
            *current = deadline;
        }
    }
}

impl Drop for TrackedTask {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

#[derive(Clone)]
struct SelectedAccount(String);

struct OpenUpgradeGuard(Arc<Stats>);

impl Drop for OpenUpgradeGuard {
    fn drop(&mut self) {
        self.0.open_upgrades.fetch_sub(1, Ordering::Relaxed);
    }
}

struct OpenBridgeSessionGuard(Arc<Stats>);

impl Drop for OpenBridgeSessionGuard {
    fn drop(&mut self) {
        self.0.open_bridge_sessions.fetch_sub(1, Ordering::Relaxed);
    }
}

struct InflightGuard<'a>(&'a AtomicUsize);

impl<'a> InflightGuard<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
}

#[derive(Clone, Copy)]
enum CapacityKind {
    Http,
    BridgeTurn,
    UpgradeHandshake,
}

/// Couples admission to accounting. Dropping a request future, response body, bridge turn, or
/// handshake releases the permit and every counter exactly once.
struct CapacityLease {
    permit: Option<OwnedSemaphorePermit>,
    stats: Arc<Stats>,
    kind: CapacityKind,
    stage_id: u64,
}

impl CapacityLease {
    fn new(permit: OwnedSemaphorePermit, stats: Arc<Stats>, kind: CapacityKind) -> Self {
        stats.inflight_http.fetch_add(1, Ordering::AcqRel);
        match kind {
            CapacityKind::Http => {
                stats.inflight_regular_http.fetch_add(1, Ordering::AcqRel);
            }
            CapacityKind::BridgeTurn => {
                stats.active_bridge_turns.fetch_add(1, Ordering::AcqRel);
            }
            CapacityKind::UpgradeHandshake => {
                stats.upgrade_handshakes.fetch_add(1, Ordering::AcqRel);
            }
        }
        let stage_id = stats.capacity_stage_started(kind.stage());
        Self {
            permit: Some(permit),
            stats,
            kind,
            stage_id,
        }
    }

    fn into_permit(mut self) -> OwnedSemaphorePermit {
        self.release_accounting();
        self.permit.take().expect("capacity permit")
    }

    fn release_accounting(&mut self) {
        if self.stage_id == 0 {
            return;
        }
        self.stats.inflight_http.fetch_sub(1, Ordering::AcqRel);
        match self.kind {
            CapacityKind::Http => {
                self.stats
                    .inflight_regular_http
                    .fetch_sub(1, Ordering::AcqRel);
            }
            CapacityKind::BridgeTurn => {
                self.stats
                    .active_bridge_turns
                    .fetch_sub(1, Ordering::AcqRel);
            }
            CapacityKind::UpgradeHandshake => {
                self.stats.upgrade_handshakes.fetch_sub(1, Ordering::AcqRel);
            }
        }
        self.stats
            .capacity_stage_finished(self.kind.stage(), self.stage_id);
        self.stage_id = 0;
    }
}

impl CapacityKind {
    fn stage(self) -> crate::state::CapacityStage {
        match self {
            Self::Http => crate::state::CapacityStage::Http,
            Self::BridgeTurn => crate::state::CapacityStage::BridgeTurn,
            Self::UpgradeHandshake => crate::state::CapacityStage::UpgradeHandshake,
        }
    }
}

impl Drop for CapacityLease {
    fn drop(&mut self) {
        self.release_accounting();
    }
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

struct BridgeSessionActivity {
    active_turns: AtomicUsize,
    last_activity: StdMutex<Instant>,
}

impl BridgeSessionActivity {
    fn new() -> Self {
        Self {
            active_turns: AtomicUsize::new(0),
            last_activity: StdMutex::new(Instant::now()),
        }
    }

    fn touch(&self) {
        *self.last_activity.lock().expect("bridge activity") = Instant::now();
    }

    fn last_activity(&self) -> Instant {
        *self.last_activity.lock().expect("bridge activity")
    }

    fn is_idle(&self) -> bool {
        self.active_turns.load(Ordering::Acquire) == 0
    }
}

struct BridgeSessionEntry {
    activity: Arc<BridgeSessionActivity>,
    evict: Option<oneshot::Sender<()>>,
    closing: bool,
}

struct BridgeSessionAdmission {
    id: u64,
    activity: Arc<BridgeSessionActivity>,
    evicted: oneshot::Receiver<()>,
}

struct BridgeTurnGuard {
    activity: Arc<BridgeSessionActivity>,
    changed: Arc<Notify>,
}

impl BridgeTurnGuard {
    fn new(activity: Arc<BridgeSessionActivity>, changed: Arc<Notify>) -> Self {
        activity.active_turns.fetch_add(1, Ordering::AcqRel);
        activity.touch();
        Self { activity, changed }
    }
}

impl Drop for BridgeTurnGuard {
    fn drop(&mut self) {
        self.activity.active_turns.fetch_sub(1, Ordering::AcqRel);
        self.activity.touch();
        self.changed.notify_waiters();
    }
}

#[derive(Clone)]
struct BridgeSender {
    sender: mpsc::Sender<(u64, Message)>,
    generation: u64,
}

impl BridgeSender {
    async fn send(&self, message: Message) -> bool {
        tokio::time::timeout(
            BRIDGE_SEND_TIMEOUT,
            self.sender.send((self.generation, message)),
        )
        .await
        .is_ok_and(|result| result.is_ok())
    }
}

impl DirectAccountLease {
    fn new(router: Arc<Router>, account: String) -> Self {
        Self {
            router,
            account: Some(account),
        }
    }

    async fn replace(&mut self, account: String) {
        if let Some(previous) = self.account.replace(account) {
            self.router.end(&previous).await;
        }
    }

    fn disarm(&mut self) {
        self.account = None;
    }
}

impl Drop for DirectAccountLease {
    fn drop(&mut self) {
        let Some(account) = self.account.take() else {
            return;
        };
        let router = self.router.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move { router.end(&account).await });
        }
    }
}

pub struct App {
    config: Arc<Config>,
    router: Arc<Router>,
    client: HttpClient,
    upgrade_client: UpgradeHttpClient,
    stats: Arc<Stats>,
    http_slots: Arc<Semaphore>,
    bridge_turn_slots: Arc<Semaphore>,
    upgrade_slots: Arc<Semaphore>,
    bridge_sessions: AsyncMutex<HashMap<u64, BridgeSessionEntry>>,
    bridge_sessions_changed: Arc<Notify>,
    next_bridge_session_id: AtomicU64,
    live_calls: LiveCallStore,
    auth: auth::Resolver,
    file_owners: Arc<AffinityStore>,
    tasks: AsyncMutex<JoinSet<()>>,
    shutting_down: AtomicBool,
    service_nonce: Option<String>,
}

impl App {
    pub fn new(config: Arc<Config>, router: Arc<Router>, stats: Arc<Stats>) -> Result<Arc<Self>> {
        config.validate()?;
        Self::build(config, router, stats)
    }

    /// Tests use loopback HTTP upstreams to exercise proxy behavior. Keep that capability outside
    /// the production constructor so manually assembled runtime configs cannot bypass validation.
    #[cfg(test)]
    fn new_unvalidated(
        config: Arc<Config>,
        router: Arc<Router>,
        stats: Arc<Stats>,
    ) -> Result<Arc<Self>> {
        Self::build(config, router, stats)
    }

    fn build(config: Arc<Config>, router: Arc<Router>, stats: Arc<Stats>) -> Result<Arc<Self>> {
        let https = codex_http_connector()?;
        let upgrade_https = codex_websocket_connector()?;
        let client = Client::builder(TokioExecutor::new()).build(https);
        let upgrade_client = Client::builder(TokioExecutor::new()).build(upgrade_https);
        let state_dir = config.proxy.state_dir.clone().unwrap_or_else(|| ".".into());
        let file_owners = Arc::new(AffinityStore::load(
            state_dir.join("file-owners.json"),
            &config.proxy.affinity_key,
            config.proxy.max_affinity_entries,
            config.proxy.max_affinity_bytes,
            Duration::from_secs(config.proxy.affinity_idle_days * 86_400),
        )?);
        stats.configure_capacity(
            config.proxy.max_inflight,
            config.proxy.max_inflight,
            config.proxy.max_upgrades,
        );
        stats.configure_bridge_session_capacity(config.proxy.max_bridge_sessions);
        Ok(Arc::new(Self {
            http_slots: Arc::new(Semaphore::new(config.proxy.max_inflight)),
            // Bridge turns use an independent bulkhead. A stalled bridge can no longer consume
            // the ordinary HTTP serving pool, while the combined counters remain truthful.
            bridge_turn_slots: Arc::new(Semaphore::new(config.proxy.max_inflight)),
            upgrade_slots: Arc::new(Semaphore::new(config.proxy.max_upgrades)),
            bridge_sessions: AsyncMutex::new(HashMap::new()),
            bridge_sessions_changed: Arc::new(Notify::new()),
            next_bridge_session_id: AtomicU64::new(1),
            live_calls: LiveCallStore::load(
                &format!(
                    "{}:{}",
                    config.proxy.installation_secret, config.proxy.affinity_key
                ),
                10_000,
                Duration::from_secs(2 * 60 * 60),
                config
                    .proxy
                    .state_dir
                    .clone()
                    .unwrap_or_else(|| ".".into())
                    .join("live-calls.json"),
            ),
            auth: auth::Resolver::new(&config),
            file_owners,
            config,
            router,
            client,
            upgrade_client,
            stats,
            tasks: AsyncMutex::new(JoinSet::new()),
            shutting_down: AtomicBool::new(false),
            service_nonce: std::env::var("COMRADEX_SERVICE_NONCE").ok(),
        }))
    }

    pub async fn flush_file_owners(&self) -> Result<()> {
        self.file_owners.flush().await
    }

    /// Sweep every configured managed account by stable ID. Accounts are intentionally handled
    /// one at a time so the configured 512-account bound also bounds refresh concurrency, and an
    /// unreadable or rejected credential cannot abort the rest of the pass.
    pub async fn refresh_managed_accounts_at(&self, now: u64) {
        self.stats
            .refresh_scheduler_ticks
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .refresh_last_sweep_unix
            .store(now, Ordering::Relaxed);
        let _inflight = InflightGuard::new(&self.stats.refresh_inflight);
        let results = self
            .auth
            .proactive_refresh_managed_at(&self.config.accounts, now)
            .await;
        self.stats
            .refresh_accounts_checked
            .fetch_add(results.len() as u64, Ordering::Relaxed);
        for (account_id, result) in results {
            match result {
                Ok(auth::ProactiveRefresh::Fresh) => {
                    self.router.proactive_auth_ready(&account_id).await;
                }
                Ok(auth::ProactiveRefresh::Refreshed) => {
                    self.stats.refresh_successes.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .refresh_last_success_unix
                        .store(now, Ordering::Relaxed);
                    self.router.proactive_auth_ready(&account_id).await;
                    info!(account = account_id, "managed credential refreshed");
                }
                Err(error) => {
                    self.stats.refresh_failures.fetch_add(1, Ordering::Relaxed);
                    if auth::is_reauth_required(&error) {
                        self.stats
                            .refresh_reauth_required
                            .fetch_add(1, Ordering::Relaxed);
                        self.router.reauth_required(&account_id).await;
                        warn!(
                            account = account_id,
                            "managed credential requires device login"
                        );
                    } else {
                        warn!(account = account_id, %error, "proactive credential refresh failed");
                    }
                }
            }
        }
    }

    async fn spawn_tracked<F>(&self, task: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if self.shutting_down.load(Ordering::Acquire) {
            return false;
        }
        let mut tasks = self.tasks.lock().await;
        while tasks.try_join_next().is_some() {}
        if self.shutting_down.load(Ordering::Acquire) {
            return false;
        }
        tasks.spawn(task);
        true
    }

    async fn spawn_tracked_task<F>(&self, task: F) -> Option<TrackedTask>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if self.shutting_down.load(Ordering::Acquire) {
            return None;
        }
        let mut tasks = self.tasks.lock().await;
        while tasks.try_join_next().is_some() {}
        if self.shutting_down.load(Ordering::Acquire) {
            return None;
        }
        let (done_tx, done) = oneshot::channel();
        let abort = tasks.spawn(async move {
            task.await;
            let _ = done_tx.send(());
        });
        Some(TrackedTask { abort, done })
    }

    pub async fn shutdown_connections(&self) {
        self.shutting_down.store(true, Ordering::Release);
        let mut tasks = self.tasks.lock().await;
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }

    pub async fn run_listener(
        self: Arc<Self>,
        name: String,
        listener: ListenerConfig,
    ) -> Result<()> {
        let tcp = TcpListener::bind(listener.address)
            .await
            .with_context(|| format!("bind {}", listener.address))?;
        self.serve_tcp(name, listener, tcp).await
    }

    async fn serve_tcp(
        self: Arc<Self>,
        name: String,
        listener: ListenerConfig,
        tcp: TcpListener,
    ) -> Result<()> {
        info!(listener = %name, address = %listener.address, pool = %listener.pool, "listening");
        loop {
            let (stream, _) = tcp.accept().await?;
            let app = self.clone();
            let listener = listener.clone();
            self.spawn_tracked(async move {
                let service = service_fn(move |req| app.clone().handle(req, listener.clone()));
                if let Err(e) = Builder::new(TokioExecutor::new())
                    .serve_connection_with_upgrades(TokioIo::new(stream), service)
                    .await
                {
                    warn!(error = %e, "client connection ended");
                }
            })
            .await;
        }
    }

    async fn handle(
        self: Arc<Self>,
        req: Request<Incoming>,
        listener: ListenerConfig,
    ) -> Result<Response<ProxyBody>, Infallible> {
        let response = if self.service_health_path(req.uri()) {
            let capacity = self.stats.capacity_snapshot();
            let (status, state) = if capacity.saturated {
                (StatusCode::SERVICE_UNAVAILABLE, "degraded")
            } else {
                (StatusCode::OK, "ok")
            };
            Response::builder()
                .status(status)
                .header(CONTENT_TYPE, "application/json")
                .body(json_body(
                    serde_json::json!({"status":state,"capacity":capacity}),
                ))
                .expect("static health response is valid")
        } else {
            match self.authorized_path(req.uri()) {
                None => error_response(StatusCode::NOT_FOUND, "not_found", "unknown proxy path"),
                Some(path)
                    if is_upgrade(&req)
                        && is_native_responses(&path)
                        && self.config.proxy.responses_websocket_mode
                            == ResponsesWebsocketMode::HttpBridge =>
                {
                    self.handle_responses_http_bridge(req, &listener, path)
                        .await
                        .unwrap_or_else(internal_error)
                }
                Some(path) if is_upgrade(&req) => match live::sideband_call_id(&path) {
                    Err(_) => error_response(
                        StatusCode::BAD_REQUEST,
                        "invalid_realtime_call_id",
                        "malformed or ambiguous realtime call id",
                    ),
                    Ok(call_id) => self
                        .handle_upgrade(req, &listener, path, call_id)
                        .await
                        .unwrap_or_else(internal_error),
                },
                Some(path) => {
                    let permit = match self.http_slots.clone().try_acquire_owned() {
                        Ok(v) => v,
                        Err(_) => {
                            self.stats
                                .http_admission_rejected
                                .fetch_add(1, Ordering::Relaxed);
                            return Ok(error_response(
                                StatusCode::SERVICE_UNAVAILABLE,
                                "at_capacity",
                                "HTTP request limit reached",
                            ));
                        }
                    };
                    let lease = CapacityLease::new(permit, self.stats.clone(), CapacityKind::Http);
                    let deadline = tokio::time::Instant::now() + HTTP_REQUEST_TIMEOUT;
                    let (result, response_deadline) = match tokio::time::timeout_at(
                        deadline,
                        self.handle_http(req, &listener, path, deadline),
                    )
                    .await
                    {
                        Ok(Ok(response)) => (response, Some(deadline)),
                        Ok(Err(error)) => (internal_error(error), Some(deadline)),
                        Err(_) => {
                            self.stats
                                .admission_timed_out
                                .fetch_add(1, Ordering::Relaxed);
                            (
                                error_response(
                                    StatusCode::GATEWAY_TIMEOUT,
                                    "upstream_request_timeout",
                                    "upstream request deadline exceeded",
                                ),
                                None,
                            )
                        }
                    };
                    bound_http_response(result, lease, response_deadline)
                }
            }
        };
        Ok(response)
    }

    fn service_health_path(&self, uri: &Uri) -> bool {
        uri.query().is_none()
            && self
                .service_nonce
                .as_deref()
                .is_some_and(|nonce| uri.path() == format!("/__comradex_health/{nonce}"))
    }

    fn authorized_path(&self, uri: &Uri) -> Option<String> {
        let prefix = format!("/{}/v1", self.config.proxy.installation_secret);
        let suffix = uri.path().strip_prefix(&prefix)?;
        if !suffix.is_empty() && !suffix.starts_with('/') {
            return None;
        }
        let mut path = if suffix.is_empty() {
            "/".to_owned()
        } else {
            suffix.to_owned()
        };
        if let Some(query) = uri.query() {
            path.push('?');
            path.push_str(query);
        }
        Some(path)
    }

    async fn handle_http(
        &self,
        req: Request<Incoming>,
        listener: &ListenerConfig,
        path: String,
        deadline: tokio::time::Instant,
    ) -> Result<Response<ProxyBody>> {
        let (parts, body) = req.into_parts();
        let inbound_headers = parts.headers;
        let method = parts.method;
        let replay = ReplayBody::read_with_timeouts(
            body,
            self.config.proxy.replay_memory_bytes,
            self.config.proxy.max_request_bytes,
            self.config.proxy.max_spool_bytes,
            self.stats.clone(),
            HTTP_REQUEST_BODY_IDLE_TIMEOUT,
            deadline.saturating_duration_since(tokio::time::Instant::now()),
        )
        .await?;
        self.handle_http_replay(inbound_headers, method, listener, path, replay, deadline)
            .await
    }

    async fn handle_http_replay(
        &self,
        inbound_headers: hyper::HeaderMap,
        method: Method,
        listener: &ListenerConfig,
        path: String,
        replay: ReplayBody,
        deadline: tokio::time::Instant,
    ) -> Result<Response<ProxyBody>> {
        self.handle_http_replay_with_routing_anchor(
            inbound_headers,
            method,
            listener,
            path,
            replay,
            None,
            deadline,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_http_replay_with_routing_anchor(
        &self,
        mut inbound_headers: hyper::HeaderMap,
        method: Method,
        listener: &ListenerConfig,
        mut path: String,
        mut replay: ReplayBody,
        routing_previous_response_id: Option<String>,
        request_deadline: tokio::time::Instant,
    ) -> Result<Response<ProxyBody>> {
        let legacy_compact = method == Method::POST && is_legacy_compact_path(&path);
        if legacy_compact {
            let bytes = replay.into_bytes().await?;
            let bytes = rewrite_legacy_compact_request(&bytes)?;
            replay = ReplayBody::from_bytes(
                bytes,
                self.config.proxy.max_request_bytes,
                self.config.proxy.max_spool_bytes,
                self.stats.clone(),
            )?;
            path = responses_path_for_legacy_compact(&path);
            mark_compaction_request(&mut inbound_headers)?;
        }
        if replay.file_ids_overflow() {
            return Ok(error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "too_many_file_references",
                "request contains more than 32 distinct file references",
            ));
        }
        let pool = self.pool(listener)?;
        let mut file_ids = replay.file_ids().to_vec();
        if let Some(file_id) = finalized_file_id(&method, &path)
            && !file_ids.contains(&file_id)
        {
            file_ids.push(file_id);
        }
        let affinity_values = metadata::affinity_values(
            &inbound_headers,
            replay.thread_id(),
            routing_previous_response_id
                .as_deref()
                .or_else(|| replay.previous_response_id()),
            replay.prompt_cache_key(),
            &file_ids,
        );
        let affinity_keys: Vec<_> = affinity_values
            .iter()
            .map(|value| (value.kind, self.router.affinity.key(&value.namespaced())))
            .collect();
        let mut bound_account: Option<String> = None;
        let mut hard_owner = false;
        let mut known_file_owners = 0usize;
        let mut missing_hard_owner = false;
        let soft_routing_key = affinity_keys
            .iter()
            .filter_map(|(kind, key)| {
                kind.soft_routing_priority()
                    .map(|priority| (priority, key.clone()))
            })
            .min_by_key(|(priority, _)| *priority)
            .map(|(_, key)| key);
        for (kind, key) in &affinity_keys {
            let binding = if *kind == metadata::AffinityKind::File {
                self.file_owners.get(key).await
            } else {
                self.router.affinity.get(key).await
            };
            let Some(binding) = binding else {
                if matches!(
                    kind,
                    metadata::AffinityKind::PreviousResponse | metadata::AffinityKind::TurnState
                ) {
                    missing_hard_owner = true;
                }
                continue;
            };
            // Session, cache, and thread aliases are routing preferences, not
            // proof that request state belongs to an account. In particular,
            // WebSocket handshake headers can outlive the response.create
            // conversation carried in the frame body.
            if !kind.is_hard_continuity() {
                continue;
            }
            if bound_account
                .as_ref()
                .is_some_and(|account| account != &binding.account_id)
            {
                return Ok(error_response(
                    StatusCode::CONFLICT,
                    "continuity_owner_conflict",
                    "request continuity keys resolve to different accounts",
                ));
            }
            hard_owner = true;
            if *kind == metadata::AffinityKind::File {
                known_file_owners += 1;
            }
            bound_account = Some(binding.account_id);
        }
        if known_file_owners > 0 && known_file_owners < file_ids.len() {
            return Ok(error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "file_owner_unavailable",
                "some referenced files have no known account owner",
            ));
        }
        if missing_hard_owner {
            return Ok(error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "continuity_owner_unavailable",
                "request carries hard continuity state with no known account owner",
            ));
        }
        let first = if let Some(account) = &bound_account {
            match self.router.select_exact(pool, account).await {
                Some(selection) => selection,
                None => {
                    return Ok(error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "continuity_owner_unavailable",
                        "required continuity account is unavailable",
                    ));
                }
            }
        } else {
            self.router
                .select(&listener.pool, pool, soft_routing_key, None)
                .await
                .context("no eligible account")?
        };
        let mut selected = first;
        let nonportable_payload = replay.has_nonportable_state();
        let mut payload_dispatch_owner: Option<String> = None;
        for attempt in 0..2 {
            let account = selected.account_id.clone();
            let credentials = self
                .auth
                .resolve(&self.config.accounts[&account], &inbound_headers)
                .await?;
            self.router.begin(&account).await;
            let mut request_lease = DirectAccountLease::new(self.router.clone(), account.clone());
            let result = self
                .send_http(
                    &method,
                    &path,
                    &inbound_headers,
                    credentials.clone(),
                    replay.body(attempt)?,
                    request_deadline,
                )
                .await;
            match result {
                Ok(response) => {
                    if nonportable_payload {
                        payload_dispatch_owner.get_or_insert_with(|| account.clone());
                    }
                    self.router
                        .observe_headers(&account, response.headers())
                        .await;
                    let status = response.status();
                    if status.is_success()
                        && let Some(turn_state) = response
                            .headers()
                            .get("x-codex-turn-state")
                            .and_then(|value| value.to_str().ok())
                            .filter(|value| !value.is_empty())
                    {
                        let alias = self
                            .router
                            .affinity
                            .key(&format!("turn-state:{turn_state}"));
                        self.router.bind(alias, &account).await;
                    }
                    if status == StatusCode::UNAUTHORIZED {
                        if attempt == 0 {
                            match self
                                .auth
                                .force_refresh(&self.config.accounts[&account], &credentials)
                                .await
                            {
                                Ok(Some(_)) => {
                                    continue;
                                }
                                Ok(None) => {}
                                Err(error) => warn!(account, %error, "credential refresh failed"),
                            }
                        }
                        if matches!(
                            self.config.accounts[&account],
                            crate::config::AccountConfig::CodexHome { .. }
                        ) {
                            self.router.auth_failure(&account).await;
                        }
                        request_lease.disarm();
                        return Ok(map_http_response_leased(
                            response,
                            self.router.clone(),
                            account,
                            false,
                        ));
                    }
                    if status == StatusCode::FORBIDDEN {
                        request_lease.disarm();
                        return Ok(map_http_response_leased(
                            response,
                            self.router.clone(),
                            account,
                            false,
                        ));
                    }
                    if status.is_success() {
                        for (kind, key) in &affinity_keys {
                            if *kind != metadata::AffinityKind::File {
                                self.router.bind(key.clone(), &account).await;
                            }
                        }
                    }
                    if status.is_success() && live::is_call_creation(&path) {
                        let bound = response
                            .headers()
                            .get(LOCATION)
                            .and_then(|value| value.to_str().ok())
                            .and_then(live::call_id_from_location);
                        let binding_ok = match bound {
                            Some(call_id) => self.live_calls.bind(&call_id, account.clone()).await,
                            None => false,
                        };
                        if !binding_ok {
                            return Ok(error_response(
                                StatusCode::SERVICE_UNAVAILABLE,
                                "realtime_call_binding_failed",
                                "successful realtime call could not be bound safely",
                            ));
                        }
                    }
                    if status.is_success() && is_file_create(&method, &path) {
                        let mapped = self.map_file_create_response(response, &account).await;
                        return mapped;
                    }
                    if status.is_success()
                        && let Some(file_id) = finalized_file_id(&method, &path)
                    {
                        return self
                            .map_file_finalize_response(response, &account, &file_id)
                            .await;
                    }
                    let retry = retryable_http_status(status, &method, &path);
                    if retry {
                        if status == StatusCode::TOO_MANY_REQUESTS
                            || status == StatusCode::PAYMENT_REQUIRED
                        {
                            self.router
                                .quota_failure(&account, response.headers())
                                .await;
                        } else {
                            self.router.soft_failure(&account).await;
                        }
                    }
                    if retry && attempt == 0 && !hard_owner && payload_dispatch_owner.is_none() {
                        let alternate = self
                            .router
                            .select(&listener.pool, pool, None, Some(&account))
                            .await;
                        if let Some(alternate) = alternate {
                            selected = alternate;
                            continue;
                        }
                    }
                    if status.is_success() && legacy_compact {
                        return map_legacy_compact_response(response).await;
                    }
                    request_lease.disarm();
                    return Ok(map_http_response_leased(
                        response,
                        self.router.clone(),
                        account,
                        status.is_success() && is_native_responses(&path),
                    ));
                }
                Err(e) => {
                    if is_account_neutral_connect_failure(&e) {
                        warn!(account, error = %e, "shared upstream network failure");
                        return Err(e);
                    }
                    if attempt == 0
                        && is_connect_failure(&e)
                        && !hard_owner
                        && !live::is_call_creation(&path)
                        && (is_native_responses(&path)
                            || matches!(method, Method::GET | Method::HEAD | Method::OPTIONS))
                    {
                        warn!(account, error = %e, "upstream connect failed; trying one alternate");
                        self.router.soft_failure(&account).await;
                        selected = self
                            .router
                            .select(&listener.pool, pool, None, Some(&account))
                            .await
                            .context("no alternate account")?;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        unreachable!()
    }

    async fn send_http(
        &self,
        method: &Method,
        path: &str,
        inbound: &hyper::HeaderMap,
        credentials: auth::Credentials,
        body: ProxyBody,
        request_deadline: tokio::time::Instant,
    ) -> Result<Response<Incoming>> {
        let uri = self.upstream_uri(path, false)?;
        let mut builder = Request::builder().method(method).uri(uri);
        *builder.headers_mut().expect("builder") = inbound.clone();
        let headers = builder.headers_mut().expect("builder");
        headers::strip_hop_by_hop(headers);
        headers.remove(CONTENT_LENGTH);
        let _ = inbound;
        apply_credentials(headers, credentials)?;
        let header_deadline =
            request_deadline.min(tokio::time::Instant::now() + HTTP_RESPONSE_HEADER_TIMEOUT);
        match tokio::time::timeout_at(header_deadline, self.client.request(builder.body(body)?))
            .await
        {
            Ok(result) => Ok(result?),
            Err(_) => anyhow::bail!("upstream response header timeout"),
        }
    }

    async fn map_file_create_response(
        &self,
        response: Response<Incoming>,
        account: &str,
    ) -> Result<Response<ProxyBody>> {
        let (mut parts, mut body) = response.into_parts();
        let mut bytes = bytes::BytesMut::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.context("read file-create response")?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            if bytes.len().saturating_add(data.len()) > FILE_CREATE_RESPONSE_LIMIT {
                anyhow::bail!("file-create response exceeds safety limit")
            }
            bytes.extend_from_slice(&data);
        }
        let file_id = serde_json::from_slice::<serde_json::Value>(&bytes)
            .context("parse file-create response")?
            .get("file_id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .context("successful file-create response has no file_id")?;
        let key = self.router.affinity.key(&format!("file:{file_id}"));
        self.file_owners.put(key, account.to_owned(), 0).await;
        if let Err(error) = self.file_owners.flush().await {
            error!(%error, "file-owner snapshot failed after creation");
        }
        headers::strip_hop_by_hop(&mut parts.headers);
        let bytes = bytes.freeze();
        parts.headers.insert(CONTENT_LENGTH, bytes.len().into());
        Ok(Response::from_parts(parts, bytes_body(bytes)))
    }

    async fn map_file_finalize_response(
        &self,
        response: Response<Incoming>,
        account: &str,
        file_id: &str,
    ) -> Result<Response<ProxyBody>> {
        let (mut parts, mut body) = response.into_parts();
        let mut bytes = bytes::BytesMut::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.context("read file-finalization response")?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            if bytes.len().saturating_add(data.len()) > FILE_CREATE_RESPONSE_LIMIT {
                anyhow::bail!("file-finalization response exceeds safety limit")
            }
            bytes.extend_from_slice(&data);
        }
        let authoritative_success = is_authoritative_file_finalize_success(&bytes);
        if authoritative_success {
            let key = self.router.affinity.key(&format!("file:{file_id}"));
            self.file_owners.put(key, account.to_owned(), 0).await;
            if let Err(error) = self.file_owners.flush().await {
                error!(%error, "file-owner snapshot failed after finalization");
            }
        }
        headers::strip_hop_by_hop(&mut parts.headers);
        let bytes = bytes.freeze();
        parts.headers.insert(CONTENT_LENGTH, bytes.len().into());
        Ok(Response::from_parts(parts, bytes_body(bytes)))
    }

    async fn admit_bridge_session(&self) -> Option<BridgeSessionAdmission> {
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(self.config.proxy.bridge_admission_timeout_millis);
        loop {
            let changed = self.bridge_sessions_changed.notified();
            let mut sessions = self.bridge_sessions.lock().await;
            if sessions.len() < self.config.proxy.max_bridge_sessions {
                let id = self.next_bridge_session_id.fetch_add(1, Ordering::Relaxed);
                let activity = Arc::new(BridgeSessionActivity::new());
                let (evict, evicted) = oneshot::channel();
                sessions.insert(
                    id,
                    BridgeSessionEntry {
                        activity: activity.clone(),
                        evict: Some(evict),
                        closing: false,
                    },
                );
                return Some(BridgeSessionAdmission {
                    id,
                    activity,
                    evicted,
                });
            }

            let lru_idle = sessions
                .iter()
                .filter(|(_, entry)| !entry.closing && entry.activity.is_idle())
                .min_by_key(|(_, entry)| entry.activity.last_activity())
                .map(|(id, _)| *id);
            if let Some(id) = lru_idle {
                let entry = sessions.get_mut(&id).expect("selected bridge session");
                entry.closing = true;
                if entry
                    .evict
                    .take()
                    .is_some_and(|evict| evict.send(()).is_err())
                {
                    sessions.remove(&id);
                    drop(sessions);
                    self.bridge_sessions_changed.notify_waiters();
                    continue;
                }
            }
            drop(sessions);

            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return None;
            }
        }
    }

    async fn finish_bridge_session(&self, id: u64) {
        if self.bridge_sessions.lock().await.remove(&id).is_some() {
            self.bridge_sessions_changed.notify_waiters();
        }
    }

    async fn handle_responses_http_bridge(
        self: &Arc<Self>,
        mut req: Request<Incoming>,
        listener: &ListenerConfig,
        path: String,
    ) -> Result<Response<ProxyBody>> {
        let key = req
            .headers()
            .get(SEC_WEBSOCKET_KEY)
            .context("missing Sec-WebSocket-Key")?
            .as_bytes();
        if req
            .headers()
            .get(SEC_WEBSOCKET_VERSION)
            .and_then(|value| value.to_str().ok())
            != Some("13")
        {
            return Ok(error_response(
                StatusCode::BAD_REQUEST,
                "unsupported_websocket_version",
                "Sec-WebSocket-Version must be 13",
            ));
        }
        let Some(admission) = self.admit_bridge_session().await else {
            self.stats
                .bridge_turn_admission_rejected
                .fetch_add(1, Ordering::Relaxed);
            return Ok(bridge_capacity_response());
        };
        let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key);
        let headers = req.headers().clone();
        let client_upgrade = hyper::upgrade::on(&mut req);
        let app = self.clone();
        let cleanup_app = self.clone();
        let listener = listener.clone();
        let session_id = admission.id;
        let activity = admission.activity;
        let evicted = admission.evicted;
        self.stats
            .open_bridge_sessions
            .fetch_add(1, Ordering::Relaxed);
        let bridge_guard = OpenBridgeSessionGuard(self.stats.clone());
        let spawned = self
            .spawn_tracked(async move {
                let _bridge_guard = bridge_guard;
                match tokio::time::timeout(WEBSOCKET_HANDSHAKE_TIMEOUT, client_upgrade).await {
                    Ok(Ok(client)) => {
                        let websocket = WebSocketStream::from_raw_socket(
                            TokioIo::new(client),
                            Role::Server,
                            None,
                        )
                        .await;
                        if let Err(error) = app
                            .run_responses_http_bridge(
                                websocket, listener, path, headers, activity, evicted,
                            )
                            .await
                        {
                            warn!(%error, "Responses HTTP bridge ended");
                        }
                    }
                    Ok(Err(error)) => warn!(%error, "Responses HTTP bridge upgrade failed"),
                    Err(_) => {
                        app.stats
                            .admission_timed_out
                            .fetch_add(1, Ordering::Relaxed);
                        warn!("Responses HTTP bridge downstream upgrade timed out");
                    }
                }
                cleanup_app.finish_bridge_session(session_id).await;
            })
            .await;
        if !spawned {
            self.finish_bridge_session(session_id).await;
            anyhow::bail!("proxy is shutting down")
        }
        Ok(Response::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, "websocket")
            .header(SEC_WEBSOCKET_ACCEPT, accept)
            .body(empty_body())?)
    }

    async fn run_responses_http_bridge(
        self: &Arc<Self>,
        websocket: UpgradedWebSocket,
        listener: ListenerConfig,
        path: String,
        mut headers: hyper::HeaderMap,
        activity: Arc<BridgeSessionActivity>,
        mut evicted: oneshot::Receiver<()>,
    ) -> Result<()> {
        headers::strip_hop_by_hop(&mut headers);
        headers.remove(SEC_WEBSOCKET_KEY);
        headers.remove(SEC_WEBSOCKET_VERSION);
        headers.remove(SEC_WEBSOCKET_ACCEPT);
        headers.remove(CONTENT_LENGTH);
        headers.insert(CONTENT_TYPE, "application/json".parse()?);
        let (mut sink, mut source) = websocket.split();
        let active_generation = Arc::new(AtomicU64::new(0));
        let delivery_gate = Arc::new(AsyncMutex::new(()));
        let (outbound, mut outgoing) = mpsc::channel::<(u64, Message)>(8);
        let (fatal_tx, mut fatal_rx) = mpsc::unbounded_channel::<String>();
        let control = BridgeSender {
            sender: outbound.clone(),
            generation: 0,
        };
        let continuation = Arc::new(StdMutex::new(None::<HttpBridgeContinuation>));
        let writer_generation = active_generation.clone();
        let writer_gate = delivery_gate.clone();
        let writer_fatal = fatal_tx.clone();
        let mut writer = Some(
            self.spawn_tracked_task(async move {
                while let Some((generation, message)) = outgoing.recv().await {
                    let _delivery = writer_gate.lock().await;
                    if generation != 0 && generation != writer_generation.load(Ordering::Acquire) {
                        continue;
                    }
                    if !matches!(
                        tokio::time::timeout(BRIDGE_SEND_TIMEOUT, sink.send(message)).await,
                        Ok(Ok(()))
                    ) {
                        let _ = writer_fatal.send("downstream WebSocket writer failed".into());
                        break;
                    }
                }
                let _ = tokio::time::timeout(BRIDGE_SEND_TIMEOUT, sink.close()).await;
            })
            .await
            .context("proxy is shutting down")?,
        );
        let mut active_turn: Option<TrackedTask> = None;
        let mut read_error: Option<String> = None;
        loop {
            let idle_timeout = wait_for_bridge_idle_timeout(
                activity.clone(),
                self.bridge_sessions_changed.clone(),
                Duration::from_secs(self.config.proxy.bridge_idle_seconds),
            );
            let message = tokio::select! {
                _ = &mut evicted => break,
                _ = idle_timeout => break,
                fatal = fatal_rx.recv() => {
                    read_error = Some(fatal.unwrap_or_else(|| "downstream WebSocket writer stopped".into()));
                    break;
                }
                message = source.next() => {
                    let Some(message) = message else { break };
                    message
                }
            };
            activity.touch();
            let message = match message {
                Ok(message) => message,
                Err(error) => {
                    read_error = Some(error.to_string());
                    break;
                }
            };
            match message {
                Message::Ping(payload) => {
                    if !control.send(Message::Pong(payload)).await {
                        break;
                    }
                }
                Message::Close(frame) => {
                    active_generation.fetch_add(1, Ordering::AcqRel);
                    if let Some(turn) = active_turn.take() {
                        turn.cancel().await;
                    }
                    let _ = control.send(Message::Close(frame)).await;
                    break;
                }
                Message::Text(text) => {
                    let Ok(mut frame) = serde_json::from_str::<serde_json::Value>(text.as_str())
                    else {
                        send_ws_error(&control, "invalid_request_error", "invalid JSON frame")
                            .await;
                        continue;
                    };
                    let frame_type = frame.get("type").and_then(serde_json::Value::as_str);
                    if frame_type == Some("response.processed") {
                        continue;
                    }
                    if frame_type != Some("response.create") {
                        send_ws_error(
                            &control,
                            "invalid_request_error",
                            "expected response.create",
                        )
                        .await;
                        continue;
                    }
                    // Protect the session from idle/LRU eviction for the entire create path,
                    // including cancellation and request validation before the turn task starts.
                    let mut turn_guard = Some(BridgeTurnGuard::new(
                        activity.clone(),
                        self.bridge_sessions_changed.clone(),
                    ));
                    // Cancel upstream work immediately. The gate is acquired only afterward,
                    // so a blocked downstream send cannot keep consuming account usage.
                    let generation = active_generation.fetch_add(1, Ordering::AcqRel) + 1;
                    if let Some(turn) = active_turn.take() {
                        turn.cancel().await;
                    }
                    // Do not begin replacement delivery until any already-started old send
                    // has either completed or hit its bounded writer timeout.
                    let delivery = delivery_gate.lock().await;
                    drop(delivery);
                    let turn_outbound = BridgeSender {
                        sender: outbound.clone(),
                        generation,
                    };
                    if frame.get("generate").and_then(serde_json::Value::as_bool) == Some(false) {
                        for warmup in warmup_frames(&frame) {
                            if !turn_outbound.send(Message::Text(warmup.into())).await {
                                break;
                            }
                        }
                        continue;
                    }
                    let Some(object) = frame.as_object_mut() else {
                        continue;
                    };
                    let routing_previous_response_id = object
                        .get("previous_response_id")
                        .and_then(serde_json::Value::as_str)
                        .filter(|value| !value.is_empty())
                        .map(str::to_owned);
                    if let Some(anchor) = routing_previous_response_id.as_deref() {
                        let cached = continuation.lock().expect("bridge continuation").clone();
                        let Some(cached) = cached.filter(|cached| cached.response_id == anchor)
                        else {
                            let error = previous_response_not_found_error();
                            send_ws_http_error(
                                &turn_outbound,
                                StatusCode::BAD_REQUEST,
                                error["error"].clone(),
                                &hyper::HeaderMap::new(),
                            )
                            .await;
                            continue;
                        };
                        let Some(delta) = object
                            .get("input")
                            .and_then(serde_json::Value::as_array)
                            .cloned()
                        else {
                            send_ws_error(
                                &turn_outbound,
                                "invalid_request_error",
                                "incremental response.create input must be an array",
                            )
                            .await;
                            continue;
                        };
                        let input = match materialize_http_bridge_continuation(cached, delta) {
                            Ok(input) => input,
                            Err(message) => {
                                send_ws_error(&turn_outbound, "invalid_request_error", message)
                                    .await;
                                continue;
                            }
                        };
                        object.insert("input".into(), serde_json::Value::Array(input));
                        object.remove("previous_response_id");
                    }
                    object.remove("type");
                    object.insert("stream".into(), serde_json::Value::Bool(true));
                    let request_input = object
                        .get("input")
                        .and_then(serde_json::Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let body = match serde_json::to_vec(&frame) {
                        Ok(body) => bytes::Bytes::from(body),
                        Err(error) => {
                            send_ws_error(
                                &turn_outbound,
                                "invalid_request_error",
                                &error.to_string(),
                            )
                            .await;
                            continue;
                        }
                    };
                    let app = self.clone();
                    let listener = listener.clone();
                    let path = path.clone();
                    let headers = headers.clone();
                    let outbound = turn_outbound;
                    let fatal = fatal_tx.clone();
                    let continuation = continuation.clone();
                    let turn_guard = turn_guard.take().expect("response.create turn guard");
                    active_turn = self
                        .spawn_tracked_task(async move {
                            let _turn_guard = turn_guard;
                            let dispatch_deadline =
                                tokio::time::Instant::now() + RESPONSES_MISSING_CREATED_TIMEOUT;
                            let permit = match app.bridge_turn_slots.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    app.stats
                                        .bridge_turn_admission_rejected
                                        .fetch_add(1, Ordering::Relaxed);
                                    send_ws_error(
                                        &outbound,
                                        "server_busy",
                                        "HTTP request limit reached",
                                    )
                                    .await;
                                    return;
                                }
                            };
                            let _capacity = CapacityLease::new(
                                permit,
                                app.stats.clone(),
                                CapacityKind::BridgeTurn,
                            );
                            let replay = match ReplayBody::from_bytes(
                                body,
                                app.config.proxy.max_request_bytes,
                                app.config.proxy.max_spool_bytes,
                                app.stats.clone(),
                            ) {
                                Ok(replay) => replay,
                                Err(error) => {
                                    send_ws_error(
                                        &outbound,
                                        "invalid_request_error",
                                        &error.to_string(),
                                    )
                                    .await;
                                    return;
                                }
                            };
                            match tokio::time::timeout_at(
                                dispatch_deadline,
                                app.handle_http_replay_with_routing_anchor(
                                    headers,
                                    Method::POST,
                                    &listener,
                                    path,
                                    replay,
                                    routing_previous_response_id,
                                    dispatch_deadline,
                                ),
                            )
                            .await
                            {
                                Ok(Ok(response)) => {
                                    let close_for_inbound_auth = response.status()
                                        == StatusCode::UNAUTHORIZED
                                        && response
                                            .extensions()
                                            .get::<SelectedAccount>()
                                            .is_some_and(|selected| {
                                                matches!(
                                                    app.config.accounts.get(&selected.0),
                                                    Some(crate::config::AccountConfig::Inbound)
                                                )
                                            });
                                    let pump_result = pump_http_response_to_websocket(
                                        response,
                                        &outbound,
                                        &app,
                                        request_input,
                                        &continuation,
                                        dispatch_deadline,
                                        RESPONSES_UPSTREAM_IDLE_TIMEOUT,
                                    )
                                    .await;
                                    if close_for_inbound_auth {
                                        let _ = outbound
                                            .send(Message::Close(Some(
                                                tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                                    code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy,
                                                    reason: "inbound credentials rejected; reconnect required".into(),
                                                },
                                            )))
                                            .await;
                                        let _ = fatal.send(
                                            "inbound credentials rejected; downstream reconnect required"
                                                .into(),
                                        );
                                        return;
                                    }
                                    if let Err(failure) = pump_result {
                                        if let Some(liveness) = failure.liveness {
                                            send_ws_nonretryable_liveness_error(
                                                &outbound,
                                                liveness,
                                            )
                                            .await;
                                            return;
                                        }
                                        if failure
                                            .error
                                            .to_string()
                                            .contains("backpressure prevented event delivery")
                                        {
                                            let _ = fatal.send(failure.error.to_string());
                                            return;
                                        }
                                        if failure.delivered_event {
                                            let _ = outbound
                                                .send(Message::Close(Some(
                                                    tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                                        code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Error,
                                                        reason: "upstream stream incomplete".into(),
                                                    },
                                                )))
                                                .await;
                                            let _ = fatal.send(format!(
                                                "upstream stream ended after visible output: {}",
                                                failure.error
                                            ));
                                        } else {
                                            send_ws_error(
                                                &outbound,
                                                "websocket_protocol_error",
                                                &failure.error.to_string(),
                                            )
                                            .await;
                                        }
                                    }
                                }
                                Ok(Err(error)) => {
                                    send_ws_error(&outbound, "proxy_error", &error.to_string())
                                        .await;
                                }
                                Err(_) => {
                                    send_ws_nonretryable_liveness_error(
                                        &outbound,
                                        HttpBridgeLivenessFailure::MissingResponseCreated,
                                    )
                                    .await;
                                }
                            }
                        })
                        .await;
                    if active_turn.is_none() {
                        break;
                    }
                }
                Message::Binary(_) => {
                    send_ws_error(
                        &control,
                        "invalid_request_error",
                        "Responses WebSocket accepts JSON text frames only",
                    )
                    .await;
                }
                Message::Pong(_) | Message::Frame(_) => {}
            }
        }
        if let Some(turn) = active_turn {
            turn.cancel().await;
        }
        drop(control);
        drop(outbound);
        if let Some(writer_task) = writer.take() {
            writer_task.wait_timeout(Duration::from_secs(6)).await;
        }
        match read_error {
            Some(error) => anyhow::bail!("Responses WebSocket bridge failed: {error}"),
            None => Ok(()),
        }
    }

    async fn route_websocket_frame(
        &self,
        listener: &ListenerConfig,
        headers: &hyper::HeaderMap,
        replay: &ReplayBody,
        preferred_account: Option<&str>,
    ) -> Result<WebSocketFrameRoute> {
        if replay.file_ids_overflow() {
            anyhow::bail!("request contains more than 32 distinct file references")
        }
        let pool = self.pool(listener)?;
        let affinity_values = metadata::affinity_values(
            headers,
            replay.thread_id(),
            replay.previous_response_id(),
            replay.prompt_cache_key(),
            replay.file_ids(),
        );
        let affinity_keys: Vec<_> = affinity_values
            .iter()
            .map(|value| (value.kind, self.router.affinity.key(&value.namespaced())))
            .collect();
        let mut hard_bound_account: Option<String> = None;
        let mut soft_bound_account: Option<String> = None;
        let mut hard_owner = false;
        let mut non_previous_hard_owner = false;
        let mut known_file_owners = 0usize;
        let mut missing_hard_owner = false;
        for (kind, key) in &affinity_keys {
            let binding = if *kind == metadata::AffinityKind::File {
                self.file_owners.get(key).await
            } else {
                self.router.affinity.get(key).await
            };
            let Some(binding) = binding else {
                if matches!(
                    kind,
                    metadata::AffinityKind::PreviousResponse | metadata::AffinityKind::TurnState
                ) {
                    missing_hard_owner = true;
                }
                continue;
            };
            if is_direct_hard_continuity(*kind) {
                if hard_bound_account
                    .as_ref()
                    .is_some_and(|account| account != &binding.account_id)
                {
                    anyhow::bail!("request continuity keys resolve to different accounts")
                }
                hard_owner = true;
                non_previous_hard_owner |= *kind != metadata::AffinityKind::PreviousResponse;
                hard_bound_account = Some(binding.account_id.clone());
            } else if soft_bound_account.is_none() {
                soft_bound_account = Some(binding.account_id.clone());
            }
            if *kind == metadata::AffinityKind::File {
                known_file_owners += 1;
            }
        }
        if known_file_owners > 0 && known_file_owners < replay.file_ids().len() {
            anyhow::bail!("some referenced files have no known account owner")
        }
        if missing_hard_owner {
            anyhow::bail!("request carries hard continuity state with no known account owner")
        }
        let selection = if let Some(account) = &hard_bound_account {
            self.router
                .select_exact(pool, account)
                .await
                .context("required continuity account is unavailable")?
        } else if let Some(account) = soft_bound_account.as_deref().or(preferred_account) {
            self.router
                .select_preferred(&listener.pool, pool, account)
                .await
                .context("no eligible account for fresh direct WebSocket frame")?
        } else {
            self.router
                .select(&listener.pool, pool, None, None)
                .await
                .context("no eligible account")?
        };
        let mut soft_keys = Vec::new();
        for (kind, key) in &affinity_keys {
            if *kind != metadata::AffinityKind::File {
                soft_keys.push(key.clone());
            }
        }
        Ok(WebSocketFrameRoute {
            account_id: selection.account_id,
            hard_owner,
            non_previous_hard_owner,
            soft_keys,
        })
    }

    async fn connect_direct_upstream(
        &self,
        account: &str,
        path: &str,
        inbound_headers: &hyper::HeaderMap,
        clear_session_state: bool,
    ) -> Result<DirectUpstream> {
        self.connect_direct_upstream_with_timeout(
            account,
            path,
            inbound_headers,
            clear_session_state,
            RESPONSES_DIRECT_CONNECT_TIMEOUT,
        )
        .await
    }

    async fn connect_direct_upstream_with_timeout(
        &self,
        account: &str,
        path: &str,
        inbound_headers: &hyper::HeaderMap,
        clear_session_state: bool,
        connect_timeout: Duration,
    ) -> Result<DirectUpstream> {
        let connect_deadline = tokio::time::Instant::now() + connect_timeout;
        for attempt in 0..2 {
            let uri = self.upstream_uri(path, false)?;
            let mut upstream_req = Request::builder()
                .method(Method::GET)
                .uri(uri)
                .body(empty_body())?;
            *upstream_req.headers_mut() = inbound_headers.clone();
            upstream_req.headers_mut().remove(HOST);
            if clear_session_state {
                strip_direct_session_headers(upstream_req.headers_mut());
            }
            normalize_websocket_beta(upstream_req.headers_mut(), path);
            let credentials = self
                .auth
                .resolve(&self.config.accounts[account], inbound_headers)
                .await?;
            apply_credentials(upstream_req.headers_mut(), credentials.clone())?;
            self.router.begin(account).await;
            let mut response = match tokio::time::timeout_at(
                connect_deadline,
                self.upgrade_client.request(upstream_req),
            )
            .await
            {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    self.router.end(account).await;
                    return Err(error.into());
                }
                Err(_) => {
                    self.router.end(account).await;
                    anyhow::bail!("upstream WebSocket handshake response timed out")
                }
            };
            if response.status() == StatusCode::SWITCHING_PROTOCOLS {
                if !selected_subprotocol_is_offered(inbound_headers, response.headers()) {
                    self.router.end(account).await;
                    anyhow::bail!("upstream selected an unoffered WebSocket subprotocol")
                }
                let upgrade =
                    tokio::time::timeout_at(connect_deadline, hyper::upgrade::on(&mut response))
                        .await;
                match upgrade {
                    Ok(Ok(upgraded)) => {
                        return Ok(DirectUpstream {
                            socket: WebSocketStream::from_raw_socket(
                                TokioIo::new(upgraded),
                                Role::Client,
                                None,
                            )
                            .await,
                            credentials,
                        });
                    }
                    Ok(Err(error)) => {
                        self.router.end(account).await;
                        return Err(error.into());
                    }
                    Err(_) => {
                        self.router.end(account).await;
                        anyhow::bail!("upstream WebSocket protocol upgrade timed out")
                    }
                }
            }
            let status = response.status();
            self.router.end(account).await;
            if status == StatusCode::UNAUTHORIZED && attempt == 0 {
                match self
                    .auth
                    .force_refresh(&self.config.accounts[account], &credentials)
                    .await
                {
                    Ok(Some(_)) => continue,
                    Ok(None) => {}
                    Err(error) => {
                        warn!(account, %error, "direct WebSocket credential refresh failed")
                    }
                }
            }
            if is_quota_status(status) {
                self.router.quota_failure(account, response.headers()).await;
            } else if is_selected_gateway_failure(status) {
                self.router.soft_failure(account).await;
            } else if status == StatusCode::UNAUTHORIZED
                && matches!(
                    self.config.accounts[account],
                    crate::config::AccountConfig::CodexHome { .. }
                )
            {
                self.router.auth_failure(account).await;
            }
            anyhow::bail!("upstream WebSocket handshake failed with {status}")
        }
        unreachable!()
    }

    async fn run_responses_direct(
        self: &Arc<Self>,
        mut client: UpgradedWebSocket,
        upstream: DirectUpstream,
        listener: ListenerConfig,
        path: String,
        headers: hyper::HeaderMap,
        mut account: String,
    ) -> Result<()> {
        let DirectUpstream {
            socket: mut upstream,
            credentials: mut upstream_credentials,
        } = upstream;
        let mut lease = DirectAccountLease::new(self.router.clone(), account.clone());
        let mut protocol = ProtocolState::new(ProtocolLimits::default())
            .map_err(|error| anyhow::anyhow!("invalid direct protocol limits: {error:?}"))?;
        let mut turns = HashMap::<TurnId, DirectTurn>::new();
        let mut awaiting_response_created: Option<TurnId> = None;
        let mut missing_created_deadline: Option<tokio::time::Instant> = None;
        let mut upstream_idle_deadlines = HashMap::<TurnId, tokio::time::Instant>::new();
        let mut queued_creates = VecDeque::<Message>::new();
        loop {
            let earliest_idle_deadline = earliest_turn_deadline(&upstream_idle_deadlines);
            let queued_message = if awaiting_response_created.is_none() {
                queued_creates.pop_front()
            } else {
                None
            };
            tokio::select! {
                biased;
                _ = wait_for_optional_deadline(missing_created_deadline) => {
                    let close_downstream = self
                        .recover_or_settle_direct_end(
                            &mut protocol,
                            &mut turns,
                            &mut client,
                            &mut upstream,
                            &mut upstream_credentials,
                            &listener,
                            &path,
                            &headers,
                            &mut account,
                            &mut lease,
                            UpstreamEnd::MissingResponseCreatedTimeout,
                        )
                        .await?;
                    awaiting_response_created = None;
                    missing_created_deadline = None;
                    upstream_idle_deadlines.clear();
                    if close_downstream { break; }
                }
                _ = wait_for_optional_deadline(earliest_idle_deadline.map(|(_, deadline)| deadline)) => {
                    let close_downstream = self
                        .recover_or_settle_direct_end(
                            &mut protocol,
                            &mut turns,
                            &mut client,
                            &mut upstream,
                            &mut upstream_credentials,
                            &listener,
                            &path,
                            &headers,
                            &mut account,
                            &mut lease,
                            UpstreamEnd::UpstreamIdleTimeout,
                        )
                        .await?;
                    awaiting_response_created = None;
                    missing_created_deadline = None;
                    upstream_idle_deadlines.clear();
                    if close_downstream { break; }
                }
                client_message = async {
                    match queued_message {
                        Some(message) => Some(Ok(message)),
                        None => client.next().await,
                    }
                } => {
                    let Some(client_message) = client_message else { break };
                    let client_message = client_message.context("read downstream Responses frame")?;
                    if let Message::Text(text) = &client_message {
                        let parsed = serde_json::from_str::<serde_json::Value>(text.as_str()).ok();
                        if parsed.as_ref().and_then(|value| value.get("type")).and_then(serde_json::Value::as_str)
                            == Some("response.create")
                        {
                            if awaiting_response_created.is_some() {
                                if queued_creates.len() >= MAX_QUEUED_DIRECT_CREATES {
                                    send_direct_error(
                                        &mut client,
                                        "server_busy",
                                        "too many response.create frames are waiting for upstream acceptance",
                                    )
                                    .await?;
                                } else {
                                    queued_creates.push_back(client_message);
                                }
                                continue;
                            }
                            if text.len() > self.config.proxy.max_request_bytes {
                                send_direct_error(&mut client, "invalid_request_error", "response.create exceeds configured request limit").await?;
                                continue;
                            }
                            let replay = ReplayBody::from_bytes(
                                bytes::Bytes::copy_from_slice(text.as_bytes()),
                                self.config.proxy.max_request_bytes,
                                self.config.proxy.max_spool_bytes,
                                self.stats.clone(),
                            )?;
                            let mut route = match self
                                .route_websocket_frame(&listener, &headers, &replay, Some(&account))
                                .await
                            {
                                Ok(route) => route,
                                Err(error) => {
                                    send_direct_error(&mut client, "continuity_error", &error.to_string()).await?;
                                    continue;
                                }
                            };
                            if protocol.pending_len() > 0 && route.account_id != account {
                                send_direct_error(
                                    &mut client,
                                    "continuity_owner_conflict",
                                    "cannot switch upstream account while other Responses turns are pending",
                                )
                                .await?;
                                continue;
                            }
                            if protocol.pending_len() == 0 && route.account_id != account {
                                let replacement = match self
                                    .connect_direct_upstream(
                                        &route.account_id,
                                        &path,
                                        &headers,
                                        route.account_id != account,
                                    )
                                    .await
                                {
                                    Ok(replacement) => replacement,
                                    Err(error) => {
                                        send_direct_error(&mut client, "upstream_error", &error.to_string()).await?;
                                        continue;
                                    }
                                };
                                let _ = tokio::time::timeout(
                                    Duration::from_secs(2),
                                    upstream.close(None),
                                )
                                .await;
                                account = route.account_id.clone();
                                lease.replace(account.clone()).await;
                                upstream = replacement.socket;
                                upstream_credentials = replacement.credentials;
                            }
                            let value = parsed.expect("response.create was checked");
                            let analysis = match analyze_response_create(
                                &value,
                                ProtocolLimits::default(),
                            ) {
                                Ok(analysis) => analysis,
                                Err(error) => {
                                    send_direct_error(
                                        &mut client,
                                        "invalid_request_error",
                                        &format!("response.create rejected: {error:?}"),
                                    )
                                    .await?;
                                    continue;
                                }
                            };
                            let turn_id = match protocol.admit_response_create(&value) {
                                Ok(turn_id) => turn_id,
                                Err(error) => {
                                    send_direct_error(
                                        &mut client,
                                        "invalid_request_error",
                                        &format!("response.create rejected: {error:?}"),
                                    )
                                    .await?;
                                    continue;
                                }
                            };
                            send_upgraded_bounded(&mut upstream, client_message.clone()).await?;
                            if analysis.has_nonportable_state {
                                route.hard_owner = true;
                                route.non_previous_hard_owner = true;
                            }
                            awaiting_response_created = Some(turn_id);
                            missing_created_deadline = Some(
                                tokio::time::Instant::now() + RESPONSES_MISSING_CREATED_TIMEOUT,
                            );
                            turns.insert(turn_id, DirectTurn {
                                route,
                                request: client_message,
                                value,
                            });
                            continue;
                        }
                    }
                    let closes = matches!(client_message, Message::Close(_));
                    send_upgraded_bounded(&mut upstream, client_message).await?;
                    if closes { break; }
                }
                upstream_message = upstream.next() => {
                    match upstream_message {
                        Some(Ok(message)) => {
                            if let Message::Close(frame) = &message {
                                let end = UpstreamEnd::Close {
                                    code: frame.as_ref().map_or(1005, |frame| u16::from(frame.code)),
                                };
                                let close_downstream = self
                                    .recover_or_settle_direct_end(
                                        &mut protocol,
                                        &mut turns,
                                        &mut client,
                                        &mut upstream,
                                        &mut upstream_credentials,
                                        &listener,
                                        &path,
                                        &headers,
                                        &mut account,
                                        &mut lease,
                                        end,
                                    )
                                    .await?;
                                if awaiting_response_created
                                    .is_some_and(|turn_id| protocol.turn(turn_id).is_none())
                                {
                                    awaiting_response_created = None;
                                    missing_created_deadline = None;
                                }
                                upstream_idle_deadlines.clear();
                                if close_downstream {
                                    break;
                                }
                                continue;
                            }
                            let parsed = match &message {
                                Message::Text(text) => serde_json::from_str::<serde_json::Value>(text.as_str()).ok(),
                                _ => None,
                            };
                            let Some(event) = parsed else {
                                send_upgraded_bounded(&mut client, message).await?;
                                continue;
                            };
                            let failure = classify_failure(&event);
                            if failure.kind != FailureKind::None
                                && matches!(
                                    event.get("type").and_then(serde_json::Value::as_str),
                                    Some("response.failed" | "response.incomplete" | "response.cancelled" | "error")
                                )
                                && let Some(response_id) = failure.response_id.as_deref()
                            {
                                protocol
                                    .associate_precreated_terminal_response_id(response_id)
                                    .map_err(|error| anyhow::anyhow!("associate pre-created terminal: {error:?}"))?;
                            }
                            let anchor_hint = direct_failure_anchor_hint(&protocol, &failure);
                            let failure_turns = direct_failure_turns(
                                &protocol,
                                &failure,
                                anchor_hint.as_deref(),
                            );
                            if failure.kind != FailureKind::None && failure_turns.len() == 1 {
                                let turn_id = failure_turns[0];
                                if let Some((replacement, replacement_account)) = self
                                    .try_replay_direct_turn(
                                        &mut protocol,
                                        &mut turns,
                                        turn_id,
                                        failure.kind,
                                        ReplayContext::from_failure(&failure),
                                        &listener,
                                        &path,
                                        &headers,
                                        &account,
                                        &upstream_credentials,
                                    )
                                    .await?
                                {
                                    let _ = tokio::time::timeout(
                                        Duration::from_secs(2),
                                        upstream.close(None),
                                    )
                                    .await;
                                    account = replacement_account;
                                    lease.replace(account.clone()).await;
                                    upstream = replacement.socket;
                                    upstream_credentials = replacement.credentials;
                                    upstream_idle_deadlines.clear();
                                    continue;
                                }
                            } else if failure.kind != FailureKind::None {
                                match failure.kind {
                                    FailureKind::Quota => {
                                        self.router
                                            .quota_failure(&account, &hyper::HeaderMap::new())
                                            .await;
                                    }
                                    FailureKind::Authentication { .. } => {
                                        self.router.auth_failure(&account).await;
                                    }
                                    FailureKind::Transient => {
                                        self.router.soft_failure(&account).await;
                                    }
                                    _ => {}
                                }
                            }
                            let association = protocol
                                .observe_upstream_event(&event, anchor_hint.as_deref())
                                .map_err(|error| anyhow::anyhow!("associate upstream event: {error:?}"))?;
                            if association.failure.kind == FailureKind::PreviousResponseNotFound
                                && !association.turn_ids.is_empty()
                            {
                                for turn_id in association.turn_ids {
                                    if awaiting_response_created == Some(turn_id) {
                                        awaiting_response_created = None;
                                        missing_created_deadline = None;
                                    }
                                    let response_id = protocol
                                        .turn(turn_id)
                                        .and_then(|turn| turn.response_id())
                                        .unwrap_or("");
                                    send_upgraded_bounded(
                                        &mut client,
                                        Message::Text(
                                            previous_response_not_found_event(response_id)
                                                .to_string()
                                                .into(),
                                        ),
                                    )
                                    .await?;
                                    let _ = protocol.settle(turn_id, Settlement::Failed);
                                    turns.remove(&turn_id);
                                    upstream_idle_deadlines.remove(&turn_id);
                                }
                                continue;
                            }
                            if association.failure.kind == FailureKind::PreviousResponseNotFound {
                                // Never expose account-scoped continuity identifiers from an
                                // unassociated upstream miss. It may belong to another in-flight
                                // request, so keep our pending turns alive.
                                send_upgraded_bounded(
                                    &mut client,
                                    Message::Text(
                                        previous_response_not_found_error().to_string().into(),
                                    ),
                                )
                                .await?;
                                continue;
                            }
                            if association.event_type.as_deref() == Some("response.created") {
                                for turn_id in &association.turn_ids {
                                    if awaiting_response_created == Some(*turn_id) {
                                        awaiting_response_created = None;
                                        missing_created_deadline = None;
                                        upstream_idle_deadlines.insert(
                                            *turn_id,
                                            tokio::time::Instant::now()
                                                + RESPONSES_UPSTREAM_IDLE_TIMEOUT,
                                        );
                                    }
                                    if let Some(turn) = turns.get(turn_id) {
                                        for key in &turn.route.soft_keys {
                                            self.router.bind(key.clone(), &account).await;
                                        }
                                    }
                                }
                            }
                            if let Some(response_id) = association.response_id.as_deref()
                                && !association.turn_ids.is_empty()
                            {
                                let key = self.router.affinity.key(&format!("previous-response:{response_id}"));
                                self.router.bind(key, &account).await;
                                for turn_id in &association.turn_ids {
                                    if let Some(turn) = turns.get(turn_id) {
                                        for key in &turn.route.soft_keys {
                                            self.router.bind(key.clone(), &account).await;
                                        }
                                    }
                                }
                            }
                            refresh_turn_deadlines(
                                &mut upstream_idle_deadlines,
                                &association.turn_ids,
                                RESPONSES_UPSTREAM_IDLE_TIMEOUT,
                            );
                            send_upgraded_bounded(&mut client, message).await?;
                            for turn_id in &association.turn_ids {
                                protocol
                                    .mark_downstream_delivered(*turn_id, &event)
                                    .map_err(|error| anyhow::anyhow!("mark downstream event: {error:?}"))?;
                            }
                            if let Some(terminal) = association.terminal {
                                let settlement = settlement_for_terminal(terminal);
                                for turn_id in association.turn_ids {
                                    if awaiting_response_created == Some(turn_id) {
                                        awaiting_response_created = None;
                                        missing_created_deadline = None;
                                    }
                                    protocol
                                        .settle(turn_id, settlement)
                                        .map_err(|error| anyhow::anyhow!("settle direct turn: {error:?}"))?;
                                    turns.remove(&turn_id);
                                    upstream_idle_deadlines.remove(&turn_id);
                                }
                            }
                        }
                        Some(Err(error)) => {
                            let close_downstream = self
                                .recover_or_settle_direct_end(
                                    &mut protocol,
                                    &mut turns,
                                    &mut client,
                                    &mut upstream,
                                    &mut upstream_credentials,
                                    &listener,
                                    &path,
                                    &headers,
                                    &mut account,
                                    &mut lease,
                                    UpstreamEnd::TransportError { process_wide: false },
                                )
                                .await?;
                            if awaiting_response_created
                                .is_some_and(|turn_id| protocol.turn(turn_id).is_none())
                            {
                                awaiting_response_created = None;
                                missing_created_deadline = None;
                            } else if awaiting_response_created.is_some() {
                                missing_created_deadline = Some(
                                    tokio::time::Instant::now()
                                        + RESPONSES_MISSING_CREATED_TIMEOUT,
                                );
                            }
                            upstream_idle_deadlines.clear();
                            if close_downstream {
                                return Err(error.into());
                            }
                        }
                        None => {
                            let close_downstream = self
                                .recover_or_settle_direct_end(
                                    &mut protocol,
                                    &mut turns,
                                    &mut client,
                                    &mut upstream,
                                    &mut upstream_credentials,
                                    &listener,
                                    &path,
                                    &headers,
                                    &mut account,
                                    &mut lease,
                                    UpstreamEnd::Eof,
                                )
                                .await?;
                            if awaiting_response_created
                                .is_some_and(|turn_id| protocol.turn(turn_id).is_none())
                            {
                                awaiting_response_created = None;
                                missing_created_deadline = None;
                            } else if awaiting_response_created.is_some() {
                                missing_created_deadline = Some(
                                    tokio::time::Instant::now()
                                        + RESPONSES_MISSING_CREATED_TIMEOUT,
                                );
                            }
                            upstream_idle_deadlines.clear();
                            if close_downstream {
                                break;
                            }
                        },
                    }
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_replay_direct_turn(
        &self,
        protocol: &mut ProtocolState,
        turns: &mut HashMap<TurnId, DirectTurn>,
        turn_id: TurnId,
        failure: FailureKind,
        context: ReplayContext,
        listener: &ListenerConfig,
        path: &str,
        headers: &hyper::HeaderMap,
        account: &str,
        failed_credentials: &Credentials,
    ) -> Result<Option<(DirectUpstream, String)>> {
        match failure {
            FailureKind::Quota => {
                self.router
                    .quota_failure(account, &hyper::HeaderMap::new())
                    .await;
            }
            FailureKind::Transient => self.router.soft_failure(account).await,
            _ => {}
        }
        let Some(turn) = turns.get(&turn_id) else {
            return Ok(None);
        };
        let mut plan = match protocol.replay_plan(turn_id, failure, context) {
            Ok(plan) => plan,
            Err(_) => {
                if matches!(failure, FailureKind::Authentication { .. }) {
                    self.router.auth_failure(account).await;
                }
                return Ok(None);
            }
        };
        let mut replacement_account =
            match plan.target {
                ReplayTarget::SameAccountAfterRefresh => {
                    match self
                        .auth
                        .force_refresh(&self.config.accounts[account], failed_credentials)
                        .await
                    {
                        Ok(Some(_)) => account.to_owned(),
                        Ok(None) | Err(_) => {
                            // Consume the unavailable refresh stage before moving to the explicit
                            // failover stage; otherwise the state machine would propose refresh again.
                            protocol.skip_auth_refresh_stage(turn_id).map_err(|error| {
                                anyhow::anyhow!("consume direct auth refresh stage: {error:?}")
                            })?;
                            plan = protocol.replay_plan(turn_id, failure, context).map_err(
                                |error| anyhow::anyhow!("plan direct auth failover: {error:?}"),
                            )?;
                            String::new()
                        }
                    }
                }
                ReplayTarget::AlternateAccount => String::new(),
                ReplayTarget::Unspecified if failure == FailureKind::PreviousResponseNotFound => {
                    // A safe full resend intentionally drops the stale response anchor. It can and
                    // should recover on the same account, including a one-account deployment.
                    account.to_owned()
                }
                ReplayTarget::Unspecified => String::new(),
            };
        if turn.route.non_previous_hard_owner {
            if plan.target == ReplayTarget::AlternateAccount {
                return Ok(None);
            }
            replacement_account = account.to_owned();
        }
        if plan.target == ReplayTarget::AlternateAccount || replacement_account.is_empty() {
            if matches!(failure, FailureKind::Authentication { .. }) {
                self.router.auth_failure(account).await;
            }
            if turn.route.hard_owner && plan.mode == ReplayMode::OriginalRequest {
                return Ok(None);
            }
            let Some(selection) = self
                .router
                .select(&listener.pool, self.pool(listener)?, None, Some(account))
                .await
            else {
                return Ok(None);
            };
            replacement_account = selection.account_id;
        }
        let mode = plan.mode;
        let mut replacement = match self
            .connect_direct_upstream(
                &replacement_account,
                path,
                headers,
                replacement_account != account
                    || mode == ReplayMode::FreshRequestWithoutPreviousResponse,
            )
            .await
        {
            Ok(replacement) => replacement,
            Err(error) => {
                warn!(%error, account = replacement_account, "safe direct replay reconnect failed");
                if plan.target == ReplayTarget::SameAccountAfterRefresh {
                    protocol.skip_auth_refresh_stage(turn_id).map_err(|error| {
                        anyhow::anyhow!("consume failed direct auth reconnect: {error:?}")
                    })?;
                    return Box::pin(self.try_replay_direct_turn(
                        protocol,
                        turns,
                        turn_id,
                        failure,
                        context,
                        listener,
                        path,
                        headers,
                        account,
                        failed_credentials,
                    ))
                    .await;
                }
                return Ok(None);
            }
        };
        let mut replacement_lease =
            DirectAccountLease::new(self.router.clone(), replacement_account.clone());
        let replay_value = match mode {
            ReplayMode::OriginalRequest => turn.value.clone(),
            ReplayMode::FreshRequestWithoutPreviousResponse => {
                fresh_replay_without_previous_response(&turn.value, ProtocolLimits::default())
                    .map_err(|error| anyhow::anyhow!("prepare fresh direct replay: {error:?}"))?
            }
        };
        let replay_message = match mode {
            ReplayMode::OriginalRequest => turn.request.clone(),
            ReplayMode::FreshRequestWithoutPreviousResponse => {
                Message::Text(serde_json::to_string(&replay_value)?.into())
            }
        };
        let committed = protocol
            .prepare_replay_plan(turn_id, failure, context)
            .map_err(|error| anyhow::anyhow!("commit direct replay plan: {error:?}"))?;
        if committed != plan {
            anyhow::bail!("direct replay plan changed before commit")
        }
        if let Err(error) =
            send_upgraded_bounded(&mut replacement.socket, replay_message.clone()).await
        {
            warn!(%error, account = replacement_account, "safe direct replay send failed");
            return Ok(None);
        }
        if let Some(turn) = turns.get_mut(&turn_id) {
            turn.request = replay_message;
            turn.value = replay_value;
            turn.route.account_id = replacement_account.clone();
            if mode == ReplayMode::FreshRequestWithoutPreviousResponse {
                turn.route.hard_owner = turn.route.non_previous_hard_owner;
            }
        }
        if replacement_account != account {
            protocol
                .reset_auth_sequence_after_account_switch(turn_id)
                .map_err(|error| anyhow::anyhow!("reset direct auth sequence: {error:?}"))?;
        }
        replacement_lease.disarm();
        Ok(Some((replacement, replacement_account)))
    }

    #[allow(clippy::too_many_arguments)]
    async fn recover_or_settle_direct_end(
        &self,
        protocol: &mut ProtocolState,
        turns: &mut HashMap<TurnId, DirectTurn>,
        client: &mut UpgradedWebSocket,
        upstream: &mut UpgradedWebSocket,
        upstream_credentials: &mut Credentials,
        listener: &ListenerConfig,
        path: &str,
        headers: &hyper::HeaderMap,
        account: &mut String,
        lease: &mut DirectAccountLease,
        end: UpstreamEnd,
    ) -> Result<bool> {
        let watchdog = matches!(
            end,
            UpstreamEnd::MissingResponseCreatedTimeout | UpstreamEnd::UpstreamIdleTimeout
        );
        if protocol.pending_len() == 1
            && !watchdog
            && !matches!(end, UpstreamEnd::Close { code: 1000 })
        {
            let turn_id = protocol.pending().next().expect("one pending").id();
            if let Some((replacement, replacement_account)) = self
                .try_replay_direct_turn(
                    protocol,
                    turns,
                    turn_id,
                    FailureKind::Transient,
                    ReplayContext::default(),
                    listener,
                    path,
                    headers,
                    account,
                    upstream_credentials,
                )
                .await?
            {
                let _ = tokio::time::timeout(Duration::from_secs(2), upstream.close(None)).await;
                *account = replacement_account;
                lease.replace(account.clone()).await;
                *upstream = replacement.socket;
                *upstream_credentials = replacement.credentials;
                return Ok(false);
            }
        }
        let plan = protocol.classify_upstream_end(end);
        if plan.penalize_account {
            self.router.soft_failure(account).await;
        }
        for action in plan.turns {
            let response_id = protocol
                .turn(action.turn_id)
                .and_then(|turn| turn.response_id())
                .unwrap_or("")
                .to_owned();
            match action.disposition {
                TurnEndDisposition::RejectedInput => {
                    send_direct_error(
                        client,
                        "upstream_rejected_input",
                        "upstream closed cleanly before accepting response.create",
                    )
                    .await?;
                    let _ = protocol.settle(action.turn_id, Settlement::RejectedInput);
                }
                TurnEndDisposition::StreamIncomplete => {
                    let (code, message) = match end {
                        UpstreamEnd::MissingResponseCreatedTimeout => (
                            "missing_response_created_timeout",
                            "upstream did not acknowledge response.create before the safety deadline",
                        ),
                        UpstreamEnd::UpstreamIdleTimeout => (
                            "upstream_idle_timeout",
                            "upstream response made no progress before the safety deadline",
                        ),
                        _ => (
                            "stream_incomplete",
                            "upstream stream ended before a terminal event",
                        ),
                    };
                    let payload = serde_json::json!({
                        "type":"response.failed",
                        "response":{
                            "id":response_id,
                            "status":"failed",
                            "error":{"type":"server_error","code":code,"message":message,"retryable":false}
                        }
                    });
                    send_upgraded_bounded(client, Message::Text(payload.to_string().into()))
                        .await?;
                    let _ = protocol.settle(action.turn_id, Settlement::Incomplete);
                }
                TurnEndDisposition::StreamIncompleteNoSynthetic => {
                    let _ = protocol.settle(action.turn_id, Settlement::Incomplete);
                }
            }
            turns.remove(&action.turn_id);
        }
        if plan.downstream == DownstreamEndAction::Close1011 {
            let _ = client
                .close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Error,
                    reason: "upstream stream incomplete".into(),
                }))
                .await;
            return Ok(true);
        }
        match self
            .connect_direct_upstream(account, path, headers, true)
            .await
        {
            Ok(replacement) => {
                let _ = tokio::time::timeout(Duration::from_secs(2), upstream.close(None)).await;
                lease.replace(account.clone()).await;
                *upstream = replacement.socket;
                *upstream_credentials = replacement.credentials;
                Ok(false)
            }
            Err(error) => {
                warn!(%error, "failed to reopen direct Responses upstream");
                let _ = client
                    .close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                        code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Error,
                        reason: "upstream reconnect failed".into(),
                    }))
                    .await;
                Ok(true)
            }
        }
    }

    async fn handle_upgrade(
        self: &Arc<Self>,
        mut req: Request<Incoming>,
        listener: &ListenerConfig,
        path: String,
        live_call_id: Option<String>,
    ) -> Result<Response<ProxyBody>> {
        let permit = match self.upgrade_slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.stats
                    .upgrade_admission_rejected
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(upgrade_capacity_response());
            }
        };
        let capacity =
            CapacityLease::new(permit, self.stats.clone(), CapacityKind::UpgradeHandshake);
        let handshake_deadline = tokio::time::Instant::now() + WEBSOCKET_HANDSHAKE_TIMEOUT;
        let inbound_headers = req.headers().clone();
        let pool = self.pool(listener)?;
        let forced_live = live_call_id.is_some();
        let frame_aware_direct = !forced_live
            && is_native_responses(&path)
            && self.config.proxy.responses_websocket_mode == ResponsesWebsocketMode::Direct;
        let affinity_values = metadata::affinity_values(&inbound_headers, None, None, None, &[]);
        let affinity_keys: Vec<_> = affinity_values
            .iter()
            .map(|value| (value.kind, self.router.affinity.key(&value.namespaced())))
            .collect();
        let mut bound_account: Option<String> = None;
        let mut hard_owner = false;
        if !forced_live {
            for (kind, key) in &affinity_keys {
                let Some(binding) = self.router.affinity.get(key).await else {
                    continue;
                };
                if bound_account
                    .as_ref()
                    .is_some_and(|account| account != &binding.account_id)
                {
                    return Ok(error_response(
                        StatusCode::CONFLICT,
                        "continuity_owner_conflict",
                        "websocket continuity keys resolve to different accounts",
                    ));
                }
                hard_owner |= if frame_aware_direct {
                    is_direct_hard_continuity(*kind)
                } else {
                    kind.is_hard_continuity()
                };
                bound_account = Some(binding.account_id);
            }
            if bound_account.is_none()
                && affinity_values
                    .iter()
                    .any(|value| value.kind == metadata::AffinityKind::TurnState)
            {
                return Ok(error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "continuity_owner_unavailable",
                    "websocket carries unknown hard continuity state",
                ));
            }
        }
        let key = affinity_keys.first().map(|(_, key)| key.clone());
        let mut selection = if let Some(call_id) = &live_call_id {
            let Some(account) = self.live_calls.account(call_id).await else {
                return Ok(error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "realtime_call_binding_failed",
                    "realtime call has no valid account binding",
                ));
            };
            let Some(selection) = self.router.select_exact(pool, &account).await else {
                return Ok(error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "realtime_call_binding_failed",
                    "bound realtime account is unavailable",
                ));
            };
            selection
        } else if let Some(account) = &bound_account
            && (!frame_aware_direct || hard_owner)
        {
            let Some(selection) = self.router.select_exact(pool, account).await else {
                return Ok(error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "continuity_owner_unavailable",
                    "required websocket continuity account is unavailable",
                ));
            };
            selection
        } else if let Some(account) = &bound_account {
            self.router
                .select_preferred(&listener.pool, pool, account)
                .await
                .context("no eligible account for fresh direct WebSocket")?
        } else {
            self.router
                .select(
                    &listener.pool,
                    pool,
                    if frame_aware_direct {
                        None
                    } else {
                        key.clone()
                    },
                    None,
                )
                .await
                .context("no eligible account")?
        };
        let attempts = 2;
        for attempt in 0..attempts {
            let uri = self.upstream_uri(&path, live::uses_v1_origin(&path))?;
            let mut upstream_req = Request::builder()
                .method(req.method())
                .uri(uri)
                .body(empty_body())?;
            *upstream_req.headers_mut() = inbound_headers.clone();
            upstream_req.headers_mut().remove(HOST);
            normalize_websocket_beta(upstream_req.headers_mut(), &path);
            let credentials = self
                .auth
                .resolve(
                    &self.config.accounts[&selection.account_id],
                    &inbound_headers,
                )
                .await?;
            apply_credentials(upstream_req.headers_mut(), credentials.clone())?;
            self.router.begin(&selection.account_id).await;
            let mut response = match tokio::time::timeout_at(
                handshake_deadline,
                self.upgrade_client.request(upstream_req),
            )
            .await
            {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    self.router.end(&selection.account_id).await;
                    return Err(error.into());
                }
                Err(_) => {
                    self.router.end(&selection.account_id).await;
                    self.stats
                        .admission_timed_out
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(error_response(
                        StatusCode::GATEWAY_TIMEOUT,
                        "upstream_websocket_handshake_timeout",
                        "upstream WebSocket handshake timed out",
                    ));
                }
            };
            if response.status() == StatusCode::SWITCHING_PROTOCOLS {
                if !selected_subprotocol_is_offered(&inbound_headers, response.headers()) {
                    self.router.end(&selection.account_id).await;
                    return Ok(error_response(
                        StatusCode::BAD_GATEWAY,
                        "upstream_websocket_subprotocol_mismatch",
                        "upstream selected a websocket subprotocol not offered by the client",
                    ));
                }
                for (_, alias) in &affinity_keys {
                    self.router.bind(alias.clone(), &selection.account_id).await;
                }
                if let Some(turn_state) = response
                    .headers()
                    .get("x-codex-turn-state")
                    .and_then(|value| value.to_str().ok())
                    .filter(|value| !value.is_empty())
                {
                    let alias = self
                        .router
                        .affinity
                        .key(&format!("turn-state:{turn_state}"));
                    self.router.bind(alias, &selection.account_id).await;
                }
                let client_upgrade = hyper::upgrade::on(&mut req);
                let upstream_upgrade = hyper::upgrade::on(&mut response);
                let stats = self.stats.clone();
                let router = self.router.clone();
                let account = selection.account_id.clone();
                let app = self.clone();
                let listener = listener.clone();
                let path = path.clone();
                let headers = inbound_headers.clone();
                let direct = !forced_live
                    && is_native_responses(&path)
                    && self.config.proxy.responses_websocket_mode == ResponsesWebsocketMode::Direct;
                stats.open_upgrades.fetch_add(1, Ordering::Relaxed);
                let upgrade_guard = OpenUpgradeGuard(stats.clone());
                let permit = capacity.into_permit();
                let spawned = self
                    .spawn_tracked(async move {
                        let _upgrade_guard = upgrade_guard;
                        let _permit = permit;
                        match tokio::time::timeout_at(handshake_deadline, async {
                            tokio::try_join!(client_upgrade, upstream_upgrade)
                        })
                        .await
                        {
                            Ok(Ok((client, upstream))) => {
                                if direct {
                                    let client = WebSocketStream::from_raw_socket(
                                        TokioIo::new(client),
                                        Role::Server,
                                        None,
                                    )
                                    .await;
                                    let upstream = WebSocketStream::from_raw_socket(
                                        TokioIo::new(upstream),
                                        Role::Client,
                                        None,
                                    )
                                    .await;
                                    if let Err(error) = app
                                        .run_responses_direct(
                                            client,
                                            DirectUpstream {
                                                socket: upstream,
                                                credentials: credentials.clone(),
                                            },
                                            listener,
                                            path,
                                            headers,
                                            account.clone(),
                                        )
                                        .await
                                    {
                                        warn!(%error, "direct Responses WebSocket ended");
                                    }
                                } else {
                                    let _ = tokio::io::copy_bidirectional(
                                        &mut TokioIo::new(client),
                                        &mut TokioIo::new(upstream),
                                    )
                                    .await;
                                    router.end(&account).await;
                                }
                            }
                            Ok(Err(e)) => {
                                warn!(error = %e, "upgrade failed");
                                router.end(&account).await;
                            }
                            Err(_) => {
                                warn!("downstream or upstream WebSocket upgrade timed out");
                                router.end(&account).await;
                            }
                        }
                    })
                    .await;
                if !spawned {
                    self.router.end(&selection.account_id).await;
                    anyhow::bail!("proxy is shutting down")
                }
                return Ok(map_upgrade_response(response));
            }
            self.router.end(&selection.account_id).await;
            if response.status() == StatusCode::UNAUTHORIZED && attempt == 0 {
                match self
                    .auth
                    .force_refresh(&self.config.accounts[&selection.account_id], &credentials)
                    .await
                {
                    Ok(Some(_)) => continue,
                    Ok(None) => {
                        self.router.auth_failure(&selection.account_id).await;
                    }
                    Err(error) => {
                        warn!(account = selection.account_id, %error, "websocket credential refresh failed");
                        self.router.auth_failure(&selection.account_id).await;
                    }
                }
            } else if response.status() == StatusCode::UNAUTHORIZED {
                self.router.auth_failure(&selection.account_id).await;
            }
            let retry = is_quota_status(response.status())
                || is_selected_gateway_failure(response.status());
            if matches!(
                response.status(),
                StatusCode::TOO_MANY_REQUESTS | StatusCode::PAYMENT_REQUIRED
            ) {
                self.router
                    .quota_failure(&selection.account_id, response.headers())
                    .await;
            } else if is_selected_gateway_failure(response.status()) {
                self.router.soft_failure(&selection.account_id).await;
            }
            if retry && !forced_live && !hard_owner && attempt == 0 {
                selection = self
                    .router
                    .select(
                        &listener.pool,
                        pool,
                        key.clone(),
                        Some(&selection.account_id),
                    )
                    .await
                    .context("no alternate account")?;
                for (_, alias) in &affinity_keys {
                    self.router.bind(alias.clone(), &selection.account_id).await;
                }
                continue;
            }
            if response.status() == StatusCode::UNAUTHORIZED
                && matches!(
                    self.config.accounts[&selection.account_id],
                    crate::config::AccountConfig::CodexHome { .. }
                )
            {
                self.router.auth_failure(&selection.account_id).await;
            }
            return Ok(map_http_response(response));
        }
        unreachable!()
    }

    fn upstream_uri(&self, path: &str, v1_origin: bool) -> Result<Uri> {
        if !v1_origin {
            return Ok(format!(
                "{}{}",
                self.config.proxy.upstream.trim_end_matches('/'),
                path
            )
            .parse()?);
        }
        let base: Uri = self.config.proxy.upstream.parse()?;
        let scheme = base.scheme_str().context("upstream has no scheme")?;
        let authority = base.authority().context("upstream has no authority")?;
        Ok(format!("{scheme}://{authority}/v1{path}").parse()?)
    }

    fn pool(&self, listener: &ListenerConfig) -> Result<&PoolConfig> {
        self.config
            .pools
            .get(&listener.pool)
            .context("listener pool disappeared")
    }
}

fn is_authoritative_file_finalize_success(bytes: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|value| {
            value
                .get("status")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .is_some_and(|status| status.eq_ignore_ascii_case("success"))
}

fn apply_credentials(headers: &mut hyper::HeaderMap, credentials: auth::Credentials) -> Result<()> {
    headers.insert(AUTHORIZATION, credentials.authorization.parse()?);
    match credentials.account_id {
        Some(id) => {
            headers.insert("chatgpt-account-id", id.parse()?);
        }
        None => {
            headers.remove("chatgpt-account-id");
        }
    }
    Ok(())
}

fn is_upgrade(req: &Request<Incoming>) -> bool {
    req.headers()
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
}

fn normalize_websocket_beta(headers: &mut hyper::HeaderMap, path: &str) {
    let mut tokens: Vec<String> = headers
        .get("openai-beta")
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|token| !token.is_empty())
                .filter(|token| !token.eq_ignore_ascii_case("responses=experimental"))
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let is_responses = path.split('?').next() == Some("/responses");
    if is_responses {
        if !tokens
            .iter()
            .any(|token| token.eq_ignore_ascii_case("responses_websockets=2026-02-06"))
        {
            tokens.push("responses_websockets=2026-02-06".to_owned());
        }
    } else {
        tokens.retain(|token| {
            !token
                .split_once('=')
                .is_some_and(|(name, _)| name.trim().eq_ignore_ascii_case("responses_websockets"))
        });
    }
    if tokens.is_empty() {
        headers.remove("openai-beta");
    } else if let Ok(value) = tokens.join(", ").parse() {
        headers.insert("openai-beta", value);
    }
}

fn strip_direct_session_headers(headers: &mut hyper::HeaderMap) {
    for name in [
        "x-codex-turn-state",
        "session_id",
        "session-id",
        "x-codex-session-id",
        "x-codex-conversation-id",
        "thread-id",
        "x-codex-parent-thread-id",
        "x-codex-turn-metadata",
    ] {
        headers.remove(name);
    }
}

fn selected_subprotocol_is_offered(
    request: &hyper::HeaderMap,
    response: &hyper::HeaderMap,
) -> bool {
    let Some(selected) = response
        .get("sec-websocket-protocol")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return true;
    };
    request
        .get("sec-websocket-protocol")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|offered| offered.split(',').any(|value| value.trim() == selected))
}

fn is_quota_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS | StatusCode::PAYMENT_REQUIRED
    )
}

fn is_selected_gateway_failure(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
    )
}

fn retryable_http_status(status: StatusCode, method: &Method, path: &str) -> bool {
    if live::is_call_creation(path) {
        return false;
    }
    if is_quota_status(status) {
        return is_native_responses(path)
            || matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS);
    }
    if !is_selected_gateway_failure(status) {
        return false;
    }
    matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) || is_native_responses(path)
}

fn is_native_responses(path: &str) -> bool {
    matches!(
        path.split('?').next().unwrap_or(path),
        "/responses" | "/responses/compact"
    )
}

fn is_legacy_compact_path(path: &str) -> bool {
    path.split('?').next() == Some("/responses/compact")
}

fn responses_path_for_legacy_compact(path: &str) -> String {
    path.strip_prefix("/responses/compact")
        .map(|suffix| format!("/responses{suffix}"))
        .unwrap_or_else(|| path.to_owned())
}

fn rewrite_legacy_compact_request(bytes: &[u8]) -> Result<bytes::Bytes> {
    let mut value: serde_json::Value =
        serde_json::from_slice(bytes).context("parse legacy compact request")?;
    let object = value
        .as_object_mut()
        .context("legacy compact request must be a JSON object")?;
    object.insert("stream".into(), serde_json::Value::Bool(true));
    object.insert("store".into(), serde_json::Value::Bool(false));
    object.remove("text");
    object.remove("tools");
    object.remove("tool_choice");
    let input = object
        .get_mut("input")
        .and_then(serde_json::Value::as_array_mut)
        .context("legacy compact request input must be an array")?;
    if !input.last().is_some_and(|item| {
        item.get("type").and_then(serde_json::Value::as_str) == Some("compaction_trigger")
    }) {
        input.push(serde_json::json!({"type":"compaction_trigger"}));
    }
    Ok(bytes::Bytes::from(serde_json::to_vec(&value)?))
}

fn mark_compaction_request(headers: &mut hyper::HeaderMap) -> Result<()> {
    let mut metadata = headers
        .get("x-codex-turn-metadata")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(value).ok()
        })
        .unwrap_or_default();
    metadata.insert(
        "request_kind".into(),
        serde_json::Value::String("compaction".into()),
    );
    headers.insert(
        "x-codex-turn-metadata",
        serde_json::to_string(&metadata)?.parse()?,
    );
    headers.remove(CONTENT_LENGTH);
    Ok(())
}

async fn map_legacy_compact_response(response: Response<Incoming>) -> Result<Response<ProxyBody>> {
    let (mut parts, mut body) = response.into_parts();
    let mut decoder = SseDecoder::default();
    let mut created_id = None;
    let mut completed = None;
    let mut output = Vec::<(usize, serde_json::Value)>::new();
    let mut received = 0usize;
    while let Some(frame) = body.frame().await {
        let frame = frame.context("read compact response")?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        received = received
            .checked_add(data.len())
            .context("compact response length overflow")?;
        if received > RESPONSES_JSON_RESPONSE_LIMIT {
            anyhow::bail!("compact response exceeds safety limit")
        }
        for event in decoder.push(&data)? {
            capture_legacy_compact_event(event.value, &mut created_id, &mut completed, &mut output);
        }
    }
    for event in decoder.finish()? {
        capture_legacy_compact_event(event.value, &mut created_id, &mut completed, &mut output);
    }
    let completed = completed.context("compact stream ended without a terminal response")?;
    output.sort_by_key(|(index, _)| *index);
    let mut payload = serde_json::Map::new();
    payload.insert(
        "object".into(),
        serde_json::Value::String("response.compact".into()),
    );
    payload.insert(
        "id".into(),
        completed
            .get("id")
            .cloned()
            .or_else(|| created_id.map(serde_json::Value::String))
            .unwrap_or(serde_json::Value::Null),
    );
    payload.insert(
        "status".into(),
        completed
            .get("status")
            .cloned()
            .unwrap_or_else(|| serde_json::Value::String("completed".into())),
    );
    if !output.is_empty() {
        payload.insert(
            "output".into(),
            serde_json::Value::Array(output.into_iter().map(|(_, item)| item).collect()),
        );
    } else if let Some(items) = completed
        .get("output")
        .filter(|value| value.as_array().is_some_and(|items| !items.is_empty()))
    {
        payload.insert("output".into(), items.clone());
    }
    for key in ["usage", "error"] {
        if let Some(value) = completed.get(key).filter(|value| !value.is_null()) {
            payload.insert(key.into(), value.clone());
        }
    }
    let bytes = bytes::Bytes::from(serde_json::to_vec(&payload)?);
    headers::strip_hop_by_hop(&mut parts.headers);
    parts.headers.remove("content-encoding");
    parts
        .headers
        .insert(CONTENT_TYPE, "application/json".parse()?);
    parts.headers.insert(CONTENT_LENGTH, bytes.len().into());
    Ok(Response::from_parts(parts, bytes_body(bytes)))
}

fn capture_legacy_compact_event(
    event: serde_json::Value,
    created_id: &mut Option<String>,
    completed: &mut Option<serde_json::Value>,
    output: &mut Vec<(usize, serde_json::Value)>,
) {
    match event.get("type").and_then(serde_json::Value::as_str) {
        Some("response.created") => {
            if let Some(id) = event
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(serde_json::Value::as_str)
                .filter(|id| !id.is_empty())
            {
                *created_id = Some(id.to_owned());
            }
        }
        Some("response.output_item.done") => {
            if let Some(item) = event.get("item").filter(|item| item.is_object()) {
                let index = event
                    .get("output_index")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|index| usize::try_from(index).ok())
                    .unwrap_or(output.len());
                output.push((index, item.clone()));
            }
        }
        Some("response.completed" | "response.failed" | "response.incomplete") => {
            *completed = event
                .get("response")
                .filter(|value| value.is_object())
                .cloned();
        }
        _ => {}
    }
}

fn is_file_create(method: &Method, path: &str) -> bool {
    *method == Method::POST && path.split('?').next() == Some("/files")
}

fn finalized_file_id(method: &Method, path: &str) -> Option<String> {
    if *method != Method::POST {
        return None;
    }
    let path = path.split('?').next()?;
    let middle = path.strip_prefix("/files/")?.strip_suffix("/uploaded")?;
    (!middle.is_empty() && !middle.contains('/')).then(|| middle.to_owned())
}

fn is_connect_failure(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<hyper_util::client::legacy::Error>()
        .is_some_and(hyper_util::client::legacy::Error::is_connect)
}

fn is_account_neutral_connect_failure(error: &anyhow::Error) -> bool {
    is_connect_failure(error)
        // HttpConnector wraps resolver failures in its private ConnectError with this
        // fixed label; the nested io::Error has no portable kind or raw OS code.
        && (error.chain().any(|source| source.to_string() == "dns error")
            || error
                .chain()
                .filter_map(|source| source.downcast_ref::<std::io::Error>())
                .any(is_shared_network_io_error))
}

fn is_shared_network_io_error(error: &std::io::Error) -> bool {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NetworkUnreachable
            | std::io::ErrorKind::HostUnreachable
            | std::io::ErrorKind::AddrNotAvailable
    ) {
        return true;
    }
    error.raw_os_error().is_some_and(|code| {
        code == libc::ENETDOWN || code == libc::ENETUNREACH || code == libc::EHOSTUNREACH
    })
}

fn map_http_response(response: Response<Incoming>) -> Response<ProxyBody> {
    let (mut parts, body) = response.into_parts();
    headers::strip_hop_by_hop(&mut parts.headers);
    Response::from_parts(parts, incoming_body(body))
}

fn bound_http_response(
    response: Response<ProxyBody>,
    lease: CapacityLease,
    total_deadline: Option<tokio::time::Instant>,
) -> Response<ProxyBody> {
    bound_http_response_with_timeouts(
        response,
        lease,
        total_deadline,
        HTTP_RESPONSE_BODY_IDLE_TIMEOUT,
    )
}

fn bound_http_response_with_timeouts(
    response: Response<ProxyBody>,
    lease: CapacityLease,
    total_deadline: Option<tokio::time::Instant>,
    idle_timeout: Duration,
) -> Response<ProxyBody> {
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        BodyExt::boxed(BoundedHttpBody {
            inner: body,
            lease: Some(lease),
            total: total_deadline.map(|deadline| Box::pin(tokio::time::sleep_until(deadline))),
            idle_timeout,
            idle: Box::pin(tokio::time::sleep(idle_timeout)),
            finished: false,
        }),
    )
}

struct BoundedHttpBody {
    inner: ProxyBody,
    lease: Option<CapacityLease>,
    total: Option<Pin<Box<tokio::time::Sleep>>>,
    idle_timeout: Duration,
    idle: Pin<Box<tokio::time::Sleep>>,
    finished: bool,
}

impl Body for BoundedHttpBody {
    type Data = bytes::Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.finished {
            return Poll::Ready(None);
        }
        if self
            .total
            .as_mut()
            .is_some_and(|deadline| deadline.as_mut().poll(cx).is_ready())
        {
            self.finished = true;
            if let Some(lease) = self.lease.take() {
                lease
                    .stats
                    .admission_timed_out
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "upstream response total deadline exceeded",
            ))));
        }
        if self.idle.as_mut().poll(cx).is_ready() {
            self.finished = true;
            self.lease.take();
            return Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "upstream response body idle timeout",
            ))));
        }
        match Pin::new(&mut self.inner).poll_frame(cx) {
            Poll::Ready(Some(frame)) => {
                let idle_timeout = self.idle_timeout;
                self.idle
                    .as_mut()
                    .reset(tokio::time::Instant::now() + idle_timeout);
                Poll::Ready(Some(frame))
            }
            Poll::Ready(None) => {
                self.finished = true;
                self.lease.take();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.finished || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

fn map_http_response_leased(
    response: Response<Incoming>,
    router: Arc<Router>,
    account: String,
    observe_response_ids: bool,
) -> Response<ProxyBody> {
    let (mut parts, body) = response.into_parts();
    headers::strip_hop_by_hop(&mut parts.headers);
    parts.extensions.insert(SelectedAccount(account.clone()));
    let observer = observe_response_ids
        .then(|| response_observer_for_content_type(parts.headers.get(CONTENT_TYPE)));
    Response::from_parts(
        parts,
        BodyExt::boxed(LeasedIncoming {
            inner: body,
            router,
            account: Some(account),
            observer,
            pending_frame: None,
            pending_binding: None,
            pending_end: false,
        }),
    )
}

struct LeasedIncoming {
    inner: Incoming,
    router: Arc<Router>,
    account: Option<String>,
    observer: Option<HttpResponseObserver>,
    pending_frame: Option<Frame<bytes::Bytes>>,
    pending_binding: Option<Pin<Box<dyn Future<Output = ()> + Send + Sync>>>,
    pending_end: bool,
}

enum HttpResponseObserver {
    Sse(SseDecoder),
    Json(Option<Vec<u8>>),
    /// Wire bytes, not Content-Type, select the observer. At most the small
    /// sniff prefix is retained before committing to streaming SSE or bounded
    /// JSON observation, so a mislabeled SSE never incurs full-body buffering.
    Undecided(Vec<u8>),
}

fn response_observer_for_content_type(
    _content_type: Option<&hyper::header::HeaderValue>,
) -> HttpResponseObserver {
    HttpResponseObserver::Undecided(Vec::new())
}

impl HttpResponseObserver {
    fn observe(&mut self, data: &[u8]) -> Vec<String> {
        match self {
            Self::Sse(decoder) => observe_sse_response_ids(decoder, data),
            Self::Json(json) => {
                if let Some(bytes) = json {
                    if bytes.len().saturating_add(data.len()) <= RESPONSES_JSON_RESPONSE_LIMIT {
                        bytes.extend_from_slice(data);
                    } else {
                        *json = None;
                    }
                }
                Vec::new()
            }
            Self::Undecided(buffered) => {
                let mut probe = buffered.clone();
                let remaining = UNKNOWN_CONTENT_SNIFF_BYTES.saturating_sub(probe.len());
                probe.extend_from_slice(&data[..data.len().min(remaining)]);
                let kind = sniffed_body_kind(&probe).or_else(|| {
                    (probe.len() >= UNKNOWN_CONTENT_SNIFF_BYTES).then_some(SniffedBodyKind::Json)
                });
                let Some(kind) = kind else {
                    *buffered = probe;
                    return Vec::new();
                };
                let previous = std::mem::take(buffered);
                *self = match kind {
                    SniffedBodyKind::Json => Self::Json(Some(Vec::new())),
                    SniffedBodyKind::Sse => Self::Sse(SseDecoder::default()),
                };
                let mut ids = self.observe(&previous);
                ids.extend(self.observe(data));
                ids
            }
        }
    }

    fn finish(self) -> Vec<String> {
        match self {
            Self::Sse(mut decoder) => response_ids_from_sse_finish(&mut decoder),
            Self::Json(Some(bytes)) => response_ids_from_json(&bytes),
            Self::Json(None) => Vec::new(),
            Self::Undecided(bytes) => response_ids_from_json(&bytes),
        }
    }
}

fn observe_sse_response_ids(decoder: &mut SseDecoder, data: &[u8]) -> Vec<String> {
    let mut response_id = None;
    for slice in data.chunks(SSE_DECODE_SLICE_BYTES) {
        if let Ok(events) = decoder.push(slice)
            && response_id.is_none()
        {
            response_id = events.into_iter().find_map(response_id_from_protocol_event);
        }
    }
    response_id.into_iter().collect()
}

fn response_ids_from_sse_finish(decoder: &mut SseDecoder) -> Vec<String> {
    decoder
        .finish()
        .unwrap_or_default()
        .into_iter()
        .filter_map(response_id_from_protocol_event)
        .collect()
}

fn response_id_from_protocol_event(event: ProtocolEvent) -> Option<String> {
    event
        .value
        .get("response")
        .and_then(|response| response.get("id"))
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

impl Body for LeasedIncoming {
    type Data = bytes::Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        loop {
            if let Some(binding) = self.pending_binding.as_mut() {
                if binding.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                self.pending_binding = None;
                if let Some(frame) = self.pending_frame.take() {
                    return Poll::Ready(Some(Ok(frame)));
                }
                if self.pending_end {
                    self.pending_end = false;
                    return Poll::Ready(None);
                }
            }

            match Pin::new(&mut self.inner).poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(error))) => {
                    return Poll::Ready(Some(Err(std::io::Error::other(error))));
                }
                Poll::Ready(Some(Ok(frame))) => {
                    let ids = frame
                        .data_ref()
                        .map(|data| self.observe_response_data(data))
                        .unwrap_or_default();
                    if ids.is_empty() {
                        return Poll::Ready(Some(Ok(frame)));
                    }
                    self.pending_frame = Some(frame);
                    self.pending_binding = self.response_binding(ids);
                }
                Poll::Ready(None) => {
                    let ids = self.finish_response_observer();
                    if ids.is_empty() {
                        return Poll::Ready(None);
                    }
                    self.pending_end = true;
                    self.pending_binding = self.response_binding(ids);
                }
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

impl LeasedIncoming {
    fn observe_response_data(&mut self, data: &[u8]) -> Vec<String> {
        self.observer
            .as_mut()
            .map(|observer| observer.observe(data))
            .unwrap_or_default()
    }

    fn finish_response_observer(&mut self) -> Vec<String> {
        self.observer
            .take()
            .map(HttpResponseObserver::finish)
            .unwrap_or_default()
    }

    fn response_binding(
        &self,
        ids: Vec<String>,
    ) -> Option<Pin<Box<dyn Future<Output = ()> + Send + Sync>>> {
        let account = self.account.clone()?;
        let router = self.router.clone();
        Some(Box::pin(async move {
            for response_id in ids {
                let key = router
                    .affinity
                    .key(&format!("previous-response:{response_id}"));
                router.bind(key, &account).await;
            }
        }))
    }
}

fn response_ids_from_json(bytes: &[u8]) -> Vec<String> {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return Vec::new();
    };
    value
        .get("response")
        .unwrap_or(&value)
        .get("id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
        .map(|id| vec![id.to_owned()])
        .unwrap_or_default()
}

impl Drop for LeasedIncoming {
    fn drop(&mut self) {
        let Some(account) = self.account.take() else {
            return;
        };
        let router = self.router.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                router.end(&account).await;
            });
        }
    }
}

fn map_upgrade_response(response: Response<Incoming>) -> Response<ProxyBody> {
    let (parts, body) = response.into_parts();
    Response::from_parts(parts, incoming_body(body))
}

fn error_response(status: StatusCode, code: &str, message: &str) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(json_body(
            serde_json::json!({"error":{"type":code,"message":message}}),
        ))
        .expect("static response")
}

fn bridge_capacity_response() -> Response<ProxyBody> {
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header(CONTENT_TYPE, "application/json")
        .header("retry-after", "1")
        .body(json_body(serde_json::json!({
            "error": {
                "type": "at_capacity",
                "message": "Responses WebSocket bridge is at capacity; retry shortly"
            }
        })))
        .expect("static bridge capacity response")
}

fn upgrade_capacity_response() -> Response<ProxyBody> {
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header(CONTENT_TYPE, "application/json")
        .header("retry-after", "5")
        .body(json_body(serde_json::json!({
            "error": {
                "type": "at_capacity",
                "message": "WebSocket capacity reached; retry later"
            }
        })))
        .expect("static upgrade capacity response")
}

async fn wait_for_bridge_idle_timeout(
    activity: Arc<BridgeSessionActivity>,
    changed: Arc<Notify>,
    idle_for: Duration,
) {
    loop {
        let elapsed = activity.last_activity().elapsed();
        if activity.is_idle() && elapsed >= idle_for {
            return;
        }
        let wait = if activity.is_idle() {
            idle_for.saturating_sub(elapsed)
        } else {
            Duration::from_secs(1)
        };
        tokio::select! {
            _ = tokio::time::sleep(wait.max(Duration::from_millis(1))) => {}
            _ = changed.notified() => {}
        }
    }
}

fn internal_error(error: anyhow::Error) -> Response<ProxyBody> {
    error!(error = %error, "request failed");
    error_response(StatusCode::BAD_GATEWAY, "proxy_error", &error.to_string())
}

fn warmup_frames(frame: &serde_json::Value) -> [String; 2] {
    let created_at = chrono::Utc::now().timestamp();
    let model = frame
        .get("model")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let base = serde_json::json!({
        "id": "",
        "object": "response",
        "created_at": created_at,
        "model": model,
        "output": [],
    });
    [
        serde_json::json!({
            "type": "response.created",
            "sequence_number": 0,
            "response": {"id":"","object":"response","created_at":created_at,"model":model,"output":[],"status":"in_progress"}
        })
        .to_string(),
        serde_json::json!({
            "type": "response.completed",
            "sequence_number": 1,
            "response": {"id":"","object":"response","created_at":created_at,"model":base["model"],"output":[],"status":"completed"}
        })
        .to_string(),
    ]
}

async fn send_ws_error(outbound: &BridgeSender, kind: &str, message: &str) {
    let (status, retryable) = match kind {
        "server_busy" => (StatusCode::SERVICE_UNAVAILABLE, true),
        "proxy_error" | "websocket_protocol_error" => (StatusCode::BAD_GATEWAY, true),
        _ => (StatusCode::BAD_REQUEST, false),
    };
    let mut safe_headers = serde_json::Map::new();
    if kind == "server_busy" {
        safe_headers.insert("retry-after".into(), serde_json::Value::String("1".into()));
    }
    let error_type = if kind == "websocket_protocol_error" {
        "protocol_error"
    } else {
        kind
    };
    let payload = serde_json::json!({
        "type": "error",
        "status": status.as_u16(),
        "error": {"type": error_type, "code": kind, "message": message, "retryable": retryable},
        "headers": safe_headers,
    })
    .to_string();
    let _ = outbound.send(Message::Text(payload.into())).await;
}

async fn send_ws_nonretryable_liveness_error(
    outbound: &BridgeSender,
    failure: HttpBridgeLivenessFailure,
) {
    let payload = serde_json::json!({
        "type": "error",
        "status": StatusCode::GATEWAY_TIMEOUT.as_u16(),
        "error": {
            "type": "upstream_timeout",
            "code": failure.code(),
            "message": failure.message(),
            "retryable": false
        },
        "headers": {},
    });
    let _ = outbound
        .send(Message::Text(payload.to_string().into()))
        .await;
}

async fn send_direct_error(
    websocket: &mut UpgradedWebSocket,
    kind: &str,
    message: &str,
) -> Result<()> {
    send_upgraded_bounded(
        websocket,
        Message::Text(
            serde_json::json!({
                "type": "error",
                "error": {"type": kind, "message": message}
            })
            .to_string()
            .into(),
        ),
    )
    .await
}

async fn send_upgraded_bounded(websocket: &mut UpgradedWebSocket, message: Message) -> Result<()> {
    match tokio::time::timeout(BRIDGE_SEND_TIMEOUT, websocket.send(message)).await {
        Ok(result) => Ok(result?),
        Err(_) => anyhow::bail!("WebSocket send timed out under downstream backpressure"),
    }
}

fn previous_response_not_found_event(response_id: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "response.failed",
        "response": {
            "id": response_id,
            "status": "failed",
            "error": {
                "type": "invalid_request_error",
                "code": "previous_response_not_found",
                "message": "Previous response was not found. Retrying the full request.",
                "retryable": true
            }
        }
    })
}

fn previous_response_not_found_error() -> serde_json::Value {
    serde_json::json!({
        "type": "error",
        "error": {
            "type": "invalid_request_error",
            "code": "previous_response_not_found",
            "message": "Previous response was not found. Retrying the full request."
        }
    })
}

fn direct_failure_turns(
    protocol: &ProtocolState,
    failure: &FailureClassification,
    anchor_hint: Option<&str>,
) -> Vec<TurnId> {
    if let Some(response_id) = failure.response_id.as_deref() {
        return protocol
            .pending()
            .filter(|turn| turn.response_id() == Some(response_id))
            .map(|turn| turn.id())
            .collect();
    }
    if failure.kind == FailureKind::PreviousResponseNotFound
        && let Some(anchor) = anchor_hint
    {
        return protocol
            .pending()
            .filter(|turn| turn.previous_response_id() == Some(anchor))
            .map(|turn| turn.id())
            .collect();
    }
    if protocol.pending_len() == 1 {
        return protocol.pending().map(|turn| turn.id()).collect();
    }
    Vec::new()
}

fn direct_failure_anchor_hint(
    protocol: &ProtocolState,
    failure: &FailureClassification,
) -> Option<String> {
    if failure.kind != FailureKind::PreviousResponseNotFound || failure.response_id.is_some() {
        return None;
    }
    let message = failure.message.as_deref().unwrap_or_default();
    let mut anchors: Vec<&str> = protocol
        .pending()
        .filter_map(|turn| turn.previous_response_id())
        .filter(|anchor| message.contains(anchor))
        .collect();
    anchors.sort_unstable();
    anchors.dedup();
    if anchors.len() == 1 {
        return Some(anchors[0].to_owned());
    }
    let mut all_anchors: Vec<&str> = protocol
        .pending()
        .filter_map(|turn| turn.previous_response_id())
        .collect();
    all_anchors.sort_unstable();
    all_anchors.dedup();
    (all_anchors.len() == 1).then(|| all_anchors[0].to_owned())
}

fn settlement_for_terminal(terminal: TerminalKind) -> Settlement {
    match terminal {
        TerminalKind::Completed => Settlement::Completed,
        TerminalKind::Failed => Settlement::Failed,
        TerminalKind::Cancelled => Settlement::Cancelled,
        TerminalKind::Incomplete => Settlement::Incomplete,
    }
}

async fn bind_response_id_from_event(app: &Arc<App>, event: &str, account: &str) {
    let Some(response_id) = serde_json::from_str::<serde_json::Value>(event)
        .ok()
        .and_then(|value| {
            value
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(serde_json::Value::as_str)
                .filter(|id| !id.is_empty())
                .map(str::to_owned)
        })
    else {
        return;
    };
    let key = app
        .router
        .affinity
        .key(&format!("previous-response:{response_id}"));
    app.router.bind(key, account).await;
}

async fn pump_http_response_to_websocket(
    response: Response<ProxyBody>,
    outbound: &BridgeSender,
    app: &Arc<App>,
    request_input: Vec<serde_json::Value>,
    continuation: &Arc<StdMutex<Option<HttpBridgeContinuation>>>,
    response_created_deadline: tokio::time::Instant,
    upstream_idle_timeout: Duration,
) -> std::result::Result<(), HttpBridgePumpFailure> {
    let mut capture = HttpBridgeCapture {
        input: request_input,
        response_id: None,
        output: Vec::new(),
        delivered_event: false,
        response_created: false,
        progress_events: 0,
    };
    let mut liveness = None;
    let status = response.status();
    let response_headers = response.headers().clone();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let (parts, mut body) = response.into_parts();
    let selected_account = parts
        .extensions
        .get::<SelectedAccount>()
        .map(|selected| selected.0.clone());
    let result: Result<()> = async {
    if !status.is_success() {
        let bytes = match tokio::time::timeout_at(
            response_created_deadline,
            collect_proxy_body(&mut body, FILE_CREATE_RESPONSE_LIMIT),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                liveness = Some(HttpBridgeLivenessFailure::MissingResponseCreated);
                anyhow::bail!("missing response.created watchdog expired")
            }
        };
        let parsed = serde_json::from_slice::<serde_json::Value>(&bytes).ok();
        let error = parsed
            .as_ref()
            .and_then(|value| value.get("error"))
            .filter(|error| error.is_object())
            .cloned()
            .unwrap_or_else(|| {
                let text = String::from_utf8_lossy(&bytes);
                let text = text.trim();
                let bounded: String = text.chars().take(4096).collect();
                let message = if bounded.is_empty() {
                    format!("HTTP {status}")
                } else {
                    format!("HTTP {status}: {bounded}")
                };
                serde_json::json!({"type":"upstream_error","code":"upstream_error","message":message})
            });
        send_ws_http_error(outbound, status, error, &response_headers).await;
        return Ok(());
    }
    let header_kind = content_type
        .contains("text/event-stream")
        .then_some(SniffedBodyKind::Sse)
        .or_else(|| {
            content_type
                .contains("application/json")
                .then_some(SniffedBodyKind::Json)
        })
        .unwrap_or(SniffedBodyKind::Json);
    // Select the protocol from a bounded wire prefix even when Content-Type is
    // explicit. Intermediaries have been observed to preserve stale response
    // headers; trusting those headers can turn a valid terminal response into
    // a retryable protocol failure. The header is only the fallback when the
    // prefix remains undecidable.
    let (body_kind, initial_chunks) = match tokio::time::timeout_at(
        response_created_deadline,
        sniff_unknown_responses_body(&mut body, header_kind),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => {
            liveness = Some(HttpBridgeLivenessFailure::MissingResponseCreated);
            anyhow::bail!("missing response.created watchdog expired")
        }
    };
    if body_kind == SniffedBodyKind::Sse {
        let mut decoder = SseDecoder::default();
        for data in initial_chunks {
            if send_sse_data(
                &mut decoder,
                &data,
                outbound,
                app,
                selected_account.as_deref(),
                &mut capture,
                continuation,
            )
            .await?
            {
                return Ok(());
            }
        }
        let mut upstream_idle_deadline = capture
            .response_created
            .then(|| tokio::time::Instant::now() + upstream_idle_timeout);
        loop {
            let next_frame = if capture.response_created {
                tokio::time::timeout_at(
                    upstream_idle_deadline.expect("created response has idle deadline"),
                    body.frame(),
                )
                .await
            } else {
                tokio::time::timeout_at(response_created_deadline, body.frame()).await
            };
            let frame = match next_frame {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(_) => {
                    liveness = Some(if capture.response_created {
                        HttpBridgeLivenessFailure::UpstreamIdle
                    } else {
                        HttpBridgeLivenessFailure::MissingResponseCreated
                    });
                    anyhow::bail!("Responses upstream liveness watchdog expired")
                }
            };
            let frame = frame?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            let progress_events = capture.progress_events;
            if send_sse_data(
                &mut decoder,
                &data,
                outbound,
                app,
                selected_account.as_deref(),
                &mut capture,
                continuation,
            )
            .await?
            {
                return Ok(());
            }
            if capture.progress_events != progress_events {
                upstream_idle_deadline =
                    Some(tokio::time::Instant::now() + upstream_idle_timeout);
            }
        }
        if send_protocol_events(
            decoder.finish()?,
            outbound,
            app,
            selected_account.as_deref(),
            &mut capture,
            continuation,
        )
        .await?
        {
            return Ok(());
        }
        anyhow::bail!("upstream SSE ended without a terminal event")
    }
    let bytes = match tokio::time::timeout_at(
        response_created_deadline,
        collect_proxy_body_with_initial(
            &mut body,
            initial_chunks,
            RESPONSES_JSON_RESPONSE_LIMIT,
        ),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => {
            liveness = Some(HttpBridgeLivenessFailure::MissingResponseCreated);
            anyhow::bail!("missing response.created watchdog expired")
        }
    };
    let value: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(json_error) if header_kind == SniffedBodyKind::Json => {
            return Err(json_error.into());
        }
        Err(_) => anyhow::bail!("successful Responses body was not valid JSON"),
    };
    let events = if value.get("type").is_some() {
        vec![ProtocolEvent::from_value(value)?]
    } else {
        let response = value.get("response").cloned().unwrap_or(value);
        responses_json_events(response)?
    };
    if !send_protocol_events(
        events,
        outbound,
        app,
        selected_account.as_deref(),
        &mut capture,
        continuation,
    )
    .await?
    {
        anyhow::bail!("successful Responses JSON produced no terminal event")
    }
    Ok(())
    }
    .await;
    result.map_err(|error| HttpBridgePumpFailure {
        error,
        delivered_event: capture.delivered_event,
        liveness,
    })
}

async fn send_protocol_events(
    events: Vec<ProtocolEvent>,
    outbound: &BridgeSender,
    app: &Arc<App>,
    account: Option<&str>,
    capture: &mut HttpBridgeCapture,
    continuation: &Arc<StdMutex<Option<HttpBridgeContinuation>>>,
) -> Result<bool> {
    for event in events {
        capture.observe(&event.value);
        if let Some(account) = account {
            bind_response_id_from_event(app, &event.payload, account).await;
        }
        let terminal = event.terminal.is_some();
        if event.terminal == Some(sse::TerminalStatus::Completed)
            && let Some(response_id) = capture.response_id.clone()
        {
            *continuation.lock().expect("bridge continuation") = Some(HttpBridgeContinuation {
                response_id,
                input: capture.input.clone(),
                output: capture.output.clone(),
            });
        }
        if !outbound.send(Message::Text(event.payload.into())).await {
            anyhow::bail!("downstream WebSocket backpressure prevented event delivery")
        }
        capture.delivered_event = true;
        if terminal {
            return Ok(true);
        }
    }
    Ok(false)
}

#[allow(clippy::too_many_arguments)]
async fn send_sse_data(
    decoder: &mut SseDecoder,
    data: &[u8],
    outbound: &BridgeSender,
    app: &Arc<App>,
    account: Option<&str>,
    capture: &mut HttpBridgeCapture,
    continuation: &Arc<StdMutex<Option<HttpBridgeContinuation>>>,
) -> Result<bool> {
    for slice in data.chunks(SSE_DECODE_SLICE_BYTES) {
        if send_protocol_events(
            decoder.push(slice)?,
            outbound,
            app,
            account,
            capture,
            continuation,
        )
        .await?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn send_ws_http_error(
    outbound: &BridgeSender,
    status: StatusCode,
    error: serde_json::Value,
    headers: &hyper::HeaderMap,
) {
    let mut safe_headers = serde_json::Map::new();
    for name in [
        "retry-after",
        "x-request-id",
        "openai-request-id",
        "openai-model",
        "x-models-etag",
        "x-reasoning-included",
        "x-codex-turn-state",
        "x-codex-primary-used-percent",
        "x-codex-secondary-used-percent",
        "x-codex-primary-window-minutes",
        "x-codex-secondary-window-minutes",
    ] {
        if let Some(value) = headers.get(name).and_then(|value| value.to_str().ok()) {
            safe_headers.insert(name.to_owned(), serde_json::Value::String(value.to_owned()));
        }
    }
    for (name, value) in headers {
        let name = name.as_str();
        if (name.starts_with("x-ratelimit-") || is_safe_codex_quota_header(name))
            && let Ok(value) = value.to_str()
        {
            safe_headers.insert(name.to_owned(), serde_json::Value::String(value.to_owned()));
        }
    }
    let payload = serde_json::json!({
        "type": "error",
        "status": status.as_u16(),
        "error": error,
        "headers": safe_headers,
    });
    let _ = outbound
        .send(Message::Text(payload.to_string().into()))
        .await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SniffedBodyKind {
    Json,
    Sse,
}

fn sniffed_body_kind(prefix: &[u8]) -> Option<SniffedBodyKind> {
    let trimmed = prefix
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .map(|offset| &prefix[offset..])?;
    if matches!(trimmed.first(), Some(b'{') | Some(b'[')) {
        return Some(SniffedBodyKind::Json);
    }
    [b"data:".as_slice(), b"event:", b"id:", b"retry:", b":"]
        .iter()
        .any(|marker| trimmed.starts_with(marker))
        .then_some(SniffedBodyKind::Sse)
}

async fn sniff_unknown_responses_body(
    body: &mut ProxyBody,
    fallback: SniffedBodyKind,
) -> Result<(SniffedBodyKind, Vec<bytes::Bytes>)> {
    let mut prefix = bytes::BytesMut::new();
    let mut chunks = Vec::new();
    while prefix.len() < UNKNOWN_CONTENT_SNIFF_BYTES {
        let Some(frame) = body.frame().await else {
            break;
        };
        let frame = frame?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        let remaining = UNKNOWN_CONTENT_SNIFF_BYTES - prefix.len();
        prefix.extend_from_slice(&data[..data.len().min(remaining)]);
        chunks.push(data);
        if let Some(kind) = sniffed_body_kind(&prefix) {
            return Ok((kind, chunks));
        }
    }
    Ok((fallback, chunks))
}

fn is_safe_codex_quota_header(name: &str) -> bool {
    name.starts_with("x-codex-")
        && (name.ends_with("-limit-name")
            || (["-primary-", "-secondary-", "-tertiary-"]
                .iter()
                .any(|part| name.contains(part))
                && [
                    "-used-percent",
                    "-window-minutes",
                    "-reset-at",
                    "-reset-after-seconds",
                ]
                .iter()
                .any(|suffix| name.ends_with(suffix))))
}

async fn collect_proxy_body(body: &mut ProxyBody, limit: usize) -> Result<bytes::Bytes> {
    collect_proxy_body_with_initial(body, Vec::new(), limit).await
}

async fn collect_proxy_body_with_initial(
    body: &mut ProxyBody,
    initial: Vec<bytes::Bytes>,
    limit: usize,
) -> Result<bytes::Bytes> {
    let mut bytes = bytes::BytesMut::new();
    for data in initial {
        if bytes.len().saturating_add(data.len()) > limit {
            anyhow::bail!("upstream response exceeds bridge safety limit")
        }
        bytes.extend_from_slice(&data);
    }
    while let Some(frame) = body.frame().await {
        let frame = frame?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if bytes.len().saturating_add(data.len()) > limit {
            anyhow::bail!("upstream response exceeds bridge safety limit")
        }
        bytes.extend_from_slice(&data);
    }
    Ok(bytes.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{AccountConfig, ProxyConfig, ResponsesWebsocketMode},
        routing::AffinityStore,
    };
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper_util::client::legacy::{Client as TestClient, connect::HttpConnector};
    use std::{collections::BTreeMap, fs, path::PathBuf, sync::Mutex, time::Duration};
    use tokio::{io::AsyncWriteExt, net::TcpStream};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    #[derive(Clone, Debug)]
    struct Seen {
        path: String,
        authorization: String,
        body: Bytes,
    }

    #[tokio::test]
    async fn inflight_guard_clears_counter_when_task_is_aborted() {
        let counter = Arc::new(AtomicUsize::new(0));
        let task_counter = Arc::clone(&counter);
        let task = tokio::spawn(async move {
            let _inflight = InflightGuard::new(&task_counter);
            std::future::pending::<()>().await;
        });
        while counter.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn http_inflight_counter_clears_when_connection_task_is_aborted() {
        let dir = tempfile::tempdir().unwrap();
        let (app, mut listener, _, stats) = direct_test_app(dir.path()).await;
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.address = tcp.local_addr().unwrap();
        let proxy = tokio::spawn(
            app.clone()
                .serve_tcp("default".into(), listener.clone(), tcp),
        );
        let mut stream = TcpStream::connect(listener.address).await.unwrap();
        stream
            .write_all(
                b"POST /0123456789abcdef/v1/responses HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\nAuthorization: Bearer caller\r\n\r\n{",
            )
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while stats.inflight_http.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("request should acquire an in-flight slot");

        app.shutdown_connections().await;
        assert_eq!(stats.inflight_http.load(Ordering::Relaxed), 0);

        drop(stream);
        proxy.abort();
    }

    #[tokio::test]
    async fn response_progress_cannot_extend_the_absolute_http_deadline() {
        let stats = Arc::new(Stats::default());
        let slots = Arc::new(Semaphore::new(1));
        let permit = slots.clone().try_acquire_owned().unwrap();
        let lease = CapacityLease::new(permit, stats.clone(), CapacityKind::Http);
        let (sender, receiver) = mpsc::channel::<std::io::Result<Frame<Bytes>>>(4);
        let stream = futures_util::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        });
        let response = Response::new(BodyExt::boxed(http_body_util::StreamBody::new(stream)));
        let response = bound_http_response_with_timeouts(
            response,
            lease,
            Some(tokio::time::Instant::now() + Duration::from_millis(40)),
            Duration::from_secs(1),
        );
        let producer = tokio::spawn(async move {
            loop {
                if sender
                    .send(Ok(Frame::data(Bytes::from_static(b"progress"))))
                    .await
                    .is_err()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        let error =
            tokio::time::timeout(Duration::from_millis(500), response.into_body().collect())
                .await
                .expect("absolute response deadline should fire")
                .expect_err("periodic response frames must not extend the total deadline");
        assert!(
            error.to_string().contains("total deadline exceeded"),
            "{error}"
        );
        assert_eq!(stats.inflight_http.load(Ordering::Acquire), 0);
        assert_eq!(stats.inflight_regular_http.load(Ordering::Acquire), 0);
        assert_eq!(slots.available_permits(), 1);
        producer.await.unwrap();
    }

    #[test]
    fn response_observer_binds_json_without_content_type() {
        let mut observer = response_observer_for_content_type(None);
        assert!(matches!(observer, HttpResponseObserver::Undecided(_)));
        assert!(
            observer
                .observe(br#"{"id":"resp_missing_type","object":"response"}"#)
                .is_empty()
        );
        assert_eq!(observer.finish(), ["resp_missing_type"]);
    }

    #[test]
    fn response_observer_binds_json_with_generic_content_type() {
        let content_type = hyper::header::HeaderValue::from_static("application/octet-stream");
        let mut observer = response_observer_for_content_type(Some(&content_type));
        assert!(matches!(observer, HttpResponseObserver::Undecided(_)));
        assert!(
            observer
                .observe(br#"{"response":{"id":"resp_generic_type"}}"#)
                .is_empty()
        );
        assert_eq!(observer.finish(), ["resp_generic_type"]);
    }

    #[test]
    fn response_observer_keeps_explicit_sse_streaming() {
        let content_type = hyper::header::HeaderValue::from_static("text/event-stream");
        let mut observer = response_observer_for_content_type(Some(&content_type));
        assert!(matches!(observer, HttpResponseObserver::Undecided(_)));
        let event = br#"data: {"type":"response.created","response":{"id":"resp_stream"}}

"#;
        let ids = observer.observe(event);
        assert_eq!(ids, ["resp_stream"]);
        assert!(matches!(observer, HttpResponseObserver::Sse(_)));
        assert!(observer.finish().is_empty());

        // A mislabeled stream takes the dual path, but still discovers the ID
        // from the current frame instead of waiting for EOF/JSON parsing.
        let generic = hyper::header::HeaderValue::from_static("application/octet-stream");
        let mut observer = response_observer_for_content_type(Some(&generic));
        let ids = observer.observe(event);
        assert_eq!(ids, ["resp_stream"]);
        assert!(observer.finish().is_empty());

        // Content-Type cannot override a JSON wire body.
        let mislabeled = hyper::header::HeaderValue::from_static("text/event-stream");
        let mut observer = response_observer_for_content_type(Some(&mislabeled));
        assert!(
            observer
                .observe(b"  {\"id\":\"resp_mislabeled_json\",\"object\":\"response\"}")
                .is_empty()
        );
        assert_eq!(observer.finish(), ["resp_mislabeled_json"]);
    }

    #[test]
    fn response_observer_ignores_malformed_and_non_response_bodies() {
        let mut malformed = response_observer_for_content_type(None);
        assert!(malformed.observe(br#"{"id":"resp_broken""#).is_empty());
        assert!(malformed.finish().is_empty());

        let generic = hyper::header::HeaderValue::from_static("text/plain");
        let mut unrelated = response_observer_for_content_type(Some(&generic));
        assert!(unrelated.observe(br#"{"ok":true,"items":[]}"#).is_empty());
        assert!(unrelated.finish().is_empty());
    }

    #[tokio::test]
    async fn public_constructor_rejects_noncanonical_credential_destinations() {
        for upstream in [
            "https://example.invalid/backend-api/codex",
            "http://127.0.0.1:12345/backend-api/codex",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let managed_home = dir.path().join("managed");
            std::fs::create_dir_all(&managed_home).unwrap();
            let listener = ListenerConfig {
                address: "127.0.0.1:0".parse().unwrap(),
                pool: "default".into(),
            };
            let config = Arc::new(Config {
                proxy: ProxyConfig {
                    upstream: upstream.into(),
                    installation_secret: "0123456789abcdef".into(),
                    affinity_key: "0123456789abcdef0123456789abcdef".into(),
                    state_dir: Some(dir.path().join("state")),
                    ..ProxyConfig::default()
                },
                listeners: BTreeMap::from([("default".into(), listener)]),
                pools: BTreeMap::from([(
                    "default".into(),
                    PoolConfig {
                        members: vec!["managed".into()],
                        preferred: None,
                    },
                )]),
                accounts: BTreeMap::from([(
                    "managed".into(),
                    AccountConfig::CodexHome { path: managed_home },
                )]),
            });
            let affinity = Arc::new(
                AffinityStore::load(
                    dir.path().join("affinity.json"),
                    &config.proxy.affinity_key,
                    100,
                    100_000,
                    Duration::from_secs(60),
                )
                .unwrap(),
            );
            let router = Arc::new(Router::new(&config, affinity));

            let error = match App::new(config, router, Arc::new(Stats::default())) {
                Ok(_) => panic!("public constructor accepted credential destination {upstream}"),
                Err(error) => error,
            };
            assert!(format!("{error:#}").contains("proxy.upstream must be exactly"));
        }
    }

    #[tokio::test]
    async fn websocket_capacity_is_typed_service_unavailable() {
        let response = upgrade_capacity_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "5");
        let body = response.into_body().collect().await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body.to_bytes()).unwrap();
        assert_eq!(payload["error"]["type"], "at_capacity");
    }

    async fn direct_test_app(
        dir: &std::path::Path,
    ) -> (Arc<App>, ListenerConfig, Arc<Router>, Arc<Stats>) {
        direct_test_app_with_upstream(dir, ProxyConfig::default().upstream).await
    }

    async fn direct_test_app_with_upstream(
        dir: &std::path::Path,
        upstream: String,
    ) -> (Arc<App>, ListenerConfig, Arc<Router>, Arc<Stats>) {
        let listener = ListenerConfig {
            address: "127.0.0.1:0".parse().unwrap(),
            pool: "default".into(),
        };
        let config = Arc::new(Config {
            proxy: ProxyConfig {
                upstream,
                responses_websocket_mode: ResponsesWebsocketMode::Direct,
                installation_secret: "0123456789abcdef".into(),
                affinity_key: "0123456789abcdef0123456789abcdef".into(),
                state_dir: Some(dir.join("state")),
                ..ProxyConfig::default()
            },
            listeners: BTreeMap::from([("default".into(), listener.clone())]),
            pools: BTreeMap::from([(
                "default".into(),
                PoolConfig {
                    members: vec!["a".into(), "b".into()],
                    preferred: None,
                },
            )]),
            accounts: BTreeMap::from([
                ("a".into(), AccountConfig::Inbound),
                ("b".into(), AccountConfig::Inbound),
            ]),
        });
        let affinity = Arc::new(
            AffinityStore::load(
                dir.join("affinity.json"),
                &config.proxy.affinity_key,
                100,
                100_000,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Arc::new(Router::new(&config, affinity));
        let stats = Arc::new(Stats::default());
        let app = App::new_unvalidated(config, router.clone(), stats.clone()).unwrap();
        (app, listener, router, stats)
    }

    async fn bridge_admission_test_app(
        dir: &std::path::Path,
        max_bridge_sessions: usize,
        bridge_admission_timeout_millis: u64,
    ) -> Arc<App> {
        let listener = ListenerConfig {
            address: "127.0.0.1:0".parse().unwrap(),
            pool: "default".into(),
        };
        let config = Arc::new(Config {
            proxy: ProxyConfig {
                max_bridge_sessions,
                bridge_admission_timeout_millis,
                installation_secret: "0123456789abcdef".into(),
                affinity_key: "0123456789abcdef0123456789abcdef".into(),
                state_dir: Some(dir.join("state")),
                ..ProxyConfig::default()
            },
            listeners: BTreeMap::from([("default".into(), listener)]),
            pools: BTreeMap::from([(
                "default".into(),
                PoolConfig {
                    members: vec!["caller".into()],
                    preferred: None,
                },
            )]),
            accounts: BTreeMap::from([("caller".into(), AccountConfig::Inbound)]),
        });
        let affinity = Arc::new(
            AffinityStore::load(
                dir.join("affinity.json"),
                &config.proxy.affinity_key,
                100,
                100_000,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Arc::new(Router::new(&config, affinity));
        App::new(config, router, Arc::new(Stats::default())).unwrap()
    }

    fn pending_sse_response(
        initial: &'static [u8],
    ) -> (
        Response<ProxyBody>,
        mpsc::Sender<std::io::Result<Frame<Bytes>>>,
    ) {
        let (sender, receiver) = mpsc::channel(2);
        sender
            .try_send(Ok(Frame::data(Bytes::from_static(initial))))
            .unwrap();
        let stream = futures_util::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        });
        let body = BodyExt::boxed(http_body_util::StreamBody::new(stream));
        (
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "text/event-stream")
                .body(body)
                .unwrap(),
            sender,
        )
    }

    async fn pump_with_test_watchdogs(
        app: &Arc<App>,
        response: Response<ProxyBody>,
        created_after: Duration,
        idle_for: Duration,
    ) -> (HttpBridgePumpFailure, mpsc::Receiver<(u64, Message)>) {
        let (sender, receiver) = mpsc::channel(8);
        let outbound = BridgeSender {
            sender,
            generation: 1,
        };
        let continuation = Arc::new(StdMutex::new(None));
        let failure = pump_http_response_to_websocket(
            response,
            &outbound,
            app,
            Vec::new(),
            &continuation,
            tokio::time::Instant::now() + created_after,
            idle_for,
        )
        .await
        .unwrap_err();
        (failure, receiver)
    }

    #[tokio::test]
    async fn bridge_capacity_evicts_lru_idle_session_without_using_upgrade_slots() {
        let dir = tempfile::tempdir().unwrap();
        let app = bridge_admission_test_app(dir.path(), 1, 500).await;
        let upstream_upgrade_slots = app.upgrade_slots.available_permits();
        let first = app.admit_bridge_session().await.unwrap();
        assert_eq!(
            app.upgrade_slots.available_permits(),
            upstream_upgrade_slots
        );

        let replacement_app = app.clone();
        let replacement =
            tokio::spawn(async move { replacement_app.admit_bridge_session().await.unwrap() });
        first.evicted.await.unwrap();
        app.finish_bridge_session(first.id).await;
        let second = replacement.await.unwrap();

        assert_ne!(first.id, second.id);
        assert_eq!(app.bridge_sessions.lock().await.len(), 1);
        app.finish_bridge_session(second.id).await;
    }

    #[tokio::test]
    async fn http_bridge_comments_do_not_disarm_missing_created_watchdog() {
        let dir = tempfile::tempdir().unwrap();
        let app = bridge_admission_test_app(dir.path(), 1, 500).await;
        let (response, _body_sender) = pending_sse_response(b": keepalive\n\n");

        let (failure, receiver) = pump_with_test_watchdogs(
            &app,
            response,
            Duration::from_millis(30),
            Duration::from_secs(1),
        )
        .await;

        assert_eq!(
            failure.liveness,
            Some(HttpBridgeLivenessFailure::MissingResponseCreated)
        );
        assert!(!failure.delivered_event);
        assert!(receiver.is_empty());
    }

    #[tokio::test]
    async fn http_bridge_prelude_does_not_disarm_missing_created_watchdog() {
        let dir = tempfile::tempdir().unwrap();
        let app = bridge_admission_test_app(dir.path(), 1, 500).await;
        let (response, _body_sender) =
            pending_sse_response(b"data: {\"type\":\"response.queued\",\"sequence_number\":0}\n\n");

        let (failure, mut receiver) = pump_with_test_watchdogs(
            &app,
            response,
            Duration::from_millis(30),
            Duration::from_secs(1),
        )
        .await;

        assert_eq!(
            failure.liveness,
            Some(HttpBridgeLivenessFailure::MissingResponseCreated)
        );
        assert!(failure.delivered_event);
        let (_, message) = receiver.recv().await.unwrap();
        assert!(message.into_text().unwrap().contains("response.queued"));
    }

    #[tokio::test]
    async fn http_bridge_response_metadata_is_visible_but_does_not_disarm_created_watchdog() {
        let dir = tempfile::tempdir().unwrap();
        let app = bridge_admission_test_app(dir.path(), 1, 500).await;
        let (response, _body_sender) = pending_sse_response(
            b"data: {\"type\":\"response.metadata\",\"sequence_number\":7,\"response\":{\"metadata\":{\"trace\":\"opaque\"}}}\n\n",
        );

        let (failure, mut receiver) = pump_with_test_watchdogs(
            &app,
            response,
            Duration::from_millis(30),
            Duration::from_secs(1),
        )
        .await;

        assert_eq!(
            failure.liveness,
            Some(HttpBridgeLivenessFailure::MissingResponseCreated)
        );
        assert!(failure.delivered_event);
        let (_, message) = receiver.recv().await.unwrap();
        let payload: serde_json::Value =
            serde_json::from_str(message.into_text().unwrap().as_ref()).unwrap();
        assert_eq!(payload["type"], "response.metadata");
        assert_eq!(payload["sequence_number"], 7);
        assert_eq!(payload["response"]["metadata"]["trace"], "opaque");
    }

    #[tokio::test]
    async fn http_bridge_created_starts_semantic_progress_watchdog() {
        let dir = tempfile::tempdir().unwrap();
        let app = bridge_admission_test_app(dir.path(), 1, 500).await;
        let (response, _body_sender) = pending_sse_response(
            b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_watchdog\"}}\n\n",
        );

        let (failure, mut receiver) = pump_with_test_watchdogs(
            &app,
            response,
            Duration::from_secs(1),
            Duration::from_millis(30),
        )
        .await;

        assert_eq!(
            failure.liveness,
            Some(HttpBridgeLivenessFailure::UpstreamIdle)
        );
        assert!(failure.delivered_event);
        let (_, message) = receiver.recv().await.unwrap();
        assert!(message.into_text().unwrap().contains("response.created"));
    }

    #[tokio::test]
    async fn http_bridge_heartbeats_do_not_refresh_idle_watchdog() {
        let dir = tempfile::tempdir().unwrap();
        let app = bridge_admission_test_app(dir.path(), 1, 500).await;
        let (response, body_sender) = pending_sse_response(
            b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_watchdog\"}}\n\n",
        );
        let heartbeat_task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
                if body_sender
                    .send(Ok(Frame::data(Bytes::from_static(b": keepalive\n\n"))))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        let (failure, _) = tokio::time::timeout(
            Duration::from_millis(150),
            pump_with_test_watchdogs(
                &app,
                response,
                Duration::from_secs(1),
                Duration::from_millis(30),
            ),
        )
        .await
        .expect("SSE heartbeats must not keep a stalled response alive");
        heartbeat_task.abort();

        assert_eq!(
            failure.liveness,
            Some(HttpBridgeLivenessFailure::UpstreamIdle)
        );
    }

    #[test]
    fn direct_progress_refreshes_only_associated_turn_deadlines() {
        let mut protocol = ProtocolState::new(ProtocolLimits::default()).unwrap();
        let first = protocol
            .admit_response_create(&serde_json::json!({"type":"response.create","input":[]}))
            .unwrap();
        let second = protocol
            .admit_response_create(&serde_json::json!({"type":"response.create","input":[]}))
            .unwrap();
        let original = tokio::time::Instant::now() + Duration::from_secs(1);
        let mut deadlines = HashMap::from([(first, original), (second, original)]);

        refresh_turn_deadlines(&mut deadlines, [&second], Duration::from_secs(30));

        assert_eq!(deadlines[&first], original);
        assert!(deadlines[&second] > original);
        assert_eq!(earliest_turn_deadline(&deadlines).unwrap().0, first);
    }

    #[tokio::test]
    async fn direct_reconnect_handshake_response_is_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let stalled_server = tokio::spawn(async move {
            let (_stream, _) = upstream.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let (app, _, _, _) = direct_test_app_with_upstream(
            dir.path(),
            format!("http://{upstream_addr}/backend-api/codex"),
        )
        .await;
        let mut headers = hyper::HeaderMap::new();
        headers.insert(AUTHORIZATION, "Bearer caller-token".parse().unwrap());

        let error = match app
            .connect_direct_upstream_with_timeout(
                "a",
                "/v1/responses",
                &headers,
                true,
                Duration::from_millis(30),
            )
            .await
        {
            Ok(_) => panic!("stalled upstream handshake unexpectedly connected"),
            Err(error) => error,
        };
        stalled_server.abort();

        assert!(format!("{error:#}").contains("handshake response timed out"));
    }

    #[tokio::test]
    async fn http_bridge_partial_json_cannot_bypass_missing_created_watchdog() {
        let dir = tempfile::tempdir().unwrap();
        let app = bridge_admission_test_app(dir.path(), 1, 500).await;
        let (mut response, _body_sender) = pending_sse_response(b"{\"response\":");
        response
            .headers_mut()
            .insert(CONTENT_TYPE, "application/json".parse().unwrap());

        let (failure, receiver) = pump_with_test_watchdogs(
            &app,
            response,
            Duration::from_millis(30),
            Duration::from_secs(1),
        )
        .await;

        assert_eq!(
            failure.liveness,
            Some(HttpBridgeLivenessFailure::MissingResponseCreated)
        );
        assert!(!failure.delivered_event);
        assert!(receiver.is_empty());
    }

    #[tokio::test]
    async fn http_bridge_error_body_cannot_bypass_missing_created_watchdog() {
        let dir = tempfile::tempdir().unwrap();
        let app = bridge_admission_test_app(dir.path(), 1, 500).await;
        let (mut response, _body_sender) = pending_sse_response(b"{\"error\":");
        *response.status_mut() = StatusCode::BAD_GATEWAY;
        response
            .headers_mut()
            .insert(CONTENT_TYPE, "application/json".parse().unwrap());

        let (failure, receiver) = pump_with_test_watchdogs(
            &app,
            response,
            Duration::from_millis(30),
            Duration::from_secs(1),
        )
        .await;

        assert_eq!(
            failure.liveness,
            Some(HttpBridgeLivenessFailure::MissingResponseCreated)
        );
        assert!(!failure.delivered_event);
        assert!(receiver.is_empty());
    }

    #[tokio::test]
    async fn http_bridge_liveness_error_is_typed_and_nonretryable() {
        let (sender, mut receiver) = mpsc::channel(1);
        let outbound = BridgeSender {
            sender,
            generation: 7,
        };
        send_ws_nonretryable_liveness_error(&outbound, HttpBridgeLivenessFailure::UpstreamIdle)
            .await;
        let (generation, message) = receiver.recv().await.unwrap();
        assert_eq!(generation, 7);
        let payload: serde_json::Value =
            serde_json::from_str(message.into_text().unwrap().as_ref()).unwrap();
        assert_eq!(payload["status"], 504);
        assert_eq!(payload["error"]["code"], "upstream_idle_timeout");
        assert_eq!(payload["error"]["retryable"], false);
    }

    #[tokio::test]
    async fn bridge_capacity_never_evicts_active_turn_and_returns_retryable_response() {
        let dir = tempfile::tempdir().unwrap();
        let app = bridge_admission_test_app(dir.path(), 1, 20).await;
        let first = app.admit_bridge_session().await.unwrap();
        let turn =
            BridgeTurnGuard::new(first.activity.clone(), app.bridge_sessions_changed.clone());

        assert!(app.admit_bridge_session().await.is_none());
        let response = bridge_capacity_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "1");

        drop(turn);
        app.finish_bridge_session(first.id).await;
    }

    #[tokio::test]
    async fn bridge_lru_eviction_closes_idle_socket_before_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let app = bridge_admission_test_app(dir.path(), 1, 1_000).await;
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = tcp.local_addr().unwrap();
        let listener = ListenerConfig {
            address,
            pool: "default".into(),
        };
        let proxy = tokio::spawn(app.clone().serve_tcp("default".into(), listener, tcp));

        let mut first = connect_test_websocket(address).await;
        let second = tokio::time::timeout(Duration::from_secs(2), connect_test_websocket(address))
            .await
            .expect("replacement bridge admission timed out");
        let first_end = tokio::time::timeout(Duration::from_secs(1), first.next())
            .await
            .expect("evicted bridge did not close");
        assert!(matches!(first_end, None | Some(Ok(Message::Close(_)))));
        assert_eq!(app.bridge_sessions.lock().await.len(), 1);

        drop(second);
        proxy.abort();
    }

    #[tokio::test]
    async fn bridge_idle_timeout_waits_for_active_turn_to_finish() {
        let activity = Arc::new(BridgeSessionActivity::new());
        let changed = Arc::new(Notify::new());
        let turn = BridgeTurnGuard::new(activity.clone(), changed.clone());
        assert!(
            tokio::time::timeout(
                Duration::from_millis(25),
                wait_for_bridge_idle_timeout(
                    activity.clone(),
                    changed.clone(),
                    Duration::from_millis(5),
                ),
            )
            .await
            .is_err()
        );

        drop(turn);
        tokio::time::timeout(
            Duration::from_millis(25),
            wait_for_bridge_idle_timeout(
                activity,
                Arc::new(Notify::new()),
                Duration::from_millis(5),
            ),
        )
        .await
        .expect("idle timeout did not resume after the active turn ended");
    }

    #[test]
    fn retries_only_intended_pre_output_statuses() {
        assert!(!retryable_http_status(
            StatusCode::TOO_MANY_REQUESTS,
            &Method::POST,
            "/realtime/calls",
        ));
        assert!(!retryable_http_status(
            StatusCode::INTERNAL_SERVER_ERROR,
            &Method::POST,
            "/responses",
        ));
        assert!(retryable_http_status(
            StatusCode::BAD_GATEWAY,
            &Method::POST,
            "/responses",
        ));
        assert!(!retryable_http_status(
            StatusCode::BAD_GATEWAY,
            &Method::POST,
            "/realtime/calls",
        ));
        assert!(!retryable_http_status(
            StatusCode::SERVICE_UNAVAILABLE,
            &Method::POST,
            "/anything-new",
        ));
    }

    #[test]
    fn only_shared_reachability_errors_are_account_neutral() {
        for kind in [
            std::io::ErrorKind::NetworkUnreachable,
            std::io::ErrorKind::HostUnreachable,
            std::io::ErrorKind::AddrNotAvailable,
        ] {
            assert!(is_shared_network_io_error(&std::io::Error::from(kind)));
        }
        for kind in [
            std::io::ErrorKind::ConnectionRefused,
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::TimedOut,
        ] {
            assert!(!is_shared_network_io_error(&std::io::Error::from(kind)));
        }
        assert!(is_shared_network_io_error(
            &std::io::Error::from_raw_os_error(libc::ENETUNREACH,)
        ));
    }

    #[test]
    fn stale_anchor_errors_keep_the_codex_retry_classifier() {
        let associated = previous_response_not_found_event("resp_current");
        assert_eq!(
            associated
                .pointer("/response/error/code")
                .and_then(serde_json::Value::as_str),
            Some("previous_response_not_found")
        );
        let unassociated = previous_response_not_found_error();
        assert_eq!(
            unassociated
                .pointer("/error/code")
                .and_then(serde_json::Value::as_str),
            Some("previous_response_not_found")
        );
        assert!(!associated.to_string().contains("previous_response_id"));
        assert!(!unassociated.to_string().contains("previous_response_id"));
    }

    #[test]
    fn file_finalization_requires_explicit_success_status() {
        assert!(is_authoritative_file_finalize_success(
            br#"{"status":"success"}"#
        ));
        assert!(is_authoritative_file_finalize_success(
            br#"{"status":"SUCCESS"}"#
        ));
        assert!(!is_authoritative_file_finalize_success(
            br#"{"status":"retry"}"#
        ));
        assert!(!is_authoritative_file_finalize_success(br#"{"ok":true}"#));
    }

    #[test]
    fn codex_quota_header_allowlist_excludes_identity_headers() {
        assert!(is_safe_codex_quota_header("x-codex-tertiary-reset-at"));
        assert!(is_safe_codex_quota_header(
            "x-codex-team-primary-window-minutes"
        ));
        assert!(is_safe_codex_quota_header("x-codex-plan-limit-name"));
        assert!(!is_safe_codex_quota_header("x-codex-account-id"));
        assert!(!is_safe_codex_quota_header("x-codex-turn-metadata"));
    }

    #[test]
    fn rewrites_legacy_compact_request_for_current_responses_protocol() {
        let bytes = rewrite_legacy_compact_request(
            br#"{"input":[{"type":"message","content":"hello"}],"store":true,"stream":false,"tools":[]}"#,
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["stream"], true);
        assert_eq!(value["store"], false);
        assert!(value.get("tools").is_none());
        assert_eq!(
            value["input"].as_array().unwrap().last().unwrap()["type"],
            "compaction_trigger"
        );
        assert_eq!(
            responses_path_for_legacy_compact("/responses/compact?test=1"),
            "/responses?test=1"
        );

        let mut headers = hyper::HeaderMap::new();
        headers.insert(
            "x-codex-turn-metadata",
            r#"{"thread_id":"task"}"#.parse().unwrap(),
        );
        mark_compaction_request(&mut headers).unwrap();
        let metadata: serde_json::Value =
            serde_json::from_str(headers["x-codex-turn-metadata"].to_str().unwrap()).unwrap();
        assert_eq!(metadata["thread_id"], "task");
        assert_eq!(metadata["request_kind"], "compaction");
    }

    #[tokio::test]
    async fn direct_fresh_frame_rotates_off_exhausted_socket_account() {
        let dir = tempfile::tempdir().unwrap();
        let (app, listener, router, stats) = direct_test_app(dir.path()).await;
        let pool = &app.config.pools["default"];
        assert_eq!(
            router
                .select("default", pool, None, None)
                .await
                .unwrap()
                .account_id,
            "a"
        );
        let mut usage = hyper::HeaderMap::new();
        usage.insert("x-codex-primary-used-percent", "100".parse().unwrap());
        router.observe_headers("a", &usage).await;
        let session = router.affinity.key("session:transport");
        assert!(router.bind(session, "a").await);
        let mut headers = hyper::HeaderMap::new();
        headers.insert("session-id", "transport".parse().unwrap());

        let replay = ReplayBody::from_bytes(
            Bytes::from_static(
                br#"{"type":"response.create","client_metadata":{"thread_id":"fresh"},"input":[]}"#,
            ),
            app.config.proxy.max_request_bytes,
            app.config.proxy.max_spool_bytes,
            stats,
        )
        .unwrap();
        let route = app
            .route_websocket_frame(&listener, &headers, &replay, Some("a"))
            .await
            .unwrap();

        assert_eq!(route.account_id, "b");
        assert!(!route.hard_owner);
    }

    #[tokio::test]
    async fn direct_hard_owner_quota_marks_account_without_cross_account_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (app, listener, router, stats) = direct_test_app(dir.path()).await;
        let pool = &app.config.pools["default"];
        assert_eq!(
            router
                .select("default", pool, None, None)
                .await
                .unwrap()
                .account_id,
            "a"
        );
        let turn_state = router.affinity.key("turn-state:opaque");
        assert!(router.bind(turn_state, "a").await);
        let mut headers = hyper::HeaderMap::new();
        headers.insert("x-codex-turn-state", "opaque".parse().unwrap());
        let value = serde_json::json!({"type":"response.create","input":[]});
        let replay = ReplayBody::from_bytes(
            Bytes::copy_from_slice(value.to_string().as_bytes()),
            app.config.proxy.max_request_bytes,
            app.config.proxy.max_spool_bytes,
            stats,
        )
        .unwrap();
        let route = app
            .route_websocket_frame(&listener, &headers, &replay, Some("a"))
            .await
            .unwrap();
        assert!(route.hard_owner);

        let mut protocol = ProtocolState::new(ProtocolLimits::default()).unwrap();
        let turn_id = protocol.admit_response_create(&value).unwrap();
        let mut turns = HashMap::from([(
            turn_id,
            DirectTurn {
                route,
                request: Message::Text(value.to_string().into()),
                value,
            },
        )]);
        let replayed = app
            .try_replay_direct_turn(
                &mut protocol,
                &mut turns,
                turn_id,
                FailureKind::Quota,
                ReplayContext::default(),
                &listener,
                "/v1/responses",
                &headers,
                "a",
                &Credentials {
                    authorization: "Bearer token-a".into(),
                    account_id: None,
                },
            )
            .await
            .unwrap();

        assert!(replayed.is_none());
        assert_eq!(
            router
                .select("default", pool, None, None)
                .await
                .unwrap()
                .account_id,
            "b"
        );
    }

    #[tokio::test]
    async fn translates_legacy_compact_and_commits_alternate_affinity() {
        let dir = tempfile::tempdir().unwrap();
        let seen = Arc::new(Mutex::new(Vec::<Seen>::new()));
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let seen_task = seen.clone();
        let upstream_task = tokio::spawn(async move {
            loop {
                let (stream, _) = upstream.accept().await.unwrap();
                let seen = seen_task.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| {
                        let seen = seen.clone();
                        async move {
                            let path = req.uri().path_and_query().unwrap().to_string();
                            let authorization = req
                                .headers()
                                .get(AUTHORIZATION)
                                .unwrap()
                                .to_str()
                                .unwrap()
                                .to_owned();
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            seen.lock().unwrap().push(Seen {
                                path,
                                authorization: authorization.clone(),
                                body,
                            });
                            let response = if authorization == "Bearer token-a" {
                                Response::builder()
                                    .status(StatusCode::TOO_MANY_REQUESTS)
                                    .body(Full::new(Bytes::from_static(b"quota")))
                                    .unwrap()
                            } else {
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .header(CONTENT_TYPE, "text/event-stream")
                                    .header("x-codex-turn-state", "opaque-state")
                                    .body(Full::new(Bytes::from_static(
                                        br#"data: {"type":"response.created","response":{"id":"resp_compact","status":"in_progress"}}

data: {"type":"response.output_item.done","output_index":0,"item":{"id":"cmp_1","type":"compaction","encrypted_content":"ciphertext"}}

data: {"type":"response.completed","response":{"id":"resp_compact","status":"completed","output":[],"usage":{"input_tokens":12}}}

"#,
                                    )))
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

        let account_a = dir.path().join("a");
        let account_b = dir.path().join("b");
        fs::create_dir_all(&account_a).unwrap();
        fs::create_dir_all(&account_b).unwrap();
        fs::write(
            account_a.join("auth.json"),
            r#"{"tokens":{"access_token":"token-a"}}"#,
        )
        .unwrap();
        fs::write(
            account_b.join("auth.json"),
            r#"{"tokens":{"access_token":"token-b"}}"#,
        )
        .unwrap();

        let proxy_tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_tcp.local_addr().unwrap();
        let listener = ListenerConfig {
            address: proxy_addr,
            pool: "default".into(),
        };
        let config = Arc::new(Config {
            proxy: ProxyConfig {
                upstream: format!("http://{upstream_addr}/backend-api/codex"),
                replay_memory_bytes: 4,
                installation_secret: "0123456789abcdef".into(),
                affinity_key: "0123456789abcdef0123456789abcdef".into(),
                state_dir: Some(dir.path().join("state")),
                ..ProxyConfig::default()
            },
            listeners: BTreeMap::from([("default".into(), listener.clone())]),
            pools: BTreeMap::from([(
                "default".into(),
                PoolConfig {
                    members: vec!["a".into(), "b".into()],
                    preferred: None,
                },
            )]),
            accounts: BTreeMap::from([
                ("a".into(), AccountConfig::CodexHome { path: account_a }),
                ("b".into(), AccountConfig::CodexHome { path: account_b }),
            ]),
        });
        let affinity = Arc::new(
            AffinityStore::load(
                PathBuf::from(dir.path()).join("affinity.json"),
                &config.proxy.affinity_key,
                100,
                100_000,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Arc::new(Router::new(&config, affinity));
        let app = App::new_unvalidated(config, router, Arc::new(Stats::default())).unwrap();
        let proxy_task = tokio::spawn(app.serve_tcp("default".into(), listener, proxy_tcp));

        let client: TestClient<HttpConnector, Full<Bytes>> =
            TestClient::builder(TokioExecutor::new()).build(HttpConnector::new());
        let payload = Bytes::from_static(
            br#"{"client_metadata":{"thread_id":"same-thread"},"input":[{"type":"message","content":"byte-identical"}]}"#,
        );
        for _ in 0..2 {
            let request = Request::builder()
                .method(Method::POST)
                .uri(format!(
                    "http://{proxy_addr}/0123456789abcdef/v1/responses/compact?test=1"
                ))
                .header(AUTHORIZATION, "Bearer inbound-ignored")
                .body(Full::new(payload.clone()))
                .unwrap();
            let response = client.request(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()["x-codex-turn-state"], "opaque-state");
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["object"], "response.compact");
            assert_eq!(body["id"], "resp_compact");
            assert_eq!(body["output"][0]["type"], "compaction");
            assert_eq!(body["output"][0]["encrypted_content"], "ciphertext");
        }

        let calls = seen.lock().unwrap().clone();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].authorization, "Bearer token-a");
        assert_eq!(calls[1].authorization, "Bearer token-b");
        assert_eq!(calls[2].authorization, "Bearer token-b");
        assert!(
            calls
                .iter()
                .all(|v| v.path == "/backend-api/codex/responses?test=1")
        );
        for call in calls {
            let body: serde_json::Value = serde_json::from_slice(&call.body).unwrap();
            assert_eq!(body["stream"], true);
            assert_eq!(body["store"], false);
            assert_eq!(
                body["input"].as_array().unwrap().last().unwrap()["type"],
                "compaction_trigger"
            );
        }

        proxy_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn preserves_quota_response_when_no_alternate_exists() {
        let dir = tempfile::tempdir().unwrap();
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let calls = Arc::new(AtomicU64::new(0));
        let calls_task = calls.clone();
        let upstream_task = tokio::spawn(async move {
            loop {
                let (stream, _) = upstream.accept().await.unwrap();
                let calls = calls_task.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |_req: Request<Incoming>| {
                        calls.fetch_add(1, Ordering::Relaxed);
                        async move {
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(StatusCode::TOO_MANY_REQUESTS)
                                    .header("retry-after", "17")
                                    .header("x-codex-primary-reset-after-seconds", "23")
                                    .body(Full::new(Bytes::from_static(b"quota")))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        let (proxy_addr, proxy_task) = start_caller_proxy(
            dir.path(),
            format!("http://{upstream_addr}/backend-api/codex"),
            ResponsesWebsocketMode::Raw,
        )
        .await;
        let client: TestClient<HttpConnector, Full<Bytes>> =
            TestClient::builder(TokioExecutor::new()).build(HttpConnector::new());
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!(
                "http://{proxy_addr}/0123456789abcdef/v1/responses/compact"
            ))
            .header(AUTHORIZATION, "Bearer caller-token")
            .body(Full::new(Bytes::from_static(
                br#"{"input":[{"type":"message","content":"compact"}]}"#,
            )))
            .unwrap();
        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()["retry-after"], "17");
        assert_eq!(
            response.headers()["x-codex-primary-reset-after-seconds"],
            "23"
        );
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "quota"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        proxy_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn dns_failure_does_not_make_the_account_ineligible() {
        let dir = tempfile::tempdir().unwrap();
        let (proxy_addr, proxy_task, router) = start_caller_proxy_with_router(
            dir.path(),
            "http://comradex-account-neutral.invalid/backend-api/codex".into(),
            ResponsesWebsocketMode::Raw,
        )
        .await;
        let client: TestClient<HttpConnector, Full<Bytes>> =
            TestClient::builder(TokioExecutor::new()).build(HttpConnector::new());
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!(
                "http://{proxy_addr}/0123456789abcdef/v1/responses/compact"
            ))
            .header(AUTHORIZATION, "Bearer caller-token")
            .body(Full::new(Bytes::from_static(
                br#"{"input":[{"type":"message","content":"compact"}]}"#,
            )))
            .unwrap();
        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert!(
            router
                .select(
                    "default",
                    &PoolConfig {
                        members: vec!["caller".into()],
                        preferred: None,
                    },
                    None,
                    None,
                )
                .await
                .is_some()
        );

        proxy_task.abort();
    }

    #[tokio::test]
    async fn uploaded_file_routes_finalize_and_responses_to_creator() {
        let dir = tempfile::tempdir().unwrap();
        let seen = Arc::new(Mutex::new(Vec::<Seen>::new()));
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let seen_task = seen.clone();
        let upstream_task = tokio::spawn(async move {
            loop {
                let (stream, _) = upstream.accept().await.unwrap();
                let seen = seen_task.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| {
                        let seen = seen.clone();
                        async move {
                            let path = req.uri().path_and_query().unwrap().to_string();
                            let authorization = req
                                .headers()
                                .get(AUTHORIZATION)
                                .unwrap()
                                .to_str()
                                .unwrap()
                                .to_owned();
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            seen.lock().unwrap().push(Seen {
                                path: path.clone(),
                                authorization,
                                body,
                            });
                            let mut builder = Response::builder()
                                .status(StatusCode::OK)
                                .header(CONTENT_TYPE, "application/json");
                            let body = if path == "/backend-api/codex/files" {
                                builder = builder.header("x-codex-primary-used-percent", "100");
                                Bytes::from_static(br#"{"file_id":"file_owned","upload_url":"https://blob.invalid/upload"}"#)
                            } else if path.ends_with("/files/file_owned/uploaded") {
                                Bytes::from_static(br#"{"status":"success"}"#)
                            } else {
                                Bytes::from_static(br#"{"ok":true}"#)
                            };
                            Ok::<_, Infallible>(builder.body(Full::new(body)).unwrap())
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        let account_a = dir.path().join("a");
        let account_b = dir.path().join("b");
        fs::create_dir_all(&account_a).unwrap();
        fs::create_dir_all(&account_b).unwrap();
        fs::write(
            account_a.join("auth.json"),
            r#"{"tokens":{"access_token":"token-a"}}"#,
        )
        .unwrap();
        fs::write(
            account_b.join("auth.json"),
            r#"{"tokens":{"access_token":"token-b"}}"#,
        )
        .unwrap();

        let proxy_tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_tcp.local_addr().unwrap();
        let listener = ListenerConfig {
            address: proxy_addr,
            pool: "default".into(),
        };
        let config = Arc::new(Config {
            proxy: ProxyConfig {
                upstream: format!("http://{upstream_addr}/backend-api/codex"),
                installation_secret: "0123456789abcdef".into(),
                affinity_key: "0123456789abcdef0123456789abcdef".into(),
                state_dir: Some(dir.path().join("state")),
                ..ProxyConfig::default()
            },
            listeners: BTreeMap::from([("default".into(), listener.clone())]),
            pools: BTreeMap::from([(
                "default".into(),
                PoolConfig {
                    members: vec!["a".into(), "b".into()],
                    preferred: None,
                },
            )]),
            accounts: BTreeMap::from([
                ("a".into(), AccountConfig::CodexHome { path: account_a }),
                ("b".into(), AccountConfig::CodexHome { path: account_b }),
            ]),
        });
        let affinity = Arc::new(
            AffinityStore::load(
                dir.path().join("affinity.json"),
                &config.proxy.affinity_key,
                100,
                100_000,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Arc::new(Router::new(&config, affinity));
        let app = App::new_unvalidated(config, router, Arc::new(Stats::default())).unwrap();
        let proxy_task = tokio::spawn(app.serve_tcp("default".into(), listener, proxy_tcp));
        let client: TestClient<HttpConnector, Full<Bytes>> =
            TestClient::builder(TokioExecutor::new()).build(HttpConnector::new());
        let base = format!("http://{proxy_addr}/0123456789abcdef/v1");

        for (path, payload) in [
            (
                "/files",
                br#"{"file_name":"a.txt","file_size":1,"use_case":"codex"}"#.as_slice(),
            ),
            ("/responses", br#"{"input":"fresh"}"#.as_slice()),
            ("/files/file_owned/uploaded", br#"{}"#.as_slice()),
            (
                "/responses",
                br#"{"input":[{"type":"input_file","file_id":"file_owned"}]}"#.as_slice(),
            ),
        ] {
            let request = Request::builder()
                .method(Method::POST)
                .uri(format!("{base}{path}"))
                .body(Full::new(Bytes::copy_from_slice(payload)))
                .unwrap();
            let response = client.request(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            response.into_body().collect().await.unwrap();
        }

        let calls = seen.lock().unwrap().clone();
        assert_eq!(calls.len(), 4);
        assert_eq!(calls[0].authorization, "Bearer token-a");
        assert_eq!(calls[1].authorization, "Bearer token-b");
        assert_eq!(calls[2].authorization, "Bearer token-a");
        assert_eq!(calls[3].authorization, "Bearer token-a");

        proxy_task.abort();
        upstream_task.abort();
    }

    async fn start_caller_proxy(
        dir: &std::path::Path,
        upstream: String,
        mode: ResponsesWebsocketMode,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<Result<()>>) {
        let (address, task, _) = start_caller_proxy_with_router(dir, upstream, mode).await;
        (address, task)
    }

    async fn start_caller_proxy_with_router(
        dir: &std::path::Path,
        upstream: String,
        mode: ResponsesWebsocketMode,
    ) -> (
        std::net::SocketAddr,
        tokio::task::JoinHandle<Result<()>>,
        Arc<Router>,
    ) {
        let proxy_tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_tcp.local_addr().unwrap();
        let listener = ListenerConfig {
            address: proxy_addr,
            pool: "default".into(),
        };
        let config = Arc::new(Config {
            proxy: ProxyConfig {
                upstream,
                responses_websocket_mode: mode,
                installation_secret: "0123456789abcdef".into(),
                affinity_key: "0123456789abcdef0123456789abcdef".into(),
                state_dir: Some(dir.join("state")),
                ..ProxyConfig::default()
            },
            listeners: BTreeMap::from([("default".into(), listener.clone())]),
            pools: BTreeMap::from([(
                "default".into(),
                PoolConfig {
                    members: vec!["caller".into()],
                    preferred: None,
                },
            )]),
            accounts: BTreeMap::from([("caller".into(), AccountConfig::Inbound)]),
        });
        let affinity = Arc::new(
            AffinityStore::load(
                dir.join("affinity.json"),
                &config.proxy.affinity_key,
                100,
                100_000,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Arc::new(Router::new(&config, affinity));
        let app = App::new_unvalidated(config, router.clone(), Arc::new(Stats::default())).unwrap();
        let task = tokio::spawn(app.serve_tcp("default".into(), listener, proxy_tcp));
        (proxy_addr, task, router)
    }

    async fn start_two_account_proxy(
        dir: &std::path::Path,
        upstream: String,
    ) -> (
        std::net::SocketAddr,
        tokio::task::JoinHandle<Result<()>>,
        Arc<Router>,
    ) {
        let account_a = dir.join("a");
        let account_b = dir.join("b");
        fs::create_dir_all(&account_a).unwrap();
        fs::create_dir_all(&account_b).unwrap();
        fs::write(
            account_a.join("auth.json"),
            r#"{"tokens":{"access_token":"token-a"}}"#,
        )
        .unwrap();
        fs::write(
            account_b.join("auth.json"),
            r#"{"tokens":{"access_token":"token-b"}}"#,
        )
        .unwrap();

        let proxy_tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_tcp.local_addr().unwrap();
        let listener = ListenerConfig {
            address: proxy_addr,
            pool: "default".into(),
        };
        let config = Arc::new(Config {
            proxy: ProxyConfig {
                upstream,
                responses_websocket_mode: ResponsesWebsocketMode::HttpBridge,
                installation_secret: "0123456789abcdef".into(),
                affinity_key: "0123456789abcdef0123456789abcdef".into(),
                state_dir: Some(dir.join("state")),
                ..ProxyConfig::default()
            },
            listeners: BTreeMap::from([("default".into(), listener.clone())]),
            pools: BTreeMap::from([(
                "default".into(),
                PoolConfig {
                    members: vec!["a".into(), "b".into()],
                    preferred: None,
                },
            )]),
            accounts: BTreeMap::from([
                ("a".into(), AccountConfig::CodexHome { path: account_a }),
                ("b".into(), AccountConfig::CodexHome { path: account_b }),
            ]),
        });
        let affinity = Arc::new(
            AffinityStore::load(
                dir.join("affinity.json"),
                &config.proxy.affinity_key,
                100,
                100_000,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Arc::new(Router::new(&config, affinity));
        let app = App::new_unvalidated(config, router.clone(), Arc::new(Stats::default())).unwrap();
        let task = tokio::spawn(app.serve_tcp("default".into(), listener, proxy_tcp));
        (proxy_addr, task, router)
    }

    async fn start_managed_caller_proxy(
        dir: &std::path::Path,
        upstream: String,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<Result<()>>) {
        let account = dir.join("managed");
        fs::create_dir_all(&account).unwrap();
        fs::write(
            account.join("auth.json"),
            r#"{"tokens":{"access_token":"managed-token"}}"#,
        )
        .unwrap();

        let proxy_tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_tcp.local_addr().unwrap();
        let listener = ListenerConfig {
            address: proxy_addr,
            pool: "default".into(),
        };
        let config = Arc::new(Config {
            proxy: ProxyConfig {
                upstream,
                responses_websocket_mode: ResponsesWebsocketMode::HttpBridge,
                installation_secret: "0123456789abcdef".into(),
                affinity_key: "0123456789abcdef0123456789abcdef".into(),
                state_dir: Some(dir.join("state")),
                ..ProxyConfig::default()
            },
            listeners: BTreeMap::from([("default".into(), listener.clone())]),
            pools: BTreeMap::from([(
                "default".into(),
                PoolConfig {
                    members: vec!["managed".into()],
                    preferred: None,
                },
            )]),
            accounts: BTreeMap::from([(
                "managed".into(),
                AccountConfig::CodexHome { path: account },
            )]),
        });
        let affinity = Arc::new(
            AffinityStore::load(
                dir.join("affinity.json"),
                &config.proxy.affinity_key,
                100,
                100_000,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Arc::new(Router::new(&config, affinity));
        let app = App::new_unvalidated(config, router, Arc::new(Stats::default())).unwrap();
        let task = tokio::spawn(app.serve_tcp("default".into(), listener, proxy_tcp));
        (proxy_addr, task)
    }

    async fn connect_test_websocket_with_headers(
        address: std::net::SocketAddr,
        extra_headers: &[(&str, &str)],
    ) -> WebSocketStream<TcpStream> {
        let stream = TcpStream::connect(address).await.unwrap();
        let mut request = format!("ws://{address}/0123456789abcdef/v1/responses")
            .into_client_request()
            .unwrap();
        for (name, value) in extra_headers {
            request.headers_mut().insert(
                hyper::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                hyper::header::HeaderValue::from_str(value).unwrap(),
            );
        }
        let (websocket, _) = tokio_tungstenite::client_async(request, stream)
            .await
            .unwrap();
        websocket
    }

    async fn connect_test_websocket(address: std::net::SocketAddr) -> WebSocketStream<TcpStream> {
        connect_test_websocket_with_token(address, "caller-token").await
    }

    async fn connect_test_websocket_with_token(
        address: std::net::SocketAddr,
        token: &str,
    ) -> WebSocketStream<TcpStream> {
        let stream = TcpStream::connect(address).await.unwrap();
        let mut request = format!("ws://{address}/0123456789abcdef/v1/responses")
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        let (websocket, _) = tokio_tungstenite::client_async(request, stream)
            .await
            .unwrap();
        websocket
    }

    async fn spawn_websocket_upstream(
        multiplex: bool,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let service = service_fn(move |mut req: Request<Incoming>| async move {
                        let key = req.headers()[SEC_WEBSOCKET_KEY].as_bytes();
                        let accept =
                            tokio_tungstenite::tungstenite::handshake::derive_accept_key(key);
                        let upgrade = hyper::upgrade::on(&mut req);
                        tokio::spawn(async move {
                            let upgraded = upgrade.await.unwrap();
                            let mut websocket = WebSocketStream::from_raw_socket(
                                TokioIo::new(upgraded),
                                Role::Server,
                                None,
                            )
                            .await;
                            if multiplex {
                                let first = websocket.next().await.unwrap().unwrap();
                                assert!(matches!(first, Message::Text(_)));
                                websocket
                                    .send(Message::Text(
                                        serde_json::json!({"type":"response.created","sequence_number":0,"response":{"id":"resp_a","status":"in_progress"}})
                                            .to_string()
                                            .into(),
                                    ))
                                    .await
                                    .unwrap();
                                let second = websocket.next().await.unwrap().unwrap();
                                assert!(matches!(second, Message::Text(_)));
                                for payload in [
                                    serde_json::json!({"type":"response.created","sequence_number":0,"response":{"id":"resp_b","status":"in_progress"}}),
                                    serde_json::json!({"type":"response.completed","sequence_number":1,"response":{"id":"resp_b","status":"completed"}}),
                                    serde_json::json!({"type":"response.completed","sequence_number":1,"response":{"id":"resp_a","status":"completed"}}),
                                ] {
                                    websocket
                                        .send(Message::Text(payload.to_string().into()))
                                        .await
                                        .unwrap();
                                }
                            } else if let Some(Ok(message)) = websocket.next().await {
                                websocket.send(message).await.unwrap();
                            }
                        });
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::SWITCHING_PROTOCOLS)
                                .header(CONNECTION, "Upgrade")
                                .header(UPGRADE, "websocket")
                                .header(SEC_WEBSOCKET_ACCEPT, accept)
                                .body(Full::new(Bytes::new()))
                                .unwrap(),
                        )
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .with_upgrades()
                        .await;
                });
            }
        });
        (address, task)
    }

    async fn spawn_metadata_websocket_upstream()
    -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = service_fn(move |mut req: Request<Incoming>| async move {
                let key = req.headers()[SEC_WEBSOCKET_KEY].as_bytes();
                let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key);
                let upgrade = hyper::upgrade::on(&mut req);
                tokio::spawn(async move {
                    let upgraded = upgrade.await.unwrap();
                    let mut websocket = WebSocketStream::from_raw_socket(
                        TokioIo::new(upgraded),
                        Role::Server,
                        None,
                    )
                    .await;
                    assert!(matches!(websocket.next().await, Some(Ok(Message::Text(_)))));
                    for payload in [
                        serde_json::json!({
                            "type":"response.metadata",
                            "sequence_number":7,
                            "response":{"metadata":{"trace":"opaque"}}
                        }),
                        serde_json::json!({
                            "type":"response.created",
                            "sequence_number":8,
                            "response":{"id":"resp_metadata","status":"in_progress"}
                        }),
                        serde_json::json!({
                            "type":"response.completed",
                            "sequence_number":9,
                            "response":{"id":"resp_metadata","status":"completed"}
                        }),
                    ] {
                        websocket
                            .send(Message::Text(payload.to_string().into()))
                            .await
                            .unwrap();
                    }
                });
                Ok::<_, Infallible>(
                    Response::builder()
                        .status(StatusCode::SWITCHING_PROTOCOLS)
                        .header(CONNECTION, "Upgrade")
                        .header(UPGRADE, "websocket")
                        .header(SEC_WEBSOCKET_ACCEPT, accept)
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades()
                .await;
        });
        (address, task)
    }

    #[tokio::test]
    async fn direct_mode_passes_response_metadata_before_created() {
        let dir = tempfile::tempdir().unwrap();
        let (upstream_addr, upstream_task) = spawn_metadata_websocket_upstream().await;
        let (proxy_addr, proxy_task) = start_caller_proxy(
            dir.path(),
            format!("http://{upstream_addr}/backend-api/codex"),
            ResponsesWebsocketMode::Direct,
        )
        .await;
        let mut websocket = connect_test_websocket(proxy_addr).await;
        websocket
            .send(Message::Text(
                serde_json::json!({"type":"response.create","input":[]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();

        for (expected_type, expected_sequence) in [
            ("response.metadata", 7),
            ("response.created", 8),
            ("response.completed", 9),
        ] {
            let payload: serde_json::Value = serde_json::from_str(
                websocket
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_text()
                    .unwrap()
                    .as_ref(),
            )
            .unwrap();
            assert_eq!(payload["type"], expected_type);
            assert_eq!(payload["sequence_number"], expected_sequence);
        }
        proxy_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn raw_websocket_mode_preserves_binary_frames() {
        let dir = tempfile::tempdir().unwrap();
        let (upstream_addr, upstream_task) = spawn_websocket_upstream(false).await;
        let (proxy_addr, proxy_task) = start_caller_proxy(
            dir.path(),
            format!("http://{upstream_addr}/backend-api/codex"),
            ResponsesWebsocketMode::Raw,
        )
        .await;
        let mut websocket = connect_test_websocket(proxy_addr).await;
        let payload = Bytes::from_static(b"opaque-binary-frame");
        websocket
            .send(Message::Binary(payload.clone()))
            .await
            .unwrap();
        assert_eq!(
            websocket.next().await.unwrap().unwrap(),
            Message::Binary(payload)
        );
        proxy_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn http_bridge_websocket_mode_converts_terminal_sse_events() {
        let dir = tempfile::tempdir().unwrap();
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            loop {
                let (stream, _) = upstream.accept().await.unwrap();
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| async move {
                        assert_eq!(req.headers()[CONTENT_TYPE], "application/json");
                        assert!(req.headers().get(SEC_WEBSOCKET_KEY).is_none());
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header(CONTENT_TYPE, "text/event-stream")
                                .body(Full::new(Bytes::from_static(
                                    b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_bridge\",\"status\":\"in_progress\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_bridge\",\"status\":\"completed\"}}\n\n",
                                )))
                                .unwrap(),
                        )
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        let (proxy_addr, proxy_task) = start_caller_proxy(
            dir.path(),
            format!("http://{upstream_addr}/backend-api/codex"),
            ResponsesWebsocketMode::HttpBridge,
        )
        .await;
        let mut websocket = connect_test_websocket(proxy_addr).await;
        websocket
            .send(Message::Text(
                serde_json::json!({"type":"response.create","input":[]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let created = websocket
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap();
        let completed = websocket
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&created).unwrap()["type"],
            "response.created"
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&completed).unwrap()["type"],
            "response.completed"
        );
        proxy_task.abort();
        upstream_task.abort();
    }

    async fn assert_http_bridge_uses_wire_body_over_content_type(
        content_type: &'static str,
        body: Bytes,
        expected_types: &[&str],
    ) {
        let dir = tempfile::tempdir().unwrap();
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (stream, _) = upstream.accept().await.unwrap();
            let service = service_fn(move |_req: Request<Incoming>| {
                let body = body.clone();
                async move {
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, content_type)
                            .body(Full::new(body))
                            .unwrap(),
                    )
                }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
        let (proxy_addr, proxy_task) = start_caller_proxy(
            dir.path(),
            format!("http://{upstream_addr}/backend-api/codex"),
            ResponsesWebsocketMode::HttpBridge,
        )
        .await;
        let mut websocket = connect_test_websocket(proxy_addr).await;
        websocket
            .send(Message::Text(
                serde_json::json!({"type":"response.create","input":[]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();

        let mut actual = Vec::new();
        for _ in expected_types {
            let message = websocket
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap();
            let event: serde_json::Value = serde_json::from_str(&message).unwrap();
            actual.push(event["type"].as_str().unwrap().to_owned());
        }
        assert_eq!(actual, expected_types);
        proxy_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn http_bridge_accepts_json_mislabeled_as_sse() {
        assert_http_bridge_uses_wire_body_over_content_type(
            "text/event-stream",
            Bytes::from_static(br#"{"id":"resp_json","status":"completed","output":[]}"#),
            &["response.created", "response.completed"],
        )
        .await;
    }

    #[tokio::test]
    async fn http_bridge_accepts_sse_mislabeled_as_json() {
        assert_http_bridge_uses_wire_body_over_content_type(
            "application/json",
            Bytes::from_static(
                b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_sse\",\"status\":\"in_progress\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_sse\",\"status\":\"completed\"}}\n\n",
            ),
            &["response.created", "response.completed"],
        )
        .await;
    }

    #[tokio::test]
    async fn http_bridge_inbound_401_closes_and_reconnect_uses_new_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_seen = seen.clone();
        let upstream_task = tokio::spawn(async move {
            loop {
                let (stream, _) = upstream.accept().await.unwrap();
                let seen = upstream_seen.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| {
                        let seen = seen.clone();
                        async move {
                            let authorization =
                                req.headers()[AUTHORIZATION].to_str().unwrap().to_owned();
                            seen.lock().unwrap().push(authorization.clone());
                            let response = if authorization == "Bearer fresh-token" {
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .header(CONTENT_TYPE, "text/event-stream")
                                    .body(Full::new(Bytes::from_static(
                                        b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_fresh\",\"status\":\"completed\"}}\n\n",
                                    )))
                                    .unwrap()
                            } else {
                                Response::builder()
                                    .status(StatusCode::UNAUTHORIZED)
                                    .header(CONTENT_TYPE, "application/json")
                                    .body(Full::new(Bytes::from_static(
                                        br#"{"error":{"type":"authentication_error","code":"invalid_token","message":"expired credential"}}"#,
                                    )))
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
        let (proxy_addr, proxy_task) = start_caller_proxy(
            dir.path(),
            format!("http://{upstream_addr}/backend-api/codex"),
            ResponsesWebsocketMode::HttpBridge,
        )
        .await;

        let mut stale = connect_test_websocket_with_token(proxy_addr, "stale-token").await;
        stale
            .send(Message::Text(
                serde_json::json!({"type":"response.create","input":[]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let error: serde_json::Value = serde_json::from_str(
            stale
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap()
                .as_ref(),
        )
        .unwrap();
        assert_eq!(error["type"], "error");
        assert_eq!(error["status"], 401);
        assert_eq!(error["error"]["code"], "invalid_token");
        let close = stale.next().await.unwrap().unwrap();
        let Message::Close(Some(close)) = close else {
            panic!("expected WebSocket close after inbound 401, got {close:?}");
        };
        assert_eq!(
            close.code,
            tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy
        );
        assert_eq!(
            close.reason,
            "inbound credentials rejected; reconnect required"
        );

        let mut fresh = connect_test_websocket_with_token(proxy_addr, "fresh-token").await;
        fresh
            .send(Message::Text(
                serde_json::json!({"type":"response.create","input":[]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let completed: serde_json::Value = serde_json::from_str(
            fresh
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap()
                .as_ref(),
        )
        .unwrap();
        assert_eq!(completed["type"], "response.completed");
        assert_eq!(
            *seen.lock().unwrap(),
            ["Bearer stale-token", "Bearer fresh-token"]
        );
        proxy_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn http_bridge_managed_401_keeps_downstream_open() {
        let dir = tempfile::tempdir().unwrap();
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            loop {
                let (stream, _) = upstream.accept().await.unwrap();
                tokio::spawn(async move {
                    let service = service_fn(move |_req: Request<Incoming>| async move {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::UNAUTHORIZED)
                                .header(CONTENT_TYPE, "application/json")
                                .body(Full::new(Bytes::from_static(
                                    br#"{"error":{"type":"authentication_error","code":"invalid_token","message":"managed credential rejected"}}"#,
                                )))
                                .unwrap(),
                        )
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        let (proxy_addr, proxy_task) = start_managed_caller_proxy(
            dir.path(),
            format!("http://{upstream_addr}/backend-api/codex"),
        )
        .await;
        let mut websocket = connect_test_websocket(proxy_addr).await;
        websocket
            .send(Message::Text(
                serde_json::json!({"type":"response.create","input":[]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let error: serde_json::Value = serde_json::from_str(
            websocket
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap()
                .as_ref(),
        )
        .unwrap();
        assert_eq!(error["status"], 401);

        let ping = Bytes::from_static(b"still-open");
        websocket.send(Message::Ping(ping.clone())).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), websocket.next())
                .await
                .expect("managed bridge should remain open")
                .unwrap()
                .unwrap(),
            Message::Pong(ping)
        );
        proxy_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn http_bridge_marks_partial_sse_eof_non_retryable() {
        let dir = tempfile::tempdir().unwrap();
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            loop {
                let (stream, _) = upstream.accept().await.unwrap();
                tokio::spawn(async move {
                    let service = service_fn(move |_req: Request<Incoming>| async move {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header(CONTENT_TYPE, "text/event-stream")
                                .body(Full::new(Bytes::from_static(
                                    b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_partial\",\"status\":\"in_progress\"}}\n\n",
                                )))
                                .unwrap(),
                        )
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        let (proxy_addr, proxy_task) = start_caller_proxy(
            dir.path(),
            format!("http://{upstream_addr}/backend-api/codex"),
            ResponsesWebsocketMode::HttpBridge,
        )
        .await;
        let mut websocket = connect_test_websocket(proxy_addr).await;
        websocket
            .send(Message::Text(
                serde_json::json!({"type":"response.create","input":[]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();

        let created: serde_json::Value = serde_json::from_str(
            websocket
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap()
                .as_ref(),
        )
        .unwrap();
        let close = websocket.next().await.unwrap().unwrap();
        assert_eq!(created["type"], "response.created");
        let Message::Close(Some(frame)) = close else {
            panic!("expected 1011 close after visible partial stream, got {close:?}")
        };
        assert_eq!(
            frame.code,
            tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Error
        );
        assert_eq!(frame.reason, "upstream stream incomplete");
        proxy_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn http_bridge_keeps_pre_output_sse_failure_retryable() {
        let dir = tempfile::tempdir().unwrap();
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            loop {
                let (stream, _) = upstream.accept().await.unwrap();
                tokio::spawn(async move {
                    let service = service_fn(move |_req: Request<Incoming>| async move {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header(CONTENT_TYPE, "text/event-stream")
                                .body(Full::new(Bytes::new()))
                                .unwrap(),
                        )
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        let (proxy_addr, proxy_task) = start_caller_proxy(
            dir.path(),
            format!("http://{upstream_addr}/backend-api/codex"),
            ResponsesWebsocketMode::HttpBridge,
        )
        .await;
        let mut websocket = connect_test_websocket(proxy_addr).await;
        websocket
            .send(Message::Text(
                serde_json::json!({"type":"response.create","input":[]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();

        let error: serde_json::Value = serde_json::from_str(
            websocket
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap()
                .as_ref(),
        )
        .unwrap();
        assert_eq!(error["type"], "error");
        assert_eq!(error["error"]["code"], "websocket_protocol_error");
        assert_eq!(error["error"]["retryable"], true);
        proxy_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn http_bridge_materializes_incremental_previous_response_turns() {
        let dir = tempfile::tempdir().unwrap();
        let bodies = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_bodies = bodies.clone();
        let upstream_task = tokio::spawn(async move {
            loop {
                let (stream, _) = upstream.accept().await.unwrap();
                let bodies = upstream_bodies.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| {
                        let bodies = bodies.clone();
                        async move {
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                            let turn = {
                                let mut bodies = bodies.lock().unwrap();
                                bodies.push(body);
                                bodies.len()
                            };
                            let response_id = format!("resp_bridge_{turn}");
                            let output = serde_json::json!({
                                "type":"message",
                                "role":"assistant",
                                "content":[{"type":"output_text","text":format!("answer {turn}")}]
                            });
                            let events = format!(
                                "data: {}\n\ndata: {}\n\ndata: {}\n\n",
                                serde_json::json!({"type":"response.created","response":{"id":response_id,"status":"in_progress"}}),
                                serde_json::json!({"type":"response.output_item.done","item":output}),
                                serde_json::json!({"type":"response.completed","response":{"id":response_id,"status":"completed"}}),
                            );
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .header(CONTENT_TYPE, "text/event-stream")
                                    .body(Full::new(Bytes::from(events)))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        let (proxy_addr, proxy_task) = start_caller_proxy(
            dir.path(),
            format!("http://{upstream_addr}/backend-api/codex"),
            ResponsesWebsocketMode::HttpBridge,
        )
        .await;
        let mut websocket = connect_test_websocket(proxy_addr).await;
        let first_input = serde_json::json!({"role":"user","content":"first"});
        websocket
            .send(Message::Text(
                serde_json::json!({"type":"response.create","input":[first_input]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        for _ in 0..3 {
            websocket.next().await.unwrap().unwrap();
        }

        let second_input = serde_json::json!({"role":"user","content":"second"});
        websocket
            .send(Message::Text(
                serde_json::json!({
                    "type":"response.create",
                    "previous_response_id":"resp_bridge_1",
                    "input":[second_input]
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        for _ in 0..3 {
            websocket.next().await.unwrap().unwrap();
        }

        let third_input = serde_json::json!({"role":"user","content":"third"});
        websocket
            .send(Message::Text(
                serde_json::json!({
                    "type":"response.create",
                    "previous_response_id":"resp_bridge_2",
                    "input":[
                        {"role":"user","content":"first"},
                        {
                            "type":"message",
                            "role":"assistant",
                            "content":[{"type":"output_text","text":"answer 1"}]
                        },
                        {"role":"user","content":"second"},
                        {
                            "type":"message",
                            "role":"assistant",
                            "content":[{"type":"output_text","text":"answer 2"}]
                        },
                        third_input
                    ]
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        for _ in 0..3 {
            websocket.next().await.unwrap().unwrap();
        }

        let bodies = bodies.lock().unwrap();
        assert_eq!(bodies.len(), 3);
        assert!(bodies[1].get("previous_response_id").is_none());
        assert_eq!(
            bodies[1]["input"],
            serde_json::json!([
                {"role":"user","content":"first"},
                {
                    "type":"message",
                    "role":"assistant",
                    "content":[{"type":"output_text","text":"answer 1"}]
                },
                {"role":"user","content":"second"}
            ])
        );
        assert!(bodies[2].get("previous_response_id").is_none());
        assert_eq!(bodies[2]["input"].as_array().unwrap().len(), 5);
        assert_eq!(
            bodies[2]["input"][4],
            serde_json::json!({"role":"user","content":"third"})
        );
        proxy_task.abort();
        upstream_task.abort();
    }

    #[test]
    fn http_bridge_continuation_rejects_partial_history_overlap() {
        let cached = HttpBridgeContinuation {
            response_id: "resp_anchor".into(),
            input: vec![serde_json::json!({"role":"user","content":"first"})],
            output: vec![serde_json::json!({"role":"assistant","content":"answer"})],
        };
        let error = materialize_http_bridge_continuation(
            cached,
            vec![
                serde_json::json!({"role":"user","content":"first"}),
                serde_json::json!({"role":"user","content":"second"}),
            ],
        )
        .unwrap_err();
        assert!(error.contains("partially overlaps"));
    }

    #[test]
    fn http_bridge_continuation_materializes_pure_delta() {
        let first = serde_json::json!({"role":"user","content":"first"});
        let answer = serde_json::json!({"role":"assistant","content":"answer"});
        let second = serde_json::json!({"role":"user","content":"second"});
        let cached = HttpBridgeContinuation {
            response_id: "resp_anchor".into(),
            input: vec![first.clone()],
            output: vec![answer.clone()],
        };
        assert_eq!(
            materialize_http_bridge_continuation(cached, vec![second.clone()]).unwrap(),
            vec![first, answer, second]
        );
    }

    #[test]
    fn http_bridge_continuation_accepts_exact_cached_prefix() {
        let first = serde_json::json!({"role":"user","content":"first"});
        let answer = serde_json::json!({"role":"assistant","content":"answer"});
        let cached = HttpBridgeContinuation {
            response_id: "resp_anchor".into(),
            input: vec![first.clone()],
            output: vec![answer.clone()],
        };
        assert_eq!(
            materialize_http_bridge_continuation(cached, vec![first.clone(), answer.clone()])
                .unwrap(),
            vec![first, answer]
        );
    }

    #[test]
    fn http_bridge_continuation_rejects_boundary_overlap() {
        let first = serde_json::json!({"role":"user","content":"first"});
        let answer = serde_json::json!({"role":"assistant","content":"answer"});
        let cached = HttpBridgeContinuation {
            response_id: "resp_anchor".into(),
            input: vec![first],
            output: vec![answer.clone()],
        };
        let error = materialize_http_bridge_continuation(
            cached,
            vec![
                answer,
                serde_json::json!({"role":"user","content":"second"}),
            ],
        )
        .unwrap_err();
        assert!(error.contains("partially overlaps"));
    }

    #[test]
    fn boundary_overlap_matching_is_linear_for_repeated_histories() {
        #[derive(Clone)]
        struct CountedValue {
            value: u8,
            comparisons: Arc<AtomicUsize>,
        }

        impl PartialEq for CountedValue {
            fn eq(&self, other: &Self) -> bool {
                self.comparisons.fetch_add(1, Ordering::Relaxed);
                self.value == other.value
            }
        }

        let comparisons = Arc::new(AtomicUsize::new(0));
        let counted = |value| CountedValue {
            value,
            comparisons: Arc::clone(&comparisons),
        };
        let mut prefix = vec![counted(1); HTTP_BRIDGE_MAX_MATERIALIZED_ITEMS - 1];
        prefix.push(counted(2));
        let incoming = vec![counted(1); HTTP_BRIDGE_MAX_MATERIALIZED_ITEMS];

        assert!(!has_suffix_prefix_overlap(&prefix, &incoming));
        assert!(
            comparisons.load(Ordering::Relaxed) < HTTP_BRIDGE_MAX_MATERIALIZED_ITEMS * 6,
            "overlap matching performed too many comparisons"
        );
    }

    #[test]
    fn http_bridge_continuation_empty_cache_preserves_input() {
        let incoming = vec![serde_json::json!({"role":"user","content":"first"})];
        let cached = HttpBridgeContinuation {
            response_id: "resp_anchor".into(),
            input: Vec::new(),
            output: Vec::new(),
        };
        assert_eq!(
            materialize_http_bridge_continuation(cached, incoming.clone()).unwrap(),
            incoming
        );
    }

    #[test]
    fn http_bridge_continuation_defers_byte_limit_to_configured_request_limit() {
        let large = "x".repeat(RESPONSES_JSON_RESPONSE_LIMIT + 1);
        let cached = HttpBridgeContinuation {
            response_id: "resp_anchor".into(),
            input: Vec::new(),
            output: Vec::new(),
        };
        let input = materialize_http_bridge_continuation(
            cached,
            vec![serde_json::json!({"role":"user","content":large})],
        )
        .expect("materialization must not impose an unrelated response-size limit");
        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!({"input":input})).expect("request JSON"),
        );
        let configured_limit = body.len();
        ReplayBody::from_bytes(
            body,
            configured_limit,
            configured_limit,
            Arc::new(Stats::default()),
        )
        .expect("the configured request limit accepts the materialized body");
    }

    #[test]
    fn http_bridge_continuation_bounds_full_history_resends() {
        let cached = HttpBridgeContinuation {
            response_id: "resp_anchor".into(),
            input: vec![serde_json::json!({"role":"user","content":"first"})],
            output: Vec::new(),
        };
        let mut incoming = vec![serde_json::json!({"role":"user","content":"first"})];
        incoming.resize(
            HTTP_BRIDGE_MAX_MATERIALIZED_ITEMS + 1,
            serde_json::Value::Null,
        );
        let error = materialize_http_bridge_continuation(cached, incoming).unwrap_err();
        assert!(error.contains("safety limit"));
    }

    #[tokio::test]
    async fn http_bridge_routes_fresh_frame_conversations_after_usage_exhaustion() {
        let dir = tempfile::tempdir().unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_seen = seen.clone();
        let upstream_task = tokio::spawn(async move {
            loop {
                let (stream, _) = upstream.accept().await.unwrap();
                let seen = upstream_seen.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| {
                        let seen = seen.clone();
                        async move {
                            let authorization =
                                req.headers()[AUTHORIZATION].to_str().unwrap().to_owned();
                            seen.lock().unwrap().push(authorization);
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .header(CONTENT_TYPE, "text/event-stream")
                                    .header("x-codex-primary-used-percent", "100")
                                    .body(Full::new(Bytes::from_static(
                                        b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp\",\"status\":\"in_progress\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp\",\"status\":\"completed\"}}\n\n",
                                    )))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        let (proxy_addr, proxy_task, _) = start_two_account_proxy(
            dir.path(),
            format!("http://{upstream_addr}/backend-api/codex"),
        )
        .await;
        let mut websocket = connect_test_websocket_with_headers(
            proxy_addr,
            &[("session-id", "reused-client-session")],
        )
        .await;

        for thread in ["fresh-thread-a", "fresh-thread-b"] {
            websocket
                .send(Message::Text(
                    serde_json::json!({
                        "type":"response.create",
                        "client_metadata":{"thread_id":thread},
                        "prompt_cache_key":"reused-client-cache",
                        "input":[]
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            for expected in ["response.created", "response.completed"] {
                let message = websocket
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_text()
                    .unwrap();
                let event: serde_json::Value = serde_json::from_str(&message).unwrap();
                assert_eq!(event["type"], expected);
            }
        }

        assert_eq!(
            *seen.lock().unwrap(),
            ["Bearer token-a".to_owned(), "Bearer token-b".to_owned()]
        );
        proxy_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn http_nonportable_payload_stays_on_its_first_dispatch_account() {
        let dir = tempfile::tempdir().unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_seen = seen.clone();
        let upstream_task = tokio::spawn(async move {
            loop {
                let (stream, _) = upstream.accept().await.unwrap();
                let seen = upstream_seen.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| {
                        seen.lock()
                            .unwrap()
                            .push(req.headers()[AUTHORIZATION].to_str().unwrap().to_owned());
                        async move {
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(StatusCode::TOO_MANY_REQUESTS)
                                    .body(Full::new(Bytes::from_static(b"quota")))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        let (proxy_addr, proxy_task, _) = start_two_account_proxy(
            dir.path(),
            format!("http://{upstream_addr}/backend-api/codex"),
        )
        .await;
        let client: TestClient<HttpConnector, Full<Bytes>> =
            TestClient::builder(TokioExecutor::new()).build(HttpConnector::new());
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!(
                "http://{proxy_addr}/0123456789abcdef/v1/responses"
            ))
            .header(AUTHORIZATION, "Bearer inbound-ignored")
            .body(Full::new(Bytes::from_static(
                br#"{"model":"gpt-test","input":[{"type":"reasoning","id":"rs_owner","encrypted_content":"ciphertext"}],"stream":true}"#,
            )))
            .unwrap();

        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(*seen.lock().unwrap(), ["Bearer token-a"]);

        proxy_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn http_bridge_preserves_hard_turn_state_owner_for_fresh_frame_thread() {
        let dir = tempfile::tempdir().unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_seen = seen.clone();
        let upstream_task = tokio::spawn(async move {
            loop {
                let (stream, _) = upstream.accept().await.unwrap();
                let seen = upstream_seen.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock()
                                .unwrap()
                                .push(req.headers()[AUTHORIZATION].to_str().unwrap().to_owned());
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .header(CONTENT_TYPE, "text/event-stream")
                                    .body(Full::new(Bytes::from_static(
                                        b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp\",\"status\":\"completed\"}}\n\n",
                                    )))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        let (proxy_addr, proxy_task, router) = start_two_account_proxy(
            dir.path(),
            format!("http://{upstream_addr}/backend-api/codex"),
        )
        .await;
        let turn_key = router.affinity.key("turn-state:owned-turn");
        assert!(router.bind(turn_key, "a").await);
        let mut exhausted = hyper::HeaderMap::new();
        exhausted.insert(
            "x-codex-primary-used-percent",
            hyper::header::HeaderValue::from_static("100"),
        );
        router.observe_headers("a", &exhausted).await;

        let mut websocket = connect_test_websocket_with_headers(
            proxy_addr,
            &[("x-codex-turn-state", "owned-turn")],
        )
        .await;
        websocket
            .send(Message::Text(
                serde_json::json!({
                    "type":"response.create",
                    "client_metadata":{"thread_id":"brand-new-thread"},
                    "input":[]
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        let event = websocket
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&event).unwrap()["type"],
            "response.completed"
        );
        assert_eq!(*seen.lock().unwrap(), ["Bearer token-a".to_owned()]);
        proxy_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn direct_websocket_mode_tracks_multiplexed_out_of_order_terminals() {
        let dir = tempfile::tempdir().unwrap();
        let (upstream_addr, upstream_task) = spawn_websocket_upstream(true).await;
        let (proxy_addr, proxy_task) = start_caller_proxy(
            dir.path(),
            format!("http://{upstream_addr}/backend-api/codex"),
            ResponsesWebsocketMode::Direct,
        )
        .await;
        let mut websocket = connect_test_websocket(proxy_addr).await;
        for thread in ["thread-a", "thread-b"] {
            websocket
                .send(Message::Text(
                    serde_json::json!({
                        "type":"response.create",
                        "client_metadata":{"thread_id":thread},
                        "input":[]
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
        }
        let mut event_types = Vec::new();
        let mut terminal_ids = Vec::new();
        for _ in 0..4 {
            let message = websocket
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap();
            let event: serde_json::Value = serde_json::from_str(&message).unwrap();
            event_types.push(event["type"].as_str().unwrap().to_owned());
            if event["type"] == "response.completed" {
                terminal_ids.push(event["response"]["id"].as_str().unwrap().to_owned());
            }
        }
        assert_eq!(
            event_types,
            [
                "response.created",
                "response.created",
                "response.completed",
                "response.completed"
            ]
        );
        assert_eq!(terminal_ids, ["resp_b", "resp_a"]);
        proxy_task.abort();
        upstream_task.abort();
    }
}
