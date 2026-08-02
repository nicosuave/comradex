mod headers;
mod replay_body;

use std::{
    convert::Infallible,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use anyhow::{Context, Result};
use http_body_util::BodyExt;
use hyper::{
    Method, Request, Response, StatusCode, Uri,
    body::{Body, Frame, Incoming, SizeHint},
    header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HOST, LOCATION},
    service::service_fn,
};
use hyper_rustls::HttpsConnector;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use tokio::{net::TcpListener, sync::Semaphore};
use tracing::{error, info, warn};

use crate::{
    auth,
    config::{Config, ListenerConfig, PoolConfig},
    routing::{
        Router,
        live::{self, LiveCallStore},
        metadata,
    },
    state::Stats,
};
use replay_body::{ProxyBody, ReplayBody, empty_body, incoming_body, json_body};

type HttpClient = Client<HttpsConnector<HttpConnector>, ProxyBody>;

pub struct App {
    config: Arc<Config>,
    router: Arc<Router>,
    client: HttpClient,
    upgrade_client: HttpClient,
    stats: Arc<Stats>,
    http_slots: Arc<Semaphore>,
    upgrade_slots: Arc<Semaphore>,
    live_calls: LiveCallStore,
    auth: auth::Resolver,
}

impl App {
    pub fn new(config: Arc<Config>, router: Arc<Router>, stats: Arc<Stats>) -> Result<Arc<Self>> {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        let upgrade_https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        let client = Client::builder(TokioExecutor::new()).build(https);
        let upgrade_client = Client::builder(TokioExecutor::new()).build(upgrade_https);
        Ok(Arc::new(Self {
            http_slots: Arc::new(Semaphore::new(config.proxy.max_inflight)),
            upgrade_slots: Arc::new(Semaphore::new(config.proxy.max_upgrades)),
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
            config,
            router,
            client,
            upgrade_client,
            stats,
        }))
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
            tokio::spawn(async move {
                let service = service_fn(move |req| app.clone().handle(req, listener.clone()));
                if let Err(e) = Builder::new(TokioExecutor::new())
                    .serve_connection_with_upgrades(TokioIo::new(stream), service)
                    .await
                {
                    warn!(error = %e, "client connection ended");
                }
            });
        }
    }

    async fn handle(
        self: Arc<Self>,
        req: Request<Incoming>,
        listener: ListenerConfig,
    ) -> Result<Response<ProxyBody>, Infallible> {
        let response = match self.authorized_path(req.uri()) {
            None => error_response(StatusCode::NOT_FOUND, "not_found", "unknown proxy path"),
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
                        return Ok(error_response(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "at_capacity",
                            "HTTP request limit reached",
                        ));
                    }
                };
                self.stats.inflight_http.fetch_add(1, Ordering::Relaxed);
                let result = self
                    .handle_http(req, &listener, path)
                    .await
                    .unwrap_or_else(internal_error);
                self.stats.inflight_http.fetch_sub(1, Ordering::Relaxed);
                drop(permit);
                result
            }
        };
        Ok(response)
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
    ) -> Result<Response<ProxyBody>> {
        let (parts, body) = req.into_parts();
        let inbound_headers = parts.headers;
        let method = parts.method;
        let mut replay = ReplayBody::read(
            body,
            self.config.proxy.replay_memory_bytes,
            self.config.proxy.max_request_bytes,
            self.config.proxy.max_spool_bytes,
            self.stats.clone(),
        )
        .await?;
        let pool = self.pool(listener)?;
        let affinity_values = metadata::affinity_values(
            &inbound_headers,
            replay.thread_id(),
            replay.previous_response_id(),
            replay.prompt_cache_key(),
        );
        let affinity_keys: Vec<_> = affinity_values
            .iter()
            .map(|value| (value.kind, self.router.affinity.key(&value.namespaced())))
            .collect();
        let mut bound_account: Option<String> = None;
        let mut hard_owner = false;
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
                    "request continuity keys resolve to different accounts",
                ));
            }
            hard_owner |= kind.is_hard_continuity();
            bound_account = Some(binding.account_id);
        }
        if bound_account.is_none()
            && affinity_values.iter().any(|value| {
                matches!(
                    value.kind,
                    metadata::AffinityKind::PreviousResponse | metadata::AffinityKind::TurnState
                )
            })
        {
            return Ok(error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "continuity_owner_unavailable",
                "request carries hard continuity state with no known account owner",
            ));
        }
        let primary_key = affinity_keys.first().map(|(_, key)| key.clone());
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
                .select(&listener.pool, pool, primary_key.clone(), None)
                .await
                .context("no eligible account")?
        };
        for (_, key) in &affinity_keys {
            self.router.bind(key.clone(), &first.account_id).await;
        }
        let mut selected = first;
        for attempt in 0..2 {
            let account = selected.account_id.clone();
            let credentials = self
                .auth
                .resolve(&self.config.accounts[&account], &inbound_headers)
                .await?;
            self.router.begin(&account).await;
            let result = self
                .send_http(
                    &method,
                    &path,
                    &inbound_headers,
                    credentials.clone(),
                    replay.body(attempt)?,
                )
                .await;
            match result {
                Ok(response) => {
                    self.router
                        .observe_headers(&account, response.headers())
                        .await;
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
                        self.router.bind(alias, &account).await;
                    }
                    let status = response.status();
                    if status == StatusCode::UNAUTHORIZED {
                        if attempt == 0 {
                            match self
                                .auth
                                .force_refresh(&self.config.accounts[&account], &credentials)
                                .await
                            {
                                Ok(Some(_)) => {
                                    self.router.end(&account).await;
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
                        return Ok(map_http_response_leased(
                            response,
                            self.router.clone(),
                            account,
                        ));
                    }
                    if status == StatusCode::FORBIDDEN {
                        return Ok(map_http_response_leased(
                            response,
                            self.router.clone(),
                            account,
                        ));
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
                            self.router.end(&account).await;
                            return Ok(error_response(
                                StatusCode::SERVICE_UNAVAILABLE,
                                "realtime_call_binding_failed",
                                "successful realtime call could not be bound safely",
                            ));
                        }
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
                    if retry && attempt == 0 && !hard_owner {
                        self.router.end(&account).await;
                        selected = self
                            .router
                            .select(&listener.pool, pool, primary_key.clone(), Some(&account))
                            .await
                            .context("no alternate account")?;
                        for (_, key) in &affinity_keys {
                            self.router.bind(key.clone(), &selected.account_id).await;
                        }
                        continue;
                    }
                    return Ok(map_http_response_leased(
                        response,
                        self.router.clone(),
                        account,
                    ));
                }
                Err(e) => {
                    self.router.end(&account).await;
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
                            .select(&listener.pool, pool, primary_key.clone(), Some(&account))
                            .await
                            .context("no alternate account")?;
                        for (_, key) in &affinity_keys {
                            self.router.bind(key.clone(), &selected.account_id).await;
                        }
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
    ) -> Result<Response<Incoming>> {
        let uri = self.upstream_uri(path, false)?;
        let mut builder = Request::builder().method(method).uri(uri);
        *builder.headers_mut().expect("builder") = inbound.clone();
        let headers = builder.headers_mut().expect("builder");
        headers::strip_hop_by_hop(headers);
        headers.remove(CONTENT_LENGTH);
        let _ = inbound;
        apply_credentials(headers, credentials)?;
        Ok(self.client.request(builder.body(body)?).await?)
    }

    async fn handle_upgrade(
        &self,
        mut req: Request<Incoming>,
        listener: &ListenerConfig,
        path: String,
        live_call_id: Option<String>,
    ) -> Result<Response<ProxyBody>> {
        let permit = self
            .upgrade_slots
            .clone()
            .try_acquire_owned()
            .context("WebSocket limit reached")?;
        let inbound_headers = req.headers().clone();
        let pool = self.pool(listener)?;
        let forced_live = live_call_id.is_some();
        let affinity_values = metadata::affinity_values(&inbound_headers, None, None, None);
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
                hard_owner |= kind.is_hard_continuity();
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
        } else if let Some(account) = &bound_account {
            let Some(selection) = self.router.select_exact(pool, account).await else {
                return Ok(error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "continuity_owner_unavailable",
                    "required websocket continuity account is unavailable",
                ));
            };
            selection
        } else {
            self.router
                .select(&listener.pool, pool, key.clone(), None)
                .await
                .context("no eligible account")?
        };
        for (_, alias) in &affinity_keys {
            self.router.bind(alias.clone(), &selection.account_id).await;
        }
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
            let mut response = match self.upgrade_client.request(upstream_req).await {
                Ok(response) => response,
                Err(error) => {
                    self.router.end(&selection.account_id).await;
                    return Err(error.into());
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
                stats.open_upgrades.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let _permit = permit;
                    match tokio::try_join!(client_upgrade, upstream_upgrade) {
                        Ok((client, upstream)) => {
                            let _ = tokio::io::copy_bidirectional(
                                &mut TokioIo::new(client),
                                &mut TokioIo::new(upstream),
                            )
                            .await;
                        }
                        Err(e) => warn!(error = %e, "upgrade failed"),
                    }
                    stats.open_upgrades.fetch_sub(1, Ordering::Relaxed);
                    router.end(&account).await;
                });
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
                    Ok(None) => {}
                    Err(error) => {
                        warn!(account = selection.account_id, %error, "websocket credential refresh failed")
                    }
                }
            }
            let retry = is_quota_status(response.status())
                || is_selected_gateway_failure(response.status());
            if retry && !forced_live && !hard_owner && attempt == 0 {
                if matches!(
                    response.status(),
                    StatusCode::TOO_MANY_REQUESTS | StatusCode::PAYMENT_REQUIRED
                ) {
                    self.router
                        .quota_failure(&selection.account_id, response.headers())
                        .await;
                } else {
                    self.router.soft_failure(&selection.account_id).await;
                }
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

fn is_connect_failure(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<hyper_util::client::legacy::Error>()
        .is_some_and(hyper_util::client::legacy::Error::is_connect)
}

fn map_http_response(response: Response<Incoming>) -> Response<ProxyBody> {
    let (mut parts, body) = response.into_parts();
    headers::strip_hop_by_hop(&mut parts.headers);
    Response::from_parts(parts, incoming_body(body))
}

fn map_http_response_leased(
    response: Response<Incoming>,
    router: Arc<Router>,
    account: String,
) -> Response<ProxyBody> {
    let (mut parts, body) = response.into_parts();
    headers::strip_hop_by_hop(&mut parts.headers);
    Response::from_parts(
        parts,
        BodyExt::boxed(LeasedIncoming {
            inner: body,
            router,
            account: Some(account),
        }),
    )
}

struct LeasedIncoming {
    inner: Incoming,
    router: Arc<Router>,
    account: Option<String>,
}

impl Body for LeasedIncoming {
    type Data = bytes::Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.inner)
            .poll_frame(cx)
            .map(|frame| frame.map(|result| result.map_err(std::io::Error::other)))
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
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

fn internal_error(error: anyhow::Error) -> Response<ProxyBody> {
    error!(error = %error, "request failed");
    error_response(StatusCode::BAD_GATEWAY, "proxy_error", &error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{AccountConfig, ProxyConfig},
        routing::AffinityStore,
    };
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper_util::client::legacy::{Client as TestClient, connect::HttpConnector};
    use std::{collections::BTreeMap, fs, path::PathBuf, sync::Mutex, time::Duration};

    #[derive(Clone, Debug)]
    struct Seen {
        path: String,
        authorization: String,
        body: Bytes,
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

    #[tokio::test]
    async fn replays_exact_body_once_and_commits_alternate_affinity() {
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
                                    .body(Full::new(Bytes::from_static(b"data: ok\n\n")))
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
        let app = App::new(config, router, Arc::new(Stats::default())).unwrap();
        let proxy_task = tokio::spawn(app.serve_tcp("default".into(), listener, proxy_tcp));

        let client: TestClient<HttpConnector, Full<Bytes>> =
            TestClient::builder(TokioExecutor::new()).build(HttpConnector::new());
        let payload = Bytes::from_static(
            br#"{"client_metadata":{"thread_id":"same-thread"},"input":"byte-identical"}"#,
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
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                "data: ok\n\n"
            );
        }

        let calls = seen.lock().unwrap().clone();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].authorization, "Bearer token-a");
        assert_eq!(calls[1].authorization, "Bearer token-b");
        assert_eq!(calls[2].authorization, "Bearer token-b");
        assert!(calls.iter().all(|v| v.body == payload));
        assert!(
            calls
                .iter()
                .all(|v| v.path == "/backend-api/codex/responses/compact?test=1")
        );

        proxy_task.abort();
        upstream_task.abort();
    }
}
