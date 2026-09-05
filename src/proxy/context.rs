//! Account-local native context tools. Their state outlives inference routing affinity.
use super::context_store::ContextAccount;
use super::*;
use futures_util::TryStreamExt;
use serde_json::Value;

const MAX_CONTEXT_BYTES: usize = 2_000_000;

pub(super) fn context_route(path: &str) -> Option<bool> {
    let path = path.split('?').next()?.trim_end_matches('/');
    match path {
        "/alpha/history/v2/list_windows"
        | "/alpha/history/v2/list_items"
        | "/alpha/history/v2/read_item"
        | "/alpha/history/v2/search_contents" => Some(true),
        "/alpha/notes/v2/thread_hint"
        | "/alpha/notes/v2/list_files_by_prefix"
        | "/alpha/notes/v2/read_file"
        | "/alpha/notes/v2/search_contents"
        | "/alpha/notes/v2/append_to_file"
        | "/alpha/notes/v2/write_file" => Some(false),
        _ => None,
    }
}

fn session_id(value: &Value) -> Option<&str> {
    value.as_str().filter(|s| {
        s.len() == 36
            && s.bytes().enumerate().all(|(i, b)| {
                if matches!(i, 8 | 13 | 18 | 23) {
                    b == b'-'
                } else {
                    b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
                }
            })
    })
}

pub(super) fn inference_session(body: &Value) -> Option<&str> {
    session_id(&body["client_metadata"]["session_id"])
}

impl App {
    pub(super) async fn context_routing_view(&self, body: &Value, scope: &str) -> Result<Value> {
        let session = inference_session(body).unwrap_or("");
        let sources = self.context_codec.source_partitions(body, scope, session)?;
        if !sources.is_empty() {
            let stored = self
                .context_store
                .lookup(scope, session)
                .await?
                .context("context source unavailable")?;
            let pool = self
                .config
                .pools
                .get(scope)
                .context("context source unavailable")?;
            for source in sources {
                let account = stored
                    .participants
                    .get(source - 1)
                    .context("context source unavailable")?;
                anyhow::ensure!(
                    pool.members.contains(&account.alias),
                    "context source unavailable"
                );
            }
        }
        self.context_codec.routing_view(body, scope, session)
    }

    pub(super) fn expand_context(&self, body: &mut Value, scope: &str) -> Result<bool> {
        let session = inference_session(body).unwrap_or("").to_owned();
        self.context_codec.expand(body, scope, &session)
    }

    pub(super) async fn record_context_dispatch(
        &self,
        body: &Value,
        scope: &str,
        account: &str,
        credentials: &auth::Credentials,
    ) -> Result<()> {
        let Some(session) = inference_session(body) else {
            anyhow::ensure!(
                body["reasoning"]["context"] != "all_turns",
                "context session unavailable"
            );
            return Ok(());
        };
        if body["reasoning"]["context"] != "all_turns"
            && self.context_store.lookup(scope, session).await?.is_none()
        {
            return Ok(());
        }
        let physical = credentials.context_identity()?;
        self.context_store
            .record_dispatch(scope, session, account, &physical)
            .await
    }

    async fn context_credentials(
        &self,
        owner: &ContextAccount,
        listener: &ListenerConfig,
        headers: &hyper::HeaderMap,
    ) -> Result<auth::Credentials> {
        // Inference quota is independent of notes/history availability. Login and pool membership
        // still apply, and a re-login of the same alias must never silently change the owner.
        anyhow::ensure!(
            self.router
                .context_account_available(self.pool(listener)?, &owner.alias)
                .await,
            "context owner unavailable"
        );
        let config = self
            .config
            .accounts
            .get(&owner.alias)
            .context("context owner unavailable")?;
        let credentials = self.auth.resolve(config, headers).await?;
        let physical = credentials.context_identity()?;
        anyhow::ensure!(
            self.context_store.physical_key(&physical) == owner.physical_id,
            "context owner unavailable"
        );
        Ok(credentials)
    }

    async fn send_context_request(
        &self,
        owner: &ContextAccount,
        listener: &ListenerConfig,
        path: &str,
        headers: &hyper::HeaderMap,
        bytes: bytes::Bytes,
    ) -> Result<Response<Incoming>> {
        let mut credentials = self.context_credentials(owner, listener, headers).await?;
        for attempt in 0..2 {
            let response = self
                .send_http(
                    &Method::POST,
                    path,
                    headers,
                    credentials.clone(),
                    bytes_body(bytes.clone()),
                )
                .await?;
            if response.status() == StatusCode::UNAUTHORIZED
                && attempt == 0
                && let Some(refreshed) = self
                    .auth
                    .force_refresh(&self.config.accounts[&owner.alias], &credentials)
                    .await?
            {
                anyhow::ensure!(
                    self.context_store
                        .physical_key(&refreshed.context_identity()?)
                        == owner.physical_id,
                    "context owner unavailable"
                );
                credentials = refreshed;
                continue;
            }
            return Ok(response);
        }
        unreachable!()
    }

    pub(super) async fn handle_context(
        &self,
        method: Method,
        path: &str,
        headers: &hyper::HeaderMap,
        listener: &ListenerConfig,
        replay: ReplayBody,
    ) -> Result<Response<ProxyBody>> {
        let Some(history) = context_route(path) else {
            return Ok(error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                "unknown context operation",
            ));
        };
        if method != Method::POST {
            return Ok(error_response(
                StatusCode::METHOD_NOT_ALLOWED,
                "invalid_request_error",
                "context operations require POST",
            ));
        }
        let bytes = replay.into_bytes().await?;
        if bytes.len() > MAX_CONTEXT_BYTES {
            return Ok(error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "invalid_request_error",
                "context request exceeds limit",
            ));
        }
        let body: Value = match serde_json::from_slice(&bytes) {
            Ok(body) => body,
            Err(_) => {
                return Ok(error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    "invalid context request",
                ));
            }
        };
        let Some(session) = session_id(&body["context"]["session_id"]) else {
            return Ok(error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "context.session_id is required",
            ));
        };
        if !body["context"]["current_agent_name"]
            .as_str()
            .is_some_and(|name| {
                name.len() <= 1024
                    && (name == "/root" || name.starts_with("/root/"))
                    && name.split('/').skip(1).all(|part| {
                        !part.is_empty()
                            && part != "."
                            && part != ".."
                            && !part.chars().any(char::is_control)
                    })
            })
        {
            return Ok(error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "invalid current agent name",
            ));
        }
        let result = self
            .relay_context(path, headers, listener, session, bytes, history)
            .await;
        // Do not echo upstream errors: encrypted tool arguments and private context can occur in them.
        Ok(result.unwrap_or_else(|_| {
            error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "context_backend_unavailable",
                "required context account is unavailable",
            )
        }))
    }

    async fn relay_context(
        &self,
        path: &str,
        headers: &hyper::HeaderMap,
        listener: &ListenerConfig,
        session: &str,
        bytes: bytes::Bytes,
        history: bool,
    ) -> Result<Response<ProxyBody>> {
        let _permit = self
            .http_slots
            .clone()
            .try_acquire_owned()
            .context("context capacity reached")?;
        let _inflight = InflightGuard::new(&self.stats.inflight_http);
        let scope = &listener.pool;
        if self.context_store.lookup(scope, session).await?.is_none() {
            let pool = self.pool(listener)?;
            let mut selected = None;
            for account in pool.preferred.iter().chain(pool.members.iter()) {
                if self.router.context_account_available(pool, account).await {
                    selected = Some(account);
                    break;
                }
            }
            let selected = selected.context("context owner unavailable")?;
            let credentials = self
                .auth
                .resolve(&self.config.accounts[selected], headers)
                .await?;
            let physical = credentials.context_identity()?;
            self.context_store
                .record_dispatch(scope, session, selected, &physical)
                .await?;
        }
        let stored = self
            .context_store
            .lookup(scope, session)
            .await?
            .context("context owner unavailable")?;
        let accounts = if history {
            stored.participants
        } else {
            vec![stored.owner]
        };
        anyhow::ensure!(
            !accounts.is_empty() && accounts.len() <= 32,
            "context participants unavailable"
        );
        // Bounded fanout. No retry on timeouts, quota, or ambiguous writes, and no model health updates.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let results: Vec<Value> = futures_util::stream::iter(accounts.into_iter().map(|account| {
            let bytes = bytes.clone();
            async move {
                let response = tokio::time::timeout_at(deadline, async {
                    let response = self
                        .send_context_request(&account, listener, path, headers, bytes)
                        .await?;
                    anyhow::ensure!(
                        response.status().is_success(),
                        "context backend unavailable"
                    );
                    let mut body = response.into_body();
                    let mut bytes = bytes::BytesMut::new();
                    while let Some(frame) = body.frame().await {
                        let frame = frame?;
                        if let Some(data) = frame.data_ref() {
                            anyhow::ensure!(
                                bytes.len().saturating_add(data.len()) <= MAX_CONTEXT_BYTES,
                                "context result exceeds limit"
                            );
                            bytes.extend_from_slice(data);
                        }
                    }
                    Ok::<Value, anyhow::Error>(serde_json::from_slice(&bytes)?)
                })
                .await??;
                Ok::<Value, anyhow::Error>(response)
            }
        }))
        .buffered(4)
        .try_collect()
        .await?;
        anyhow::ensure!(!results.is_empty(), "context backend unavailable");
        let value = if path
            .split('?')
            .next()
            .unwrap_or(path)
            .trim_end_matches('/')
            .ends_with("/thread_hint")
        {
            results
                .into_iter()
                .next()
                .context("context hint unavailable")?
        } else {
            self.context_codec.pack(scope, session, results, history)?
        };
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(json_body(value))?)
    }
}
