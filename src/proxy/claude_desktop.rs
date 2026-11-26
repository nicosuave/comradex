//! Claude Desktop's third-party Messages endpoint and local model discovery.
use super::*;
use crate::config::ClaudeDesktopConfig;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use http_body_util::{LengthLimitError, Limited};
use hyper::HeaderMap;
use serde::Serialize;
use serde_json::{Value, json};
use std::net::SocketAddr;

mod acp;
mod codex;
#[cfg(test)]
mod codex_tests;

const MLX_PREFIX: &str = "claude-sonnet-mlx-";
const MAX_MODEL_ID: usize = 512;
const MAX_CATALOG_BYTES: usize = 4 * 1024 * 1024;

impl App {
    pub async fn run_claude_desktop_listener(
        self: Arc<Self>,
        config: ClaudeDesktopConfig,
    ) -> Result<()> {
        let tcp = TcpListener::bind(config.address)
            .await
            .context("bind Claude Desktop listener")?;
        self.serve_claude_desktop_tcp(config, tcp).await
    }

    async fn serve_claude_desktop_tcp(
        self: Arc<Self>,
        config: ClaudeDesktopConfig,
        tcp: TcpListener,
    ) -> Result<()> {
        info!(address = %tcp.local_addr()?, "listening for Claude Desktop");
        let config = Arc::new(config);
        let connections = Arc::new(Semaphore::new(self.config.proxy.max_inflight));
        loop {
            let (stream, peer) = tcp.accept().await?;
            if !peer.ip().is_loopback() {
                continue;
            }
            let Ok(connection) = connections.clone().try_acquire_owned() else {
                continue;
            };
            let app = self.clone();
            let config = config.clone();
            self.spawn_tracked(async move {
                let _connection = connection;
                let service = service_fn(move |request| {
                    let app = app.clone();
                    let config = config.clone();
                    async move {
                        Ok::<_, Infallible>(app.claude_desktop_request(request, &config).await)
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .timer(hyper_util::rt::TokioTimer::new())
                    .header_read_timeout(DOWNSTREAM_HEADER_READ_TIMEOUT)
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            })
            .await;
        }
    }

    async fn claude_desktop_request(
        &self,
        request: Request<Incoming>,
        config: &ClaudeDesktopConfig,
    ) -> Response<ProxyBody> {
        if request.method() == Method::GET && self.service_health_path(request.uri()) {
            return self.health_response();
        }
        if !authenticated(request.headers(), &config.api_key) {
            return messages_error(
                StatusCode::UNAUTHORIZED,
                "authentication_error",
                "invalid gateway key",
            );
        }
        let path = request.uri().path();
        if request.method() == Method::GET && path == "/v1/models" {
            return self.claude_desktop_catalog(config).await;
        }
        if request.method() != Method::POST || path != "/v1/messages" {
            return messages_error(
                StatusCode::NOT_FOUND,
                "not_found_error",
                "unknown Messages endpoint",
            );
        }

        let (parts, incoming) = request.into_parts();
        let body = match collect_limited(incoming, self.config.proxy.max_request_bytes).await {
            Ok(value) => value,
            Err(status) => {
                return messages_error(
                    status,
                    "invalid_request_error",
                    "could not read request body",
                );
            }
        };
        let Ok(mut message) = serde_json::from_slice::<Value>(&body) else {
            return messages_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "expected a JSON Messages request",
            );
        };
        let Some(model) = message["model"].as_str() else {
            return messages_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "model is required",
            );
        };
        if !message["safeguards"].is_null() {
            return messages_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "provider-side Auto mode review is unavailable",
            );
        }
        if let Some(agent) = config.acp.get(model) {
            return self.forward_acp_messages(agent, model, &message).await;
        }
        if let Some(upstream) = config.mlx_serve
            && let Some(upstream_id) = mlx_model_id(model)
        {
            if let Some(error) = self.mlx_model_error(upstream, model).await {
                return error;
            }
            let body = message
                .as_object_mut()
                .expect("Messages request has a model");
            body.remove("model");
            let path = parts.uri.path_and_query().unwrap();
            return self
                .forward_mlx(
                    upstream,
                    path.as_str(),
                    parts.headers,
                    &upstream_id,
                    body,
                    config.mlx_response_timeout(),
                )
                .await
                .unwrap_or_else(|_| {
                    messages_error(
                        StatusCode::BAD_GATEWAY,
                        "api_error",
                        "MLX-Serve request failed",
                    )
                });
        }
        if let Some(codex) = config.codex.as_ref()
            && let Some(upstream_id) = codex.models.get(model)
        {
            let listener = ListenerConfig {
                address: config.address,
                pool: codex.pool.clone(),
            };
            return self
                .forward_codex_messages(&listener, model, upstream_id, &parts.headers, &message)
                .await;
        }
        messages_error(
            StatusCode::NOT_FOUND,
            "not_found_error",
            "model is not configured",
        )
    }

    async fn claude_desktop_catalog(&self, config: &ClaudeDesktopConfig) -> Response<ProxyBody> {
        let mut models = Vec::new();
        if let Some(upstream) = config.mlx_serve {
            match self.mlx_models(upstream).await {
                Ok(local) => models = local,
                Err(_) if config.codex.is_some() || !config.acp.is_empty() => (),
                Err(_) => {
                    return messages_error(
                        StatusCode::BAD_GATEWAY,
                        "api_error",
                        "could not load MLX-Serve models",
                    );
                }
            }
        }
        if let Some(codex) = &config.codex {
            models.extend(codex.models.iter().map(|(alias, upstream)| {
                json!({
                    "type": "model",
                    "id": alias,
                    "display_name": format!("{upstream} (ChatGPT subscription)"),
                })
            }));
        }
        models.extend(config.acp.iter().map(|(alias, agent)| {
            json!({
                "type": "model",
                "id": alias,
                "display_name": agent.display_name,
                "description": "Local ACP agent",
            })
        }));
        Response::builder()
            .header(CONTENT_TYPE, "application/json")
            .body(json_body(json!({
                "data": models,
                "has_more": false,
                "first_id": models.first().map(|model| &model["id"]),
                "last_id": models.last().map(|model| &model["id"]),
            })))
            .expect("static model catalog response")
    }

    async fn mlx_models(&self, upstream: SocketAddr) -> Result<Vec<Value>> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let request = Request::get(format!("http://{upstream}/v1/models")).body(empty_body())?;
            let response = self.client.request(request).await?;
            anyhow::ensure!(
                response.status().is_success(),
                "MLX-Serve catalog returned an error"
            );
            let bytes = Limited::new(response.into_body(), MAX_CATALOG_BYTES)
                .collect()
                .await
                .map_err(|error| anyhow::anyhow!("read MLX-Serve catalog: {error}"))?
                .to_bytes();
            let list: Value = serde_json::from_slice(&bytes)?;
            Ok(list["data"]
                .as_array()
                .context("missing model list")?
                .iter()
                .filter_map(mlx_catalog_model)
                .collect())
        })
        .await?
    }

    async fn mlx_model_error(
        &self,
        upstream: SocketAddr,
        alias: &str,
    ) -> Option<Response<ProxyBody>> {
        match self.mlx_models(upstream).await {
            Ok(models) if models.iter().any(|entry| entry["id"] == alias) => None,
            Ok(_) => Some(messages_error(
                StatusCode::NOT_FOUND,
                "not_found_error",
                "MLX-Serve model is unavailable",
            )),
            Err(_) => Some(messages_error(
                StatusCode::BAD_GATEWAY,
                "api_error",
                "could not load MLX-Serve models",
            )),
        }
    }

    async fn forward_mlx(
        &self,
        upstream: SocketAddr,
        path: &str,
        inbound: HeaderMap,
        model: &str,
        message: &serde_json::Map<String, Value>,
        response_timeout: Duration,
    ) -> Result<Response<ProxyBody>> {
        // MLX-Serve scans the raw JSON for model selection. Put the top-level
        // model first so a nested field in a tool schema cannot select it.
        #[derive(Serialize)]
        struct MlxMessage<'a> {
            model: &'a str,
            #[serde(flatten)]
            content: &'a serde_json::Map<String, Value>,
        }
        let body = serde_json::to_vec(&MlxMessage {
            model,
            content: message,
        })?;
        let mut request = Request::post(format!("http://{upstream}{path}"))
            .header(CONTENT_TYPE, "application/json")
            .body(bytes_body(body.into()))?;
        for name in ["anthropic-version", "anthropic-beta", "accept"] {
            if let Some(value) = inbound.get(name) {
                request.headers_mut().insert(name, value.clone());
            }
        }
        let response =
            match tokio::time::timeout(response_timeout, self.client.request(request)).await {
                Ok(response) => response?,
                Err(_) => {
                    return Ok(messages_error(
                        StatusCode::GATEWAY_TIMEOUT,
                        "api_error",
                        "MLX-Serve timed out waiting for a response",
                    ));
                }
            };
        // MLX-Serve can fall back to its default when a model disappears,
        // then echo the requested ID in its output. Recheck the registry
        // before exposing bytes from a potentially substituted model.
        if response.status().is_success()
            && let Some(error) = self.mlx_model_error(upstream, &mlx_alias(model)).await
        {
            return Ok(error);
        }
        let (mut parts, body) = response.into_parts();
        headers::strip_hop_by_hop(&mut parts.headers);
        Ok(Response::from_parts(parts, incoming_body(body)))
    }
}

fn authenticated(headers: &HeaderMap, expected: &str) -> bool {
    let key = headers
        .get("x-api-key")
        .map(|value| value.as_bytes())
        .or_else(|| {
            headers
                .get(AUTHORIZATION)?
                .as_bytes()
                .strip_prefix(b"Bearer ")
        });
    key.is_some_and(|key| blake3::hash(key) == blake3::hash(expected.as_bytes()))
}

async fn collect_limited(body: Incoming, limit: usize) -> Result<Bytes, StatusCode> {
    tokio::time::timeout(
        HTTP_UPSTREAM_UPLOAD_IDLE_TIMEOUT,
        Limited::new(body, limit).collect(),
    )
    .await
    .map_err(|_| StatusCode::REQUEST_TIMEOUT)?
    .map(|body| body.to_bytes())
    .map_err(|error| {
        if error.is::<LengthLimitError>() {
            StatusCode::PAYLOAD_TOO_LARGE
        } else {
            StatusCode::BAD_REQUEST
        }
    })
}

fn messages_error(status: StatusCode, kind: &str, message: &str) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(json_body(error_event(kind, message)))
        .expect("static Messages error response")
}

fn error_event(kind: &str, message: &str) -> Value {
    json!({"type":"error","error":{"type":kind,"message":message}})
}

fn emit(out: &mut String, value: Value) {
    use std::fmt::Write;
    writeln!(
        out,
        "event: {}\ndata: {value}\n",
        value["type"].as_str().unwrap()
    )
    .expect("write to a string");
}

fn mlx_catalog_model(model: &Value) -> Option<Value> {
    // The loopback server also lists models proxied to cloud providers and LAN peers.
    if model["state"] == "remote" || !model["provider"].is_null() {
        return None;
    }
    if let Some(capabilities) = model["meta"]["capabilities"]
        .as_array()
        .or_else(|| model["capabilities"].as_array())
        && !capabilities.iter().any(|capability| capability == "chat")
    {
        return None;
    }
    let id = model["id"].as_str().filter(|id| valid_mlx_id(id))?;
    let context = model["meta"]["context_length"]
        .as_u64()
        .or_else(|| model["context_length"].as_u64())
        .or_else(|| model["max_model_len"].as_u64());
    let mut entry = json!({
        "type":"model",
        "id":mlx_alias(id),
        "display_name":format!("{id} (MLX-Serve)"),
        "anthropic_family_tier":"sonnet",
    });
    if let Some(context) = context {
        entry["max_input_tokens"] = json!(context);
        entry["supports_1m"] = json!(context >= 1_000_000);
    }
    Some(entry)
}

fn mlx_alias(id: &str) -> String {
    format!("{MLX_PREFIX}{}", URL_SAFE_NO_PAD.encode(id))
}

fn mlx_model_id(alias: &str) -> Option<String> {
    let encoded = alias.strip_prefix(MLX_PREFIX)?;
    if encoded.is_empty() || encoded.len() > MAX_MODEL_ID.div_ceil(3) * 4 {
        return None;
    }
    let id = String::from_utf8(URL_SAFE_NO_PAD.decode(encoded).ok()?).ok()?;
    valid_mlx_id(&id).then_some(id)
}

fn valid_mlx_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_MODEL_ID
        && id != "mlx-serve"
        && !id.starts_with('/')
        && !id.contains(['@', '"', '\\'])
        && !id.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "fixture-key-for-claude-desktop-only";

    #[tokio::test]
    async fn mlx_catalog_and_streaming_tool_call_use_the_native_api() {
        let ml_server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ml_address = ml_server.local_addr().unwrap();
        let upstream = tokio::spawn(async move {
            let removed = Arc::new(AtomicBool::new(false));
            loop {
                let (stream, _) = ml_server.accept().await.unwrap();
                let removed = removed.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let removed = removed.clone();
                        async move {
                            assert!(!request.headers().contains_key("x-api-key"));
                            assert!(!request.headers().contains_key(AUTHORIZATION));
                            let response = if request.uri().path() == "/v1/models" {
                                let mut data = json!([
                                    {"id":"owner/local-model","meta":{"context_length":1048576,"capabilities":["chat","tool_use"]}},
                                    {"id":"embed","meta":{"capabilities":["embeddings"]}},
                                    {"id":"gpt-model@cloud","provider":"cloud","state":"remote","capabilities":["chat"]}
                                ]);
                                if removed.load(Ordering::Relaxed) {
                                    data[0]["id"] = json!("owner/new-default");
                                }
                                Response::new(json_body(json!({"data": data})))
                            } else {
                                assert_eq!(
                                    request.uri().path_and_query().unwrap().as_str(),
                                    "/v1/messages?beta=true"
                                );
                                assert_eq!(
                                    request.headers()["anthropic-beta"],
                                    "context-1m-2025-08-07"
                                );
                                let raw = request.into_body().collect().await.unwrap().to_bytes();
                                assert!(raw.starts_with(b"{\"model\":\"owner/local-model\","));
                                let body: Value = serde_json::from_slice(&raw).unwrap();
                                assert_eq!(body["model"], "owner/local-model");
                                assert_eq!(body["tools"][0]["name"], "inspect");
                                assert_eq!(body["stream"], true);
                                match body["metadata"]["fixture"].as_str() {
                                    Some("slow") => {
                                        tokio::time::sleep(Duration::from_millis(1200)).await
                                    }
                                    Some("catalog_change") => {
                                        removed.store(true, Ordering::Relaxed)
                                    }
                                    _ => (),
                                }
                                // Some MLX-Serve versions echo the request's model
                                // name even when a different model answered.
                                Response::builder().header(CONTENT_TYPE, "text/event-stream")
                                .body(bytes_body(Bytes::from_static(
                                    b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"owner/local-model\"}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_a\",\"name\":\"inspect\",\"input\":{}}}\n\n"
                                ))).unwrap()
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
        let directory = tempfile::tempdir().unwrap();
        let (app, _, _, _) = super::super::tests::direct_test_app_with_upstream(
            directory.path(),
            crate::config::CANONICAL_UPSTREAM.into(),
        );
        let gateway = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = gateway.local_addr().unwrap();
        let listener = tokio::spawn(app.clone().serve_claude_desktop_tcp(
            ClaudeDesktopConfig {
                address,
                api_key: KEY.into(),
                mlx_serve: Some(ml_address),
                mlx_response_timeout_seconds: std::num::NonZeroU32::new(1),
                codex: None,
                acp: Default::default(),
            },
            gateway,
        ));
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let url = format!("http://{address}/v1/models");
        assert_eq!(
            client.get(&url).send().await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let catalog: Value = serde_json::from_slice(
            &client
                .get(url)
                .bearer_auth(KEY)
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(catalog["data"].as_array().unwrap().len(), 1);
        assert_eq!(catalog["data"][0]["supports_1m"], true);
        let unknown = format!("{MLX_PREFIX}{}", URL_SAFE_NO_PAD.encode("owner/removed"));
        assert_eq!(
            client
                .post(format!("http://{address}/v1/messages"))
                .header("x-api-key", KEY)
                .body(json!({"model": unknown, "messages": []}).to_string())
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        let send = |case| {
            client
                .post(format!("http://{address}/v1/messages?beta=true"))
                .header("x-api-key", KEY)
                .header("anthropic-beta", "context-1m-2025-08-07")
                .body(
                    json!({"model": catalog["data"][0]["id"], "stream": true,
                    "tools": [{"name":"inspect"}], "metadata":{"fixture":case}})
                    .to_string(),
                )
                .send()
        };
        let response = send("normal").await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], "text/event-stream");
        assert!(
            response
                .text()
                .await
                .unwrap()
                .contains("\"name\":\"inspect\"")
        );
        assert_eq!(
            send("slow").await.unwrap().status(),
            StatusCode::GATEWAY_TIMEOUT
        );
        let missing = send("catalog_change").await.unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert!(
            !missing
                .text()
                .await
                .unwrap()
                .contains("content_block_start")
        );
        listener.abort();
        upstream.abort();
        app.shutdown_connections().await;
    }
}
