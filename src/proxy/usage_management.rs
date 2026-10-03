use super::*;
use crate::config::AccountConfig;
use http_body_util::Limited;
use serde::Deserialize;
use serde_json::{Value, json};

const MANAGEMENT_ROOT: &str = "/v0/management";
const CLAUDE_USAGE_URL: &str = "https://api.anthropic.com/api/oauth/usage";

#[derive(Deserialize)]
struct ApiCall {
    auth_index: String,
    method: String,
    url: String,
    #[serde(default)]
    data: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResetQuota {
    auth_index: String,
}

impl App {
    pub(super) fn usage_management_path(uri: &Uri) -> bool {
        uri.path() == MANAGEMENT_ROOT || uri.path().starts_with("/v0/management/")
    }

    pub(super) async fn handle_usage_management(
        &self,
        request: Request<Incoming>,
        listener: &ListenerConfig,
    ) -> Response<ProxyBody> {
        if !listener.address.ip().is_loopback() {
            return error_response(StatusCode::NOT_FOUND, "not_found", "unknown proxy path");
        }
        let authorized = request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|key| {
                blake3::hash(key.as_bytes())
                    == blake3::hash(self.config.proxy.installation_secret.as_bytes())
            });
        if !authorized {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "management key required",
            );
        }
        if request.uri().query().is_some() {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "unexpected query",
            );
        }
        match request.uri().path() {
            "/v0/management/auth-files" if request.method() == Method::GET => {
                let config = self.config.clone();
                let files = tokio::task::spawn_blocking(move || {
                    config
                        .accounts
                        .iter()
                        .filter_map(|(name, account)| {
                            let provider = managed_provider(account)?;
                            let mut file = json!({
                                "id": name,
                                "auth_index": name,
                                "provider": provider,
                                "disabled": false,
                            });
                            if let Some(metadata) = account_metadata(account) {
                                file.as_object_mut()
                                    .unwrap()
                                    .extend(metadata.as_object().unwrap().clone());
                            }
                            Some(file)
                        })
                        .collect::<Vec<_>>()
                })
                .await;
                match files {
                    Ok(files) => management_json(json!({ "files": files })),
                    Err(_) => error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal_error",
                        "account list could not be built",
                    ),
                }
            }
            "/v0/management/api-call" | "/v0/management/reset-quota"
                if request.method() == Method::POST =>
            {
                let reset_quota = request.uri().path() == "/v0/management/reset-quota";
                // Request headers are never forwarded; each permitted operation constructs
                // its own fixed provider URL and account authentication.
                let body = tokio::time::timeout(
                    HTTP_UPSTREAM_UPLOAD_IDLE_TIMEOUT,
                    Limited::new(request.into_body(), MAX_USAGE_RESPONSE_BYTES).collect(),
                )
                .await;
                let bytes = match body {
                    Ok(Ok(body)) => body.to_bytes(),
                    _ => {
                        return error_response(
                            StatusCode::BAD_REQUEST,
                            "invalid_request",
                            "management request body could not be read within its limits",
                        );
                    }
                };
                if reset_quota {
                    let Ok(call) = serde_json::from_slice::<ResetQuota>(&bytes) else {
                        return error_response(
                            StatusCode::BAD_REQUEST,
                            "invalid_request",
                            "invalid reset acknowledgment",
                        );
                    };
                    return self.acknowledge_credit_reset(&call.auth_index).await;
                }
                let call: ApiCall = match serde_json::from_slice(&bytes) {
                    Ok(call) => call,
                    Err(_) => {
                        return error_response(
                            StatusCode::BAD_REQUEST,
                            "invalid_request",
                            "invalid usage request",
                        );
                    }
                };
                self.management_usage(call).await
            }
            "/v0/management/auth-files"
            | "/v0/management/api-call"
            | "/v0/management/reset-quota" => error_response(
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "unsupported method",
            ),
            _ => error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                "unsupported management endpoint",
            ),
        }
    }

    async fn management_usage(&self, call: ApiCall) -> Response<ProxyBody> {
        let Some(account) = self
            .config
            .accounts
            .get(&call.auth_index)
            .filter(|account| managed_provider(account).is_some())
        else {
            return error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                "unknown managed account",
            );
        };
        let claude = account.is_claude();
        if !claude
            && call.url == reset_credits::CODEX_CREDITS_URL
            && call.method == "GET"
            && call.data.is_none()
        {
            return self.codex_reset_credits(&call.auth_index, None).await;
        }
        if !claude
            && call.url == format!("{}/consume", reset_credits::CODEX_CREDITS_URL)
            && call.method == "POST"
        {
            let Some(credit) = reset_credits::ConsumeCredit::parse(call.data.as_deref()) else {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    "invalid reset credit",
                );
            };
            return self
                .codex_reset_credits(&call.auth_index, Some(credit))
                .await;
        }
        let allowed_usage = if claude {
            call.url == CLAUDE_USAGE_URL
                || call.url == format!("{CLAUDE_USAGE_URL}?cedar_ember=1&skip_spend=1")
        } else {
            call.url == usage::USAGE_URL
        };
        if call.method != "GET" || !allowed_usage || call.data.is_some() {
            return error_response(
                StatusCode::BAD_REQUEST,
                "unsupported_usage_request",
                "unsupported provider usage or reset-credit operation",
            );
        }
        let snapshot = self.router.routing_snapshot().await;
        let Some(state) = snapshot.account_states.get(&call.auth_index) else {
            return usage_unavailable();
        };
        if matches!(
            state.unavailable_reason.as_deref(),
            Some("needs_login" | "login_in_progress" | "access_token_rejected")
        ) {
            return usage_unavailable();
        }
        let mut windows = serde_json::Map::new();
        for (name, field) in if claude {
            &[("5h", "five_hour"), ("7d", "seven_day")][..]
        } else {
            &[
                ("primary", "primary_window"),
                ("secondary", "secondary_window"),
                ("tertiary", "tertiary_window"),
            ][..]
        } {
            let Some(window) = state.usage_windows.get(*name) else {
                continue;
            };
            let Some(percent) = window.used_percent else {
                continue;
            };
            if window.limit_window_seconds == Some(0) {
                continue;
            }
            let value = if claude {
                let reset = window
                    .reset_at_unix
                    .and_then(|timestamp| chrono::DateTime::from_timestamp(timestamp, 0))
                    .map(|date| date.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
                json!({ "utilization": percent, "resets_at": reset })
            } else {
                let mut value = json!({
                    "used_percent": percent,
                    "reset_at": window.reset_at_unix,
                });
                if let Some(seconds) = window.limit_window_seconds {
                    value["limit_window_seconds"] = json!(seconds);
                }
                value
            };
            windows.insert((*field).into(), value);
        }
        if windows.is_empty() {
            return usage_unavailable();
        }
        let body = if claude {
            if let Some(reported) = self.claude_reporting_usage(&call.auth_index).await {
                windows.extend(reported);
            }
            if let Some(credits) = self.claude_reset_credits(&call.auth_index).await {
                windows.insert("cedar_ember".into(), credits);
            }
            Value::Object(windows)
        } else {
            json!({ "rate_limit": windows })
        };
        let mut headers = json!({ "Content-Type": ["application/json"] });
        if let Some(updated) = state.usage_updated_at_unix {
            headers["X-Comradex-Usage-Updated-At"] = json!([updated.to_string()]);
        }
        management_json(json!({
            "status_code": 200,
            "header": headers,
            "body": body.to_string(),
        }))
    }
}

fn managed_provider(account: &AccountConfig) -> Option<&'static str> {
    match account {
        AccountConfig::CodexHome { .. } => Some("codex"),
        AccountConfig::ClaudeHome { .. } => Some("claude"),
        _ => None,
    }
}

pub(super) fn management_json(value: Value) -> Response<ProxyBody> {
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .header("cache-control", "no-store")
        .body(json_body(value))
        .expect("static management response is valid")
}

fn usage_unavailable() -> Response<ProxyBody> {
    management_json(json!({
        "status_code": 503,
        "header": { "Content-Type": ["application/json"] },
        "body": json!({ "error": "account usage is not available yet" }).to_string(),
    }))
}

fn account_metadata(account: &AccountConfig) -> Option<Value> {
    let read_json =
        |path| -> Option<Value> { serde_json::from_slice(&std::fs::read(path).ok()?).ok() };
    let mut metadata = json!({});
    match account {
        AccountConfig::CodexHome { path } => {
            let document = read_json(path.join("auth.json"))?;
            let token = document["tokens"]["id_token"].as_str()?;
            let claims = auth::jwt_payload(token)?;
            let identity = &claims["https://api.openai.com/auth"];
            if let Some(configured) = document["tokens"]["account_id"].as_str()
                && identity["chatgpt_account_id"].as_str() != Some(configured)
            {
                return None;
            }
            if let Some(email) = claims["email"].as_str() {
                metadata["email"] = json!(email);
            }
            let mut id_token = json!({});
            for field in ["chatgpt_account_id", "chatgpt_plan_type"] {
                if let Some(value) = identity[field].as_str() {
                    id_token[field] = json!(value);
                }
            }
            metadata["id_token"] = id_token;
        }
        AccountConfig::ClaudeHome { path } => {
            let credential = crate::claude::auth::read(path).ok()?;
            let profile = read_json(path.join("native-login/.claude.json"))?;
            let identity = &profile["oauthAccount"];
            if identity["accountUuid"].as_str() != Some(&credential.account_uuid)
                || identity["organizationUuid"].as_str() != Some(&credential.organization_uuid)
            {
                return None;
            }
            if let Some(email) = identity["emailAddress"].as_str() {
                metadata["email"] = json!(email);
            }
        }
        _ => return None,
    }
    Some(metadata)
}

pub(super) fn api_response(status: StatusCode, body: Value) -> Response<ProxyBody> {
    management_json(json!({
        "status_code": status.as_u16(),
        "header": { "Content-Type": ["application/json"] },
        "body": body.to_string(),
    }))
}
