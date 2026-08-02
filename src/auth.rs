use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use chrono::Utc;
use http_body_util::{BodyExt, Full};
use hyper::{HeaderMap, Request, StatusCode, header::CONTENT_TYPE};
use hyper_rustls::HttpsConnector;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::config::{AccountConfig, Config};

const REFRESH_URL: &str = "https://auth.openai.com/oauth/token";
const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const REFRESH_WINDOW_SECONDS: u64 = 5 * 60;
const MAX_REFRESH_RESPONSE_BYTES: usize = 1024 * 1024;

type AuthClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

#[derive(Debug, Clone)]
pub struct Credentials {
    pub authorization: String,
    pub account_id: Option<String>,
}

pub struct Resolver {
    client: AuthClient,
    locks: HashMap<PathBuf, Arc<Mutex<()>>>,
}

impl Resolver {
    pub fn new(config: &Config) -> Self {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_only()
            .enable_http1()
            .enable_http2()
            .build();
        let locks = config
            .accounts
            .values()
            .filter_map(|account| match account {
                AccountConfig::CodexHome { path } => Some((path.clone(), Arc::new(Mutex::new(())))),
                AccountConfig::Inbound => None,
            })
            .collect();
        Self {
            client: Client::builder(TokioExecutor::new()).build(https),
            locks,
        }
    }

    pub async fn resolve(
        &self,
        account: &AccountConfig,
        inbound: &HeaderMap,
    ) -> Result<Credentials> {
        match account {
            AccountConfig::Inbound => inbound_credentials(inbound),
            AccountConfig::CodexHome { path } => {
                let document = read_auth(&path.join("auth.json"))?;
                if access_token_needs_refresh(&document.value) {
                    let lock = self.lock(path);
                    let _guard = lock.lock().await;
                    let current = read_auth(&path.join("auth.json"))?;
                    if access_token_needs_refresh(&current.value) {
                        return self.refresh(path, current).await;
                    }
                    return Ok(current.credentials);
                }
                Ok(document.credentials)
            }
        }
    }

    pub async fn force_refresh(
        &self,
        account: &AccountConfig,
        previous: &Credentials,
    ) -> Result<Option<Credentials>> {
        let AccountConfig::CodexHome { path } = account else {
            return Ok(None);
        };
        let lock = self.lock(path);
        let _guard = lock.lock().await;
        let current = read_auth(&path.join("auth.json"))?;
        if current.credentials.authorization != previous.authorization {
            return Ok(Some(current.credentials));
        }
        self.refresh(path, current).await.map(Some)
    }

    fn lock(&self, path: &Path) -> Arc<Mutex<()>> {
        self.locks
            .get(path)
            .cloned()
            .unwrap_or_else(|| Arc::new(Mutex::new(())))
    }

    async fn refresh(&self, home: &Path, mut document: AuthDocument) -> Result<Credentials> {
        let refresh_token = lookup(&document.value, &["tokens", "refresh_token"])
            .filter(|value| !value.is_empty())
            .context("Codex auth has no refresh token")?
            .to_owned();
        let body = Bytes::from(serde_json::to_vec(&json!({
            "client_id": CLIENT_ID,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }))?);
        let request = Request::post(REFRESH_URL)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(body))?;
        let response = self.client.request(request).await?;
        let status = response.status();
        let mut response_body = response.into_body();
        let mut bytes = Vec::new();
        while let Some(frame) = response_body.frame().await {
            let frame = frame?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            if bytes.len().saturating_add(data.len()) > MAX_REFRESH_RESPONSE_BYTES {
                bail!("OAuth refresh response exceeds configured safety limit")
            }
            bytes.extend_from_slice(&data);
        }
        if !status.is_success() {
            let error_value = serde_json::from_slice::<Value>(&bytes).ok();
            let code = error_value
                .as_ref()
                .and_then(refresh_error_code)
                .unwrap_or("unknown");
            if status == StatusCode::UNAUTHORIZED
                || matches!(
                    code,
                    "refresh_token_expired" | "refresh_token_reused" | "refresh_token_invalidated"
                )
            {
                bail!("OAuth refresh requires a new device login ({code})")
            }
            bail!("OAuth refresh failed with HTTP {status} ({code})")
        }
        let refreshed: Value = serde_json::from_slice(&bytes).context("parse OAuth refresh")?;
        let tokens = document
            .value
            .get_mut("tokens")
            .and_then(Value::as_object_mut)
            .context("Codex auth tokens are not an object")?;
        for key in ["id_token", "access_token", "refresh_token"] {
            if let Some(value) = refreshed.get(key).and_then(Value::as_str) {
                tokens.insert(key.to_owned(), Value::String(value.to_owned()));
            }
        }
        if let Some(account_id) = tokens
            .get("id_token")
            .and_then(Value::as_str)
            .and_then(jwt_account_id)
        {
            tokens.insert("account_id".to_owned(), Value::String(account_id));
        }
        document.value["last_refresh"] = Value::String(Utc::now().to_rfc3339());
        atomic_write_auth(
            &home.join("auth.json"),
            &serde_json::to_vec_pretty(&document.value)?,
        )?;
        credentials_from_value(&document.value)
    }
}

struct AuthDocument {
    value: Value,
    credentials: Credentials,
}

fn inbound_credentials(inbound: &HeaderMap) -> Result<Credentials> {
    let authorization = inbound
        .get("authorization")
        .context("inbound account requires Authorization")?
        .to_str()
        .context("invalid Authorization header")?
        .to_owned();
    let account_id = inbound
        .get("chatgpt-account-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    Ok(Credentials {
        authorization,
        account_id,
    })
}

fn read_auth(path: &Path) -> Result<AuthDocument> {
    let value: Value = serde_json::from_slice(
        &fs::read(path).with_context(|| format!("read {}", path.display()))?,
    )
    .with_context(|| format!("parse {}", path.display()))?;
    let credentials = credentials_from_value(&value)?;
    Ok(AuthDocument { value, credentials })
}

fn credentials_from_value(value: &Value) -> Result<Credentials> {
    let token = lookup(value, &["access_token"])
        .or_else(|| lookup(value, &["tokens", "access_token"]))
        .or_else(|| lookup(value, &["tokens", "accessToken"]))
        .or_else(|| lookup(value, &["OPENAI_API_KEY"]))
        .filter(|token| !token.is_empty())
        .map(str::to_owned);
    let Some(token) = token else {
        bail!("Codex auth contains no supported access token")
    };
    let authorization = if token.starts_with("Bearer ") {
        token
    } else {
        format!("Bearer {token}")
    };
    let account_id = lookup(value, &["account_id"])
        .or_else(|| lookup(value, &["tokens", "account_id"]))
        .or_else(|| lookup(value, &["tokens", "accountId"]))
        .map(str::to_owned)
        .or_else(|| {
            lookup(value, &["tokens", "id_token"])
                .or_else(|| lookup(value, &["tokens", "idToken"]))
                .and_then(jwt_account_id)
        });
    Ok(Credentials {
        authorization,
        account_id,
    })
}

fn access_token_needs_refresh(value: &Value) -> bool {
    let token = lookup(value, &["tokens", "access_token"])
        .or_else(|| lookup(value, &["tokens", "accessToken"]));
    let Some(expiration) = token.and_then(jwt_expiration) else {
        return false;
    };
    expiration <= unix_now().saturating_add(REFRESH_WINDOW_SECONDS)
}

fn jwt_expiration(token: &str) -> Option<u64> {
    jwt_payload(token)?.get("exp")?.as_u64()
}

fn jwt_account_id(token: &str) -> Option<String> {
    let payload = jwt_payload(token)?;
    payload
        .get("https://api.openai.com/auth")?
        .get("chatgpt_account_id")?
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn jwt_payload(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    serde_json::from_slice(&decoded).ok()
}

fn refresh_error_code(value: &Value) -> Option<&str> {
    value
        .get("error")
        .and_then(|error| {
            error
                .as_str()
                .or_else(|| error.get("code").and_then(Value::as_str))
        })
        .or_else(|| value.get("code").and_then(Value::as_str))
}

fn atomic_write_auth(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    temp.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn lookup<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut current = value;
    for part in path {
        current = current.get(*part)?;
    }
    current.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_account_and_expiry_from_jwt_claims() {
        let payload = URL_SAFE_NO_PAD.encode(
            br#"{"exp":4102444800,"https://api.openai.com/auth":{"chatgpt_account_id":"acct"}}"#,
        );
        let jwt = format!("e30.{payload}.sig");
        assert_eq!(jwt_expiration(&jwt), Some(4_102_444_800));
        assert_eq!(jwt_account_id(&jwt).as_deref(), Some("acct"));
    }
}
