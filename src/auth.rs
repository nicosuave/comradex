use std::{
    collections::{BTreeMap, HashMap},
    error::Error as StdError,
    fmt, fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
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

use crate::{
    auth_lock::HomeAuthLock,
    config::{AccountConfig, Config, normalize_codex_home},
};

const REFRESH_URL: &str = "https://auth.openai.com/oauth/token";
const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
/// Managed credentials are refreshed once their JWT expires within five minutes. This is a
/// deliberately small safety window for completing an in-flight request, not an assumption about
/// an undocumented OAuth idle lifetime.
pub const REFRESH_WINDOW_SECONDS: u64 = 5 * 60;
/// A one-minute sweep keeps the five-minute safety window responsive while bounding idle work to
/// one sequential pass over the configured (at most 512) managed accounts per minute.
pub const PROACTIVE_REFRESH_INTERVAL_SECONDS: u64 = 60;
/// Maximum wall-clock time spent checking one managed account during a proactive sweep. This
/// bounds both lock contention and OAuth I/O so one stalled account cannot block later accounts
/// or keep the next scheduled sweep waiting indefinitely.
pub const PROACTIVE_REFRESH_ACCOUNT_TIMEOUT: Duration = Duration::from_secs(15);
/// Request-time credential recovery must not consume global request capacity indefinitely while
/// another task or process owns the managed home.
pub const REQUEST_AUTH_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
/// Includes DNS, TCP/TLS connection establishment, request upload, and response headers.
pub const OAUTH_RESPONSE_HEADERS_TIMEOUT: Duration = Duration::from_secs(15);
/// Maximum silence between response body frames. The size limit below remains an independent
/// bound for peers that continuously trickle data.
pub const OAUTH_RESPONSE_BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
/// Caps the complete OAuth exchange even when every individual stage continues making progress.
pub const OAUTH_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_REFRESH_RESPONSE_BYTES: usize = 1024 * 1024;
const DEFAULT_OAUTH_EXPIRES_IN_SECONDS: u64 = 60 * 60;
const MAX_OAUTH_EXPIRES_IN_SECONDS: u64 = 24 * 60 * 60;
const ACCESS_TOKEN_EXPIRES_AT_KEY: &str = "comradex_access_token_expires_at";

type AuthClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

#[derive(Debug, Clone)]
pub struct Credentials {
    pub authorization: String,
    pub account_id: Option<String>,
}

pub struct Resolver {
    client: AuthClient,
    locks: HashMap<PathBuf, Arc<Mutex<()>>>,
    refresh_url: hyper::Uri,
    timeouts: AuthTimeouts,
}

#[derive(Debug, Clone, Copy)]
struct AuthTimeouts {
    lock: Duration,
    oauth_headers: Duration,
    oauth_body_idle: Duration,
    oauth_total: Duration,
}

impl Default for AuthTimeouts {
    fn default() -> Self {
        Self {
            lock: REQUEST_AUTH_LOCK_TIMEOUT,
            oauth_headers: OAUTH_RESPONSE_HEADERS_TIMEOUT,
            oauth_body_idle: OAUTH_RESPONSE_BODY_IDLE_TIMEOUT,
            oauth_total: OAUTH_TOTAL_TIMEOUT,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProactiveRefresh {
    Fresh,
    Refreshed,
}

#[derive(Debug)]
struct ReauthRequired {
    code: String,
}

impl fmt::Display for ReauthRequired {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "OAuth refresh requires a new device login ({})",
            self.code
        )
    }
}

impl StdError for ReauthRequired {}

pub fn is_reauth_required(error: &anyhow::Error) -> bool {
    error.downcast_ref::<ReauthRequired>().is_some()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthFailureRecovery {
    ReuseCurrent,
    RefreshCurrent,
}

fn auth_failure_recovery(current: &Credentials, failed: &Credentials) -> AuthFailureRecovery {
    if current.authorization == failed.authorization {
        AuthFailureRecovery::RefreshCurrent
    } else {
        AuthFailureRecovery::ReuseCurrent
    }
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
                AccountConfig::CodexHome { path } => Some((
                    normalize_codex_home(path).expect("Config::validate normalized account home"),
                    Arc::new(Mutex::new(())),
                )),
                AccountConfig::Inbound => None,
            })
            .collect();
        Self {
            client: Client::builder(TokioExecutor::new()).build(https),
            locks,
            refresh_url: REFRESH_URL.parse().expect("static refresh URL is valid"),
            timeouts: AuthTimeouts::default(),
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
                    return self.spawn_request_refresh(path, None).await;
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
        self.spawn_request_refresh(path, Some(previous.clone()))
            .await
            .map(Some)
    }

    /// Refresh a managed account only when its access-token JWT is within the documented safety
    /// window. The auth file is always re-read while holding the same normalized-home lock used by
    /// request-time resolution and forced auth recovery.
    pub async fn proactive_refresh_at(
        &self,
        account: &AccountConfig,
        now: u64,
    ) -> Result<Option<ProactiveRefresh>> {
        let AccountConfig::CodexHome { path } = account else {
            return Ok(None);
        };
        self.spawn_proactive_refresh(path, now).await.map(Some)
    }

    /// Run a bounded pass over managed accounts. Every result is retained independently so one
    /// account's filesystem, network, or OAuth failure cannot stop later configured IDs.
    pub async fn proactive_refresh_managed_at(
        &self,
        accounts: &BTreeMap<String, AccountConfig>,
        now: u64,
    ) -> Vec<(String, Result<ProactiveRefresh>)> {
        self.proactive_refresh_managed_at_with_timeout(
            accounts,
            now,
            PROACTIVE_REFRESH_ACCOUNT_TIMEOUT,
        )
        .await
    }

    async fn proactive_refresh_managed_at_with_timeout(
        &self,
        accounts: &BTreeMap<String, AccountConfig>,
        now: u64,
        per_account_timeout: Duration,
    ) -> Vec<(String, Result<ProactiveRefresh>)> {
        let mut results = Vec::new();
        for (account_id, account) in accounts {
            if !matches!(account, AccountConfig::CodexHome { .. }) {
                continue;
            }
            let result = match tokio::time::timeout(
                per_account_timeout,
                self.proactive_refresh_at(account, now),
            )
            .await
            {
                Ok(result) => result.and_then(|outcome| {
                    outcome.context("managed account returned no refresh state")
                }),
                Err(_) => Err(anyhow::anyhow!(
                    "managed account refresh timed out after {} ms",
                    per_account_timeout.as_millis()
                )),
            };
            results.push((account_id.clone(), result));
        }
        results
    }

    fn lock(&self, path: &Path) -> Result<Arc<Mutex<()>>> {
        let identity = normalize_codex_home(path)?;
        self.locks
            .get(&identity)
            .cloned()
            .with_context(|| format!("no resolver lock for codex_home {}", identity.display()))
    }

    /// Run request-time refresh independently of the waiting caller. If the caller disconnects
    /// after the OAuth server rotates its refresh token, the exchange still reaches the guarded
    /// compare-and-swap write instead of abandoning the new token and poisoning later refreshes.
    async fn spawn_request_refresh(
        &self,
        home: &Path,
        failed: Option<Credentials>,
    ) -> Result<Credentials> {
        let home = home.to_owned();
        let lock = self.lock(&home)?;
        let client = self.client.clone();
        let refresh_url = self.refresh_url.clone();
        let timeouts = self.timeouts;
        tokio::spawn(async move {
            let _guard = tokio::time::timeout(timeouts.lock, lock.lock_owned())
                .await
                .with_context(|| {
                    format!(
                        "timed out after {} ms waiting for in-process auth lock {}",
                        timeouts.lock.as_millis(),
                        home.display()
                    )
                })?;
            let _home_guard =
                HomeAuthLock::acquire_async_with_timeout(&home, timeouts.lock).await?;
            let current = read_auth(&home.join("auth.json"))?;
            if let Some(failed) = failed.as_ref() {
                if auth_failure_recovery(&current.credentials, failed)
                    == AuthFailureRecovery::ReuseCurrent
                {
                    return Ok(current.credentials);
                }
            } else if !access_token_needs_refresh(&current.value) {
                return Ok(current.credentials);
            }
            refresh_with(&client, &refresh_url, &home, current, unix_now(), timeouts).await
        })
        .await
        .context("request-time auth refresh task failed")?
    }

    /// Keep a proactive OAuth exchange alive if its sweep deadline expires or shutdown cancels
    /// the waiter. The bounded worker must either persist/CAS-reconcile a rotated token or fail on
    /// its own lock/network deadline.
    async fn spawn_proactive_refresh(&self, home: &Path, now: u64) -> Result<ProactiveRefresh> {
        let home = home.to_owned();
        let lock = self.lock(&home)?;
        let client = self.client.clone();
        let refresh_url = self.refresh_url.clone();
        let timeouts = self.timeouts;
        tokio::spawn(async move {
            let _guard = tokio::time::timeout(timeouts.lock, lock.lock_owned())
                .await
                .with_context(|| {
                    format!(
                        "timed out after {} ms waiting for in-process auth lock {}",
                        timeouts.lock.as_millis(),
                        home.display()
                    )
                })?;
            let _home_guard =
                HomeAuthLock::acquire_async_with_timeout(&home, timeouts.lock).await?;
            let current = read_auth(&home.join("auth.json"))?;
            if !access_token_needs_refresh_at(&current.value, now) {
                return Ok(ProactiveRefresh::Fresh);
            }
            refresh_with(&client, &refresh_url, &home, current, now, timeouts).await?;
            Ok(ProactiveRefresh::Refreshed)
        })
        .await
        .context("proactive auth refresh task failed")?
    }
}

async fn refresh_with(
    client: &AuthClient,
    refresh_url: &hyper::Uri,
    home: &Path,
    mut document: AuthDocument,
    now: u64,
    timeouts: AuthTimeouts,
) -> Result<Credentials> {
    let original = document.value.clone();
    let refresh_token = lookup(&original, &["tokens", "refresh_token"])
        .filter(|value| !value.is_empty())
        .context("Codex auth has no refresh token")?
        .to_owned();
    let body = Bytes::from(serde_json::to_vec(&json!({
        "client_id": CLIENT_ID,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }))?);
    let request = Request::post(refresh_url.clone())
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(body))?;
    let exchange = async {
        let response = tokio::time::timeout(timeouts.oauth_headers, client.request(request))
            .await
            .context("OAuth refresh timed out waiting for response headers")??;
        let status = response.status();
        let mut response_body = response.into_body();
        let mut bytes = Vec::new();
        loop {
            let frame = tokio::time::timeout(timeouts.oauth_body_idle, response_body.frame())
                .await
                .context("OAuth refresh response body became idle")?;
            let Some(frame) = frame else { break };
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
                return Err(ReauthRequired {
                    code: code.to_owned(),
                }
                .into());
            }
            bail!("OAuth refresh failed with HTTP {status} ({code})")
        }
        let refreshed: Value = serde_json::from_slice(&bytes).context("parse OAuth refresh")?;
        Result::<Value>::Ok(refreshed)
    };
    let refreshed = tokio::time::timeout(timeouts.oauth_total, exchange)
        .await
        .context("OAuth refresh exceeded total deadline")??;
    let access_token = refreshed
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .context("OAuth refresh response has no access token")?;
    let expires_at = now.saturating_add(validated_expires_in(&refreshed));
    let tokens = document
        .value
        .get_mut("tokens")
        .and_then(Value::as_object_mut)
        .context("Codex auth tokens are not an object")?;
    tokens.insert(
        "access_token".to_owned(),
        Value::String(access_token.to_owned()),
    );
    tokens.insert(
        ACCESS_TOKEN_EXPIRES_AT_KEY.to_owned(),
        Value::Number(expires_at.into()),
    );
    for key in ["id_token", "refresh_token"] {
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
    let refreshed_at = chrono::DateTime::<Utc>::from_timestamp(now as i64, 0)
        .unwrap_or_else(Utc::now)
        .to_rfc3339();
    document.value["last_refresh"] = Value::String(refreshed_at);
    // `codex login` does not honor Comradex's advisory lock. Re-read immediately before the
    // atomic replace and never overwrite any external writer's newer document.
    let latest = read_auth(&home.join("auth.json"))?;
    if latest.value != original {
        return Ok(latest.credentials);
    }
    atomic_write_auth(
        &home.join("auth.json"),
        &serde_json::to_vec_pretty(&document.value)?,
    )?;
    credentials_from_value(&document.value)
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
    access_token_needs_refresh_at(value, unix_now())
}

fn access_token_needs_refresh_at(value: &Value, now: u64) -> bool {
    let token = lookup(value, &["tokens", "access_token"])
        .or_else(|| lookup(value, &["tokens", "accessToken"]));
    let Some(token) = token else {
        // Top-level API keys and legacy non-managed credential shapes are not refresh candidates.
        return false;
    };
    let jwt_expiration = jwt_expiration(token);
    let fallback_value = value
        .get("tokens")
        .and_then(|tokens| tokens.get(ACCESS_TOKEN_EXPIRES_AT_KEY));
    let fallback_expiration = fallback_value
        .and_then(Value::as_u64)
        .filter(|expiration| *expiration <= now.saturating_add(MAX_OAUTH_EXPIRES_IN_SECONDS));
    let expiration = match (jwt_expiration, fallback_expiration) {
        (Some(jwt), Some(fallback)) => jwt.min(fallback),
        (Some(jwt), None) => jwt,
        (None, Some(fallback)) => fallback,
        // A persisted but malformed fallback came from a Comradex refresh and must fail closed.
        (None, None) if fallback_value.is_some() => return true,
        // Preserve compatibility with existing opaque managed credentials. Every successful
        // Comradex refresh writes the bounded fallback, so only legacy/external tokens reach here.
        (None, None) => return false,
    };
    expiration <= now.saturating_add(REFRESH_WINDOW_SECONDS)
}

fn validated_expires_in(response: &Value) -> u64 {
    response
        .get("expires_in")
        .and_then(Value::as_u64)
        .filter(|seconds| (1..=MAX_OAUTH_EXPIRES_IN_SECONDS).contains(seconds))
        .unwrap_or(DEFAULT_OAUTH_EXPIRES_IN_SECONDS)
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

    fn jwt(expiration: u64, marker: &str) -> String {
        let payload = URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&json!({ "exp": expiration, "marker": marker })).unwrap());
        format!("e30.{payload}.sig")
    }

    fn write_managed_auth(home: &Path, access_token: &str) {
        fs::create_dir_all(home).unwrap();
        fs::write(
            home.join("auth.json"),
            serde_json::to_vec(&json!({
                "tokens": {
                    "access_token": access_token,
                    "refresh_token": "test-refresh-token"
                }
            }))
            .unwrap(),
        )
        .unwrap();
    }

    fn test_resolver(homes: &[&Path], refresh_url: hyper::Uri) -> Resolver {
        test_resolver_with_timeouts(homes, refresh_url, AuthTimeouts::default())
    }

    fn test_resolver_with_timeouts(
        homes: &[&Path],
        refresh_url: hyper::Uri,
        timeouts: AuthTimeouts,
    ) -> Resolver {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        let locks = homes
            .iter()
            .map(|home| {
                (
                    normalize_codex_home(home).unwrap(),
                    Arc::new(Mutex::new(())),
                )
            })
            .collect();
        Resolver {
            client: Client::builder(TokioExecutor::new()).build(https),
            locks,
            refresh_url,
            timeouts,
        }
    }

    async fn serve_refresh_response(status: &str, body: Value) -> hyper::Uri {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let body = serde_json::to_vec(&body).unwrap();
        let status = status.to_owned();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let headers = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(headers.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
        });
        format!("http://{address}/oauth/token").parse().unwrap()
    }

    async fn serve_paused_refresh_response() -> hyper::Uri {
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            std::future::pending::<()>().await;
        });
        format!("http://{address}/oauth/token").parse().unwrap()
    }

    async fn serve_paused_refresh_body() -> hyper::Uri {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1024\r\n\r\n{",
                )
                .await
                .unwrap();
            std::future::pending::<()>().await;
        });
        format!("http://{address}/oauth/token").parse().unwrap()
    }

    async fn serve_controlled_refresh_response(
        body: Value,
    ) -> (
        hyper::Uri,
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let body = serde_json::to_vec(&body).unwrap();
        let (request_seen_tx, request_seen_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let _ = request_seen_tx.send(());
            let _ = release_rx.await;
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(headers.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
        });
        (
            format!("http://{address}/oauth/token").parse().unwrap(),
            request_seen_rx,
            release_tx,
        )
    }

    fn short_timeouts() -> AuthTimeouts {
        AuthTimeouts {
            lock: Duration::from_millis(50),
            oauth_headers: Duration::from_millis(100),
            oauth_body_idle: Duration::from_millis(100),
            oauth_total: Duration::from_millis(500),
        }
    }

    fn credentials(token: &str) -> Credentials {
        Credentials {
            authorization: format!("Bearer {token}"),
            account_id: None,
        }
    }

    fn simulated_recovery_refresh_count(current: &Credentials, failed: &Credentials) -> usize {
        usize::from(auth_failure_recovery(current, failed) == AuthFailureRecovery::RefreshCurrent)
    }

    #[test]
    fn derives_account_and_expiry_from_jwt_claims() {
        let payload = URL_SAFE_NO_PAD.encode(
            br#"{"exp":4102444800,"https://api.openai.com/auth":{"chatgpt_account_id":"acct"}}"#,
        );
        let jwt = format!("e30.{payload}.sig");
        assert_eq!(jwt_expiration(&jwt), Some(4_102_444_800));
        assert_eq!(jwt_account_id(&jwt).as_deref(), Some("acct"));
    }

    #[test]
    fn proactive_threshold_is_exactly_five_minutes() {
        let now = 1_000_000;
        let outside = json!({ "tokens": { "access_token": jwt(now + 301, "outside") } });
        let boundary = json!({ "tokens": { "access_token": jwt(now + 300, "boundary") } });
        assert!(!access_token_needs_refresh_at(&outside, now));
        assert!(access_token_needs_refresh_at(&boundary, now));
    }

    #[test]
    fn missing_or_untrusted_managed_expiry_is_never_indefinitely_fresh() {
        let now = 1_000_000;
        for value in [
            json!({
                "tokens": {
                    "access_token": "opaque-token",
                    ACCESS_TOKEN_EXPIRES_AT_KEY: "not-a-timestamp"
                }
            }),
            json!({
                "tokens": {
                    "access_token": "opaque-token",
                    ACCESS_TOKEN_EXPIRES_AT_KEY: u64::MAX
                }
            }),
        ] {
            assert!(access_token_needs_refresh_at(&value, now));
        }

        let legacy_opaque = json!({ "tokens": { "access_token": "opaque-token" } });
        assert!(!access_token_needs_refresh_at(&legacy_opaque, now));

        let far_future_jwt = json!({
            "tokens": {
                "access_token": jwt(u64::MAX, "far-future"),
                "refresh_token": "test-refresh-token"
            }
        });
        assert!(!access_token_needs_refresh_at(&far_future_jwt, now));

        let bounded = json!({
            "tokens": {
                "access_token": "opaque-token",
                ACCESS_TOKEN_EXPIRES_AT_KEY: now + 3_600
            }
        });
        assert!(!access_token_needs_refresh_at(&bounded, now));
        assert!(access_token_needs_refresh_at(&bounded, now + 3_300));
    }

    #[test]
    fn oauth_expires_in_is_strictly_bounded() {
        assert_eq!(validated_expires_in(&json!({ "expires_in": 7_200 })), 7_200);
        for response in [
            json!({}),
            json!({ "expires_in": "3600" }),
            json!({ "expires_in": -1 }),
            json!({ "expires_in": 0 }),
            json!({ "expires_in": MAX_OAUTH_EXPIRES_IN_SECONDS + 1 }),
        ] {
            assert_eq!(
                validated_expires_in(&response),
                DEFAULT_OAUTH_EXPIRES_IN_SECONDS
            );
        }
    }

    #[tokio::test]
    async fn refreshed_opaque_access_token_gets_bounded_fallback_expiry() {
        let now = 1_000_000;
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("managed");
        write_managed_auth(&home, &jwt(now + 60, "stale"));
        let refresh_url = serve_refresh_response(
            "200 OK",
            json!({
                "access_token": "opaque-replacement",
                "refresh_token": "replacement-refresh-token",
                "expires_in": u64::MAX
            }),
        )
        .await;
        let resolver = test_resolver(&[&home], refresh_url);
        let account = AccountConfig::CodexHome { path: home.clone() };

        assert_eq!(
            resolver.proactive_refresh_at(&account, now).await.unwrap(),
            Some(ProactiveRefresh::Refreshed)
        );
        let written: Value =
            serde_json::from_slice(&fs::read(home.join("auth.json")).unwrap()).unwrap();
        assert_eq!(
            written["tokens"][ACCESS_TOKEN_EXPIRES_AT_KEY],
            now + DEFAULT_OAUTH_EXPIRES_IN_SECONDS
        );
        assert!(!access_token_needs_refresh_at(&written, now));
        assert!(access_token_needs_refresh_at(
            &written,
            now + DEFAULT_OAUTH_EXPIRES_IN_SECONDS - REFRESH_WINDOW_SECONDS
        ));
    }

    #[tokio::test]
    async fn managed_sweep_refreshes_stale_accounts_and_continues_after_failure() {
        let now = 1_000_000;
        let directory = tempfile::tempdir().unwrap();
        let stale_home = directory.path().join("stale");
        let missing_home = directory.path().join("missing");
        let fresh_home = directory.path().join("fresh");
        write_managed_auth(&stale_home, &jwt(now + 60, "stale"));
        fs::create_dir_all(&missing_home).unwrap();
        write_managed_auth(&fresh_home, &jwt(now + 3_600, "fresh"));
        let refreshed_token = jwt(now + 7_200, "refreshed");
        let refresh_url = serve_refresh_response(
            "200 OK",
            json!({
                "access_token": refreshed_token,
                "refresh_token": "replacement-refresh-token"
            }),
        )
        .await;
        let resolver = test_resolver(&[&stale_home, &missing_home, &fresh_home], refresh_url);
        let stale = AccountConfig::CodexHome {
            path: stale_home.clone(),
        };
        let missing = AccountConfig::CodexHome { path: missing_home };
        let fresh = AccountConfig::CodexHome { path: fresh_home };
        let inbound = AccountConfig::Inbound;

        let accounts = BTreeMap::from([
            ("1-stale".to_owned(), stale),
            ("2-missing".to_owned(), missing),
            ("3-inbound".to_owned(), inbound),
            ("4-fresh".to_owned(), fresh),
        ]);
        let results = resolver.proactive_refresh_managed_at(&accounts, now).await;

        assert_eq!(results.len(), 3, "inbound accounts must never be touched");
        assert_eq!(results[0].0, "1-stale");
        assert!(matches!(results[0].1, Ok(ProactiveRefresh::Refreshed)));
        assert_eq!(results[1].0, "2-missing");
        assert!(results[1].1.is_err());
        assert_eq!(results[2].0, "4-fresh");
        assert!(matches!(results[2].1, Ok(ProactiveRefresh::Fresh)));
        let written: Value =
            serde_json::from_slice(&fs::read(stale_home.join("auth.json")).unwrap()).unwrap();
        assert_eq!(
            lookup(&written, &["tokens", "refresh_token"]),
            Some("replacement-refresh-token")
        );
    }

    #[tokio::test]
    async fn managed_sweep_times_out_stalled_refresh_and_checks_later_account() {
        let now = 1_000_000;
        let directory = tempfile::tempdir().unwrap();
        let stalled_home = directory.path().join("stalled");
        let fresh_home = directory.path().join("fresh");
        write_managed_auth(&stalled_home, &jwt(now + 60, "stalled"));
        write_managed_auth(&fresh_home, &jwt(now + 3_600, "fresh"));
        let refresh_url = serve_paused_refresh_response().await;
        let resolver = test_resolver(&[&stalled_home, &fresh_home], refresh_url);
        let accounts = BTreeMap::from([
            (
                "1-stalled".to_owned(),
                AccountConfig::CodexHome {
                    path: stalled_home.clone(),
                },
            ),
            (
                "2-fresh".to_owned(),
                AccountConfig::CodexHome { path: fresh_home },
            ),
        ]);

        let results = resolver
            .proactive_refresh_managed_at_with_timeout(&accounts, now, Duration::from_millis(50))
            .await;

        assert_eq!(results.len(), 2);
        assert_eq!(
            results.iter().filter(|(_, result)| result.is_err()).count(),
            1
        );
        assert_eq!(results[0].0, "1-stalled");
        assert!(format!("{:#}", results[0].1.as_ref().unwrap_err()).contains("timed out"));
        assert_eq!(results[1].0, "2-fresh");
        assert!(matches!(results[1].1, Ok(ProactiveRefresh::Fresh)));
        let lock = resolver.lock(&stalled_home).unwrap();
        assert!(
            lock.try_lock().is_err(),
            "the sweep timeout must detach rather than cancel the in-flight OAuth exchange"
        );
        assert!(
            HomeAuthLock::try_acquire(&stalled_home).unwrap().is_none(),
            "the detached exchange must retain exclusive ownership until its own deadline"
        );
    }

    #[tokio::test]
    async fn request_time_refresh_bounds_cross_process_lock_wait() {
        let now = unix_now();
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("managed");
        write_managed_auth(&home, &jwt(now + 60, "stale"));
        let resolver = test_resolver_with_timeouts(
            &[&home],
            "http://127.0.0.1:9/oauth/token".parse().unwrap(),
            short_timeouts(),
        );
        let account = AccountConfig::CodexHome { path: home.clone() };
        let login_guard = HomeAuthLock::acquire(&home).unwrap();

        let error = resolver
            .resolve(&account, &HeaderMap::new())
            .await
            .unwrap_err();

        assert!(format!("{error:#}").contains("timed out"));
        drop(login_guard);
        tokio::time::sleep(Duration::from_millis(75)).await;
        assert!(
            HomeAuthLock::try_acquire(&home).unwrap().is_some(),
            "the timed-out detached refresh must not later acquire the home"
        );
    }

    #[tokio::test]
    async fn oauth_response_headers_and_body_idle_are_independently_bounded() {
        let now = unix_now();
        for (label, refresh_url, expected) in [
            (
                "headers",
                serve_paused_refresh_response().await,
                "response headers",
            ),
            (
                "body",
                serve_paused_refresh_body().await,
                "response body became idle",
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let home = directory.path().join(label);
            write_managed_auth(&home, &jwt(now + 60, label));
            let resolver = test_resolver_with_timeouts(&[&home], refresh_url, short_timeouts());
            let account = AccountConfig::CodexHome { path: home };

            let error = resolver
                .resolve(&account, &HeaderMap::new())
                .await
                .unwrap_err();

            assert!(
                format!("{error:#}").contains(expected),
                "{label} timeout returned an unexpected error: {error:#}"
            );
        }
    }

    #[tokio::test]
    async fn caller_cancellation_does_not_abandon_rotated_refresh_token() {
        let now = unix_now();
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("managed");
        write_managed_auth(&home, &jwt(now + 60, "stale"));
        let replacement = jwt(now + 7_200, "replacement");
        let (refresh_url, request_seen, release) = serve_controlled_refresh_response(json!({
            "access_token": replacement,
            "refresh_token": "rotated-refresh-token"
        }))
        .await;
        let resolver = Arc::new(test_resolver(&[&home], refresh_url));
        let account = AccountConfig::CodexHome { path: home.clone() };
        let caller = {
            let resolver = resolver.clone();
            tokio::spawn(async move { resolver.resolve(&account, &HeaderMap::new()).await })
        };
        request_seen.await.unwrap();

        caller.abort();
        release.send(()).unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let written = read_auth(&home.join("auth.json")).unwrap();
                if lookup(&written.value, &["tokens", "refresh_token"])
                    == Some("rotated-refresh-token")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("detached refresh should persist the rotated token");
    }

    #[tokio::test]
    async fn proactive_cancellation_persists_rotated_token_without_lock_convoy() {
        let now = 1_000_000;
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("managed");
        write_managed_auth(&home, &jwt(now + 60, "stale"));
        let replacement = jwt(now + 7_200, "replacement");
        let (refresh_url, request_seen, release) = serve_controlled_refresh_response(json!({
            "access_token": replacement,
            "refresh_token": "proactively-rotated-refresh-token"
        }))
        .await;
        let resolver = Arc::new(test_resolver(&[&home], refresh_url));
        let account = AccountConfig::CodexHome { path: home.clone() };
        let sweep_waiter = {
            let resolver = resolver.clone();
            let account = account.clone();
            tokio::spawn(async move { resolver.proactive_refresh_at(&account, now).await })
        };
        request_seen.await.unwrap();

        sweep_waiter.abort();
        release.send(()).unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let written = read_auth(&home.join("auth.json")).unwrap();
                if lookup(&written.value, &["tokens", "refresh_token"])
                    == Some("proactively-rotated-refresh-token")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("canceled sweep waiter must not abandon the rotated token");

        let next = tokio::time::timeout(
            Duration::from_millis(250),
            resolver.proactive_refresh_at(&account, now),
        )
        .await
        .expect("completed detached worker must release the per-home locks")
        .unwrap();
        assert_eq!(next, Some(ProactiveRefresh::Fresh));
    }

    #[tokio::test]
    async fn external_auth_writer_wins_compare_and_swap_race() {
        let now = unix_now();
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("managed");
        let stale = jwt(now + 60, "stale");
        write_managed_auth(&home, &stale);
        let oauth_replacement = jwt(now + 7_200, "oauth-replacement");
        let (refresh_url, request_seen, release) = serve_controlled_refresh_response(json!({
            "access_token": oauth_replacement,
            "refresh_token": "oauth-rotated-refresh-token"
        }))
        .await;
        let resolver = Arc::new(test_resolver(&[&home], refresh_url));
        let account = AccountConfig::CodexHome { path: home.clone() };
        let refresh = {
            let resolver = resolver.clone();
            tokio::spawn(async move { resolver.resolve(&account, &HeaderMap::new()).await })
        };
        request_seen.await.unwrap();
        let external = jwt(now + 8_000, "external-writer");
        write_managed_auth(&home, &external);
        release.send(()).unwrap();

        let resolved = refresh.await.unwrap().unwrap();
        let written = read_auth(&home.join("auth.json")).unwrap();

        assert_eq!(resolved.authorization, format!("Bearer {external}"));
        assert_eq!(
            written.credentials.authorization,
            format!("Bearer {external}")
        );
        assert_eq!(
            lookup(&written.value, &["tokens", "refresh_token"]),
            Some("test-refresh-token"),
            "the OAuth result must not overwrite an externally replaced auth document"
        );
    }

    #[tokio::test]
    async fn refresh_token_rejection_is_typed_as_reauth_required() {
        let now = 1_000_000;
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("managed");
        write_managed_auth(&home, &jwt(now + 60, "stale"));
        let refresh_url = serve_refresh_response(
            "401 Unauthorized",
            json!({ "error": { "code": "refresh_token_expired" } }),
        )
        .await;
        let resolver = test_resolver(&[&home], refresh_url);
        let account = AccountConfig::CodexHome { path: home };

        let error = resolver
            .proactive_refresh_at(&account, now)
            .await
            .unwrap_err();

        assert!(is_reauth_required(&error));
        assert!(!format!("{error:#}").contains("test-refresh-token"));
    }

    #[tokio::test]
    async fn proactive_refresh_waits_for_cross_process_home_lock() {
        let now = 1_000_000;
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("managed");
        write_managed_auth(&home, &jwt(now + 3_600, "fresh"));
        let resolver = Arc::new(test_resolver(
            &[&home],
            "http://127.0.0.1:9/oauth/token".parse().unwrap(),
        ));
        let account = AccountConfig::CodexHome { path: home.clone() };
        let login_guard = HomeAuthLock::acquire(&home).unwrap();
        let refresh = {
            let resolver = resolver.clone();
            tokio::spawn(async move { resolver.proactive_refresh_at(&account, now).await })
        };
        let mut refresh = Box::pin(refresh);

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut refresh)
                .await
                .is_err(),
            "refresh must not read or write auth.json while login owns the home"
        );
        drop(login_guard);

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), &mut refresh)
            .await
            .expect("refresh should resume after login releases the lock")
            .unwrap()
            .unwrap();
        assert_eq!(outcome, Some(ProactiveRefresh::Fresh));
    }

    #[test]
    fn near_expiry_resolve_refresh_is_reused_after_old_socket_auth_failure() {
        let credential_used_by_failed_socket = credentials("near-expiry");
        let credential_refreshed_during_later_resolve = credentials("already-refreshed");

        assert_eq!(
            auth_failure_recovery(
                &credential_refreshed_during_later_resolve,
                &credential_used_by_failed_socket,
            ),
            AuthFailureRecovery::ReuseCurrent
        );
        assert_eq!(
            simulated_recovery_refresh_count(
                &credential_refreshed_during_later_resolve,
                &credential_used_by_failed_socket,
            ),
            0
        );
    }

    #[test]
    fn concurrent_credential_replacement_is_reused_without_refresh() {
        let credential_used_by_failed_socket = credentials("old");
        let concurrently_replaced_credential = credentials("new");

        assert_eq!(
            auth_failure_recovery(
                &concurrently_replaced_credential,
                &credential_used_by_failed_socket,
            ),
            AuthFailureRecovery::ReuseCurrent
        );
        assert_eq!(
            simulated_recovery_refresh_count(
                &concurrently_replaced_credential,
                &credential_used_by_failed_socket,
            ),
            0
        );
    }

    #[test]
    fn unchanged_failed_credential_gets_exactly_one_forced_refresh() {
        let credential_used_by_failed_socket = credentials("unchanged");
        let current_credential = credential_used_by_failed_socket.clone();

        assert_eq!(
            auth_failure_recovery(&current_credential, &credential_used_by_failed_socket),
            AuthFailureRecovery::RefreshCurrent
        );
        assert_eq!(
            simulated_recovery_refresh_count(
                &current_credential,
                &credential_used_by_failed_socket,
            ),
            1
        );
    }

    #[tokio::test]
    async fn force_refresh_executes_one_real_exchange_for_the_failed_credential() {
        let now = unix_now();
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("managed");
        let failed_token = jwt(now + 3_600, "failed-socket");
        write_managed_auth(&home, &failed_token);
        let replacement = jwt(now + 7_200, "replacement");
        let refresh_url = serve_refresh_response(
            "200 OK",
            json!({
                "access_token": replacement,
                "refresh_token": "replacement-refresh-token"
            }),
        )
        .await;
        let resolver = test_resolver(&[&home], refresh_url);
        let account = AccountConfig::CodexHome { path: home.clone() };

        let refreshed = resolver
            .force_refresh(&account, &credentials(&failed_token))
            .await
            .unwrap()
            .unwrap();

        assert_ne!(refreshed.authorization, format!("Bearer {failed_token}"));
        let written: Value =
            serde_json::from_slice(&fs::read(home.join("auth.json")).unwrap()).unwrap();
        assert_eq!(
            lookup(&written, &["tokens", "refresh_token"]),
            Some("replacement-refresh-token")
        );
    }
}
