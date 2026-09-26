use crate::{
    auth_lock::HomeAuthLock,
    config::{AccountConfig, Config},
};
use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Request, Uri};
use hyper_rustls::HttpsConnector;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};
use tokio::sync::Mutex;

const TOKEN_URL: &str = "https://platform.claude.com/v1/oauth/token";
const CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
type AuthClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

// Intentionally no Debug: token documents must never appear in diagnostics.
#[derive(Clone, Serialize, Deserialize)]
pub struct Credential {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: u64,
    pub account_uuid: String,
    pub organization_uuid: String,
    pub device_id: String,
}

pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
pub fn read(home: &Path) -> Result<Credential> {
    let bytes = fs::read(home.join("claude-auth.json")).context("Claude account needs login")?;
    let credentials: Credential = serde_json::from_slice(&bytes)
        .map_err(|_| anyhow::anyhow!("invalid Claude credential document"))?;
    credentials.validate()?;
    Ok(credentials)
}

impl Credential {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.access_token.starts_with("sk-ant-oat")
                && !self.access_token.bytes().any(|b| b.is_ascii_whitespace()),
            "Claude subscription OAuth token required"
        );
        ensure!(
            super::wire::uuid(&self.account_uuid) && super::wire::uuid(&self.organization_uuid),
            "real Claude account and organization IDs required"
        );
        ensure!(
            self.device_id.len() == 64 && self.device_id.bytes().all(|b| b.is_ascii_hexdigit()),
            "native Claude device ID required"
        );
        ensure!(self.expires_at > 0, "Claude token expiry required");
        Ok(())
    }
    pub(crate) fn owner(&self) -> crate::auth::QuotaOwner {
        crate::auth::QuotaOwner::claude(&self.organization_uuid, &self.account_uuid)
    }
    fn persist(&self, home: &Path) -> Result<()> {
        self.validate()?;
        fs::create_dir_all(home)?;
        let mut temp = tempfile::NamedTempFile::new_in(home)?;
        temp.write_all(&serde_json::to_vec(self)?)?;
        temp.as_file().sync_all()?;
        temp.persist(home.join("claude-auth.json"))
            .map_err(|e| e.error)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Resolver {
    locks: HashMap<PathBuf, Arc<Mutex<()>>>,
    failures: Arc<Mutex<HashMap<PathBuf, (blake3::Hash, u64)>>>,
    client: AuthClient,
    token_url: Uri,
}

impl Resolver {
    #[cfg(test)]
    pub(crate) fn use_test_endpoint(&mut self, url: &str) {
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        self.client = Client::builder(TokioExecutor::new()).build(connector);
        self.token_url = url.parse().unwrap();
    }
    pub fn new(config: &Config) -> Self {
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_only()
            .enable_http1()
            .build();
        Self {
            locks: config
                .accounts
                .values()
                .filter_map(|account| match account {
                    AccountConfig::ClaudeHome { path } => {
                        Some((path.clone(), Arc::new(Mutex::new(()))))
                    }
                    _ => None,
                })
                .collect(),
            failures: Arc::new(Mutex::new(HashMap::new())),
            client: Client::builder(TokioExecutor::new()).build(connector),
            token_url: TOKEN_URL.parse().unwrap(),
        }
    }
    pub async fn resolve(&self, home: &Path, rejected: Option<&str>) -> Result<Credential> {
        let resolver = self.clone();
        let home = home.to_owned();
        let rejected = rejected.map(str::to_owned);
        // A disconnected inference client must not cancel a rotating refresh grant.
        tokio::spawn(async move {
            tokio::time::timeout(
                std::time::Duration::from_secs(15),
                resolver.resolve_inner(&home, rejected.as_deref()),
            )
            .await
            .context("Claude credential resolution timed out")?
        })
        .await
        .context("join Claude refresh")?
    }
    pub async fn needs_login(&self, home: &Path) -> bool {
        let Ok(current) = read(home) else { return true };
        if current.refresh_token.is_empty() && current.expires_at <= now() + 60 {
            return true;
        }
        self.failures
            .lock()
            .await
            .get(home)
            .is_some_and(|(fingerprint, until)| {
                *until == u64::MAX && *fingerprint == blake3::hash(current.refresh_token.as_bytes())
            })
    }
    pub(crate) async fn require_login(&self, home: &Path) {
        if let Ok(current) = read(home) {
            self.failures.lock().await.insert(
                home.to_owned(),
                (blake3::hash(current.refresh_token.as_bytes()), u64::MAX),
            );
        }
    }
    async fn resolve_inner(&self, home: &Path, rejected: Option<&str>) -> Result<Credential> {
        let lock = self
            .locks
            .get(home)
            .context("unknown Claude credential store")?;
        let _lock = lock.lock().await;
        let _file_lock = HomeAuthLock::acquire_async(home).await?;
        let mut current = read(home)?;
        let fingerprint = blake3::hash(current.refresh_token.as_bytes());
        if let Some((old, until)) = self.failures.lock().await.get(home) {
            ensure!(
                *old != fingerprint || *until <= now(),
                "Claude refresh unavailable; wait or log in again"
            );
        }
        if current.expires_at > now() + 60 && rejected != Some(current.access_token.as_str()) {
            return Ok(current);
        }
        ensure!(
            !current.refresh_token.is_empty(),
            "Claude login cannot be refreshed; log in again"
        );
        let request=Request::post(self.token_url.clone()).header("content-type","application/json")
            .body(Full::new(Bytes::from(serde_json::to_vec(&json!({
                "grant_type":"refresh_token","refresh_token":current.refresh_token,"client_id":CLIENT_ID
            }))?)))?;
        let response = self
            .client
            .request(request)
            .await
            .context("Claude token refresh transport failed")?;
        let status = response.status();
        let retry = response
            .headers()
            .get("retry-after")
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.parse::<u64>().ok())
            .unwrap_or(60);
        let bytes = Limited::new(response.into_body(), 1024 * 1024)
            .collect()
            .await
            .map_err(|_| anyhow::anyhow!("invalid Claude refresh response"))?
            .to_bytes();
        if !status.is_success() {
            let permanent = status.as_u16() == 400 || status.as_u16() == 401;
            self.failures.lock().await.insert(
                home.to_owned(),
                (
                    fingerprint,
                    if permanent {
                        u64::MAX
                    } else {
                        now().saturating_add(retry)
                    },
                ),
            );
            bail!(
                "Claude refresh rejected (HTTP {}); {}",
                status.as_u16(),
                if permanent {
                    "log in again"
                } else {
                    "retry after cooldown"
                }
            );
        }
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("invalid Claude refresh JSON"))?;
        current.access_token = value["access_token"]
            .as_str()
            .context("refresh omitted access token")?
            .to_owned();
        if let Some(refresh) = value["refresh_token"].as_str().filter(|s| !s.is_empty()) {
            current.refresh_token = refresh.to_owned()
        }
        let lifetime = value["expires_in"]
            .as_u64()
            .filter(|n| *n > 0)
            .context("refresh omitted expiry")?;
        current.expires_at = now()
            .checked_add(lifetime)
            .context("invalid refresh expiry")?;
        current.persist(home)?;
        self.failures.lock().await.remove(home);
        Ok(current)
    }
}

/// Login uses a dedicated native profile only for the official login ceremony. The relay
/// owns the imported grant afterwards; running Claude in this profile would race refresh.
pub fn login(home: &Path) -> Result<()> {
    fs::create_dir_all(home)?;
    let _guard = HomeAuthLock::acquire(home)?;
    ensure!(
        login_command(home)?
            .status()
            .context("launch official Claude login")?
            .success(),
        "Claude login did not complete"
    );
    complete_login(home)
}

pub(crate) fn executable() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("CLAUDE_EXECUTABLE") {
        let path = PathBuf::from(path);
        ensure!(
            path.is_absolute() && path.is_file(),
            "CLAUDE_EXECUTABLE must name an absolute executable path"
        );
        return Ok(path);
    }
    let mut paths = std::env::var_os("PATH")
        .map(|p| {
            std::env::split_paths(&p)
                .map(|p| p.join("claude"))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if let Some(home) = std::env::var_os("HOME") {
        paths.push(PathBuf::from(home).join(".local/bin/claude"));
    }
    paths.extend([
        PathBuf::from("/opt/homebrew/bin/claude"),
        PathBuf::from("/usr/local/bin/claude"),
    ]);
    paths
        .into_iter()
        .find(|p| p.is_file())
        .context("Claude Code is not installed; set CLAUDE_EXECUTABLE")
}

pub(crate) fn clean_command() -> Result<Command> {
    let mut command = Command::new(executable()?);
    for key in [
        "ANTHROPIC_BASE_URL",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "ANTHROPIC_CUSTOM_HEADERS",
        "CLAUDE_CODE_OAUTH_TOKEN",
        "CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR",
        "CLAUDE_CODE_API_KEY_FILE_DESCRIPTOR",
        "ANTHROPIC_PROFILE",
        "CLAUDE_CODE_USE_BEDROCK",
        "CLAUDE_CODE_USE_VERTEX",
        "CLAUDE_CODE_USE_FOUNDRY",
    ] {
        command.env_remove(key);
    }
    Ok(command)
}

pub(crate) fn login_command(home: &Path) -> Result<Command> {
    let native = home.join("native-login");
    fs::create_dir_all(&native)?;
    let native = fs::canonicalize(native)?;
    let mut command = clean_command()?;
    command
        .args(["auth", "login"])
        .current_dir(&native)
        .env("CLAUDE_CONFIG_DIR", &native)
        .env("CLAUDE_SECURESTORAGE_CONFIG_DIR", &native);
    Ok(command)
}

pub(crate) fn complete_login(home: &Path) -> Result<()> {
    let native = fs::canonicalize(home.join("native-login"))?;
    let credential_bytes = read_native_credentials(&native)?;
    let settings =
        fs::read(native.join(".claude.json")).context("native login metadata unavailable")?;
    import_native(home, &credential_bytes, &settings)
}

fn read_native_credentials(native: &Path) -> Result<Vec<u8>> {
    #[cfg(target_os = "macos")]
    {
        use sha2::{Digest, Sha256};
        let digest = format!("{:x}", Sha256::digest(native.to_string_lossy().as_bytes()));
        let service = format!("Claude Code-credentials-{}", &digest[..8]);
        let output = Command::new("/usr/bin/security")
            .args(["find-generic-password", "-s", &service, "-w"])
            .output()
            .context("read isolated Claude Keychain item")?;
        if output.status.success() {
            return Ok(output.stdout);
        }
    }
    fs::read(native.join(".credentials.json"))
        .context("isolated Claude login credentials unavailable")
}

fn import_native(home: &Path, credentials: &[u8], settings: &[u8]) -> Result<()> {
    let credentials: Value = serde_json::from_slice(credentials)
        .map_err(|_| anyhow::anyhow!("invalid native credentials"))?;
    let settings: Value =
        serde_json::from_slice(settings).map_err(|_| anyhow::anyhow!("invalid native settings"))?;
    let token = &credentials["claudeAiOauth"];
    let account = &settings["oauthAccount"];
    Credential {
        access_token: token["accessToken"]
            .as_str()
            .context("native access token unavailable")?
            .to_owned(),
        refresh_token: token["refreshToken"]
            .as_str()
            .context("native refresh token unavailable")?
            .to_owned(),
        expires_at: token["expiresAt"]
            .as_u64()
            .context("native expiry unavailable")?
            / 1000,
        account_uuid: account["accountUuid"]
            .as_str()
            .context("native account identity unavailable")?
            .to_owned(),
        organization_uuid: account["organizationUuid"]
            .as_str()
            .context("native organization identity unavailable")?
            .to_owned(),
        device_id: settings["userID"]
            .as_str()
            .context("native device identity unavailable")?
            .to_owned(),
    }
    .persist(home)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::{Response, body::Incoming, service::service_fn};
    use hyper_util::rt::TokioIo;
    use std::{
        convert::Infallible,
        sync::atomic::{AtomicUsize, Ordering},
    };

    fn credential() -> Credential {
        Credential {
            access_token: "sk-ant-oat01-old".into(),
            refresh_token: "synthetic-refresh".into(),
            expires_at: now() - 1,
            account_uuid: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".into(),
            organization_uuid: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb".into(),
            device_id: "a".repeat(64),
        }
    }

    #[test]
    fn native_import_requires_identity_and_writes_private_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let c = credential();
        let tokens = serde_json::to_vec(&json!({"claudeAiOauth":{"accessToken":c.access_token,"refreshToken":c.refresh_token,"expiresAt":(now()+3600)*1000}})).unwrap();
        assert!(import_native(dir.path(), &tokens, b"{}").is_err());
        assert!(!dir.path().join("claude-auth.json").exists());
        let metadata = serde_json::to_vec(&json!({"oauthAccount":{"accountUuid":c.account_uuid,"organizationUuid":c.organization_uuid},"userID":c.device_id})).unwrap();
        import_native(dir.path(), &tokens, &metadata).unwrap();
        let stored = read(dir.path()).unwrap();
        assert_eq!(stored.account_uuid, c.account_uuid);
        assert_eq!(stored.device_id, c.device_id);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(dir.path().join("claude-auth.json"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        let previous = fs::read(dir.path().join("claude-auth.json")).unwrap();
        assert!(import_native(dir.path(), b"malformed", &metadata).is_err());
        assert_eq!(
            fs::read(dir.path().join("claude-auth.json")).unwrap(),
            previous
        );
    }

    async fn refresh_server(
        home: &Path,
        status: u16,
    ) -> (Resolver, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/token", listener.local_addr().unwrap());
        let count = Arc::new(AtomicUsize::new(0));
        let seen = count.clone();
        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let seen = seen.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let seen = seen.clone();
                        async move {
                            assert_eq!(request.uri().path(), "/token");
                            let bytes = request.into_body().collect().await.unwrap().to_bytes();
                            let body: Value = serde_json::from_slice(&bytes).unwrap();
                            assert_eq!(
                                body,
                                json!({"grant_type":"refresh_token","refresh_token":"synthetic-refresh","client_id":CLIENT_ID})
                            );
                            seen.fetch_add(1, Ordering::SeqCst);
                            let response=Response::builder().status(status).header("retry-after","3600").body(Full::new(Bytes::from_static(br#"{"access_token":"sk-ant-oat01-new","refresh_token":"synthetic-rotated","expires_in":3600}"#))).unwrap();
                            Ok::<_, Infallible>(response)
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        let lock = Arc::new(Mutex::new(()));
        let resolver = Resolver {
            locks: HashMap::from([(home.to_owned(), lock)]),
            failures: Arc::new(Mutex::new(HashMap::new())),
            client: Client::builder(TokioExecutor::new()).build(connector),
            token_url: url.parse().unwrap(),
        };
        (resolver, count, task)
    }

    #[tokio::test]
    async fn concurrent_refresh_rotates_once_and_preserves_native_identity() {
        let dir = tempfile::tempdir().unwrap();
        let before = credential();
        before.persist(dir.path()).unwrap();
        let (resolver, count, task) = refresh_server(dir.path(), 200).await;
        let (one, two) = tokio::join!(
            resolver.resolve(dir.path(), Some(&before.access_token)),
            resolver.resolve(dir.path(), Some(&before.access_token))
        );
        assert_eq!(one.unwrap().access_token, "sk-ant-oat01-new");
        assert_eq!(two.unwrap().access_token, "sk-ant-oat01-new");
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let after = read(dir.path()).unwrap();
        assert_eq!(after.refresh_token, "synthetic-rotated");
        assert_eq!(after.account_uuid, before.account_uuid);
        assert_eq!(after.device_id, before.device_id);
        assert_eq!(after.organization_uuid, before.organization_uuid);
        task.abort();
    }

    #[tokio::test]
    async fn refresh_failures_respect_cooldown_and_distinguish_login_required() {
        for (status, permanent) in [(400, true), (401, true), (429, false), (503, false)] {
            let dir = tempfile::tempdir().unwrap();
            credential().persist(dir.path()).unwrap();
            let original = fs::read(dir.path().join("claude-auth.json")).unwrap();
            let (resolver, count, task) = refresh_server(dir.path(), status).await;
            for _ in 0..2 {
                assert!(resolver.resolve(dir.path(), None).await.is_err());
            }
            assert_eq!(count.load(Ordering::SeqCst), 1);
            assert_eq!(resolver.needs_login(dir.path()).await, permanent);
            assert_eq!(
                fs::read(dir.path().join("claude-auth.json")).unwrap(),
                original
            );
            task.abort();
        }
    }
}
