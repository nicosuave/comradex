//! Authenticated local control channel for routing changes that must not restart the daemon.

use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs,
    future::Future,
    io::{BufRead, BufReader, Read, Write},
    os::unix::{
        fs::{FileTypeExt, PermissionsExt},
        net::UnixStream as StdUnixStream,
    },
    path::{Path, PathBuf},
    pin::Pin,
    process::Stdio,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader as AsyncBufReader},
    net::{UnixListener, UnixStream},
    process::Command,
    sync::{Mutex, Notify, Semaphore},
};
use tracing::warn;

use crate::{
    accounts,
    auth_lock::HomeAuthLock,
    config::{self, Config},
    routing::{Router, RoutingSnapshot},
    state::{Stats, StatsSnapshot},
};

const SOCKET_NAME: &str = "control.sock";
const MAX_MESSAGE_BYTES: usize = 16 * 1024;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONTROL_CLIENTS: usize = 32;
const MAX_LOGIN_OUTPUT_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AccountRole {
    Preferred,
    Normal,
    Preserved,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum Request {
    SetPreferred {
        secret: String,
        pool: String,
        account: Option<String>,
    },
    SetPreserved {
        secret: String,
        pool: String,
        account: Option<String>,
    },
    RoutingStatus {
        secret: String,
    },
    UiStatus,
    UiRefreshUsage,
    UiSetAccountRole {
        pool: String,
        account: String,
        role: AccountRole,
    },
    UiSetPreferred {
        pool: String,
        account: Option<String>,
    },
    UiStartLogin {
        account: String,
    },
    UiConnectExistingLogin {
        account: String,
        #[serde(default)]
        codex_home: Option<PathBuf>,
    },
    UiLoginStatus {
        session_id: String,
    },
}

impl Request {
    fn secret(&self) -> Option<&str> {
        match self {
            Self::SetPreferred { secret, .. }
            | Self::SetPreserved { secret, .. }
            | Self::RoutingStatus { secret } => Some(secret),
            Self::UiStatus
            | Self::UiRefreshUsage
            | Self::UiSetAccountRole { .. }
            | Self::UiSetPreferred { .. }
            | Self::UiStartLogin { .. }
            | Self::UiConnectExistingLogin { .. }
            | Self::UiLoginStatus { .. } => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Response {
    ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    routing: Option<RoutingSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status: Option<UiStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    login: Option<UiLoginStatus>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UiStatus {
    pub daemon_running: bool,
    pub accounts: Vec<UiAccountStatus>,
    pub pools: Vec<UiPoolStatus>,
    pub routing: RoutingSnapshot,
    pub traffic: StatsSnapshot,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UiAccountStatus {
    #[serde(default)]
    pub reauth_required: bool,
    pub name: String,
    pub kind: UiAccountKind,
    pub signed_in: bool,
    pub auth_state: UiAccountAuthState,
    pub pools: Vec<String>,
    pub available: bool,
    pub unavailable_reason: Option<String>,
    pub retry_at_unix: Option<i64>,
    pub usage_percent: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_updated_at_unix: Option<i64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub usage_windows: BTreeMap<String, crate::routing::QuotaWindowStatus>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiAccountKind {
    Inbound,
    CodexHome,
    ClaudeHome,
    ClaudeInbound,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiAccountAuthState {
    Inbound,
    SignedIn,
    SignedOut,
    LoginInProgress,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UiPoolStatus {
    pub name: String,
    pub members: Vec<String>,
    pub preferred: Option<String>,
    pub preserved: Option<String>,
    pub active: Option<String>,
    /// Last account actually wired to upstream for this pool. `active` is only the last fresh
    /// pick and never reflects bound/select_exact traffic; `wired` is recorded after
    /// revalidation immediately before send, so UI readers must not conflate the two.
    #[serde(default)]
    pub wired: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UiLoginStatus {
    #[serde(default)]
    pub provider: String,
    pub session_id: String,
    pub account: String,
    pub state: UiLoginState,
    pub verification_uri: Option<String>,
    pub user_code: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiLoginState {
    Running,
    Succeeded,
    Failed,
}

#[derive(Default)]
struct BoundedLoginOutput {
    bytes: Vec<u8>,
    truncated: bool,
}

impl BoundedLoginOutput {
    fn append(&mut self, bytes: &[u8]) {
        let remaining = MAX_LOGIN_OUTPUT_BYTES.saturating_sub(self.bytes.len());
        self.bytes
            .extend_from_slice(&bytes[..bytes.len().min(remaining)]);
        self.truncated |= bytes.len() > remaining;
    }

    fn allowed_fields(&self) -> (Option<String>, Option<String>) {
        // Codex emits SGR colors even to piped stdout. Normalize the accumulated
        // bytes so an escape split across reads is handled on the next poll too.
        let mut plain = Vec::with_capacity(self.bytes.len());
        let mut remaining = self.bytes.as_slice();
        while let Some((&byte, rest)) = remaining.split_first() {
            if let Some(parameters) = remaining.strip_prefix(b"\x1b[") {
                let length = parameters
                    .iter()
                    .take_while(|byte| byte.is_ascii_digit() || **byte == b';')
                    .count();
                if parameters.get(length) == Some(&b'm') {
                    remaining = &parameters[length + 1..];
                    continue;
                }
            }
            plain.push(byte);
            remaining = rest;
        }
        let text = String::from_utf8_lossy(&plain);
        // A live read may end halfway through a code (or its trailing reset).
        // Codex terminates prompt lines with newlines; expose only complete tokens.
        let text = text
            .rfind(char::is_whitespace)
            .map_or("", |end| &text[..end]);
        let verification_uri = text.split_whitespace().find_map(|token| {
            let token = token.trim_matches(|character: char| {
                matches!(
                    character,
                    '(' | ')' | '[' | ']' | '<' | '>' | ',' | '"' | '\''
                )
            });
            (token == "https://auth.openai.com/codex/device"
                || token.parse::<hyper::Uri>().is_ok_and(|uri| {
                    uri.scheme_str() == Some("https")
                        && uri.authority().is_some_and(|a| a.as_str() == "claude.ai")
                        && uri.path() == "/oauth/authorize"
                        && uri.query().is_some()
                }))
            .then(|| token.to_owned())
        });
        let user_code = text.split_whitespace().find_map(|token| {
            let token = token.trim_matches(|character: char| {
                !character.is_ascii_alphanumeric() && character != '-'
            });
            let valid = (6..=20).contains(&token.len())
                && token.contains('-')
                && token.chars().all(|character| {
                    character.is_ascii_uppercase() || character.is_ascii_digit() || character == '-'
                })
                && token
                    .chars()
                    .any(|character| character.is_ascii_uppercase());
            valid.then(|| token.to_owned())
        });
        (verification_uri, user_code)
    }
}

type SharedLoginOutput = Arc<StdMutex<BoundedLoginOutput>>;
type LoginFuture = Pin<Box<dyn Future<Output = Result<bool>> + Send>>;

trait LoginRunner: Send + Sync {
    fn run(&self, home: PathBuf, output: SharedLoginOutput, claude: bool) -> LoginFuture;
}

struct SystemLoginRunner;

impl LoginRunner for SystemLoginRunner {
    fn run(&self, home: PathBuf, output: SharedLoginOutput, claude: bool) -> LoginFuture {
        Box::pin(async move {
            if claude {
                run_claude_login(home, output).await
            } else {
                run_codex_login(home, output).await
            }
        })
    }
}

#[derive(Clone)]
struct LoginSession {
    claude: bool,
    account: String,
    state: UiLoginState,
    output: SharedLoginOutput,
    error: Option<String>,
    abort: Option<tokio::task::AbortHandle>,
}

#[derive(Clone)]
struct LoginManager {
    sessions: Arc<StdMutex<BTreeMap<String, LoginSession>>>,
    runner: Arc<dyn LoginRunner>,
    router: Arc<Router>,
}

impl LoginManager {
    fn new(runner: Arc<dyn LoginRunner>, router: Arc<Router>) -> Self {
        Self {
            sessions: Arc::new(StdMutex::new(BTreeMap::new())),
            runner,
            router,
        }
    }

    async fn start(&self, account: String, home: PathBuf, claude: bool) -> Result<UiLoginStatus> {
        const MAX_LOGIN_SESSIONS: usize = 16;
        {
            let mut sessions = self.sessions.lock().expect("login sessions mutex poisoned");
            if sessions
                .values()
                .any(|session| session.account == account && session.state == UiLoginState::Running)
            {
                bail!("login is already running for account {account}")
            }
            while sessions.len() >= MAX_LOGIN_SESSIONS {
                let completed = sessions
                    .iter()
                    .find(|(_, session)| session.state != UiLoginState::Running)
                    .map(|(session_id, _)| session_id.clone())
                    .context("too many login sessions are running")?;
                sessions.remove(&completed);
            }
        }
        if !self.router.begin_login(&account).await {
            bail!("account {account} is unavailable for login")
        }

        let session_id = format!("{:032x}", rand::random::<u128>());
        let output = Arc::new(StdMutex::new(BoundedLoginOutput::default()));
        {
            let mut sessions = self.sessions.lock().expect("login sessions mutex poisoned");
            sessions.insert(
                session_id.clone(),
                LoginSession {
                    claude,
                    account: account.clone(),
                    state: UiLoginState::Running,
                    output: output.clone(),
                    error: None,
                    abort: None,
                },
            );
        }

        let manager = self.clone();
        let task_account = account.clone();
        let task_session_id = session_id.clone();
        let task = tokio::spawn(async move {
            let result: Result<bool> = async {
                let _auth_lock = HomeAuthLock::acquire_async(&home).await?;
                manager.runner.run(home, output.clone(), claude).await
            }
            .await;
            let state = match &result {
                Ok(true) => UiLoginState::Succeeded,
                Ok(false) | Err(_) => UiLoginState::Failed,
            };
            manager
                .router
                .finish_login(&task_account, state == UiLoginState::Succeeded)
                .await;
            if let Some(session) = manager
                .sessions
                .lock()
                .expect("login sessions mutex poisoned")
                .get_mut(&task_session_id)
            {
                session.state = state;
                session.abort = None;
                session.error = match &result {
                    Ok(false) => Some(
                        if claude {
                            "claude_login_failed"
                        } else {
                            "codex_login_failed"
                        }
                        .to_owned(),
                    ),
                    Err(_) => Some(
                        if claude {
                            "claude_login_unavailable"
                        } else {
                            "codex_login_unavailable"
                        }
                        .to_owned(),
                    ),
                    Ok(true) => None,
                };
            }
        });
        if let Some(session) = self
            .sessions
            .lock()
            .expect("login sessions mutex poisoned")
            .get_mut(&session_id)
        {
            session.abort = Some(task.abort_handle());
        }

        Ok(UiLoginStatus {
            provider: if claude { "claude" } else { "codex" }.into(),
            session_id,
            account,
            state: UiLoginState::Running,
            verification_uri: None,
            user_code: None,
            error: None,
        })
    }

    fn status(&self, session_id: &str) -> Result<UiLoginStatus> {
        let session = self
            .sessions
            .lock()
            .expect("login sessions mutex poisoned")
            .get(session_id)
            .cloned()
            .context("unknown login session")?;
        let (verification_uri, user_code) = session
            .output
            .lock()
            .expect("login output mutex poisoned")
            .allowed_fields();
        Ok(UiLoginStatus {
            provider: if session.claude { "claude" } else { "codex" }.into(),
            session_id: session_id.to_owned(),
            account: session.account,
            state: session.state,
            verification_uri,
            user_code,
            error: session.error,
        })
    }

    fn account_in_progress(&self, account: &str) -> bool {
        self.sessions
            .lock()
            .expect("login sessions mutex poisoned")
            .values()
            .any(|session| session.account == account && session.state == UiLoginState::Running)
    }

    fn abort_all(&self) {
        for session in self
            .sessions
            .lock()
            .expect("login sessions mutex poisoned")
            .values()
        {
            if let Some(abort) = &session.abort {
                abort.abort();
            }
        }
    }
}

async fn run_codex_login(home: PathBuf, output: SharedLoginOutput) -> Result<bool> {
    let executable = resolve_codex_executable()?;
    let mut child = Command::new(&executable)
        .arg("login")
        .arg("--device-auth")
        .env("CODEX_HOME", &home)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("launch {} login", executable.display()))?;
    let stdout = child.stdout.take().context("capture codex login stdout")?;
    let stderr = child.stderr.take().context("capture codex login stderr")?;
    let stdout_task = tokio::spawn(capture_login_output(stdout, output.clone()));
    let stderr_task = tokio::spawn(capture_login_output(stderr, output));
    let status = child.wait().await.context("wait for codex login")?;
    stdout_task
        .await
        .context("join codex login stdout reader")??;
    stderr_task
        .await
        .context("join codex login stderr reader")??;
    Ok(status.success())
}

async fn run_claude_login(home: PathBuf, output: SharedLoginOutput) -> Result<bool> {
    let mut command = Command::from(crate::claude::auth::login_command(&home)?);
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("launch Claude login")?;
    let stdout = child.stdout.take().context("capture Claude login stdout")?;
    let stderr = child.stderr.take().context("capture Claude login stderr")?;
    let capture = async {
        tokio::try_join!(
            capture_login_output(stdout, output.clone()),
            capture_login_output(stderr, output)
        )?;
        Ok::<_, anyhow::Error>(())
    };
    let (status, ()) = tokio::try_join!(
        async { child.wait().await.map_err(anyhow::Error::from) },
        capture
    )?;
    if !status.success() {
        return Ok(false);
    }
    tokio::task::spawn_blocking(move || crate::claude::auth::complete_login(&home)).await??;
    Ok(true)
}

async fn capture_login_output(
    mut reader: impl AsyncRead + Unpin,
    output: SharedLoginOutput,
) -> Result<()> {
    let mut bytes = [0u8; 1024];
    loop {
        let read = reader.read(&mut bytes).await?;
        if read == 0 {
            return Ok(());
        }
        output
            .lock()
            .expect("login output mutex poisoned")
            .append(&bytes[..read]);
    }
}

fn resolve_codex_executable() -> Result<PathBuf> {
    resolve_codex_executable_from(
        std::env::var_os("CODEX_EXECUTABLE"),
        std::env::var_os("HOME"),
        std::env::var_os("PATH"),
    )
}

fn resolve_codex_executable_from(
    configured: Option<OsString>,
    home: Option<OsString>,
    path: Option<OsString>,
) -> Result<PathBuf> {
    if let Some(configured) = configured.filter(|value| !value.is_empty()) {
        let configured = PathBuf::from(configured);
        if configured.components().count() > 1 {
            if configured.is_file() {
                return Ok(configured);
            }
            bail!("CODEX_EXECUTABLE does not point to a file")
        }
        if let Some(found) = find_in_path(configured.as_os_str(), path.as_deref()) {
            return Ok(found);
        }
        bail!("CODEX_EXECUTABLE was not found on PATH")
    }

    let mut candidates = vec![
        PathBuf::from("/opt/homebrew/bin/codex"),
        PathBuf::from("/usr/local/bin/codex"),
    ];
    if let Some(home) = home.filter(|value| !value.is_empty()) {
        let home = PathBuf::from(home);
        candidates.push(home.join(".local/bin/codex"));
        candidates.push(home.join(".bun/bin/codex"));
    }
    if let Some(candidate) = candidates.into_iter().find(|candidate| candidate.is_file()) {
        return Ok(candidate);
    }
    find_in_path(OsStr::new("codex"), path.as_deref())
        .context("could not find codex; set CODEX_EXECUTABLE")
}

fn find_in_path(executable: &OsStr, path: Option<&OsStr>) -> Option<PathBuf> {
    std::env::split_paths(path?)
        .map(|directory| directory.join(executable))
        .find(|candidate| candidate.is_file())
}

struct SocketGuard(PathBuf);

impl Drop for SocketGuard {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.0)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            warn!(path = %self.0.display(), %error, "failed to remove control socket");
        }
    }
}

struct ConfigChanges {
    edit_lock: Mutex<()>,
    reload: Arc<Notify>,
    usage_refresh: Arc<Notify>,
}

pub struct ControlServer {
    listener: UnixListener,
    _guard: SocketGuard,
    config_path: PathBuf,
    config: Arc<Config>,
    router: Arc<Router>,
    stats: Arc<Stats>,
    login_manager: LoginManager,
    changes: Arc<ConfigChanges>,
    clients: Arc<Semaphore>,
}

pub fn socket_path(state_dir: &Path) -> PathBuf {
    state_dir.join(SOCKET_NAME)
}

impl ControlServer {
    pub fn bind(
        state_dir: &Path,
        config_path: PathBuf,
        config: Arc<Config>,
        router: Arc<Router>,
        stats: Arc<Stats>,
    ) -> Result<Self> {
        fs::set_permissions(state_dir, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("secure state directory {}", state_dir.display()))?;
        let path = socket_path(state_dir);
        remove_stale_socket(&path)?;
        let listener = UnixListener::bind(&path)
            .with_context(|| format!("bind control socket {}", path.display()))?;
        let guard = SocketGuard(path.clone());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("secure control socket {}", path.display()))?;
        let login_manager = LoginManager::new(Arc::new(SystemLoginRunner), router.clone());
        Ok(Self {
            listener,
            _guard: guard,
            config_path,
            config,
            router,
            stats,
            login_manager,
            changes: Arc::new(ConfigChanges {
                edit_lock: Mutex::new(()),
                reload: Arc::new(Notify::new()),
                usage_refresh: Arc::new(Notify::new()),
            }),
            clients: Arc::new(Semaphore::new(MAX_CONTROL_CLIENTS)),
        })
    }

    pub fn reload_requested(&self) -> Arc<Notify> {
        self.changes.reload.clone()
    }

    pub fn usage_refresh_requested(&self) -> Arc<Notify> {
        self.changes.usage_refresh.clone()
    }

    pub async fn run(self) -> Result<()> {
        struct LoginShutdownGuard(LoginManager);
        impl Drop for LoginShutdownGuard {
            fn drop(&mut self) {
                self.0.abort_all();
            }
        }
        let _login_shutdown = LoginShutdownGuard(self.login_manager.clone());
        let mut handlers = tokio::task::JoinSet::new();
        loop {
            let (stream, _) = tokio::select! {
                connection = self.listener.accept() => connection?,
                Some(_) = handlers.join_next(), if !handlers.is_empty() => continue,
            };
            let Ok(permit) = self.clients.clone().try_acquire_owned() else {
                continue;
            };
            let config_path = self.config_path.clone();
            let config = self.config.clone();
            let router = self.router.clone();
            let stats = self.stats.clone();
            let login_manager = self.login_manager.clone();
            let changes = self.changes.clone();
            handlers.spawn(async move {
                let _permit = permit;
                if let Err(error) = handle(
                    stream,
                    config_path,
                    config,
                    router,
                    stats,
                    login_manager,
                    changes,
                )
                .await
                {
                    warn!(%error, "control request failed");
                }
            });
        }
    }
}

fn remove_stale_socket(path: &Path) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).with_context(|| format!("inspect {}", path.display())),
    };
    if !metadata.file_type().is_socket() {
        bail!(
            "refusing to replace non-socket control path {}",
            path.display()
        )
    }
    if StdUnixStream::connect(path).is_ok() {
        bail!(
            "another Comradex daemon is already listening at {}",
            path.display()
        )
    }
    fs::remove_file(path).with_context(|| format!("remove stale socket {}", path.display()))
}

async fn handle(
    stream: UnixStream,
    config_path: PathBuf,
    config: Arc<Config>,
    router: Arc<Router>,
    stats: Arc<Stats>,
    login_manager: LoginManager,
    changes: Arc<ConfigChanges>,
) -> Result<()> {
    if !peer_is_current_user(&stream)? {
        bail!("control peer is not the daemon user")
    }
    let (reader, mut writer) = stream.into_split();
    let mut reader = AsyncBufReader::new(reader).take((MAX_MESSAGE_BYTES + 1) as u64);
    let mut bytes = Vec::new();
    let read = tokio::time::timeout(CLIENT_TIMEOUT, reader.read_until(b'\n', &mut bytes))
        .await
        .context("control request timed out")?
        .context("read control request")?;
    let mut reload_after_response = false;
    let response = if read == 0 || bytes.len() > MAX_MESSAGE_BYTES || !bytes.ends_with(b"\n") {
        Response {
            ok: false,
            error: Some("invalid control request framing".into()),
            routing: None,
            status: None,
            login: None,
        }
    } else {
        match serde_json::from_slice::<Request>(&bytes) {
            Ok(request) => {
                if matches!(request, Request::UiRefreshUsage) {
                    changes.usage_refresh.notify_one();
                }
                reload_after_response = matches!(request, Request::UiConnectExistingLogin { .. });
                process(
                    request,
                    &config_path,
                    &config,
                    &router,
                    &stats,
                    &login_manager,
                    &changes.edit_lock,
                )
                .await
            }
            Err(error) => Response {
                ok: false,
                error: Some(format!("invalid control request: {error}")),
                routing: None,
                status: None,
                login: None,
            },
        }
    };
    let reload_after_response = reload_after_response && response.ok;
    let mut encoded = serde_json::to_vec(&response)?;
    if encoded.len() > MAX_RESPONSE_BYTES {
        encoded = serde_json::to_vec(&Response::error("control response is too large"))?;
    }
    encoded.push(b'\n');
    let sent = tokio::time::timeout(RESPONSE_TIMEOUT, async {
        writer.write_all(&encoded).await?;
        writer.shutdown().await
    })
    .await
    .context("control response timed out");
    // The config is already persisted. Reload even if the client disconnected while
    // receiving its acknowledgement, but never tear down a successful response early.
    if reload_after_response {
        changes.reload.notify_one();
    }
    sent??;
    Ok(())
}

async fn process(
    request: Request,
    config_path: &Path,
    config: &Config,
    router: &Router,
    stats: &Stats,
    login_manager: &LoginManager,
    edit_lock: &Mutex<()>,
) -> Response {
    if let Some(secret) = request.secret()
        && !secrets_equal(secret, &config.proxy.installation_secret)
    {
        return Response::error("unauthorized");
    }
    let result: Result<Response> = async {
        Ok(match request {
            Request::RoutingStatus { .. } => Response::routing(router.routing_snapshot().await),
            Request::SetPreferred { pool, account, .. }
            | Request::UiSetPreferred { pool, account } => {
                update_preferred(config_path, config, router, edit_lock, &pool, account).await?;
                Response::routing(router.routing_snapshot().await)
            }
            Request::SetPreserved { pool, account, .. } => {
                update_preserved(config_path, config, router, edit_lock, &pool, account).await?;
                Response::routing(router.routing_snapshot().await)
            }
            Request::UiSetAccountRole {
                pool,
                account,
                role,
            } => {
                update_account_role(
                    config_path,
                    config,
                    router,
                    edit_lock,
                    &pool,
                    &account,
                    role,
                )
                .await?;
                Response::status(build_ui_status(config, router, stats, login_manager).await)
            }
            Request::UiStatus | Request::UiRefreshUsage => {
                Response::status(build_ui_status(config, router, stats, login_manager).await)
            }
            Request::UiStartLogin { account } => {
                let home = managed_account_home(config, &account)?.to_owned();
                let claude = config.accounts[&account].is_claude();
                Response::login(login_manager.start(account, home, claude).await?)
            }
            Request::UiConnectExistingLogin {
                account,
                codex_home,
            } => {
                let home = match codex_home {
                    Some(home) => home,
                    None => accounts::existing_codex_home()?,
                };
                connect_existing_login(config_path, edit_lock, &account, &home).await?;
                Response::routing(router.routing_snapshot().await)
            }
            Request::UiLoginStatus { session_id } => {
                Response::login(login_manager.status(&session_id)?)
            }
        })
    }
    .await;
    match result {
        Ok(response) => response,
        Err(error) => Response::error(format!("{error:#}")),
    }
}

impl Response {
    fn error(error: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: Some(error.into()),
            routing: None,
            status: None,
            login: None,
        }
    }

    fn routing(routing: RoutingSnapshot) -> Self {
        Self {
            ok: true,
            error: None,
            routing: Some(routing),
            status: None,
            login: None,
        }
    }

    fn status(status: UiStatus) -> Self {
        Self {
            ok: true,
            error: None,
            routing: None,
            status: Some(status),
            login: None,
        }
    }

    fn login(login: UiLoginStatus) -> Self {
        Self {
            ok: true,
            error: None,
            routing: None,
            status: None,
            login: Some(login),
        }
    }
}

async fn update_preferred(
    config_path: &Path,
    config: &Config,
    router: &Router,
    edit_lock: &Mutex<()>,
    pool: &str,
    account: Option<String>,
) -> Result<()> {
    let pool_config = config
        .pools
        .get(pool)
        .with_context(|| format!("unknown pool {pool}"))?;
    if let Some(account) = &account
        && !pool_config.members.contains(account)
    {
        bail!("account {account} is not a member of pool {pool}")
    }
    let _guard = edit_lock.lock().await;
    let text = fs::read_to_string(config_path)
        .with_context(|| format!("read {}", config_path.display()))?;
    let updated = accounts::set_preferred_account(&text, pool, account.as_deref())?;
    config::write_validated(config_path, &updated)?;
    router.set_preferred(pool, account).await;
    Ok(())
}

// Change one account's role against the latest saved settings, never a stale UI snapshot.
async fn update_account_role(
    config_path: &Path,
    config: &Config,
    router: &Router,
    edit_lock: &Mutex<()>,
    pool: &str,
    account: &str,
    role: AccountRole,
) -> Result<()> {
    let pool_config = config.pools.get(pool).context("unknown pool")?;
    if !pool_config.members.iter().any(|member| member == account) {
        bail!("account {account} is not a member of pool {pool}")
    }
    let _guard = edit_lock.lock().await;
    let mut text = fs::read_to_string(config_path)?;
    let saved: Config = toml::from_str(&text)?;
    let saved_pool = saved.pools.get(pool).context("unknown pool")?;
    if !saved_pool.members.iter().any(|member| member == account) {
        bail!("account {account} is not a member of pool {pool}")
    }
    if saved_pool.preferred.as_deref() == Some(account) {
        text = accounts::set_preferred_account(&text, pool, None)?;
    }
    if saved_pool.preserved.as_deref() == Some(account) {
        text = accounts::set_preserved_account(&text, pool, None)?;
    }
    text = match role {
        AccountRole::Preferred => accounts::set_preferred_account(&text, pool, Some(account))?,
        AccountRole::Preserved => accounts::set_preserved_account(&text, pool, Some(account))?,
        AccountRole::Normal => text,
    };
    let updated: Config = toml::from_str(&text)?;
    let order = &updated.pools[pool];
    config::write_validated(config_path, &text)?;
    router
        .set_account_order(pool, order.preferred.clone(), order.preserved.clone())
        .await;
    Ok(())
}

async fn update_preserved(
    config_path: &Path,
    config: &Config,
    router: &Router,
    edit_lock: &Mutex<()>,
    pool: &str,
    account: Option<String>,
) -> Result<()> {
    let pool_config = config
        .pools
        .get(pool)
        .with_context(|| format!("unknown pool {pool}"))?;
    if let Some(account) = &account
        && !pool_config.members.contains(account)
    {
        bail!("account {account} is not a member of pool {pool}")
    }
    let _guard = edit_lock.lock().await;
    let text = fs::read_to_string(config_path)
        .with_context(|| format!("read {}", config_path.display()))?;
    let updated = accounts::set_preserved_account(&text, pool, account.as_deref())?;
    config::write_validated(config_path, &updated)?;
    router.set_preserved(pool, account).await;
    Ok(())
}

async fn connect_existing_login(
    config_path: &Path,
    edit_lock: &Mutex<()>,
    account: &str,
    home: &Path,
) -> Result<()> {
    let _guard = edit_lock.lock().await;
    let text = fs::read_to_string(config_path).context("read Comradex configuration")?;
    let updated = accounts::connect_existing_account(&text, account, home)?;
    config::write_validated(config_path, &updated)
}

async fn build_ui_status(
    config: &Config,
    router: &Router,
    stats: &Stats,
    login_manager: &LoginManager,
) -> UiStatus {
    let traffic = stats.snapshot(router).await;
    let routing = traffic.routing.clone();
    let accounts_needing_login = router.accounts_needing_login().await;
    let accounts = config
        .accounts
        .iter()
        .map(|(name, account)| {
            let pools = config
                .pools
                .iter()
                .filter(|(_, pool)| pool.members.iter().any(|member| member == name))
                .map(|(pool_name, _)| pool_name.clone())
                .collect();
            let (kind, signed_in, auth_state) = match account {
                crate::config::AccountConfig::ClaudeInbound => (
                    UiAccountKind::ClaudeInbound,
                    true,
                    UiAccountAuthState::Inbound,
                ),
                crate::config::AccountConfig::ClaudeHome { .. }
                    if login_manager.account_in_progress(name) =>
                {
                    (
                        UiAccountKind::ClaudeHome,
                        false,
                        UiAccountAuthState::LoginInProgress,
                    )
                }
                crate::config::AccountConfig::ClaudeHome { path } => {
                    let ready = crate::claude::auth::read(path).is_ok()
                        && !accounts_needing_login.contains(name);
                    (
                        UiAccountKind::ClaudeHome,
                        ready,
                        if ready {
                            UiAccountAuthState::SignedIn
                        } else {
                            UiAccountAuthState::SignedOut
                        },
                    )
                }
                crate::config::AccountConfig::Inbound => {
                    (UiAccountKind::Inbound, true, UiAccountAuthState::Inbound)
                }
                crate::config::AccountConfig::CodexHome { path }
                    if login_manager.account_in_progress(name) =>
                {
                    (
                        UiAccountKind::CodexHome,
                        path.join("auth.json").exists(),
                        UiAccountAuthState::LoginInProgress,
                    )
                }
                crate::config::AccountConfig::CodexHome { .. }
                    if accounts_needing_login.contains(name) =>
                {
                    (
                        UiAccountKind::CodexHome,
                        false,
                        UiAccountAuthState::SignedOut,
                    )
                }
                crate::config::AccountConfig::CodexHome { path }
                    if path.join("auth.json").exists() =>
                {
                    (UiAccountKind::CodexHome, true, UiAccountAuthState::SignedIn)
                }
                crate::config::AccountConfig::CodexHome { .. } => (
                    UiAccountKind::CodexHome,
                    false,
                    UiAccountAuthState::SignedOut,
                ),
            };
            UiAccountStatus {
                reauth_required: routing
                    .account_states
                    .get(name)
                    .is_some_and(|state| state.reauth_required),
                name: name.clone(),
                kind,
                signed_in,
                auth_state,
                pools,
                available: routing
                    .account_states
                    .get(name)
                    .is_none_or(|state| state.available),
                unavailable_reason: routing
                    .account_states
                    .get(name)
                    .and_then(|state| state.unavailable_reason.clone()),
                retry_at_unix: routing
                    .account_states
                    .get(name)
                    .and_then(|state| state.retry_at_unix),
                usage_percent: routing
                    .account_states
                    .get(name)
                    .and_then(|state| state.usage_percent),
                usage_updated_at_unix: routing
                    .account_states
                    .get(name)
                    .and_then(|state| state.usage_updated_at_unix),
                usage_windows: routing
                    .account_states
                    .get(name)
                    .map(|state| state.usage_windows.clone())
                    .unwrap_or_default(),
            }
        })
        .collect();
    let pools = config
        .pools
        .iter()
        .map(|(name, pool)| UiPoolStatus {
            name: name.clone(),
            members: pool.members.clone(),
            preferred: routing.preferred_accounts.get(name).cloned(),
            preserved: routing.preserved_accounts.get(name).cloned(),
            // Display honesty (fix1): `active` = last fresh pick only; `wired` = last
            // actually-sent account (bound traffic included). Never derive one from other.
            active: routing.active_accounts.get(name).cloned(),
            wired: routing.wired_accounts.get(name).cloned(),
        })
        .collect();
    UiStatus {
        daemon_running: true,
        accounts,
        pools,
        routing,
        traffic,
    }
}

fn managed_account_home<'a>(config: &'a Config, account: &str) -> Result<&'a Path> {
    match config
        .accounts
        .get(account)
        .with_context(|| format!("unknown account {account}"))?
    {
        crate::config::AccountConfig::ClaudeHome { path } => Ok(path),
        crate::config::AccountConfig::ClaudeInbound => {
            bail!("use `comradex account login` for Claude accounts")
        }
        crate::config::AccountConfig::Inbound => {
            bail!(
                "this account uses the requesting client's login; connect an existing Codex login first"
            )
        }
        crate::config::AccountConfig::CodexHome { path } => Ok(path),
    }
}

#[cfg(target_os = "macos")]
fn peer_is_current_user(stream: &UnixStream) -> Result<bool> {
    use std::os::fd::AsRawFd;

    let mut uid = 0;
    let mut gid = 0;
    if unsafe { libc::getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) } != 0 {
        return Err(std::io::Error::last_os_error()).context("inspect control peer credentials");
    }
    Ok(uid == unsafe { libc::geteuid() })
}

#[cfg(not(target_os = "macos"))]
fn peer_is_current_user(_stream: &UnixStream) -> Result<bool> {
    Ok(true)
}

fn secrets_equal(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        difference |= usize::from(*left.get(index).unwrap_or(&0) ^ *right.get(index).unwrap_or(&0));
    }
    difference == 0
}

pub fn set_preferred(
    state_dir: &Path,
    secret: &str,
    pool: &str,
    account: Option<&str>,
) -> Result<RoutingSnapshot> {
    send(
        state_dir,
        &Request::SetPreferred {
            secret: secret.to_owned(),
            pool: pool.to_owned(),
            account: account.map(str::to_owned),
        },
    )
}

pub fn set_preserved(
    state_dir: &Path,
    secret: &str,
    pool: &str,
    account: Option<&str>,
) -> Result<RoutingSnapshot> {
    send(
        state_dir,
        &Request::SetPreserved {
            secret: secret.to_owned(),
            pool: pool.to_owned(),
            account: account.map(str::to_owned),
        },
    )
}

pub fn routing_status(state_dir: &Path, secret: &str) -> Result<RoutingSnapshot> {
    send(
        state_dir,
        &Request::RoutingStatus {
            secret: secret.to_owned(),
        },
    )
}

fn send(state_dir: &Path, request: &Request) -> Result<RoutingSnapshot> {
    let path = socket_path(state_dir);
    let mut stream = StdUnixStream::connect(&path)
        .with_context(|| format!("connect to running daemon at {}", path.display()))?;
    stream.set_read_timeout(Some(CLIENT_TIMEOUT))?;
    stream.set_write_timeout(Some(CLIENT_TIMEOUT))?;
    serde_json::to_writer(&mut stream, request)?;
    stream.write_all(b"\n")?;
    let mut line = String::new();
    BufReader::new(stream)
        .take(MAX_RESPONSE_BYTES as u64)
        .read_line(&mut line)?;
    let response: Response = serde_json::from_str(&line).context("decode control response")?;
    if !response.ok {
        bail!(
            response
                .error
                .unwrap_or_else(|| "control request failed".into())
        )
    }
    response
        .routing
        .context("control response omitted routing status")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::AffinityStore;
    use std::sync::atomic::Ordering;
    use tokio::sync::Notify;

    fn send_raw(state_dir: &Path, request: serde_json::Value) -> Response {
        let mut stream = StdUnixStream::connect(socket_path(state_dir)).unwrap();
        stream.set_read_timeout(Some(RESPONSE_TIMEOUT)).unwrap();
        serde_json::to_writer(&mut stream, &request).unwrap();
        stream.write_all(b"\n").unwrap();
        let mut line = String::new();
        BufReader::new(stream)
            .take(MAX_RESPONSE_BYTES as u64)
            .read_line(&mut line)
            .unwrap();
        serde_json::from_str(&line).unwrap()
    }

    #[test]
    fn constant_time_comparison_handles_different_lengths() {
        assert!(secrets_equal("secret", "secret"));
        assert!(!secrets_equal("secret", "secrex"));
        assert!(!secrets_equal("secret", "secret-longer"));
    }

    #[tokio::test]
    async fn live_switch_is_authenticated_persisted_and_visible() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("comradex.toml");
        fs::write(
            &config_path,
            r#"[proxy]
installation_secret = "0123456789abcdef"
affinity_key = "0123456789abcdef0123456789abcdef"
state_dir = "state"

[listeners.default]
address = "127.0.0.1:10100"
pool = "default"

[pools.default]
members = ["a", "b"]
preferred = "a"

[accounts.a]
kind = "inbound"

[accounts.b]
kind = "inbound"
"#,
        )
        .unwrap();
        let config = Arc::new(Config::load(&config_path).unwrap());
        let state_dir = config.proxy.state_dir.clone().unwrap();
        fs::create_dir_all(&state_dir).unwrap();
        let affinity = Arc::new(
            AffinityStore::load(
                state_dir.join("affinity.json"),
                &config.proxy.affinity_key,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Arc::new(Router::new(&config, affinity));
        let stats = Arc::new(Stats::default());
        stats.inflight_http.store(4, Ordering::Relaxed);
        stats.inflight_bridge_turns.store(7, Ordering::Relaxed);
        let server = ControlServer::bind(
            &state_dir,
            config_path.clone(),
            config.clone(),
            router.clone(),
            stats,
        )
        .unwrap();
        assert_eq!(
            fs::metadata(&state_dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(socket_path(&state_dir))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let usage_refresh = server.usage_refresh_requested();
        let task = tokio::spawn(server.run());

        let state = state_dir.clone();
        let refreshed = tokio::task::spawn_blocking(move || {
            send_raw(&state, serde_json::json!({"command": "ui_refresh_usage"}))
        })
        .await
        .unwrap();
        assert!(refreshed.ok);
        tokio::time::timeout(Duration::from_secs(1), usage_refresh.notified())
            .await
            .expect("manual refresh must wake the usage scheduler");

        let state = state_dir.clone();
        let unauthorized = tokio::task::spawn_blocking(move || {
            set_preferred(&state, "wrong-secret", "default", Some("b"))
        })
        .await
        .unwrap();
        assert!(
            unauthorized
                .unwrap_err()
                .to_string()
                .contains("unauthorized")
        );

        let state = state_dir.clone();
        let ui_status = tokio::task::spawn_blocking(move || {
            send_raw(&state, serde_json::json!({"command": "ui_status"}))
        })
        .await
        .unwrap();
        assert!(ui_status.ok);
        let status = ui_status.status.unwrap();
        assert!(status.daemon_running);
        assert_eq!(status.traffic.inflight_http, 4);
        assert_eq!(status.traffic.inflight_bridge_turns, 7);
        assert_eq!(status.accounts.len(), 2);
        let encoded = serde_json::to_string(&status).unwrap();
        assert!(!encoded.contains("0123456789abcdef"));
        assert!(!encoded.contains(config_path.to_string_lossy().as_ref()));

        let state = state_dir.clone();
        let response = tokio::task::spawn_blocking(move || {
            send_raw(
                &state,
                serde_json::json!({
                    "command": "ui_set_preferred",
                    "pool": "default",
                    "account": "b"
                }),
            )
        })
        .await
        .unwrap();
        assert!(response.ok);
        let routing = response.routing.unwrap();
        assert_eq!(routing.preferred_accounts["default"], "b");
        assert!(!routing.active_accounts.contains_key("default"));
        assert!(
            fs::read_to_string(&config_path)
                .unwrap()
                .contains("preferred = \"b\"")
        );
        assert_eq!(
            router.routing_snapshot().await.preferred_accounts["default"],
            "b"
        );

        let state = state_dir.clone();
        let routing = tokio::task::spawn_blocking(move || {
            set_preferred(&state, "0123456789abcdef", "default", None)
        })
        .await
        .unwrap()
        .unwrap();
        assert!(!routing.preferred_accounts.contains_key("default"));
        let state = state_dir.clone();
        let routing = tokio::task::spawn_blocking(move || {
            set_preferred(&state, "0123456789abcdef", "default", Some("b"))
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(routing.preferred_accounts["default"], "b");

        let selected = router
            .select(
                "default",
                &config.pools["default"],
                Some(router.affinity.key("fresh")),
                None,
            )
            .await
            .unwrap();
        assert_eq!(selected.account_id, "b");
        let state = state_dir.clone();
        let routing =
            tokio::task::spawn_blocking(move || routing_status(&state, "0123456789abcdef"))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(routing.active_accounts["default"], "b");

        let state = state_dir.clone();
        assert!(
            tokio::task::spawn_blocking(move || {
                set_preserved(&state, "wrong-secret", "default", Some("a"))
            })
            .await
            .unwrap()
            .is_err()
        );
        for account in [Some("a"), None] {
            let state = state_dir.clone();
            let routing = tokio::task::spawn_blocking(move || {
                set_preserved(&state, "0123456789abcdef", "default", account)
            })
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                routing
                    .preserved_accounts
                    .get("default")
                    .map(String::as_str),
                account
            );
            let saved = Config::load(&config_path).unwrap();
            assert_eq!(saved.pools["default"].preserved.as_deref(), account);
            let restarted = Router::new(&saved, router.affinity.clone());
            assert_eq!(
                restarted.routing_snapshot().await.preserved_accounts,
                routing.preserved_accounts
            );
        }
        for account in ["b", "missing"] {
            let before = fs::read_to_string(&config_path).unwrap();
            let state = state_dir.clone();
            assert!(
                tokio::task::spawn_blocking(move || {
                    set_preserved(&state, "0123456789abcdef", "default", Some(account))
                })
                .await
                .unwrap()
                .is_err()
            );
            assert_eq!(fs::read_to_string(&config_path).unwrap(), before);
            assert!(
                router
                    .routing_snapshot()
                    .await
                    .preserved_accounts
                    .is_empty()
            );
        }

        // UI roles move directly between preferred and preserved, replacing only that role.
        for (account, role, preferred, preserved) in [
            ("b", "preserved", None, Some("b")),
            ("a", "preferred", Some("a"), Some("b")),
            ("b", "preferred", Some("b"), None),
            ("a", "preserved", Some("b"), Some("a")),
            ("b", "preserved", None, Some("b")),
            ("a", "normal", None, Some("b")),
            ("b", "normal", None, None),
            ("a", "preferred", Some("a"), None),
            ("a", "normal", None, None),
        ] {
            let state = state_dir.clone();
            let response = tokio::task::spawn_blocking(move || send_raw(&state, serde_json::json!({
                "command": "ui_set_account_role", "pool": "default", "account": account, "role": role
            }))).await.unwrap();
            assert!(response.ok, "{:?}", response.error);
            let status = response.status.unwrap();
            assert_eq!(status.pools[0].preferred.as_deref(), preferred);
            assert_eq!(status.pools[0].preserved.as_deref(), preserved);
            let saved = Config::load(&config_path).unwrap();
            assert_eq!(saved.pools["default"].preferred.as_deref(), preferred);
            assert_eq!(saved.pools["default"].preserved.as_deref(), preserved);
            let live = router.routing_snapshot().await;
            assert_eq!(
                live.preferred_accounts.get("default").map(String::as_str),
                preferred
            );
            assert_eq!(
                live.preserved_accounts.get("default").map(String::as_str),
                preserved
            );
            let restarted = Router::new(&saved, router.affinity.clone());
            assert_eq!(
                restarted.routing_snapshot().await.preserved_accounts,
                live.preserved_accounts
            );
            // Changing roles never moves the already-bound conversation.
            assert_eq!(
                router
                    .select(
                        "default",
                        &config.pools["default"],
                        Some(router.affinity.key("fresh")),
                        None
                    )
                    .await
                    .unwrap()
                    .account_id,
                "b"
            );
        }
        for (pool, account) in [("missing", "a"), ("default", "missing")] {
            let before = fs::read_to_string(&config_path).unwrap();
            let state = state_dir.clone();
            let response = tokio::task::spawn_blocking(move || send_raw(&state, serde_json::json!({
                "command": "ui_set_account_role", "pool": pool, "account": account, "role": "preserved"
            }))).await.unwrap();
            assert!(!response.ok);
            assert_eq!(fs::read_to_string(&config_path).unwrap(), before);
            assert!(
                router
                    .routing_snapshot()
                    .await
                    .preserved_accounts
                    .is_empty()
            );
        }

        let valid = fs::read_to_string(&config_path).unwrap();
        let invalid = valid.replace(
            "members = [\"a\", \"b\"]",
            "members = [\"a\", \"b\", \"missing\"]",
        );
        fs::write(&config_path, &invalid).unwrap();
        let state = state_dir.clone();
        let response = tokio::task::spawn_blocking(move || {
            send_raw(&state, serde_json::json!({
                "command": "ui_set_account_role", "pool": "default", "account": "a", "role": "preserved"
            }))
        }).await.unwrap();
        assert!(!response.ok);
        assert_eq!(fs::read_to_string(&config_path).unwrap(), invalid);
        assert!(
            router
                .routing_snapshot()
                .await
                .preserved_accounts
                .is_empty()
        );

        task.abort();
        let _ = task.await;
        assert!(!socket_path(&state_dir).exists());
    }

    #[derive(Clone, Copy)]
    enum FakeLoginOutcome {
        Success,
        ExitFailure,
        RunnerError,
    }

    struct FakeLoginRunner {
        started: Arc<Notify>,
        finish: Arc<Notify>,
        output: Vec<u8>,
        outcome: FakeLoginOutcome,
    }

    impl LoginRunner for FakeLoginRunner {
        fn run(&self, home: PathBuf, output: SharedLoginOutput, _claude: bool) -> LoginFuture {
            let started = self.started.clone();
            let finish = self.finish.clone();
            let bytes = self.output.clone();
            let outcome = self.outcome;
            Box::pin(async move {
                assert!(HomeAuthLock::try_acquire(&home)?.is_none());
                output
                    .lock()
                    .expect("login output mutex poisoned")
                    .append(&bytes);
                started.notify_one();
                finish.notified().await;
                match outcome {
                    FakeLoginOutcome::Success => Ok(true),
                    FakeLoginOutcome::ExitFailure => Ok(false),
                    FakeLoginOutcome::RunnerError => bail!("private runner detail"),
                }
            })
        }
    }

    fn managed_login_fixture() -> (tempfile::TempDir, Arc<Config>, Arc<Router>) {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("comradex.toml");
        fs::write(
            &config_path,
            r#"[proxy]
installation_secret = "0123456789abcdef"
affinity_key = "0123456789abcdef0123456789abcdef"
state_dir = "state"

[listeners.default]
address = "127.0.0.1:10100"
pool = "default"

[pools.default]
members = ["app", "work"]

[accounts.app]
kind = "inbound"

[accounts.work]
kind = "codex_home"
path = "accounts/work"
"#,
        )
        .unwrap();
        let config = Arc::new(Config::load(&config_path).unwrap());
        let state_dir = config.proxy.state_dir.clone().unwrap();
        fs::create_dir_all(&state_dir).unwrap();
        let affinity = Arc::new(
            AffinityStore::load(
                state_dir.join("affinity.json"),
                &config.proxy.affinity_key,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Arc::new(Router::new(&config, affinity));
        (dir, config, router)
    }

    async fn wait_for_login(manager: &LoginManager, session_id: &str) -> UiLoginStatus {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let status = manager.status(session_id).unwrap();
                if status.state != UiLoginState::Running {
                    return status;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn connect_existing_login_acknowledges_then_reloads_and_rejects_invalid_login() {
        let (dir, config, router) = managed_login_fixture();
        let config_path = dir.path().join("comradex.toml");
        let state = config.proxy.state_dir.clone().unwrap();
        let server = ControlServer::bind(
            &state,
            config_path.clone(),
            config.clone(),
            router,
            Arc::new(Stats::default()),
        )
        .unwrap();
        let reload = server.reload_requested();
        let task = tokio::spawn(server.run());
        let original = fs::read_to_string(&config_path).unwrap();
        let home = dir.path().join("existing-login");
        let state_copy = state.clone();
        let home_copy = home.clone();
        let rejected = tokio::task::spawn_blocking(move || send_raw(&state_copy, serde_json::json!({
            "command": "ui_connect_existing_login", "account": "app", "codex_home": home_copy,
        }))).await.unwrap();
        assert!(!rejected.ok);
        assert_eq!(fs::read_to_string(&config_path).unwrap(), original);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), reload.notified())
                .await
                .is_err()
        );

        fs::create_dir_all(&home).unwrap();
        let auth =
            r#"{"tokens":{"access_token":"fixture-access","refresh_token":"fixture-refresh"}}"#;
        fs::write(home.join("auth.json"), auth).unwrap();
        let state_copy = state.clone();
        let home_copy = home.clone();
        let accepted = tokio::task::spawn_blocking(move || send_raw(&state_copy, serde_json::json!({
            "command": "ui_connect_existing_login", "account": "app", "codex_home": home_copy,
        }))).await.unwrap();
        assert!(accepted.ok, "{:?}", accepted.error);
        tokio::time::timeout(Duration::from_secs(1), reload.notified())
            .await
            .unwrap();
        assert_eq!(fs::read_to_string(home.join("auth.json")).unwrap(), auth);
        assert!(
            !serde_json::to_string(&accepted)
                .unwrap()
                .contains("fixture-access")
        );
        task.abort();
        let _ = task.await;
        let updated = Arc::new(Config::load(&config_path).unwrap());
        assert_eq!(
            updated.pools["default"].members,
            config.pools["default"].members
        );
        assert_eq!(
            updated.pools["default"].preferred,
            config.pools["default"].preferred
        );
        assert!(matches!(
            updated.accounts["app"],
            config::AccountConfig::CodexHome { .. }
        ));
        let affinity = Arc::new(
            AffinityStore::load(
                state.join("affinity.json"),
                &updated.proxy.affinity_key,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Arc::new(Router::new(&updated, affinity));
        let server = ControlServer::bind(
            &state,
            config_path,
            updated,
            router,
            Arc::new(Stats::default()),
        )
        .unwrap();
        let task = tokio::spawn(server.run());
        let status = tokio::task::spawn_blocking(move || {
            send_raw(&state, serde_json::json!({"command":"ui_status"}))
        })
        .await
        .unwrap();
        let app = status
            .status
            .unwrap()
            .accounts
            .into_iter()
            .find(|account| account.name == "app")
            .unwrap();
        assert!(app.signed_in);
        assert_eq!(app.auth_state, UiAccountAuthState::SignedIn);
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn managed_login_is_exclusive_bounded_and_returns_only_allowlisted_fields() {
        let (_dir, config, router) = managed_login_fixture();
        let started = Arc::new(Notify::new());
        let finish = Arc::new(Notify::new());
        let mut output = b"visit https://evil.example/device secret=never-return\nvisit \x1b[94mhttps://auth.openai.com/codex/device\x1b[0m\ncode \x1b[94mABCD-EFGH\x1b[0m\n".to_vec();
        output.extend(std::iter::repeat_n(b'x', MAX_LOGIN_OUTPUT_BYTES * 2));
        let manager = LoginManager::new(
            Arc::new(FakeLoginRunner {
                started: started.clone(),
                finish: finish.clone(),
                output,
                outcome: FakeLoginOutcome::Success,
            }),
            router.clone(),
        );
        router.reauth_required("work").await;
        let home = managed_account_home(&config, "work").unwrap().to_owned();

        let started_status = manager
            .start("work".to_owned(), home.clone(), false)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        assert_eq!(started_status.state, UiLoginState::Running);
        assert_eq!(started_status.session_id.len(), 32);
        assert!(
            manager
                .start("work".to_owned(), home.clone(), false)
                .await
                .is_err()
        );
        assert!(
            router
                .select_exact(&config.pools["default"], "work")
                .await
                .is_none()
        );
        let running = manager.status(&started_status.session_id).unwrap();
        assert_eq!(
            running.verification_uri.as_deref(),
            Some("https://auth.openai.com/codex/device")
        );
        assert_eq!(running.user_code.as_deref(), Some("ABCD-EFGH"));
        let encoded = serde_json::to_string(&running).unwrap();
        assert!(!encoded.contains("evil.example"));
        assert!(!encoded.contains("never-return"));
        assert!(running.verification_uri.as_deref().unwrap().len() < MAX_LOGIN_OUTPUT_BYTES);

        finish.notify_one();
        let completed = wait_for_login(&manager, &started_status.session_id).await;
        assert_eq!(completed.state, UiLoginState::Succeeded);
        assert!(completed.error.is_none());
        assert!(HomeAuthLock::try_acquire(&home).unwrap().is_some());
        assert!(
            router
                .select_exact(&config.pools["default"], "work")
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn claude_menu_login_exposes_browser_authorization_and_tracks_account_availability() {
        let (_dir, mut config, router) = managed_login_fixture();
        let home = managed_account_home(&config, "work").unwrap().to_owned();
        Arc::make_mut(&mut config).accounts.insert(
            "work".into(),
            crate::config::AccountConfig::ClaudeHome { path: home.clone() },
        );
        Arc::make_mut(&mut config)
            .accounts
            .insert("app".into(), crate::config::AccountConfig::ClaudeInbound);
        let router = Arc::new(Router::new(&config, router.affinity.clone()));
        let started = Arc::new(Notify::new());
        let finish = Arc::new(Notify::new());
        let manager=LoginManager::new(Arc::new(FakeLoginRunner{started:started.clone(),finish:finish.clone(),output:b"Ignore https://claude.ai.evil.example/oauth/authorize?state=evil\nOpen https://claude.ai/oauth/authorize?state=synthetic\n".to_vec(),outcome:FakeLoginOutcome::Success}),router.clone());
        let session = manager.start("work".into(), home, true).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        let progress = manager.status(&session.session_id).unwrap();
        assert_eq!(progress.provider, "claude");
        assert_eq!(
            progress.verification_uri.as_deref(),
            Some("https://claude.ai/oauth/authorize?state=synthetic")
        );
        let status = build_ui_status(&config, &router, &Stats::default(), &manager).await;
        let account = status.accounts.iter().find(|a| a.name == "work").unwrap();
        assert_eq!(account.auth_state, UiAccountAuthState::LoginInProgress);
        assert!(!account.available);
        finish.notify_one();
        assert_eq!(
            wait_for_login(&manager, &session.session_id).await.state,
            UiLoginState::Succeeded
        );
        assert!(router.routing_snapshot().await.account_states["work"].available);
    }

    #[tokio::test]
    async fn ui_status_keeps_valid_bearer_signed_in_while_renewal_needs_login() {
        use crate::auth::tests::{jwt, serve_refresh_response, test_resolver, write_managed_auth};
        let (_dir, config, router) = managed_login_fixture();
        let home = managed_account_home(&config, "work").unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        write_managed_auth(home, &jwt(now + 120, "valid"));
        let mut resolver = test_resolver(
            &[home],
            serve_refresh_response(
                "401 Unauthorized",
                serde_json::json!({"error": "refresh_token_expired"}),
            )
            .await,
        );
        resolver.health = router.auth_health.clone();
        assert!(
            resolver
                .proactive_refresh_at(&config.accounts["work"], now)
                .await
                .is_err()
        );
        let manager = LoginManager::new(Arc::new(SystemLoginRunner), router.clone());
        let status = build_ui_status(&config, &router, &Stats::default(), &manager).await;
        let work = status
            .accounts
            .iter()
            .find(|account| account.name == "work")
            .unwrap();
        assert!(work.signed_in);
        assert_eq!(work.auth_state, UiAccountAuthState::SignedIn);
        assert!(work.available);
        assert!(work.reauth_required);
        assert!(work.unavailable_reason.is_none());
    }

    #[tokio::test]
    async fn ui_status_reports_router_reauthentication_even_with_auth_file() {
        let (_dir, config, router) = managed_login_fixture();
        let home = managed_account_home(&config, "work").unwrap();
        fs::create_dir_all(home).unwrap();
        fs::write(home.join("auth.json"), "{}").unwrap();
        router.reauth_required("work").await;
        let login_manager = LoginManager::new(Arc::new(SystemLoginRunner), router.clone());

        let status = build_ui_status(&config, &router, &Stats::default(), &login_manager).await;
        let work = status
            .accounts
            .iter()
            .find(|account| account.name == "work")
            .unwrap();

        assert!(!work.signed_in);
        assert_eq!(work.auth_state, UiAccountAuthState::SignedOut);
        assert!(!work.available);
        assert_eq!(work.unavailable_reason.as_deref(), Some("needs_login"));
        assert!(work.retry_at_unix.is_none());
    }

    #[tokio::test]
    async fn managed_login_failure_is_stable_and_restores_routing_availability() {
        for (outcome, expected_error) in [
            (FakeLoginOutcome::ExitFailure, "codex_login_failed"),
            (FakeLoginOutcome::RunnerError, "codex_login_unavailable"),
        ] {
            let (_dir, config, router) = managed_login_fixture();
            let started = Arc::new(Notify::new());
            let finish = Arc::new(Notify::new());
            let manager = LoginManager::new(
                Arc::new(FakeLoginRunner {
                    started: started.clone(),
                    finish: finish.clone(),
                    output: b"internal detail that must not escape".to_vec(),
                    outcome,
                }),
                router.clone(),
            );
            let home = managed_account_home(&config, "work").unwrap().to_owned();
            let session = manager.start("work".to_owned(), home, false).await.unwrap();
            tokio::time::timeout(Duration::from_secs(1), started.notified())
                .await
                .unwrap();
            finish.notify_one();
            let completed = wait_for_login(&manager, &session.session_id).await;
            assert_eq!(completed.state, UiLoginState::Failed);
            assert_eq!(completed.error.as_deref(), Some(expected_error));
            assert!(completed.verification_uri.is_none());
            assert!(completed.user_code.is_none());
            assert!(
                router
                    .select_exact(&config.pools["default"], "work")
                    .await
                    .is_some()
            );
        }
    }

    #[test]
    fn ui_protocol_rejects_inbound_login_and_polls_by_session_id() {
        assert!(matches!(
            serde_json::from_value::<Request>(serde_json::json!({
                "command": "ui_login_status",
                "session_id": "opaque"
            }))
            .unwrap(),
            Request::UiLoginStatus { session_id } if session_id == "opaque"
        ));
        let (_dir, config, _) = managed_login_fixture();
        assert!(managed_account_home(&config, "app").is_err());
    }

    #[test]
    fn bounded_login_output_reads_codex_colored_device_prompt_across_chunks() {
        // Codex prints these ANSI colors even when stdout is a pipe.
        let prompt = b"\nWelcome to Codex [v\x1b[90m0.153.4\x1b[0m]\n\
            \x1b[90mOpenAI's command-line coding agent\x1b[0m\n\
            \nFollow these steps to sign in with ChatGPT using device code authorization:\n\
            \n1. Open this link in your browser and sign in to your account\n\
               \x1b[94mhttps://auth.openai.com/codex/device\x1b[0m\n\
            \n2. Enter this one-time code \x1b[90m(expires in 15 minutes)\x1b[0m\n\
               \x1b[94mABCD-EFGH\x1b[0m\n";
        for split in 0..=prompt.len() {
            let mut output = BoundedLoginOutput::default();
            output.append(&prompt[..split]);
            if split < prompt.len() {
                assert_eq!(output.allowed_fields().1, None, "split at {split}");
            }
            output.append(&prompt[split..]);
            let (uri, code) = output.allowed_fields();
            assert_eq!(uri.as_deref(), Some("https://auth.openai.com/codex/device"));
            assert_eq!(code.as_deref(), Some("ABCD-EFGH"));
        }
    }

    #[test]
    fn bounded_login_output_keeps_colored_url_allowlist_exact() {
        for url in [
            "https://evil.example/codex/device",
            "https://auth.openai.com/codex/device/other",
            "https://auth.openai.com/codex/device?secret=never-return",
        ] {
            let mut output = BoundedLoginOutput::default();
            output.append(format!("\x1b[1;94m{url}\x1b[0m\n").as_bytes());
            assert_eq!(output.allowed_fields(), (None, None));
        }
    }

    #[test]
    fn bounded_login_output_rejects_untrusted_urls_and_accepts_letter_only_codes() {
        let mut output = BoundedLoginOutput::default();
        output.append(
            b"https://evil.example/codex/device https://auth.openai.com/codex/device WXYZ-ABCD\n",
        );
        output.append(&vec![b'x'; MAX_LOGIN_OUTPUT_BYTES * 2]);
        assert!(output.bytes.len() <= MAX_LOGIN_OUTPUT_BYTES);
        assert!(output.truncated);
        let (uri, code) = output.allowed_fields();
        assert_eq!(uri.as_deref(), Some("https://auth.openai.com/codex/device"));
        assert_eq!(code.as_deref(), Some("WXYZ-ABCD"));
    }

    #[test]
    fn codex_executable_resolution_honors_override_then_path() {
        let dir = tempfile::tempdir().unwrap();
        let override_path = dir.path().join("custom-codex");
        fs::write(&override_path, "").unwrap();
        assert_eq!(
            resolve_codex_executable_from(
                Some(override_path.clone().into_os_string()),
                None,
                None,
            )
            .unwrap(),
            override_path
        );

        let path_dir = dir.path().join("bin");
        fs::create_dir_all(&path_dir).unwrap();
        let path_codex = path_dir.join("codex");
        fs::write(&path_codex, "").unwrap();
        let search_path = std::env::join_paths([&path_dir]).unwrap();
        assert_eq!(
            resolve_codex_executable_from(Some(OsString::from("codex")), None, Some(search_path),)
                .unwrap(),
            path_codex
        );
    }
}
