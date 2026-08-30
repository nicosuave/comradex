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
    sync::{Mutex, Semaphore},
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum Request {
    SetPreferred {
        secret: String,
        pool: String,
        account: Option<String>,
    },
    RoutingStatus {
        secret: String,
    },
    UiStatus,
    UiSetPreferred {
        pool: String,
        account: Option<String>,
    },
    UiStartLogin {
        account: String,
    },
    UiLoginStatus {
        session_id: String,
    },
}

impl Request {
    fn secret(&self) -> Option<&str> {
        match self {
            Self::SetPreferred { secret, .. } | Self::RoutingStatus { secret } => Some(secret),
            Self::UiStatus
            | Self::UiSetPreferred { .. }
            | Self::UiStartLogin { .. }
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
    pub name: String,
    pub kind: UiAccountKind,
    pub signed_in: bool,
    pub auth_state: UiAccountAuthState,
    pub pools: Vec<String>,
    pub available: bool,
    pub unavailable_reason: Option<String>,
    pub retry_at_unix: Option<i64>,
    pub usage_percent: Option<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiAccountKind {
    Inbound,
    CodexHome,
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
    pub active: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UiLoginStatus {
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
        let text = String::from_utf8_lossy(&self.bytes);
        let verification_uri = text.split_whitespace().find_map(|token| {
            let token = token.trim_matches(|character: char| {
                matches!(
                    character,
                    '(' | ')' | '[' | ']' | '<' | '>' | ',' | '"' | '\''
                )
            });
            (token == "https://auth.openai.com/codex/device").then(|| token.to_owned())
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
    fn run(&self, home: PathBuf, output: SharedLoginOutput) -> LoginFuture;
}

struct SystemLoginRunner;

impl LoginRunner for SystemLoginRunner {
    fn run(&self, home: PathBuf, output: SharedLoginOutput) -> LoginFuture {
        Box::pin(run_codex_login(home, output))
    }
}

#[derive(Clone)]
struct LoginSession {
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

    async fn start(&self, account: String, home: PathBuf) -> Result<UiLoginStatus> {
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
                manager.runner.run(home, output.clone()).await
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
                    Ok(false) => Some("codex_login_failed".to_owned()),
                    Err(_) => Some("codex_login_unavailable".to_owned()),
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

pub struct ControlServer {
    listener: UnixListener,
    _guard: SocketGuard,
    config_path: PathBuf,
    config: Arc<Config>,
    router: Arc<Router>,
    stats: Arc<Stats>,
    login_manager: LoginManager,
    edit_lock: Arc<Mutex<()>>,
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
            edit_lock: Arc::new(Mutex::new(())),
            clients: Arc::new(Semaphore::new(MAX_CONTROL_CLIENTS)),
        })
    }

    pub async fn run(self) -> Result<()> {
        struct LoginShutdownGuard(LoginManager);
        impl Drop for LoginShutdownGuard {
            fn drop(&mut self) {
                self.0.abort_all();
            }
        }
        let _login_shutdown = LoginShutdownGuard(self.login_manager.clone());
        loop {
            let (stream, _) = self.listener.accept().await?;
            let Ok(permit) = self.clients.clone().try_acquire_owned() else {
                continue;
            };
            let config_path = self.config_path.clone();
            let config = self.config.clone();
            let router = self.router.clone();
            let stats = self.stats.clone();
            let login_manager = self.login_manager.clone();
            let edit_lock = self.edit_lock.clone();
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(error) = handle(
                    stream,
                    config_path,
                    config,
                    router,
                    stats,
                    login_manager,
                    edit_lock,
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
    edit_lock: Arc<Mutex<()>>,
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
                process(
                    request,
                    &config_path,
                    &config,
                    &router,
                    &stats,
                    &login_manager,
                    &edit_lock,
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
    let mut encoded = serde_json::to_vec(&response)?;
    if encoded.len() > MAX_RESPONSE_BYTES {
        encoded = serde_json::to_vec(&Response::error("control response is too large"))?;
    }
    encoded.push(b'\n');
    tokio::time::timeout(RESPONSE_TIMEOUT, async {
        writer.write_all(&encoded).await?;
        writer.shutdown().await
    })
    .await
    .context("control response timed out")??;
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
            Request::UiStatus => {
                Response::status(build_ui_status(config, router, stats, login_manager).await)
            }
            Request::UiStartLogin { account } => {
                let home = managed_account_home(config, &account)?.to_owned();
                Response::login(login_manager.start(account, home).await?)
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
            }
        })
        .collect();
    let pools = config
        .pools
        .iter()
        .map(|(name, pool)| UiPoolStatus {
            name: name.clone(),
            members: pool.members.clone(),
            preferred: routing
                .preferred_accounts
                .get(name)
                .cloned()
                .or_else(|| pool.preferred.clone()),
            active: routing.active_accounts.get(name).cloned(),
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
        crate::config::AccountConfig::Inbound => {
            bail!("inbound accounts use the Codex App login and cannot be logged in here")
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
                100,
                100_000,
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
        let task = tokio::spawn(server.run());

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
        fn run(&self, home: PathBuf, output: SharedLoginOutput) -> LoginFuture {
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
                100,
                100_000,
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
    async fn managed_login_is_exclusive_bounded_and_returns_only_allowlisted_fields() {
        let (_dir, config, router) = managed_login_fixture();
        let started = Arc::new(Notify::new());
        let finish = Arc::new(Notify::new());
        let mut output = b"visit https://evil.example/device secret=never-return\nvisit https://auth.openai.com/codex/device\ncode ABCD-EFGH\n".to_vec();
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
            .start("work".to_owned(), home.clone())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        assert_eq!(started_status.state, UiLoginState::Running);
        assert_eq!(started_status.session_id.len(), 32);
        assert!(
            manager
                .start("work".to_owned(), home.clone())
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
            let session = manager.start("work".to_owned(), home).await.unwrap();
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
