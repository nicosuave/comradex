//! Local desktop router: one Codex app-server connection backed by both the
//! genuine Codex app-server, the Claude facade, and an optional local ACP facade.
//!
//! The desktop spawns this process in place of `codex app-server` (through
//! `CODEX_CLI_PATH`). Every request goes to genuine Codex unless it concerns a
//! Claude model or a Claude-owned thread; thread lists and the model picker
//! merge both backends. The facade itself runs as the separate login-session
//! service and is reached through its private Unix WebSocket. On macOS the
//! launcher execs genuine Codex in the desktop's direct child and keeps the
//! router in a separate process, preserving native browser trust ancestry.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::{Context, Result};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::mpsc,
};
use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};

use crate::{
    local_acp::LocalAcp,
    protocol::{ADOPT_THREAD_METHOD, SECTION_CHANGED_METHOD},
};

const MAX_FRAME: usize = 32 * 1024 * 1024;
/// Prefix for request ids the router itself sends to a backend.
const INTERNAL_ID: &str = "claudex-router-";
/// Prefix for facade server-request ids as presented to the desktop.
const CLAUDE_REQUEST_ID: &str = "claudex:";
const ACP_REQUEST_ID: &str = "claudex-acp:";
const CURSOR_PREFIX: &str = "claudex-router:";
/// Claude's catalog alias for "whatever Claude Code defaults to"; it would only
/// duplicate a concrete model in the shared picker.
const CLAUDE_DEFAULT_ALIAS: &str = "default";
const CLAUDE_PROVIDER: &str = "anthropic";
/// Fields the facade's thread/list accepts; the desktop sends newer ones too.
const CLAUDE_LIST_FILTERS: &[&str] = &[
    "archived",
    "cwd",
    "projectId",
    "searchTerm",
    "sourceKinds",
    "sortKey",
    "sortDirection",
    "parentThreadId",
    "ancestorThreadId",
    "attachedToThreadId",
    "includeAttachedThreads",
];

/// Codex app (connector) methods. A chat's `threadId` only selects Codex's
/// per-thread app configuration; apps do not depend on the chat's model.
const APP_METHODS: &[&str] = &["app/list", "app/installed", "app/read"];

/// Facade methods whose `input` carries the user's message content.
const INPUT_METHODS: &[&str] = &[
    "turn/start",
    "turn/steer",
    "thread/queue/add",
    "thread/queue/update",
];
/// Encoded image bytes one forwarded request may carry, leaving room for the
/// rest of the message within the facade's frame limit.
const MAX_INLINED_IMAGES: usize = MAX_FRAME - 1024 * 1024;

static LOG: std::sync::OnceLock<std::sync::Mutex<std::fs::File>> = std::sync::OnceLock::new();

/// Diagnostics go to stderr (the desktop's app-server log) and, when
/// configured, to the router's own log file.
macro_rules! note {
    ($($arg:tt)*) => {{
        let line = format!("claudex router: {}", format_args!($($arg)*));
        eprintln!("{line}");
        if let Some(file) = LOG.get()
            && let Ok(mut file) = file.lock()
        {
            use std::io::Write;
            let _ = writeln!(file, "{} {line}", crate::protocol::now_ms());
        }
    }};
}

#[path = "router_delegation.rs"]
mod agents;
#[path = "router_local_provider.rs"]
mod local_provider;

/// Whether `args` (everything after the codex executable) is the desktop's
/// stdio app-server launch. Anything else runs genuine Codex unchanged.
pub fn is_app_server_launch(args: &[String]) -> bool {
    if is_desktop_proxy(args) {
        return true;
    }
    let Some(position) = args.iter().position(|arg| arg == "app-server") else {
        return false;
    };
    config_flags_only(&args[..position], false) && config_flags_only(&args[position + 1..], true)
}

/// Desktop SSH carries a WebSocket over the proxy's stdin/stdout, rather than
/// the JSON lines used by a local desktop launch.
fn is_desktop_proxy(args: &[String]) -> bool {
    args == ["app-server", "proxy"]
}

/// Keep the desktop's direct child as genuine Codex, with the router alongside
/// it on private stdio. The browser-access service checks signed process
/// ancestry; inserting our router above Codex breaks that check.
///
/// Returns only in the router child (or for an SSH launch). The original process
/// replaces itself with Codex, retaining its PID, parent, signals and exit status.
/// Neither executable nor the desktop's authorization policy is modified.
///
/// # Safety
/// Call before starting any threads, including the Tokio runtime. The forked
/// child continues running Rust rather than immediately calling exec.
#[cfg(target_os = "macos")]
pub unsafe fn prepare_local_launch(
    codex: &Path,
    args: &[String],
) -> Result<Option<std::os::unix::net::UnixStream>> {
    use std::os::{
        fd::OwnedFd,
        unix::{net::UnixStream, process::CommandExt},
    };

    if is_desktop_proxy(args) || !is_app_server_launch(args) {
        return Ok(None);
    }
    let (router, backend) = UnixStream::pair().context("Cannot create router transport")?;
    let backend_input = backend.try_clone()?;
    // SAFETY: the caller guarantees there are no other threads or locked state.
    let child = unsafe { libc::fork() };
    if child < 0 {
        return Err(std::io::Error::last_os_error()).context("Cannot start router process");
    }
    if child == 0 {
        drop(backend_input);
        drop(backend);
        return Ok(Some(router));
    }
    drop(router);
    let error = std::process::Command::new(codex)
        .args(args)
        .stdin(Stdio::from(OwnedFd::from(backend_input)))
        .stdout(Stdio::from(OwnedFd::from(backend)))
        .exec();
    // An exec failure must not leave a router holding the desktop's pipes open.
    // SAFETY: child is the exact live process just forked above, not a reused PID.
    unsafe {
        libc::kill(child, libc::SIGKILL);
        while libc::waitpid(child, std::ptr::null_mut(), 0) < 0
            && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
        {}
    }
    Err(error).with_context(|| format!("Cannot run {}", codex.display()))
}

fn config_flags_only(args: &[String], app_server: bool) -> bool {
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-c" | "--config" | "--enable" | "--disable" => {
                if args.next().is_none() {
                    return false;
                }
            }
            "--analytics-default-enabled" if app_server => {}
            flag if flag.starts_with("--config=")
                || flag.starts_with("--enable=")
                || flag.starts_with("--disable=") => {}
            _ => return false,
        }
    }
    true
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
enum Side {
    Codex,
    Claude,
    Acp,
}

impl Side {
    const FACADES: [Self; 2] = [Self::Claude, Self::Acp];

    fn name(self) -> &'static str {
        match self {
            Self::Codex => "Codex",
            Self::Claude => "Claude",
            Self::Acp => "local ACP",
        }
    }

    fn service(self) -> &'static str {
        match self {
            Self::Codex => "The Codex service",
            Self::Claude => "The Claude service",
            Self::Acp => "The local ACP service",
        }
    }

    fn request_prefix(self) -> Option<&'static str> {
        match self {
            Self::Codex => None,
            Self::Claude => Some(CLAUDE_REQUEST_ID),
            Self::Acp => Some(ACP_REQUEST_ID),
        }
    }
}

enum Input {
    Client(Value),
    ClientClosed,
    Backend(Side, Value),
    BackendClosed(Side),
    FacadeConnected(Side, mpsc::Sender<Value>),
    LocalProviderModels(Result<crate::local_provider::Config, String>),
}

enum Continuation {
    Ignore,
    LocalProviderDefaults,
    LocalProviderList {
        request: Value,
    },
    LocalProviderProbe {
        thread_id: String,
        request: Value,
    },
    LocalProviderStart {
        replacement: Box<local_provider::Replacement>,
    },
    LocalProviderSettings {
        replacement: Box<local_provider::Replacement>,
    },
    LocalProviderMetadata {
        replacement: Box<local_provider::Replacement>,
    },
    Probe {
        thread_id: String,
        request: Value,
        remaining: VecDeque<Side>,
    },
    FacadeModels,
    Adopt {
        thread_id: String,
        turn: Value,
    },
    Resume {
        thread_id: String,
        request: Value,
    },
    Section {
        side: Side,
        thread_id: String,
        request: Value,
    },
    StartDefault {
        request: Value,
    },
    Merge {
        merge: u64,
        side: Side,
    },
    Delegation {
        side: Side,
        step: agents::Step,
    },
}

enum MergeKind {
    Models,
    Threads { search: bool },
    Loaded,
    Owners,
}

struct Merge {
    client_id: Value,
    kind: MergeKind,
    params: Value,
    /// Codex cursor and the sort boundary of the previous merged page.
    boundary: Option<i64>,
    codex: Option<Result<Value, Value>>,
    codex_pages: Vec<Value>,
    facade_items: Vec<(Side, Value)>,
    /// Read receipts reported with facade pages, by thread id.
    facade_read_states: serde_json::Map<String, Value>,
    pending_facades: HashSet<Side>,
}

struct Forwarded {
    method: String,
    params: Value,
    delegation_scope: Option<String>,
}

#[derive(Clone)]
struct FreshThread {
    params: Value,
    metadata: Value,
    permissions: Value,
    settings: Value,
}

fn remember_permissions(settings: &mut Value, update: &Value) {
    if ["permissions", "sandboxPolicy"]
        .iter()
        .any(|&key| !update[key].is_null())
    {
        let settings = settings.as_object_mut().unwrap();
        settings.remove("permissions");
        settings.remove("sandboxPolicy");
    }
    for key in [
        "permissions",
        "sandboxPolicy",
        "approvalPolicy",
        "approvalsReviewer",
    ] {
        if let Some(value) = update.get(key).filter(|value| !value.is_null()) {
            settings[key] = value.clone();
        }
    }
}

fn explicit_full_access(params: &Value) -> bool {
    params["permissions"] == ":danger-full-access" || params["sandbox"] == "danger-full-access"
}

#[derive(Default)]
struct Facade {
    sender: Option<mpsc::Sender<Value>>,
    initialized: bool,
    models: Option<Vec<Value>>,
    models_requested: bool,
}

pub struct Router {
    client: mpsc::Sender<Value>,
    codex: mpsc::Sender<Value>,
    facades: HashMap<Side, Facade>,
    acp: Option<LocalAcp>,
    local_provider: Option<crate::local_provider::Integration>,
    events: Option<mpsc::Sender<Input>>,
    local_provider_refreshing: bool,
    local_provider_conflicts: HashSet<String>,
    local_provider_checked: HashSet<String>,
    local_provider_replacing: usize,
    local_provider_notifications: Vec<Value>,
    local_provider_discarded: HashSet<String>,
    codex_provider: String,
    threads: HashMap<String, Side>,
    /// Codex threads started on this connection that have not run a turn;
    /// the desktop prewarms these before the user picks a model.
    fresh: HashMap<String, FreshThread>,
    initialize_params: Option<Value>,
    facade_loaded: HashSet<String>,
    /// Actual providers behind the desktop prediction compatibility alias.
    prediction_providers: HashMap<String, String>,
    /// Facade turns the desktop saw start and not yet finish, by thread. The
    /// desktop's connection outlives a facade restart, so if the facade goes
    /// away first, only the router can end them.
    facade_turns: HashMap<String, Value>,
    /// Facade server requests awaiting an answer, with backend and thread.
    facade_requests: HashMap<String, (Side, String)>,
    forwarded: HashMap<(Side, String), Forwarded>,
    internal: HashMap<String, (Side, Continuation)>,
    next_internal: u64,
    known_claude_models: HashSet<String>,
    merges: HashMap<u64, Merge>,
    next_merge: u64,
    /// Client requests waiting for a routing fact, in arrival order.
    queue: VecDeque<Value>,
    /// Every request waits while the Claude catalog or the saved default model
    /// decides where a new thread goes.
    blocked: Option<Block>,
    /// Requests for these threads wait while their ownership, subscription or
    /// adoption is settled. Other chats, including genuine Codex ones, proceed.
    blocked_threads: HashMap<String, Block>,
    draining: bool,
    redrain: bool,
    agents: agents::State,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Block {
    Probe,
    Models,
    Adopt,
    Resume,
    Section,
    DefaultModel,
    LocalProvider,
}

/// Run the router on stdio until the desktop or genuine Codex disconnects.
pub async fn run(
    codex: &Path,
    args: &[String],
    claude_socket: Option<PathBuf>,
    log: Option<PathBuf>,
    codex_connection: Option<std::os::unix::net::UnixStream>,
) -> Result<()> {
    let websocket = is_desktop_proxy(args);
    let proxy_args = [
        "-c".to_owned(),
        "features.code_mode_host=true".to_owned(),
        "app-server".to_owned(),
    ];
    let args = if is_desktop_proxy(args) {
        &proxy_args[..]
    } else {
        args
    };
    if let Some(path) = log {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("Cannot open {}", path.display()))?;
        let _ = LOG.set(std::sync::Mutex::new(file));
    }
    note!("started for {}", codex.display());
    let (events, mut receiver) = mpsc::channel::<Input>(512);

    let (client, mut client_output) = mpsc::channel::<Value>(512);
    let client_writer = if websocket {
        let events = events.clone();
        tokio::spawn(async move {
            let config = WebSocketConfig::default()
                .max_message_size(Some(MAX_FRAME))
                .max_frame_size(Some(MAX_FRAME));
            let stream = tokio::io::join(tokio::io::stdin(), tokio::io::stdout());
            match tokio_tungstenite::accept_async_with_config(stream, Some(config)).await {
                Ok(mut socket) => loop {
                    tokio::select! {
                        message = client_output.recv() => {
                            let Some(message) = message else {
                                break;
                            };
                            if socket.send(Message::Text(message.to_string().into())).await.is_err() {
                                break;
                            }
                        }
                        message = socket.next() => match message {
                            Some(Ok(Message::Text(text))) => match serde_json::from_str(&text) {
                                Ok(message) => {
                                    if events.send(Input::Client(message)).await.is_err() {
                                        break;
                                    }
                                },
                                Err(error) => {
                                    note!("invalid SSH WebSocket RPC: {error}");
                                    break;
                                }
                            },
                            Some(Ok(Message::Ping(bytes))) => {
                                if socket.send(Message::Pong(bytes)).await.is_err() {
                                    break;
                                }
                            }
                            Some(Ok(Message::Pong(_))) => {},
                            _ => break,
                        }
                    }
                },
                Err(error) => note!("SSH WebSocket handshake failed: {error}"),
            }
            let _ = events.send(Input::ClientClosed).await;
        })
    } else {
        spawn_lines(
            tokio::io::stdin(),
            events.clone(),
            Input::Client,
            Input::ClientClosed,
        );
        tokio::spawn(async move {
            let mut stdout = tokio::io::stdout();
            while let Some(message) = client_output.recv().await {
                let mut line = message.to_string();
                line.push('\n');
                if stdout.write_all(line.as_bytes()).await.is_err() || stdout.flush().await.is_err()
                {
                    break;
                }
            }
        })
    };

    let (codex_sender, codex_output) = mpsc::channel::<Value>(512);
    let mut child = if let Some(connection) = codex_connection {
        connection.set_nonblocking(true)?;
        let connection = tokio::net::UnixStream::from_std(connection)?;
        let (reader, writer) = connection.into_split();
        spawn_line_writer(writer, codex_output);
        spawn_lines(
            reader,
            events.clone(),
            |message| Input::Backend(Side::Codex, message),
            Input::BackendClosed(Side::Codex),
        );
        None
    } else {
        let mut child = Command::new(codex)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("Cannot start {}", codex.display()))?;
        spawn_line_writer(child.stdin.take().unwrap(), codex_output);
        spawn_lines(
            child.stdout.take().unwrap(),
            events.clone(),
            |message| Input::Backend(Side::Codex, message),
            Input::BackendClosed(Side::Codex),
        );
        Some(child)
    };

    let mut paths = HashMap::new();
    let mut acp = None;
    if let Some(path) = &claude_socket {
        paths.insert(Side::Claude, path.clone());
        if !websocket && let Some(root) = LocalAcp::root_for_socket(path) {
            match LocalAcp::read(root) {
                Ok(Some(config)) => {
                    paths.insert(Side::Acp, LocalAcp::socket(root));
                    acp = Some(config);
                }
                Ok(None) => {}
                Err(error) => note!("Local ACP configuration is unavailable: {error:#}"),
            }
        }
    }
    let mut facades = HashMap::new();
    let mut reconnecting = HashSet::new();
    for side in Side::FACADES {
        let Some(path) = paths.get(&side) else {
            continue;
        };
        let sender = match connect_facade(side, path, events.clone(), false).await {
            Ok(sender) => Some(sender),
            Err(error) => {
                note!("{} is unavailable ({error:#})", side.name());
                reconnect_facade(side, path.clone(), events.clone());
                reconnecting.insert(side);
                None
            }
        };
        facades.insert(
            side,
            Facade {
                sender,
                ..Facade::default()
            },
        );
    }
    let mut router = Router::new(client, codex_sender, facades, acp);
    router.events = Some(events.clone());
    if let Some(root) = claude_socket.as_deref().and_then(LocalAcp::root_for_socket) {
        match crate::local_provider::Integration::read(root) {
            Ok(local_provider) => router.local_provider = local_provider,
            Err(e) => note!("self-hosted provider configuration is unavailable: {e:#}"),
        }
    }
    let (calls, mut agent_calls) = mpsc::channel(64);
    router.agents.calls = Some(calls);
    if let Some(socket) = &claude_socket {
        router.agents.registry =
            crate::delegation::Registry::open(socket.with_extension("agents"))?;
    }
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
    loop {
        let input = tokio::select! {
            input = receiver.recv() => match input { Some(input) => input, None => break },
            Some(call) = agent_calls.recv() => { router.agent_mcp_call(call); continue; },
            _ = tick.tick() => { router.agent_tick(); continue; },
        };
        match input {
            Input::LocalProviderModels(result) => router.local_provider_models_received(result),
            Input::Client(message) => router.client_message(message),
            Input::Backend(side, message) => router.backend_message(side, message),
            Input::ClientClosed | Input::BackendClosed(Side::Codex) => break,
            Input::BackendClosed(side) => {
                router.facade_failed(side);
                if let Some(path) = paths.get(&side)
                    && reconnecting.insert(side)
                {
                    reconnect_facade(side, path.clone(), events.clone());
                }
            }
            Input::FacadeConnected(side, sender) => {
                reconnecting.remove(&side);
                router.facade_connected(side, sender);
            }
        }
    }
    router.stop_agents();
    // Native Claude sessions live in the separate service. Wait for interruption
    // acknowledgements before closing its transport; never leave delegated work
    // running merely because the desktop disconnected.
    let shutdown = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while router.agents.has_shutdown_work() && tokio::time::Instant::now() < shutdown {
        match tokio::time::timeout_at(shutdown, receiver.recv()).await {
            Ok(Some(Input::Backend(side, message))) => router.backend_message(side, message),
            Ok(Some(Input::BackendClosed(side))) => router.agent_backend_failed(side),
            _ => break,
        }
    }
    // Closing Codex's stdin lets it finish its own shutdown.
    drop(router);
    if let Some(child) = &mut child {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait()).await;
    }
    client_writer.abort();
    Ok(())
}

fn spawn_lines<R>(
    reader: R,
    events: mpsc::Sender<Input>,
    wrap: impl Fn(Value) -> Input + Send + 'static,
    closed: Input,
) where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let message =
                serde_json::from_str(&line).unwrap_or_else(|_| json!({"__routerParseError": true}));
            if events.send(wrap(message)).await.is_err() {
                return;
            }
        }
        let _ = events.send(closed).await;
    });
}

fn spawn_line_writer<W>(mut writer: W, mut messages: mpsc::Receiver<Value>)
where
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            let mut line = message.to_string();
            line.push('\n');
            if writer.write_all(line.as_bytes()).await.is_err() || writer.flush().await.is_err() {
                break;
            }
        }
    });
}

async fn connect_facade(
    side: Side,
    path: &Path,
    events: mpsc::Sender<Input>,
    announce: bool,
) -> Result<mpsc::Sender<Value>> {
    let stream = tokio::net::UnixStream::connect(path)
        .await
        .with_context(|| format!("Cannot connect to {}", path.display()))?;
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME))
        .max_frame_size(Some(MAX_FRAME));
    let (socket, _) =
        tokio_tungstenite::client_async_with_config("ws://localhost/rpc", stream, Some(config))
            .await
            .context("Facade WebSocket handshake failed")?;
    let (mut sink, mut source) = socket.split();
    let (sender, mut messages) = mpsc::channel::<Value>(512);
    // Announce before starting the reader so even an immediate close is
    // ordered after the connection event, never mistaken for the old socket.
    if announce {
        events
            .send(Input::FacadeConnected(side, sender.clone()))
            .await
            .map_err(|_| anyhow::anyhow!("Desktop connection closed"))?;
    }
    tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            if sink
                .send(Message::Text(message.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
        }
        let _ = sink.close().await;
    });
    tokio::spawn(async move {
        while let Some(frame) = source.next().await {
            match frame {
                Ok(Message::Text(text)) => {
                    let Ok(message) = serde_json::from_str(&text) else {
                        continue;
                    };
                    if events.send(Input::Backend(side, message)).await.is_err() {
                        return;
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
        let _ = events.send(Input::BackendClosed(side)).await;
    });
    Ok(sender)
}

/// Keep the desktop's Codex connection alive while the login service restarts.
/// No user requests are replayed: only the handshake and catalog are refreshed.
fn reconnect_facade(side: Side, path: PathBuf, events: mpsc::Sender<Input>) {
    tokio::spawn(async move {
        while !events.is_closed() {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            if let Ok(Ok(sender)) = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                connect_facade(side, &path, events.clone(), true),
            )
            .await
            {
                drop(sender);
                return;
            }
        }
    });
}

pub(crate) fn native_claude_alias(model: &str) -> bool {
    model.starts_with("claude-") || ["opus", "sonnet", "haiku", "fable"].contains(&model)
}

/// Collaboration mode settings override the top-level model in both runtimes.
fn selected_model(params: &Value) -> Option<&str> {
    params["collaborationMode"]["settings"]["model"]
        .as_str()
        .or_else(|| params["model"].as_str())
}

fn id_key(id: &Value) -> String {
    id.to_string()
}

fn error(id: Value, message: impl Into<String>) -> Value {
    json!({"id": id, "error": {"code": -32600, "message": message.into()}})
}

/// Thread ids a message refers to, so ownership is learned from traffic.
fn thread_ids(message: &Value) -> Vec<String> {
    let mut ids = Vec::new();
    for value in [
        &message["params"]["threadId"],
        &message["params"]["thread"]["id"],
        &message["result"]["thread"]["id"],
    ] {
        if let Some(id) = value.as_str() {
            ids.push(id.to_owned());
        }
    }
    ids
}

fn sort_field(params: &Value) -> &'static str {
    match params["sortKey"].as_str() {
        Some("updated_at") => "updatedAt",
        Some("recency_at") => "recencyAt",
        _ => "createdAt",
    }
}

fn descending(params: &Value) -> bool {
    params["sortDirection"].as_str() != Some("asc")
}

fn list_thread(item: &Value) -> &Value {
    if item["thread"].is_object() {
        &item["thread"]
    } else {
        item
    }
}

fn sort_value(item: &Value, field: &str) -> i64 {
    list_thread(item)[field].as_i64().unwrap_or(0)
}

fn encode_cursor(codex: &Value, boundary: Option<i64>) -> String {
    let state = json!({"codex": codex, "boundary": boundary});
    format!(
        "{CURSOR_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(state.to_string())
    )
}

/// Every Claude owner, independent of the desktop's Codex-driven page.
fn claude_owner_query(params: &Value, cursor: Option<Value>) -> Value {
    json!({"attachmentType":params["attachmentType"],"identityKey":params["identityKey"],
        "archived":params["archived"],"limit":1000,"cursor":cursor})
}

/// Router cursors carry Codex's cursor and the sort value where the previous
/// page ended. Foreign cursors are passed to Codex unchanged.
fn decode_cursor(cursor: &Value) -> (Value, Option<i64>) {
    let Some(encoded) = cursor.as_str().and_then(|c| c.strip_prefix(CURSOR_PREFIX)) else {
        return (cursor.clone(), None);
    };
    let state: Value = URL_SAFE_NO_PAD
        .decode(encoded)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default();
    (state["codex"].clone(), state["boundary"].as_i64())
}

/// Embed `localImage` attachments as data URLs before input reaches the facade.
///
/// The desktop can attach files that only its own process tree may open, such
/// as screenshots dragged from the capture thumbnail. The router runs in that
/// tree; the facade is a separate login-session service that macOS refuses.
/// Malformed items are left for the facade to reject.
fn inline_local_images(params: &mut Value) -> Result<(), String> {
    let Some(input) = params.get_mut("input").and_then(Value::as_array_mut) else {
        return Ok(());
    };
    let mut inlined = 0;
    for item in input {
        let Some(path) = item["path"]
            .as_str()
            .filter(|_| item["type"] == "localImage")
        else {
            continue;
        };
        let (mime, bytes) =
            crate::translate::read_local_image(Path::new(path)).map_err(|error| error.message)?;
        let url = format!("data:{mime};base64,{}", STANDARD.encode(bytes));
        inlined += url.len();
        if inlined > MAX_INLINED_IMAGES {
            return Err("Attached images are too large to send together".into());
        }
        let item = item.as_object_mut().expect("typed input items are objects");
        item.remove("path");
        item.insert("type".into(), json!("image"));
        item.insert("url".into(), json!(url));
    }
    Ok(())
}

fn claude_list_query(params: &Value, cursor: Option<Value>) -> Value {
    let mut query = json!({"limit": 1000});
    for field in CLAUDE_LIST_FILTERS {
        if let Some(value) = params.get(*field).filter(|v| !v.is_null()) {
            query[*field] = value.clone();
        }
    }
    // Present and null is a filter of its own: threads in no section.
    if let Some(section) = params.get("sectionId") {
        query["sectionId"] = section.clone();
    }
    if let Some(cursor) = cursor {
        query["cursor"] = cursor;
    }
    query
}

impl Router {
    fn new(
        client: mpsc::Sender<Value>,
        codex: mpsc::Sender<Value>,
        facades: HashMap<Side, Facade>,
        acp: Option<LocalAcp>,
    ) -> Self {
        Self {
            client,
            codex,
            facades,
            acp,
            local_provider: None,
            events: None,
            local_provider_refreshing: false,
            local_provider_conflicts: HashSet::new(),
            local_provider_checked: HashSet::new(),
            local_provider_replacing: 0,
            local_provider_notifications: vec![],
            local_provider_discarded: HashSet::new(),
            codex_provider: "openai".into(),
            threads: HashMap::new(),
            fresh: HashMap::new(),
            initialize_params: None,
            facade_loaded: HashSet::new(),
            prediction_providers: HashMap::new(),
            facade_turns: HashMap::new(),
            facade_requests: HashMap::new(),
            forwarded: HashMap::new(),
            internal: HashMap::new(),
            next_internal: 1,
            known_claude_models: HashSet::new(),
            merges: HashMap::new(),
            next_merge: 1,
            queue: VecDeque::new(),
            blocked: None,
            blocked_threads: HashMap::new(),
            draining: false,
            redrain: false,
            agents: agents::State::default(),
        }
    }

    fn send_client(&self, message: Value) {
        // A stalled desktop must not stall the backends indefinitely; the
        // desktop restarts the app-server when its connection breaks.
        if self.client.try_send(message).is_err() {
            note!("desktop output queue is full or closed");
        }
    }

    fn send_backend_checked(&self, side: Side, mut message: Value) -> bool {
        if side == Side::Codex
            && let Some(local_provider) = &self.local_provider
        {
            local_provider.to_native(&mut message);
        }
        let sender = match side {
            Side::Codex => Some(&self.codex),
            _ => self
                .facades
                .get(&side)
                .and_then(|facade| facade.sender.as_ref()),
        };
        if sender.is_none_or(|sender| sender.try_send(message).is_err()) {
            note!("{side:?} input queue is full or closed");
            return false;
        }
        true
    }

    fn send_backend(&mut self, side: Side, message: Value) {
        self.send_backend_checked(side, message);
    }

    fn internal_request(
        &mut self,
        side: Side,
        method: &str,
        params: Value,
        continuation: Continuation,
    ) {
        let id = format!("{INTERNAL_ID}{}", self.next_internal);
        self.next_internal += 1;
        self.internal.insert(id.clone(), (side, continuation));
        if !self.send_backend_checked(side, json!({"id": id, "method": method, "params": params})) {
            let (_, continuation) = self.internal.remove(&id).unwrap();
            self.continue_with(
                side,
                continuation,
                Err(json!({"code":-32000,"message":"Native backend input queue is unavailable"})),
            );
        }
    }

    fn forward(&mut self, side: Side, mut message: Value) {
        if side != Side::Codex && !self.facade_available(side) {
            if let Some(id) = message.get("id") {
                self.send_client(error(
                    id.clone(),
                    format!(
                        "{} is reconnecting; retry this request shortly",
                        side.service()
                    ),
                ));
            }
            return;
        }
        if side == Side::Acp
            && message["method"] == "thread/start"
            && !explicit_full_access(&message["params"])
        {
            self.send_client(error(
                message["id"].clone(),
                "Local ACP chats require Full access",
            ));
            return;
        }
        if side == Side::Claude
            && message["params"]["modelProvider"] == "openai"
            && let Some(provider) = message["params"]["threadId"]
                .as_str()
                .and_then(|thread| self.prediction_providers.get(thread))
        {
            // The desktop can echo the resume response's compatibility alias.
            // Resolve it only after thread ownership has selected Claude.
            message["params"]["modelProvider"] = json!(provider);
        }
        if side != Side::Codex
            && matches!(
                message["method"].as_str(),
                Some("turn/start" | "turn/steer" | "turn/interrupt" | "thread/compact/start")
            )
            && let Some(thread) = message["params"]["threadId"].as_str()
            && !self.facade_loaded.contains(thread)
        {
            let thread_id = thread.to_owned();
            self.block_thread(&thread_id, Block::Resume);
            self.internal_request(
                side,
                "thread/resume",
                json!({"threadId":thread_id,"excludeTurns":true}),
                Continuation::Resume {
                    thread_id: thread_id.clone(),
                    request: message,
                },
            );
            return;
        }
        if side == Side::Claude
            && message["method"] == "thread/resume"
            && message["params"]["threadId"]
                .as_str()
                .is_some_and(|thread| self.agents.registry.agents.contains_key(thread))
            && let Some(params) = message["params"].as_object_mut()
        {
            // Desktop supplies its own launch settings when opening a chat.
            // A delegated session owns its inherited permissions, instructions
            // and tools: opening it only hydrates and subscribes, including while
            // its turn is active. Explicit settings/turn requests still apply.
            params.retain(|key, _| {
                matches!(
                    key.as_str(),
                    "threadId" | "path" | "excludeTurns" | "initialTurnsPage"
                )
            });
        }
        if side != Side::Codex
            && message["method"]
                .as_str()
                .is_some_and(|method| INPUT_METHODS.contains(&method))
            && let Some(params) = message.get_mut("params")
            && let Err(failure) = inline_local_images(params)
        {
            self.send_client(error(message["id"].clone(), failure));
            return;
        }
        let delegation_scope = match self.prepare_agent_tools(side, &mut message) {
            Ok(scope) => scope,
            Err(failure) => {
                self.send_client(error(message["id"].clone(), failure.to_string()));
                return;
            }
        };
        self.invalidate_agent_snapshot(&message);
        if message["method"] == "turn/interrupt"
            && let Some(thread) = message["params"]["threadId"].as_str()
        {
            self.interrupt_descendants(thread);
        }
        // Keep the original start payload for recreating an empty prewarm on
        // either provider; Self-hosted overrides belong only to its native request.
        let tracked_params = message["params"].clone();
        if side == Side::Codex
            && let Err(e) = self.apply_local_provider_settings(&mut message)
        {
            self.send_client(error(message["id"].clone(), e.to_string()));
            return;
        }
        if let Some(id) = message.get("id") {
            self.forwarded.insert(
                (side, id_key(id)),
                Forwarded {
                    method: message["method"].as_str().unwrap_or("").to_owned(),
                    params: tracked_params,
                    delegation_scope,
                },
            );
        }
        self.send_backend(side, message);
    }

    fn claude_available(&self) -> bool {
        self.facade_available(Side::Claude)
    }

    fn facade_available(&self, side: Side) -> bool {
        self.facades
            .get(&side)
            .is_some_and(|facade| facade.sender.is_some() && facade.initialized)
    }

    fn available_facades(&self) -> Vec<Side> {
        Side::FACADES
            .into_iter()
            .filter(|&side| self.facade_available(side))
            .collect()
    }

    /// `None` until the facade's catalog is known.
    fn is_claude_model(&self, model: &str) -> Option<bool> {
        if self.known_claude_models.contains(model) || native_claude_alias(model) {
            return Some(true);
        }
        if !self.claude_available() {
            return Some(false);
        }
        self.facades[&Side::Claude]
            .models
            .as_ref()
            .map(|models| models.iter().any(|entry| entry["id"] == model))
    }

    fn is_acp_model(&self, model: &str) -> bool {
        self.acp
            .as_ref()
            .is_some_and(|config| config.models().any(|id| id == model))
    }

    fn model_side(&self, model: &str) -> Option<Side> {
        if self.is_acp_model(model) {
            return Some(Side::Acp);
        }
        self.is_claude_model(model)
            .map(|owned| if owned { Side::Claude } else { Side::Codex })
    }

    /// The backend that owns a provider. Codex owns every provider a configured
    /// facade does not claim.
    fn provider_side(&self, provider: &str) -> Side {
        let side = if provider == CLAUDE_PROVIDER {
            Side::Claude
        } else if self
            .acp
            .as_ref()
            .is_some_and(|config| config.provider_id == provider)
        {
            Side::Acp
        } else {
            return Side::Codex;
        };
        if self.facades.contains_key(&side) {
            side
        } else {
            Side::Codex
        }
    }

    /// Whether the desktop may pick this facade catalog entry.
    fn selectable_model(&self, side: Side, entry: &Value) -> bool {
        entry["id"].as_str().is_some_and(|id| {
            id != CLAUDE_DEFAULT_ALIAS && (side != Side::Acp || self.is_acp_model(id))
        })
    }

    fn request_facade_models(&mut self, side: Side) {
        if !self.facade_available(side) || self.facades[&side].models_requested {
            return;
        }
        self.facades.get_mut(&side).unwrap().models_requested = true;
        self.internal_request(
            side,
            "model/list",
            json!({"limit": 1000}),
            Continuation::FacadeModels,
        );
    }

    /// Hold client traffic until a routing fact arrives, preserving order.
    fn block(&mut self, reason: Block, message: Value) {
        self.queue.push_front(message);
        self.blocked = Some(reason);
    }

    fn unblock(&mut self, reason: Block) {
        if self.blocked != Some(reason) {
            return;
        }
        self.blocked = None;
        self.drain();
    }

    /// Hold one thread's traffic until a routing fact about it arrives. The
    /// request that needed the fact is held by its continuation.
    fn block_thread(&mut self, thread: &str, reason: Block) {
        self.blocked_threads.insert(thread.to_owned(), reason);
    }

    fn unblock_thread(&mut self, thread: &str, reason: Block) {
        if self.blocked_threads.get(thread) != Some(&reason) {
            return;
        }
        self.blocked_threads.remove(thread);
        self.drain();
    }

    /// Whether a request must stay behind an outstanding routing fact or an
    /// earlier waiting request it may depend on: one for the same thread, or
    /// one without a thread.
    fn must_wait(&self, message: &Value, earlier: &VecDeque<Value>) -> bool {
        if self.blocked.is_some() {
            return true;
        }
        let thread = message["params"]["threadId"].as_str();
        if thread.is_some_and(|thread| self.blocked_threads.contains_key(thread)) {
            return true;
        }
        earlier.iter().any(|queued| {
            let queued = queued["params"]["threadId"].as_str();
            queued.is_none() || queued == thread
        })
    }

    /// Dispatch every queued request that no longer has to wait, in order.
    fn drain(&mut self) {
        // A dispatch can settle another routing fact synchronously.
        if self.draining {
            self.redrain = true;
            return;
        }
        self.draining = true;
        loop {
            self.redrain = false;
            let mut waiting = VecDeque::new();
            while let Some(message) = self.queue.pop_front() {
                if self.must_wait(&message, &waiting) {
                    waiting.push_back(message);
                } else {
                    self.dispatch(message);
                }
            }
            self.queue = waiting;
            if !self.redrain {
                break;
            }
        }
        self.draining = false;
    }

    fn client_message(&mut self, message: Value) {
        if message.get("__routerParseError").is_some() {
            self.send_client(
                json!({"id": null, "error": {"code": -32700, "message": "Invalid JSON"}}),
            );
            return;
        }
        // Responses to server requests are routed by their id and never wait.
        if message.get("method").is_none() && message.get("id").is_some() {
            self.client_response(message);
            return;
        }
        if self.must_wait(&message, &self.queue) {
            self.queue.push_back(message);
        } else {
            self.dispatch(message);
        }
    }

    fn client_response(&mut self, mut message: Value) {
        let decoded = message["id"].as_str().and_then(|id| {
            self.facade_requests.remove(id);
            Side::FACADES.into_iter().find_map(|side| {
                let raw = id.strip_prefix(side.request_prefix()?)?;
                Some((side, serde_json::from_str::<Value>(raw).ok()?))
            })
        });
        match decoded {
            Some((side, id)) => {
                message["id"] = id;
                self.send_backend(side, message);
            }
            None => self.send_backend(Side::Codex, message),
        }
    }

    fn dispatch(&mut self, message: Value) {
        self.refresh_local_provider_routing();
        let Some(method) = message["method"].as_str().map(str::to_owned) else {
            self.send_backend(Side::Codex, message);
            return;
        };
        let Some(id) = message.get("id").cloned() else {
            if method == "initialized" {
                for side in self.available_facades() {
                    self.send_backend(side, message.clone());
                }
            }
            self.send_backend(Side::Codex, message);
            if method == "initialized" {
                self.recover_agents();
            }
            return;
        };
        let params = message["params"].clone();
        if matches!(method.as_str(), "thread/list" | "thread/search")
            && self.local_provider.is_some()
            && params["modelProviders"].is_null()
        {
            self.internal_request(
                Side::Codex,
                "config/read",
                json!({"includeLayers":false,"cwd":null}),
                Continuation::LocalProviderList { request: message },
            );
            return;
        }
        match method.as_str() {
            "initialize" => {
                self.initialize_params = Some(params.clone());
                for side in Side::FACADES {
                    if let Some(facade) = self.facades.get_mut(&side)
                        && facade.sender.is_some()
                    {
                        facade.initialized = true;
                        self.internal_request(
                            side,
                            "initialize",
                            params.clone(),
                            Continuation::Ignore,
                        );
                    }
                }
                self.forward(Side::Codex, message);
            }
            "model/list"
                if params["modelProvider"].is_null()
                    && (!self.available_facades().is_empty() || self.local_provider.is_some()) =>
            {
                self.request_local_provider_models();
                for side in self.available_facades() {
                    self.request_facade_models(side);
                }
                self.start_merge(id, MergeKind::Models, params);
            }
            "model/list"
                if params["modelProvider"] == crate::local_provider::PROVIDER
                    && self.local_provider.is_some() =>
            {
                let config = &self.local_provider.as_ref().unwrap().config;
                let models: Vec<Value> = config
                    .models
                    .iter()
                    .filter(|model| self.local_provider_model(&model.id))
                    .map(|model| model.picker(&config.name, config.reasoning))
                    .collect();
                self.send_client(json!({"id":id,"result":{"data":models,"nextCursor":null}}));
            }
            // The desktop scopes an existing chat's picker to its provider.
            "model/list" => {
                let side = params["modelProvider"]
                    .as_str()
                    .map_or(Side::Codex, |provider| self.provider_side(provider));
                self.forward(side, message);
            }
            "thread/loaded/list" if !self.available_facades().is_empty() => {
                self.start_merge(id, MergeKind::Loaded, params);
            }
            "thread/attachmentOwner/list" if !self.available_facades().is_empty() => {
                self.start_merge(id, MergeKind::Owners, params);
            }
            "thread/list" | "thread/search" if !self.available_facades().is_empty() => {
                self.start_merge(
                    id,
                    MergeKind::Threads {
                        search: method == "thread/search",
                    },
                    params,
                );
            }
            "thread/sample/cancel" => {
                // A cancel names only the sample's request id, which tells
                // which backend received that sample.
                let request = id_key(&params["requestId"]);
                let side = Side::FACADES
                    .into_iter()
                    .find(|&side| {
                        self.forwarded
                            .get(&(side, request.clone()))
                            .is_some_and(|forwarded| forwarded.method == "thread/sample")
                    })
                    .unwrap_or(Side::Codex);
                self.forward(side, message);
            }
            "thread/start" => match selected_model(&params) {
                Some(model) => match self.model_side(model) {
                    None => {
                        // Block first: a failed request completes synchronously.
                        self.block(Block::Models, message);
                        self.request_facade_models(Side::Claude);
                    }
                    Some(side) => self.forward(side, message),
                },
                None => {
                    // The desktop saves its picked model as a Codex config
                    // default, then can prewarm without an explicit model.
                    // Resolve it in this cwd instead of assuming the default
                    // belongs to Codex or caching another project's config.
                    self.blocked = Some(Block::DefaultModel);
                    self.internal_request(
                        Side::Codex,
                        "config/read",
                        json!({"cwd":params["cwd"],"includeLayers":false}),
                        Continuation::StartDefault { request: message },
                    );
                }
            },
            _ => match params["threadId"].as_str() {
                Some(thread) => {
                    let thread = thread.to_owned();
                    self.route_by_thread(&thread, message);
                }
                None => self.forward(Side::Codex, message),
            },
        }
    }

    fn route_by_thread(&mut self, thread: &str, message: Value) {
        let side = match self.threads.get(thread) {
            Some(side) => *side,
            None if !self.facades.is_empty() => {
                let thread_id = thread.to_owned();
                self.block_thread(&thread_id, Block::Probe);
                self.probe_thread(thread_id, message, self.available_facades().into());
                return;
            }
            None => Side::Codex,
        };
        let method = message["method"].as_str().unwrap_or("").to_owned();
        if side == Side::Codex && self.route_local_provider_selection(thread, &message) {
            return;
        }
        if let Some(model) = selected_model(&message["params"]).map(str::to_owned) {
            let Some(target) = self.model_side(&model) else {
                self.block(Block::Models, message);
                self.request_facade_models(Side::Claude);
                return;
            };
            if target != side {
                if matches!(method.as_str(), "turn/start" | "thread/settings/update")
                    && side == Side::Codex
                    && self.fresh.contains_key(thread)
                {
                    self.adopt(target, thread, message);
                } else {
                    self.send_client(error(
                        message["id"].clone(),
                        format!(
                            "This chat runs on {}; start a new chat to use {model}",
                            side.name()
                        ),
                    ));
                }
                return;
            }
        }
        if side != Side::Codex && APP_METHODS.contains(&method.as_str()) {
            // Genuine Codex serves apps; it does not know facade threads.
            let mut message = message;
            if let Some(params) = message["params"].as_object_mut() {
                params.remove("threadId");
            }
            self.forward(Side::Codex, message);
            return;
        }
        if method == "thread/section/move" {
            self.move_to_section(side, message);
            return;
        }
        if side == Side::Codex && method == "turn/start" {
            self.fresh.remove(thread);
        }
        self.forward(side, message);
    }

    fn probe_thread(&mut self, thread_id: String, request: Value, mut remaining: VecDeque<Side>) {
        if let Some(side) = remaining.pop_front() {
            self.internal_request(
                side,
                "thread/read",
                json!({"threadId": thread_id}),
                Continuation::Probe {
                    thread_id,
                    request,
                    remaining,
                },
            );
            return;
        }
        if let Some(side) = Side::FACADES
            .into_iter()
            .find(|side| self.facades.contains_key(side) && !self.facade_available(*side))
        {
            self.send_client(error(
                request["id"].clone(),
                format!(
                    "{} is reconnecting; thread ownership cannot be checked yet",
                    side.service()
                ),
            ));
        } else {
            self.threads.insert(thread_id.clone(), Side::Codex);
            self.dispatch(request);
        }
        self.unblock_thread(&thread_id, Block::Probe);
    }

    /// Each backend files its own threads, and neither can place a thread
    /// relative to the other's: a `beforeThreadId` on the other backend
    /// appends instead. Sections belong to genuine Codex, so a Claude thread's
    /// destination is first looked up there.
    fn move_to_section(&mut self, side: Side, mut message: Value) {
        let foreign_before = message["params"]["beforeThreadId"]
            .as_str()
            .is_some_and(|before| self.threads.get(before).is_some_and(|owner| *owner != side));
        if foreign_before {
            message["params"]["beforeThreadId"] = Value::Null;
        }
        if side == Side::Codex
            || message["params"]["sectionId"]
                .as_str()
                .is_none_or(|section| section.trim().is_empty())
        {
            self.forward(side, message);
            return;
        }
        let thread_id = message["params"]["threadId"]
            .as_str()
            .unwrap_or("")
            .to_owned();
        self.block_thread(&thread_id, Block::Section);
        self.internal_request(
            Side::Codex,
            "threadSection/list",
            json!({"cursor": null, "limit": 100}),
            Continuation::Section {
                side,
                thread_id,
                request: message,
            },
        );
    }

    /// A deleted thread's id never returns: forget its ownership and state.
    fn forget_thread(&mut self, thread: &str) {
        self.threads.remove(thread);
        self.fresh.remove(thread);
        self.facade_loaded.remove(thread);
        self.prediction_providers.remove(thread);
        self.facade_turns.remove(thread);
        self.agent_thread_deleted(thread);
    }

    /// Facade threads keep their own copy of the section they are filed in.
    fn section_changed(&mut self, section_id: Value, section: Value) {
        if !section_id.is_string() {
            return;
        }
        for side in self.available_facades() {
            self.internal_request(
                side,
                SECTION_CHANGED_METHOD,
                json!({"sectionId": section_id, "section": section}),
                Continuation::Ignore,
            );
        }
    }

    /// Move the desktop's prewarmed, still-empty Codex thread to a facade under
    /// the same id, then run the requested turn there.
    fn adopt(&mut self, side: Side, thread: &str, turn: Value) {
        if !self.facade_available(side) {
            self.send_client(error(
                turn["id"].clone(),
                format!(
                    "{} is reconnecting; retry this request shortly",
                    side.service()
                ),
            ));
            return;
        }
        let fresh = self
            .fresh
            .get(thread)
            .expect("fresh thread checked before adoption")
            .clone();
        let mut params = fresh.params;
        if side == Side::Acp {
            let method = turn["method"].as_str().unwrap_or("turn/start");
            let mut overrides = fresh.permissions;
            remember_permissions(&mut overrides, &turn["params"]);
            let overrides = match crate::desktop::normalize(method, &overrides) {
                Ok((overrides, _warnings)) => overrides,
                Err(failure) => {
                    self.send_client(failure.response(turn["id"].clone()));
                    return;
                }
            };
            if let Some(policy) = overrides
                .get("sandboxPolicy")
                .filter(|value| !value.is_null())
            {
                if policy["type"] != "dangerFullAccess" {
                    self.send_client(error(
                        turn["id"].clone(),
                        "Local ACP chats require Full access",
                    ));
                    return;
                }
                params.as_object_mut().unwrap().remove("permissions");
                params["sandbox"] = json!("danger-full-access");
            }
            for key in ["approvalPolicy", "approvalsReviewer"] {
                if let Some(value) = overrides.get(key).filter(|value| !value.is_null()) {
                    params[key] = value.clone();
                }
            }
            if !explicit_full_access(&params) {
                self.send_client(error(
                    turn["id"].clone(),
                    "Local ACP chats require Full access",
                ));
                return;
            }
        }
        if let Some(config) = params["config"].as_object_mut() {
            config.remove("mcp_servers.claudex_agents");
        }
        if let Some(tools) = params["dynamicTools"].as_array_mut() {
            tools.retain(|tool| *tool != crate::delegation::dynamic_spec());
        }
        let mut start = json!({"method":"thread/start","params":params});
        if let Err(failure) = self.prepare_agent_tools(side, &mut start) {
            self.send_client(error(turn["id"].clone(), failure.to_string()));
            return;
        }
        let mut params = start["params"].clone();
        params["model"] = json!(selected_model(&turn["params"]));
        if let Some(effort) = turn["params"].get("effort").filter(|v| !v.is_null()) {
            params["effort"] = effort.clone();
        }
        let thread_id = thread.to_owned();
        self.block_thread(&thread_id, Block::Adopt);
        self.internal_request(
            side,
            ADOPT_THREAD_METHOD,
            json!({"threadId": thread_id, "params": params, "metadata": {
                "projectId":fresh.metadata["projectId"],
                "gitInfo":fresh.metadata["gitInfo"],
                "daybreakEnabled":fresh.metadata["daybreakEnabled"]
            }}),
            Continuation::Adopt {
                thread_id: thread_id.clone(),
                turn,
            },
        );
    }

    fn start_merge(&mut self, client_id: Value, kind: MergeKind, params: Value) {
        let merge = self.next_merge;
        self.next_merge += 1;
        let mut codex_params = params.clone();
        let mut boundary = None;
        if let Some(cursor) = params.get("cursor").filter(|c| !c.is_null()) {
            let (codex_cursor, previous) = decode_cursor(cursor);
            codex_params["cursor"] = codex_cursor;
            boundary = previous;
        }
        let codex_method = match kind {
            MergeKind::Models => "model/list",
            MergeKind::Loaded => "thread/loaded/list",
            MergeKind::Owners => "thread/attachmentOwner/list",
            MergeKind::Threads { search: true } => "thread/search",
            MergeKind::Threads { search: false } => "thread/list",
        };
        if matches!(kind, MergeKind::Threads { search: false }) && agents::shared_listing(&params) {
            codex_params.as_object_mut().unwrap().remove("cursor");
            codex_params["limit"] = json!(1000);
            boundary = None;
        }
        let query = match kind {
            MergeKind::Models => None,
            MergeKind::Loaded => Some(("thread/loaded/list", json!({"limit": 1000}))),
            MergeKind::Owners => Some((
                "thread/attachmentOwner/list",
                claude_owner_query(&params, None),
            )),
            MergeKind::Threads { .. } => Some(("thread/list", claude_list_query(&params, None))),
        };
        let facades = if query.is_some() {
            self.available_facades()
        } else {
            vec![]
        };
        self.merges.insert(
            merge,
            Merge {
                client_id,
                kind,
                params,
                boundary,
                codex: None,
                codex_pages: vec![],
                facade_items: vec![],
                facade_read_states: serde_json::Map::new(),
                pending_facades: facades.iter().copied().collect(),
            },
        );
        self.internal_request(
            Side::Codex,
            codex_method,
            codex_params,
            Continuation::Merge {
                merge,
                side: Side::Codex,
            },
        );
        if let Some((method, params)) = query {
            for side in facades {
                self.internal_request(
                    side,
                    method,
                    params.clone(),
                    Continuation::Merge { merge, side },
                );
            }
        }
    }

    fn merge_part(&mut self, merge: u64, side: Side, result: Result<Value, Value>) {
        let Some(state) = self.merges.get_mut(&merge) else {
            return;
        };
        let mut next_page = None;
        match (side, result) {
            (Side::Codex, Ok(mut page))
                if matches!(state.kind, MergeKind::Threads { search: false })
                    && agents::shared_listing(&state.params) =>
            {
                state
                    .codex_pages
                    .extend(page["data"].as_array().cloned().unwrap_or_default());
                if let Some(cursor) = page["nextCursor"].as_str() {
                    let mut query = state.params.clone();
                    query["limit"] = json!(1000);
                    query["cursor"] = json!(cursor);
                    next_page = Some(("thread/list", query));
                } else {
                    page["data"] = json!(std::mem::take(&mut state.codex_pages));
                    state.codex = Some(Ok(page));
                }
            }
            (Side::Codex, result) => state.codex = Some(result),
            (side, Ok(page)) => {
                state.facade_items.extend(
                    page["data"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .cloned()
                        .map(|item| (side, item)),
                );
                if let Some(read_states) = page["readStates"].as_object() {
                    state.facade_read_states.extend(read_states.clone());
                }
                match page["nextCursor"].as_str() {
                    Some(cursor) => {
                        let cursor = json!(cursor);
                        next_page = Some(match state.kind {
                            MergeKind::Loaded => (
                                "thread/loaded/list",
                                json!({"limit": 1000, "cursor": cursor}),
                            ),
                            MergeKind::Owners => (
                                "thread/attachmentOwner/list",
                                claude_owner_query(&state.params, Some(cursor)),
                            ),
                            _ => (
                                "thread/list",
                                claude_list_query(&state.params, Some(cursor)),
                            ),
                        });
                    }
                    None => {
                        state.pending_facades.remove(&side);
                    }
                }
            }
            // A facade failure never hides the other backends.
            (side, Err(error)) => {
                note!("{} list failed: {error}", side.name());
                state.pending_facades.remove(&side);
            }
        }
        if let Some((method, query)) = next_page {
            self.internal_request(side, method, query, Continuation::Merge { merge, side });
        }
        self.finish_merges();
    }

    fn finish_merges(&mut self) {
        let models_ready = self
            .available_facades()
            .into_iter()
            .all(|side| self.facades[&side].models.is_some())
            && !self.local_provider_refreshing;
        let ready: Vec<u64> = self
            .merges
            .iter()
            .filter(|(_, m)| {
                m.codex.is_some()
                    && m.pending_facades.is_empty()
                    && (!matches!(m.kind, MergeKind::Models) || models_ready)
            })
            .map(|(id, _)| *id)
            .collect();
        for merge in ready {
            let state = self.merges.remove(&merge).unwrap();
            let reply = self.merged(state);
            self.send_client(reply);
        }
    }

    fn merged(&mut self, mut state: Merge) -> Value {
        let mut result = match state.codex.unwrap() {
            Ok(result) => result,
            Err(error) => return json!({"id": state.client_id, "error": error}),
        };
        let last_page = result["nextCursor"].is_null();
        // Preserve backend order for ties and across pages of a section.
        state.facade_items.sort_by_key(|(side, _)| *side);
        match state.kind {
            MergeKind::Models => {
                if last_page && let Some(data) = result["data"].as_array_mut() {
                    let mut seen: HashSet<Value> =
                        data.iter().map(|model| model["id"].clone()).collect();
                    for side in self.available_facades() {
                        if let Some(models) = &self.facades[&side].models {
                            data.extend(
                                models
                                    .iter()
                                    .filter(|model| seen.insert(model["id"].clone()))
                                    .cloned(),
                            );
                        }
                    }
                    if let Some(local_provider) = &self.local_provider {
                        self.local_provider_conflicts = local_provider
                            .config
                            .models
                            .iter()
                            .filter(|model| seen.contains(&json!(model.id)))
                            .map(|model| model.id.clone())
                            .collect();
                        data.extend(
                            local_provider
                                .config
                                .models
                                .iter()
                                .filter(|model| seen.insert(json!(model.id)))
                                .map(|model| {
                                    model.picker(
                                        &local_provider.config.name,
                                        local_provider.config.reasoning,
                                    )
                                }),
                        );
                    }
                }
            }
            MergeKind::Loaded => {
                if let Some(local_provider) = &self.local_provider
                    && let Some(data) = result["data"].as_array_mut()
                {
                    for id in data {
                        if let Some(native) = id.as_str() {
                            *id = json!(local_provider.public_id(native));
                        }
                    }
                }
                for (side, item) in &state.facade_items {
                    if let Some(id) = item.as_str() {
                        self.threads.insert(id.to_owned(), *side);
                    }
                }
                if last_page && let Some(data) = result["data"].as_array_mut() {
                    data.extend(state.facade_items.into_iter().map(|(_, item)| item));
                    let mut seen = HashSet::new();
                    data.retain(|id| seen.insert(id.to_string()));
                }
            }
            MergeKind::Owners => {
                for (side, owner) in &state.facade_items {
                    if let Some(id) = owner["threadId"].as_str() {
                        self.threads.insert(id.to_owned(), *side);
                    }
                }
                // Facade owners follow Codex's, like other combined lists.
                if last_page && let Some(data) = result["data"].as_array_mut() {
                    data.extend(state.facade_items.into_iter().map(|(_, item)| item));
                    let mut seen = HashSet::new();
                    data.retain(|owner| seen.insert(owner["threadId"].to_string()));
                }
            }
            MergeKind::Threads { search } => {
                let field = sort_field(&state.params);
                let descending = descending(&state.params);
                // Codex orders a section by its own positions, which cannot
                // interleave with the facades': their members follow Codex's.
                let section_order = state.params["sortKey"] == "section_position";
                let data = result["data"].as_array().cloned().unwrap_or_default();
                // An adopted prewarm can still exist in Codex's metadata.
                // Its facade owns that id; never publish both copies or let the
                // Codex copy overwrite it when the desktop hydrates its list.
                for (side, thread) in &state.facade_items {
                    if let Some(id) = thread["id"].as_str() {
                        self.threads.insert(id.to_owned(), *side);
                    }
                }
                for item in &data {
                    if let Some(id) = list_thread(item)["id"].as_str() {
                        self.threads.entry(id.to_owned()).or_insert(Side::Codex);
                    }
                }
                // Each facade thread belongs to the page whose Codex sort
                // values bracket it, so cursors stay Codex-driven and stable.
                let end = if last_page {
                    None
                } else {
                    data.last().map(|item| sort_value(item, field))
                };
                let in_window = |value: i64| {
                    let after_previous = state.boundary.is_none_or(|boundary| {
                        if descending {
                            value < boundary
                        } else {
                            value > boundary
                        }
                    });
                    let before_end = end.is_none_or(|end| {
                        if descending {
                            value >= end
                        } else {
                            value <= end
                        }
                    });
                    after_previous && before_end
                };
                let mut merged: Vec<Value> = data
                    .into_iter()
                    .filter(|item| {
                        list_thread(item)["id"]
                            .as_str()
                            .is_none_or(|id| self.threads.get(id) == Some(&Side::Codex))
                    })
                    .collect();
                // The facade's thread/list predates the originator filter.
                let originators = state.params["originators"]
                    .as_array()
                    .filter(|list| !list.is_empty());
                for (_, thread) in state.facade_items {
                    let placed = if section_order {
                        last_page
                    } else {
                        in_window(sort_value(&thread, field))
                    };
                    if originators.is_none_or(|list| list.contains(&thread["originator"])) && placed
                    {
                        merged.push(thread);
                    }
                }
                if !section_order {
                    merged.sort_by_key(|item| {
                        let value = sort_value(item, field);
                        if descending { -value } else { value }
                    });
                }
                // Receipts come from each thread's own backend, for this page's
                // threads only. Null still means Codex's receipts are unavailable.
                if !state.facade_read_states.is_empty() || result["readStates"].is_object() {
                    let mut read_states = result["readStates"]
                        .as_object()
                        .cloned()
                        .unwrap_or_default();
                    for thread in &merged {
                        if let Some(id) = list_thread(thread)["id"].as_str()
                            && let Some(read_state) = state.facade_read_states.get(id)
                        {
                            read_states.insert(id.to_owned(), read_state.clone());
                        }
                    }
                    result["readStates"] = json!(read_states);
                }
                result["data"] = json!(merged);
                self.decorate_agent_listing(&state.params, &mut result);
                if search {
                    for entry in result["data"].as_array_mut().unwrap() {
                        if !entry["thread"].is_object() {
                            let snippet = entry["name"]
                                .as_str()
                                .or_else(|| entry["preview"].as_str())
                                .unwrap_or("")
                                .to_owned();
                            *entry = json!({"thread": entry.take(), "snippet": snippet});
                        }
                    }
                }
                if !search
                    && agents::shared_listing(&state.params)
                    && let Err(failure) = agents::paginate_listing(&state.params, &mut result)
                {
                    return error(state.client_id, failure.to_string());
                }
                if !last_page {
                    let boundary = end.or(state.boundary);
                    result["nextCursor"] = json!(encode_cursor(&result["nextCursor"], boundary));
                }
                if result.get("backwardsCursor").is_some() {
                    result["backwardsCursor"] = Value::Null;
                }
            }
        }
        json!({"id": state.client_id, "result": result})
    }

    fn backend_message(&mut self, side: Side, mut message: Value) {
        if side == Side::Codex {
            self.refresh_local_provider_routing();
        }
        if side == Side::Codex && !self.local_provider_incoming(&mut message) {
            return;
        }
        let method = message["method"].as_str().map(str::to_owned);
        let id = message.get("id").cloned();
        match (method, id) {
            (None, Some(id)) => self.backend_response(side, id, message),
            (Some(_), Some(id)) => {
                self.learn(side, &message);
                if self.agent_dynamic_call(side, &message) {
                    return;
                }
                if let Some(prefix) = side.request_prefix() {
                    let request_id = format!("{prefix}{id}");
                    if let Some(thread) = message["params"]["threadId"].as_str() {
                        self.facade_requests
                            .insert(request_id.clone(), (side, thread.to_owned()));
                    }
                    message["id"] = json!(request_id);
                }
                self.send_client(message);
            }
            (Some(method), None) => {
                let ids = thread_ids(&message);
                if side != Side::Codex && ids.is_empty() {
                    // Facade-wide plugin events duplicate genuine Codex's own.
                    return;
                }
                if ids
                    .iter()
                    .any(|id| self.threads.get(id).is_some_and(|owner| *owner != side))
                {
                    // A thread that moved to a facade no longer belongs to Codex.
                    return;
                }
                self.learn(side, &message);
                if method == "thread/deleted" {
                    for id in &ids {
                        self.forget_thread(id);
                    }
                }
                self.agent_notification(side, &message);
                self.decorate_agent_message(&mut message);
                if side == Side::Codex && method == "turn/started" {
                    for id in &ids {
                        self.fresh.remove(id);
                    }
                }
                if method == "serverRequest/resolved"
                    && let Some(prefix) = side.request_prefix()
                    && let Some(request) = message["params"].get("requestId").cloned()
                {
                    let request_id = format!("{prefix}{request}");
                    self.facade_requests.remove(&request_id);
                    message["params"]["requestId"] = json!(request_id);
                }
                if side != Side::Codex
                    && let Some(thread) = message["params"]["threadId"].as_str()
                {
                    match method.as_str() {
                        "turn/started" => {
                            self.facade_turns
                                .insert(thread.to_owned(), message["params"]["turn"].clone());
                        }
                        "turn/completed" => {
                            self.facade_turns.remove(thread);
                        }
                        _ => {}
                    }
                }
                self.send_client(message);
            }
            (None, None) => {}
        }
    }

    fn learn(&mut self, side: Side, message: &Value) {
        for id in thread_ids(message) {
            self.threads.entry(id).or_insert(side);
        }
    }

    fn backend_response(&mut self, side: Side, id: Value, mut message: Value) {
        let result = match message.get("error") {
            Some(error) => Err(error.clone()),
            None => Ok(message["result"].clone()),
        };
        if let Ok(result) = &result {
            self.remember_agent_snapshot(result);
        }
        if let Some(key) = id.as_str()
            && self
                .internal
                .get(key)
                .is_some_and(|(owner, _)| *owner == side)
        {
            let (_, continuation) = self.internal.remove(key).unwrap();
            self.continue_with(side, continuation, result);
            return;
        }
        let forwarded = self.forwarded.remove(&(side, id_key(&id)));
        if let Some(forwarded) = &forwarded {
            if side == Side::Codex
                && let Ok(result) = &result
            {
                self.remember_local_provider_result(forwarded, result);
                if forwarded.method == "initialize" && self.local_provider.is_some() {
                    self.internal_request(
                        Side::Codex,
                        "config/read",
                        json!({"includeLayers":false,"cwd":null}),
                        Continuation::LocalProviderDefaults,
                    );
                }
            }
            if side == Side::Codex
                && result.is_ok()
                && forwarded.method == "thread/settings/update"
                && let Some(fresh) = forwarded.params["threadId"]
                    .as_str()
                    .and_then(|thread| self.fresh.get_mut(thread))
            {
                remember_permissions(&mut fresh.permissions, &forwarded.params);
                if let Some(settings) = forwarded.params.as_object() {
                    for (key, value) in settings {
                        if key != "threadId" {
                            fresh.settings[key] = value.clone();
                        }
                    }
                }
            }
            if result.is_ok()
                && matches!(
                    forwarded.method.as_str(),
                    "thread/unsubscribe" | "thread/archive"
                )
                && let Some(thread) = forwarded.params["threadId"].as_str()
            {
                self.agent_thread_unloaded(thread);
                // Its later turn events no longer reach this connection.
                self.facade_turns.remove(thread);
            }
            if result.is_ok()
                && forwarded.method == "thread/delete"
                && let Some(thread) = forwarded.params["threadId"].as_str()
            {
                self.forget_thread(thread);
            }
            if side == Side::Codex
                && let Ok(result) = &result
            {
                match forwarded.method.as_str() {
                    "threadSection/update" => self.section_changed(
                        result["section"]["id"].clone(),
                        result["section"].clone(),
                    ),
                    "threadSection/delete" => {
                        self.section_changed(forwarded.params["sectionId"].clone(), Value::Null)
                    }
                    _ => {}
                }
            }
            if result
                .as_ref()
                .is_ok_and(|result| result["turn"]["status"] == "inProgress")
                && forwarded.method == "turn/start"
            {
                self.agent_notification(side,&json!({"method":"turn/started","params":{"threadId":forwarded.params["threadId"],"turn":message["result"]["turn"]}}));
            }
            if side != Side::Codex
                && forwarded.method == "model/list"
                && let Some(models) = message
                    .pointer_mut("/result/data")
                    .and_then(Value::as_array_mut)
            {
                models.retain(|model| self.selectable_model(side, model));
                if side == Side::Acp
                    && let Some(config) = &self.acp
                {
                    for model in models {
                        model["isDefault"] = json!(model["id"] == config.model.as_str());
                    }
                }
            }
            self.decorate_agent_pages(&forwarded.method, &forwarded.params, &mut message);
            if side == Side::Claude
                && forwarded.method == "thread/resume"
                && message["result"]["claudexPredictionFork"] == true
                && let Some(thread) = message["result"]["thread"]["id"].as_str()
                && let Some(provider) = message["result"]["modelProvider"].as_str()
            {
                self.prediction_providers
                    .insert(thread.into(), provider.into());
                // The unmodified desktop gates composer predictions on this
                // top-level field. Keep the actual model, nested thread provider,
                // persisted metadata and backend routing unchanged.
                message["result"]["modelProvider"] = json!("openai");
            }
        }
        if side != Side::Codex
            && let (Some(forwarded), Err(error)) = (&forwarded, &result)
        {
            note!("{} {} failed: {error}", side.name(), forwarded.method);
        }
        if let Some(forwarded) = forwarded
            && let Ok(result) = &result
            && let Some(thread) = result["thread"]["id"].as_str()
        {
            if let Some(scope) = &forwarded.delegation_scope {
                self.agents.scopes.insert(scope.clone(), thread.to_owned());
                self.agents.attached.insert(thread.to_owned());
            }
            if side == Side::Claude && forwarded.method == "thread/resume" {
                self.internal_request(
                    Side::Claude,
                    "claudex/delegation/attach",
                    json!({"threadId":thread}),
                    Continuation::Ignore,
                );
            }
            self.threads.insert(thread.to_owned(), side);
            if side != Side::Codex
                && matches!(forwarded.method.as_str(), "thread/start" | "thread/resume")
            {
                self.facade_loaded.insert(thread.to_owned());
            }
            if side == Side::Codex && forwarded.method == "thread/start" {
                self.fresh.insert(
                    thread.to_owned(),
                    FreshThread {
                        params: forwarded.params,
                        metadata: result["thread"].clone(),
                        permissions: json!({}),
                        settings: json!({}),
                    },
                );
            } else if side == Side::Codex
                && forwarded.method == "thread/metadata/update"
                && let Some(fresh) = self.fresh.get_mut(thread)
            {
                fresh.metadata = result["thread"].clone();
            }
        }
        self.decorate_agent_message(&mut message);
        self.send_client(message);
    }

    fn continue_with(
        &mut self,
        side: Side,
        continuation: Continuation,
        result: Result<Value, Value>,
    ) {
        match continuation {
            Continuation::Ignore => {}
            Continuation::LocalProviderDefaults => {
                if let Ok(result) = result {
                    self.codex_provider = result["config"]["model_provider"]
                        .as_str()
                        .unwrap_or("openai")
                        .into();
                }
            }
            Continuation::LocalProviderList { mut request } => match result {
                Ok(result) => {
                    self.codex_provider = result["config"]["model_provider"]
                        .as_str()
                        .unwrap_or("openai")
                        .into();
                    request["params"]["modelProviders"] =
                        json!([self.codex_provider, crate::local_provider::PROVIDER]);
                    self.dispatch(request);
                }
                Err(e) => self.send_client(json!({"id":request["id"],"error":e})),
            },
            Continuation::LocalProviderProbe { thread_id, request } => {
                self.local_provider_probe_received(thread_id, request, result)
            }
            Continuation::LocalProviderStart { replacement } => {
                self.local_provider_start_received(replacement, result)
            }
            Continuation::LocalProviderSettings { replacement } => match result {
                Ok(_) => self.local_provider_restore_metadata(replacement),
                Err(e) => self.local_provider_replacement_failed(replacement, e),
            },
            Continuation::LocalProviderMetadata { mut replacement } => match result {
                Ok(result) => {
                    if result["thread"].is_object() {
                        replacement.started.as_mut().unwrap()["thread"] = result["thread"].clone();
                    }
                    self.local_provider_replacement_ready(replacement);
                }
                Err(e) => self.local_provider_replacement_failed(replacement, e),
            },
            Continuation::Probe {
                thread_id,
                request,
                remaining,
            } => {
                let blocked = thread_id.clone();
                match result {
                    Ok(_) => {
                        self.threads.insert(thread_id, side);
                        self.dispatch(request);
                    }
                    Err(failure)
                        if failure["code"] == -32602
                            && failure["message"]
                                .as_str()
                                .is_some_and(|m| m.starts_with("Thread not found:")) =>
                    {
                        self.probe_thread(thread_id, request, remaining);
                        return;
                    }
                    Err(failure) => self.send_client(json!({"id":request["id"],"error":failure})),
                }
                self.unblock_thread(&blocked, Block::Probe);
            }
            Continuation::FacadeModels => {
                let models: Vec<Value> = match result {
                    Ok(result) => result["data"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter(|entry| self.selectable_model(side, entry))
                        .map(|model| {
                            let mut model = model.clone();
                            model["isDefault"] = json!(false);
                            model
                        })
                        .collect(),
                    Err(error) => {
                        note!("{} model catalog failed: {error}", side.name());
                        vec![]
                    }
                };
                if side == Side::Claude {
                    for model in &models {
                        if let Some(id) = model["id"].as_str() {
                            self.known_claude_models.insert(id.to_owned());
                        }
                    }
                }
                self.facades.get_mut(&side).unwrap().models = Some(models);
                self.finish_merges();
                if side == Side::Claude {
                    self.unblock(Block::Models);
                }
            }
            Continuation::Adopt { thread_id, turn } => {
                let blocked = thread_id.clone();
                match result {
                    Ok(_) => {
                        self.fresh.remove(&thread_id);
                        self.threads.insert(thread_id.clone(), side);
                        self.facade_loaded.insert(thread_id.clone());
                        self.forward(side, turn);
                        self.internal_request(
                            Side::Codex,
                            "thread/unsubscribe",
                            json!({"threadId": thread_id}),
                            Continuation::Ignore,
                        );
                    }
                    Err(failure) => {
                        self.send_client(json!({"id": turn["id"], "error": failure}));
                    }
                }
                self.unblock_thread(&blocked, Block::Adopt);
            }
            Continuation::Resume { thread_id, request } => {
                let blocked = thread_id.clone();
                match result {
                    Ok(_) => {
                        self.facade_loaded.insert(thread_id);
                        self.forward(side, request);
                    }
                    Err(failure) => self.send_client(json!({"id":request["id"],"error":failure})),
                }
                self.unblock_thread(&blocked, Block::Resume);
            }
            Continuation::Section {
                side,
                thread_id,
                mut request,
            } => {
                let wanted = request["params"]["sectionId"]
                    .as_str()
                    .unwrap_or("")
                    .to_owned();
                match result {
                    Ok(page) => {
                        let section = page["data"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .find(|section| section["id"] == wanted.as_str())
                            .cloned();
                        match (section, page["nextCursor"].as_str()) {
                            (Some(section), _) => {
                                request["params"]["claudexSection"] = section;
                                self.forward(side, request);
                            }
                            (None, Some(cursor)) => {
                                // The thread stays blocked while the next page is read.
                                self.internal_request(
                                    Side::Codex,
                                    "threadSection/list",
                                    json!({"cursor": cursor, "limit": 100}),
                                    Continuation::Section {
                                        side,
                                        thread_id,
                                        request,
                                    },
                                );
                                return;
                            }
                            (None, None) => self.send_client(error(
                                request["id"].clone(),
                                format!("section {wanted} does not exist"),
                            )),
                        }
                    }
                    Err(failure) => self.send_client(json!({"id":request["id"],"error":failure})),
                }
                self.unblock_thread(&thread_id, Block::Section);
            }
            Continuation::StartDefault { mut request } => {
                match result {
                    Ok(result) => match result["config"]["model"].as_str() {
                        Some(model) if request["params"].is_object() => {
                            request["params"]["model"] = json!(model);
                            self.dispatch(request);
                        }
                        _ => self.forward(Side::Codex, request),
                    },
                    Err(failure) => self.send_client(json!({"id":request["id"],"error":failure})),
                }
                self.unblock(Block::DefaultModel);
            }
            Continuation::Merge { merge, side } => self.merge_part(merge, side, result),
            Continuation::Delegation { side, step } => self.agent_continue(side, step, result),
        }
    }

    /// Fail pending work for the disconnected facade without changing ownership.
    fn facade_failed(&mut self, side: Side) {
        let facade = self.facades.get_mut(&side).unwrap();
        if facade.sender.take().is_none() {
            return;
        }
        *facade = Facade::default();
        note!("lost the {} facade connection", side.name());
        self.agent_backend_failed(side);
        self.facade_loaded
            .retain(|thread| self.threads.get(thread) != Some(&side));
        let failed: Vec<(Side, String)> = self
            .forwarded
            .keys()
            .filter(|(owner, _)| *owner == side)
            .cloned()
            .collect();
        for key in failed {
            self.forwarded.remove(&key);
            if let Ok(id) = serde_json::from_str::<Value>(&key.1) {
                self.send_client(error(id, format!("{} disconnected", side.service())));
            }
        }
        let pending: Vec<String> = self
            .internal
            .iter()
            .filter(|(_, (owner, _))| *owner == side)
            .map(|(id, _)| id.clone())
            .collect();
        let failure =
            json!({"code": -32000, "message": format!("{} disconnected", side.service())});
        for id in pending {
            let (_, continuation) = self.internal.remove(&id).unwrap();
            self.continue_with(side, continuation, Err(failure.clone()));
        }
        self.finish_merges();
        // The facade ends its turns on shutdown, but that notification cannot
        // reach the desktop once the connection is gone, and a crash sends none.
        // Its saved record marks the same turns interrupted when it restarts.
        let requests: Vec<String> = self
            .facade_requests
            .iter()
            .filter(|(_, (owner, _))| *owner == side)
            .map(|(id, _)| id.clone())
            .collect();
        for request_id in requests {
            let (_, thread_id) = self.facade_requests.remove(&request_id).unwrap();
            self.send_client(json!({"method":"serverRequest/resolved",
                "params":{"threadId":thread_id,"requestId":request_id}}));
        }
        let completed_at = crate::protocol::now();
        let threads: Vec<String> = self
            .facade_turns
            .keys()
            .filter(|&thread| self.threads.get(thread) == Some(&side))
            .cloned()
            .collect();
        for thread_id in threads {
            let mut turn = self.facade_turns.remove(&thread_id).unwrap();
            turn["status"] = json!("interrupted");
            turn["error"] = json!({"message":format!("{} stopped while this turn was running", side.service()),
                "codexErrorInfo":null,"additionalDetails":null,"misalignment":null});
            // Items already streamed to the desktop stay as they are.
            turn["items"] = json!([]);
            turn["itemsView"] = json!("notLoaded");
            turn["completedAt"] = json!(completed_at);
            if let Some(started_at) = turn["startedAt"].as_u64() {
                turn["durationMs"] = json!(completed_at.saturating_sub(started_at) * 1000);
            }
            self.send_client(
                json!({"method":"turn/completed","params":{"threadId":thread_id,"turn":turn}}),
            );
            self.send_client(json!({"method":"thread/status/changed",
                "params":{"threadId":thread_id,"status":{"type":"idle"}}}));
        }
    }

    fn facade_connected(&mut self, side: Side, sender: mpsc::Sender<Value>) {
        note!("reconnected to {}", side.name());
        let facade = self.facades.get_mut(&side).unwrap();
        facade.sender = Some(sender);
        if let Some(params) = self.initialize_params.clone() {
            facade.initialized = true;
            self.internal_request(side, "initialize", params, Continuation::Ignore);
            self.send_backend(side, json!({"method":"initialized","params":{}}));
            self.request_facade_models(side);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| (*arg).to_owned()).collect()
    }

    #[test]
    fn recognizes_desktop_stdio_and_default_ssh_proxy_launches() {
        assert!(is_app_server_launch(&strings(&[
            "-c",
            "features.code_mode_host=true",
            "app-server",
            "--analytics-default-enabled",
            "-c",
            "chatgpt_base_url=\"x\"",
        ])));
        assert!(is_app_server_launch(&strings(&["app-server"])));
        assert!(is_app_server_launch(&strings(&["app-server", "proxy"])));
        for other in [
            vec!["--version"],
            vec!["exec", "hi"],
            vec!["app-server", "proxy", "--sock", "/custom.sock"],
            vec!["app-server", "--listen", "ws://127.0.0.1:1"],
            vec!["app-server", "generate-json-schema", "--out", "x"],
            vec!["-c"],
        ] {
            assert!(!is_app_server_launch(&strings(&other)), "{other:?}");
        }
    }

    #[test]
    fn cursors_round_trip_and_pass_foreign_cursors_through() {
        let cursor = encode_cursor(&json!("codex-page-2"), Some(17));
        assert_eq!(
            decode_cursor(&json!(cursor)),
            (json!("codex-page-2"), Some(17))
        );
        assert_eq!(decode_cursor(&json!("raw")), (json!("raw"), None));
    }

    #[tokio::test]
    async fn failed_ownership_probe_never_forwards_a_turn_to_codex() {
        let (client, mut replies) = mpsc::channel(16);
        let (codex, mut codex_requests) = mpsc::channel(16);
        let (claude, mut claude_requests) = mpsc::channel(16);
        let mut router = Router::new(
            client,
            codex,
            HashMap::from([(
                Side::Claude,
                Facade {
                    sender: Some(claude),
                    ..Facade::default()
                },
            )]),
            None,
        );
        router.client_message(json!({"id":0,"method":"initialize","params":{}}));
        assert_eq!(codex_requests.recv().await.unwrap()["method"], "initialize");
        assert_eq!(
            claude_requests.recv().await.unwrap()["method"],
            "initialize"
        );
        router.client_message(json!({"id":1,"method":"turn/start","params":{
            "threadId":"unknown-owner","input":[{"type":"text","text":"first message","text_elements":[]}]}}));
        assert_eq!(
            claude_requests.recv().await.unwrap()["method"],
            "thread/read"
        );
        router.facade_failed(Side::Claude);
        let failure = replies.recv().await.unwrap();
        assert_eq!(failure["id"], 1);
        assert_eq!(failure["error"]["code"], -32000);
        assert!(
            codex_requests.try_recv().is_err(),
            "Ownership failure forwarded a turn to Codex"
        );
        assert!(!router.threads.contains_key("unknown-owner"));
    }

    #[tokio::test]
    async fn a_pending_facade_round_trip_holds_only_its_own_thread() {
        let (client, _replies) = mpsc::channel(16);
        let (codex, mut codex_requests) = mpsc::channel(16);
        let (claude, mut claude_requests) = mpsc::channel(16);
        let mut router = Router::new(
            client,
            codex,
            HashMap::from([(
                Side::Claude,
                Facade {
                    sender: Some(claude),
                    ..Facade::default()
                },
            )]),
            None,
        );
        router.client_message(json!({"id":0,"method":"initialize","params":{}}));
        assert_eq!(codex_requests.recv().await.unwrap()["method"], "initialize");
        assert_eq!(
            claude_requests.recv().await.unwrap()["method"],
            "initialize"
        );
        router.client_message(json!({"id":1,"method":"turn/start","params":{
            "threadId":"slow","input":[{"type":"text","text":"first","text_elements":[]}]}}));
        let probe = claude_requests.recv().await.unwrap();
        assert_eq!(probe["method"], "thread/read");
        router.client_message(json!({"id":2,"method":"thread/read","params":{"threadId":"slow"}}));
        // A slow facade must not hold up genuine Codex or other chats.
        router.client_message(json!({"id":3,"method":"config/read","params":{}}));
        assert_eq!(codex_requests.try_recv().unwrap()["id"], 3);
        router.client_message(json!({"id":4,"method":"thread/read","params":{"threadId":"other"}}));
        assert_eq!(
            claude_requests.try_recv().unwrap()["params"]["threadId"],
            "other"
        );
        assert!(claude_requests.try_recv().is_err());
        router.backend_message(
            Side::Claude,
            json!({"id":probe["id"],"error":{"code":-32602,"message":"Thread not found: slow"}}),
        );
        // The thread's own requests then follow in their original order.
        assert_eq!(codex_requests.try_recv().unwrap()["id"], 1);
        assert_eq!(codex_requests.try_recv().unwrap()["id"], 2);
    }
}
