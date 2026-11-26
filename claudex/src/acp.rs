//! Agent Client Protocol (v1) agents behind the Claude control contract.
//!
//! The server drives every backend with Claude stream-json frames: user prompts,
//! permission and MCP control requests, replay acknowledgements and a terminal
//! `result`. This module speaks ACP to the agent and keeps that contract, so
//! turn lifecycle, approvals, steering and desktop tools work unchanged. Item
//! content is forwarded as `{"type":"acp","update":..}` frames, which the
//! translator maps to Codex items without passing through Claude's tool names.
use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt},
    process::{ChildStdin, Command},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::{
    codec::{FramedRead, LinesCodec},
    sync::CancellationToken,
};

use crate::backend::{
    BackendConfig, BackendEvent, FACADE_ENVIRONMENT, MAX_FRAME, ProcessGroupGuard, STDERR_TAIL,
    SessionOptions, kill_group,
};

/// An ACP agent this process (or an installation) serves instead of Claude.
#[derive(Clone, Debug)]
pub struct AcpAgent {
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub provider_id: String,
    pub provider_name: String,
    pub model: Option<String>,
    /// Recorded in an installed service's launcher.
    pub initialize_timeout_seconds: u64,
}

impl AcpAgent {
    /// The provider id defaults to the agent executable's name.
    pub fn new(
        executable: PathBuf,
        args: Vec<String>,
        provider_id: Option<String>,
        provider_name: Option<String>,
        model: Option<String>,
        initialize_timeout_seconds: u64,
    ) -> Result<Self> {
        let provider_id = match provider_id {
            Some(id) => id,
            None => executable
                .file_stem()
                .and_then(|name| name.to_str())
                .context("Cannot derive a provider id from the ACP agent; pass --provider-id")?
                .to_lowercase(),
        };
        if provider_id.is_empty()
            || !provider_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            bail!("--provider-id must contain only letters, digits, - and _");
        }
        Ok(Self {
            provider_name: provider_name.unwrap_or_else(|| provider_id.clone()),
            executable,
            args,
            provider_id,
            model,
            initialize_timeout_seconds,
        })
    }
}

pub const PROTOCOL_VERSION: u64 = 1;
/// Codex reasoning efforts an agent's thought-level values can map onto.
const EFFORTS: &[&str] = &["low", "medium", "high", "xhigh", "max"];
/// Model setting that keeps the agent's own default selection.
pub const DEFAULT_MODEL: &str = "default";
/// The stdio MCP server name under which desktop tools reach the agent.
const DESKTOP_SERVER: &str = "codex_desktop";

type Pending = Arc<Mutex<HashMap<i64, oneshot::Sender<Result<Value, RpcFailure>>>>>;

/// A JSON-RPC error returned by the agent, or a transport failure (code None).
#[derive(Debug, Clone)]
pub struct RpcFailure {
    pub code: Option<i64>,
    pub message: String,
}

impl std::fmt::Display for RpcFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RpcFailure {}

enum Incoming {
    Message(Value),
    Closed { success: bool, message: String },
}

/// The client side of one agent's JSON-RPC stream.
struct Connection {
    stdin: tokio::sync::Mutex<Option<ChildStdin>>,
    pending: Pending,
    next_id: AtomicI64,
}

impl Connection {
    async fn write(&self, mut message: Value) -> Result<()> {
        message["jsonrpc"] = json!("2.0");
        let mut bytes = serde_json::to_vec(&message)?;
        if bytes.len() > MAX_FRAME {
            bail!("ACP message exceeds {MAX_FRAME} bytes");
        }
        bytes.push(b'\n');
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut stdin = self.stdin.lock().await;
            let stdin = stdin.as_mut().context("ACP agent stdin is closed")?;
            stdin
                .write_all(&bytes)
                .await
                .context("writing ACP agent stdin")?;
            stdin.flush().await.context("flushing ACP agent stdin")
        })
        .await
        .context("ACP agent stdin write timed out")?
    }

    async fn request(&self, method: &str, params: Value) -> Result<Value, RpcFailure> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        if let Err(error) = self
            .write(json!({"id":id,"method":method,"params":params}))
            .await
        {
            self.pending.lock().unwrap().remove(&id);
            return Err(RpcFailure {
                code: None,
                message: format!("{error:#}"),
            });
        }
        rx.await.unwrap_or_else(|_| {
            Err(RpcFailure {
                code: None,
                message: "ACP agent connection closed".into(),
            })
        })
    }

    async fn notify(&self, method: &str, params: Value) -> Result<()> {
        self.write(json!({"method":method,"params":params})).await
    }

    async fn respond(&self, id: &Value, result: Value) -> Result<()> {
        self.write(json!({"id":id,"result":result})).await
    }

    async fn respond_error(&self, id: &Value, code: i64, message: &str) -> Result<()> {
        self.write(json!({"id":id,"error":{"code":code,"message":message}}))
            .await
    }
}

/// What the agent negotiated during initialize.
#[derive(Clone, Debug, Default)]
struct Agent {
    capabilities: Value,
    auth_methods: Value,
    info: Value,
}

impl Agent {
    fn session_capability(&self, name: &str) -> bool {
        self.capabilities["sessionCapabilities"][name].is_object()
    }
}

pub struct AcpBackend {
    input: mpsc::Sender<Value>,
    connection: Arc<Connection>,
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    process_id: u32,
    alive: Arc<AtomicBool>,
    initialization: Value,
    session_id: String,
    agent: Agent,
    ephemeral: bool,
    bridge: Option<Bridge>,
}

impl AcpBackend {
    pub async fn spawn_cancellable(
        config: &BackendConfig,
        options: &SessionOptions,
        startup_cancel: CancellationToken,
    ) -> Result<(Self, mpsc::Receiver<BackendEvent>)> {
        if options.resume && options.fork_from.is_some() {
            bail!("Cannot resume and fork an ACP session simultaneously");
        }
        if options.resume_at.is_some() && options.fork_from.is_none() {
            bail!("ACP agents cannot rewind a session to an earlier message");
        }
        let mut command = Command::new(&config.executable);
        for key in FACADE_ENVIRONMENT {
            command.env_remove(key);
        }
        if let Some(environment) = options.native_settings["env"].as_object() {
            for (key, value) in environment {
                if let Some(value) = value.as_str() {
                    command.env(key, value);
                }
            }
        }
        command
            .args(&config.extra_args)
            .current_dir(&options.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .spawn()
            .with_context(|| format!("starting ACP agent {}", config.executable.display()))?;
        let process_id = child.id().context("ACP agent has no process ID")?;
        let stdin = child.stdin.take().context("missing ACP agent stdin")?;
        let stdout = child.stdout.take().context("missing ACP agent stdout")?;
        let mut stderr = child.stderr.take().context("missing ACP agent stderr")?;
        let connection = Arc::new(Connection {
            stdin: tokio::sync::Mutex::new(Some(stdin)),
            pending: Arc::default(),
            next_id: AtomicI64::new(0),
        });
        let cancel = CancellationToken::new();
        let alive = Arc::new(AtomicBool::new(true));
        let (incoming_tx, mut incoming) = mpsc::unbounded_channel();
        let reader = {
            let pending = connection.pending.clone();
            let cancel = cancel.clone();
            let alive = alive.clone();
            tokio::spawn(async move {
                let _process_guard = ProcessGroupGuard {
                    id: process_id,
                    alive: alive.clone(),
                };
                let tail = Arc::new(Mutex::new(Vec::<u8>::new()));
                let stderr_tail = tail.clone();
                let mut stderr_task = tokio::spawn(async move {
                    let mut buffer = [0; 4096];
                    while let Ok(count) = stderr.read(&mut buffer).await {
                        if count == 0 {
                            break;
                        }
                        let mut tail = stderr_tail.lock().unwrap();
                        tail.extend_from_slice(&buffer[..count]);
                        let excess = tail.len().saturating_sub(STDERR_TAIL);
                        tail.drain(..excess);
                    }
                });
                let mut lines = FramedRead::new(stdout, LinesCodec::new_with_max_length(MAX_FRAME));
                let outcome: Result<()> = async {
                    loop {
                        let line = tokio::select! {
                            _ = cancel.cancelled() => return Ok(()),
                            line = lines.next() => line,
                        };
                        let Some(line) = line else {
                            return Ok(());
                        };
                        let line = line.context("reading ACP agent stdout")?;
                        let trimmed = line.trim();
                        // Agents may log to stdout before or between frames.
                        if !trimmed.starts_with('{') {
                            continue;
                        }
                        let value: Value = serde_json::from_str(trimmed)
                            .context("malformed ACP JSON-RPC frame")?;
                        if value.get("method").is_none()
                            && (value.get("result").is_some() || value.get("error").is_some())
                        {
                            let Some(id) = value["id"].as_i64() else {
                                continue;
                            };
                            if let Some(sender) = pending.lock().unwrap().remove(&id) {
                                let result = match value.get("error") {
                                    Some(error) => Err(RpcFailure {
                                        code: error["code"].as_i64(),
                                        message: error["message"]
                                            .as_str()
                                            .unwrap_or("ACP request failed")
                                            .to_owned(),
                                    }),
                                    None => Ok(value["result"].clone()),
                                };
                                let _ = sender.send(result);
                            }
                        } else if value.get("method").is_some()
                            && incoming_tx.send(Incoming::Message(value)).is_err()
                        {
                            return Ok(());
                        }
                    }
                }
                .await;
                let status = if cancel.is_cancelled() {
                    None
                } else {
                    tokio::time::timeout(Duration::from_millis(500), child.wait())
                        .await
                        .ok()
                        .and_then(Result::ok)
                };
                kill_group(process_id);
                let _ = child.start_kill();
                let status = match status {
                    Some(status) => Some(status),
                    None => child.wait().await.ok(),
                };
                alive.store(false, Ordering::Release);
                if tokio::time::timeout(Duration::from_millis(250), &mut stderr_task)
                    .await
                    .is_err()
                {
                    stderr_task.abort();
                }
                let success = outcome.is_ok() && status.is_some_and(|s| s.success());
                let mut message = match outcome {
                    Ok(()) => format!(
                        "ACP agent exited: {}",
                        status
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| "unknown status".into())
                    ),
                    Err(error) => format!("{error:#}"),
                };
                let tail = String::from_utf8_lossy(&tail.lock().unwrap())
                    .trim()
                    .to_owned();
                if !tail.is_empty() {
                    message.push_str("; stderr: ");
                    message.push_str(&tail);
                }
                for (_, sender) in pending.lock().unwrap().drain() {
                    let _ = sender.send(Err(RpcFailure {
                        code: None,
                        message: message.clone(),
                    }));
                }
                let _ = incoming_tx.send(Incoming::Closed { success, message });
            })
        };
        let mut backend = Self {
            input: mpsc::channel(1).0,
            connection: connection.clone(),
            cancel: cancel.clone(),
            tasks: vec![reader],
            process_id,
            alive,
            initialization: Value::Null,
            session_id: String::new(),
            agent: Agent::default(),
            ephemeral: options.ephemeral,
            bridge: None,
        };
        let setup = tokio::select! {
            result = tokio::time::timeout(config.initialize_timeout, backend.setup(options)) =>
                result.unwrap_or_else(|_| Err(anyhow!("ACP agent did not finish initialization within {:?}", config.initialize_timeout))),
            _ = startup_cancel.cancelled() => Err(anyhow!("ACP agent startup cancelled")),
        };
        let session = match setup {
            Ok(session) => session,
            Err(error) => {
                // Prefer the agent's own exit diagnostic when it died during startup.
                let exit =
                    match tokio::time::timeout(Duration::from_millis(500), incoming.recv()).await {
                        Ok(Some(Incoming::Closed { message, .. })) => Some(message),
                        _ => None,
                    };
                backend.terminate().await?;
                return Err(match exit {
                    Some(message) => error.context(message),
                    None => error,
                });
            }
        };
        // Updates received while setting up the session (including a load
        // replay of earlier turns) belong to history the facade already has.
        while let Ok(message) = incoming.try_recv() {
            if let Incoming::Closed { message, .. } = message {
                backend.terminate().await?;
                bail!("ACP agent exited during session setup: {message}");
            }
        }
        let (events_tx, events) = mpsc::channel(128);
        let (input_tx, input) = mpsc::channel(64);
        backend.input = input_tx;
        events_tx
            .send(BackendEvent::Message(
                json!({"type":"system","subtype":"init","session_id":backend.session_id,"acp":backend.initialization["agent"]}),
            ))
            .await
            .map_err(|_| anyhow!("ACP event consumer closed"))?;
        let (bridge_calls, bridge_rx) = mpsc::channel(32);
        if let Some(bridge) = &backend.bridge {
            bridge.calls.set(bridge_calls).expect("new ACP bridge");
        }
        let (prompt_tx, prompt_rx) = mpsc::channel(1);
        let driver = Driver {
            connection,
            session_id: backend.session_id.clone(),
            events: events_tx,
            prompt_capabilities: backend.agent.capabilities["promptCapabilities"].clone(),
            instructions: session.instructions,
            queue: VecDeque::new(),
            running: false,
            prompts: prompt_tx,
            cancelled: false,
            permissions: HashMap::new(),
            mcp_routes: HashMap::new(),
            mcp_serial: 0,
            tool_state: HashMap::new(),
            desktop_tool_names: options
                .dynamic_tools
                .iter()
                .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
                .collect(),
            desktop_calls: HashSet::new(),
            context_window: None,
        };
        backend.tasks.push(tokio::spawn(
            driver.run(input, incoming, bridge_rx, prompt_rx, cancel),
        ));
        Ok((backend, events))
    }

    /// Initialize, then create, resume, load or fork the session and apply the
    /// requested model and effort. Returns per-session setup for the driver.
    async fn setup(&mut self, options: &SessionOptions) -> Result<SessionSetup> {
        let initialized = self
            .connection
            .request(
                "initialize",
                json!({"protocolVersion":PROTOCOL_VERSION,
                    "clientCapabilities":{"fs":{"readTextFile":false,"writeTextFile":false},"terminal":false},
                    "clientInfo":{"name":"claude-codex-server","title":"Codex","version":crate::protocol::VERSION}}),
            )
            .await
            .map_err(|error| anyhow!("ACP initialize failed: {error}"))?;
        if initialized["protocolVersion"].as_u64() != Some(PROTOCOL_VERSION) {
            bail!(
                "ACP agent negotiated protocol version {}; only version {PROTOCOL_VERSION} is supported",
                initialized["protocolVersion"]
            );
        }
        self.agent = Agent {
            capabilities: initialized["agentCapabilities"].clone(),
            auth_methods: initialized["authMethods"].clone(),
            info: initialized["agentInfo"].clone(),
        };
        let mcp_servers = if options.dynamic_tools.is_empty() {
            json!([])
        } else {
            // Agents may synchronously discover MCP tools during session setup.
            let bridge = Bridge::start(options.dynamic_tools.clone(), self.cancel.clone())?;
            let servers = json!([bridge.server_entry()?]);
            self.bridge = Some(bridge);
            servers
        };
        let cwd = options.cwd.to_string_lossy();
        let (method, params) = if let Some(source) = &options.fork_from {
            if !self.agent.session_capability("fork") {
                bail!("This ACP agent does not support forking sessions");
            }
            (
                "session/fork",
                json!({"sessionId":source,"cwd":cwd,"mcpServers":mcp_servers}),
            )
        } else if options.resume {
            if self.agent.session_capability("resume") {
                (
                    "session/resume",
                    json!({"sessionId":options.session_id,"cwd":cwd,"mcpServers":mcp_servers}),
                )
            } else if self.agent.capabilities["loadSession"] == true {
                (
                    "session/load",
                    json!({"sessionId":options.session_id,"cwd":cwd,"mcpServers":mcp_servers}),
                )
            } else {
                bail!(
                    "This ACP agent can neither resume nor load sessions, so a thread cannot continue past its first turn"
                );
            }
        } else {
            ("session/new", json!({"cwd":cwd,"mcpServers":mcp_servers}))
        };
        let session = self
            .connection
            .request(method, params)
            .await
            .map_err(|error| self.session_error(method, error))?;
        self.session_id = match method {
            "session/new" | "session/fork" => session["sessionId"]
                .as_str()
                .filter(|id| !id.is_empty())
                .with_context(|| format!("{method} returned no sessionId"))?
                .to_owned(),
            _ => options.session_id.clone(),
        };
        let mut state = SessionState::from_response(&session);
        if options.model != DEFAULT_MODEL {
            state = self.select_model(&state, &options.model).await?;
        }
        if let Some(effort) = &options.effort {
            state = self.select_effort(&state, effort).await?;
        }
        self.initialization = json!({
            "models": state.catalog(),
            "agent": {"agentInfo": self.agent.info, "agentCapabilities": self.agent.capabilities,
                "authMethods": self.agent.auth_methods},
        });
        Ok(SessionSetup {
            instructions: (method == "session/new")
                .then(|| instructions(options))
                .flatten(),
        })
    }

    fn session_error(&self, method: &str, error: RpcFailure) -> anyhow::Error {
        // ACP reserves -32000 for authentication required.
        if error.code == Some(-32000) {
            let methods: Vec<&str> = self
                .agent
                .auth_methods
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|method| method["name"].as_str().or(method["id"].as_str()))
                .collect();
            return anyhow!(
                "The ACP agent requires authentication ({}). Sign in with the agent's own CLI, then retry: {}",
                if methods.is_empty() {
                    "no methods advertised".to_owned()
                } else {
                    methods.join(", ")
                },
                error.message
            );
        }
        anyhow!("ACP {method} failed: {error}")
    }

    async fn select_model(&self, state: &SessionState, model: &str) -> Result<SessionState> {
        if let Some(option) = state.option("model") {
            if option["currentValue"] == model {
                return Ok(state.clone());
            }
            if !select_values(option).any(|value| value["value"] == model) {
                bail!("Model {model} is not offered by this ACP agent; choose one from model/list");
            }
            return self.set_option(state, option, model).await;
        }
        if state.models.is_object() {
            if state.models["currentModelId"] == model {
                return Ok(state.clone());
            }
            if !state.models["availableModels"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|entry| entry["modelId"] == model)
            {
                bail!("Model {model} is not offered by this ACP agent; choose one from model/list");
            }
            // Agents predating session config options expose this unstable method.
            self.connection
                .request(
                    "session/set_model",
                    json!({"sessionId":self.session_id,"modelId":model}),
                )
                .await
                .map_err(|error| anyhow!("ACP session/set_model failed: {error}"))?;
            let mut next = state.clone();
            next.models["currentModelId"] = json!(model);
            return Ok(next);
        }
        bail!("This ACP agent does not offer model selection; use model {DEFAULT_MODEL}")
    }

    /// Efforts map onto an agent's thought-level option by value. An agent
    /// without a matching value keeps its own reasoning default.
    async fn select_effort(&self, state: &SessionState, effort: &str) -> Result<SessionState> {
        let Some(option) = state.option("thought_level") else {
            return Ok(state.clone());
        };
        let Some(value) = select_values(option)
            .filter_map(|value| value["value"].as_str())
            .find(|value| value.eq_ignore_ascii_case(effort))
            .map(str::to_owned)
        else {
            return Ok(state.clone());
        };
        if option["currentValue"] == value.as_str() {
            return Ok(state.clone());
        }
        self.set_option(state, option, &value).await
    }

    async fn set_option(
        &self,
        state: &SessionState,
        option: &Value,
        value: &str,
    ) -> Result<SessionState> {
        let response = self
            .connection
            .request(
                "session/set_config_option",
                json!({"sessionId":self.session_id,"configId":option["id"],"value":value}),
            )
            .await
            .map_err(|error| anyhow!("ACP session/set_config_option failed: {error}"))?;
        let mut next = state.clone();
        if let Some(options) = response["configOptions"].as_array() {
            next.config_options = options.clone();
        } else if let Some(entry) = next
            .config_options
            .iter_mut()
            .find(|entry| entry["id"] == option["id"])
        {
            entry["currentValue"] = json!(value);
        }
        Ok(next)
    }

    pub fn is_running(&self) -> bool {
        !self.cancel.is_cancelled()
            && self.alive.load(Ordering::Acquire)
            && self.tasks.iter().all(|task| !task.is_finished())
    }

    pub fn initialization(&self) -> &Value {
        &self.initialization
    }

    pub async fn send(&self, message: Value) -> Result<()> {
        if self.cancel.is_cancelled() {
            bail!("ACP backend is closed");
        }
        self.input
            .send(message)
            .await
            .map_err(|_| anyhow!("ACP backend is closed"))
    }

    async fn delete_ephemeral(&self) {
        if self.ephemeral
            && !self.session_id.is_empty()
            && self.agent.session_capability("delete")
            && self.alive.load(Ordering::Acquire)
        {
            let _ = tokio::time::timeout(
                Duration::from_secs(2),
                self.connection
                    .request("session/delete", json!({"sessionId":self.session_id})),
            )
            .await;
        }
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.delete_ephemeral().await;
        // EOF lets the agent persist the session before it exits.
        if let Ok(mut stdin) =
            tokio::time::timeout(Duration::from_millis(250), self.connection.stdin.lock()).await
        {
            stdin.take();
        }
        // The reader finishes once the agent exits and closes stdout. Agents
        // persist the session on exit, and a resumed session lacks whatever a
        // killed agent did not save, so allow a slow (loaded) agent time.
        if let Some(reader) = self.tasks.first_mut()
            && tokio::time::timeout(Duration::from_secs(30), reader)
                .await
                .is_ok()
        {
            self.tasks.remove(0);
        }
        self.terminate().await
    }

    pub async fn terminate(&mut self) -> Result<()> {
        self.delete_ephemeral().await;
        self.cancel.cancel();
        if self.alive.load(Ordering::Acquire) {
            kill_group(self.process_id);
        }
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
        if let Ok(mut stdin) = self.connection.stdin.try_lock() {
            stdin.take();
        }
        drop(self.bridge.take());
        Ok(())
    }
}

impl Drop for AcpBackend {
    fn drop(&mut self) {
        self.cancel.cancel();
        if self.alive.load(Ordering::Acquire) {
            kill_group(self.process_id);
        }
    }
}

struct SessionSetup {
    instructions: Option<String>,
}

/// Session configuration as last reported by the agent.
#[derive(Clone, Debug, Default)]
struct SessionState {
    config_options: Vec<Value>,
    /// Unstable pre-config-option model state, still used by some agents.
    models: Value,
}

impl SessionState {
    fn from_response(response: &Value) -> Self {
        Self {
            config_options: response["configOptions"]
                .as_array()
                .cloned()
                .unwrap_or_default(),
            models: response["models"].clone(),
        }
    }

    fn option(&self, category: &str) -> Option<&Value> {
        self.config_options
            .iter()
            .find(|option| option["category"] == category && option["type"] == "select")
    }

    /// Models in the shape of Claude's initialize catalog, so model/list
    /// presents both backends identically.
    fn catalog(&self) -> Value {
        let efforts: Vec<String> = self
            .option("thought_level")
            .map(|option| {
                select_values(option)
                    .filter_map(|value| value["value"].as_str())
                    .map(str::to_ascii_lowercase)
                    .filter(|value| EFFORTS.contains(&value.as_str()))
                    .collect()
            })
            .unwrap_or_default();
        let entry = |value: &Value, name: &Value, description: &Value| {
            json!({"value": value, "displayName": name.as_str().or(value.as_str()),
                "description": description.as_str().unwrap_or(""), "supportedEffortLevels": efforts})
        };
        if let Some(option) = self.option("model") {
            return json!(
                select_values(option)
                    .map(|value| entry(&value["value"], &value["name"], &value["description"]))
                    .collect::<Vec<_>>()
            );
        }
        json!(
            self.models["availableModels"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|model| entry(&model["modelId"], &model["name"], &model["description"]))
                .collect::<Vec<_>>()
        )
    }
}

/// A select option's values, flattening grouped options.
fn select_values(option: &Value) -> impl Iterator<Item = &Value> {
    option["options"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|entry| match entry["options"].as_array() {
            Some(group) => group.iter().collect::<Vec<_>>(),
            None => vec![entry],
        })
}

/// ACP has no system prompt. Instructions are delivered once, ahead of the
/// first prompt of a new session; resumed sessions already contain them.
fn instructions(options: &SessionOptions) -> Option<String> {
    let parts: Vec<&str> = [&options.system_prompt, &options.append_system_prompt]
        .into_iter()
        .filter_map(|part| part.as_deref())
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect();
    (!parts.is_empty()).then(|| {
        format!(
            "<codex-instructions>\nThe Codex client supplied these session instructions. Follow them for this session.\n\n{}\n</codex-instructions>",
            parts.join("\n\n")
        )
    })
}

struct PermissionRequest {
    id: Value,
    options: Vec<Value>,
}

/// A desktop tool call routed from the MCP bridge to the server.
struct BridgeCall {
    message: Value,
    reply: mpsc::Sender<Value>,
}

struct Driver {
    connection: Arc<Connection>,
    session_id: String,
    events: mpsc::Sender<BackendEvent>,
    prompt_capabilities: Value,
    instructions: Option<String>,
    /// Steering input received while a prompt runs, sent as the next prompt.
    queue: VecDeque<Value>,
    /// Whether a session/prompt request is in flight; its result arrives on `prompts`.
    running: bool,
    prompts: mpsc::Sender<Result<Value, RpcFailure>>,
    cancelled: bool,
    permissions: HashMap<String, PermissionRequest>,
    mcp_routes: HashMap<String, (mpsc::Sender<Value>, Value)>,
    mcp_serial: u64,
    tool_state: HashMap<String, Value>,
    desktop_tool_names: Vec<String>,
    desktop_calls: HashSet<String>,
    context_window: Option<u64>,
}

impl Driver {
    async fn run(
        mut self,
        mut input: mpsc::Receiver<Value>,
        mut incoming: mpsc::UnboundedReceiver<Incoming>,
        mut bridge: mpsc::Receiver<BridgeCall>,
        mut prompts: mpsc::Receiver<Result<Value, RpcFailure>>,
        cancel: CancellationToken,
    ) {
        loop {
            // Biased: the reader queues an agent's updates before it resolves the
            // prompt they belong to, so draining them first keeps their order.
            let outcome: Result<bool> = tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                message = incoming.recv() => match message {
                    Some(Incoming::Message(message)) => self.agent_message(message).await.map(|_| true),
                    Some(Incoming::Closed { success, message }) => {
                        let _ = self.events.send(BackendEvent::Exited { success, message }).await;
                        return;
                    }
                    None => return,
                },
                frame = input.recv() => match frame {
                    Some(frame) => self.frame(frame).await.map(|_| true),
                    None => return,
                },
                // Without a bridge the channel is closed; the pattern disables the branch.
                Some(call) = bridge.recv() => self.bridge_call(call).await.map(|_| true),
                Some(done) = prompts.recv() => {
                    self.running = false;
                    self.prompt_finished(done).await
                }
            };
            match outcome {
                Ok(true) => {}
                Ok(false) => return,
                Err(error) => {
                    let _ = self
                        .events
                        .send(BackendEvent::Exited {
                            success: false,
                            message: format!("{error:#}"),
                        })
                        .await;
                    return;
                }
            }
        }
    }

    async fn emit(&self, message: Value) -> Result<()> {
        self.events
            .send(BackendEvent::Message(message))
            .await
            .map_err(|_| anyhow!("ACP event consumer closed"))
    }

    /// A Claude-shaped frame from the server.
    async fn frame(&mut self, frame: Value) -> Result<()> {
        match frame["type"].as_str() {
            Some("user") => {
                if self.running {
                    self.queue.push_back(frame);
                    Ok(())
                } else {
                    self.start_prompt(frame).await
                }
            }
            Some("control_response") => {
                let response = &frame["response"];
                let key = response["request_id"].as_str().unwrap_or("");
                if let Some(request) = self.permissions.remove(key) {
                    self.answer_permission(request, response).await
                } else if let Some((reply, id)) = self.mcp_routes.remove(key) {
                    let message = if response["subtype"] == "error" {
                        json!({"jsonrpc":"2.0","id":id,"error":{"code":-32603,"message":response["error"]}})
                    } else {
                        response["response"]["mcp_response"].clone()
                    };
                    // The bridge connection may have closed after a turn ended.
                    let _ = reply.send(message).await;
                    Ok(())
                } else {
                    Ok(())
                }
            }
            Some("control_request") if frame["request"]["subtype"] == "interrupt" => {
                self.interrupt().await
            }
            _ => Ok(()),
        }
    }

    async fn start_prompt(&mut self, frame: Value) -> Result<()> {
        let mut prompt =
            match prompt_content(&frame["message"]["content"], &self.prompt_capabilities) {
                Ok(prompt) => prompt,
                Err(error) => {
                    return self
                        .emit(result_frame(false, Some(&error), None, self.context_window))
                        .await;
                }
            };
        if let Some(instructions) = self.instructions.take() {
            prompt.insert(0, json!({"type":"text","text":instructions}));
        }
        // The replay acknowledgement tells the turn runner this input was consumed.
        self.emit(json!({"type":"user","isReplay":true,"uuid":frame["uuid"],
            "parent_tool_use_id":null,"message":{"role":"user","content":[]}}))
            .await?;
        let connection = self.connection.clone();
        let prompts = self.prompts.clone();
        let params = json!({"sessionId":self.session_id,"prompt":prompt});
        tokio::spawn(async move {
            let _ = prompts
                .send(connection.request("session/prompt", params).await)
                .await;
        });
        self.running = true;
        Ok(())
    }

    async fn prompt_finished(&mut self, done: Result<Value, RpcFailure>) -> Result<bool> {
        match done {
            Ok(response) => {
                let stop = response["stopReason"].as_str().unwrap_or("end_turn");
                if stop == "end_turn"
                    && !self.cancelled
                    && let Some(next) = self.queue.pop_front()
                {
                    self.start_prompt(next).await?;
                    return Ok(true);
                }
                self.queue.clear();
                let error = match stop {
                    "end_turn" => None,
                    "cancelled" => Some("The agent cancelled the turn".to_owned()),
                    "max_tokens" => Some("The agent stopped at its maximum token limit".to_owned()),
                    "max_turn_requests" => {
                        Some("The agent stopped at its maximum number of model requests".to_owned())
                    }
                    "refusal" => Some("The agent refused to continue".to_owned()),
                    other => Some(format!("The agent stopped: {other}")),
                };
                let interrupted = self.cancelled || stop == "cancelled";
                self.cancelled = false;
                self.emit(result_frame(
                    interrupted,
                    error.as_deref(),
                    Some(&response["usage"]),
                    self.context_window,
                ))
                .await?;
            }
            Err(error) => {
                self.queue.clear();
                let interrupted = std::mem::take(&mut self.cancelled);
                self.emit(result_frame(
                    interrupted,
                    Some(&format!("ACP session/prompt failed: {error}")),
                    None,
                    self.context_window,
                ))
                .await?;
            }
        }
        Ok(true)
    }

    async fn interrupt(&mut self) -> Result<()> {
        self.queue.clear();
        // The client must answer outstanding permission requests as cancelled.
        for (_, request) in self.permissions.drain() {
            let _ = self
                .connection
                .respond(&request.id, json!({"outcome":{"outcome":"cancelled"}}))
                .await;
        }
        if !self.running {
            return self
                .emit(result_frame(true, None, None, self.context_window))
                .await;
        }
        self.cancelled = true;
        self.connection
            .notify("session/cancel", json!({"sessionId":self.session_id}))
            .await
    }

    async fn agent_message(&mut self, message: Value) -> Result<()> {
        let method = message["method"].as_str().unwrap_or("");
        if let Some(id) = message.get("id").filter(|id| !id.is_null()) {
            return match method {
                "session/request_permission" => {
                    self.permission(id.clone(), &message["params"]).await
                }
                // Filesystem, terminal and elicitation capabilities are not advertised.
                _ => {
                    self.connection
                        .respond_error(
                            id,
                            -32601,
                            &format!("Method not supported by the Codex client: {method}"),
                        )
                        .await
                }
            };
        }
        match method {
            "session/update" if message["params"]["sessionId"] == self.session_id.as_str() => {
                self.update(message["params"]["update"].clone()).await
            }
            "$/cancel_request" => {
                let id = &message["params"]["requestId"];
                let key = self
                    .permissions
                    .iter()
                    .find(|(_, request)| request.id == *id)
                    .map(|(key, _)| key.clone());
                if let Some(key) = key {
                    self.permissions.remove(&key);
                    self.emit(json!({"type":"control_cancel_request","request_id":key}))
                        .await?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn update(&mut self, mut update: Value) -> Result<()> {
        match update["sessionUpdate"].as_str() {
            // Echoes of the client's own prompt; the facade records input itself.
            Some("user_message_chunk") | None => return Ok(()),
            Some("usage_update") => {
                self.context_window = update["size"].as_u64().or(self.context_window);
            }
            Some("tool_call" | "tool_call_update") => {
                let id = update["toolCallId"].as_str().unwrap_or("").to_owned();
                let state = self
                    .tool_state
                    .entry(id.clone())
                    .or_insert_with(|| json!({}));
                merge_tool(state, &update);
                if desktop_tool(state, &self.desktop_tool_names).is_some() {
                    self.desktop_calls.insert(id.clone());
                }
                if self.desktop_calls.contains(&id) {
                    // The server owns the lifecycle of bridged desktop tools.
                    update["desktopTool"] = json!(true);
                }
            }
            _ => {}
        }
        self.emit(json!({"type":"acp","update":update})).await
    }

    async fn permission(&mut self, id: Value, params: &Value) -> Result<()> {
        if !self.running || params["sessionId"] != self.session_id.as_str() {
            return self
                .connection
                .respond(&id, json!({"outcome":{"outcome":"cancelled"}}))
                .await;
        }
        let call = &params["toolCall"];
        let call_id = call["toolCallId"].as_str().unwrap_or("").to_owned();
        if !self.tool_state.contains_key(&call_id) {
            // Ensure the item exists before its approval is shown.
            let mut update = call.clone();
            update["sessionUpdate"] = json!("tool_call");
            if update["title"].as_str().is_none() {
                update["title"] = json!(call["name"].as_str().unwrap_or("Tool call"));
            }
            self.update(update).await?;
        } else {
            let mut update = call.clone();
            update["sessionUpdate"] = json!("tool_call_update");
            self.update(update).await?;
        }
        let state = self.tool_state[&call_id].clone();
        let (tool, input) = permission_subject(&state, &self.desktop_tool_names);
        let key = format!("acp-permission-{id}");
        self.permissions.insert(
            key.clone(),
            PermissionRequest {
                id,
                options: params["options"].as_array().cloned().unwrap_or_default(),
            },
        );
        self.emit(json!({"type":"control_request","request_id":key,"request":{
            "subtype":"can_use_tool","tool_name":tool,"input":input,"tool_use_id":call_id,
            "acp_tool_name":state["name"],"decision_reason":state["title"]}}))
            .await
    }

    async fn answer_permission(
        &mut self,
        request: PermissionRequest,
        response: &Value,
    ) -> Result<()> {
        let data = &response["response"];
        let allowed = response["subtype"] == "success" && data["behavior"] == "allow";
        let pick = |kinds: &[&str]| {
            kinds.iter().find_map(|kind| {
                request
                    .options
                    .iter()
                    .find(|option| option["kind"] == *kind)
                    .map(|option| option["optionId"].clone())
            })
        };
        let choice = if allowed {
            pick(&["allow_once", "allow_always"])
        } else if data["interrupt"] == true {
            None
        } else {
            pick(&["reject_once", "reject_always"])
        };
        let outcome = match choice {
            Some(option) => json!({"outcome":"selected","optionId":option}),
            None => json!({"outcome":"cancelled"}),
        };
        self.connection
            .respond(&request.id, json!({"outcome":outcome}))
            .await?;
        if !allowed && data["interrupt"] == true {
            self.interrupt().await?;
        }
        Ok(())
    }

    async fn bridge_call(&mut self, call: BridgeCall) -> Result<()> {
        self.mcp_serial += 1;
        let key = format!("acp-mcp-{}", self.mcp_serial);
        self.mcp_routes
            .insert(key.clone(), (call.reply, call.message["id"].clone()));
        self.emit(json!({"type":"control_request","request_id":key,"request":{
            "subtype":"mcp_message","server_name":DESKTOP_SERVER,"message":call.message}}))
            .await
    }
}

fn result_frame(
    interrupted: bool,
    error: Option<&str>,
    usage: Option<&Value>,
    context_window: Option<u64>,
) -> Value {
    let mut frame = json!({"type":"result",
        "subtype": if error.is_none() { "success" } else { "error_during_execution" },
        "is_error": error.is_some() && !interrupted, "result": "",
        "errors": error.into_iter().collect::<Vec<_>>()});
    if let Some(usage) = usage.filter(|usage| usage.is_object()) {
        let read = usage["cachedReadTokens"].as_u64().unwrap_or(0);
        let write = usage["cachedWriteTokens"].as_u64().unwrap_or(0);
        frame["usage"] = json!({
            "input_tokens": usage["inputTokens"].as_u64().unwrap_or(0).saturating_sub(read + write),
            "cache_read_input_tokens": read, "cache_creation_input_tokens": write,
            "output_tokens": usage["outputTokens"].as_u64().unwrap_or(0)});
    }
    if let Some(size) = context_window {
        frame["modelUsage"] = json!({"acp": {"contextWindow": size}});
    }
    frame
}

/// Convert Claude content blocks from the turn runner into ACP content blocks.
fn prompt_content(content: &Value, capabilities: &Value) -> Result<Vec<Value>, String> {
    let blocks = match content {
        Value::String(text) => return Ok(vec![json!({"type":"text","text":text})]),
        Value::Array(blocks) => blocks,
        _ => return Err("Prompt content must be text or content blocks".into()),
    };
    blocks
        .iter()
        .map(|block| match block["type"].as_str() {
            Some("text") => Ok(json!({"type":"text","text":block["text"]})),
            Some("image") => {
                let source = &block["source"];
                match source["type"].as_str() {
                    Some("base64") if capabilities["image"] == true => Ok(
                        json!({"type":"image","data":source["data"],"mimeType":source["media_type"]}),
                    ),
                    Some("base64") => Err("This ACP agent does not accept image input".into()),
                    Some("url") => {
                        let uri = source["url"].as_str().unwrap_or("");
                        Ok(json!({"type":"resource_link","uri":uri,
                            "name":uri.rsplit('/').next().filter(|name| !name.is_empty()).unwrap_or("image")}))
                    }
                    _ => Err("Unsupported image source".into()),
                }
            }
            _ => Err(format!("Unsupported prompt block {}", block["type"])),
        })
        .collect()
}

/// Fold a tool_call or tool_call_update into the accumulated call state.
pub fn merge_tool(state: &mut Value, update: &Value) {
    for (key, value) in update.as_object().into_iter().flatten() {
        if key != "sessionUpdate" && !value.is_null() {
            state[key] = value.clone();
        }
    }
}

/// The desktop tool a call invokes through the bridge, if any. Agents name MCP
/// tools differently (`mcp__codex_desktop__x`, `x (codex_desktop MCP Server)`),
/// but each includes the server and tool names.
pub fn desktop_tool(call: &Value, names: &[String]) -> Option<String> {
    let label = format!(
        "{} {}",
        call["name"].as_str().unwrap_or(""),
        call["title"].as_str().unwrap_or("")
    );
    if !label.contains(DESKTOP_SERVER) {
        return None;
    }
    names
        .iter()
        .filter(|name| label.contains(name.as_str()))
        .max_by_key(|name| name.len())
        .cloned()
}

/// The command text of an execute call.
pub fn command_text(call: &Value) -> String {
    let input = &call["rawInput"];
    match &input["command"] {
        Value::String(command) => command.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(" "),
        _ => input["cmd"]
            .as_str()
            .or(call["title"].as_str())
            .unwrap_or("")
            .to_owned(),
    }
}

/// The primary file a call touches: a diff's path, else its first location.
pub fn call_path(call: &Value) -> Option<&str> {
    call["content"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|content| content["type"] == "diff")
        .and_then(|diff| diff["path"].as_str())
        .or_else(|| call["locations"][0]["path"].as_str())
}

/// The Claude-style tool name and input whose approval UI fits this call.
fn permission_subject(call: &Value, desktop_names: &[String]) -> (String, Value) {
    if let Some(name) = desktop_tool(call, desktop_names)
        && call["rawInput"].is_object()
    {
        return (
            format!("mcp__{DESKTOP_SERVER}__{name}"),
            call["rawInput"].clone(),
        );
    }
    match call["kind"].as_str() {
        Some("execute") => ("Bash".into(), json!({"command":command_text(call)})),
        Some("edit" | "delete" | "move") => (
            "Edit".into(),
            json!({"file_path":call_path(call).or(call["title"].as_str()).unwrap_or("")}),
        ),
        Some("read") if call_path(call).is_some() => {
            ("Read".into(), json!({"file_path":call_path(call)}))
        }
        _ => {
            let name = call["title"]
                .as_str()
                .or(call["name"].as_str())
                .filter(|name| !name.is_empty())
                .unwrap_or("tool");
            let input = if call["rawInput"].is_object() {
                call["rawInput"].clone()
            } else {
                json!({})
            };
            (name.to_owned(), input)
        }
    }
}

/// A private Unix socket the `mcp-bridge` helper connects to. The agent
/// launches the helper as a stdio MCP server; the helper relays its stdio here.
struct Bridge {
    directory: PathBuf,
    socket: PathBuf,
    calls: Arc<OnceLock<mpsc::Sender<BridgeCall>>>,
}

impl Bridge {
    fn start(tools: Vec<Value>, cancel: CancellationToken) -> Result<Self> {
        // Socket paths are limited to about 100 bytes, so use the short
        // temporary directory rather than the facade state directory.
        let directory = std::env::temp_dir().join(format!(
            "claudex-mcp-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .context("creating MCP bridge directory")?;
        let bridge = Self {
            socket: directory.join("mcp.sock"),
            directory,
            calls: Arc::default(),
        };
        let listener =
            tokio::net::UnixListener::bind(&bridge.socket).context("binding MCP bridge")?;
        let tools = Arc::new(tools);
        let calls = bridge.calls.clone();
        tokio::spawn(async move {
            loop {
                let stream = tokio::select! {
                    _ = cancel.cancelled() => return,
                    accepted = listener.accept() => match accepted {
                        Ok((stream, _)) => stream,
                        Err(_) => return,
                    },
                };
                tokio::spawn(serve_bridge_connection(
                    stream,
                    tools.clone(),
                    calls.clone(),
                    cancel.clone(),
                ));
            }
        });
        Ok(bridge)
    }

    fn server_entry(&self) -> Result<Value> {
        let executable = std::env::current_exe().context("locating facade executable")?;
        Ok(json!({"name":DESKTOP_SERVER,"command":executable,
            "args":["mcp-bridge","--socket",self.socket],"env":[]}))
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket);
        let _ = std::fs::remove_dir(&self.directory);
    }
}

async fn serve_bridge_connection(
    stream: tokio::net::UnixStream,
    tools: Arc<Vec<Value>>,
    calls: Arc<OnceLock<mpsc::Sender<BridgeCall>>>,
    cancel: CancellationToken,
) {
    let (read, mut write) = stream.into_split();
    let (reply_tx, mut replies) = mpsc::channel::<Value>(32);
    let writer = tokio::spawn(async move {
        while let Some(message) = replies.recv().await {
            let mut bytes = message.to_string().into_bytes();
            bytes.push(b'\n');
            if write.write_all(&bytes).await.is_err() {
                return;
            }
        }
    });
    let mut lines = tokio::io::BufReader::new(read).lines();
    loop {
        let line = tokio::select! {
            _ = cancel.cancelled() => break,
            line = lines.next_line() => match line {
                Ok(Some(line)) => line,
                _ => break,
            },
        };
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            let _ = reply_tx
                .send(json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}))
                .await;
            continue;
        };
        match crate::backend::mcp_local(&message, &tools) {
            crate::backend::McpLocal::Reply(reply) => {
                let _ = reply_tx.send(reply).await;
            }
            crate::backend::McpLocal::Notification => {}
            crate::backend::McpLocal::Call => {
                let Some(calls) = calls.get() else {
                    let _ = reply_tx
                        .send(json!({"jsonrpc":"2.0","id":message["id"],"error":{
                            "code":-32000,"message":"ACP session setup is still in progress"}}))
                        .await;
                    continue;
                };
                if calls
                    .send(BridgeCall {
                        message,
                        reply: reply_tx.clone(),
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }
    drop(reply_tx);
    let _ = writer.await;
}

/// Relay an agent-launched stdio MCP server to the facade's bridge socket.
#[cfg(unix)]
pub async fn run_bridge(socket: &Path) -> Result<()> {
    let stream = tokio::net::UnixStream::connect(socket)
        .await
        .with_context(|| format!("connecting MCP bridge {}", socket.display()))?;
    let (mut read, mut write) = stream.into_split();
    let input = async {
        tokio::io::copy(&mut tokio::io::stdin(), &mut write).await?;
        write.shutdown().await
    };
    let output = async {
        let mut stdout = tokio::io::stdout();
        tokio::io::copy(&mut read, &mut stdout).await?;
        stdout.flush().await
    };
    // Either side closing ends the relay: the agent stopped the server, or
    // the turn that owned the bridge finished.
    tokio::select! {
        result = input => result?,
        result = output => result?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_flattens_groups_and_maps_thought_levels() {
        let state = SessionState::from_response(&json!({"configOptions":[
            {"id":"model","name":"Model","category":"model","type":"select","currentValue":"a",
             "options":[{"group":"g","name":"Group","options":[{"value":"a","name":"Model A"}]},
                        {"value":"b","name":"Model B","description":"Second"}]},
            {"id":"effort","name":"Effort","category":"thought_level","type":"select","currentValue":"low",
             "options":[{"value":"low","name":"Low"},{"value":"High","name":"High"},{"value":"turbo","name":"Turbo"}]}
        ]}));
        assert_eq!(
            state.catalog(),
            json!([
                {"value":"a","displayName":"Model A","description":"","supportedEffortLevels":["low","high"]},
                {"value":"b","displayName":"Model B","description":"Second","supportedEffortLevels":["low","high"]}
            ])
        );
        let legacy = SessionState::from_response(&json!({"models":{"currentModelId":"x",
            "availableModels":[{"modelId":"x","name":"X"}]}}));
        assert_eq!(
            legacy.catalog(),
            json!([{"value":"x","displayName":"X","description":"","supportedEffortLevels":[]}])
        );
    }

    #[test]
    fn permission_subjects_select_matching_approval_ui() {
        let names = vec!["lookup".to_owned()];
        let execute = json!({"kind":"execute","title":"Run","rawInput":{"command":["ls","-la"]}});
        assert_eq!(
            permission_subject(&execute, &names),
            ("Bash".into(), json!({"command":"ls -la"}))
        );
        let edit = json!({"kind":"edit","title":"Edit","content":[{"type":"diff","path":"/w/a.rs","newText":"x"}]});
        assert_eq!(
            permission_subject(&edit, &names),
            ("Edit".into(), json!({"file_path":"/w/a.rs"}))
        );
        let read = json!({"kind":"read","title":"Read","locations":[{"path":"/w/b.rs"}]});
        assert_eq!(
            permission_subject(&read, &names),
            ("Read".into(), json!({"file_path":"/w/b.rs"}))
        );
        let desktop =
            json!({"kind":"other","title":"mcp__codex_desktop__lookup","rawInput":{"id":1}});
        assert_eq!(
            permission_subject(&desktop, &names),
            ("mcp__codex_desktop__lookup".into(), json!({"id":1}))
        );
        let gemini = json!({"title":"lookup (codex_desktop MCP Server)","rawInput":{"id":2}});
        assert_eq!(desktop_tool(&gemini, &names).as_deref(), Some("lookup"));
        let fetch = json!({"kind":"fetch","title":"Fetch page","rawInput":{"url":"u"}});
        assert_eq!(
            permission_subject(&fetch, &names),
            ("Fetch page".into(), json!({"url":"u"}))
        );
    }

    #[test]
    fn prompt_blocks_respect_agent_capabilities() {
        let image = json!([{"type":"text","text":"see"},
            {"type":"image","source":{"type":"base64","media_type":"image/png","data":"AA=="}}]);
        assert!(prompt_content(&image, &json!({"image":false})).is_err());
        assert_eq!(
            prompt_content(&image, &json!({"image":true})).unwrap(),
            vec![
                json!({"type":"text","text":"see"}),
                json!({"type":"image","data":"AA==","mimeType":"image/png"})
            ]
        );
        assert_eq!(
            prompt_content(&json!("plain"), &json!({})).unwrap(),
            vec![json!({"type":"text","text":"plain"})]
        );
    }

    #[test]
    fn results_carry_usage_and_context_window() {
        let frame = result_frame(
            false,
            None,
            Some(
                &json!({"inputTokens":100,"outputTokens":7,"totalTokens":107,"cachedReadTokens":60}),
            ),
            Some(200_000),
        );
        assert_eq!(frame["subtype"], "success");
        assert_eq!(frame["usage"]["input_tokens"], 40);
        assert_eq!(frame["usage"]["cache_read_input_tokens"], 60);
        assert_eq!(frame["modelUsage"]["acp"]["contextWindow"], 200_000);
        let interrupted = result_frame(true, Some("cancelled"), None, None);
        assert_eq!(interrupted["is_error"], false);
        assert_eq!(interrupted["subtype"], "error_during_execution");
    }
}
