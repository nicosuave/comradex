//! Bounded, bidirectional Claude Code stream-json transport.
use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{ChildStdin, Command},
    sync::{Semaphore, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::{
    codec::{FramedRead, LinesCodec},
    sync::CancellationToken,
};

pub(crate) const MAX_FRAME: usize = 32 * 1024 * 1024;
pub(crate) const STDERR_TAIL: usize = 16 * 1024;
// Well below the native hook's 60-second fail-open timeout.
const FILE_CALLBACK_DEADLINE: Duration = Duration::from_secs(5);
// Workspace sessions need --managed-settings and list_permission_rules.
const MIN_WORKSPACE_CLAUDE: &str = "2.1.294";
// Guarded sessions keep reading stdout while the event consumer catches up,
// so a hook callback is never stuck behind queued events.
const EVENT_BACKLOG: usize = if cfg!(test) { 64 * 1024 } else { 4 * MAX_FRAME };
// Deployment settings for the facade process itself. Claude sessions must not
// inherit them: a codex or facade run inside a session would otherwise use the
// facade's private homes and sockets instead of the user's.
pub(crate) const FACADE_ENVIRONMENT: &[&str] = &[
    "CODEX_HOME",
    "CODEX_INSTALL_DIR",
    "CODEX_SSH_SKIP_APP_SERVER_BOOT",
    "CLAUDE_CODEX_HOME",
    "CLAUDE_CODEX_SOCKET_DIR",
    "CLAUDE_CODEX_PLUGIN_HOME",
    "CLAUDE_CODEX_PLUGIN_EXECUTABLE",
];
type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, String>>>>>;
type SharedStdin = Arc<tokio::sync::Mutex<Option<ChildStdin>>>;

/// Which agent protocol the configured executable speaks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BackendProtocol {
    /// Claude Code's bidirectional stream-json transport.
    #[default]
    Claude,
    /// An Agent Client Protocol (v1) agent over stdio.
    Acp,
}

#[derive(Clone, Debug)]
pub struct BackendConfig {
    pub executable: PathBuf,
    pub extra_args: Vec<String>,
    pub initialize_timeout: Duration,
    pub protocol: BackendProtocol,
}

#[derive(Clone, Debug)]
pub struct SessionOptions {
    pub cwd: PathBuf,
    pub state_dir: PathBuf,
    pub session_id: String,
    pub resume: bool,
    pub fork_from: Option<String>,
    pub resume_at: Option<String>,
    pub model: String,
    pub permission_mode: String,
    pub system_prompt: Option<String>,
    pub append_system_prompt: Option<String>,
    pub effort: Option<String>,
    pub output_schema: Option<Value>,
    pub ephemeral: bool,
    pub dynamic_tools: Vec<Value>,
    pub native_settings: Value,
    pub sandbox: crate::sandbox::Policy,
}

#[derive(Debug)]
pub enum BackendEvent {
    Message(Value),
    Exited { success: bool, message: String },
}

pub struct ClaudeBackend {
    stdin: SharedStdin,
    pending: Pending,
    controls: Arc<Semaphore>,
    cancel: CancellationToken,
    task: Option<JoinHandle<()>>,
    process_id: u32,
    alive: Arc<AtomicBool>,
    initialization: Value,
}

struct PreparedLaunch {
    command: Command,
    file_guard: Option<crate::file_guard::FileGuard>,
    managed_settings: Option<Value>,
}

#[cfg(test)]
fn command(config: &BackendConfig, options: &SessionOptions) -> Result<Command> {
    Ok(prepare_launch(config, options)?.command)
}

fn prepare_launch(config: &BackendConfig, options: &SessionOptions) -> Result<PreparedLaunch> {
    if options.sandbox.is_read_only()
        && (!options.ephemeral
            || options.resume
            || options.fork_from.is_some()
            || !options.dynamic_tools.is_empty()
            || options.permission_mode != "dontAsk"
            || !config.extra_args.is_empty())
    {
        bail!(
            "Read-only helpers require a fresh ephemeral Claude session without tools or launch overrides"
        );
    }
    let profile = if options.sandbox.is_workspace() {
        Some(crate::native_profile::NativeProfile::prepare(
            &options.state_dir,
            &options.cwd,
            &options.session_id,
        )?)
    } else {
        None
    };
    let mut cmd = Command::new(&config.executable);
    for key in FACADE_ENVIRONMENT {
        cmd.env_remove(key);
    }
    let (extra_args, mut settings) = launch_settings(config, options, profile.as_ref())?;
    if cfg!(target_os = "macos") && options.sandbox.is_workspace() {
        crate::ssh_proxy::add_hook(
            settings.as_mut().context("workspace settings missing")?,
            &std::env::current_exe()?,
        )?;
    }
    let managed_settings = profile
        .as_ref()
        .map(|profile| {
            options.sandbox.managed_settings(
                settings.as_ref().context("workspace settings missing")?,
                &options.cwd,
                &options.permission_mode,
                profile,
            )
        })
        .transpose()?;
    let file_guard = crate::file_guard::FileGuard::new(
        &options.cwd,
        &options.sandbox,
        profile
            .as_ref()
            .map(|p| p.protected_paths.as_slice())
            .unwrap_or(&[]),
        profile.as_ref().map(|p| p.transcripts.as_path()),
    )?;
    cmd.args(extra_args).args([
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
        "--include-partial-messages",
        "--replay-user-messages",
        "--permission-prompt-tool",
        "stdio",
        // Desktop instructions and plugin catalogs can change between turns.
        // Claude's default snapshot otherwise ignores new launch instructions
        // on resume, even though the transcript is resumed correctly.
        "--system-prompt-snapshot",
        "off",
    ]);
    if let Some(settings) = settings {
        cmd.arg(format!("--settings={settings}"));
    }
    if let Some(managed) = &managed_settings {
        cmd.arg(format!("--managed-settings={managed}"));
    }
    if let Some(profile) = &profile {
        cmd.envs(&profile.environment);
    }
    if let Some(source) = &options.fork_from {
        cmd.arg(format!("--resume={source}"))
            .arg("--fork-session")
            .arg(format!("--session-id={}", options.session_id));
    } else {
        cmd.arg(format!(
            "--{}={}",
            if options.resume {
                "resume"
            } else {
                "session-id"
            },
            options.session_id
        ));
    }
    cmd.arg(format!("--model={}", options.model));
    if let Some(message_id) = &options.resume_at {
        cmd.arg(format!("--resume-session-at={message_id}"));
    }
    cmd.arg(format!("--permission-mode={}", options.permission_mode));
    if options.sandbox.is_read_only() {
        // These helpers receive their entire context in the prompt. Disable
        // execution surfaces rather than pretending dontAsk is a read-only OS
        // sandbox. Safe mode retains normal login and managed policy.
        cmd.args([
            "--safe-mode",
            "--tools=",
            "--strict-mcp-config",
            "--disable-slash-commands",
            "--no-chrome",
        ]);
    }
    for (flag, value) in [
        ("system-prompt", &options.system_prompt),
        ("append-system-prompt", &options.append_system_prompt),
        ("effort", &options.effort),
    ] {
        if let Some(value) = value {
            cmd.arg(format!("--{flag}={value}"));
        }
    }
    if let Some(schema) = &options.output_schema {
        cmd.arg(format!("--json-schema={schema}"));
    }
    if options.ephemeral {
        cmd.arg("--no-session-persistence");
    }
    if !options.dynamic_tools.is_empty() {
        cmd.arg("--mcp-config").arg(
            json!({"mcpServers":{"codex_desktop":{"type":"sdk","name":"codex_desktop"}}})
                .to_string(),
        );
    }
    cmd.current_dir(&options.cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // The official SDK waits for idle before closing a bidirectional session.
    cmd.env("CLAUDE_CODE_SDK_READS_SESSION_STATE", "1");
    #[cfg(unix)]
    cmd.process_group(0);
    Ok(PreparedLaunch {
        command: cmd,
        file_guard,
        managed_settings,
    })
}

/// Preserve caller-provided settings (including permission rules) while adding
/// session-local environment/plugin overrides through one native settings flag.
fn launch_settings(
    config: &BackendConfig,
    options: &SessionOptions,
    profile: Option<&crate::native_profile::NativeProfile>,
) -> Result<(Vec<String>, Option<Value>)> {
    let mut args = Vec::new();
    let mut settings = json!({});
    let mut supplied = false;
    let mut sources = "user,project,local".to_owned();
    let mut overlays = Vec::new();
    let mut input = config.extra_args.iter();
    while let Some(arg) = input.next() {
        if options.sandbox.is_workspace() {
            if arg == "--setting-sources" {
                sources = input
                    .next()
                    .context("--setting-sources requires a value")?
                    .clone();
                continue;
            }
            if let Some(value) = arg.strip_prefix("--setting-sources=") {
                sources = value.to_owned();
                continue;
            }
            if [
                "--add-dir",
                "--allowedTools",
                "--allowed-tools",
                "--dangerously-skip-permissions",
                "--allow-dangerously-skip-permissions",
                "--permission-mode",
                "--managed-settings",
                "--project-config-root",
                "--bare",
                "--restricted",
            ]
            .iter()
            .any(|flag| arg == flag || arg.starts_with(&format!("{flag}=")))
            {
                bail!(
                    "{arg} conflicts with the selected workspace sandbox; configure permissions through the facade"
                );
            }
        }
        let value = if arg == "--settings" {
            Some(
                input
                    .next()
                    .context("--settings requires JSON or a file path")?
                    .as_str(),
            )
        } else {
            arg.strip_prefix("--settings=")
        };
        if let Some(value) = value {
            let mut parsed: Value = if value.trim_start().starts_with('{') {
                serde_json::from_str(value).context("invalid Claude --settings JSON")?
            } else {
                let path = options.cwd.join(value);
                serde_json::from_slice(
                    &std::fs::read(path).context("reading Claude --settings file")?,
                )
                .context("invalid Claude --settings file JSON")?
            };
            if !parsed.is_object() {
                bail!("Claude --settings must contain an object");
            }
            if options.sandbox.is_workspace() {
                let anchor = if value.trim_start().starts_with('{') {
                    options.cwd.clone()
                } else {
                    options
                        .cwd
                        .join(value)
                        .parent()
                        .context("settings path has no parent")?
                        .to_path_buf()
                };
                crate::sandbox::rebase_rules(&mut parsed, &anchor)?;
                crate::sandbox::rebase_sandbox_paths(&mut parsed, &anchor)?;
                overlays.push(parsed);
            } else {
                merge_settings(&mut settings, parsed);
            }
            supplied = true;
        } else {
            args.push(arg.clone());
        }
    }
    if options.sandbox.is_workspace() {
        let sources: Vec<&str> = sources.split(',').filter(|s| !s.is_empty()).collect();
        if sources
            .iter()
            .any(|source| !["user", "project", "local"].contains(source))
        {
            bail!("unsupported Claude setting source");
        }
        // Only permissions/sandbox restrictions need lifting into host policy.
        // Native sources retain ownership of skills, hooks and other features;
        // snapshotting project hooks here would execute them a second time.
        let mut restrictions = crate::sandbox::load_settings(
            &options.cwd,
            &sources
                .iter()
                .copied()
                .filter(|s| *s != "user")
                .collect::<Vec<_>>(),
        )?;
        if sources.contains(&"user") {
            let mut user = profile
                .context("workspace native profile missing")?
                .user_settings
                .clone();
            crate::sandbox::merge(&mut user, restrictions);
            restrictions = user;
        }
        for key in ["permissions", "sandbox"] {
            if let Some(value) = restrictions.get(key) {
                settings[key] = value.clone();
            }
        }
        for overlay in overlays {
            crate::sandbox::merge(&mut settings, overlay);
        }
        args.extend(["--setting-sources".into(), sources.join(",")]);
    }
    let native = options
        .native_settings
        .as_object()
        .context("native Claude settings must be an object")?;
    supplied |= !native.is_empty();
    merge_settings(&mut settings, options.native_settings.clone());
    if options.sandbox.is_workspace() {
        options
            .sandbox
            .apply_native(&mut settings, &options.cwd, &options.permission_mode)?;
        supplied = true;
    }
    Ok((args, supplied.then_some(settings)))
}

/// Stop a helper if the native runtime exposes any execution surface. Its
/// synthetic StructuredOutput tool only returns the requested JSON value.
fn verify_helper_frame(frame: &Value, structured_output: bool) -> Result<()> {
    let allowed = |name: &str| structured_output && name == "StructuredOutput";
    if frame["type"] == "system" && frame["subtype"] == "init" {
        let tools = frame["tools"]
            .as_array()
            .context("read-only helper omitted its tool inventory")?;
        if tools.iter().any(|tool| !tool.as_str().is_some_and(allowed))
            || frame["mcp_servers"]
                .as_array()
                .is_some_and(|servers| !servers.is_empty())
        {
            bail!("Read-only helper exposed tools or MCP servers");
        }
    }
    if frame["type"] == "control_request" {
        let request = &frame["request"];
        if request["subtype"] == "mcp_message"
            || (request["subtype"] == "can_use_tool"
                && !request["tool_name"].as_str().is_some_and(allowed))
        {
            bail!("Read-only helper attempted tool execution");
        }
    }
    if frame["message"]["content"]
        .as_array()
        .is_some_and(|blocks| {
            blocks.iter().any(|block| {
                block["type"] == "tool_use" && !block["name"].as_str().is_some_and(allowed)
            })
        })
    {
        bail!("Read-only helper attempted tool execution");
    }
    Ok(())
}

fn merge_settings(target: &mut Value, overlay: Value) {
    match (target, overlay) {
        (Value::Object(target), Value::Object(overlay)) => {
            for (key, value) in overlay {
                merge_settings(target.entry(key).or_insert(Value::Null), value);
            }
        }
        (target, overlay) => *target = overlay,
    }
}

// The process group belongs to this backend, including tool subprocesses.
pub(crate) fn kill_group(id: u32) {
    #[cfg(unix)]
    unsafe {
        libc::kill(-(id as i32), libc::SIGKILL);
    }
    #[cfg(not(unix))]
    let _ = id;
}

// A supervisor panic must also kill descendants, not just Child's main process.
pub(crate) struct ProcessGroupGuard {
    pub(crate) id: u32,
    pub(crate) alive: Arc<AtomicBool>,
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        if self.alive.load(Ordering::Acquire) {
            kill_group(self.id);
        }
    }
}

fn parse_frame(line: &str) -> Result<Option<Value>> {
    let line = line.trim();
    if line.is_empty() || !(line.starts_with('{') || line.starts_with('[')) {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(line).context("malformed Claude JSON frame")?;
    if !value.is_object() {
        bail!("Claude frame must be a JSON object");
    }
    Ok(Some(value))
}

// Discovery must run in the reader itself: Claude can await it before answering
// initialize, while the caller cannot consume BackendEvents until spawn returns.
fn mcp_control_response(value: &Value, tools: &[Value]) -> Option<Value> {
    if value["type"] != "control_request" || value["request"]["subtype"] != "mcp_message" {
        return None;
    }
    let request = &value["request"];
    let message = &request["message"];
    let response = if request["server_name"] != "codex_desktop" || tools.is_empty() {
        mcp_error(message, -32601, "SDK MCP server not registered")
    } else {
        match mcp_local(message, tools) {
            McpLocal::Call => return None,
            McpLocal::Reply(response) => response,
            // Notifications have no JSON-RPC response, but their outer control
            // request still needs the SDK's empty acknowledgement.
            McpLocal::Notification => json!({"jsonrpc":"2.0","result":{}}),
        }
    };
    Some(mcp_response_envelope(value, response))
}

/// How the desktop tool MCP server handles one inbound JSON-RPC message.
pub(crate) enum McpLocal {
    /// tools/call: the server executes it and supplies the result.
    Call,
    /// Answered locally from the registered tool catalog.
    Reply(Value),
    /// A notification, which has no JSON-RPC response.
    Notification,
}

pub(crate) fn mcp_local(message: &Value, tools: &[Value]) -> McpLocal {
    if !message.is_object() || message["jsonrpc"] != "2.0" {
        return McpLocal::Reply(mcp_error(message, -32600, "Invalid JSON-RPC message"));
    }
    if message.get("id").is_none() {
        return McpLocal::Notification;
    }
    let result = match message["method"].as_str() {
        Some("tools/call") => return McpLocal::Call,
        Some("initialize") => {
            let requested = message["params"]["protocolVersion"].as_str().unwrap_or("");
            let version = match requested {
                "2024-11-05" | "2025-03-26" | "2025-06-18" | "2025-11-25" => requested,
                _ => "2025-11-25",
            };
            json!({"protocolVersion":version,"capabilities":{"tools":{}},
                "serverInfo":{"name":"codex_desktop","version":env!("CARGO_PKG_VERSION")}})
        }
        Some("tools/list") => json!({"tools":tools}),
        Some("ping") => json!({}),
        Some(_) => return McpLocal::Reply(mcp_error(message, -32601, "Method not found")),
        None => return McpLocal::Reply(mcp_error(message, -32600, "Invalid JSON-RPC request")),
    };
    McpLocal::Reply(json!({"jsonrpc":"2.0","id":message["id"],"result":result}))
}

fn mcp_error(message: &Value, code: i32, text: &str) -> Value {
    json!({"jsonrpc":"2.0","id":message["id"],"error":{"code":code,"message":text}})
}

fn mcp_response_envelope(request: &Value, response: Value) -> Value {
    json!({"type":"control_response","response":{"subtype":"success",
        "request_id":request["request_id"],"response":{"mcp_response":response}}})
}

async fn write_frame(stdin: &SharedStdin, message: &Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(message)?;
    if bytes.len() > MAX_FRAME {
        bail!("Claude input exceeds {MAX_FRAME} bytes");
    }
    bytes.push(b'\n');
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut stdin = stdin.lock().await;
        let stdin = stdin.as_mut().context("Claude stdin is closed")?;
        stdin
            .write_all(&bytes)
            .await
            .context("writing Claude stdin")?;
        stdin.flush().await.context("flushing Claude stdin")
    })
    .await
    .context("Claude stdin write timed out")?
}

impl ClaudeBackend {
    pub async fn spawn(
        config: &BackendConfig,
        options: &SessionOptions,
    ) -> Result<(Self, mpsc::Receiver<BackendEvent>)> {
        Self::spawn_cancellable(config, options, CancellationToken::new()).await
    }

    pub async fn spawn_cancellable(
        config: &BackendConfig,
        options: &SessionOptions,
        startup_cancel: CancellationToken,
    ) -> Result<(Self, mpsc::Receiver<BackendEvent>)> {
        if options.resume && options.fork_from.is_some() {
            bail!("Cannot resume and fork a Claude session simultaneously");
        }
        let PreparedLaunch {
            mut command,
            file_guard,
            managed_settings,
        } = prepare_launch(config, options)?;
        let hooks = file_guard
            .as_ref()
            .map(|guard| guard.hooks())
            .unwrap_or(Value::Null);
        let mut child = command.spawn().context("starting Claude subprocess")?;
        let process_id = child.id().context("Claude subprocess has no process ID")?;
        let stdin = Arc::new(tokio::sync::Mutex::new(Some(
            child.stdin.take().context("missing Claude stdin")?,
        )));
        let stdout = child.stdout.take().context("missing Claude stdout")?;
        let mut stderr = child.stderr.take().context("missing Claude stderr")?;
        let cancel = CancellationToken::new();
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let (tx, rx) = mpsc::channel(128);
        let task_cancel = cancel.clone();
        let task_pending = pending.clone();
        let alive = Arc::new(AtomicBool::new(true));
        let task_alive = alive.clone();
        let task_stdin = stdin.clone();
        let dynamic_tools = options.dynamic_tools.clone();
        let read_only_helper = options.sandbox.is_read_only();
        let structured_output = options.output_schema.is_some();
        let task = tokio::spawn(async move {
            let _process_guard = ProcessGroupGuard {
                id: process_id,
                alive: task_alive.clone(),
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
            let mut status = None;
            let mut backlog = VecDeque::<(BackendEvent, usize)>::new();
            let mut backlog_bytes = 0;
            let outcome: Result<()> = async {
                loop {
                    tokio::select! {
                        _ = task_cancel.cancelled() => break,
                        permit = tx.reserve(), if !backlog.is_empty() => {
                            let Ok(permit) = permit else { break; };
                            let (event, size) = backlog.pop_front().unwrap();
                            backlog_bytes -= size;
                            permit.send(event);
                        }
                        result = child.wait(), if status.is_none() => {
                            status = Some(result.context("waiting for Claude")?);
                            // Reap descendants holding inherited pipes open as well.
                            kill_group(process_id);
                        }
                        _ = tokio::time::sleep(Duration::from_secs(1)), if status.is_some() => {
                            bail!("Claude stdout did not close after process exit");
                        }
                        line = lines.next() => {
                            let Some(line) = line else { break; };
                            let line = line.context("reading Claude stdout")?;
                            let size = line.len();
                            let Some(value) = parse_frame(&line)? else { continue; };
                            if read_only_helper {
                                verify_helper_frame(&value, structured_output)?;
                            }
                            // Recognize even malformed hook envelopes before generic
                            // control routing can return a permissive protocol error.
                            if crate::file_guard::is_callback(&value) {
                                let guard = file_guard.clone().context("unregistered native hook callback")?;
                                tokio::select! {
                                    _ = task_cancel.cancelled() => break,
                                    result = tokio::time::timeout(FILE_CALLBACK_DEADLINE, async {
                                        let response = tokio::task::spawn_blocking(move || guard.response(&value))
                                            .await.context("file callback task failed")??;
                                        write_frame(&task_stdin, &response).await
                                    }) => result.context("native file callback deadline exceeded")??,
                                }
                                continue;
                            }
                            if value["type"] == "control_response" {
                                let response = &value["response"];
                                if let Some(id) = response["request_id"].as_str()
                                    && let Some(sender) = task_pending.lock().unwrap().remove(id) {
                                        let result = if response["subtype"] == "error" {
                                            Err(response["error"].as_str().unwrap_or("Claude control request failed").to_owned())
                                        } else { Ok(response["response"].clone()) };
                                        let _ = sender.send(result);
                                    }
                                continue;
                            }
                            if let Some(response) = mcp_control_response(&value, &dynamic_tools) {
                                tokio::select! {
                                    _ = task_cancel.cancelled() => break,
                                    result = tokio::time::timeout(FILE_CALLBACK_DEADLINE, write_frame(&task_stdin, &response)) => result.context("native control reply deadline exceeded")??,
                                }
                                continue;
                            }
                            if let Some(guard) = &file_guard {
                                guard.observe(&value);
                                // A full event queue must not stall stdout and hide a pending
                                // hook until Claude's fail-open timeout. Buffer behind a slow
                                // consumer; stop the process only if the backlog keeps growing.
                                let event = BackendEvent::Message(value);
                                if backlog.is_empty() {
                                    match tx.try_send(event) {
                                        Ok(()) => continue,
                                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                                        Err(mpsc::error::TrySendError::Full(event)) => backlog.push_back((event, size)),
                                    }
                                } else {
                                    backlog.push_back((event, size));
                                }
                                backlog_bytes += size;
                                if backlog_bytes > EVENT_BACKLOG {
                                    bail!("native event consumer stalled during guarded session");
                                }
                            } else {
                                tokio::select! {
                                    _ = task_cancel.cancelled() => break,
                                    result = tx.send(BackendEvent::Message(value)) => if result.is_err() { break; },
                                }
                            }
                        }
                    }
                }
                Ok(())
            }.await;
            if status.is_none() {
                // EOF normally accompanies exit. Allow it briefly before killing a stalled child.
                if outcome.is_ok()
                    && !task_cancel.is_cancelled()
                    && let Ok(result) =
                        tokio::time::timeout(Duration::from_millis(250), child.wait()).await
                {
                    status = result.ok();
                }
                kill_group(process_id);
                let _ = child.start_kill();
                if status.is_none() {
                    status = child.wait().await.ok();
                }
            }
            task_alive.store(false, Ordering::Release);
            if tokio::time::timeout(Duration::from_millis(250), &mut stderr_task)
                .await
                .is_err()
            {
                stderr_task.abort();
                let _ = stderr_task.await;
            }
            let success = outcome.is_ok() && status.as_ref().is_some_and(|s| s.success());
            let mut message = match outcome {
                Ok(()) => format!(
                    "Claude exited: {}",
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
            for (_, sender) in task_pending.lock().unwrap().drain() {
                let _ = sender.send(Err(message.clone()));
            }
            // Deliver buffered events ahead of the exit, as an unguarded reader would.
            while let Some((event, _)) = backlog.pop_front() {
                tokio::select! {
                    _ = task_cancel.cancelled() => break,
                    result = tx.send(event) => if result.is_err() { break; },
                }
            }
            tokio::select! { _ = task_cancel.cancelled() => {}, _ = tx.send(BackendEvent::Exited { success, message }) => {} }
        });
        let mut backend = Self {
            stdin,
            pending,
            controls: Arc::new(Semaphore::new(8)),
            cancel,
            task: Some(task),
            process_id,
            alive,
            initialization: Value::Null,
        };
        let initialized = tokio::select! {
            result = backend.control(json!({"subtype":"initialize", "hooks":hooks, "forwardSubagentText":true}), config.initialize_timeout) => result,
            _ = startup_cancel.cancelled() => Err(anyhow!("Claude startup cancelled")),
        };
        match initialized {
            Ok(value) => backend.initialization = value,
            Err(error) => {
                backend.terminate().await?;
                if options.sandbox.is_workspace() && format!("{error:#}").contains("unknown option")
                {
                    return Err(error.context(format!(
                        "workspace sessions require Claude Code {MIN_WORKSPACE_CLAUDE} or newer"
                    )));
                }
                return Err(error);
            }
        }
        if options.sandbox.is_workspace() {
            let verified = tokio::select! {
                result = async {
                    let settings = backend.control(json!({"subtype":"get_settings"}), config.initialize_timeout).await?;
                    let status = backend.control(json!({"subtype":"get_sandbox_dialog"}), config.initialize_timeout).await?;
                    let rules = backend.control(json!({"subtype":"list_permission_rules"}), config.initialize_timeout).await?;
                    options.sandbox.verify_native_with_rules(&settings, &status, &rules,
                        managed_settings.as_ref().context("workspace managed policy missing")?, &options.cwd)
                } => result,
                _ = startup_cancel.cancelled() => Err(anyhow!("Claude startup cancelled")),
            };
            if let Err(error) = verified {
                backend.terminate().await?;
                return Err(error.context(format!(
                    "verifying Claude workspace sandbox (requires Claude Code {MIN_WORKSPACE_CLAUDE} or newer)"
                )));
            }
        }
        Ok((backend, rx))
    }

    pub fn initialization(&self) -> &Value {
        &self.initialization
    }

    fn prediction_client(&self) -> PredictionClient {
        PredictionClient {
            stdin: self.stdin.clone(),
            pending: self.pending.clone(),
            controls: self.controls.clone(),
            closed: self.cancel.clone(),
            cached: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Wait, at most `deadline`, until no MCP server is still connecting. Claude
    /// sends requests without servers that are still connecting; their tools
    /// join a later request and invalidate the prompt cache for the whole
    /// conversation. Claude without `mcp_status` proceeds immediately.
    pub async fn wait_for_mcp(&self, deadline: Duration) {
        let until = tokio::time::Instant::now() + deadline;
        while tokio::time::Instant::now() < until {
            let Ok(status) = self
                .control(json!({"subtype":"mcp_status"}), Duration::from_secs(5))
                .await
            else {
                return;
            };
            let connecting = status["mcpServers"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|server| server["status"] == "pending");
            if !connecting {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    pub fn is_running(&self) -> bool {
        !self.cancel.is_cancelled() && self.task.as_ref().is_some_and(|task| !task.is_finished())
    }

    pub async fn send(&self, message: Value) -> Result<()> {
        if self.cancel.is_cancelled() || self.task.as_ref().is_none_or(|task| task.is_finished()) {
            bail!("Claude backend is closed");
        }
        let result = write_frame(&self.stdin, &message).await;
        if result.is_err() {
            self.cancel.cancel();
        }
        result
    }

    async fn control(&self, request: Value, timeout: Duration) -> Result<Value> {
        let _permit = self.controls.acquire().await?;
        let id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id.clone(), tx);
        let _cleanup = PendingGuard {
            pending: self.pending.clone(),
            id: id.clone(),
        };
        tokio::time::timeout(timeout, async {
            if let Err(error) = self
                .send(json!({"type":"control_request", "request_id":id, "request":request}))
                .await
            {
                // A child can exit before the write completes. send cancels the
                // supervisor, which drains stderr before failing pending requests.
                // Keep that diagnostic instead of reporting only a broken pipe.
                // Drop any sender still in the map: a finished supervisor may
                // have drained the map before this request was registered.
                if self.task.as_ref().is_none_or(|task| task.is_finished()) {
                    self.pending.lock().unwrap().remove(&id);
                }
                return match rx.await {
                    Ok(Err(message)) => Err(error.context(message)),
                    _ => Err(error),
                };
            }
            rx.await
                .context("Claude control response channel closed")?
                .map_err(|error| anyhow!(error))
        })
        .await
        .context("Claude control request timed out")?
    }

    pub async fn interrupt(&self) -> Result<()> {
        self.control(json!({"subtype":"interrupt"}), Duration::from_secs(10))
            .await
            .map(|_| ())
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        // EOF gives Claude a chance to persist its transcript before termination.
        if let Ok(mut stdin) =
            tokio::time::timeout(Duration::from_millis(250), self.stdin.lock()).await
        {
            stdin.take();
        }
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
                Ok(result) => {
                    result.context("joining Claude backend")?;
                }
                Err(_) => {
                    self.cancel.cancel();
                    if self.alive.load(Ordering::Acquire) {
                        kill_group(self.process_id);
                    }
                    task.await.context("joining Claude backend")?;
                }
            }
        }
        self.cancel.cancel();
        Ok(())
    }

    /// Cancel I/O, kill the entire process group, and await the supervisor's reap.
    /// Callers must await this before reusing a session after forced cancellation.
    pub async fn terminate(&mut self) -> Result<()> {
        self.cancel.cancel();
        if self.alive.load(Ordering::Acquire) {
            kill_group(self.process_id);
        }
        if let Some(task) = self.task.take() {
            task.await.context("joining terminated Claude backend")?;
        }
        if let Ok(mut stdin) = self.stdin.try_lock() {
            stdin.take();
        }
        Ok(())
    }
}

impl Drop for ClaudeBackend {
    fn drop(&mut self) {
        self.cancel.cancel();
        if self.alive.load(Ordering::Acquire) {
            kill_group(self.process_id);
        }
    }
}

struct PendingGuard {
    pending: Pending,
    id: String,
}
impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.id);
    }
}

/// Read-only questions share the live parent's native request prefix. Dropping
/// this handle never shuts down or interrupts the parent process.
#[derive(Clone)]
pub(crate) struct PredictionClient {
    stdin: SharedStdin,
    pending: Pending,
    controls: Arc<Semaphore>,
    closed: CancellationToken,
    cached: Arc<tokio::sync::Mutex<Option<(String, Value)>>>,
}

impl PredictionClient {
    pub async fn ask(&self, question: &str, cancel: &CancellationToken) -> Result<Value> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let request = async {
            // One entry per parked parent also coalesces identical requests.
            let mut cached = self.cached.lock().await;
            if let Some((previous, response)) = cached.as_ref()
                && previous == question
            {
                return Ok(response.clone());
            }
            let _permit = self.controls.acquire().await?;
            let (sender, receiver) = oneshot::channel();
            self.pending
                .lock()
                .unwrap()
                .insert(request_id.clone(), sender);
            let _cleanup = PendingGuard {
                pending: self.pending.clone(),
                id: request_id.clone(),
            };
            write_frame(
                &self.stdin,
                &json!({
                    "type":"control_request", "request_id":request_id,
                    "request":{"subtype":"side_question","question":question,"history":[]}
                }),
            )
            .await?;
            let response = receiver
                .await
                .context("Claude prediction channel closed")?
                .map_err(|error| anyhow!(error))?;
            *cached = Some((question.to_owned(), response.clone()));
            Ok(response)
        };
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(anyhow!("Prediction cancelled")),
            _ = self.closed.cancelled() => Err(anyhow!("Claude parent closed")),
            result = tokio::time::timeout(Duration::from_secs(10), request) => {
                match result {
                    Ok(result) => result,
                    Err(_) => Err(anyhow!("Claude prediction timed out")),
                }
            }
        };
        if result.is_err() {
            // Cancel only this read-only question, never the parent's turn.
            let _ = tokio::time::timeout(
                Duration::from_millis(250),
                write_frame(
                    &self.stdin,
                    &json!({"type":"control_cancel_request","request_id":request_id}),
                ),
            )
            .await;
        }
        result
    }
}

/// One turn's agent process. Both variants accept and emit Claude stream-json
/// control frames, so the server drives either through the same contract.
pub enum Backend {
    Claude(ClaudeBackend),
    Acp(Box<crate::acp::AcpBackend>),
}

impl Backend {
    pub(crate) fn prediction_client(&self) -> Option<PredictionClient> {
        match self {
            Self::Claude(backend) => Some(backend.prediction_client()),
            Self::Acp(_) => None,
        }
    }

    pub async fn spawn(
        config: &BackendConfig,
        options: &SessionOptions,
    ) -> Result<(Self, mpsc::Receiver<BackendEvent>)> {
        Self::spawn_cancellable(config, options, CancellationToken::new()).await
    }

    pub async fn spawn_cancellable(
        config: &BackendConfig,
        options: &SessionOptions,
        startup_cancel: CancellationToken,
    ) -> Result<(Self, mpsc::Receiver<BackendEvent>)> {
        Ok(match config.protocol {
            BackendProtocol::Claude => {
                let (backend, events) =
                    ClaudeBackend::spawn_cancellable(config, options, startup_cancel).await?;
                (Self::Claude(backend), events)
            }
            BackendProtocol::Acp => {
                let (backend, events) =
                    crate::acp::AcpBackend::spawn_cancellable(config, options, startup_cancel)
                        .await?;
                (Self::Acp(Box::new(backend)), events)
            }
        })
    }

    pub fn initialization(&self) -> &Value {
        match self {
            Self::Claude(backend) => backend.initialization(),
            Self::Acp(backend) => backend.initialization(),
        }
    }

    /// Claude only: an ACP agent connects its MCP servers before answering
    /// session setup, so there is nothing left to wait for.
    pub async fn wait_for_mcp(&self, deadline: Duration) {
        if let Self::Claude(backend) = self {
            backend.wait_for_mcp(deadline).await;
        }
    }

    pub fn is_running(&self) -> bool {
        match self {
            Self::Claude(backend) => backend.is_running(),
            Self::Acp(backend) => backend.is_running(),
        }
    }

    pub async fn send(&self, message: Value) -> Result<()> {
        match self {
            Self::Claude(backend) => backend.send(message).await,
            Self::Acp(backend) => backend.send(message).await,
        }
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        match self {
            Self::Claude(backend) => backend.shutdown().await,
            Self::Acp(backend) => backend.shutdown().await,
        }
    }

    pub async fn terminate(&mut self) -> Result<()> {
        match self {
            Self::Claude(backend) => backend.terminate().await,
            Self::Acp(backend) => backend.terminate().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sdk_mcp_discovery_and_routing() {
        let tools = vec![json!({"name":"lookup","description":"Find a record",
            "inputSchema":{"type":"object"}})];
        let request = |method: &str| {
            json!({"type":"control_request","request_id":"outer",
            "request":{"subtype":"mcp_message","server_name":"codex_desktop",
                "message":{"jsonrpc":"2.0","id":7,"method":method,
                    "params":{"protocolVersion":"2025-06-18"}}}})
        };
        let response = mcp_control_response(&request("initialize"), &tools).unwrap();
        assert_eq!(response["response"]["request_id"], "outer");
        assert_eq!(response["response"]["response"]["mcp_response"]["id"], 7);
        assert_eq!(
            response["response"]["response"]["mcp_response"]["result"]["protocolVersion"],
            "2025-06-18"
        );
        let response = mcp_control_response(&request("tools/list"), &tools).unwrap();
        assert_eq!(
            response["response"]["response"]["mcp_response"]["result"]["tools"],
            json!(tools)
        );
        assert!(mcp_control_response(&request("tools/call"), &tools).is_none());
        for method in ["unsupported", "tools/call"] {
            let response = mcp_control_response(&request(method), &[]).unwrap();
            assert_eq!(
                response["response"]["response"]["mcp_response"]["error"]["code"],
                -32601
            );
        }
        let response = mcp_control_response(&request("unsupported"), &tools).unwrap();
        assert_eq!(
            response["response"]["response"]["mcp_response"]["error"]["code"],
            -32601
        );
        let mut notification = request("notifications/initialized");
        notification["request"]["message"]
            .as_object_mut()
            .unwrap()
            .remove("id");
        let response = mcp_control_response(&notification, &tools).unwrap();
        assert_eq!(
            response["response"]["response"]["mcp_response"],
            json!({"jsonrpc":"2.0","result":{}})
        );
        let mut wrong_server = request("tools/call");
        wrong_server["request"]["server_name"] = json!("unknown");
        assert!(mcp_control_response(&wrong_server, &tools).is_some());
    }

    #[test]
    fn diagnostics_and_bad_frames() {
        assert!(parse_frame("warning: hello").unwrap().is_none());
        assert!(parse_frame("{broken}").is_err());
        assert!(parse_frame("[]").is_err());
        assert_eq!(
            parse_frame("{\"type\":\"result\"}").unwrap().unwrap()["type"],
            "result"
        );
    }

    #[cfg(unix)]
    fn test_options() -> SessionOptions {
        SessionOptions {
            cwd: std::env::temp_dir(),
            state_dir: std::env::temp_dir().join("claudex-backend-tests"),
            session_id: "test-session".into(),
            resume: false,
            fork_from: None,
            resume_at: None,
            model: "fake".into(),
            permission_mode: "default".into(),
            system_prompt: None,
            append_system_prompt: None,
            effort: None,
            output_schema: None,
            ephemeral: true,
            dynamic_tools: Vec::new(),
            native_settings: json!({}),
            sandbox: Default::default(),
        }
    }

    #[cfg(unix)]
    fn shell_config(script: &str) -> BackendConfig {
        BackendConfig {
            executable: "/bin/sh".into(),
            extra_args: vec!["-c".into(), script.into()],
            initialize_timeout: Duration::from_secs(2),
            protocol: BackendProtocol::Claude,
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn launch_merges_settings_into_one_argument_without_losing_permissions() {
        let fixture = tempfile::tempdir().unwrap();
        std::fs::write(
            fixture.path().join("settings.json"),
            json!({
                "permissions":{"ask":["Bash"]},
                "env":{"KEEP":"existing","OVERLAY":"old"},
                "enabledPlugins":{"keep@market":true,"disabled@market":true}
            })
            .to_string(),
        )
        .unwrap();
        let mut options = test_options();
        options.cwd = fixture.path().into();
        options.native_settings = json!({
            "env":{"OVERLAY":"a value with spaces and $literal"},
            "enabledPlugins":{"disabled@market":false}
        });
        let mut config = shell_config("printf '%s\\n' \"$@\"");
        config.extra_args.extend([
            "launcher".into(),
            "--settings".into(),
            "settings.json".into(),
            "--settings={\"env\":{\"SECOND\":\"inline\"}}".into(),
        ]);
        let output = command(&config, &options).unwrap().output().await.unwrap();
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).unwrap();
        let settings: Vec<_> = stdout
            .lines()
            .filter_map(|line| line.strip_prefix("--settings="))
            .collect();
        assert_eq!(settings.len(), 1);
        let settings: Value = serde_json::from_str(settings[0]).unwrap();
        assert_eq!(settings["permissions"], json!({"ask":["Bash"]}));
        assert_eq!(
            settings["env"],
            json!({"KEEP":"existing","SECOND":"inline","OVERLAY":"a value with spaces and $literal"})
        );
        assert_eq!(
            settings["enabledPlugins"],
            json!({"keep@market":true,"disabled@market":false})
        );
        assert!(!stdout.lines().any(|line| line == "--settings"));
    }

    #[cfg(unix)]
    #[test]
    fn sessions_do_not_inherit_facade_deployment_environment() {
        let command = command(&shell_config(":"), &test_options()).unwrap();
        let removed: Vec<_> = command
            .as_std()
            .get_envs()
            .filter(|(_, value)| value.is_none())
            .map(|(key, _)| key.to_string_lossy().into_owned())
            .collect();
        for key in FACADE_ENVIRONMENT {
            assert!(removed.iter().any(|k| k == key), "{key} is inherited");
        }
    }

    #[cfg(unix)]
    #[test]
    fn malformed_native_settings_fail_before_process_spawn() {
        let mut config = shell_config(":");
        config.extra_args.push("--settings".into());
        assert!(command(&config, &test_options()).is_err());
        config.extra_args.pop();
        let mut options = test_options();
        options.native_settings = Value::Null;
        assert!(command(&config, &options).is_err());
    }

    // UUID control IDs do not contain shell or JSON metacharacters. This tiny
    // peer keeps driver tests independent of building a separate test binary.
    #[cfg(unix)]
    const ACK: &str = r#"
        id=${line#*\"request_id\":\"}
        id=${id%%\"*}
        printf '{"type":"control_response","response":{"subtype":"success","request_id":"%s","response":{"models":[]}}}\n' "$id"
    "#;

    #[cfg(unix)]
    #[tokio::test]
    async fn sdk_discovery_completes_before_initialization_and_calls_are_forwarded() {
        let script = format!(
            r#"
            read -r line
            initialize=$line
            for method in initialize tools/list ping notifications/initialized; do
                rpc_id='"id":1,'
                case "$method" in notifications/*) rpc_id='' ;; esac
                printf '{{"type":"control_request","request_id":"discovery","request":{{"subtype":"mcp_message","server_name":"codex_desktop","message":{{"jsonrpc":"2.0",%s"method":"%s","params":{{"protocolVersion":"2025-06-18"}}}}}}}}\n' "$rpc_id" "$method"
                read -r response
                case "$response" in *error*) exit 3 ;; esac
                case "$response" in *mcp_response*) ;; *) exit 2 ;; esac
            done
            line=$initialize
            {ACK}
            printf '{{"type":"control_request","request_id":"call","request":{{"subtype":"mcp_message","server_name":"codex_desktop","message":{{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{{"name":"lookup","arguments":{{}}}}}}}}}}\n'
            while read -r line; do :; done
        "#
        );
        let mut options = test_options();
        options.dynamic_tools = vec![
            json!({"name":"lookup","description":"Find a record","inputSchema":{"type":"object"}}),
        ];
        let (mut backend, mut events) = ClaudeBackend::spawn(&shell_config(&script), &options)
            .await
            .unwrap();
        let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(event, BackendEvent::Message(value) if value["request_id"] == "call"
            && value["request"]["message"]["params"]["name"] == "lookup")
        );
        backend.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn handshake_and_interrupt_ack_are_not_turn_events() {
        let script = format!(
            r#"
            read -r line
            {ACK}
            printf '{{"type":"system","subtype":"init"}}\n'
            read -r line
            {ACK}
            printf '{{"type":"result","result":"interrupted"}}\n'
            while read -r line; do :; done
        "#
        );
        let (mut backend, mut events) =
            ClaudeBackend::spawn(&shell_config(&script), &test_options())
                .await
                .unwrap();
        assert_eq!(backend.initialization(), &json!({"models":[]}));
        backend.interrupt().await.unwrap();
        assert!(
            matches!(events.recv().await, Some(BackendEvent::Message(value)) if value["type"] == "system")
        );
        assert!(
            matches!(events.recv().await, Some(BackendEvent::Message(value)) if value["type"] == "result")
        );
        backend.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn forced_termination_reaps_the_backend_before_returning() {
        let script = format!("read -r line\n{ACK}\nwhile read -r line; do :; done");
        let (mut backend, _events) = ClaudeBackend::spawn(&shell_config(&script), &test_options())
            .await
            .unwrap();
        let process_id = backend.process_id as i32;
        assert_eq!(unsafe { libc::kill(process_id, 0) }, 0);
        backend.terminate().await.unwrap();
        assert_eq!(unsafe { libc::kill(process_id, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn early_exit_includes_stderr() {
        let result = ClaudeBackend::spawn(
            &shell_config("echo initialization-failed >&2; exit 23"),
            &test_options(),
        )
        .await;
        let error = match result {
            Ok(_) => panic!("expected initialization failure"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("initialization-failed"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn malformed_frame_terminates_backend() {
        let script = format!(
            r#"read -r line
            {ACK}
            read -r line
            printf '{{invalid JSON\n'
            sleep 60
        "#
        );
        let (mut backend, mut events) =
            ClaudeBackend::spawn(&shell_config(&script), &test_options())
                .await
                .unwrap();
        backend
            .send(json!({"type":"user","message":{"role":"user","content":"malformed"}}))
            .await
            .unwrap();
        let event = tokio::time::timeout(Duration::from_secs(3), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(event, BackendEvent::Exited { success: false, message } if message.contains("malformed Claude JSON frame"))
        );
        backend.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    fn guarded_options(dir: &std::path::Path) -> SessionOptions {
        let cwd = dir.join("workspace");
        std::fs::create_dir_all(&cwd).unwrap();
        SessionOptions {
            cwd,
            state_dir: dir.join("state"),
            session_id: uuid::Uuid::new_v4().to_string(),
            sandbox: crate::sandbox::Policy::workspace(),
            ..test_options()
        }
    }

    #[cfg(unix)]
    fn file_request(cwd: &std::path::Path, path: &std::path::Path) -> Value {
        json!({"type":"control_request","request_id":"file-hook","request":{
            "subtype":"hook_callback","callback_id":crate::file_guard::CALLBACK_ID,
            "input":{"hook_event_name":"PreToolUse","cwd":cwd,"tool_name":"Write",
                "tool_input":{"file_path":path,"content":"fixture"}}
        }})
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_callback_is_registered_and_serviced_before_init_on_every_mode_and_resume() {
        let dir = tempfile::tempdir().unwrap();
        let mut options = guarded_options(dir.path());
        for (mode, resume) in [("acceptEdits", false), ("auto", true), ("dontAsk", true)] {
            options.permission_mode = mode.into();
            options.resume = resume;
            let inside = file_request(&options.cwd, &options.cwd.join("new/file"));
            let outside = file_request(&options.cwd, &dir.path().join("outside"));
            let script = format!(
                r#"
                read -r initialization
                case "$initialization" in *claudex-workspace-files*) ;; *) exit 11 ;; esac
                case " $* " in *--permission-mode={mode}*) ;; *) exit 12 ;; esac
                printf '%s\n' '{inside}'
                read -r response
                case "$response" in *'"response":{{}}'*) ;; *) exit 13 ;; esac
                printf '%s\n' '{outside}'
                read -r response
                case "$response" in *'"permissionDecision":"deny"'*) ;; *) exit 14 ;; esac
                case "$response" in *'"subtype":"success"'*) ;; *) exit 15 ;; esac
                echo 'verified file callback decisions' >&2
                exit 37
            "#
            );
            let error = ClaudeBackend::spawn(&shell_config(&script), &options)
                .await
                .err()
                .unwrap();
            assert!(
                format!("{error:#}").contains("verified file callback decisions"),
                "{mode} resume={resume}: {error:#}"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unrouteable_hook_and_broken_callback_transport_kill_the_process() {
        let dir = tempfile::tempdir().unwrap();
        let options = guarded_options(dir.path());
        for failure in ["identity", "subtype", "outer-type", "transport"] {
            let broken_transport = failure == "transport";
            let marker = dir.path().join(format!("{failure}-escaped"));
            let mut request = file_request(&options.cwd, &dir.path().join("outside"));
            match failure {
                "identity" => request["request"]["callback_id"] = json!("unregistered"),
                "subtype" => request["request"]
                    .as_object_mut()
                    .unwrap()
                    .remove("subtype")
                    .map(|_| ())
                    .unwrap(),
                "outer-type" => request["type"] = json!("invalid"),
                _ => {}
            }
            let script = format!(
                r#"
                read -r initialization
                {}
                printf '%s\n' '{request}'
                sleep 1
                printf 'native continued' > '{}'
                sleep 10
            "#,
                if broken_transport { "exec 0<&-" } else { ":" },
                marker.display()
            );
            let error = ClaudeBackend::spawn(&shell_config(&script), &options)
                .await
                .err()
                .unwrap();
            let message = format!("{error:#}");
            let expected = match failure {
                "transport" => "writing Claude stdin",
                "identity" => "unexpected native file callback",
                _ => "invalid native file callback envelope",
            };
            assert!(message.contains(expected), "{message}");
            assert!(!marker.exists(), "native process survived failed callback");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn blocked_callback_delivery_and_event_backpressure_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let options = guarded_options(dir.path());
        let callback = file_request(&options.cwd, &dir.path().join("outside"));
        // Never drain callback replies. Once the pipe fills, the host must
        // terminate before native's 60-second permissive hook timeout.
        let script = format!(
            r#"
            read -r initialization
            i=0
            while [ "$i" -lt 2000 ]; do
                printf '%s\n' '{callback}'
                i=$((i + 1))
            done
            sleep 60
        "#
        );
        let mut config = shell_config(&script);
        config.initialize_timeout = Duration::from_secs(15);
        let started = std::time::Instant::now();
        let error = ClaudeBackend::spawn(&config, &options).await.err().unwrap();
        assert!(
            format!("{error:#}").contains("native file callback deadline exceeded"),
            "{error:#}"
        );
        assert!(started.elapsed() < Duration::from_secs(12));

        // Nothing drains events during startup. A burst beyond the channel
        // capacity is buffered, and the callback behind it is still answered.
        let script = format!(
            r#"
            read -r initialization
            i=0
            while [ "$i" -lt 300 ]; do
                printf '{{"type":"system","subtype":"fixture"}}\n'
                i=$((i + 1))
            done
            printf '%s\n' '{callback}'
            read -r response
            case "$response" in *'"permissionDecision":"deny"'*) ;; *) exit 13 ;; esac
            echo 'callback answered behind backlog' >&2
            exit 37
        "#
        );
        let error = ClaudeBackend::spawn(&shell_config(&script), &options)
            .await
            .err()
            .unwrap();
        assert!(
            format!("{error:#}").contains("callback answered behind backlog"),
            "{error:#}"
        );

        let padding = "x".repeat(1024);
        let script = format!(
            r#"
            read -r initialization
            i=0
            while [ "$i" -lt 400 ]; do
                printf '{{"type":"system","subtype":"fixture","padding":"{padding}"}}\n'
                i=$((i + 1))
            done
            sleep 60
        "#
        );
        let error = ClaudeBackend::spawn(&shell_config(&script), &options)
            .await
            .err()
            .unwrap();
        assert!(
            format!("{error:#}").contains("native event consumer stalled"),
            "{error:#}"
        );
    }
}
