//! Standalone, connection-owned host commands. These do not use the Claude backend.
use crate::protocol::{
    RpcError, RpcResult, notification, required_str, response, supported_fields,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    os::unix::process::{CommandExt, ExitStatusExt},
    path::Path,
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, unix::AsyncFd},
    process::{Child, ChildStdin, Command},
    sync::{Mutex, mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

const DEFAULT_CAP: usize = 1024 * 1024;
const DEFAULT_TIMEOUT_MS: u64 = 10_000;
type Pty = Arc<AsyncFd<OwnedFd>>;
type Key = (u64, ProcessId);

#[derive(Clone, Hash, PartialEq, Eq)]
enum ProcessId {
    Client(String),
    Process(String),
    Internal(String),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Api {
    Command,
    Process,
}
impl Api {
    fn key(self, id: String) -> ProcessId {
        match self {
            Self::Command => ProcessId::Client(id),
            Self::Process => ProcessId::Process(id),
        }
    }
}

/// The process API uses explicit null to disable limits, unlike command/exec.
/// Normalize only the request fields; lifecycle and notification shapes remain distinct.
fn process_params(method: &str, mut params: Value) -> RpcResult<Value> {
    let fields: &[&str] = match method {
        "process/spawn" => &[
            "command",
            "processHandle",
            "cwd",
            "tty",
            "streamStdin",
            "streamStdoutStderr",
            "outputBytesCap",
            "timeoutMs",
            "env",
            "size",
        ],
        "process/writeStdin" => &["processHandle", "deltaBase64", "closeStdin"],
        "process/resizePty" => &["processHandle", "size"],
        _ => &["processHandle"],
    };
    supported_fields(&params, fields)?;
    let handle = required_str(&params, "processHandle")?.to_owned();
    if method == "process/spawn" {
        let cwd = required_str(&params, "cwd")?;
        if !Path::new(cwd).is_absolute() {
            return Err(RpcError::invalid("process/spawn cwd must be absolute"));
        }
        if params.get("outputBytesCap").is_some_and(Value::is_null) {
            params["disableOutputCap"] = json!(true);
        }
        if params.get("timeoutMs").is_some_and(Value::is_null) {
            params["disableTimeout"] = json!(true);
        }
    }
    if method == "process/writeStdin"
        && params["deltaBase64"].is_null()
        && params["closeStdin"] != true
    {
        return Err(RpcError::invalid(
            "process/writeStdin requires deltaBase64 or closeStdin",
        ));
    }
    params.as_object_mut().unwrap().remove("processHandle");
    params["processId"] = json!(handle);
    Ok(params)
}

#[derive(Clone, Default)]
pub struct CommandManager {
    sessions: Arc<Mutex<HashMap<Key, Arc<Session>>>>,
}

struct Session {
    stdin: Mutex<Option<Input>>,
    writes: mpsc::Sender<WriteRequest>,
    pty: Option<Pty>,
    cancel: CancellationToken,
    disconnected: CancellationToken,
    done: CancellationToken,
}
struct WriteRequest {
    bytes: Vec<u8>,
    close: bool,
    reply: oneshot::Sender<RpcResult<()>>,
}
enum Input {
    Pipe(ChildStdin),
    Pty(Pty),
}
enum Reader {
    Pipe(Box<dyn AsyncRead + Send + Unpin>),
    Pty(Pty),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExecParams {
    command: Vec<String>,
    cwd: Option<String>,
    env: Option<HashMap<String, Option<String>>>,
    process_id: Option<String>,
    #[serde(default)]
    tty: bool,
    #[serde(default)]
    stream_stdin: bool,
    #[serde(default)]
    stream_stdout_stderr: bool,
    output_bytes_cap: Option<usize>,
    #[serde(default)]
    disable_output_cap: bool,
    timeout_ms: Option<i64>,
    #[serde(default)]
    disable_timeout: bool,
    sandbox_policy: Option<Value>,
    permission_profile: Option<String>,
    size: Option<Size>,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct Size {
    rows: u16,
    cols: u16,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WriteParams {
    process_id: String,
    delta_base64: Option<String>,
    #[serde(default)]
    close_stdin: bool,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResizeParams {
    process_id: String,
    size: Size,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TerminateParams {
    process_id: String,
}

fn parse<T: serde::de::DeserializeOwned>(params: Value) -> RpcResult<T> {
    serde_json::from_value(params).map_err(|e| RpcError::invalid(e.to_string()))
}

impl CommandManager {
    /// `None` means this manager owns the deferred response. Never await process exit
    /// in the server's request loop; controls and other requests must remain available.
    pub async fn dispatch(
        &self,
        owner: u64,
        output: mpsc::Sender<Value>,
        request_id: Value,
        method: &str,
        params: Value,
        default_cwd: &Path,
    ) -> RpcResult<Option<Value>> {
        let (api, method, params) = match method {
            "process/spawn" => (
                Api::Process,
                "command/exec",
                process_params(method, params)?,
            ),
            "process/writeStdin" => (
                Api::Process,
                "command/exec/write",
                process_params(method, params)?,
            ),
            "process/resizePty" => (
                Api::Process,
                "command/exec/resize",
                process_params(method, params)?,
            ),
            "process/kill" => (
                Api::Process,
                "command/exec/terminate",
                process_params(method, params)?,
            ),
            _ => (Api::Command, method, params),
        };
        match method {
            "command/exec" => {
                self.start(owner, output, request_id, parse(params)?, default_cwd, api)
                    .await?;
                Ok(None)
            }
            "command/exec/write" => {
                let params: WriteParams = parse(params)?;
                let delta = params
                    .delta_base64
                    .map(|s| STANDARD.decode(s))
                    .transpose()
                    .map_err(|_| RpcError::invalid("deltaBase64 must be valid base64"))?
                    .unwrap_or_default();
                let session = self.session(owner, &params.process_id, api).await?;
                if session.pty.is_some() && params.close_stdin {
                    return Err(RpcError::invalid(
                        "closeStdin is unsupported for PTYs; send terminal EOF bytes or terminate the process",
                    ));
                }
                // Preserve request order without blocking the server on a full pipe.
                let (reply, completion) = oneshot::channel();
                session
                    .writes
                    .try_send(WriteRequest {
                        bytes: delta,
                        close: params.close_stdin,
                        reply,
                    })
                    .map_err(|_| {
                        RpcError::invalid("command stdin is closed or its write queue is full")
                    })?;
                tokio::spawn(async move {
                    let result = completion
                        .await
                        .unwrap_or_else(|_| Err(RpcError::invalid("command exited")));
                    let message = match result {
                        Ok(()) => response(request_id, json!({})),
                        Err(e) => e.response(request_id),
                    };
                    tokio::select! { _ = output.send(message) => {}, _ = session.disconnected.cancelled() => {} }
                });
                Ok(None)
            }
            "command/exec/resize" => {
                let params: ResizeParams = parse(params)?;
                let session = self.session(owner, &params.process_id, api).await?;
                let pty = session
                    .pty
                    .as_ref()
                    .ok_or_else(|| RpcError::invalid("resize requires a PTY"))?;
                resize(pty.as_raw_fd(), params.size).map_err(RpcError::internal)?;
                Ok(Some(json!({})))
            }
            "command/exec/terminate" => {
                let params: TerminateParams = parse(params)?;
                self.session(owner, &params.process_id, api)
                    .await?
                    .cancel
                    .cancel();
                Ok(Some(json!({})))
            }
            _ => Err(RpcError::unsupported(method)),
        }
    }

    async fn session(&self, owner: u64, id: &str, api: Api) -> RpcResult<Arc<Session>> {
        self.sessions
            .lock()
            .await
            .get(&(owner, api.key(id.into())))
            .cloned()
            .ok_or_else(|| {
                RpcError::invalid("no active command/exec with this processId on this connection")
            })
    }

    /// Cancels all commands, including buffered requests without public process IDs,
    /// and waits until their children have been reaped.
    pub async fn disconnect(&self, owner: u64) {
        let sessions: Vec<_> = self
            .sessions
            .lock()
            .await
            .iter()
            .filter(|((id, _), _)| *id == owner)
            .map(|(_, session)| session.clone())
            .collect();
        for session in &sessions {
            session.disconnected.cancel();
            session.cancel.cancel();
        }
        for session in sessions {
            session.done.cancelled().await;
        }
    }

    async fn start(
        &self,
        owner: u64,
        output: mpsc::Sender<Value>,
        request_id: Value,
        params: ExecParams,
        default_cwd: &Path,
        api: Api,
    ) -> RpcResult<()> {
        if params.command.is_empty() || params.command[0].is_empty() {
            return Err(RpcError::invalid("command must contain a program"));
        }
        if params.permission_profile.is_some() {
            return Err(RpcError::invalid(
                "permissionProfile is unsupported; only dangerFullAccess host commands are available",
            ));
        }
        if let Some(policy) = &params.sandbox_policy
            && policy != &json!({"type":"dangerFullAccess"})
        {
            return Err(RpcError::invalid(
                "only sandboxPolicy dangerFullAccess is supported",
            ));
        }
        if params.disable_output_cap && params.output_bytes_cap.is_some() {
            return Err(RpcError::invalid(
                "disableOutputCap cannot be combined with outputBytesCap",
            ));
        }
        if params.disable_timeout && params.timeout_ms.is_some() {
            return Err(RpcError::invalid(
                "disableTimeout cannot be combined with timeoutMs",
            ));
        }
        if params.timeout_ms.is_some_and(|n| n < 0) {
            return Err(RpcError::invalid("timeoutMs must be non-negative"));
        }
        if params.size.is_some() && !params.tty {
            return Err(RpcError::invalid("size requires tty"));
        }
        if (params.tty || params.stream_stdin || params.stream_stdout_stderr)
            && params.process_id.is_none()
        {
            return Err(RpcError::invalid("tty and streaming require processId"));
        }
        let key = (
            owner,
            params
                .process_id
                .clone()
                .map(|id| api.key(id))
                .unwrap_or_else(|| ProcessId::Internal(uuid::Uuid::new_v4().to_string())),
        );
        let mut sessions = self.sessions.lock().await;
        if sessions.contains_key(&key) {
            return Err(RpcError::invalid("duplicate active command/exec processId"));
        }
        let mut command = Command::new(&params.command[0]);
        command
            .args(&params.command[1..])
            .current_dir(default_cwd.join(params.cwd.as_deref().unwrap_or(".")))
            .kill_on_drop(true);
        for (name, value) in params.env.iter().flatten() {
            match value {
                Some(value) => {
                    command.env(name, value);
                }
                None => {
                    command.env_remove(name);
                }
            }
        }
        let streaming = params.tty || params.stream_stdout_stderr;
        let (child, stdin, stdout, stderr, pty) =
            spawn(&mut command, params.tty, params.stream_stdin, params.size)
                .map_err(RpcError::internal)?;
        let (writes, mut requests) = mpsc::channel::<WriteRequest>(32);
        let session = Arc::new(Session {
            stdin: Mutex::new(stdin),
            writes,
            pty,
            cancel: CancellationToken::new(),
            disconnected: CancellationToken::new(),
            done: CancellationToken::new(),
        });
        sessions.insert(key.clone(), session.clone());
        let writer_session = session.clone();
        tokio::spawn(async move {
            loop {
                let request = tokio::select! {
                    biased;
                    _ = writer_session.cancel.cancelled() => break,
                    _ = writer_session.done.cancelled() => break,
                    request = requests.recv() => match request { Some(request) => request, None => break },
                };
                let result = tokio::select! {
                    biased;
                    _ = writer_session.cancel.cancelled() => Err(RpcError::invalid("command terminated")),
                    _ = writer_session.done.cancelled() => Err(RpcError::invalid("command exited")),
                    result = write_input(&writer_session, &request.bytes, request.close) => result,
                };
                let _ = request.reply.send(result);
            }
        });
        let manager = self.clone();
        tokio::spawn(async move {
            if api == Api::Process {
                // Installed desktop registers output handlers before spawning, but
                // the protocol still requires the successful spawn ack first.
                tokio::select! {
                    _ = output.send(response(request_id.clone(), json!({}))) => {},
                    _ = session.disconnected.cancelled() => {},
                }
            }
            let cap = if params.disable_output_cap {
                None
            } else {
                Some(params.output_bytes_cap.unwrap_or(DEFAULT_CAP))
            };
            let timeout = if params.disable_timeout {
                None
            } else {
                Some(Duration::from_millis(
                    params
                        .timeout_ms
                        .map(|n| n as u64)
                        .unwrap_or(DEFAULT_TIMEOUT_MS),
                ))
            };
            let result = run(
                child,
                stdout,
                stderr,
                &session,
                &output,
                request_id,
                params.process_id,
                streaming,
                cap,
                timeout,
                api,
            )
            .await;
            manager.sessions.lock().await.remove(&key);
            session.done.cancel();
            // A handle is reusable as soon as its exit notification is observable.
            tokio::select! {
                biased;
                _ = output.send(result) => {},
                _ = session.disconnected.cancelled() => {}
            }
        });
        Ok(())
    }
}

async fn write_input(session: &Session, bytes: &[u8], close: bool) -> RpcResult<()> {
    let mut input = session.stdin.lock().await;
    let writer = input
        .as_mut()
        .ok_or_else(|| RpcError::invalid("stdin is closed or streaming is disabled"))?;
    match writer {
        Input::Pipe(pipe) => pipe.write_all(bytes).await.map_err(RpcError::internal)?,
        Input::Pty(pty) => {
            let mut remaining = bytes;
            while !remaining.is_empty() {
                let mut ready = pty.writable().await.map_err(RpcError::internal)?;
                match ready.try_io(|fd| {
                    let n = unsafe {
                        libc::write(fd.as_raw_fd(), remaining.as_ptr().cast(), remaining.len())
                    };
                    if n < 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(n as usize)
                    }
                }) {
                    Ok(Ok(0)) => return Err(RpcError::internal("PTY write returned zero")),
                    Ok(Ok(n)) => remaining = &remaining[n..],
                    Ok(Err(e)) => return Err(RpcError::internal(e)),
                    Err(_) => continue,
                }
            }
        }
    }
    if close {
        input.take();
    }
    Ok(())
}

type Spawned = (Child, Option<Input>, Reader, Option<Reader>, Option<Pty>);
fn spawn(
    command: &mut Command,
    tty: bool,
    stream_stdin: bool,
    size: Option<Size>,
) -> io::Result<Spawned> {
    if !tty {
        command
            .stdin(if stream_stdin {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command.as_std_mut().process_group(0);
        let mut child = command.spawn()?;
        let stdin = child.stdin.take().map(Input::Pipe);
        let stdout = Reader::Pipe(Box::new(child.stdout.take().unwrap()));
        let stderr = Some(Reader::Pipe(Box::new(child.stderr.take().unwrap())));
        return Ok((child, stdin, stdout, stderr, None));
    }
    let mut master = -1;
    let mut slave = -1;
    let size = size.unwrap_or(Size { rows: 24, cols: 80 });
    let mut window = window_size(size)?;
    if unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &raw mut window,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    let master = unsafe { OwnedFd::from_raw_fd(master) };
    let slave = unsafe { OwnedFd::from_raw_fd(slave) };
    for fd in [master.as_raw_fd(), slave.as_raw_fd()] {
        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    if unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let pty = Arc::new(AsyncFd::new(master)?);
    command
        .stdin(Stdio::from(slave.try_clone()?))
        .stdout(Stdio::from(slave.try_clone()?))
        .stderr(Stdio::from(slave));
    // Only async-signal-safe system calls are allowed after fork.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY as _, 0) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn()?;
    Ok((
        child,
        Some(Input::Pty(pty.clone())),
        Reader::Pty(pty.clone()),
        None,
        Some(pty),
    ))
}

fn window_size(size: Size) -> io::Result<libc::winsize> {
    if size.rows == 0 || size.cols == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "terminal dimensions must be nonzero",
        ));
    }
    Ok(libc::winsize {
        ws_row: size.rows,
        ws_col: size.cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    })
}
fn resize(fd: i32, size: Size) -> io::Result<()> {
    let size = window_size(size)?;
    if unsafe { libc::ioctl(fd, libc::TIOCSWINSZ as _, &size) } < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

impl Reader {
    async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        match self {
            Reader::Pipe(pipe) => pipe.read(bytes).await,
            Reader::Pty(pty) => loop {
                let mut ready = pty.readable().await?;
                match ready.try_io(|fd| {
                    let n = unsafe {
                        libc::read(fd.as_raw_fd(), bytes.as_mut_ptr().cast(), bytes.len())
                    };
                    if n < 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(n as usize)
                    }
                }) {
                    // Linux reports EIO when the last slave closes; macOS reports EOF.
                    Ok(Err(e)) if e.raw_os_error() == Some(libc::EIO) => return Ok(0),
                    Ok(result) => return result,
                    Err(_) => continue,
                }
            },
        }
    }
}

struct Output {
    reader: Reader,
    stream: &'static str,
    streaming: bool,
    cap: Option<usize>,
    process_id: Option<String>,
    sender: mpsc::Sender<Value>,
    stop: CancellationToken,
    api: Api,
}
#[derive(Debug, Default)]
struct Capture {
    text: String,
    cap_reached: bool,
}
async fn capture(mut output: Output) -> io::Result<Capture> {
    let mut buffer = Vec::new();
    let mut observed = 0usize;
    let mut cap_notified = false;
    let mut cap_reached = false;
    let mut bytes = [0u8; 8192];
    loop {
        let n = tokio::select! { _ = output.stop.cancelled() => break, n = output.reader.read(&mut bytes) => n? };
        if n == 0 {
            break;
        }
        let keep = output
            .cap
            .map_or(n, |cap| cap.saturating_sub(observed).min(n));
        observed = observed.saturating_add(keep);
        let capped = output.cap.is_some_and(|cap| observed >= cap);
        cap_reached = capped;
        if output.streaming && !cap_notified {
            let (method, id_field) = match output.api {
                Api::Command => ("command/exec/outputDelta", "processId"),
                Api::Process => ("process/outputDelta", "processHandle"),
            };
            let message = notification(
                method,
                json!({id_field:output.process_id,"stream":output.stream,"deltaBase64":STANDARD.encode(&bytes[..keep]),"capReached":capped}),
            );
            tokio::select! { _ = output.stop.cancelled() => break, result = output.sender.send(message) => { if result.is_err() { break; } } }
            cap_notified = capped;
        } else if !output.streaming {
            buffer.extend_from_slice(&bytes[..keep]);
        }
        // Continue draining after the cap so a verbose child cannot deadlock on a full pipe.
    }
    Ok(Capture {
        text: String::from_utf8_lossy(&buffer).into_owned(),
        cap_reached,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run(
    mut child: Child,
    stdout: Reader,
    stderr: Option<Reader>,
    session: &Session,
    output: &mpsc::Sender<Value>,
    request_id: Value,
    process_id: Option<String>,
    streaming: bool,
    cap: Option<usize>,
    timeout: Option<Duration>,
    api: Api,
) -> Value {
    let pid = child.id().expect("new child has a pid") as i32;
    let stop = CancellationToken::new();
    let make_output = |reader, stream| Output {
        reader,
        stream,
        streaming,
        cap,
        process_id: process_id.clone(),
        sender: output.clone(),
        stop: stop.clone(),
        api,
    };
    let stdout = tokio::spawn(capture(make_output(stdout, "stdout")));
    let stderr = stderr.map(|reader| tokio::spawn(capture(make_output(reader, "stderr"))));
    let deadline = async {
        match timeout {
            Some(duration) => tokio::time::sleep(duration).await,
            None => std::future::pending().await,
        }
    };
    let mut timed_out = false;
    let status = tokio::select! {
        status = child.wait() => status,
        _ = session.cancel.cancelled() => { kill_group(pid); child.wait().await },
        _ = output.closed() => { kill_group(pid); child.wait().await },
        _ = deadline => { timed_out = true; kill_group(pid); child.wait().await },
    };
    // The command's lifetime owns descendants too, including ones holding inherited pipes.
    kill_group(pid);
    let drain_stop = stop.clone();
    let drain = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        drain_stop.cancel();
    });
    let stdout = stdout.await;
    let stderr = match stderr {
        Some(handle) => handle.await,
        None => Ok(Ok(Capture::default())),
    };
    drain.abort();
    match (status, stdout, stderr) {
        (Ok(status), Ok(Ok(stdout)), Ok(Ok(stderr))) => {
            let exit_code = if timed_out {
                124
            } else {
                status
                    .code()
                    .unwrap_or_else(|| 128 + status.signal().unwrap_or(0))
            };
            match api {
                Api::Command => response(
                    request_id,
                    json!({"exitCode":exit_code,"stdout":stdout.text,"stderr":stderr.text}),
                ),
                Api::Process => notification(
                    "process/exited",
                    json!({"processHandle":process_id,"exitCode":exit_code,"stdout":stdout.text,"stderr":stderr.text,"stdoutCapReached":stdout.cap_reached,"stderrCapReached":stderr.cap_reached}),
                ),
            }
        }
        (status, stdout, stderr) => {
            let error = format!(
                "command execution failed: {status:?}; stdout: {stdout:?}; stderr: {stderr:?}"
            );
            match api {
                Api::Command => RpcError::internal(error).response(request_id),
                Api::Process => notification(
                    "process/exited",
                    json!({"processHandle":process_id,"exitCode":-1,"stdout":"","stderr":error,"stdoutCapReached":false,"stderrCapReached":false}),
                ),
            }
        }
    }
}
fn kill_group(pid: i32) {
    unsafe {
        libc::kill(-pid, libc::SIGKILL);
    }
}
