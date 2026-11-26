//! Multiplexed transport to a genuine Codex app-server. This module owns no
//! catalog, credentials, thread mapping, or model execution policy.
use crate::protocol::{RpcError, RpcResult};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::{Notify, Semaphore, mpsc, oneshot},
};
use tokio_util::{
    codec::{FramedRead, LinesCodec},
    sync::CancellationToken,
};

const MAX_FRAME: usize = 32 * 1024 * 1024;
const MAX_IN_FLIGHT: usize = 64;
const WRITE_QUEUE: usize = 32;
const EVENT_QUEUE: usize = 128;
type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<RpcResult<Value>>>>>;

#[derive(Clone, Debug)]
pub struct Config {
    pub executable: PathBuf,
    pub home: PathBuf,
    pub cwd: PathBuf,
    /// Native TOML key=value overrides, each passed as its own `-c` argument.
    pub overrides: Vec<String>,
}

struct Write {
    frame: Vec<u8>,
    written: Option<oneshot::Sender<RpcResult<()>>>,
}

struct State {
    alive: AtomicBool,
    finished: AtomicBool,
    failure: Mutex<Option<String>>,
    done: Notify,
}

impl State {
    fn error(&self) -> RpcError {
        RpcError::internal(
            self.failure
                .lock()
                .unwrap()
                .as_deref()
                .unwrap_or("Codex plugin service is closed"),
        )
    }
}

struct Inner {
    writer: mpsc::Sender<Write>,
    pending: Pending,
    cancel: CancellationToken,
    state: Arc<State>,
    slots: Semaphore,
    prefix: String,
    serial: AtomicU64,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Clones share one initialized sidecar. Dropping the final clone initiates
/// process cleanup; `shutdown` additionally waits for reaping.
#[derive(Clone)]
pub struct Client(Arc<Inner>);

#[derive(Clone)]
pub struct WeakClient(std::sync::Weak<Inner>);
impl WeakClient {
    pub fn upgrade(&self) -> Option<Client> {
        self.0.upgrade().map(Client)
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

impl Client {
    pub fn downgrade(&self) -> WeakClient {
        WeakClient(Arc::downgrade(&self.0))
    }
    pub async fn spawn(config: Config) -> RpcResult<(Self, mpsc::Receiver<Value>)> {
        if !config.executable.is_absolute()
            || !config.home.is_absolute()
            || !config.cwd.is_absolute()
        {
            return Err(RpcError::invalid(
                "Codex executable, home, and cwd must be absolute paths",
            ));
        }
        let executable = config.executable.canonicalize().map_err(|e| {
            RpcError::internal(format!(
                "Cannot resolve genuine Codex executable {}: {e}",
                config.executable.display()
            ))
        })?;
        let current = std::env::current_exe()
            .and_then(|p| p.canonicalize())
            .map_err(RpcError::internal)?;
        if executable == current {
            return Err(RpcError::invalid(
                "Codex plugin executable resolves to the facade itself",
            ));
        }
        let mut command = Command::new(&executable);
        for value in &config.overrides {
            command.arg("-c").arg(value);
        }
        command
            .args(["app-server", "--stdio"])
            .env("CODEX_HOME", &config.home)
            .current_dir(&config.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn().map_err(|e| {
            RpcError::internal(format!(
                "Cannot start genuine Codex plugin service {}: {e}",
                executable.display()
            ))
        })?;
        let process_id = child.id();
        let mut input = child.stdin.take().expect("piped stdin");
        let output = child.stdout.take().expect("piped stdout");
        let mut stderr = child.stderr.take().expect("piped stderr");
        let (writer, mut writes) = mpsc::channel::<Write>(WRITE_QUEUE);
        let (events, receiver) = mpsc::channel(EVENT_QUEUE);
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let state = Arc::new(State {
            alive: AtomicBool::new(true),
            finished: AtomicBool::new(false),
            failure: Mutex::new(None),
            done: Notify::new(),
        });
        let cancel = CancellationToken::new();
        let client = Self(Arc::new(Inner {
            writer,
            pending: pending.clone(),
            cancel: cancel.clone(),
            state: state.clone(),
            slots: Semaphore::new(MAX_IN_FLIGHT),
            prefix: format!("facade-plugin-{}-", uuid::Uuid::new_v4()),
            serial: AtomicU64::new(1),
        }));

        let read_pending = pending.clone();
        let read_state = state.clone();
        let mut reader = tokio::spawn(async move {
            let mut lines = FramedRead::new(output, LinesCodec::new_with_max_length(MAX_FRAME));
            let result = async {
                while let Some(line) = lines.next().await {
                    let line = line.map_err(|_| {
                        "Codex plugin service emitted an invalid or oversized frame".to_owned()
                    })?;
                    let envelope: Value = serde_json::from_str(&line)
                        .map_err(|_| "Codex plugin service emitted invalid JSON".to_owned())?;
                    if !envelope.is_object() {
                        return Err("Codex plugin service emitted a non-object envelope".to_owned());
                    }
                    if envelope.get("method").is_some() {
                        // Never drop an approval/elicitation silently. Queue overflow
                        // closes the sidecar and fails its pending callers explicitly.
                        events.try_send(envelope).map_err(|error| match error {
                            mpsc::error::TrySendError::Full(_) => {
                                "Codex plugin event queue is full".to_owned()
                            }
                            mpsc::error::TrySendError::Closed(_) => {
                                "Codex plugin event receiver is closed".to_owned()
                            }
                        })?;
                        continue;
                    }
                    let Some(id) = envelope["id"].as_str() else {
                        return Err("Codex plugin response has no string request id".to_owned());
                    };
                    let sender = read_pending.lock().unwrap().remove(id);
                    // A reply may arrive after its caller timed out or was cancelled.
                    if let Some(sender) = sender {
                        let response = if let Some(error) = envelope.get("error") {
                            Err(RpcError {
                                code: error["code"]
                                    .as_i64()
                                    .and_then(|n| i32::try_from(n).ok())
                                    .unwrap_or(-32603),
                                message: error["message"]
                                    .as_str()
                                    .unwrap_or("Codex plugin request failed")
                                    .to_owned(),
                                data: error.get("data").cloned(),
                            })
                        } else if let Some(result) = envelope.get("result") {
                            Ok(result.clone())
                        } else {
                            Err(RpcError::internal(
                                "Codex plugin response has neither result nor error",
                            ))
                        };
                        let _ = sender.send(response);
                    }
                }
                Ok::<(), String>(())
            }
            .await;
            match result {
                Err(failure) => {
                    // Record the reader's cause while stdout is still open.
                    // Dropping it can make the child exit with SIGPIPE before
                    // the supervisor observes this task's completion.
                    read_state
                        .failure
                        .lock()
                        .unwrap()
                        .get_or_insert_with(|| failure.clone());
                    Err(failure)
                }
                Ok(()) => Err("Codex plugin service closed stdout".to_owned()),
            }
        });
        let mut write_task = tokio::spawn(async move {
            while let Some(write) = writes.recv().await {
                match input.write_all(&write.frame).await {
                    Ok(()) => {
                        if let Some(written) = write.written {
                            let _ = written.send(Ok(()));
                        }
                    }
                    Err(error) => {
                        if let Some(written) = write.written {
                            let _ = written.send(Err(RpcError::internal(
                                "Cannot write to Codex plugin service",
                            )));
                        }
                        return Err::<(), String>(format!(
                            "Codex plugin transport write failed: {}",
                            error.kind()
                        ));
                    }
                }
            }
            Ok(())
        });
        // Drain without retaining or echoing logs: startup diagnostics can
        // contain endpoint headers, tokens, or user configuration values.
        let drain = tokio::spawn(async move {
            let mut buffer = [0u8; 8192];
            while matches!(stderr.read(&mut buffer).await,Ok(n) if n > 0) {}
        });
        tokio::spawn(async move {
            let failure = tokio::select! {
                _ = cancel.cancelled() => "Codex plugin service shut down".to_owned(),
                status = child.wait() => match status {Ok(status)=>format!("Codex plugin service exited ({status})"),Err(error)=>format!("Cannot wait for Codex plugin service: {}",error.kind())},
                result = &mut reader => task_failure("reader",result),
                result = &mut write_task => task_failure("writer",result),
            };
            state.failure.lock().unwrap().get_or_insert(failure);
            state.alive.store(false, Ordering::Release);
            for (_, sender) in pending.lock().unwrap().drain() {
                let _ = sender.send(Err(state.error()));
            }
            reader.abort();
            write_task.abort();
            drain.abort();
            // The isolated process group includes ordinary child MCP servers.
            // Independently daemonized descendants are outside this ownership.
            #[cfg(unix)]
            if let Some(pid) = process_id {
                unsafe {
                    libc::kill(-(pid as i32), libc::SIGKILL);
                }
            }
            #[cfg(not(unix))]
            let _ = process_id;
            let _ = child.start_kill();
            let _ = child.wait().await;
            state.finished.store(true, Ordering::Release);
            state.done.notify_waiters();
        });
        let initialized = client.request_with_timeout("initialize",json!({"clientInfo":{"name":"claude-codex-plugin-bridge","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":true}}),Duration::from_secs(30)).await;
        if let Err(error) = initialized {
            client.shutdown().await;
            return Err(RpcError {
                code: error.code,
                message: format!(
                    "Genuine Codex plugin initialization failed: {}",
                    error.message
                ),
                data: error.data,
            });
        }
        if let Err(error) = client.write(json!({"method":"initialized"})).await {
            client.shutdown().await;
            return Err(error);
        }
        Ok((client, receiver))
    }

    pub fn is_alive(&self) -> bool {
        self.0.state.alive.load(Ordering::Acquire)
    }

    pub async fn request(&self, method: &str, params: Value) -> RpcResult<Value> {
        let timeout = match method {
            "mcpServer/tool/call" | "mcpServer/oauth/login" => Duration::from_secs(30 * 60),
            "plugin/install" | "plugin/reconcile" => Duration::from_secs(10 * 60),
            _ => Duration::from_secs(120),
        };
        self.request_with_timeout(method, params, timeout).await
    }

    /// A timeout cannot establish whether a remote mutation ran. Callers must
    /// reconcile state before retrying non-idempotent operations.
    pub async fn request_with_timeout(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> RpcResult<Value> {
        let operation = async {
            let _slot = self
                .0
                .slots
                .acquire()
                .await
                .map_err(|_| self.0.state.error())?;
            let id = format!(
                "{}{}",
                self.0.prefix,
                self.0.serial.fetch_add(1, Ordering::Relaxed)
            );
            let frame = encode(json!({"id":id,"method":method,"params":params}))?;
            let (sender, receiver) = oneshot::channel();
            {
                let mut pending = self.0.pending.lock().unwrap();
                if !self.is_alive() {
                    return Err(self.0.state.error());
                }
                pending.insert(id.clone(), sender);
            }
            let _guard = PendingGuard {
                pending: self.0.pending.clone(),
                id,
            };
            self.0
                .writer
                .send(Write {
                    frame,
                    written: None,
                })
                .await
                .map_err(|_| self.0.state.error())?;
            receiver.await.map_err(|_| self.0.state.error())?
        };
        tokio::time::timeout(timeout, operation)
            .await
            .map_err(|_| {
                RpcError::new(
                    -32000,
                    format!("Codex plugin {method} timed out; the operation may still be running"),
                )
            })?
    }

    pub async fn respond(&self, id: Value, result: Result<Value, RpcError>) -> RpcResult<()> {
        if !id.is_string() && !id.is_number() {
            return Err(RpcError::invalid(
                "Codex server request id must be a string or number",
            ));
        }
        let envelope = match result {
            Ok(result) => json!({"id":id,"result":result}),
            Err(error) => error.response(id),
        };
        self.write(envelope).await
    }

    async fn write(&self, envelope: Value) -> RpcResult<()> {
        if !self.is_alive() {
            return Err(self.0.state.error());
        }
        let frame = encode(envelope)?;
        let (written, ack) = oneshot::channel();
        tokio::time::timeout(Duration::from_secs(30), async {
            self.0
                .writer
                .send(Write {
                    frame,
                    written: Some(written),
                })
                .await
                .map_err(|_| self.0.state.error())?;
            ack.await.map_err(|_| self.0.state.error())?
        })
        .await
        .map_err(|_| RpcError::internal("Codex plugin response write timed out"))?
    }

    pub async fn shutdown(&self) {
        self.0.cancel.cancel();
        loop {
            let notified = self.0.state.done.notified();
            if self.0.state.finished.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

fn task_failure(name: &str, result: Result<Result<(), String>, tokio::task::JoinError>) -> String {
    match result {
        Ok(Err(message)) => message,
        Ok(Ok(())) => format!("Codex plugin {name} closed"),
        Err(_) => format!("Codex plugin {name} task failed"),
    }
}

fn encode(value: Value) -> RpcResult<Vec<u8>> {
    let mut bytes = serde_json::to_vec(&value).map_err(RpcError::internal)?;
    if bytes.len() > MAX_FRAME {
        return Err(RpcError::invalid(
            "Codex plugin request exceeds the 32 MiB frame limit",
        ));
    }
    bytes.push(b'\n');
    Ok(bytes)
}
