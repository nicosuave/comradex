//! Backend processes (Claude or an ACP agent) kept between turns.
//!
//! Every Claude launch renders its request prefix again: MCP servers race to
//! connect, and the Codex plugin catalog is rebuilt. Any difference invalidates
//! the prompt cache for the whole conversation, so a thread keeps its process
//! while idle and the next turn reuses it when nothing it was launched with changed.
//!
//! An idle Claude process can also start work by itself: a background command
//! or agent that outlived its turn finishes, and Claude answers the
//! notification. The process is then handed back to the server as a new turn.
//! While such work is running, the process is not an idle cache: it is neither
//! expired nor evicted, since closing it would kill the work.
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde_json::{Value, json};
use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::backend::{Backend, BackendEvent, SessionOptions};

pub struct WarmSession {
    pub backend: Backend,
    pub output: mpsc::Receiver<BackendEvent>,
    pub plugins: Option<crate::plugin_runtime::Runtime>,
    /// Background tasks still running in the process.
    pub background: HashSet<String>,
    /// Frames read while idle that the next turn must still process.
    pub replay: Vec<Value>,
}

/// After a background task finishes, Claude may take a moment to answer it.
const ANSWER_GRACE: Duration = Duration::from_secs(60);
/// Idle status frames kept for replay; older ones carry nothing a turn needs.
const MAX_REPLAY: usize = 256;

/// Track Claude's background tasks (commands, monitors, agents, workflows).
pub fn track_background(background: &mut HashSet<String>, frame: &Value) {
    if frame["type"] != "system" || !frame["parent_tool_use_id"].is_null() {
        return;
    }
    let Some(id) = frame["task_id"].as_str() else {
        return;
    };
    match frame["subtype"].as_str() {
        Some("task_started") => {
            background.insert(id.to_owned());
        }
        Some("task_notification") => {
            background.remove(id);
        }
        Some("task_updated")
            if matches!(
                frame["patch"]["status"].as_str(),
                Some("completed" | "failed" | "killed" | "stopped")
            ) =>
        {
            background.remove(id);
        }
        _ => {}
    }
}

impl WarmSession {
    pub async fn close(mut self) {
        // Shutdown sends EOF first so Claude persists its transcript.
        let _ = self.backend.shutdown().await;
        if let Some(runtime) = self.plugins {
            runtime.client.shutdown().await;
        }
    }
}

/// An idle session owned by a watcher task until a turn reclaims it.
pub struct Parked {
    pub key: Value,
    pub session_id: String,
    pub parked_at: Instant,
    pub(crate) prediction: Option<crate::backend::PredictionClient>,
    running: Arc<AtomicUsize>,
    woke: Arc<AtomicBool>,
    reclaim: CancellationToken,
    watcher: JoinHandle<Option<WarmSession>>,
}

/// Where an idle session goes when Claude starts work by itself.
pub struct Wake {
    pub thread_id: String,
    pub events: mpsc::Sender<crate::server::Event>,
}

impl Parked {
    pub fn new(
        key: Value,
        session_id: String,
        session: WarmSession,
        timeout: Option<Duration>,
        wake: Wake,
    ) -> Self {
        let reclaim = CancellationToken::new();
        let running = Arc::new(AtomicUsize::new(session.background.len()));
        let woke = Arc::new(AtomicBool::new(false));
        let prediction = session.backend.prediction_client();
        Self {
            watcher: tokio::spawn(watch(
                session,
                reclaim.clone(),
                timeout,
                key.clone(),
                wake,
                running.clone(),
                woke.clone(),
            )),
            running,
            woke,
            key,
            session_id,
            parked_at: Instant::now(),
            prediction,
            reclaim,
        }
    }

    /// Background tasks that closing this session would stop.
    pub fn running_background(&self) -> usize {
        self.running.load(Ordering::Relaxed)
    }

    /// Claude started work by itself; the session is on its way to a new turn.
    pub fn woke(&self) -> bool {
        self.woke.load(Ordering::Relaxed)
    }

    /// False once the watcher closed the session or handed it back as a wake.
    pub fn is_open(&self) -> bool {
        !self.watcher.is_finished()
    }

    pub async fn take(self) -> Option<WarmSession> {
        self.reclaim.cancel();
        self.watcher.await.ok().flatten()
    }

    pub async fn close(self) {
        if let Some(session) = self.take().await {
            session.close().await;
        }
    }
}

async fn watch(
    mut session: WarmSession,
    reclaim: CancellationToken,
    timeout: Option<Duration>,
    key: Value,
    wake: Wake,
    running: Arc<AtomicUsize>,
    woke: Arc<AtomicBool>,
) -> Option<WarmSession> {
    // Running background work suspends expiry; it resumes once the work ends.
    let mut deadline = session
        .background
        .is_empty()
        .then(|| timeout.map(|timeout| Instant::now() + timeout))
        .flatten();
    loop {
        tokio::select! {
            _ = reclaim.cancelled() => {
                // Frames queued before the reclaim belong to the next turn,
                // including the start of work Claude began by itself.
                while let Ok(event) = session.output.try_recv() {
                    match event {
                        BackendEvent::Message(frame) => {
                            track_background(&mut session.background, &frame);
                            session.replay.push(frame);
                        }
                        BackendEvent::Exited { .. } => break,
                    }
                }
                if session.backend.is_running() {
                    return Some(session);
                }
                break;
            }
            _ = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => break,
            event = session.output.recv() => match event {
                Some(BackendEvent::Message(frame)) => {
                    let was_running = !session.background.is_empty();
                    track_background(&mut session.background, &frame);
                    running.store(session.background.len(), Ordering::Relaxed);
                    let busy = !quiet(&BackendEvent::Message(frame.clone()));
                    session.replay.push(frame);
                    if busy {
                        woke.store(true, Ordering::Relaxed);
                        let woke = crate::server::Event::Woke {
                            thread_id: wake.thread_id,
                            key,
                            session,
                        };
                        // A closed event loop drops the session, which kills the process.
                        let _ = wake.events.send(woke).await;
                        return None;
                    }
                    if session.replay.len() > MAX_REPLAY {
                        session.replay.drain(..MAX_REPLAY / 2);
                    }
                    if session.background.is_empty() {
                        if was_running {
                            deadline = timeout.map(|timeout| Instant::now() + timeout.max(ANSWER_GRACE));
                        }
                    } else {
                        deadline = None;
                    }
                }
                _ => break,
            },
        }
    }
    session.close().await;
    None
}

/// Idle Claude only reports status. Anything else means it started work, which
/// needs a turn to observe and approve it.
fn quiet(event: &BackendEvent) -> bool {
    match event {
        BackendEvent::Message(frame) => match frame["type"].as_str() {
            Some("system") => {
                frame["subtype"] != "session_state_changed" || frame["state"] == "idle"
            }
            Some("rate_limit_event") => true,
            // ACP agents announce commands, modes, config, titles and usage while idle.
            Some("acp") => matches!(
                frame["update"]["sessionUpdate"].as_str(),
                Some(
                    "available_commands_update"
                        | "current_mode_update"
                        | "config_option_update"
                        | "session_info_update"
                        | "usage_update"
                )
            ),
            _ => false,
        },
        BackendEvent::Exited { .. } => false,
    }
}

/// Launch settings a later turn must match to reuse the process. None when the
/// launch is turn-specific.
pub fn launch_key(options: &SessionOptions) -> Option<Value> {
    if options.output_schema.is_some() || options.sandbox.is_read_only() {
        return None;
    }
    Some(json!({
        "cwd": options.cwd,
        "ephemeral": options.ephemeral,
        "model": options.model,
        "permissionMode": options.permission_mode,
        "systemPrompt": options.system_prompt,
        "appendSystemPrompt": options.append_system_prompt,
        "effort": options.effort,
        "dynamicTools": options.dynamic_tools,
        "nativeSettings": options.native_settings,
        "sandbox": options.sandbox,
    }))
}

/// Why a parked session cannot serve this launch, or None when it can.
pub fn mismatch(parked: &Parked, options: &SessionOptions, key: Option<&Value>) -> Option<String> {
    if parked.key.is_null() {
        return Some("it was kept only for background work".into());
    }
    if !options.resume || options.fork_from.is_some() || options.resume_at.is_some() {
        return Some("history was rewound or forked".into());
    }
    if options.session_id != parked.session_id {
        return Some("native session changed".into());
    }
    let Some(key) = key else {
        return Some("turn needs its own launch".into());
    };
    let changed: Vec<&str> = parked
        .key
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(name, value)| key.get(name.as_str()) != Some(*value))
        .map(|(name, _)| name.as_str())
        .collect();
    (!changed.is_empty()).then(|| format!("changed {}", changed.join(", ")))
}
