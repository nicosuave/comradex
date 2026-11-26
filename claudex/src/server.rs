use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    backend::{Backend, BackendConfig, BackendEvent, BackendProtocol, SessionOptions},
    protocol::{
        self, ADOPT_THREAD_METHOD, RpcError, RpcResult, SECTION_CHANGED_METHOD, Schemas,
        notification, now_ms, required_str, response, supported_fields,
    },
    store::{ModelDefaults, Record, Settings, Store},
    translate::{self, Translator},
};

/// How long a new Claude process may wait for MCP servers before its first prompt.
const MCP_CONNECT_DEADLINE: Duration = Duration::from_secs(30);
/// Streamed progress is saved this long after it arrives, so a burst of
/// frames becomes one write rather than one per frame.
const PERSIST_DELAY: Duration = Duration::from_millis(250);
// Match native suggestions' cold-prefix budget. Older or evicted parents do
// not start a fresh full-history request merely to fill the composer.
const PREDICTION_COLD_INPUT_LIMIT: u64 = 10_000;
const PREDICTION_CACHE_AGE_MS: u64 = 5 * 60 * 1_000;
const PREDICTION_PROMPT_BYTES: usize = 8 * 1_024;

#[derive(Clone)]
pub struct ServerConfig {
    pub backend: BackendConfig,
    pub default_cwd: PathBuf,
    pub default_model: String,
    pub approval_timeout: Duration,
    /// How long a thread keeps its Claude process between turns; zero disables reuse.
    pub idle_session_timeout: Duration,
    /// Idle Claude processes kept across all threads; zero disables reuse.
    pub max_idle_sessions: usize,
    /// Reviews ACP permission requests when a thread selects automatic review.
    pub reviewer: Option<crate::auto_review::ReviewerConfig>,
}

pub enum Event {
    PluginNotification(Value),
    /// The turn's Claude process is ready to serve the thread's next turn.
    Parked {
        thread_id: String,
        turn_id: String,
        session: crate::warm::WarmSession,
    },
    /// An idle Claude process started work by itself.
    Woke {
        thread_id: String,
        key: Value,
        session: crate::warm::WarmSession,
    },
    PluginsReady {
        thread_id: String,
        turn_id: String,
        runtime: crate::plugin_runtime::Runtime,
    },
    PluginToolFinished {
        thread_id: String,
        turn_id: String,
        item: Value,
    },
    Connected {
        id: u64,
        output: mpsc::Sender<Value>,
        cancel: CancellationToken,
    },
    Input {
        id: u64,
        message: Value,
    },
    ParseError {
        id: u64,
        message: String,
    },
    Disconnected {
        id: u64,
    },
    Backend {
        thread_id: String,
        turn_id: String,
        event: BackendEvent,
    },
    ApprovalTimeout {
        request_id: String,
        generation: u64,
    },
    SessionGrant {
        thread_id: String,
        turn_id: String,
        grant: crate::approvals::SessionGrant,
    },
    AutoReviewFinished {
        review_id: String,
        outcome: crate::auto_review::Outcome,
    },
    /// Save a thread's streamed progress.
    Persist {
        thread_id: String,
    },
    WriteFailed(crate::store::WriteFailure),
    /// The model catalog probe finished with Claude's initialization response.
    ModelCatalog(Result<Value, String>),
    Shutdown,
}

enum Control {
    Reply(Value),
    GrantReply(Value, crate::approvals::SessionGrant),
    UserInput(Value),
    Interrupt,
}

#[derive(Default)]
struct InputAdmission {
    queued: usize,
    closed: bool,
}

impl InputAdmission {
    fn close_if_drained(&mut self) -> bool {
        if self.queued != 0 {
            return false;
        }
        self.closed = true;
        true
    }
}

struct Connection {
    output: mpsc::Sender<Value>,
    cancel: CancellationToken,
    initialized: bool,
    ready: bool,
    experimental: bool,
    originator: String,
    opt_out: HashSet<String>,
    subscriptions: HashSet<String>,
}

struct ActiveTurn {
    /// Set when the process may serve later turns with the same launch settings.
    launch_key: Option<Value>,
    usage: crate::usage::Tracker,
    plugins: Option<crate::plugin_runtime::Runtime>,
    plugin_calls: HashMap<String, Value>,
    id: String,
    owner: Option<u64>,
    started_ms: u64,
    control: mpsc::Sender<Control>,
    input_admission: Arc<Mutex<InputAdmission>>,
    translator: Translator,
    agents: crate::subagents::Agents,
    breaker: crate::auto_review::Breaker,
    interrupting: bool,
    /// Whether this turn asked Claude for fast mode.
    fast_mode: bool,
    /// Whether this turn runs a manual compaction, which cannot be steered.
    compaction: bool,
    prediction_parent: Option<String>,
    worker: tokio::task::JoinHandle<()>,
    cancel: CancellationToken,
}

/// Running `thread/sample` requests by (client, request id), with their
/// source thread.
type Samples = Arc<Mutex<HashMap<(u64, String), (String, CancellationToken)>>>;

struct Pending {
    owner: Option<u64>,
    request: Value,
    timeout: Option<PendingTimeout>,
    thread_id: String,
    turn_id: String,
    claude_request_id: String,
    input: Value,
    questions: Option<crate::approvals::QuestionFlow>,
    tool: String,
    generic_tool: bool,
    dynamic: Option<DynamicPending>,
}

/// Only connected time counts toward the human response deadline. A transport
/// outage must not deny a permission or answer a question on the user's behalf.
struct PendingTimeout {
    remaining: Duration,
    running: Option<(tokio::time::Instant, CancellationToken)>,
    generation: u64,
}

impl PendingTimeout {
    fn pause(&mut self) {
        if let Some((started, cancel)) = self.running.take() {
            self.remaining = self.remaining.saturating_sub(started.elapsed());
            cancel.cancel();
        }
        self.generation += 1;
    }
}

impl Drop for PendingTimeout {
    fn drop(&mut self) {
        if let Some((_, cancel)) = &self.running {
            cancel.cancel();
        }
    }
}

/// A permission request awaiting its automatic review.
struct Review {
    pending: Pending,
    action: Value,
    target: Value,
    started_ms: u64,
}

struct DynamicPending {
    message_id: Value,
    item: Value,
    started_ms: u64,
}

pub struct Server {
    plugin_manager: Arc<crate::plugin_runtime::Manager>,
    config: ServerConfig,
    defaults: ModelDefaults,
    workspace_defaults: crate::sandbox::WorkspaceDefaults,
    model_catalog: Option<Vec<Value>>,
    /// ACP agent info and capabilities from its most recent initialization.
    acp_agent: Option<Value>,
    store: Store,
    records: BTreeMap<String, Record>,
    loaded: HashSet<String>,
    connections: HashMap<u64, Connection>,
    active: HashMap<String, ActiveTurn>,
    idle_sessions: HashMap<String, crate::warm::Parked>,
    pending: HashMap<String, Pending>,
    /// Permission requests under automatic review, by review id.
    reviews: HashMap<String, Review>,
    commands: crate::commands::CommandManager,
    command_cleanup: tokio::task::JoinSet<()>,
    /// Threads whose streamed progress has not been saved yet.
    unsaved: HashSet<String>,
    write_failures: Option<mpsc::UnboundedReceiver<crate::store::WriteFailure>>,
    /// Claude processes still closing, by thread.
    closing: HashMap<String, tokio::task::JoinHandle<()>>,
    /// model/list requests waiting for the catalog probe.
    catalog_waiters: Vec<(u64, Value, Value)>,
    events: mpsc::Sender<Event>,
    /// Running `thread/sample` requests. A request is answered by whoever
    /// removes its entry.
    samples: Samples,
}

impl Server {
    pub fn new(
        config: ServerConfig,
        mut store: Store,
        events: mpsc::Sender<Event>,
    ) -> Result<Self> {
        let write_failures = store.take_write_failures();
        let records = store.load()?;
        let defaults = store.load_defaults(config.default_model.clone())?;
        let workspace_defaults = crate::sandbox::WorkspaceDefaults::load(store.root())?;
        // Compile the supported wire contracts once, before accepting connections.
        Schemas::get();
        Ok(Self {
            plugin_manager: Arc::default(),
            config,
            defaults,
            workspace_defaults,
            model_catalog: None,
            acp_agent: None,
            store,
            records,
            loaded: HashSet::new(),
            connections: HashMap::new(),
            active: HashMap::new(),
            idle_sessions: HashMap::new(),
            pending: HashMap::new(),
            reviews: HashMap::new(),
            commands: crate::commands::CommandManager::default(),
            command_cleanup: tokio::task::JoinSet::new(),
            unsaved: HashSet::new(),
            write_failures,
            closing: HashMap::new(),
            catalog_waiters: Vec::new(),
            events,
            samples: Arc::default(),
        })
    }

    pub async fn run(mut self, mut events: mpsc::Receiver<Event>) {
        let mut write_failures = self.write_failures.take();
        loop {
            let event = tokio::select! {
                event = events.recv() => match event {
                    Some(event) => event,
                    None => break,
                },
                Some(failure) = async {
                    match &mut write_failures {
                        Some(failures) => failures.recv().await,
                        None => std::future::pending().await,
                    }
                } => Event::WriteFailed(failure),
            };
            match event {
                Event::PluginsReady {
                    thread_id,
                    turn_id,
                    runtime,
                } => {
                    if let Some(active) = self
                        .active
                        .get_mut(&thread_id)
                        .filter(|active| active.id == turn_id)
                    {
                        active.plugins = Some(runtime);
                    } else {
                        tokio::spawn(async move { runtime.client.shutdown().await });
                    }
                }
                Event::Parked {
                    thread_id,
                    turn_id,
                    session,
                } => {
                    let key =
                        self.active
                            .get_mut(&thread_id)
                            .filter(|active| active.id == turn_id)
                            .and_then(|active| {
                                // The parked session owns the plugin runtime from now on.
                                active.plugins = None;
                                // A null key never serves another turn.
                                active.launch_key.clone().or_else(|| {
                                    (!session.background.is_empty()).then_some(Value::Null)
                                })
                            });
                    match key {
                        Some(key) => self.park(thread_id, key, session),
                        None => self.close_in_background(&thread_id, session.close()),
                    }
                }
                Event::Woke {
                    thread_id,
                    key,
                    session,
                } => self.wake(thread_id, key, session),
                Event::PluginToolFinished {
                    thread_id,
                    turn_id,
                    item,
                } => {
                    if let Some(active) = self
                        .active
                        .get_mut(&thread_id)
                        .filter(|active| active.id == turn_id)
                        && active
                            .plugin_calls
                            .remove(item["id"].as_str().unwrap_or(""))
                            .is_some()
                    {
                        self.translated(&thread_id,vec![notification("item/completed",json!({"threadId":thread_id,"turnId":turn_id,"item":item,"completedAtMs":now_ms()}))]);
                    }
                }
                Event::PluginNotification(message) => {
                    for (&client, connection) in &self.connections {
                        if connection.ready {
                            self.send(client, message.clone());
                        }
                    }
                }
                Event::Connected { id, output, cancel } => {
                    self.connections.insert(
                        id,
                        Connection {
                            output,
                            cancel,
                            initialized: false,
                            ready: false,
                            experimental: false,
                            originator: String::new(),
                            opt_out: HashSet::new(),
                            subscriptions: HashSet::new(),
                        },
                    );
                }
                Event::Input { id, message } => self.input(id, message).await,
                Event::ParseError { id, message } => {
                    self.send(id, RpcError::new(-32700, message).response(Value::Null))
                }
                Event::Disconnected { id } => self.disconnect(id).await,
                Event::Backend {
                    thread_id,
                    turn_id,
                    event,
                } => self.backend_event(&thread_id, &turn_id, event).await,
                Event::ApprovalTimeout {
                    request_id,
                    generation,
                } => {
                    let expires = self.pending.get(&request_id).is_some_and(|p| {
                        p.timeout
                            .as_ref()
                            .is_some_and(|t| t.generation == generation && t.running.is_some())
                            && p.owner.is_some_and(|owner| self.connected(owner))
                    });
                    if expires && let Some(pending) = self.pending.remove(&request_id) {
                        self.deny_pending(&request_id, pending, "Approval request timed out")
                            .await;
                    }
                }
                Event::SessionGrant {
                    thread_id,
                    turn_id,
                    grant,
                } => {
                    if self
                        .active
                        .get(&thread_id)
                        .is_some_and(|active| active.id == turn_id)
                    {
                        let mut record = self.records[&thread_id].clone();
                        if !record.session_grants.contains(&grant) {
                            record.session_grants.push(grant);
                            match self.store.save(&record) {
                                Ok(()) => { self.records.insert(thread_id, record); }
                                Err(error) => self.emit(&thread_id, notification("warning", json!({"threadId":thread_id,"message":format!("Session approval was not retained: {error}")}))),
                            }
                        }
                    }
                }
                Event::AutoReviewFinished { review_id, outcome } => {
                    self.finish_review(&review_id, outcome).await
                }
                Event::Persist { thread_id } => {
                    if self.unsaved.remove(&thread_id)
                        && let Some(record) = self.records.get(&thread_id)
                        && let Err(error) = self.store.save_deferred(record)
                    {
                        self.write_failed(&thread_id, &format!("{error:#}"));
                    }
                }
                Event::WriteFailed(failure) => {
                    self.write_failed(&failure.thread_id, &failure.error)
                }
                Event::ModelCatalog(result) => self.catalog_ready(result),
                Event::Shutdown => break,
            }
        }
        let ids: Vec<String> = self.active.keys().cloned().collect();
        for id in ids {
            self.finish(&id, "interrupted", Some("Facade is shutting down".into()))
                .await;
        }
        for (_, parked) in self.idle_sessions.drain() {
            parked.close().await;
        }
        for (_, closing) in self.closing.drain() {
            let _ = closing.await;
        }
        for id in std::mem::take(&mut self.unsaved) {
            if let Some(record) = self.records.get(&id)
                && let Err(error) = self.store.save(record)
            {
                eprintln!("Cannot save thread {id}: {error:#}");
            }
        }
        for (&client, connection) in &self.connections {
            connection.cancel.cancel();
            self.commands.disconnect(client).await;
        }
        while self.command_cleanup.join_next().await.is_some() {}
        self.plugin_manager.shutdown().await;
    }

    fn send(&self, client: u64, message: Value) {
        if let Some(connection) = self.connections.get(&client) {
            // A stalled peer must not block other threads or consume unlimited memory.
            if connection.output.try_send(message).is_err() {
                connection.cancel.cancel();
            }
        }
    }
    fn emit(&self, thread_id: &str, message: Value) {
        let method = message["method"].as_str().unwrap_or("");
        for connection in self.connections.values() {
            if connection.ready
                && connection.subscriptions.contains(thread_id)
                && !connection.opt_out.contains(method)
                && connection.output.try_send(message.clone()).is_err()
            {
                connection.cancel.cancel();
            }
        }
    }
    fn subscribe(&mut self, client: u64, thread_id: &str) {
        if let Some(connection) = self.connections.get_mut(&client) {
            connection.subscriptions.insert(thread_id.to_owned());
        }
        self.loaded.insert(thread_id.to_owned());
    }

    fn plugin_service(&self, cwd: Option<&str>) -> crate::plugins::PluginService {
        crate::plugins::PluginService::new(
            self.config.backend.executable.clone(),
            None,
            cwd.map(PathBuf::from)
                .unwrap_or_else(|| self.config.default_cwd.clone()),
        )
    }

    fn connected(&self, client: u64) -> bool {
        self.connections.get(&client).is_some_and(|connection| {
            connection.ready && !connection.cancel.is_cancelled() && !connection.output.is_closed()
        })
    }

    /// A live owner cannot be replaced by another observer. Once it disconnects,
    /// explicit resume (or a delayed response with the original request ID) can
    /// recover ownership on the replacement transport.
    fn claim_detached_turn(&mut self, client: u64, id: &str) -> bool {
        if !self.connected(client) {
            return false;
        }
        let Some(active) = self.active.get(id) else {
            return false;
        };
        if let Some(owner) = active.owner
            && self.connected(owner)
        {
            return owner == client;
        }
        self.active.get_mut(id).unwrap().owner = Some(client);
        for pending in self.pending.values_mut().filter(|p| p.thread_id == id) {
            if let Some(timeout) = &mut pending.timeout {
                timeout.pause();
            }
            pending.owner = Some(client);
        }
        self.subscribe(client, id);
        true
    }

    fn deliver_pending(&mut self, request_id: &str) {
        let Some(owner) = self.pending.get(request_id).and_then(|p| p.owner) else {
            return;
        };
        if !self.connected(owner) {
            return;
        }
        let pending = self.pending.get_mut(request_id).unwrap();
        let request = pending.request.clone();
        if let Some(timeout) = &mut pending.timeout
            && timeout.running.is_none()
        {
            let cancel = CancellationToken::new();
            timeout.running = Some((tokio::time::Instant::now(), cancel.clone()));
            let generation = timeout.generation;
            let duration = timeout.remaining;
            let events = self.events.clone();
            let request_id = request_id.to_owned();
            tokio::spawn(async move {
                tokio::select! {
                    _ = cancel.cancelled() => {},
                    _ = tokio::time::sleep(duration) => {
                        let _ = events.send(Event::ApprovalTimeout { request_id, generation }).await;
                    }
                }
            });
        }
        self.send(owner, request);
    }

    fn replay_pending(&mut self, client: u64, id: &str) {
        let mut requests: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, p)| p.thread_id == id && p.owner == Some(client))
            .map(|(request_id, _)| request_id.clone())
            .collect();
        requests.sort();
        for request_id in requests {
            self.deliver_pending(&request_id);
        }
    }
    fn record(&self, id: &str) -> RpcResult<&Record> {
        self.records
            .get(id)
            .ok_or_else(|| RpcError::invalid(format!("Thread not found: {id}")))
    }
    fn idle(&self, id: &str) -> RpcResult<()> {
        self.record(id)?;
        if self.active.contains_key(id) {
            return Err(RpcError::invalid("Thread already has an active turn"));
        }
        // Its turn starts as soon as the server handles the wake.
        if self
            .idle_sessions
            .get(id)
            .is_some_and(|parked| parked.woke())
        {
            return Err(RpcError::invalid(
                "Claude is starting a turn for finished background work",
            ));
        }
        Ok(())
    }
    fn park(&mut self, id: String, key: Value, session: crate::warm::WarmSession) {
        self.idle_sessions.retain(|_, parked| parked.is_open());
        let ephemeral = self.records[&id].ephemeral();
        // Processes running background work are not idle caches; evicting one
        // would kill that work.
        // Ephemeral chats need this process for their history, so they are not
        // expendable cache entries. Closing the chat releases it.
        let idle = |parked: &crate::warm::Parked| {
            parked.running_background() == 0 && parked.key["ephemeral"] != true
        };
        while !ephemeral
            && self.idle_sessions.values().filter(|p| idle(p)).count()
                >= self.config.max_idle_sessions
        {
            let Some(oldest) = self
                .idle_sessions
                .iter()
                .filter(|(_, parked)| idle(parked))
                .min_by_key(|(_, parked)| parked.parked_at)
                .map(|(id, _)| id.clone())
            else {
                break;
            };
            let parked = self.idle_sessions.remove(&oldest).unwrap();
            self.close_in_background(&oldest, parked.close());
        }
        let session_id = self.records[&id].session_id.clone();
        let wake = crate::warm::Wake {
            thread_id: id.clone(),
            events: self.events.clone(),
        };
        let parked = crate::warm::Parked::new(
            key,
            session_id,
            session,
            (!ephemeral).then_some(self.config.idle_session_timeout),
            wake,
        );
        self.idle_sessions.insert(id, parked);
    }

    /// The thread's idle Claude process, if it can serve this launch. Any other
    /// idle process is closed first: two processes must never share a session.
    async fn reclaim(
        &mut self,
        id: &str,
        options: &SessionOptions,
        key: Option<&Value>,
    ) -> Option<crate::warm::WarmSession> {
        let parked = self.idle_sessions.remove(id)?;
        match crate::warm::mismatch(&parked, options, key) {
            None => parked.take().await,
            Some(reason) => {
                eprintln!("Restarting Claude for thread {id}: {reason}");
                self.warn_stopped_background(id, &parked, "This turn needs a new Claude process");
                self.close_in_background(id, parked.close());
                None
            }
        }
    }

    /// Read the model catalog from a short-lived Claude process.
    fn probe_models(&self) {
        let options = SessionOptions {
            state_dir: self.store.root().to_path_buf(),
            sandbox: Default::default(),
            cwd: self.config.default_cwd.clone(),
            session_id: protocol::id(),
            resume: false,
            fork_from: None,
            resume_at: None,
            // An ACP probe keeps the agent default, so an unavailable
            // configured default cannot hide the catalog.
            model: if self.acp() {
                crate::acp::DEFAULT_MODEL.into()
            } else {
                self.defaults.model.clone()
            },
            permission_mode: "dontAsk".into(),
            system_prompt: None,
            append_system_prompt: None,
            effort: None,
            output_schema: None,
            ephemeral: true,
            dynamic_tools: vec![],
            native_settings: json!({}),
        };
        let config = self.config.backend.clone();
        let events = self.events.clone();
        tokio::spawn(async move {
            let initialization = async {
                let (mut backend, _events) = Backend::spawn(&config, &options).await?;
                let initialization = backend.initialization().clone();
                backend.terminate().await?;
                anyhow::Ok(initialization)
            }
            .await
            .map_err(|error| format!("{error:#}"));
            let _ = events.send(Event::ModelCatalog(initialization)).await;
        });
    }

    fn catalog_ready(&mut self, initialization: Result<Value, String>) {
        let catalog = initialization
            .map_err(RpcError::internal)
            .and_then(|initialization| {
                if self.acp() {
                    self.acp_agent = Some(initialization["agent"].clone());
                }
                // An agent without model selection still serves its own default.
                initialization["models"]
                    .as_array()
                    .cloned()
                    .filter(|models| self.acp() || !models.is_empty())
                    .ok_or_else(|| RpcError::internal("Claude returned no model catalog"))
            });
        if let Ok(models) = &catalog {
            self.model_catalog = Some(models.clone());
        }
        // A failed probe answers its waiters; the next request probes again.
        for (client, rpc_id, params) in std::mem::take(&mut self.catalog_waiters) {
            let page = match &catalog {
                Ok(_) => self.model_page(&params),
                Err(error) => Err(error.clone()),
            };
            self.send(
                client,
                match page {
                    Ok(page) => response(rpc_id, page),
                    Err(error) => error.response(rpc_id),
                },
            );
        }
    }

    fn model_page(&self, p: &Value) -> RpcResult<Value> {
        let mut models = self
            .model_catalog
            .as_ref()
            .unwrap()
            .iter()
            .map(|model| protocol::claude_model_entry(model, &self.defaults.model))
            .collect::<RpcResult<Vec<_>>>()?;
        if !models.iter().any(|m| m["isDefault"] == true) {
            let mut entry = protocol::model_entry(&self.defaults.model, true);
            if self.acp() && self.defaults.model == crate::acp::DEFAULT_MODEL {
                entry["displayName"] = json!("Agent default");
                entry["description"] = json!("The model the agent selects by default");
            }
            models.insert(0, entry);
        }
        if let Some(agent) = self.acp_agent.as_ref().filter(|_| self.acp())
            && agent["agentCapabilities"]["promptCapabilities"]["image"] != true
        {
            for model in &mut models {
                model["inputModalities"] = json!(["text"]);
            }
        }
        let mut page = page(models, p, false, "models")?;
        page.as_object_mut().unwrap().remove("backwardsCursor");
        Ok(page)
    }

    /// Close a thread's Claude process without holding up other threads. The
    /// thread's next process waits for it: two processes must never share a session.
    fn close_in_background(&mut self, id: &str, close: impl Future<Output = ()> + Send + 'static) {
        self.closing.retain(|_, task| !task.is_finished());
        let previous = self.closing.remove(id);
        let task = tokio::spawn(async move {
            if let Some(previous) = previous {
                let _ = previous.await;
            }
            close.await;
        });
        self.closing.insert(id.to_owned(), task);
    }

    /// Save a thread's streamed progress shortly, coalescing a burst of frames.
    fn persist_soon(&mut self, id: &str) {
        if self.unsaved.insert(id.to_owned()) {
            let events = self.events.clone();
            let thread_id = id.to_owned();
            tokio::spawn(async move {
                tokio::time::sleep(PERSIST_DELAY).await;
                let _ = events.send(Event::Persist { thread_id }).await;
            });
        }
    }

    /// Queue a save of the thread's current state right away.
    fn persist_now(&mut self, id: &str) {
        self.unsaved.remove(id);
        if let Err(error) = self.store.save_deferred(&self.records[id]) {
            self.write_failed(id, &format!("{error:#}"));
        }
    }

    /// A deferred save failed. The turn continues; its completion is saved
    /// synchronously and fails the turn if the state still cannot be written.
    fn write_failed(&self, id: &str, error: &str) {
        if self.records.contains_key(id) {
            let message = format!("Thread state was not saved: {error}");
            self.emit(
                id,
                notification("warning", json!({"threadId": id, "message": message})),
            );
        }
    }

    fn session_options(
        &self,
        record: &Record,
        output_schema: Option<Value>,
        fast_mode: bool,
    ) -> RpcResult<SessionOptions> {
        record.validate_read_only_helper()?;
        if record.settings.sandbox.is_read_only() && record.has_session {
            return Err(RpcError::invalid(
                "Read-only Claude helpers support one turn; start a fresh ephemeral helper",
            ));
        }
        Ok(SessionOptions {
            state_dir: self.store.root().to_path_buf(),
            sandbox: record.settings.sandbox.effective(&record.settings.cwd),
            cwd: record.settings.cwd.clone(),
            session_id: record.session_id.clone(),
            resume: record.has_session,
            fork_from: if record.has_session {
                None
            } else {
                record.fork_from.clone()
            },
            resume_at: if record.has_session || record.fork_from.is_none() {
                None
            } else {
                record.backend_message_id.clone()
            },
            model: record.settings.model.clone(),
            permission_mode: record.settings.native_permission_mode().into(),
            system_prompt: record.settings.base_instructions.clone(),
            append_system_prompt: record.settings.developer_instructions.clone(),
            effort: record.settings.effort.clone(),
            output_schema,
            ephemeral: record.ephemeral(),
            native_settings: {
                let mut settings =
                    crate::plugins::disabled_settings(&record.settings.disabled_plugin_ids)?;
                settings["env"] = json!(record.settings.environment_overrides);
                // Claude honors fast mode from SDK clients only as a flag setting.
                if fast_mode {
                    settings["fastMode"] = json!(true);
                }
                settings
            },
            dynamic_tools: record
                .settings
                .dynamic_tools
                .iter()
                .map(|t| t.mcp_spec())
                .collect(),
        })
    }

    fn prediction_client(&self, record: &Record) -> Option<crate::backend::PredictionClient> {
        let parent_id = record.thread["forkedFromId"].as_str()?;
        let parent = self.records.get(parent_id)?;
        if record.fork_from.as_deref() != Some(parent.session_id.as_str())
            || record.backend_message_id != parent.backend_message_id
            || !changed_settings(&record.settings, &parent.settings)
                .ok()?
                .is_empty()
        {
            return None;
        }
        self.warm_question_client(parent)
    }

    /// A read-only question client on the parent's parked process, only while
    /// its cached prefix is warm and small enough to reuse.
    fn warm_question_client(&self, parent: &Record) -> Option<crate::backend::PredictionClient> {
        let parent_id = parent.id();
        if parent.archived || self.active.contains_key(parent_id) {
            return None;
        }
        // Cache TTL starts when the API request starts, not when output ends.
        let started = parent.turns.last()?["startedAt"]
            .as_u64()?
            .saturating_mul(1_000);
        if now_ms().saturating_sub(started) >= PREDICTION_CACHE_AGE_MS {
            return None;
        }
        let usage = &parent.token_usage["last"];
        let input = usage["inputTokens"].as_u64()?;
        let cached = usage["cachedInputTokens"].as_u64()?;
        if input == 0 || input.saturating_sub(cached) > PREDICTION_COLD_INPUT_LIMIT {
            return None;
        }
        let parked = self.idle_sessions.get(parent_id)?;
        if !parked.is_open() || parked.woke() || parked.running_background() != 0 {
            return None;
        }
        let options = self
            .session_options(parent, None, parent.settings.service_tier.is_some())
            .ok()?;
        let key = crate::warm::launch_key(&options)?;
        if crate::warm::mismatch(parked, &options, Some(&key)).is_some() {
            return None;
        }
        parked.prediction.clone()
    }

    fn cancel_predictions(&mut self, parent: &str) {
        for (thread, cancel) in self.samples.lock().unwrap().values() {
            if thread == parent {
                cancel.cancel();
            }
        }
        for active in self
            .active
            .values_mut()
            .filter(|active| active.prediction_parent.as_deref() == Some(parent))
        {
            active.interrupting = true;
            let _ = active.control.try_send(Control::Interrupt);
        }
    }

    /// Start the backend worker for a turn already recorded on the thread.
    fn spawn_turn(&mut self, record: &Record, turn: LaunchedTurn) {
        let id = record.id().to_owned();
        if !record.prediction_mode {
            // Includes turns Claude starts for completed background work.
            self.cancel_predictions(&id);
        }
        let (control, commands) = mpsc::channel(32);
        let input_admission = Arc::new(Mutex::new(InputAdmission::default()));
        let cancel = CancellationToken::new();
        let prediction_parent = record.thread["forkedFromId"]
            .as_str()
            .filter(|_| record.prediction_mode)
            .map(str::to_owned);
        let worker = if record.prediction_mode {
            tokio::spawn(run_prediction(PredictionTurn {
                client: self.prediction_client(record),
                output_schema: turn.output_schema,
                thread_id: id.clone(),
                turn_id: turn.turn_id.clone(),
                session_id: record.session_id.clone(),
                model: record.settings.model.clone(),
                input: turn.input,
                commands,
                events: self.events.clone(),
                cancel: cancel.clone(),
            }))
        } else {
            tokio::spawn(run_backend(BackendTurn {
                previous_process: self.closing.remove(&id),
                config: self.config.backend.clone(),
                plugins: (!record.settings.sandbox.is_read_only())
                    .then(|| crate::plugin_runtime::configured(record.settings.cwd.clone()))
                    .flatten()
                    .map(|config| {
                        (
                            config,
                            record.settings.disabled_plugin_ids.clone(),
                            record.settings.dynamic_tools.clone(),
                        )
                    }),
                options: turn.options,
                warm: turn.warm,
                park: turn.launch_key.is_some(),
                thread_id: id.clone(),
                turn_id: turn.turn_id.clone(),
                input: turn.input,
                commands,
                input_admission: input_admission.clone(),
                events: self.events.clone(),
                cancel: cancel.clone(),
            }))
        };
        self.active.insert(
            id.clone(),
            ActiveTurn {
                launch_key: turn.launch_key,
                usage: crate::usage::Tracker::default(),
                plugins: None,
                plugin_calls: HashMap::new(),
                id: turn.turn_id.clone(),
                owner: turn.owner,
                started_ms: now_ms(),
                control,
                input_admission,
                translator: Translator::new(&id, &turn.turn_id, &record.settings.cwd)
                    .with_reasoning(record.settings.reasoning_summary.as_deref() != Some("none")),
                agents: Default::default(),
                breaker: Default::default(),
                interrupting: false,
                fast_mode: turn.fast_mode,
                compaction: turn.compaction,
                prediction_parent,
                worker,
                cancel,
            },
        );
    }

    /// Claude started work in an idle process, for example to answer a
    /// background task that finished after its turn. Codex has no equivalent,
    /// so the work becomes a new turn on the thread instead of being discarded.
    fn wake(&mut self, id: String, key: Value, session: crate::warm::WarmSession) {
        self.idle_sessions.retain(|_, parked| parked.is_open());
        let available = self
            .records
            .get(&id)
            .is_some_and(|record| !record.archived && !self.active.contains_key(&id));
        if !available {
            self.close_in_background(&id, session.close());
            return;
        }
        let mut record = self.records[&id].clone();
        // The process keeps its own launch settings, including fast mode.
        let fast_mode = key["nativeSettings"]["fastMode"] == true;
        let options = match self.session_options(&record, None, fast_mode) {
            Ok(options) => options,
            Err(error) => {
                eprintln!("Cannot resume Claude's background work for thread {id}: {error}");
                self.close_in_background(&id, session.close());
                return;
            }
        };
        let turn_id = protocol::id();
        let turn = protocol::new_turn(&turn_id, None);
        record.turns.push(turn.clone());
        record.thread["status"] = json!({"type": "active", "activeFlags": []});
        record.touch();
        if let Err(error) = self.store.save(&record) {
            eprintln!("Cannot record Claude's background work for thread {id}: {error}");
            self.close_in_background(&id, session.close());
            return;
        }
        // Approvals go to a client showing the thread; otherwise the turn is
        // detached until a client resumes it.
        let owner = self
            .connections
            .iter()
            .filter(|(_, connection)| connection.ready && connection.subscriptions.contains(&id))
            .map(|(client, _)| *client)
            .min();
        self.spawn_turn(
            &record,
            LaunchedTurn {
                turn_id,
                output_schema: None,
                options,
                warm: Some(session),
                launch_key: Some(key),
                input: vec![],
                owner,
                fast_mode,
                compaction: false,
            },
        );
        self.records.insert(id.clone(), record);
        self.emit(
            &id,
            notification(
                "thread/status/changed",
                json!({"threadId": id, "status": {"type": "active", "activeFlags": []}}),
            ),
        );
        self.emit(
            &id,
            notification("turn/started", json!({"threadId": id, "turn": turn})),
        );
    }

    fn close_idle_session(&mut self, id: &str) {
        if let Some(parked) = self.idle_sessions.remove(id) {
            self.warn_stopped_background(id, &parked, "Closing this chat's Claude process");
            self.close_in_background(id, parked.close());
        }
    }

    /// Closing a process kills its background work, and with it any later answer.
    fn warn_stopped_background(&self, id: &str, parked: &crate::warm::Parked, cause: &str) {
        let running = parked.running_background();
        if running > 0 {
            let message = format!(
                "{cause} stopped {running} background task(s) that were still running; their results will not be delivered."
            );
            self.emit(
                id,
                notification("warning", json!({"threadId": id, "message": message})),
            );
        }
    }

    fn acp(&self) -> bool {
        self.config.backend.protocol == BackendProtocol::Acp
    }

    /// Settings an ACP agent cannot honor. Its tools run in its own process, so
    /// the facade cannot confine them or delegate review to Claude's classifier.
    fn check_backend_settings(&self, settings: &Settings) -> RpcResult<()> {
        if !self.acp() {
            return Ok(());
        }
        if settings.sandbox.is_read_only() {
            return Err(RpcError::invalid(
                "Read-only helpers require the native Claude backend",
            ));
        }
        if settings.sandbox.is_workspace() {
            return Err(RpcError::invalid(
                "ACP agents run their own tools, which this facade cannot confine to a workspace sandbox; use danger-full-access",
            ));
        }
        if settings.service_tier.is_some() {
            return Err(acp_fast_mode());
        }
        if settings.approvals_reviewer != "user" && self.config.reviewer.is_none() {
            return Err(RpcError::invalid(
                "Automatic approval review needs a Claude reviewer; start the facade with --claude",
            ));
        }
        Ok(())
    }

    /// Whether the cached ACP agent capabilities rule out a session operation.
    /// Unknown capabilities (no turn has run yet) defer the check to the agent.
    fn acp_lacks(&self, capability: &str) -> bool {
        self.acp_agent.as_ref().is_some_and(|agent| {
            !agent["agentCapabilities"]["sessionCapabilities"][capability].is_object()
        })
    }

    async fn input(&mut self, client: u64, message: Value) {
        if !self.connections.contains_key(&client) {
            return;
        }
        let Some(object) = message.as_object() else {
            self.send(
                client,
                RpcError::new(
                    -32600,
                    "Expected a JSON-RPC object; batches are unsupported",
                )
                .response(Value::Null),
            );
            return;
        };
        let id = object.get("id").cloned();
        if object.get("jsonrpc").is_some_and(|v| v != "2.0")
            || id
                .as_ref()
                .is_some_and(|v| !(v.is_string() || v.is_i64() || v.is_u64()))
        {
            self.send(
                client,
                RpcError::new(-32600, "Invalid JSON-RPC envelope").response(Value::Null),
            );
            return;
        }
        let Some(method) = message["method"].as_str() else {
            if let Some(id) = id {
                if message.get("result").is_some() ^ message.get("error").is_some() {
                    self.approval_response(client, &id, &message).await;
                } else {
                    self.send(
                        client,
                        RpcError::new(-32600, "Invalid response envelope").response(id),
                    );
                }
            } else {
                self.send(
                    client,
                    RpcError::new(-32600, "Missing method").response(Value::Null),
                );
            }
            return;
        };
        if id.is_none() {
            if method == "initialized" {
                let connection = self.connections.get_mut(&client).unwrap();
                if connection.initialized {
                    connection.ready = true;
                }
            }
            return;
        }
        let id = id.unwrap();
        let result = if method != "initialize" && !self.connections[&client].ready {
            Err(RpcError::new(-32000, "Not initialized"))
        } else if method == "initialize" && self.connections[&client].initialized {
            Err(RpcError::new(-32600, "Already initialized"))
        } else if method == ADOPT_THREAD_METHOD {
            self.adopt_thread(client, &id, &message["params"]).await
        } else if method == "claudex/delegation/attach" {
            self.attach_delegation_tools(&message["params"])
        } else if method == SECTION_CHANGED_METHOD {
            self.section_changed(&message["params"])
        } else {
            match Schemas::get().validate(method, &message) {
                Ok(()) => {
                    self.request(client, &id, method, &message["params"], None)
                        .await
                }
                Err(error) => Err(error),
            }
        };
        // Lifecycle handlers send their reply before scheduling notifications.
        match result {
            Ok(Some(result)) => self.send(client, response(id, result)),
            Ok(None) => {}
            Err(error) => self.send(client, error.response(id)),
        }
    }

    /// The local router merges its delegation catalog into an older saved chat.
    /// No caller-supplied descriptors or permission changes are accepted here.
    fn attach_delegation_tools(&mut self, params: &Value) -> RpcResult<Option<Value>> {
        supported_fields(params, &["threadId"])?;
        let id = required_str(params, "threadId")?;
        #[cfg(unix)]
        {
            let mut record = self.record(id)?.clone();
            if record.settings.sandbox.is_read_only() {
                return Err(RpcError::invalid(
                    "Read-only helpers cannot attach delegation tools",
                ));
            }
            let expected =
                crate::dynamic_tools::parse(&json!([crate::delegation::dynamic_spec()]))?;
            let current: Vec<_> = record
                .settings
                .dynamic_tools
                .iter()
                .filter(|tool| tool.namespace.as_deref() == Some(crate::delegation::NAMESPACE))
                .collect();
            if !current.is_empty() {
                if serde_json::to_value(current).map_err(RpcError::internal)? != json!(expected) {
                    return Err(RpcError::invalid(
                        "The claudex_agents namespace is reserved by the router",
                    ));
                }
                return Ok(Some(json!({})));
            }
            if self.active.contains_key(id) {
                return Err(RpcError::invalid(
                    "Delegation tools can only be attached between turns",
                ));
            }
            record.settings.dynamic_tools.extend(expected);
            self.store.save(&record).map_err(RpcError::internal)?;
            self.records.insert(id.to_owned(), record);
            Ok(Some(json!({})))
        }
        #[cfg(not(unix))]
        {
            let _ = id;
            Err(RpcError::unsupported("claudex/delegation/attach"))
        }
    }

    /// Start a Claude thread under an identifier the client already holds. The
    /// local router uses this when the desktop's prewarmed (still empty) Codex
    /// thread receives its first turn for a Claude model.
    async fn adopt_thread(
        &mut self,
        client: u64,
        rpc_id: &Value,
        p: &Value,
    ) -> RpcResult<Option<Value>> {
        supported_fields(p, &["threadId", "params", "metadata"])?;
        let thread_id = required_str(p, "threadId")?;
        if uuid::Uuid::parse_str(thread_id).is_err() {
            return Err(RpcError::invalid("threadId must be a UUID"));
        }
        if self.records.contains_key(thread_id) {
            return Err(RpcError::invalid("Thread already exists"));
        }
        let params = p.get("params").cloned().unwrap_or_else(|| json!({}));
        let start = json!({"id": rpc_id, "method": "thread/start", "params": params});
        Schemas::get().validate("thread/start", &start)?;
        let metadata = p
            .get("metadata")
            .filter(|v| !v.is_null())
            .cloned()
            .unwrap_or_else(|| json!({}));
        supported_fields(&metadata, &["projectId", "gitInfo", "daybreakEnabled"])?;
        let mut metadata_params = metadata.clone();
        metadata_params["threadId"] = json!(thread_id);
        Schemas::get().validate(
            "thread/metadata/update",
            &json!({"id":rpc_id,"method":"thread/metadata/update","params":metadata_params}),
        )?;
        self.request(
            client,
            rpc_id,
            "thread/start",
            &params,
            Some((thread_id, &metadata)),
        )
        .await
    }

    async fn request(
        &mut self,
        client: u64,
        rpc_id: &Value,
        method: &str,
        p: &Value,
        adopted_id: Option<(&str, &Value)>,
    ) -> RpcResult<Option<Value>> {
        let (mut params, warnings) = crate::desktop::normalize(method, p).inspect_err(|error| {
            eprintln!(
                "Rejected desktop {method} for thread {}: {}",
                p["threadId"], error.message
            )
        })?;
        // Named workspace presets use the host defaults. Never override an
        // explicit sandboxPolicy, including an explicit networkAccess=false.
        if matches!(
            method,
            "thread/start" | "thread/resume" | "thread/fork" | "turn/start"
        ) && (p["permissions"] == ":workspace"
            || (params["sandbox"] == "workspace-write" && params["sandboxPolicy"].is_null()))
        {
            params.as_object_mut().unwrap().remove("sandbox");
            params["sandboxPolicy"] = json!(self.workspace_defaults.policy());
        }
        let p = &params;
        for message in warnings {
            self.send(
                client,
                notification(
                    "warning",
                    json!({"message": message, "threadId": p["threadId"]}),
                ),
            );
        }
        // These supported fields are absent from the pinned non-experimental
        // schema. Other experimental options are rejected by supported_fields.
        let experimental_fields: &[&str] = match method {
            "thread/start" => &["historyMode", "projectId"],
            "thread/list" => &["projectId"],
            "thread/fork" => &["experimentalPredictionMode"],
            _ => &[],
        };
        if !self.connections[&client].experimental
            && let Some(field) = experimental_fields
                .iter()
                .find(|field| !p[**field].is_null())
        {
            return Err(RpcError::invalid(format!(
                "{method}.{field} requires initialize.capabilities.experimentalApi=true"
            )));
        }
        if let Some(config) = crate::plugin_runtime::configured(
            p["cwd"]
                .as_str()
                .map(PathBuf::from)
                .unwrap_or_else(|| self.config.default_cwd.clone()),
        ) {
            let plugin_write = matches!(method, "config/batchWrite" | "config/value/write")
                && (p["keyPath"]
                    .as_str()
                    .is_some_and(|key| key.starts_with("plugins."))
                    || p["edits"].as_array().is_some_and(|edits| {
                        !edits.is_empty()
                            && edits.iter().all(|edit| {
                                edit["keyPath"]
                                    .as_str()
                                    .is_some_and(|key| key.starts_with("plugins."))
                            })
                    }));
            if method.starts_with("plugin/")
                || method.starts_with("skills/")
                || method.starts_with("hooks/")
                || method.starts_with("mcpServer/")
                || method == "mcpServerStatus/list"
                || plugin_write
            {
                if !p["threadId"].is_null() && method == "mcpServer/tool/call" {
                    let thread_id = required_str(p, "threadId")?;
                    let record = self.record(thread_id)?.clone();
                    if record.settings.sandbox.is_read_only() {
                        return Err(RpcError::invalid(
                            "Read-only helpers cannot execute plugin tools",
                        ));
                    }
                    let runtime = self
                        .active
                        .get(thread_id)
                        .and_then(|active| active.plugins.clone());
                    let config =
                        crate::plugin_runtime::configured(record.settings.cwd.clone()).unwrap();
                    let output = self.connections[&client].output.clone();
                    let rpc_id = rpc_id.clone();
                    let params = p.clone();
                    tokio::spawn(async move {
                        let owned = runtime.is_none();
                        let result = async {
                            let runtime = match runtime {
                                Some(runtime) => runtime,
                                None => {
                                    crate::plugin_runtime::Runtime::start(
                                        config,
                                        &record.settings.disabled_plugin_ids,
                                        &record.settings.dynamic_tools,
                                    )
                                    .await?
                                }
                            };
                            let name = runtime
                                .tools
                                .iter()
                                .find(|(_, (_, server, tool))| {
                                    params["server"] == *server && params["tool"] == *tool
                                })
                                .map(|(name, _)| name.clone());
                            let result = match name {
                                Some(name) => {
                                    runtime
                                        .call(
                                            &name,
                                            params
                                                .get("arguments")
                                                .cloned()
                                                .unwrap_or_else(|| json!({})),
                                        )
                                        .await
                                }
                                None => Err(RpcError::invalid(
                                    "Plugin tool is unavailable in this thread",
                                )),
                            };
                            if owned {
                                runtime.client.shutdown().await;
                            }
                            result
                        }
                        .await;
                        let _ = output
                            .send(match result {
                                Ok(value) => response(rpc_id, value),
                                Err(error) => error.response(rpc_id),
                            })
                            .await;
                    });
                    return Ok(None);
                }
                let mut params = p.clone();
                // Genuine Codex does not know facade threads. As in Codex, a
                // thread's hooks are those of its working directory; the
                // shared Codex process otherwise answers with its own config.
                if !p["threadId"].is_null() {
                    let cwd = self
                        .record(required_str(p, "threadId")?)?
                        .settings
                        .cwd
                        .clone();
                    params.as_object_mut().unwrap().remove("threadId");
                    if method == "hooks/list" && params["cwds"].as_array().is_none_or(Vec::is_empty)
                    {
                        params["cwds"] = json!([cwd]);
                    }
                }
                let output = self.connections[&client].output.clone();
                let rpc_id = rpc_id.clone();
                let method = method.to_owned();
                let manager = self.plugin_manager.clone();
                let events = self.events.clone();
                tokio::spawn(async move {
                    let result = manager.request(config, &method, params, events).await;
                    let reply = match result {
                        Ok(value) => response(rpc_id, value),
                        Err(error) => error.response(rpc_id),
                    };
                    let _ = output.send(reply).await;
                });
                return Ok(None);
            }
        }
        match method {
            "initialize" => {
                let c = self.connections.get_mut(&client).unwrap();
                c.initialized = true;
                // Codex desktop starts discovery immediately after this reply;
                // the optional initialized notification is not a readiness gate.
                c.ready = true;
                c.originator = required_str(&p["clientInfo"], "name")?.to_owned();
                c.experimental = p["capabilities"]["experimentalApi"]
                    .as_bool()
                    .unwrap_or(false);
                c.opt_out = p["capabilities"]["optOutNotificationMethods"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect();
                Ok(Some(
                    // Desktop parses the first product version for its protocol
                    // gate. Keep the implementation version separately labeled.
                    json!({"userAgent": format!("codex-compatible/{} (claude-codex-server/{})", protocol::CODEX_VERSION, protocol::VERSION),
                    "codexHome": self.store.root(), "platformFamily": std::env::consts::FAMILY, "platformOs": std::env::consts::OS}),
                ))
            }
            "thread/start" => {
                supported_fields(
                    p,
                    &[
                        "cwd",
                        "model",
                        "effort",
                        "serviceTier",
                        "modelProvider",
                        "approvalPolicy",
                        "approvalsReviewer",
                        "sandbox",
                        "sandboxPolicy",
                        "baseInstructions",
                        "environmentOverrides",
                        "disabledPluginIds",
                        "dynamicTools",
                        "developerInstructions",
                        "ephemeral",
                        "historyMode",
                        "projectId",
                        "daybreakEnabled",
                        "threadSource",
                    ],
                )?;
                let mut settings =
                    Settings::new(self.config.default_cwd.clone(), self.defaults.model.clone());
                settings.effort = self.defaults.effort.clone();
                settings.service_tier = self.defaults.service_tier.clone();
                settings.sandbox = if self.acp() {
                    crate::sandbox::Policy::DangerFullAccess
                } else {
                    self.workspace_defaults.policy()
                };
                settings.apply(p)?;
                self.check_backend_settings(&settings)?;
                let originator = &self.connections[&client].originator;
                let mut record = match adopted_id {
                    Some((id, _)) => Record::with_id(
                        id.to_owned(),
                        settings,
                        self.store.threads_dir(),
                        p,
                        originator,
                    ),
                    None => Record::new(settings, self.store.threads_dir(), p, originator),
                };
                if let Some((_, metadata)) = adopted_id {
                    record.patch_metadata(metadata)?;
                }
                record.validate_read_only_helper()?;
                self.store.save(&record).map_err(RpcError::internal)?;
                let thread_id = record.id().to_owned();
                let result = record.response(false, false);
                let view = record.view(false, true);
                self.records.insert(thread_id.clone(), record);
                self.subscribe(client, &thread_id);
                self.send(client, response(rpc_id.clone(), result));
                self.emit(
                    &thread_id,
                    notification("thread/started", json!({"thread": view})),
                );
                Ok(None)
            }
            "thread/resume" => {
                supported_fields(
                    p,
                    &[
                        "threadId",
                        "cwd",
                        "model",
                        "effort",
                        "serviceTier",
                        "modelProvider",
                        "approvalPolicy",
                        "approvalsReviewer",
                        "sandbox",
                        "sandboxPolicy",
                        "baseInstructions",
                        "environmentOverrides",
                        "disabledPluginIds",
                        "dynamicTools",
                        "developerInstructions",
                        "excludeTurns",
                        "path",
                        "initialTurnsPage",
                    ],
                )?;
                let requested_id = required_str(p, "threadId")?;
                let path = p["path"].as_str().filter(|path| !path.is_empty());
                // A running thread owns its identity; otherwise path takes
                // precedence, matching ThreadResumeParams. Only our persisted
                // records are resumable, never arbitrary files from the client.
                let id = if self.active.contains_key(requested_id) || path.is_none() {
                    requested_id
                } else {
                    self.records
                        .values()
                        .find(|record| record.thread["path"].as_str() == path)
                        .map(Record::id)
                        .ok_or_else(|| {
                            RpcError::invalid("Resume path does not identify a facade thread")
                        })?
                }
                .to_owned();
                let id = id.as_str();
                let mut record = self.record(id)?.clone();
                if record.settings.sandbox.is_read_only() {
                    return Err(RpcError::invalid(
                        "Read-only Claude helpers cannot be resumed; start a fresh ephemeral helper",
                    ));
                }
                if path.is_some() && record.thread["path"].as_str() != path {
                    return Err(RpcError::invalid(
                        "Resume path does not match the active thread",
                    ));
                }
                if record.archived {
                    return Err(RpcError::invalid("Unarchive the thread before resuming it"));
                }
                if self.active.contains_key(id) {
                    // As in Codex, resuming a running thread attaches to it as it
                    // is. A client opening the chat sends its own launch settings
                    // (such as a sandbox preset instead of the turn's explicit
                    // policy); rejecting them would leave the chat unhydrated
                    // until the turn ends.
                    let mut requested = record.settings.clone();
                    let ignored = match requested.apply(p) {
                        Ok(()) => changed_settings(&record.settings, &requested)?.join(", "),
                        Err(error) => error.message,
                    };
                    if !ignored.is_empty() {
                        eprintln!(
                            "thread/resume overrides ignored for running thread {id}: {ignored}"
                        );
                    }
                } else {
                    record.apply_settings(p)?;
                    self.check_backend_settings(&record.settings)?;
                    if !changed_settings(&self.records[id].settings, &record.settings)?.is_empty() {
                        self.cancel_predictions(id);
                    }
                }
                // The next turn reuses the thread's idle Claude process only if
                // these settings leave its launch unchanged; otherwise it starts
                // a new process with --resume and the current instructions.
                record.touch();
                let mut result =
                    record.response(!p["excludeTurns"].as_bool().unwrap_or(false), true);
                // The router uses this capability for desktop prediction
                // compatibility; direct facade replies keep the native provider.
                if !self.acp() {
                    result["claudexPredictionFork"] = json!(true);
                }
                if let Some(options) = p.get("initialTurnsPage").filter(|value| !value.is_null()) {
                    let mut options = options.clone();
                    options["threadId"] = json!(id);
                    result["initialTurnsPage"] = turns_page(&record, &options)?;
                }
                // The snapshot boundary is inclusive. Hydration starts at the
                // newest persisted turn/item even when thread.turns is omitted.
                if let Some(turn) = record.turns.last() {
                    result["turnsBackwardsCursor"] = resume_cursor(id, "turns", turn["id"].clone());
                }
                if let Some(item) = record
                    .turns
                    .iter()
                    .rev()
                    .flat_map(|turn| turn["items"].as_array().unwrap().iter().rev())
                    .next()
                {
                    result["itemsBackwardsCursor"] = resume_cursor(id, "items", item["id"].clone());
                }
                self.store.save(&record).map_err(RpcError::internal)?;
                self.records.insert(id.to_owned(), record);
                self.subscribe(client, id);
                self.claim_detached_turn(client, id);
                // Desktop buffers requests during resume; its snapshot must
                // arrive before replayed approvals and dynamic tool calls.
                self.send(client, response(rpc_id.clone(), result));
                if let Some(turn) = self.records[id].turns.last() {
                    self.send(client, notification("thread/tokenUsage/updated", json!({"threadId":id,"turnId":turn["id"],"tokenUsage":self.records[id].token_usage})));
                }
                self.replay_pending(client, id);
                Ok(None)
            }
            "thread/fork" => {
                supported_fields(
                    p,
                    &[
                        "threadId",
                        "cwd",
                        "model",
                        "serviceTier",
                        "modelProvider",
                        "approvalPolicy",
                        "approvalsReviewer",
                        "sandbox",
                        "sandboxPolicy",
                        "ephemeral",
                        "excludeTurns",
                        "experimentalPredictionMode",
                        "threadSource",
                        "beforeTurnId",
                        "lastTurnId",
                        "disabledPluginIds",
                        "environmentOverrides",
                        "baseInstructions",
                        "developerInstructions",
                        "attachToSource",
                    ],
                )?;
                let source_id = required_str(p, "threadId")?;
                let prediction_mode = p["experimentalPredictionMode"] == true;
                // As in Codex: a persistent side chat of a paginated source.
                let attach = p["attachToSource"] == true;
                if attach && (p["ephemeral"] == true || prediction_mode) {
                    return Err(RpcError::invalid(
                        "Attached forks are persistent and cannot be ephemeral",
                    ));
                }
                if prediction_mode {
                    if self.acp() {
                        return Err(RpcError::invalid(
                            "Prediction forks require the native Claude backend",
                        ));
                    }
                    if !self.loaded.contains(source_id) {
                        return Err(RpcError::invalid(
                            "Resume the parent thread before requesting a prediction fork",
                        ));
                    }
                    self.idle(source_id)?;
                }
                // ACP forks copy the whole live session. Claude can instead
                // snapshot a completed prefix while the parent keeps working.
                if self.acp() {
                    self.idle(source_id)?;
                }
                let source = self.record(source_id)?;
                if crate::subagents::is_child(source) {
                    return Err(subagent_read_only());
                }
                if source.ephemeral() {
                    return Err(RpcError::invalid(
                        "Ephemeral Claude sessions cannot be forked",
                    ));
                }
                let mut boundary = crate::history_recovery::HistoryBoundary::for_fork(p)?;
                if !self.acp()
                    && boundary == crate::history_recovery::HistoryBoundary::Full
                    && let Some(turn) = source.turns.last().filter(|t| t["status"] == "inProgress")
                {
                    boundary = crate::history_recovery::HistoryBoundary::BeforeTurn(
                        required_str(turn, "id")?.into(),
                    );
                }
                if self.acp() {
                    if self.acp_lacks("fork") {
                        return Err(RpcError::invalid(
                            "This ACP agent does not support forking sessions",
                        ));
                    }
                    // session/fork copies the whole session; it cannot fork at an earlier turn.
                    let latest = source.turns.last().and_then(|turn| turn["id"].as_str());
                    match &boundary {
                        crate::history_recovery::HistoryBoundary::Full => {}
                        crate::history_recovery::HistoryBoundary::ThroughTurn(id)
                            if Some(id.as_str()) == latest => {}
                        _ => {
                            return Err(RpcError::invalid(
                                "ACP agents can fork a thread only at its latest turn",
                            ));
                        }
                    }
                }
                let mut settings = source.settings.clone();
                settings.apply(p)?;
                self.check_backend_settings(&settings)?;
                let mut record = Record::new(
                    settings,
                    self.store.threads_dir(),
                    p,
                    &self.connections[&client].originator,
                );
                if attach && source.thread["historyMode"] != "paginated" {
                    return Err(RpcError::invalid(
                        "Attached forks require a paginated source",
                    ));
                }
                record.prediction_mode = prediction_mode;
                record.attached_to = attach.then(|| source_id.to_owned());
                record.thread["forkedFromId"] = json!(source_id);
                record.thread["sessionId"] = source.thread["sessionId"].clone();
                record.thread["preview"] = source.thread["preview"].clone();
                record.thread["historyMode"] = source.thread["historyMode"].clone();
                record.thread["projectId"] = source.thread["projectId"].clone();
                record.turns = source.turns.clone();
                record.item_times = source.item_times.clone();
                record.turn_anchors = source.turn_anchors.clone();
                record.tracks_turn_anchors = source.tracks_turn_anchors;
                record.has_session = source.has_session;
                record.session_id = source.session_id.clone();
                record.fork_from = source.fork_from.clone();
                record.backend_message_id = source.backend_message_id.clone();
                record.recover_history(&boundary)?;
                self.store.save(&record).map_err(RpcError::internal)?;
                let id = record.id().to_owned();
                let result = record.response(!p["excludeTurns"].as_bool().unwrap_or(false), false);
                let view = record.view(false, true);
                self.records.insert(id.clone(), record);
                self.subscribe(client, &id);
                self.send(client, response(rpc_id.clone(), result));
                self.emit(&id, notification("thread/started", json!({"thread": view})));
                Ok(None)
            }
            "thread/inject_items" => {
                supported_fields(p, &["threadId", "items"])?;
                let id = required_str(p, "threadId")?;
                self.idle(id)?;
                let mut record = self.record(id)?.clone();
                if record.archived || crate::subagents::is_child(&record) {
                    return Err(RpcError::invalid(
                        "Cannot inject into an archived or child thread",
                    ));
                }
                // The desktop injects the side-conversation boundary as a
                // model-visible user message, without running a turn for it.
                // Validate the entire batch before queuing any of its context.
                let mut context = Vec::new();
                for item in p["items"].as_array().unwrap() {
                    if item["type"] != "message" || item["role"] != "user" {
                        return Err(RpcError::invalid("Only user text messages can be injected"));
                    }
                    let content = item["content"].as_array().ok_or_else(|| {
                        RpcError::invalid("Injected messages require a content array")
                    })?;
                    for part in content {
                        if part["type"] != "input_text" || !part["text"].is_string() {
                            return Err(RpcError::invalid(
                                "Only input_text content can be injected",
                            ));
                        }
                        context.push(part["text"].as_str().unwrap().to_owned());
                    }
                }
                record.pending_context.extend(context);
                self.store.save(&record).map_err(RpcError::internal)?;
                self.records.insert(id.to_owned(), record);
                Ok(Some(json!({})))
            }
            "thread/revert" | "thread/rollback" => {
                supported_fields(p, &["threadId", "beforeTurnId", "numTurns"])?;
                if self.acp() {
                    return Err(RpcError::invalid(
                        "ACP agents cannot rewind a session, so thread history cannot be reverted",
                    ));
                }
                let id = required_str(p, "threadId")?;
                self.idle(id)?;
                if !self.loaded.contains(id) {
                    return Err(RpcError::invalid(
                        "Resume the thread before recovering its history",
                    ));
                }
                let mut record = self.record(id)?.clone();
                if record.ephemeral() {
                    return Err(RpcError::invalid(
                        "Ephemeral Claude history cannot be recovered",
                    ));
                }
                if record.archived {
                    return Err(RpcError::invalid(
                        "Unarchive the thread before recovering its history",
                    ));
                }
                if crate::subagents::is_child(&record) {
                    return Err(subagent_read_only());
                }
                let boundary = if method == "thread/revert" {
                    if record.thread["historyMode"] != "paginated" {
                        return Err(RpcError::invalid(
                            "thread/revert requires paginated history",
                        ));
                    }
                    crate::history_recovery::HistoryBoundary::BeforeTurn(
                        required_str(p, "beforeTurnId")?.into(),
                    )
                } else {
                    crate::history_recovery::HistoryBoundary::Rollback(
                        p["numTurns"]
                            .as_u64()
                            .and_then(|n| u32::try_from(n).ok())
                            .filter(|n| *n > 0)
                            .ok_or_else(|| RpcError::invalid("numTurns must be a positive u32"))?,
                    )
                };
                record.recover_history(&boundary)?;
                let result = if method == "thread/revert" {
                    json!({"thread":record.view(false,true),
                        "turnsBackwardsCursor":record.turns.last().map(|turn|resume_cursor(id,"turns",turn["id"].clone())),
                        "itemsBackwardsCursor":record.turns.iter().rev().flat_map(|turn|turn["items"].as_array().into_iter().flatten().rev())
                            .next().map(|item|resume_cursor(id,"items",item["id"].clone()))})
                } else {
                    json!({"thread":record.view(true,true)})
                };
                self.store.save(&record).map_err(RpcError::internal)?;
                self.records.insert(id.into(), record);
                self.send(client, response(rpc_id.clone(), result));
                self.emit(id, notification("thread/reverted", json!({"threadId":id})));
                Ok(None)
            }
            "thread/attachment/add" | "thread/attachment/list" | "thread/attachment/remove" => {
                let id = required_str(p, "threadId")?;
                let mut record = self.record(id)?.clone();
                let change = crate::attachments::dispatch(&mut record.attachments, method, p)?;
                if change.notification.is_some() {
                    self.store.save(&record).map_err(RpcError::internal)?;
                    self.records.insert(id.into(), record);
                }
                self.send(client, response(rpc_id.clone(), change.response));
                if let Some(notification) = change.notification {
                    self.emit(id, notification);
                }
                Ok(None)
            }
            "thread/read" => {
                supported_fields(p, &["threadId", "includeTurns"])?;
                let id = required_str(p, "threadId")?;
                let record = self.record(id)?;
                Ok(Some(
                    json!({"thread": record.view(p["includeTurns"].as_bool().unwrap_or(false), self.loaded.contains(id)),
                        "readState": record.read_state()}),
                ))
            }
            "thread/readState/update" => {
                supported_fields(p, &["threadId", "operation", "expectedRevision"])?;
                let id = required_str(p, "threadId")?;
                let unread = p["operation"]["type"] == "unread";
                let mut record = self.record(id)?.clone();
                let read_state =
                    match record.mark_read_state(required_str(p, "expectedRevision")?, unread) {
                        Ok(read_state) => read_state,
                        Err(Value::Null) => {
                            return Err(RpcError::invalid("thread read state unavailable"));
                        }
                        // As in Codex, a stale edit reports what changed instead.
                        Err(current) => {
                            let mut error = RpcError::invalid("read state changed");
                            error.data =
                                Some(json!({"reason":"readStateConflict","readState":current}));
                            return Err(error);
                        }
                    };
                self.store.save(&record).map_err(RpcError::internal)?;
                self.records.insert(id.to_owned(), record);
                self.emit(
                    id,
                    notification(
                        "thread/readState/changed",
                        json!({"threadId": id, "readState": read_state}),
                    ),
                );
                Ok(Some(json!({"readState": read_state})))
            }
            "thread/list" => self.list_threads(p).map(Some),
            "thread/loaded/list" => {
                supported_fields(p, &["cursor", "limit"])?;
                let mut ids: Vec<Value> = self.loaded.iter().map(|id| json!(id)).collect();
                ids.sort_by_key(Value::to_string);
                let mut page = page(ids, p, false, "loaded")?;
                page.as_object_mut().unwrap().remove("backwardsCursor");
                Ok(Some(page))
            }
            "thread/timeline/list" => {
                supported_fields(p, &["threadId", "cursor", "limit"])?;
                let id = required_str(p, "threadId")?;
                Ok(Some(crate::timeline::list(id, &self.record(id)?.turns, p)?))
            }
            "thread/turns/list" => {
                supported_fields(
                    p,
                    &["threadId", "cursor", "limit", "sortDirection", "itemsView"],
                )?;
                let id = required_str(p, "threadId")?;
                turns_page(self.record(id)?, p).map(Some)
            }
            "thread/items/list" => {
                supported_fields(
                    p,
                    &["threadId", "turnId", "cursor", "limit", "sortDirection"],
                )?;
                let id = required_str(p, "threadId")?;
                let record = self.record(id)?;
                if let Some(turn_id) = p["turnId"].as_str()
                    && !record.turns.iter().any(|t| t["id"] == turn_id)
                {
                    return Err(RpcError::invalid("Turn not found"));
                }
                let mut items: Vec<Value> = record
                    .turns
                    .iter()
                    .flat_map(|turn| {
                        turn["items"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|item| record.item_entry(&turn["id"], item))
                    })
                    .collect();
                let mut options = p.clone();
                apply_resume_boundary(&mut items, &mut options, id, "items")?;
                items.retain(|item| p["turnId"].is_null() || p["turnId"] == item["turnId"]);
                page(
                    items,
                    &options,
                    false,
                    &format!("items:{id}:{}", p["turnId"]),
                )
                .map(Some)
            }
            "thread/items/read" => {
                supported_fields(p, &["threadId", "turnId", "itemIds"])?;
                let ids = p["itemIds"].as_array().unwrap();
                if !(1..=100).contains(&ids.len()) {
                    return Err(RpcError::invalid("itemIds must contain 1 to 100 IDs"));
                }
                let record = self.record(required_str(p, "threadId")?)?;
                let turn = record
                    .turns
                    .iter()
                    .find(|turn| turn["id"] == p["turnId"])
                    .ok_or_else(|| RpcError::invalid("Turn not found"))?;
                // As in Codex: stored order, and unknown ids are omitted.
                let data: Vec<Value> = turn["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|item| ids.contains(&item["id"]))
                    .map(|item| record.item_entry(&turn["id"], item))
                    .collect();
                Ok(Some(json!({"data": data})))
            }
            "thread/metadata/update" => {
                supported_fields(p, &["threadId", "projectId", "gitInfo", "daybreakEnabled"])?;
                let id = required_str(p, "threadId")?;
                let mut record = self.record(id)?.clone();
                if record.ephemeral() {
                    return Err(RpcError::invalid(
                        "Metadata updates require a persistent thread",
                    ));
                }
                let previous_project = record.thread["projectId"].clone();
                record.patch_metadata(p)?;
                record.touch();
                let result = json!({"thread": record.view(false, self.loaded.contains(id))});
                self.store.save(&record).map_err(RpcError::internal)?;
                let project = record.thread["projectId"].clone();
                self.records.insert(id.to_owned(), record);
                self.send(client, response(rpc_id.clone(), result));
                if previous_project != project {
                    self.emit(
                        id,
                        notification(
                            "thread/project/updated",
                            json!({"threadId":id,"projectId":project}),
                        ),
                    );
                }
                Ok(None)
            }
            "thread/settings/update" => {
                supported_fields(
                    p,
                    &[
                        "threadId",
                        "model",
                        "effort",
                        "serviceTier",
                        "summary",
                        "cwd",
                        "approvalPolicy",
                        "approvalsReviewer",
                        "sandboxPolicy",
                        "disabledPluginIds",
                        "developerInstructions",
                    ],
                )?;
                let id = required_str(p, "threadId")?;
                self.idle(id)?;
                let mut record = self.record(id)?.clone();
                record.apply_settings(p)?;
                self.check_backend_settings(&record.settings)?;
                record.touch();
                let settings = record.thread_settings();
                self.store.save(&record).map_err(RpcError::internal)?;
                self.cancel_predictions(id);
                self.records.insert(id.to_owned(), record);
                self.send(client, response(rpc_id.clone(), json!({})));
                self.emit(
                    id,
                    notification(
                        "thread/settings/updated",
                        json!({"threadId":id,"threadSettings":settings}),
                    ),
                );
                Ok(None)
            }
            "thread/name/set" => {
                supported_fields(p, &["threadId", "name"])?;
                let id = required_str(p, "threadId")?;
                let name = required_str(p, "name")?;
                let mut record = self.record(id)?.clone();
                record.thread["name"] = json!(name);
                record.touch();
                self.store.save(&record).map_err(RpcError::internal)?;
                self.records.insert(id.to_owned(), record);
                self.send(client, response(rpc_id.clone(), json!({})));
                self.emit(
                    id,
                    notification(
                        "thread/name/updated",
                        json!({"threadId": id, "threadName": name}),
                    ),
                );
                Ok(None)
            }
            "thread/archive" | "thread/unarchive" => {
                supported_fields(p, &["threadId"])?;
                let id = required_str(p, "threadId")?;
                self.idle(id)?;
                if method == "thread/archive" {
                    self.close_idle_session(id);
                }
                let mut record = self.record(id)?.clone();
                record.archived = method == "thread/archive";
                record.touch();
                let result = if record.archived {
                    json!({})
                } else {
                    json!({"thread": record.view(false, self.loaded.contains(id))})
                };
                self.store.save(&record).map_err(RpcError::internal)?;
                self.records.insert(id.to_owned(), record);
                self.send(client, response(rpc_id.clone(), result));
                self.emit(
                    id,
                    notification(
                        if method == "thread/archive" {
                            "thread/archived"
                        } else {
                            "thread/unarchived"
                        },
                        json!({"threadId": id}),
                    ),
                );
                if method == "thread/archive" {
                    self.loaded.remove(id);
                }
                Ok(None)
            }
            "thread/delete" => {
                supported_fields(p, &["threadId"])?;
                let id = required_str(p, "threadId")?;
                self.delete_thread(client, rpc_id, id).await?;
                Ok(None)
            }
            "thread/goal/get" => {
                supported_fields(p, &["threadId"])?;
                self.record(required_str(p, "threadId")?)?;
                // Goals are a Codex runtime feature; Claude threads never have one.
                Ok(Some(json!({"goal": null})))
            }
            "feedback/save" => {
                supported_fields(
                    p,
                    &[
                        "classification",
                        "reason",
                        "threadId",
                        "includeLogs",
                        "extraLogFiles",
                        "tags",
                        "savePath",
                    ],
                )?;
                // Genuine Codex writes the diagnostics archive with its logs.
                // A facade thread contributes its saved record instead of a
                // Codex rollout.
                let Some(config) =
                    crate::plugin_runtime::configured(self.config.default_cwd.clone())
                else {
                    return Err(RpcError::invalid(
                        "Saving diagnostics requires the genuine Codex executable",
                    ));
                };
                let mut params = p.clone();
                let thread_id = p["threadId"].as_str().map(str::to_owned);
                if let Some(id) = &thread_id {
                    let record = self.record(id)?;
                    params.as_object_mut().unwrap().remove("threadId");
                    if let Some(path) = record.thread["path"].as_str() {
                        let mut files = params["extraLogFiles"]
                            .as_array()
                            .cloned()
                            .unwrap_or_default();
                        files.push(json!(path));
                        params["extraLogFiles"] = json!(files);
                    }
                }
                let output = self.connections[&client].output.clone();
                let rpc_id = rpc_id.clone();
                let manager = self.plugin_manager.clone();
                let events = self.events.clone();
                tokio::spawn(async move {
                    let result = manager
                        .request(config, "feedback/save", params, events)
                        .await
                        .map(|mut saved| {
                            if let Some(id) = thread_id {
                                saved["threadId"] = json!(id);
                            }
                            saved
                        });
                    let _ = output
                        .send(match result {
                            Ok(value) => response(rpc_id, value),
                            Err(error) => error.response(rpc_id),
                        })
                        .await;
                });
                Ok(None)
            }
            "thread/memory/read" => {
                supported_fields(p, &["threadId"])?;
                self.record(required_str(p, "threadId")?)?;
                // Codex memories are not part of a Claude thread's context.
                Ok(Some(json!({"summary": null})))
            }
            // Genuine Codex 0.166 answers the same way.
            "thread/prediction/request" => Err(RpcError::new(
                -32601,
                "thread/prediction/request is not implemented yet",
            )),
            "thread/attachmentOwner/list" => {
                supported_fields(
                    p,
                    &[
                        "attachmentType",
                        "identityKey",
                        "archived",
                        "cursor",
                        "limit",
                    ],
                )?;
                let (kind, key) = (
                    required_str(p, "attachmentType")?,
                    required_str(p, "identityKey")?,
                );
                // Every persisted owner, including threads without user messages.
                let owners: Vec<Value> = self
                    .records
                    .values()
                    .filter(|record| {
                        !record.ephemeral()
                            && p["archived"].as_bool().is_none_or(|a| a == record.archived)
                            && record
                                .attachments
                                .iter()
                                .any(|a| a.attachment_type == kind && a.identity_key == key)
                    })
                    .map(|record| json!(record.id()))
                    .collect();
                let mut result = page(owners, p, false, &format!("owners:{kind}:{key}"))?;
                result.as_object_mut().unwrap().remove("backwardsCursor");
                result["data"] = result["data"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|id| json!({"threadId": id, "archived": self.records[id.as_str().unwrap()].archived}))
                    .collect();
                Ok(Some(result))
            }
            "thread/sample" => {
                supported_fields(
                    p,
                    &[
                        "threadId",
                        "expectedTurnId",
                        "prompt",
                        "effort",
                        "threadSource",
                        "turnTrigger",
                        "responsesapiClientMetadata",
                        "disableMcp",
                    ],
                )?;
                let id = required_str(p, "threadId")?;
                let prompt = required_str(p, "prompt")?.to_owned();
                if prompt.trim().is_empty() || prompt.len() > PREDICTION_PROMPT_BYTES {
                    return Err(RpcError::invalid(
                        "thread/sample requires a nonempty prompt of at most 8 KiB",
                    ));
                }
                let record = self.record(id)?;
                // As in Codex, only the loaded source's latest settled turn.
                if !self.loaded.contains(id)
                    || self.active.contains_key(id)
                    || record.turns.last().is_none_or(|turn| {
                        turn["id"] != p["expectedTurnId"] || turn["status"] == "inProgress"
                    })
                {
                    return Err(RpcError::invalid(
                        "The sample source turn is stale or still active",
                    ));
                }
                // A read-only question on the parent's warm process keeps its
                // model, tools and cached prefix; effort and MCP options would
                // invalidate that cache. A cold parent is not sampled at all.
                let question = self.warm_question_client(record);
                let cancel = CancellationToken::new();
                let key = (client, rpc_id.to_string());
                self.samples
                    .lock()
                    .unwrap()
                    .insert(key.clone(), (id.to_owned(), cancel.clone()));
                let samples = self.samples.clone();
                let output = self.connections[&client].output.clone();
                let rpc_id = rpc_id.clone();
                let source_turn = p["expectedTurnId"].clone();
                tokio::spawn(async move {
                    let answer = match question {
                        Some(question) => question.ask(&prompt, &cancel).await.ok(),
                        None => None,
                    };
                    // Cancellation already answered the request.
                    if samples.lock().unwrap().remove(&key).is_none() {
                        return;
                    }
                    let text = answer
                        .filter(|answer| answer["synthetic"] != true)
                        .and_then(|answer| answer["response"].as_str().map(str::to_owned));
                    // Native side questions report no per-request usage.
                    let result = json!({"responseId": protocol::id(), "sourceTurnId": source_turn,
                        "text": text, "inputTokens": null, "cachedInputTokens": null,
                        "outputTokens": null});
                    let _ = output.send(response(rpc_id, result)).await;
                });
                Ok(None)
            }
            "thread/sample/cancel" => {
                supported_fields(p, &["requestId"])?;
                let key = (client, p["requestId"].to_string());
                let Some((_, cancel)) = self.samples.lock().unwrap().remove(&key) else {
                    return Err(RpcError::invalid("No running sample has this request id"));
                };
                cancel.cancel();
                self.send(
                    client,
                    RpcError::invalid("The sample was cancelled").response(p["requestId"].clone()),
                );
                Ok(Some(json!({})))
            }
            "thread/searchOccurrences" => {
                supported_fields(p, &["threadId", "searchTerm", "cursor", "limit"])?;
                let id = required_str(p, "threadId")?;
                let turns = &self.record(id)?.turns;
                crate::occurrences::search(id, turns, p, |turn| turn_cursor(id, turn)).map(Some)
            }
            "thread/section/move" => {
                let result = self.move_to_section(p)?;
                let method = "thread/section/changed";
                let changed = notification(
                    method,
                    json!({"threadId": p["threadId"], "sectionId": p["sectionId"]}),
                );
                for (&peer, connection) in &self.connections {
                    if connection.ready && !connection.opt_out.contains(method) {
                        self.send(peer, changed.clone());
                    }
                }
                Ok(Some(result))
            }
            "thread/unsubscribe" => {
                supported_fields(p, &["threadId"])?;
                let id = required_str(p, "threadId")?;
                if self.active.get(id).is_some_and(|a| a.owner == Some(client)) {
                    return Err(RpcError::invalid(
                        "Interrupt your active turn before unsubscribing",
                    ));
                }
                let status = if !self.loaded.contains(id) {
                    "notLoaded"
                } else if self
                    .connections
                    .get_mut(&client)
                    .unwrap()
                    .subscriptions
                    .remove(id)
                {
                    "unsubscribed"
                } else {
                    "notSubscribed"
                };
                if !self
                    .connections
                    .values()
                    .any(|c| c.subscriptions.contains(id))
                    && !self.active.contains_key(id)
                {
                    self.loaded.remove(id);
                    if self.records.get(id).is_some_and(Record::ephemeral) {
                        self.close_idle_session(id);
                        self.records.remove(id);
                    }
                }
                Ok(Some(json!({"status": status})))
            }
            "turn/start" => {
                let request = TurnRequest::Input {
                    queued_submission_id: None,
                };
                self.start_turn(client, rpc_id, p, request).await?;
                Ok(None)
            }
            "thread/compact/start" => {
                supported_fields(p, &["threadId"])?;
                self.start_turn(client, rpc_id, p, TurnRequest::Compaction)
                    .await?;
                Ok(None)
            }
            "thread/approveGuardianDeniedAction" => self.approve_denied_action(p).map(Some),
            "thread/queue/add"
            | "thread/queue/list"
            | "thread/queue/update"
            | "thread/queue/delete"
            | "thread/queue/reorder"
            | "thread/queue/start" => self.queue_request(client, rpc_id, method, p).await,
            "turn/steer" => {
                self.steer_turn(client, rpc_id, p).await?;
                Ok(None)
            }
            "turn/interrupt" => {
                supported_fields(p, &["threadId", "turnId"])?;
                let id = required_str(p, "threadId")?;
                let turn_id = required_str(p, "turnId")?;
                let active = self
                    .active
                    .get_mut(id)
                    .ok_or_else(|| RpcError::invalid("Thread has no active turn"))?;
                if active.id != turn_id {
                    return Err(RpcError::invalid("turnId does not match the active turn"));
                }
                active.interrupting = true;
                active
                    .control
                    .try_send(Control::Interrupt)
                    .map_err(RpcError::internal)?;
                Ok(Some(json!({})))
            }
            "command/exec"
            | "command/exec/write"
            | "command/exec/resize"
            | "command/exec/terminate"
            | "process/spawn"
            | "process/writeStdin"
            | "process/kill"
            | "process/resizePty" => {
                self.commands
                    .dispatch(
                        client,
                        self.connections[&client].output.clone(),
                        rpc_id.clone(),
                        method,
                        p.clone(),
                        &self.config.default_cwd,
                    )
                    .await
            }
            "fs/readFile" | "fs/writeFile" | "fs/copy" | "fs/remove" => {
                let output = self.connections[&client].output.clone();
                let rpc_id = rpc_id.clone();
                let method = method.to_owned();
                let params = p.clone();
                tokio::spawn(async move {
                    let message = match crate::filesystem::dispatch(&method, &params).await {
                        Ok(result) => response(rpc_id, result),
                        Err(error) => error.response(rpc_id),
                    };
                    let _ = output.send(message).await;
                });
                Ok(None)
            }
            "fs/getMetadata" | "fs/readDirectory" => {
                supported_fields(p, &["path"])?;
                let path = PathBuf::from(required_str(p, "path")?);
                if !path.is_absolute() {
                    return Err(RpcError::invalid("path must be absolute"));
                }
                if method == "fs/getMetadata" {
                    let link = tokio::fs::symlink_metadata(&path)
                        .await
                        .map_err(RpcError::internal)?;
                    let metadata = tokio::fs::metadata(&path)
                        .await
                        .map_err(RpcError::internal)?;
                    let millis = |time: std::io::Result<std::time::SystemTime>| {
                        time.ok()
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
                            .unwrap_or(0)
                    };
                    Ok(Some(
                        json!({"isDirectory": metadata.is_dir(), "isFile": metadata.is_file(),
                        "isSymlink": link.is_symlink(), "createdAtMs": millis(metadata.created()),
                        "modifiedAtMs": millis(metadata.modified())}),
                    ))
                } else {
                    let mut directory = tokio::fs::read_dir(&path)
                        .await
                        .map_err(RpcError::internal)?;
                    let mut entries = Vec::new();
                    while let Some(entry) =
                        directory.next_entry().await.map_err(RpcError::internal)?
                    {
                        // Follow symlinks for the directory picker, retaining
                        // dangling entries with neither file nor directory set.
                        let metadata = tokio::fs::metadata(entry.path()).await;
                        entries.push(json!({"fileName": entry.file_name().to_string_lossy(),
                            "isDirectory": metadata.as_ref().is_ok_and(|m| m.is_dir()),
                            "isFile": metadata.as_ref().is_ok_and(|m| m.is_file())}));
                    }
                    Ok(Some(json!({"entries": entries})))
                }
            }
            "fs/createDirectory" => {
                supported_fields(p, &["path", "recursive"])?;
                let path = PathBuf::from(required_str(p, "path")?);
                if !path.is_absolute() {
                    return Err(RpcError::invalid("path must be absolute"));
                }
                if p["recursive"].as_bool().unwrap_or(true) {
                    tokio::fs::create_dir_all(path).await
                } else {
                    tokio::fs::create_dir(path).await
                }
                .map_err(RpcError::internal)?;
                Ok(Some(json!({})))
            }
            "model/list" => {
                supported_fields(p, &["cursor", "limit", "includeHidden", "modelProvider"])?;
                if let Some(provider) = p["modelProvider"].as_str()
                    && provider != protocol::provider().id
                {
                    return Err(RpcError {
                        code: -32600,
                        message: format!("Model provider '{provider}' is not configured"),
                        data: Some(
                            json!({"type":"modelProviderNotConfigured","modelProvider":provider}),
                        ),
                    });
                }
                if self.model_catalog.is_some() {
                    return self.model_page(p).map(Some);
                }
                // Probing starts a Claude process; other threads must not wait for it.
                self.catalog_waiters
                    .push((client, rpc_id.clone(), p.clone()));
                if self.catalog_waiters.len() == 1 {
                    self.probe_models();
                }
                Ok(None)
            }
            "getAuthStatus" => {
                // OpenAI credentials do not apply to this provider. Claude
                // handles its own credentials when a turn is started.
                Ok(Some(json!({"authMethod": null, "authToken": null,
                    "requiresOpenaiAuth": false})))
            }
            "account/read" => {
                supported_fields(p, &["refreshToken"])?;
                if p["refreshToken"] == true {
                    return Err(RpcError::invalid(
                        "Claude manages its own authentication; refresh through claude auth",
                    ));
                }
                Ok(Some(
                    json!({"account": null, "requiresOpenaiAuth": false, "workspaceRouting": null}),
                ))
            }
            "plugin/list" | "plugin/installed" | "plugin/search" | "plugin/read"
            | "plugin/install" | "plugin/uninstall" => {
                // Claude's native plugin catalog does not apply to another agent.
                if self.acp() {
                    return match method {
                        "plugin/list" => Ok(Some(
                            json!({"marketplaces":[],"marketplaceLoadErrors":[],"featuredPluginIds":[]}),
                        )),
                        "plugin/installed" => {
                            Ok(Some(json!({"marketplaces":[],"marketplaceLoadErrors":[]})))
                        }
                        "plugin/search" => Ok(Some(json!({"data":[],"nextCursor":null}))),
                        _ => Err(RpcError::invalid(
                            "No plugin catalog is configured for this ACP agent",
                        )),
                    };
                }
                let service = self.plugin_service(None);
                let output = self.connections[&client].output.clone();
                let rpc_id = rpc_id.clone();
                let method = method.to_owned();
                let params = p.clone();
                tokio::spawn(async move {
                    let reply = match service.handle(&method, &params).await {
                        Ok(value) => response(rpc_id, value),
                        Err(error) => error.response(rpc_id),
                    };
                    let _ = output.send(reply).await;
                });
                Ok(None)
            }
            "config/batchWrite" => {
                supported_fields(
                    p,
                    &["edits", "filePath", "expectedVersion", "reloadUserConfig"],
                )?;
                if !p["filePath"].is_null() {
                    return Err(RpcError::invalid(
                        "Only facade model defaults can be written",
                    ));
                }
                if let Some(version) = p["expectedVersion"].as_str()
                    && version != self.defaults.version
                {
                    return Err(RpcError::invalid("Configuration version changed"));
                }
                let edits = p["edits"].as_array().unwrap();
                if edits.iter().any(|edit| {
                    edit["keyPath"]
                        .as_str()
                        .is_some_and(|key| key.starts_with("plugins."))
                }) {
                    if self.acp() {
                        return Err(RpcError::invalid(
                            "No plugin catalog is configured for this ACP agent",
                        ));
                    }
                    if edits.len() != 1 {
                        return Err(RpcError::invalid(
                            "Native plugin edits must be submitted individually; mixed transactions cannot be atomic",
                        ));
                    }
                    let edit = &edits[0];
                    let id =
                        crate::plugins::enabled_key_id(required_str(edit, "keyPath")?)?.to_owned();
                    let enabled = edit["value"]
                        .as_bool()
                        .ok_or_else(|| RpcError::invalid("Plugin enabled must be boolean"))?;
                    if edit["mergeStrategy"] != "upsert" {
                        return Err(RpcError::invalid("Plugin enabled edits require upsert"));
                    }
                    if !p["expectedVersion"].is_null() {
                        return Err(RpcError::invalid(
                            "Native plugin config does not support facade version preconditions",
                        ));
                    }
                    let service = self.plugin_service(None);
                    let output = self.connections[&client].output.clone();
                    let rpc_id = rpc_id.clone();
                    tokio::spawn(async move {
                        let reply = match service.set_enabled(&id, enabled).await {
                            Ok(()) => response(
                                rpc_id,
                                json!({"status":"ok","version":"native","filePath":service.settings_path(),"overriddenMetadata":null}),
                            ),
                            Err(error) => error.response(rpc_id),
                        };
                        let _ = output.send(reply).await;
                    });
                    return Ok(None);
                }
                let mut next = self.defaults.clone();
                for edit in p["edits"].as_array().unwrap() {
                    if edit["mergeStrategy"] != "upsert" {
                        return Err(RpcError::invalid("Model defaults require upsert"));
                    }
                    let mut settings =
                        Settings::new(self.config.default_cwd.clone(), next.model.clone());
                    match edit["keyPath"].as_str() {
                        Some("model") if edit["value"].is_string() => {
                            settings.apply(&json!({"model": edit["value"]}))?;
                            next.model = settings.model;
                        }
                        Some("model_reasoning_effort")
                            if edit["value"].is_string() || edit["value"].is_null() =>
                        {
                            settings.apply(&json!({"effort": edit["value"]}))?;
                            next.effort = settings.effort;
                        }
                        Some("service_tier") => {
                            next.service_tier = crate::store::service_tier(&edit["value"])?;
                            if self.acp() && next.service_tier.is_some() {
                                return Err(acp_fast_mode());
                            }
                        }
                        _ => {
                            return Err(RpcError::invalid(
                                "Only model, model_reasoning_effort and service_tier defaults are supported",
                            ));
                        }
                    }
                }
                next.version = protocol::id();
                self.store
                    .save_defaults(&next)
                    .map_err(RpcError::internal)?;
                self.defaults = next;
                Ok(Some(
                    json!({"status": "ok", "version": self.defaults.version,
                    "filePath": self.store.defaults_path(), "overriddenMetadata": null}),
                ))
            }
            "config/read" => {
                // Facade configuration has no per-project layers. Claude loads
                // its own project settings when executing a turn.
                supported_fields(p, &["includeLayers", "cwd"])?;
                let schema: Value = serde_json::from_str(protocol::SCHEMA).unwrap();
                let mut config = serde_json::Map::new();
                for key in schema["definitions"]["v2"]["Config"]["properties"]
                    .as_object()
                    .unwrap()
                    .keys()
                {
                    config.insert(key.clone(), Value::Null);
                }
                config.insert("model".into(), json!(self.defaults.model));
                config.insert("model_reasoning_effort".into(), json!(self.defaults.effort));
                config.insert("service_tier".into(), json!(self.defaults.service_tier));
                let provider = protocol::provider();
                config.insert("model_provider".into(), json!(provider.id));
                config.insert(
                    "model_providers".into(),
                    json!({ provider.id.clone(): {
                        "name": provider.name, "requires_openai_auth": false
                    }}),
                );
                config.insert("approval_policy".into(), json!("on-request"));
                config.insert("approvals_reviewer".into(), json!("user"));
                config.insert(
                    "sandbox_mode".into(),
                    json!(if self.acp() {
                        "danger-full-access"
                    } else {
                        "workspace-write"
                    }),
                );
                // These are host-configured facade defaults, not the plugin
                // sidecar's permissions. The desktop must show the actual grants.
                config.insert(
                    "sandbox_workspace_write".into(),
                    self.workspace_defaults.desktop_config(),
                );
                if let Some(sidecar_config) = crate::plugin_runtime::configured(
                    p["cwd"]
                        .as_str()
                        .map(PathBuf::from)
                        .unwrap_or_else(|| self.config.default_cwd.clone()),
                ) {
                    let output = self.connections[&client].output.clone();
                    let rpc_id = rpc_id.clone();
                    let params = p.clone();
                    let manager = self.plugin_manager.clone();
                    let events = self.events.clone();
                    tokio::spawn(async move {
                        let result = async {
                            let mut result = manager
                                .request(sidecar_config, "config/read", params, events)
                                .await?;
                            // Preserve genuine plugin config provenance and versions;
                            // model/permission defaults remain facade-owned.
                            for (key, value) in config {
                                if key != "plugins" && !value.is_null() {
                                    result["config"][key] = value;
                                }
                            }
                            Ok::<_, RpcError>(result)
                        }
                        .await;
                        let _ = output
                            .send(match result {
                                Ok(value) => response(rpc_id, value),
                                Err(error) => error.response(rpc_id),
                            })
                            .await;
                    });
                    return Ok(None);
                }
                if self.acp() {
                    config.insert("plugins".into(), json!({}));
                    let layers = if p["includeLayers"] == true {
                        json!([])
                    } else {
                        Value::Null
                    };
                    return Ok(Some(json!({"config":config,"origins":{},"layers":layers})));
                }
                let service = self.plugin_service(p["cwd"].as_str());
                let output = self.connections[&client].output.clone();
                let rpc_id = rpc_id.clone();
                let include_layers = p["includeLayers"] == true;
                tokio::spawn(async move {
                    let reply = match service.enabled_config().await {
                        Ok(plugins) => {
                            config.insert("plugins".into(), plugins);
                            response(
                                rpc_id,
                                json!({"config":config,"origins":{},"layers":if include_layers {json!([])} else {Value::Null}}),
                            )
                        }
                        Err(error) => error.response(rpc_id),
                    };
                    let _ = output.send(reply).await;
                });
                Ok(None)
            }
            "configRequirements/read" => Ok(Some(if self.acp() {
                json!({"requirements": {
                    "allowedSandboxModes": ["danger-full-access"],
                    "allowedApprovalPolicies": ["on-request", "never"],
                    "allowedApprovalsReviewers": if self.config.reviewer.is_some() {
                        json!(["user", "auto_review", "guardian_subagent"])
                    } else {
                        json!(["user"])
                    }
                }})
            } else {
                json!({"requirements": {
                    "allowedSandboxModes": ["workspace-write", "danger-full-access"],
                    "allowedApprovalPolicies": ["on-request", "never"],
                    "allowedApprovalsReviewers": ["user", "auto_review", "guardian_subagent"]
                }})
            })),
            // Advertise no Codex-only modes/features. Claude's native features
            // are not interchangeable with these desktop capabilities.
            "collaborationMode/list" => Ok(Some(json!({"data": []}))),
            "experimentalFeature/list" => {
                supported_fields(p, &["cursor", "limit"])?;
                let mut result = page(vec![], p, false, "features")?;
                result.as_object_mut().unwrap().remove("backwardsCursor");
                Ok(Some(result))
            }
            _ => Err(RpcError::unsupported(method)),
        }
    }

    fn list_threads(&self, p: &Value) -> RpcResult<Value> {
        supported_fields(
            p,
            &[
                "cursor",
                "limit",
                "sortKey",
                "sortDirection",
                "modelProviders",
                "sourceKinds",
                "archived",
                "projectId",
                "cwd",
                "searchTerm",
                "useStateDbOnly",
                "ancestorThreadId",
                "parentThreadId",
                "sectionId",
                "excludedThreadIds",
                "attachedToThreadId",
                "includeAttachedThreads",
                "currentAccountOnly",
            ],
        )?;
        let excluded = match &p["excludedThreadIds"] {
            Value::Null => vec![],
            Value::Array(ids) if ids.len() <= 100 && ids.iter().all(Value::is_string) => {
                ids.clone()
            }
            _ => {
                return Err(RpcError::invalid(
                    "excludedThreadIds must list at most 100 thread ids",
                ));
            }
        };
        let include_attached = p["includeAttachedThreads"] == true;
        if include_attached
            && p["searchTerm"]
                .as_str()
                .is_none_or(|term| term.trim().is_empty())
        {
            return Err(RpcError::new(
                -32600,
                "includeAttachedThreads requires a non-empty searchTerm",
            ));
        }
        let relations = ["parentThreadId", "ancestorThreadId", "attachedToThreadId"];
        if relations.iter().filter(|key| !p[**key].is_null()).count() > 1 {
            return Err(RpcError::invalid(
                "parentThreadId, ancestorThreadId and attachedToThreadId are mutually exclusive",
            ));
        }
        let relation = match (p["parentThreadId"].as_str(), p["ancestorThreadId"].as_str()) {
            (Some(parent), _) => Some((parent, false)),
            (None, Some(ancestor)) => Some((ancestor, true)),
            (None, None) => None,
        };
        // None orders a section by its own positions, as Codex does.
        let sort_key = match p["sortKey"].as_str().unwrap_or("created_at") {
            "created_at" => Some("createdAt"),
            "updated_at" => Some("updatedAt"),
            "recency_at" => Some("recencyAt"),
            "section_position" if p["sectionId"].is_string() => None,
            "section_position" => {
                return Err(RpcError::invalid(
                    "section-position sorting requires a section filter",
                ));
            }
            _ => return Err(RpcError::invalid("Unsupported thread sortKey")),
        };
        let mut records: Vec<&Record> = self
            .records
            .values()
            .filter(|r| {
                let related = relation
                    .is_none_or(|(ancestor, any_depth)| self.descends(r, ancestor, any_depth));
                // As in Codex: a relation filter without sourceKinds spans every
                // source; otherwise the default is interactive (non-subagent) threads.
                let source = match (relation, p["sourceKinds"].as_array()) {
                    (_, Some(kinds)) if !kinds.is_empty() => kinds.iter().any(|kind| {
                        kind.as_str()
                            .is_some_and(|kind| crate::subagents::source_matches(r, kind))
                    }),
                    (Some(_), None) => true,
                    _ => !crate::subagents::is_child(r),
                };
                let attached = match p["attachedToThreadId"].as_str() {
                    Some(parent) => r.attached_to.as_deref() == Some(parent),
                    None => r.attached_to.is_none() || (include_attached && relation.is_none()),
                };
                related
                    && source
                    && attached
                    && !excluded.contains(&r.thread["id"])
                    && r.archived == p["archived"].as_bool().unwrap_or(false)
                    && !r.ephemeral()
                    && p.get("projectId")
                        .is_none_or(|v| *v == r.thread["projectId"])
                    // Present and null selects unsectioned threads.
                    && p.get("sectionId")
                        .is_none_or(|v| *v == r.thread["section"]["id"])
                    && p["modelProviders"].as_array().is_none_or(|v| {
                        v.is_empty() || v.iter().any(|p| *p == protocol::provider().id.as_str())
                    })
                    && (p["cwd"].is_null()
                        || p["cwd"] == r.thread["cwd"]
                        || p["cwd"]
                            .as_array()
                            .is_some_and(|v| v.contains(&r.thread["cwd"])))
                    && p["searchTerm"].as_str().is_none_or(|s| {
                        r.thread["name"]
                            .as_str()
                            .unwrap_or_else(|| r.thread["preview"].as_str().unwrap_or(""))
                            .to_lowercase()
                            .contains(&s.to_lowercase())
                    })
            })
            .collect();
        records.sort_by_key(|r| {
            let value = match sort_key {
                Some(key) => r.thread[key].as_u64().unwrap_or(0),
                None => r
                    .section_position
                    .and_then(|position| u64::try_from(position).ok())
                    .unwrap_or(0),
            };
            (value, r.id().to_owned())
        });
        let threads = records
            .into_iter()
            .map(|r| r.view(false, self.loaded.contains(r.id())))
            .collect();
        // Timestamps default to newest first; section positions to first placed.
        let mut result = page(threads, p, sort_key.is_some(), "threads")?;
        // As in Codex: receipts for the eligible threads on this page only.
        let read_states: serde_json::Map<String, Value> = result["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|thread| {
                let id = thread["id"].as_str()?;
                Some((id.to_owned(), self.records.get(id)?.read_state()?))
            })
            .collect();
        result["readStates"] = json!(read_states);
        Ok(result)
    }

    /// Whether `record` is a (transitive, when `any_depth`) child of `ancestor`.
    fn descends(&self, record: &Record, ancestor: &str, any_depth: bool) -> bool {
        let mut parent = record.thread["parentThreadId"].as_str();
        while let Some(id) = parent {
            if id == ancestor {
                return true;
            }
            if !any_depth {
                return false;
            }
            parent = self
                .records
                .get(id)
                .and_then(|r| r.thread["parentThreadId"].as_str());
        }
        false
    }

    /// Codex's `thread/delete`: the thread and its subagent descendants,
    /// deepest first, each announced to every client. Claude's own session
    /// transcripts under `~/.claude` are the user's data and are kept.
    async fn delete_thread(&mut self, client: u64, rpc_id: &Value, id: &str) -> RpcResult<()> {
        if self.record(id)?.ephemeral() {
            return Err(RpcError::invalid(format!(
                "Thread is not persisted and cannot be deleted: {id}"
            )));
        }
        // As in Codex; the desktop deletes a source's side chats first.
        if self
            .records
            .values()
            .any(|record| record.attached_to.as_deref() == Some(id))
        {
            return Err(RpcError::invalid(
                "Delete this thread's attached threads before deleting it",
            ));
        }
        let subtree = self.subtree(id);
        // Codex shuts a running thread down first. A subagent's work runs in
        // its root's turn, which cannot stop for one subagent alone.
        let in_use = self.active.iter().any(|(root, active)| {
            root.as_str() != id
                && active.agents.runs.values().any(|run| {
                    run.thread_id
                        .as_ref()
                        .is_some_and(|thread| subtree.contains(thread))
                })
        });
        if in_use {
            return Err(RpcError::invalid(
                "A running turn on the parent thread still uses this subagent thread; interrupt it first",
            ));
        }
        if self.active.contains_key(id) {
            self.finish(id, "interrupted", Some("Thread was deleted".into()))
                .await;
        }
        let order: Vec<String> = subtree[1..]
            .iter()
            .rev()
            .chain(&subtree[..1])
            .cloned()
            .collect();
        let mut deleted = Vec::new();
        let mut failure = None;
        for thread in order {
            if let Err(error) = self.store.delete(&thread) {
                failure = Some(RpcError::internal(format!(
                    "Cannot delete thread {thread}: {error:#}"
                )));
                break;
            }
            self.forget_thread(&thread);
            deleted.push(thread);
        }
        self.forget_agent_threads(&deleted);
        if let Some(error) = failure {
            self.announce_deleted(&deleted);
            return Err(error);
        }
        self.send(client, response(rpc_id.clone(), json!({})));
        self.announce_deleted(&deleted);
        Ok(())
    }

    /// `id` followed by its subagent descendants, breadth first.
    fn subtree(&self, id: &str) -> Vec<String> {
        let mut subtree = vec![id.to_owned()];
        let mut next = 0;
        while let Some(parent) = subtree.get(next).cloned() {
            let children: Vec<String> = self
                .records
                .values()
                .filter(|record| record.thread["parentThreadId"] == parent.as_str())
                .map(|record| record.id().to_owned())
                .collect();
            subtree.extend(children);
            next += 1;
        }
        subtree
    }

    /// Drop a deleted thread's live state: its idle Claude process, record,
    /// pending save, subscriptions and loaded status.
    fn forget_thread(&mut self, id: &str) {
        self.close_idle_session(id);
        self.records.remove(id);
        self.unsaved.remove(id);
        self.loaded.remove(id);
        for connection in self.connections.values_mut() {
            connection.subscriptions.remove(id);
        }
    }

    /// A later continuation of a deleted subagent must not resume its thread.
    fn forget_agent_threads(&mut self, deleted: &[String]) {
        let owners: Vec<String> = self
            .records
            .values()
            .filter(|record| {
                record
                    .agents
                    .values()
                    .any(|thread| deleted.contains(thread))
            })
            .map(|record| record.id().to_owned())
            .collect();
        for owner in owners {
            if let Some(record) = self.records.get_mut(&owner) {
                record.agents.retain(|_, thread| !deleted.contains(thread));
            }
            self.persist(&owner);
        }
    }

    /// Codex announces deletions to every client, not only subscribers.
    fn announce_deleted(&self, ids: &[String]) {
        for id in ids {
            let message = notification("thread/deleted", json!({"threadId": id}));
            for (&client, connection) in &self.connections {
                if connection.ready && !connection.opt_out.contains("thread/deleted") {
                    self.send(client, message.clone());
                }
            }
        }
    }

    /// Codex's `thread/section/move`. Sections belong to genuine Codex, so the
    /// router finds the destination there and passes it as `claudexSection`.
    fn move_to_section(&mut self, p: &Value) -> RpcResult<Value> {
        supported_fields(
            p,
            &["threadId", "sectionId", "beforeThreadId", "claudexSection"],
        )?;
        let id = required_str(p, "threadId")?;
        let section_id = p["sectionId"].as_str();
        let before = p["beforeThreadId"].as_str();
        if section_id.is_some_and(|section| section.trim().is_empty()) {
            return Err(RpcError::invalid("sectionId must not be empty"));
        }
        if section_id.is_none() && before.is_some() {
            return Err(RpcError::invalid(
                "beforeThreadId requires a non-null sectionId",
            ));
        }
        let mut record = self.record(id)?.clone();
        if record.ephemeral() {
            return Err(RpcError::invalid(format!(
                "Ephemeral thread does not support section moves: {id}"
            )));
        }
        let Some(section_id) = section_id else {
            record.thread["section"] = Value::Null;
            record.thread["sectionEnteredAt"] = Value::Null;
            record.section_position = None;
            self.store.save(&record).map_err(RpcError::internal)?;
            self.records.insert(id.to_owned(), record);
            return Ok(json!({}));
        };
        let section = &p["claudexSection"];
        if section["id"] != section_id || !section["name"].is_string() {
            return Err(RpcError::invalid(
                "Thread sections belong to genuine Codex; move Claude threads through the desktop router",
            ));
        }
        if before == Some(id) {
            return Err(RpcError::invalid(format!(
                "Thread {id} cannot be moved before itself"
            )));
        }
        let mut members: Vec<&Record> = self
            .records
            .values()
            .filter(|member| member.id() != id && member.thread["section"]["id"] == section_id)
            .collect();
        members.sort_by_key(|member| (member.section_position, member.id().to_owned()));
        let mut order: Vec<String> = members
            .iter()
            .map(|member| member.id().to_owned())
            .collect();
        let index = match before {
            Some(before) => order
                .iter()
                .position(|member| member == before)
                .ok_or_else(|| {
                    RpcError::invalid(format!(
                        "Before thread {before} is not in section {section_id}"
                    ))
                })?,
            None => order.len(),
        };
        order.insert(index, id.to_owned());
        if record.thread["section"]["id"] != section_id {
            record.thread["section"] = section.clone();
            record.thread["sectionEnteredAt"] = json!(protocol::now());
        }
        // Members are renumbered in order; only changed positions are saved.
        let mut changed = Vec::new();
        for (position, member) in order.into_iter().enumerate() {
            let position = i64::try_from(position).ok();
            if member == id {
                record.section_position = position;
            } else if self.records[&member].section_position != position {
                let mut updated = self.records[&member].clone();
                updated.section_position = position;
                changed.push(updated);
            }
        }
        changed.push(record);
        for updated in &changed {
            self.store.save(updated).map_err(RpcError::internal)?;
        }
        for updated in changed {
            self.records.insert(updated.id().to_owned(), updated);
        }
        Ok(json!({}))
    }

    /// The router reports a section genuine Codex renamed (`section`) or
    /// deleted (`section: null`). As in Codex, deletion unsections its threads.
    fn section_changed(&mut self, p: &Value) -> RpcResult<Option<Value>> {
        supported_fields(p, &["sectionId", "section"])?;
        let section_id = required_str(p, "sectionId")?;
        let section = &p["section"];
        if !section.is_null() && section["id"] != section_id {
            return Err(RpcError::invalid("section does not match sectionId"));
        }
        let members: Vec<String> = self
            .records
            .values()
            .filter(|record| record.thread["section"]["id"] == section_id)
            .map(|record| record.id().to_owned())
            .collect();
        for id in members {
            let mut record = self.records[&id].clone();
            if section.is_null() {
                record.thread["section"] = Value::Null;
                record.thread["sectionEnteredAt"] = Value::Null;
                record.section_position = None;
            } else {
                record.thread["section"] = section.clone();
            }
            self.store.save(&record).map_err(RpcError::internal)?;
            self.records.insert(id, record);
        }
        Ok(Some(json!({})))
    }

    async fn steer_turn(&mut self, client: u64, rpc_id: &Value, p: &Value) -> RpcResult<()> {
        supported_fields(
            p,
            &[
                "threadId",
                "expectedTurnId",
                "input",
                "clientUserMessageId",
                "additionalContext",
                "responsesapiClientMetadata",
                "responsesapiClientMetadataPatch",
            ],
        )?;
        let id = required_str(p, "threadId")?;
        let expected = required_str(p, "expectedTurnId")?;
        let active = self
            .active
            .get(id)
            .ok_or_else(|| RpcError::invalid("Thread has no active turn to steer"))?;
        if active.id != expected {
            return Err(RpcError::invalid(
                "expectedTurnId does not match the active turn",
            ));
        }
        if active.owner != Some(client) || !self.connections[&client].subscriptions.contains(id) {
            return Err(RpcError::invalid(
                "Only the active turn's requesting client may steer it",
            ));
        }
        if active.interrupting {
            return Err(RpcError::invalid("Cannot steer an interrupting turn"));
        }
        if active.compaction {
            return Err(compaction_not_steerable());
        }
        if active.prediction_parent.is_some() {
            return Err(RpcError::invalid("Prediction turns cannot be steered"));
        }
        if let Some(client_id) = p["clientUserMessageId"].as_str() {
            let items = self.records[id].turns.last().unwrap()["items"]
                .as_array()
                .unwrap();
            if let Some(item) = items
                .iter()
                .find(|v| v["type"] == "userMessage" && v["clientId"] == client_id)
            {
                let (input, _) = translate::user_input(&p["input"]).await?;
                if item["content"] != json!(input) {
                    return Err(RpcError::invalid(
                        "clientUserMessageId already identifies different input",
                    ));
                }
                self.send(client, response(rpc_id.clone(), json!({"turnId":expected})));
                return Ok(());
            }
        }
        let (input, mut claude_input) = translate::user_input(&p["input"]).await?;
        append_additional_context(&mut claude_input, &p["additionalContext"]);
        let item_id = protocol::id();
        let message = json!({"type":"user","uuid":item_id,"session_id":self.records[id].session_id,
            "parent_tool_use_id":null,"message":{"role":"user","content":claude_input}});
        let item = json!({"type":"userMessage","id":item_id,"clientId":p["clientUserMessageId"],"content":input});
        let mut record = self.records[id].clone();
        record.update_item(item.clone(), true);
        record.touch();
        let control = self.active[id].control.clone();
        let admission = self.active[id].input_admission.clone();
        {
            let permit = control
                .try_reserve()
                .map_err(|_| RpcError::invalid("Active turn is no longer accepting input"))?;
            let mut admission = admission.lock().unwrap();
            if admission.closed {
                return Err(RpcError::invalid(
                    "Active turn is no longer accepting input",
                ));
            }
            // Admission and finalization share this short, synchronous critical
            // section. Never await backend I/O while owning the server actor: a
            // full backend event channel would otherwise deadlock steering.
            self.store.save(&record).map_err(RpcError::internal)?;
            admission.queued += 1;
            permit.send(Control::UserInput(message));
        }
        self.records.insert(id.to_owned(), record);
        self.send(client, response(rpc_id.clone(), json!({"turnId":expected})));
        for method in ["item/started", "item/completed"] {
            let mut params = json!({"threadId":id,"turnId":expected,"item":item});
            params[if method == "item/started" {
                "startedAtMs"
            } else {
                "completedAtMs"
            }] = json!(now_ms());
            self.emit(id, notification(method, params));
        }
        Ok(())
    }

    async fn queue_request(
        &mut self,
        client: u64,
        rpc_id: &Value,
        method: &str,
        p: &Value,
    ) -> RpcResult<Option<Value>> {
        let id = required_str(p, "threadId")?;
        let mut record = self.record(id)?.clone();
        if method == "thread/queue/list" {
            supported_fields(p, &["threadId", "cursor", "limit"])?;
            let mut result = page(record.queued_submissions, p, false, &format!("queue:{id}"))?;
            result.as_object_mut().unwrap().remove("backwardsCursor");
            return Ok(Some(result));
        }
        if !self.connections[&client].subscriptions.contains(id) {
            return Err(RpcError::invalid(
                "Resume the thread before changing its queue",
            ));
        }
        if record.archived {
            return Err(RpcError::invalid(
                "Unarchive the thread before changing its queue",
            ));
        }
        let result = match method {
            "thread/queue/add" => {
                supported_fields(p, &["threadId", "input", "clientUserMessageId"])?;
                let client_id = required_str(p, "clientUserMessageId")?;
                let (input, _) = translate::user_input(&p["input"]).await?;
                if let Some(existing) = record
                    .queued_submissions
                    .iter()
                    .find(|v| v["clientUserMessageId"] == client_id)
                {
                    if existing["input"] != json!(input) {
                        return Err(RpcError::invalid(
                            "clientUserMessageId already identifies different queued input",
                        ));
                    }
                    return Ok(Some(json!({"queuedSubmission":existing})));
                }
                if record.turns.iter().any(|turn| {
                    turn["items"].as_array().is_some_and(|items| {
                        items.iter().any(|item| {
                            item["type"] == "userMessage" && item["clientId"] == client_id
                        })
                    })
                }) {
                    return Err(RpcError::invalid(
                        "This clientUserMessageId has already been submitted",
                    ));
                }
                let entry =
                    json!({"id":protocol::id(),"clientUserMessageId":client_id,"input":input});
                record.queued_submissions.push(entry.clone());
                json!({"queuedSubmission":entry})
            }
            "thread/queue/update" => {
                supported_fields(p, &["threadId", "queuedSubmissionId", "input"])?;
                let queued_id = required_str(p, "queuedSubmissionId")?;
                let (input, _) = translate::user_input(&p["input"]).await?;
                let entry = record
                    .queued_submissions
                    .iter_mut()
                    .find(|v| v["id"] == queued_id)
                    .ok_or_else(|| RpcError::invalid("Queued submission not found"))?;
                entry["input"] = json!(input);
                json!({"queuedSubmission":entry})
            }
            "thread/queue/delete" => {
                supported_fields(p, &["threadId", "queuedSubmissionId"])?;
                let queued_id = required_str(p, "queuedSubmissionId")?;
                let count = record.queued_submissions.len();
                record.queued_submissions.retain(|v| v["id"] != queued_id);
                if count == record.queued_submissions.len() {
                    return Ok(Some(json!({"deleted":false})));
                }
                json!({"deleted":true})
            }
            "thread/queue/reorder" => {
                supported_fields(p, &["threadId", "queuedSubmissionIds"])?;
                let ids = p["queuedSubmissionIds"]
                    .as_array()
                    .ok_or_else(|| RpcError::invalid("queuedSubmissionIds must be an array"))?;
                let mut entries: HashMap<String, Value> = record
                    .queued_submissions
                    .iter()
                    .map(|v| (v["id"].as_str().unwrap().to_owned(), v.clone()))
                    .collect();
                let mut ordered = Vec::new();
                for queued_id in ids {
                    let entry = queued_id
                        .as_str()
                        .and_then(|id| entries.remove(id))
                        .ok_or_else(|| {
                            RpcError::invalid("Queue order contains an unknown or duplicate id")
                        })?;
                    ordered.push(entry);
                }
                if !entries.is_empty() {
                    return Err(RpcError::invalid(
                        "Queue order must include every queued submission",
                    ));
                }
                record.queued_submissions = ordered;
                json!({})
            }
            "thread/queue/start" => {
                supported_fields(p, &["threadId", "queuedSubmissionId"])?;
                let entry = if let Some(queued_id) = p["queuedSubmissionId"].as_str() {
                    record
                        .queued_submissions
                        .iter()
                        .find(|v| v["id"] == queued_id)
                } else {
                    record.queued_submissions.first()
                }
                .ok_or_else(|| RpcError::invalid("Queued submission not found"))?;
                let params = json!({"threadId":id,"input":entry["input"],
                    "clientUserMessageId":entry["clientUserMessageId"]});
                let request = TurnRequest::Input {
                    queued_submission_id: entry["id"].as_str(),
                };
                self.start_turn(client, rpc_id, &params, request).await?;
                return Ok(None);
            }
            _ => return Err(RpcError::unsupported(method)),
        };
        record.touch();
        self.store.save(&record).map_err(RpcError::internal)?;
        self.records.insert(id.to_owned(), record);
        self.send(client, response(rpc_id.clone(), result));
        self.emit(
            id,
            notification("thread/queue/changed", json!({"threadId":id})),
        );
        Ok(None)
    }

    async fn start_turn(
        &mut self,
        client: u64,
        rpc_id: &Value,
        p: &Value,
        request: TurnRequest<'_>,
    ) -> RpcResult<()> {
        supported_fields(
            p,
            &[
                "threadId",
                "input",
                "clientUserMessageId",
                "cwd",
                "model",
                "approvalPolicy",
                "approvalsReviewer",
                "sandboxPolicy",
                "effort",
                "serviceTier",
                "serviceTierForTurn",
                "summary",
                "outputSchema",
                "developerInstructions",
                "additionalContext",
                "environmentOverrides",
                "disabledPluginIds",
                "parentTurnId",
                "rootTurnId",
            ],
        )?;
        let id = required_str(p, "threadId")?;
        self.idle(id)?;
        if !self.connections[&client].subscriptions.contains(id) {
            return Err(RpcError::invalid(
                "Resume the thread before starting a turn",
            ));
        }
        let mut record = self.record(id)?.clone();
        if record.archived {
            return Err(RpcError::invalid(
                "Unarchive the thread before starting a turn",
            ));
        }
        if crate::subagents::is_child(&record) {
            return Err(subagent_read_only());
        }
        if record.prediction_mode && record.has_session {
            return Err(RpcError::invalid(
                "Prediction forks support one turn; create a new prediction fork",
            ));
        }
        let compaction = matches!(request, TurnRequest::Compaction);
        if compaction {
            if self.acp() {
                return Err(RpcError::invalid(
                    "ACP agents have no request for manual compaction",
                ));
            }
            // A fork's first turn compacts the history it was forked from.
            if !record.has_session && record.fork_from.is_none() {
                return Err(RpcError::invalid(
                    "This thread has no history to compact yet",
                ));
            }
        }
        if record.prediction_mode {
            supported_fields(
                p,
                &[
                    "threadId",
                    "input",
                    "clientUserMessageId",
                    "effort",
                    "outputSchema",
                ],
            )?;
            if p["input"].as_array().is_none_or(|input| {
                input.is_empty() || input.iter().any(|part| part["type"] != "text")
            }) {
                return Err(RpcError::invalid("Prediction input must be nonempty text"));
            }
            if compaction {
                return Err(RpcError::invalid("Prediction turns cannot compact history"));
            }
            // Keep the parent's native prefix and effort. The desktop's optional
            // effort override cannot be applied without invalidating that cache.
        } else {
            record.apply_settings(p)?;
            self.check_backend_settings(&record.settings)?;
        }
        // A per-turn tier, including "default", leaves the thread's tier unchanged.
        let fast_mode = match p.get("serviceTierForTurn").filter(|v| !v.is_null()) {
            Some(tier) => crate::store::service_tier(tier)?.is_some(),
            None => record.settings.service_tier.is_some(),
        };
        if self.acp() {
            if fast_mode {
                return Err(acp_fast_mode());
            }
            if p.get("outputSchema")
                .is_some_and(|schema| !schema.is_null())
            {
                return Err(RpcError::invalid(
                    "ACP agents do not support structured output schemas",
                ));
            }
            if !record.has_session && record.fork_from.is_some() {
                if self.acp_lacks("fork") {
                    return Err(RpcError::invalid(
                        "This ACP agent does not support forking sessions",
                    ));
                }
                // session/fork copies the source as it is now, so the fork is exact
                // only while the source has not advanced since thread/fork.
                if let Some(source) = record.thread["forkedFromId"]
                    .as_str()
                    .and_then(|source| self.records.get(source))
                    && source.backend_message_id != record.backend_message_id
                {
                    return Err(RpcError::invalid(
                        "The source thread continued after this fork; fork it again from its latest turn",
                    ));
                }
            }
        }
        let options = self.session_options(
            &record,
            p.get("outputSchema").filter(|v| !v.is_null()).cloned(),
            fast_mode,
        )?;
        let launch_key = crate::warm::launch_key(&options).filter(|_| {
            !record.prediction_mode
                && (record.ephemeral()
                    || (self.config.max_idle_sessions > 0
                        && !self.config.idle_session_timeout.is_zero()))
        });
        let (codex_input, mut claude_input) = match request {
            TurnRequest::Input { .. } => translate::user_input(&p["input"]).await?,
            TurnRequest::Compaction => (vec![], vec![json!({"type":"text","text":"/compact"})]),
        };
        let needs_ephemeral_process = record.ephemeral() && record.has_session;
        if needs_ephemeral_process {
            let parked = self
                .idle_sessions
                .get(id)
                .filter(|p| p.is_open())
                .ok_or_else(|| {
                    RpcError::invalid(
                        "This temporary chat's Claude process ended; open a new side chat",
                    )
                })?;
            if let Some(reason) = crate::warm::mismatch(parked, &options, launch_key.as_ref()) {
                return Err(RpcError::invalid(format!(
                    "Cannot restart a temporary chat without losing its history: {reason}"
                )));
            }
        }
        record.tracks_turn_anchors = true;
        // Claude recognizes `/compact` only as the whole message, so context
        // queued while the thread was idle waits for the next user turn.
        if !compaction {
            append_additional_context(&mut claude_input, &p["additionalContext"]);
            // Context queued while the thread was idle precedes the new input.
            for text in std::mem::take(&mut record.pending_context)
                .into_iter()
                .rev()
            {
                claude_input.insert(0, json!({"type":"text","text":text}));
            }
        }
        let turn_id = protocol::id();
        // `parentTurnId` is provenance only; the root is what turns record.
        let turn = protocol::new_turn(&turn_id, p["rootTurnId"].as_str());
        // As in Codex, a compaction turn has no user message.
        let item = (!compaction).then(|| json!({"type": "userMessage", "id": protocol::id(), "clientId": p["clientUserMessageId"], "content": codex_input}));
        if record.thread["preview"] == "" {
            record.thread["preview"] = json!(
                codex_input
                    .iter()
                    .filter_map(|v| v["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
                    .chars()
                    .take(200)
                    .collect::<String>()
            );
        }
        record.thread["status"] = json!({"type": "active", "activeFlags": []});
        record.turns.push(turn.clone());
        if let Some(item) = &item {
            record.update_item(item.clone(), true);
        }
        let queued_submission_id = match request {
            TurnRequest::Input {
                queued_submission_id,
            } => queued_submission_id,
            TurnRequest::Compaction => None,
        };
        // Consuming a queued message and recording its turn share one durable write.
        if let Some(queued_id) = queued_submission_id {
            record
                .queued_submissions
                .retain(|entry| entry["id"] != queued_id);
        }
        record.touch();
        self.store.save(&record).map_err(RpcError::internal)?;
        let warm = self.reclaim(id, &options, launch_key.as_ref()).await;
        if needs_ephemeral_process && warm.is_none() {
            return Err(RpcError::invalid(
                "This temporary chat's Claude process ended; open a new side chat",
            ));
        }
        self.spawn_turn(
            &record,
            LaunchedTurn {
                turn_id: turn_id.clone(),
                output_schema: p.get("outputSchema").filter(|v| !v.is_null()).cloned(),
                options,
                warm,
                launch_key,
                input: claude_input,
                owner: Some(client),
                fast_mode,
                compaction,
            },
        );
        self.records.insert(id.to_owned(), record);
        // thread/compact/start answers `{}` and reports its turn by notification.
        let result = if compaction {
            json!({})
        } else {
            json!({"turn": turn})
        };
        self.send(client, response(rpc_id.clone(), result));
        if queued_submission_id.is_some() {
            self.emit(
                id,
                notification("thread/queue/changed", json!({"threadId":id})),
            );
        }
        self.emit(
            id,
            notification(
                "thread/status/changed",
                json!({"threadId": id, "status": {"type": "active", "activeFlags": []}}),
            ),
        );
        self.emit(
            id,
            notification("turn/started", json!({"threadId": id, "turn": turn})),
        );
        if let Some(item) = item {
            self.emit(
                id,
                notification(
                    "item/started",
                    json!({"threadId": id, "turnId": turn_id, "item": item, "startedAtMs": now_ms()}),
                ),
            );
            self.emit(
                id,
                notification(
                    "item/completed",
                    json!({"threadId": id, "turnId": turn_id, "item": item, "completedAtMs": now_ms()}),
                ),
            );
        }
        Ok(())
    }

    fn translated(&mut self, id: &str, events: Vec<Value>) {
        for event in events {
            let method = event["method"].as_str().unwrap_or("");
            if method == "item/started" || method == "item/completed" {
                self.records
                    .get_mut(id)
                    .unwrap()
                    .update_item(event["params"]["item"].clone(), method == "item/completed");
            }
            self.emit(id, event);
        }
        // Keep hydration current between item start and completion, including
        // text received while no transport is subscribed.
        if let Some(active) = self.active.get(id) {
            let record = self.records.get_mut(id).unwrap();
            for item in active.translator.snapshot() {
                record.update_item(item.clone(), false);
            }
        }
    }

    async fn backend_event(&mut self, id: &str, turn_id: &str, event: BackendEvent) {
        if self.active.get(id).is_none_or(|a| a.id != turn_id) {
            return;
        }
        match event {
            BackendEvent::Exited { success, message } => {
                let interrupted = self.active[id].interrupting;
                self.finish(
                    id,
                    if interrupted { "interrupted" } else { "failed" },
                    Some(if success {
                        "Claude exited without a result message".into()
                    } else {
                        message
                    }),
                )
                .await;
            }
            BackendEvent::Message(message) => {
                if message["type"] == "control_request" {
                    self.permission(id, &message).await;
                    return;
                }
                if self.route_agent(id, &message) {
                    return;
                }
                if !message["parent_tool_use_id"].is_null() {
                    // Subagent frames never anchor the parent session. Without a linked
                    // child thread, the translator only completes items that a subagent
                    // permission request started.
                    let translated = self
                        .active
                        .get_mut(id)
                        .unwrap()
                        .translator
                        .receive(&message);
                    if let Ok(events) = translated {
                        self.translated(id, events);
                    }
                    return;
                }
                if message["type"] == "control_cancel_request" {
                    let request_ids: Vec<String> = self
                        .pending
                        .iter()
                        .filter(|(_, p)| {
                            p.thread_id == id && p.claude_request_id == message["request_id"]
                        })
                        .map(|(id, _)| id.clone())
                        .collect();
                    for request_id in request_ids {
                        let pending = self.pending.remove(&request_id).unwrap();
                        if pending.dynamic.is_some() {
                            self.complete_dynamic(
                                &request_id,
                                pending,
                                crate::dynamic_tools::failure("Claude cancelled the tool call"),
                            );
                            continue;
                        }
                        self.emit_resolved(&pending, json!(request_id));
                    }
                    self.update_wait_status(id);
                    return;
                }
                let record = &self.records[id];
                let identity = (record.session_id.clone(), record.has_session);
                if let Some(session_id) = message["session_id"].as_str() {
                    // A fork can receive a fresh backend id; facade thread ids remain stable.
                    self.records.get_mut(id).unwrap().session_id = session_id.to_owned();
                }
                if message["type"] == "system"
                    && message["subtype"] == "init"
                    && message["acp"].is_object()
                {
                    self.acp_agent = Some(message["acp"].clone());
                }
                if message["type"] == "assistant"
                    || (message["type"] == "result" && message["subtype"] == "success")
                {
                    self.records.get_mut(id).unwrap().has_session = true;
                }
                if (message["type"] == "assistant" || message["type"] == "user")
                    && let Some(uuid) = message["uuid"].as_str()
                {
                    let record = self.records.get_mut(id).unwrap();
                    record.backend_message_id = Some(uuid.to_owned());
                    record.turn_anchors.insert(
                        turn_id.into(),
                        crate::history_recovery::NativeAnchor {
                            session_id: record.session_id.clone(),
                            message_id: uuid.into(),
                        },
                    );
                }
                if message["type"] == "user" {
                    self.agent_results(id, None, &message);
                }
                let translated = self
                    .active
                    .get_mut(id)
                    .unwrap()
                    .translator
                    .receive(&message);
                match translated {
                    Ok(events) => self.translated(id, events),
                    Err(error) => {
                        self.finish(id, "failed", Some(error.message)).await;
                        return;
                    }
                }
                self.link_spawns(id, None);
                if let Some(last) = self.active.get_mut(id).unwrap().usage.receive(&message) {
                    let window = self.active[id].usage.context_window(&Value::Null);
                    let record = self.records.get_mut(id).unwrap();
                    record.tracks_request_usage = true;
                    record.token_usage["last"] = last;
                    if let Some(window) = window {
                        record.token_usage["modelContextWindow"] = json!(window);
                    }
                    self.emit(id, notification("thread/tokenUsage/updated", json!({"threadId":id,"turnId":turn_id,"tokenUsage":self.records[id].token_usage})));
                }
                if message["type"] == "result" {
                    self.record_usage(id, turn_id, &message);
                    // Claude falls back to standard speed instead of failing,
                    // for example when the account has no extra usage.
                    if self.active[id].fast_mode
                        && let Some(state) = message["fast_mode_state"]
                            .as_str()
                            .filter(|state| *state != "on")
                    {
                        let reason = message["fast_mode_disabled_reason"]
                            .as_str()
                            .unwrap_or(state);
                        self.emit(id, notification("warning", json!({"threadId":id,
                            "message":format!("Claude fast mode was requested but this turn ran at standard speed ({reason}).")})));
                    }
                    let interrupted = self.active[id].interrupting;
                    let failed = message["is_error"] == true
                        || message["subtype"].as_str().is_some_and(|s| s != "success");
                    let error = if failed {
                        Some(
                            message["errors"]
                                .as_array()
                                .map(|e| {
                                    e.iter()
                                        .filter_map(Value::as_str)
                                        .collect::<Vec<_>>()
                                        .join("; ")
                                })
                                .filter(|s| !s.is_empty())
                                .or_else(|| message["result"].as_str().map(str::to_owned))
                                .unwrap_or_else(|| {
                                    format!("Claude turn failed: {}", message["subtype"])
                                }),
                        )
                    } else {
                        None
                    };
                    self.finish(
                        id,
                        if interrupted {
                            "interrupted"
                        } else if failed {
                            "failed"
                        } else {
                            "completed"
                        },
                        error,
                    )
                    .await;
                } else if message["type"] == "assistant" || message["type"] == "user" {
                    // Which native session to resume must survive a crash at once.
                    let record = &self.records[id];
                    if identity != (record.session_id.clone(), record.has_session) {
                        self.persist_now(id);
                    } else {
                        self.persist_soon(id);
                    }
                }
            }
        }
    }

    fn record_usage(&mut self, id: &str, turn_id: &str, result: &Value) {
        let u = &result["usage"];
        if !u.is_object() {
            return;
        }
        let total = crate::usage::from_native(u);
        let window = self.active[id].usage.context_window(result);
        let record = self.records.get_mut(id).unwrap();
        for (key, value) in total.as_object().unwrap() {
            record.token_usage["total"][key] = json!(
                record.token_usage["total"][key]
                    .as_u64()
                    .unwrap_or(0)
                    .saturating_add(value.as_u64().unwrap_or(0))
            );
        }
        if let Some(window) = window {
            record.token_usage["modelContextWindow"] = json!(window);
        }
        let token_usage = record.token_usage.clone();
        self.emit(
            id,
            notification(
                "thread/tokenUsage/updated",
                json!({"threadId": id, "turnId": turn_id, "tokenUsage": token_usage}),
            ),
        );
    }

    async fn finish(&mut self, id: &str, status: &str, error: Option<String>) {
        // A background agent keeps its root turn open, so a child still running
        // here was cut short with its root.
        let open: Vec<String> = self.active.get(id).map_or(vec![], |active| {
            active
                .agents
                .runs
                .iter()
                .filter(|(_, run)| run.turn.is_some())
                .map(|(key, _)| key.clone())
                .collect()
        });
        let child_status = if status == "failed" {
            "failed"
        } else {
            "interrupted"
        };
        for key in open {
            self.finish_agent(id, &key, child_status, None, None);
        }
        let Some(mut active) = self.active.remove(id) else {
            return;
        };
        active.cancel.cancel();
        if let Some(runtime) = active.plugins.take() {
            tokio::spawn(async move { runtime.client.shutdown().await });
        }
        // A finished worker has already released its process; a cancelled one
        // only has to kill it.
        let _ = active.worker.await;
        let events = active.translator.finish();
        self.translated(id, events);
        for (_, mut item) in active.plugin_calls.drain() {
            item["status"] = json!("failed");
            item["error"] = json!({"message":"Turn ended before the plugin tool returned"});
            self.translated(
                id,
                vec![notification(
                    "item/completed",
                    json!({"threadId":id,"turnId":active.id,"item":item,"completedAtMs":now_ms()}),
                )],
            );
        }
        let reviews: Vec<String> = self
            .reviews
            .iter()
            .filter(|(_, review)| review.pending.thread_id == id)
            .map(|(key, _)| key.clone())
            .collect();
        for review_id in reviews {
            let review = self.reviews.remove(&review_id).unwrap();
            self.review_completed(
                &review_id,
                &review,
                json!({"status":"aborted","riskLevel":null,"userAuthorization":null,"rationale":null}),
            );
        }
        let pending: Vec<String> = self
            .pending
            .iter()
            .filter(|(_, p)| p.thread_id == id)
            .map(|(key, _)| key.clone())
            .collect();
        for request_id in pending {
            let pending = self.pending.remove(&request_id).unwrap();
            if pending.dynamic.is_some() {
                self.complete_dynamic(
                    &request_id,
                    pending,
                    crate::dynamic_tools::failure("Turn ended before the tool returned"),
                );
                continue;
            }
            self.emit_resolved(&pending, json!(request_id));
        }
        // The completed turn is saved below, with everything streamed before it.
        self.unsaved.remove(id);
        let record = self.records.get_mut(id).unwrap();
        let turn = record.turns.last_mut().unwrap();
        // As in Codex, a terminal result needs the user's attention when it
        // failed or answered; an interrupted turn only with output or an error.
        let final_text = turn["items"]
            .as_array()
            .and_then(|items| {
                items
                    .iter()
                    .rev()
                    .find(|item| item["type"] == "agentMessage")
            })
            .and_then(|item| item["text"].as_str())
            .map(str::to_owned);
        let visible = match status {
            "failed" => true,
            "completed" => final_text.as_deref().is_some_and(|text| {
                !text.trim().is_empty() && !text.contains("<decision>DONT_NOTIFY</decision>")
            }),
            _ => error.is_some() || final_text.is_some(),
        };
        turn["status"] = json!(status);
        turn["completedAt"] = json!(protocol::now());
        turn["durationMs"] = json!(now_ms().saturating_sub(active.started_ms));
        turn["error"] = error.map(|message| json!({"message": message, "codexErrorInfo": null, "additionalDetails": null, "misalignment": null})).unwrap_or(Value::Null);
        let attention = visible && record.publish_attention(&active.id);
        record.thread["status"] = json!({"type": "idle"});
        record.touch();
        if let Err(error) = self.store.save(record) {
            let turn = record.turns.last_mut().unwrap();
            turn["status"] = json!("failed");
            turn["error"] = json!({"message": format!("Cannot persist completed turn: {error}"), "codexErrorInfo": null, "additionalDetails": null, "misalignment": null});
        }
        let turn = record.timed_turn(record.turns.last().unwrap());
        let read_state = record.read_state().filter(|_| attention);
        if !self
            .connections
            .values()
            .any(|c| c.subscriptions.contains(id))
        {
            self.loaded.remove(id);
        }
        if turn["status"] == "failed" {
            self.emit(id, notification("error", json!({"threadId": id, "turnId": active.id, "error": turn["error"], "willRetry": false})));
        }
        self.emit(
            id,
            notification("turn/completed", json!({"threadId": id, "turn": turn})),
        );
        if let Some(read_state) = read_state {
            self.emit(
                id,
                notification(
                    "thread/readState/changed",
                    json!({"threadId": id, "readState": read_state}),
                ),
            );
        }
        self.emit(
            id,
            notification(
                "thread/status/changed",
                json!({"threadId": id, "status": {"type": "idle"}}),
            ),
        );
    }

    /// The translator that owns items for `owner`: the root thread's own, or a
    /// linked agent's open child turn.
    fn agent_translator(&mut self, root: &str, owner: Option<&str>) -> Option<&mut Translator> {
        let active = self.active.get_mut(root)?;
        match owner {
            None => Some(&mut active.translator),
            Some(key) => active
                .agents
                .runs
                .get_mut(key)?
                .turn
                .as_mut()
                .map(|turn| &mut turn.translator),
        }
    }

    fn owner_events(&mut self, root: &str, owner: Option<&str>, events: Vec<Value>) {
        match owner {
            None => self.translated(root, events),
            Some(key) => {
                let Some(run) = self.active.get(root).and_then(|a| a.agents.runs.get(key)) else {
                    return;
                };
                let Some(thread) = run.thread_id.clone() else {
                    return;
                };
                let snapshot = run
                    .turn
                    .as_ref()
                    .map(|turn| turn.translator.snapshot().to_vec())
                    .unwrap_or_default();
                self.thread_events(&thread, events, snapshot);
            }
        }
    }

    /// Persist item lifecycles on a child thread and publish its events.
    fn thread_events(&mut self, thread: &str, events: Vec<Value>, snapshot: Vec<Value>) {
        let Some(record) = self.records.get_mut(thread) else {
            return;
        };
        for event in &events {
            let method = event["method"].as_str().unwrap_or("");
            if method == "item/started" || method == "item/completed" {
                record.update_item(event["params"]["item"].clone(), method == "item/completed");
            }
        }
        for item in snapshot {
            record.update_item(item, false);
        }
        for event in events {
            self.emit(thread, event);
        }
    }

    /// Register Agent calls a translator started since it last reported them.
    fn link_spawns(&mut self, root: &str, owner: Option<&str>) {
        let Some(translator) = self.agent_translator(root, owner) else {
            return;
        };
        let sender = translator.thread_id().to_owned();
        let spawns = translator.take_spawns();
        let agents = &mut self.active.get_mut(root).unwrap().agents;
        for (tool_id, input) in spawns {
            agents.spawned(&tool_id, input, owner.map(str::to_owned), &sender);
        }
    }

    /// Agent tool results end a foreground agent, or acknowledge a background one.
    fn agent_results(&mut self, root: &str, owner: Option<&str>, frame: &Value) {
        let metadata = &frame["tool_use_result"];
        let blocks: Vec<Value> = frame["message"]["content"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|block| block["type"] == "tool_result")
            .cloned()
            .collect();
        for block in blocks {
            let Some(key) = block["tool_use_id"].as_str().filter(|key| {
                self.active
                    .get(root)
                    .and_then(|a| a.agents.runs.get(*key))
                    .and_then(|run| run.spawn.as_ref())
                    .is_some_and(|spawn| spawn.owner.as_deref() == owner)
            }) else {
                continue;
            };
            let report = crate::subagents::report(&block, metadata);
            if let Some(agent_id) = metadata["agentId"].as_str() {
                self.alias_agent(root, agent_id, key);
            }
            if block["is_error"] == true {
                self.finish_agent(root, key, "failed", Some(report), None);
            } else if crate::subagents::launch_ack(metadata) {
                self.ensure_child(root, key);
            } else {
                self.finish_agent(
                    root,
                    key,
                    "completed",
                    Some(report),
                    Some(&metadata["usage"]),
                );
            }
        }
    }

    fn alias_agent(&mut self, root: &str, agent_id: &str, key: &str) {
        let Some(active) = self.active.get_mut(root) else {
            return;
        };
        active.agents.alias(agent_id, key);
        if let Some(thread) = active.agents.runs[key].thread_id.clone() {
            self.remember_agent(root, agent_id, &thread);
        }
    }

    fn remember_agent(&mut self, root: &str, agent_id: &str, thread: &str) {
        let record = self.records.get_mut(root).unwrap();
        if record.agents.get(agent_id).map(String::as_str) != Some(thread) {
            record.agents.insert(agent_id.to_owned(), thread.to_owned());
            self.persist(root);
        }
    }

    /// Save a record outside a root turn's own checkpoints. Failure is reported
    /// rather than ending the root turn, which owns the native session.
    fn persist(&self, thread: &str) {
        if let Err(error) = self.store.save(&self.records[thread]) {
            let message = format!("Subagent state was not saved: {error}");
            self.emit(
                thread,
                notification("warning", json!({"threadId": thread, "message": message})),
            );
        }
    }

    /// The run's child thread, created (with its first turn) on first use.
    fn ensure_child(&mut self, root: &str, key: &str) -> Option<String> {
        let run = self.active.get(root)?.agents.runs.get(key)?;
        if let Some(thread) = &run.thread_id {
            return Some(thread.clone());
        }
        let spawn = run.spawn.as_ref()?;
        let (tool_id, owner, sender, input) = (
            spawn.tool_id.clone(),
            spawn.owner.clone(),
            spawn.owner_thread.clone(),
            spawn.input.clone(),
        );
        let agent_id = run.agent_id.clone();
        let record = crate::subagents::child_record(
            self.records.get(&sender)?,
            self.store.threads_dir(),
            &input,
        );
        let child = record.id().to_owned();
        if let Err(error) = self.store.save(&record) {
            let message = format!("Subagent thread was not created: {error}");
            self.emit(
                root,
                notification("warning", json!({"threadId": root, "message": message})),
            );
            return None;
        }
        self.records.insert(child.clone(), record);
        let run = self.active.get_mut(root)?.agents.runs.get_mut(key)?;
        run.thread_id = Some(child.clone());
        if let Some(agent_id) = agent_id {
            self.remember_agent(root, &agent_id, &child);
        }
        self.open_child_turn(root, key, input["prompt"].as_str());
        let mut events = vec![];
        if let Some(translator) = self.agent_translator(root, owner.as_deref()) {
            events.extend(translator.end(&tool_id, crate::subagents::spawn_completed(&child)));
            events.extend(translator.begin(crate::subagents::wait_item(&tool_id, &sender, &child)));
            self.active
                .get_mut(root)?
                .agents
                .runs
                .get_mut(key)?
                .spawn
                .as_mut()?
                .waiting = true;
        }
        self.owner_events(root, owner.as_deref(), events);
        Some(child)
    }

    fn open_child_turn(&mut self, root: &str, key: &str, prompt: Option<&str>) {
        let Some(thread) = self
            .active
            .get(root)
            .and_then(|a| a.agents.runs.get(key))
            .and_then(|run| run.thread_id.clone())
        else {
            return;
        };
        // Codex subscribes every initialized connection to a spawned child; a
        // continued agent's turn is announced the same way.
        let clients: Vec<u64> = self
            .connections
            .iter()
            .filter(|(_, connection)| connection.ready)
            .map(|(client, _)| *client)
            .collect();
        for client in clients {
            self.subscribe(client, &thread);
        }
        self.loaded.insert(thread.clone());
        let root_turn = self.active.get(root).and_then(|active| {
            self.records
                .get(root)?
                .turns
                .iter()
                .find(|turn| turn["id"] == active.id.as_str())?["rootTurnId"]
                .as_str()
                .map(str::to_owned)
        });
        let Some(record) = self.records.get_mut(&thread) else {
            return;
        };
        let (turn, item) = crate::subagents::new_turn(record, prompt, root_turn.as_deref());
        let turn_id = turn["id"].as_str().unwrap().to_owned();
        let child_turn = crate::subagents::child_turn(&thread, &turn_id, record);
        self.persist(&thread);
        self.active
            .get_mut(root)
            .unwrap()
            .agents
            .runs
            .get_mut(key)
            .unwrap()
            .turn = Some(child_turn);
        self.emit(
            &thread,
            notification(
                "thread/status/changed",
                json!({"threadId": thread, "status": {"type": "active", "activeFlags": []}}),
            ),
        );
        self.emit(
            &thread,
            notification("turn/started", json!({"threadId": thread, "turn": turn})),
        );
        if let Some(item) = item {
            for (method, time) in [
                ("item/started", "startedAtMs"),
                ("item/completed", "completedAtMs"),
            ] {
                self.emit(&thread, notification(method,
                    json!({"threadId": thread, "turnId": turn_id, "item": item, time: now_ms()})));
            }
        }
    }

    /// Close the run's child turn and its wait item. Idempotent: a foreground
    /// agent reports both a tool result and a task notification.
    fn finish_agent(
        &mut self,
        root: &str,
        key: &str,
        status: &str,
        report: Option<String>,
        usage: Option<&Value>,
    ) {
        let Some(run) = self.active.get(root).and_then(|a| a.agents.runs.get(key)) else {
            return;
        };
        if run.thread_id.is_none() {
            if status == "completed" {
                self.ensure_child(root, key);
            } else {
                // The call failed before Claude started an agent.
                let Some(spawn) = run.spawn.as_ref() else {
                    return;
                };
                let (tool_id, owner) = (spawn.tool_id.clone(), spawn.owner.clone());
                let events = self
                    .agent_translator(root, owner.as_deref())
                    .map(|t| t.end(&tool_id, json!({"status": "failed"})))
                    .unwrap_or_default();
                self.owner_events(root, owner.as_deref(), events);
                self.active.get_mut(root).unwrap().agents.runs.remove(key);
                return;
            }
        }
        let run = self
            .active
            .get_mut(root)
            .unwrap()
            .agents
            .runs
            .get_mut(key)
            .unwrap();
        let Some(mut turn) = run.turn.take() else {
            return;
        };
        let thread = run.thread_id.clone().unwrap();
        let wait = run
            .spawn
            .as_ref()
            .filter(|spawn| spawn.waiting)
            .map(|spawn| (spawn.tool_id.clone(), spawn.owner.clone()));
        let report = report.filter(|text| !text.is_empty());
        let mut events = vec![];
        if status == "completed"
            && let Some(text) = &report
            && !turn.translator.has_agent_message()
        {
            events.extend(turn.translator.final_message(text));
        }
        events.extend(turn.translator.finish());
        let snapshot = turn.translator.snapshot().to_vec();
        self.thread_events(&thread, events, snapshot);
        let record = self.records.get_mut(&thread).unwrap();
        if let Some(usage) = usage.filter(|u| u.is_object()) {
            for (field, value) in crate::usage::from_native(usage).as_object().unwrap() {
                record.token_usage["total"][field] = json!(
                    record.token_usage["total"][field].as_u64().unwrap_or(0)
                        + value.as_u64().unwrap_or(0)
                );
            }
        }
        if let Some(entry) = record.turns.iter_mut().find(|t| t["id"] == turn.turn_id) {
            entry["status"] = json!(status);
            entry["completedAt"] = json!(protocol::now());
            entry["durationMs"] = json!(now_ms().saturating_sub(turn.started_ms));
            entry["error"] = if status == "failed" {
                json!({"message": report.clone().unwrap_or_else(|| "Subagent failed".into()),
                    "codexErrorInfo": null, "additionalDetails": null, "misalignment": null})
            } else {
                Value::Null
            };
        }
        record.thread["status"] = json!({"type": "idle"});
        record.touch();
        let completed = record
            .turns
            .iter()
            .find(|t| t["id"] == turn.turn_id)
            .cloned();
        let token_usage = record.token_usage.clone();
        self.persist(&thread);
        self.emit(
            &thread,
            notification(
                "thread/tokenUsage/updated",
                json!({"threadId": thread, "turnId": turn.turn_id, "tokenUsage": token_usage}),
            ),
        );
        if let Some(completed) = completed {
            self.emit(
                &thread,
                notification(
                    "turn/completed",
                    json!({"threadId": thread, "turn": completed}),
                ),
            );
        }
        self.emit(
            &thread,
            notification(
                "thread/status/changed",
                json!({"threadId": thread, "status": {"type": "idle"}}),
            ),
        );
        if let Some((tool_id, owner)) = wait {
            let message = report.as_deref();
            let events = self
                .agent_translator(root, owner.as_deref())
                .map(|t| {
                    t.end(
                        &crate::subagents::wait_id(&tool_id),
                        crate::subagents::agent_finished(&thread, status, message),
                    )
                })
                .unwrap_or_default();
            self.owner_events(root, owner.as_deref(), events);
        }
    }

    /// Subagent frames and task events for linked agents. Returns false for
    /// messages the root translator should handle.
    fn route_agent(&mut self, root: &str, message: &Value) -> bool {
        match (message["type"].as_str(), message["subtype"].as_str()) {
            (Some("system"), Some("task_started")) => {
                let (Some(key), Some(task)) =
                    (message["tool_use_id"].as_str(), message["task_id"].as_str())
                else {
                    return false;
                };
                if !self.active[root].agents.runs.contains_key(key) {
                    return false;
                }
                let key = key.to_owned();
                self.alias_agent(root, task, &key);
                self.ensure_child(root, &key);
                true
            }
            (Some("system"), Some("task_updated")) => {
                // Claude patches a task's status before its notification, which
                // carries the report; that notification (or the Agent result) ends it.
                self.active[root]
                    .agents
                    .find(message["task_id"].as_str(), None)
                    .is_some()
            }
            (Some("system"), Some("task_notification")) => {
                let Some(key) = self.active[root]
                    .agents
                    .find(message["task_id"].as_str(), message["tool_use_id"].as_str())
                else {
                    return false;
                };
                let status = match message["status"].as_str() {
                    Some("completed") => "completed",
                    Some("failed") => "failed",
                    Some("killed" | "stopped") => "interrupted",
                    _ => return true,
                };
                let report = message["summary"].as_str().map(str::to_owned);
                self.finish_agent(root, &key, status, report, None);
                true
            }
            (Some("assistant" | "user"), _) if !message["parent_tool_use_id"].is_null() => {
                let agent_id = message["agent_id"].as_str();
                let key = self.active[root]
                    .agents
                    .find(agent_id, message["parent_tool_use_id"].as_str())
                    .or_else(|| {
                        // A continued agent from an earlier turn keeps its thread.
                        let agent_id = agent_id?;
                        let thread = self.records[root].agents.get(agent_id)?.clone();
                        Some(
                            self.active
                                .get_mut(root)
                                .unwrap()
                                .agents
                                .continued(agent_id, &thread),
                        )
                    });
                let Some(key) = key else {
                    return false;
                };
                if self.ensure_child(root, &key).is_none() {
                    return false;
                }
                let mut frame = message.clone();
                frame["parent_tool_use_id"] = Value::Null;
                if self.active[root].agents.runs[&key].turn.is_none() {
                    let prompt = (message["type"] == "user")
                        .then(|| user_text(&frame))
                        .flatten();
                    self.open_child_turn(root, &key, prompt.as_deref());
                    if prompt.is_some() {
                        return true;
                    }
                }
                if frame["type"] == "user" {
                    self.agent_results(root, Some(&key), &frame);
                }
                let Some(turn) = self
                    .active
                    .get_mut(root)
                    .unwrap()
                    .agents
                    .runs
                    .get_mut(&key)
                    .unwrap()
                    .turn
                    .as_mut()
                else {
                    return true;
                };
                let events = turn.translator.receive(&frame).unwrap_or_default();
                let usage = turn.usage.receive(&frame);
                let turn_id = turn.turn_id.clone();
                self.owner_events(root, Some(&key), events);
                self.link_spawns(root, Some(&key));
                let thread = self.active[root].agents.runs[&key]
                    .thread_id
                    .clone()
                    .unwrap();
                let record = self.records.get_mut(&thread).unwrap();
                if let Some(last) = usage {
                    record.token_usage["last"] = last;
                    let token_usage = record.token_usage.clone();
                    self.emit(&thread, notification("thread/tokenUsage/updated",
                        json!({"threadId": thread, "turnId": turn_id, "tokenUsage": token_usage})));
                }
                self.persist_soon(&thread);
                true
            }
            _ => false,
        }
    }

    async fn dynamic_tool(&mut self, id: &str, message: &Value) {
        let request = &message["request"];
        let rpc = &request["message"];
        let request_id = message["request_id"].as_str().unwrap_or("");
        let name = rpc["params"]["name"].as_str().unwrap_or("");
        let arguments = rpc["params"]
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        if request["server_name"] == "codex_desktop"
            && rpc["method"] == "tools/call"
            && let Some(runtime) = self.active[id]
                .plugins
                .as_ref()
                .filter(|runtime| runtime.tools.contains_key(name))
        {
            let runtime = runtime.clone();
            let (_, server, tool) = &runtime.tools[name];
            let turn_id = self.active[id].id.clone();
            let started = now_ms();
            let mut item = json!({"type":"mcpToolCall","id":protocol::id(),"server":server,"tool":tool,
                "status":"inProgress","arguments":arguments,"appContext":null,"mcpAppUi":null,
                "pluginId":null,"readOnlyHint":null,"result":null,"error":null,"durationMs":null});
            self.active
                .get_mut(id)
                .unwrap()
                .plugin_calls
                .insert(item["id"].as_str().unwrap().to_owned(), item.clone());
            self.translated(
                id,
                vec![notification(
                    "item/started",
                    json!({"threadId":id,"turnId":turn_id,"item":item,"startedAtMs":started}),
                )],
            );
            let thread_id = id.to_owned();
            let events = self.events.clone();
            let control = self.active[id].control.clone();
            let request_id = request_id.to_owned();
            let message_id = rpc["id"].clone();
            let name = name.to_owned();
            tokio::spawn(async move {
                let result = match runtime.call(&name, arguments).await {
                    Ok(result) => result,
                    Err(error) => {
                        json!({"isError":true,"content":[{"type":"text","text":error.to_string()}]})
                    }
                };
                item["durationMs"] = json!(now_ms().saturating_sub(started));
                item["status"] = json!(if result["isError"] == true {
                    "failed"
                } else {
                    "completed"
                });
                item["result"] = json!({"content":result["content"],"structuredContent":result["structuredContent"],"_meta":result["_meta"]});
                if result["isError"] == true {
                    item["error"] = json!({"message":"Codex plugin tool returned an error"});
                }
                let _ = events
                    .send(Event::PluginToolFinished {
                        thread_id,
                        turn_id,
                        item,
                    })
                    .await;
                let _ = control
                    .send(Control::Reply(control_reply(
                        &request_id,
                        json!({"mcp_response":{"jsonrpc":"2.0","id":message_id,"result":result}}),
                    )))
                    .await;
            });
            return;
        }
        let tool = self.records[id]
            .settings
            .dynamic_tools
            .iter()
            .find(|t| t.mcp_name == name)
            .cloned();
        let error = if request["server_name"] != "codex_desktop" || rpc["method"] != "tools/call" {
            Some("Unsupported desktop MCP request".to_owned())
        } else if let Some(tool) = &tool {
            tool.validate_arguments(&arguments)
                .err()
                .map(|e| e.to_string())
        } else {
            Some("Unknown desktop tool".to_owned())
        };
        if let Some(error) = error {
            let _ = self.active[id]
                .control
                .try_send(Control::Reply(control_reply(
                    request_id,
                    json!({"mcp_response":{"jsonrpc":"2.0","id":rpc["id"],
                    "error":{"code":-32602,"message":error}}}),
                )));
            return;
        }
        let tool = tool.unwrap();
        let active = &self.active[id];
        let owner = active.owner;
        let turn_id = active.id.clone();
        let call_id = protocol::id();
        let item = json!({"type":"dynamicToolCall","id":call_id,"namespace":tool.namespace,
            "tool":tool.name,"arguments":arguments,"status":"inProgress",
            "contentItems":null,"success":null,"durationMs":null});
        let started_ms = now_ms();
        self.translated(
            id,
            vec![notification(
                "item/started",
                json!({"threadId":id,
            "turnId":turn_id,"item":item,"startedAtMs":started_ms}),
            )],
        );
        let pending_id = format!("tool-{}", protocol::id());
        let request = json!({"id":pending_id,"method":"item/tool/call",
            "params":{"threadId":id,"turnId":turn_id,"callId":call_id,
                "namespace":tool.namespace,"tool":tool.name,"arguments":arguments}});
        let pending = Pending {
            owner,
            request,
            timeout: None,
            thread_id: id.into(),
            turn_id: turn_id.clone(),
            claude_request_id: request_id.into(),
            input: arguments.clone(),
            questions: None,
            tool: String::new(),
            generic_tool: false,
            dynamic: Some(DynamicPending {
                message_id: rpc["id"].clone(),
                item,
                started_ms,
            }),
        };
        self.pending.insert(pending_id.clone(), pending);
        self.deliver_pending(&pending_id);
        // Execution can legitimately outlive a permission dialog. The client
        // retains the same callId across reconnect; never mint a replacement call.
    }

    fn complete_dynamic(&mut self, rpc_id: &str, mut pending: Pending, result: Value) {
        let dynamic = pending.dynamic.take().unwrap();
        let (result, content) = match crate::dynamic_tools::result_content(&result) {
            Ok(content) => (result, content),
            Err(error) => {
                let result = crate::dynamic_tools::failure(&error.to_string());
                let content = crate::dynamic_tools::result_content(&result).unwrap();
                (result, content)
            }
        };
        if let Some(active) = self
            .active
            .get(&pending.thread_id)
            .filter(|a| a.id == pending.turn_id)
        {
            let _ = active.control.try_send(Control::Reply(control_reply(
                &pending.claude_request_id,
                json!({"mcp_response":{"jsonrpc":"2.0","id":dynamic.message_id,"result":content}}),
            )));
        }
        let mut item = dynamic.item;
        item["status"] = json!(if result["success"] == true {
            "completed"
        } else {
            "failed"
        });
        item["success"] = result["success"].clone();
        item["contentItems"] = result["contentItems"].clone();
        item["durationMs"] = json!(now_ms().saturating_sub(dynamic.started_ms));
        self.translated(&pending.thread_id, vec![notification("item/completed", json!({
            "threadId":pending.thread_id,"turnId":pending.turn_id,"item":item,"completedAtMs":now_ms()}))]);
        self.emit_resolved(&pending, json!(rpc_id));
    }

    async fn permission(&mut self, id: &str, message: &Value) {
        let request = &message["request"];
        let request_id = message["request_id"].as_str().unwrap_or("");
        if request["subtype"] == "mcp_message" {
            self.dynamic_tool(id, message).await;
            return;
        }
        let active = &self.active[id];
        if request["subtype"] != "can_use_tool" {
            let reply = json!({"type": "control_response", "response": {"subtype": "error", "request_id": request_id, "error": "Unsupported Claude control request"}});
            let _ = active.control.try_send(Control::Reply(reply));
            return;
        }
        let input = &request["input"];
        let tool = request["tool_name"].as_str().unwrap_or("unknown");
        // Registered desktop tools execute in the client, which owns their
        // approval UI. Authorize only this catalog entry, never arbitrary MCP.
        if let Some(name) = tool.strip_prefix("mcp__codex_desktop__")
            && let Some(spec) = self.records[id]
                .settings
                .dynamic_tools
                .iter()
                .find(|t| t.mcp_name == name)
        {
            let mut input = input.clone();
            let mut validation = spec.validate_arguments(&input);
            // Antigravity adds a summary to its MCP permission preview. The
            // bridge validates the actual tools/call arguments independently.
            if validation.is_err()
                && request["acp_tool_name"] == format!("mcp_codex_desktop_{name}")
                && spec.input_schema["properties"].get("toolSummary").is_none()
                && let Some(arguments) = input.as_object_mut()
                && arguments.get("toolSummary").is_some_and(Value::is_string)
            {
                arguments.remove("toolSummary");
                validation = spec.validate_arguments(&input);
            }
            let data = match validation {
                Ok(()) => json!({"behavior":"allow","updatedInput":input}),
                Err(error) => json!({"behavior":"deny","message":error.to_string()}),
            };
            let _ = active
                .control
                .try_send(Control::Reply(control_reply(request_id, data)));
            return;
        }
        let item_id = request["tool_use_id"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(protocol::id);
        let owner = active.owner;
        let turn_id = active.id.clone();
        // A linked subagent's request belongs to its child thread; the native
        // control channel stays with the root turn.
        let agent = request["agent_id"]
            .as_str()
            .and_then(|agent| self.active[id].agents.find(Some(agent), None))
            .filter(|key| self.ensure_child(id, key).is_some());
        if let Some(key) = &agent
            && self.active[id].agents.runs[key].turn.is_none()
        {
            self.open_child_turn(id, key, None);
        }
        let (view_thread, view_turn) = match &agent {
            Some(key) => {
                let run = &self.active[id].agents.runs[key];
                (
                    run.thread_id.clone().unwrap(),
                    run.turn.as_ref().unwrap().turn_id.clone(),
                )
            }
            None => (id.to_owned(), turn_id.clone()),
        };
        let events = self
            .agent_translator(id, agent.as_deref())
            .unwrap()
            .tool_start(&item_id, tool, input);
        self.owner_events(id, agent.as_deref(), events);
        self.link_spawns(id, agent.as_deref());
        let record = &self.records[id];
        if record.session_grants.iter().any(|grant| {
            grant.permits(
                tool,
                input,
                &record.settings.cwd.to_string_lossy(),
                &record.settings.approval_policy,
            )
        }) {
            let _ = self.active[id]
                .control
                .try_send(Control::Reply(control_reply(
                    request_id,
                    json!({"behavior":"allow","updatedInput":input}),
                )));
            return;
        }
        let mut questions = None;
        let generic_tool =
            !["Bash", "Write", "Edit", "MultiEdit", "AskUserQuestion"].contains(&tool);
        let (method, mut params) = match tool {
            "Bash" => (
                "item/commandExecution/requestApproval",
                json!({"kind": "command", "startedAtMs": now_ms(),
                "environmentId": null, "command": input["command"], "cwd": self.records[id].settings.cwd,
                "reason": format!("{}{}. Session approval applies only to this exact tool input in this thread and working directory.",
                    if input["dangerouslyDisableSandbox"] == true { "This command will run outside the workspace sandbox. " } else { "" },
                    request["decision_reason"].as_str().unwrap_or("Claude requests permission to run this command")),
                "availableDecisions": ["accept", "acceptForSession", "decline", "cancel"]}),
            ),
            "Write" | "Edit" | "MultiEdit" => (
                "item/fileChange/requestApproval",
                json!({"startedAtMs": now_ms(),
                "reason": format!("Claude requests permission for {tool}: {}. Session approval applies only to this exact tool input in this thread and working directory.", input["file_path"].as_str().unwrap_or(""))}),
            ),
            "Read" if input["file_path"].as_str().is_some() => {
                let path = record
                    .settings
                    .cwd
                    .join(input["file_path"].as_str().unwrap());
                (
                    "item/permissions/requestApproval",
                    json!({"startedAtMs":now_ms(),"cwd":record.settings.cwd,
                        "permissions":{"fileSystem":{"read":[path]}},
                        "reason":"Claude requests this file read. Session approval applies only to this exact tool input in this thread and working directory."}),
                )
            }
            "AskUserQuestion" => {
                let flow = match crate::approvals::QuestionFlow::new(input) {
                    Ok(flow) => flow,
                    Err(error) => {
                        let _ = self.active[id]
                            .control
                            .try_send(Control::Reply(control_reply(
                                request_id,
                                json!({"behavior":"deny","message":error.to_string()}),
                            )));
                        return;
                    }
                };
                let next = flow.next_request().unwrap();
                questions = Some(flow);
                next
            }
            _ => (
                "item/tool/requestUserInput",
                json!({"questions": [{"id": "permission", "header": "Permission", "question": format!("Allow Claude tool {tool} with input {input}?"),
                "isOther": false, "isSecret": false, "options": [{"label": "Allow", "description": "Allow this tool call once"}, {"label": "Allow for session", "description": "Allow this exact tool input for this thread"}, {"label": "Deny", "description": "Deny this tool call"}]}],
                "isBlocking": true, "autoResolutionMs": null}),
            ),
        };
        params["threadId"] = json!(view_thread);
        params["turnId"] = json!(view_turn);
        params["itemId"] = json!(item_id);
        let rpc_id = format!("approval-{}", protocol::id());
        let pending = Pending {
            owner,
            request: json!({"id":rpc_id,"method":method,"params":params}),
            timeout: Some(PendingTimeout {
                remaining: self.config.approval_timeout,
                running: None,
                generation: 0,
            }),
            thread_id: id.to_owned(),
            turn_id,
            claude_request_id: request_id.to_owned(),
            input: input.clone(),
            questions,
            tool: tool.into(),
            generic_tool,
            dynamic: None,
        };
        if self.records[id].settings.approval_policy == "never" && pending.questions.is_none() {
            self.deny_pending(&rpc_id, pending, "Permission prompts are disabled")
                .await;
            return;
        }
        if self.acp()
            && self.records[id].settings.approvals_reviewer == "auto_review"
            && pending.questions.is_none()
        {
            self.start_review(id, pending);
            return;
        }
        self.pending.insert(rpc_id.clone(), pending);
        self.update_wait_status(id);
        self.deliver_pending(&rpc_id);
    }

    /// Review an ACP permission request instead of showing it to the user.
    fn start_review(&mut self, id: &str, pending: Pending) {
        // A turn being interrupted (for example by repeated denials) gets no
        // further reviews; its remaining requests are refused.
        if self.active[id].interrupting {
            let _ = self.active[id]
                .control
                .try_send(Control::Reply(control_reply(
                    &pending.claude_request_id,
                    json!({"behavior":"deny","message":"The turn is being interrupted"}),
                )));
            return;
        }
        let review_id = format!("review-{}", protocol::id());
        let record = &self.records[id];
        let action = crate::auto_review::action(
            &pending.tool,
            &pending.input,
            &record.settings.cwd,
            &protocol::provider().id,
        );
        let target = pending.request["params"]["itemId"].clone();
        let started_ms = now_ms();
        self.emit(
            id,
            notification(
                "item/autoApprovalReview/started",
                json!({"threadId":id,"turnId":pending.turn_id,"reviewId":review_id,
                    "targetItemId":target,"startedAtMs":started_ms,"action":action,
                    "review":{"status":"inProgress","riskLevel":null,"userAuthorization":null,"rationale":null}}),
            ),
        );
        let transcript = crate::auto_review::transcript(&record.turns);
        let approved = record.approved_actions.clone();
        let cwd = record.settings.cwd.clone();
        let reviewer = self.config.reviewer.clone();
        let events = self.events.clone();
        let reviewed = action.clone();
        let finished_id = review_id.clone();
        tokio::spawn(async move {
            let outcome = match reviewer {
                Some(reviewer) => {
                    crate::auto_review::review(&reviewer, &cwd, &transcript, &reviewed, &approved)
                        .await
                }
                None => crate::auto_review::Outcome::Failed("no reviewer is configured".into()),
            };
            let _ = events
                .send(Event::AutoReviewFinished {
                    review_id: finished_id,
                    outcome,
                })
                .await;
        });
        self.reviews.insert(
            review_id,
            Review {
                pending,
                action,
                target,
                started_ms,
            },
        );
    }

    fn review_completed(&self, review_id: &str, review: &Review, result: Value) {
        let thread = &review.pending.thread_id;
        self.emit(
            thread,
            notification(
                "item/autoApprovalReview/completed",
                json!({"threadId":thread,"turnId":review.pending.turn_id,"reviewId":review_id,
                    "targetItemId":review.target,"startedAtMs":review.started_ms,"completedAtMs":now_ms(),
                    "decisionSource":"agent","action":review.action,"review":result}),
            ),
        );
    }

    async fn finish_review(&mut self, review_id: &str, outcome: crate::auto_review::Outcome) {
        let Some(review) = self.reviews.remove(review_id) else {
            return;
        };
        let id = review.pending.thread_id.clone();
        // A review that outlived its turn was already reported as aborted.
        if self
            .active
            .get(&id)
            .is_none_or(|active| active.id != review.pending.turn_id)
        {
            return;
        }
        self.review_completed(review_id, &review, outcome.review());
        let allowed = outcome.allowed();
        let data = if allowed {
            json!({"behavior":"allow","updatedInput":review.pending.input})
        } else {
            json!({"behavior":"deny","message":outcome.denial_message()})
        };
        let active = self.active.get_mut(&id).unwrap();
        let trip = active.breaker.record(!allowed);
        let _ = active.control.try_send(Control::Reply(control_reply(
            &review.pending.claude_request_id,
            data,
        )));
        if allowed {
            return;
        }
        let mut record = self.records[&id].clone();
        record
            .auto_review_denials
            .push(json!({"reviewId":review_id,"tool":review.pending.tool,
            "input":review.pending.input,"action":review.action}));
        let excess = record.auto_review_denials.len().saturating_sub(20);
        record.auto_review_denials.drain(..excess);
        match self.store.save(&record) {
            Ok(()) => {
                self.records.insert(id.clone(), record);
            }
            Err(error) => self.emit(
                &id,
                notification(
                    "warning",
                    json!({"threadId":id,
                "message":format!("The denied action cannot be approved later: {error}")}),
                ),
            ),
        }
        if trip {
            self.emit(&id, notification("guardianWarning", json!({"threadId":id,
                "message":"Automatic review denied several actions in a row, so this turn was interrupted. Review the denials, approve an action if you intend it, or give the agent new instructions."})));
            let active = self.active.get_mut(&id).unwrap();
            active.interrupting = true;
            let _ = active.control.try_send(Control::Interrupt);
        }
    }

    /// The user approves an action automatic review denied: allow that exact
    /// action from now on in this thread, and tell the agent so it can retry.
    fn approve_denied_action(&mut self, p: &Value) -> RpcResult<Value> {
        supported_fields(p, &["threadId", "event"])?;
        let id = required_str(p, "threadId")?;
        if !self.acp() {
            return Err(RpcError::invalid(
                "Claude threads use Claude's native auto review; ask Claude to retry the action after approving it in chat",
            ));
        }
        let review_id = required_str(&p["event"], "id")?;
        if p["event"]["status"]
            .as_str()
            .is_some_and(|status| status != "denied")
        {
            return Err(RpcError::invalid("Only denied reviews can be approved"));
        }
        let mut record = self.record(id)?.clone();
        let denial = record
            .auto_review_denials
            .iter()
            .find(|denial| denial["reviewId"] == review_id)
            .cloned()
            .ok_or_else(|| RpcError::invalid("Denied review not found in this thread"))?;
        let grant = crate::approvals::SessionGrant::new(
            denial["tool"].as_str().unwrap_or(""),
            &denial["input"],
            &record.settings.cwd.to_string_lossy(),
        );
        if !record.session_grants.contains(&grant) {
            record.session_grants.push(grant);
        }
        record
            .auto_review_denials
            .retain(|denial| denial["reviewId"] != review_id);
        record.approved_actions.push(denial["action"].clone());
        let excess = record.approved_actions.len().saturating_sub(20);
        record.approved_actions.drain(..excess);
        // The same instruction Codex gives its own agent.
        let text = format!(
            "The user has manually approved a specific action that was previously `Rejected`.\n\nTreat this as approval to perform that exact action in the same context in which it was originally requested.\nDo not assume this also authorizes similar operations with different payloads.\n\nApproved action:\n{}",
            serde_json::to_string_pretty(&json!({"action":denial["action"],"outcome":"allowed"}))
                .unwrap_or_default()
        );
        // Persist the grant before the agent can act on the approval.
        self.store.save(&record).map_err(RpcError::internal)?;
        let delivered = self.active.get(id).is_some_and(|active| {
            let Ok(permit) = active.control.try_reserve() else {
                return false;
            };
            let mut admission = active.input_admission.lock().unwrap();
            if admission.closed {
                return false;
            }
            admission.queued += 1;
            permit.send(Control::UserInput(
                json!({"type":"user","uuid":protocol::id(),
                "session_id":record.session_id,"parent_tool_use_id":null,
                "message":{"role":"user","content":[{"type":"text","text":text}]}}),
            ));
            true
        });
        if !delivered {
            record.pending_context.push(text);
            self.store.save(&record).map_err(RpcError::internal)?;
        }
        self.records.insert(id.to_owned(), record);
        Ok(json!({}))
    }

    /// Resolution goes to the thread the request was shown on (a subagent's
    /// child thread or the root).
    fn emit_resolved(&self, pending: &Pending, request_id: Value) {
        let thread = pending.request["params"]["threadId"]
            .as_str()
            .unwrap_or(&pending.thread_id);
        self.emit(
            thread,
            notification(
                "serverRequest/resolved",
                json!({"threadId": thread, "requestId": request_id}),
            ),
        );
    }

    fn update_wait_status(&mut self, id: &str) {
        if !self.active.contains_key(id) {
            return;
        }
        let waiting = self
            .pending
            .values()
            .find(|p| p.thread_id == id && p.dynamic.is_none());
        let flag = waiting.map(|p| {
            if p.questions.is_some() {
                "waitingOnUserInput"
            } else {
                "waitingOnApproval"
            }
        });
        let status = json!({"type": "active", "activeFlags": flag.into_iter().collect::<Vec<_>>()});
        self.records.get_mut(id).unwrap().thread["status"] = status.clone();
        self.emit(
            id,
            notification(
                "thread/status/changed",
                json!({"threadId": id, "status": status}),
            ),
        );
        // A subagent's own requests also mark its child thread.
        let children: Vec<String> = self.active[id]
            .agents
            .open_threads()
            .map(str::to_owned)
            .collect();
        for child in children {
            let flag = self
                .pending
                .values()
                .find(|p| p.thread_id == id && p.request["params"]["threadId"] == child.as_str())
                .map(|p| {
                    if p.questions.is_some() {
                        "waitingOnUserInput"
                    } else {
                        "waitingOnApproval"
                    }
                });
            let status =
                json!({"type": "active", "activeFlags": flag.into_iter().collect::<Vec<_>>()});
            let record = self.records.get_mut(&child).unwrap();
            if record.thread["status"] != status {
                record.thread["status"] = status.clone();
                self.emit(
                    &child,
                    notification(
                        "thread/status/changed",
                        json!({"threadId": child, "status": status}),
                    ),
                );
            }
        }
    }

    async fn approval_response(&mut self, client: u64, rpc_id: &Value, message: &Value) {
        let Some(key) = rpc_id.as_str() else {
            return;
        };
        let Some(thread_id) = self.pending.get(key).map(|p| p.thread_id.clone()) else {
            return;
        };
        let detached = self
            .active
            .get(&thread_id)
            .is_some_and(|active| active.owner.is_none_or(|owner| !self.connected(owner)));
        // The desktop can finish a tool during reconnect and send its cached
        // response as soon as initialization completes, before thread/resume.
        if !self.claim_detached_turn(client, &thread_id)
            || self
                .pending
                .get(key)
                .is_none_or(|p| p.owner != Some(client))
        {
            return;
        }
        let mut pending = self.pending.remove(key).unwrap();
        if detached {
            self.replay_pending(client, &thread_id);
        }
        if pending.dynamic.is_some() {
            let result = if message.get("error").is_some() {
                crate::dynamic_tools::failure(
                    message["error"]["message"]
                        .as_str()
                        .unwrap_or("Desktop tool failed"),
                )
            } else {
                message["result"].clone()
            };
            self.complete_dynamic(key, pending, result);
            return;
        }
        let result = &message["result"];
        let mut input = pending.input.clone();
        let mut deny_message = "Permission denied by client".to_owned();
        let mut decision = crate::approvals::Decision::Deny;
        let allowed = if !message["error"].is_null() {
            false
        } else if let Some(flow) = &mut pending.questions {
            match flow.respond(result) {
                Ok(Some(updated)) => {
                    input = updated;
                    true
                }
                Ok(None) => {
                    let (method, mut params) = flow.next_request().unwrap();
                    params["threadId"] = pending.request["params"]["threadId"].clone();
                    params["turnId"] = pending.request["params"]["turnId"].clone();
                    params["itemId"] = pending.request["params"]["itemId"].clone();
                    let next_id = format!("approval-{}", protocol::id());
                    pending.request = json!({"id":next_id,"method":method,"params":params});
                    // Each new question receives a full response budget; replay
                    // of the same question preserves its remaining budget.
                    pending.timeout = Some(PendingTimeout {
                        remaining: self.config.approval_timeout,
                        running: None,
                        generation: 0,
                    });
                    self.emit_resolved(&pending, json!(rpc_id));
                    self.pending.insert(next_id.clone(), pending);
                    self.deliver_pending(&next_id);
                    return;
                }
                Err(error) => {
                    deny_message = error.to_string();
                    false
                }
            }
        } else {
            decision = if pending.request["method"] == "item/permissions/requestApproval" {
                crate::approvals::file_read_decision(
                    result,
                    &pending.request["params"]["permissions"],
                )
            } else {
                crate::approvals::decision(result, pending.generic_tool)
            };
            matches!(
                decision,
                crate::approvals::Decision::Once | crate::approvals::Decision::Session
            ) && self.records[&pending.thread_id].settings.approval_policy != "never"
        };
        let cancelled = decision == crate::approvals::Decision::Cancel;
        let data = if allowed {
            json!({"behavior":"allow","updatedInput":input})
        } else {
            json!({"behavior":"deny","message":deny_message,"interrupt":cancelled})
        };
        if let Some(active) = self
            .active
            .get_mut(&pending.thread_id)
            .filter(|a| a.id == pending.turn_id)
        {
            active.interrupting |= cancelled;
            let reply = control_reply(&pending.claude_request_id, data);
            let command = if allowed && decision == crate::approvals::Decision::Session {
                Control::GrantReply(
                    reply,
                    crate::approvals::SessionGrant::new(
                        &pending.tool,
                        &pending.input,
                        &self.records[&pending.thread_id]
                            .settings
                            .cwd
                            .to_string_lossy(),
                    ),
                )
            } else {
                Control::Reply(reply)
            };
            if active.control.try_send(command).is_err() {
                self.finish(
                    &pending.thread_id,
                    "failed",
                    Some("Claude control channel unavailable".into()),
                )
                .await;
                return;
            }
        }
        self.emit_resolved(&pending, json!(rpc_id));
        self.update_wait_status(&pending.thread_id);
    }
    async fn deny_pending(&mut self, rpc_id: &str, pending: Pending, reason: &str) {
        if pending.dynamic.is_some() {
            self.complete_dynamic(rpc_id, pending, crate::dynamic_tools::failure(reason));
            return;
        }
        if let Some(active) = self
            .active
            .get(&pending.thread_id)
            .filter(|a| a.id == pending.turn_id)
        {
            let _ = active.control.try_send(Control::Reply(control_reply(
                &pending.claude_request_id,
                json!({"behavior": "deny", "message": reason}),
            )));
        }
        self.emit_resolved(&pending, json!(rpc_id));
        self.update_wait_status(&pending.thread_id);
    }
    async fn disconnect(&mut self, client: u64) {
        let Some(connection) = self.connections.remove(&client) else {
            return;
        };
        connection.cancel.cancel();
        while self.command_cleanup.try_join_next().is_some() {}
        let commands = self.commands.clone();
        self.command_cleanup
            .spawn(async move { commands.disconnect(client).await });
        for active in self.active.values_mut().filter(|a| a.owner == Some(client)) {
            active.owner = None;
        }
        for pending in self
            .pending
            .values_mut()
            .filter(|p| p.owner == Some(client))
        {
            pending.owner = None;
            if let Some(timeout) = &mut pending.timeout {
                timeout.pause();
            }
        }
        self.loaded.retain(|id| {
            self.active.contains_key(id)
                || self
                    .connections
                    .values()
                    .any(|c| c.subscriptions.contains(id))
        });
    }
}

fn acp_fast_mode() -> RpcError {
    RpcError::invalid("Fast mode is available only with Claude, not ACP agents")
}

/// Codex's error for steering a manual compaction, with the turn error the desktop reads.
fn compaction_not_steerable() -> RpcError {
    let message = "cannot steer a compact turn";
    RpcError {
        data: Some(json!({"message": message, "additionalDetails": null,
            "codexErrorInfo": {"activeTurnNotSteerable": {"turnKind": "compact"}}})),
        ..RpcError::invalid(message)
    }
}

fn subagent_read_only() -> RpcError {
    RpcError::invalid(
        "Claude subagent threads are read-only; continue the agent from its parent thread",
    )
}

/// Names of the settings fields that differ between two thread configurations.
fn changed_settings(current: &Settings, requested: &Settings) -> RpcResult<Vec<String>> {
    let current = serde_json::to_value(current).map_err(RpcError::internal)?;
    let requested = serde_json::to_value(requested).map_err(RpcError::internal)?;
    Ok(current
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(name, value)| requested.get(name.as_str()) != Some(*value))
        .map(|(name, _)| name.clone())
        .collect())
}

/// Text of a user frame that carries no tool results (a continued agent's input).
fn user_text(frame: &Value) -> Option<String> {
    let content = &frame["message"]["content"];
    if let Some(text) = content.as_str() {
        return Some(text.to_owned());
    }
    let blocks = content.as_array()?;
    if blocks.iter().any(|block| block["type"] == "tool_result") {
        return None;
    }
    let text: Vec<&str> = blocks
        .iter()
        .filter_map(|block| block["text"].as_str())
        .collect();
    (!text.is_empty()).then(|| text.join("\n"))
}

fn control_reply(request_id: &str, data: Value) -> Value {
    json!({"type": "control_response", "response": {"subtype": "success", "request_id": request_id, "response": data}})
}

fn resume_cursor(thread_id: &str, kind: &str, anchor: Value) -> Value {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    json!(
        URL_SAFE_NO_PAD
            .encode(json!({"resumeThread": thread_id, "kind": kind, "anchor": anchor}).to_string())
    )
}

/// An inclusive cursor at one turn that `thread/turns/list` accepts in either
/// direction, as Codex's search results provide for opening a match.
fn turn_cursor(thread_id: &str, turn_id: &str) -> String {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    URL_SAFE_NO_PAD.encode(
        json!({"scope": format!("turns:{thread_id}"), "anchor": turn_id,
            "inclusive": true, "anyDirection": true})
        .to_string(),
    )
}

/// A resume boundary applies to the complete snapshot before turn filtering.
/// The same item boundary can therefore hydrate each turn independently.
fn apply_resume_boundary(
    items: &mut Vec<Value>,
    options: &mut Value,
    thread_id: &str,
    kind: &str,
) -> RpcResult<()> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let Some(cursor) = options["cursor"].as_str() else {
        return Ok(());
    };
    let decoded: Value = URL_SAFE_NO_PAD
        .decode(cursor)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or(Value::Null);
    if decoded.get("resumeThread").is_none() {
        return Ok(());
    }
    if decoded["resumeThread"] != thread_id
        || decoded["kind"] != kind
        || options["sortDirection"].as_str().unwrap_or("desc") != "desc"
    {
        return Err(RpcError::invalid(
            "Resume cursor belongs to a different thread, list, or direction",
        ));
    }
    let end = items
        .iter()
        .position(|item| {
            let id = if kind == "items" {
                &item["item"]["id"]
            } else {
                &item["id"]
            };
            id == &decoded["anchor"]
        })
        .ok_or_else(|| RpcError::invalid("Resume cursor anchor is no longer in this list"))?;
    items.truncate(end + 1);
    options.as_object_mut().unwrap().remove("cursor");
    options["sortDirection"] = json!("desc");
    Ok(())
}

fn turns_page(record: &Record, options: &Value) -> RpcResult<Value> {
    let mut turns = record.timed_turns();
    let mut options = options.clone();
    apply_resume_boundary(&mut turns, &mut options, record.id(), "turns")?;
    if options["itemsView"] == "notLoaded" {
        for turn in &mut turns {
            turn["items"] = json!([]);
            turn["itemsView"] = json!("notLoaded");
        }
    }
    // Full items also satisfy summary requests, and remain labelled as full.
    page(turns, &options, true, &format!("turns:{}", record.id()))
}

/// Cursors anchor to an item rather than an offset, so inserts ahead of a page do not shift it.
fn page(mut items: Vec<Value>, p: &Value, default_desc: bool, scope: &str) -> RpcResult<Value> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let descending = p["sortDirection"]
        .as_str()
        .map_or(default_desc, |s| s == "desc");
    if descending {
        items.reverse();
    }
    let key = |item: &Value| -> String {
        item.as_str()
            .or_else(|| item["id"].as_str())
            .or_else(|| item["item"]["id"].as_str())
            .unwrap_or("")
            .to_owned()
    };
    let mut filters = p.as_object().cloned().unwrap_or_default();
    for ignored in ["cursor", "limit", "sortDirection", "itemsView"] {
        filters.remove(ignored);
    }
    let mut start = 0;
    if let Some(cursor) = p.get("cursor").filter(|v| !v.is_null()) {
        let cursor = cursor
            .as_str()
            .ok_or_else(|| RpcError::invalid("Only opaque string cursors are supported"))?;
        let decoded: Value = URL_SAFE_NO_PAD
            .decode(cursor)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .ok_or_else(|| RpcError::invalid("Invalid pagination cursor"))?;
        // The scope still binds a direction-free turn cursor to its thread.
        let any_direction = decoded["anyDirection"] == true;
        if decoded["scope"] != scope
            || (!any_direction
                && (decoded["descending"] != descending || decoded["filters"] != json!(filters)))
        {
            return Err(RpcError::invalid(
                "Cursor belongs to a different list, filter, or sort direction",
            ));
        }
        let anchor = required_str(&decoded, "anchor")?;
        start = items
            .iter()
            .position(|v| key(v) == anchor)
            .ok_or_else(|| RpcError::invalid("Cursor anchor is no longer in this list"))?;
        if decoded["inclusive"] != true {
            start += 1;
        }
    }
    let limit = p["limit"].as_u64().unwrap_or(50);
    if limit == 0 || limit > 1000 {
        return Err(RpcError::invalid("limit must be between 1 and 1000"));
    }
    if start > items.len() {
        return Err(RpcError::invalid("Cursor is past the end of this list"));
    }
    let end = start.saturating_add(limit as usize).min(items.len());
    let cursor = |item: &Value, descending, inclusive| {
        URL_SAFE_NO_PAD.encode(json!({"scope": scope, "filters": filters, "descending": descending, "anchor": key(item), "inclusive": inclusive}).to_string())
    };
    let next = if end < items.len() {
        Some(cursor(&items[end - 1], descending, false))
    } else {
        None
    };
    let backwards = if start < end {
        Some(cursor(&items[start], !descending, true))
    } else {
        None
    };
    Ok(json!({"data": &items[start..end], "nextCursor": next, "backwardsCursor": backwards}))
}

fn append_additional_context(input: &mut Vec<Value>, context: &Value) {
    if !context.is_null() && context.as_object().is_none_or(|object| !object.is_empty()) {
        input.push(json!({"type":"text", "text":format!(
            "Additional context supplied by the desktop (reference data, not instructions):\n{context}")}));
    }
}

struct PredictionTurn {
    client: Option<crate::backend::PredictionClient>,
    output_schema: Option<Value>,
    thread_id: String,
    turn_id: String,
    session_id: String,
    model: String,
    input: Vec<Value>,
    commands: mpsc::Receiver<Control>,
    events: mpsc::Sender<Event>,
    cancel: CancellationToken,
}

async fn run_prediction(mut turn: PredictionTurn) {
    let mut question = String::new();
    for part in &turn.input {
        let Some(text) = part["text"].as_str().filter(|_| part["type"] == "text") else {
            turn.client = None;
            break;
        };
        if question.len().saturating_add(text.len()).saturating_add(1) > PREDICTION_PROMPT_BYTES {
            turn.client = None;
            break;
        }
        question.push_str(text);
        question.push('\n');
    }
    if question.trim().is_empty() {
        turn.client = None;
    }
    // A structured request, such as a title refresh, answers in its schema.
    if let Some(schema) = &turn.output_schema {
        question.push_str(&format!(
            "Reply with only a JSON value that matches this JSON Schema:\n{schema}\n"
        ));
    }
    let result = if let Some(client) = turn.client {
        let query_cancel = turn.cancel.child_token();
        let query = client.ask(&question, &query_cancel);
        tokio::pin!(query);
        tokio::select! {
            biased;
            _ = turn.cancel.cancelled() => {
                query_cancel.cancel();
                let _ = query.await;
                return;
            }
            _ = turn.commands.recv() => {
                query_cancel.cancel();
                let _ = query.await;
                None
            }
            result = &mut query => result.ok(),
        }
    } else {
        None
    };
    let answer = result
        .filter(|value| value["synthetic"] != true)
        .and_then(|value| serde_json::from_str::<Value>(value["response"].as_str()?.trim()).ok());
    let text = if turn.output_schema.is_some() {
        // The caller validates the value against its schema.
        answer.map(|value| value.to_string()).unwrap_or_default()
    } else {
        let suggestion = answer
            .and_then(|value| value["suggestion"].as_str().map(str::to_owned))
            .filter(|text| !text.trim().is_empty() && text.len() <= 512);
        json!({"suggestion":suggestion}).to_string()
    };
    // Native side_question does not return a per-request usage object. Leave
    // prediction usage unknown rather than reporting the parent's cumulative bill.
    for message in [
        json!({"type":"assistant","session_id":turn.session_id,"uuid":protocol::id(),
            "message":{"id":protocol::id(),"role":"assistant","model":turn.model,
                "content":[{"type":"text","text":text}]}}),
        json!({"type":"result","subtype":"success","is_error":false,
            "session_id":turn.session_id,"result":text}),
    ] {
        if turn.cancel.is_cancelled() {
            return;
        }
        let event = Event::Backend {
            thread_id: turn.thread_id.clone(),
            turn_id: turn.turn_id.clone(),
            event: BackendEvent::Message(message),
        };
        tokio::select! {
            _ = turn.cancel.cancelled() => return,
            sent = turn.events.send(event) => if sent.is_err() { return; },
        }
    }
}

struct LaunchedTurn {
    turn_id: String,
    /// A prediction turn's structured-output schema.
    output_schema: Option<Value>,
    options: SessionOptions,
    warm: Option<crate::warm::WarmSession>,
    launch_key: Option<Value>,
    input: Vec<Value>,
    owner: Option<u64>,
    fast_mode: bool,
    compaction: bool,
}

/// What a new turn submits to Claude.
#[derive(Clone, Copy)]
enum TurnRequest<'a> {
    /// The user's input, possibly a queued submission that the turn consumes.
    Input {
        queued_submission_id: Option<&'a str>,
    },
    /// Codex's manual compaction, run as Claude's `/compact` command.
    Compaction,
}

struct BackendTurn {
    /// The thread's previous Claude process, still closing.
    previous_process: Option<tokio::task::JoinHandle<()>>,
    config: BackendConfig,
    plugins: Option<(
        crate::codex_plugins::Config,
        Vec<String>,
        Vec<crate::dynamic_tools::Tool>,
    )>,
    options: SessionOptions,
    /// The thread's idle process from an earlier turn with identical launch settings.
    warm: Option<crate::warm::WarmSession>,
    /// Keep the process for the next turn after a clean finish.
    park: bool,
    thread_id: String,
    turn_id: String,
    /// Empty when a woken idle process is already working without a prompt.
    input: Vec<Value>,
    commands: mpsc::Receiver<Control>,
    input_admission: Arc<Mutex<InputAdmission>>,
    events: mpsc::Sender<Event>,
    cancel: CancellationToken,
}

async fn run_backend(turn: BackendTurn) {
    let BackendTurn {
        mut previous_process,
        config,
        plugins,
        mut options,
        warm,
        park,
        thread_id,
        turn_id,
        input,
        mut commands,
        input_admission,
        events,
        cancel,
    } = turn;
    // Auto-mode terminal denials never create a can_use_tool callback. Explain
    // the supported per-chat recovery instead of suggesting global allow rules.
    // ACP agents have no Claude auto mode, so the guidance does not apply.
    if config.protocol == BackendProtocol::Claude {
        options.append_system_prompt = Some(format!(
            "{}\nThis session runs in Codex through the Claude facade. If Claude's auto-mode classifier denies an action, report the denial and its reason. There is no pending approval to answer for that denied call. If the user subsequently gives explicit authorization for that specific denied action (for example, 'I allow that' in direct response to the denial), retry that same action once through the normal tool and native reviewer so the new authorization is considered. Preserve its destination and scope; do not substitute another remote, tool, or encoding to evade review. User authorization is context for a fresh review, not an automatic allow or a permission-mode change. If the fresh review denies it again, report that outcome and offer 'Ask for approval' in this chat's permissions menu as the manual review path. Do not require a permissions-menu change before trying newly authorized work. Do not claim global permissions.allow edits or Full Access are required, and do not change permission settings yourself. Explicit native deny rules and managed restrictions still apply.\n",
            options.append_system_prompt.as_deref().unwrap_or("")
        ));
    }
    // The desktop validates inline visualization paths against this thread's
    // workspace, not the genuine Codex subprocess's plugin home.
    let visualization_dir = options.cwd.join("outputs/visualizations").join(&thread_id);
    options.append_system_prompt = Some(format!(
        "{}\nDesktop visualization output directory for this chat: {}. For inline Visualize output, create this directory as needed and save lowercase-hyphenated .html files there. Emit the absolute path in the visualization reference. Do not infer a directory under ~/.codex from the installed skill path; that home belongs to plugin discovery and is not this chat's permitted visualization directory.\n",
        options.append_system_prompt.as_deref().unwrap_or(""),
        visualization_dir.display()
    ));
    let send = |event| {
        let envelope = Event::Backend {
            thread_id: thread_id.clone(),
            turn_id: turn_id.clone(),
            event,
        };
        let events = events.clone();
        let cancel = cancel.clone();
        async move {
            tokio::select! { _ = cancel.cancelled() => false, result = events.send(envelope) => result.is_ok() }
        }
    };
    let startup_cancel = cancel.child_token();
    let mut queued_input = Vec::new();
    let spawned = {
        let future = async {
            // Two processes must never share a session.
            if let Some(previous) = &mut previous_process {
                let _ = previous.await;
            }
            if let Some(warm) = warm {
                if let Some(runtime) = &warm.plugins {
                    events
                        .send(Event::PluginsReady {
                            thread_id: thread_id.clone(),
                            turn_id: turn_id.clone(),
                            runtime: runtime.clone(),
                        })
                        .await
                        .map_err(|_| anyhow::anyhow!("Facade event loop closed"))?;
                }
                return Ok((
                    warm.backend,
                    warm.output,
                    warm.plugins,
                    warm.background,
                    warm.replay,
                ));
            }
            let mut plugin_runtime = None;
            if let Some((plugin_config, disabled, desktop)) = plugins {
                // Ask the configured Claude installation which plugins it already
                // supplies. Failure retains Codex plugin guidance rather than hiding it.
                let native_service = crate::plugins::PluginService::new(
                    config.executable.clone(),
                    None,
                    options.cwd.clone(),
                );
                let native_config = if config.protocol == BackendProtocol::Claude {
                    tokio::select! {
                        _ = startup_cancel.cancelled() => anyhow::bail!("Plugin startup cancelled"),
                        result = native_service.enabled_config() => result.unwrap_or_default(),
                    }
                } else {
                    Value::Null
                };
                let native_plugins = native_config
                    .as_object()
                    .into_iter()
                    .flatten()
                    .filter(|(_, value)| value["enabled"] == true)
                    .map(|(id, _)| id.clone())
                    .collect();
                let runtime = tokio::select! {
                    _ = startup_cancel.cancelled() => anyhow::bail!("Plugin startup cancelled"),
                    result = crate::plugin_runtime::Runtime::start_with_native_plugins(plugin_config,&disabled,&desktop,&native_plugins) => result.map_err(|error|anyhow::anyhow!("Codex plugin startup: {error}"))?,
                };
                options
                    .dynamic_tools
                    .extend(runtime.tools.values().map(|(tool, _, _)| tool.mcp_spec()));
                options.append_system_prompt = Some(format!(
                    "{}\n{}",
                    options.append_system_prompt.as_deref().unwrap_or(""),
                    runtime.instructions
                ));
                plugin_runtime = Some(runtime.clone());
                events
                    .send(Event::PluginsReady {
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        runtime,
                    })
                    .await
                    .map_err(|_| anyhow::anyhow!("Facade event loop closed"))?;
            }
            let (mut backend, output) =
                Backend::spawn_cancellable(&config, &options, startup_cancel.clone()).await?;
            tokio::select! {
                _ = backend.wait_for_mcp(MCP_CONNECT_DEADLINE) => {}
                _ = startup_cancel.cancelled() => {
                    backend.terminate().await?;
                    anyhow::bail!("Claude startup cancelled");
                }
            }
            Ok((backend, output, plugin_runtime, HashSet::new(), vec![]))
        };
        tokio::pin!(future);
        loop {
            tokio::select! {
                result = &mut future => break result,
                command = commands.recv() => {
                    match command {
                        Some(Control::UserInput(message)) => {
                            queued_input.push(message);
                            input_admission.lock().unwrap().queued -= 1;
                        }
                        Some(Control::Reply(_) | Control::GrantReply(_, _)) => {}
                        Some(Control::Interrupt) | None => {
                            input_admission.lock().unwrap().closed = true;
                            startup_cancel.cancel();
                            // Reap the spawning child before exposing a terminal state.
                            let _ = future.await;
                            send(BackendEvent::Exited { success: false, message: "Turn interrupted during Claude startup".into() }).await;
                            return;
                        }
                    }
                }
            }
        }
    };
    let (mut backend, mut output, plugin_runtime, background, replay) = match spawned {
        Ok(spawned) => spawned,
        Err(error) => {
            input_admission.lock().unwrap().closed = true;
            send(BackendEvent::Exited {
                success: false,
                message: format!("Cannot start Claude: {error:#}"),
            })
            .await;
            return;
        }
    };
    // A woken process is already working; it continues from its replayed frames.
    let prompt = (!input.is_empty()).then(|| json!({"type": "user", "uuid":protocol::id(), "session_id": options.session_id, "message": {"role": "user", "content": input}, "parent_tool_use_id": null}));
    let mut replay = std::collections::VecDeque::from(replay);
    let mut run = RunState {
        background,
        ..RunState::default()
    };
    let mut interrupt_deadline = None;
    // Only a turn that ran to Claude's idle state leaves a process fit for reuse.
    let mut idle = false;
    let run_future = async {
        for message in prompt.into_iter().chain(queued_input) {
            run.submitted(&message);
            if let Err(error) = backend.send(message).await {
                return Some(BackendEvent::Exited {
                    success: false,
                    message: format!("Cannot send Claude prompt: {error:#}"),
                });
            }
        }
        loop {
            tokio::select! {
                biased;
                command = commands.recv() => {
                    match command {
                        Some(Control::UserInput(message)) => {
                            run.submitted(&message);
                            input_admission.lock().unwrap().queued -= 1;
                            if let Err(error) = backend.send(message).await {
                                let message = format!("Cannot send Claude steering input: {error:#}");
                                return Some(BackendEvent::Exited {success:false, message});
                            }
                        }
                        Some(Control::Reply(message)) => {
                            if let Err(error) = backend.send(message).await {
                                return Some(BackendEvent::Exited { success: false, message: format!("Claude control write failed: {error:#}") });
                            }
                        }
                        Some(Control::GrantReply(message, grant)) => {
                            if let Err(error) = backend.send(message).await {
                                return Some(BackendEvent::Exited { success:false,message:format!("Claude control write failed: {error:#}") });
                            }
                            let _ = events.send(Event::SessionGrant { thread_id:thread_id.clone(),turn_id:turn_id.clone(),grant }).await;
                        }
                        Some(Control::Interrupt) => {
                            // The reader consumes the ACK. Keep draining transcript frames until
                            // Claude emits its interrupted result, instead of treating ACK as done.
                            let request = json!({"type":"control_request", "request_id":protocol::id(), "request":{"subtype":"interrupt"}});
                            if let Err(error) = backend.send(request).await {
                                return Some(BackendEvent::Exited { success: false, message: format!("Claude interrupt failed: {error:#}") });
                            }
                            interrupt_deadline = Some(tokio::time::Instant::now() + Duration::from_secs(5));
                        }
                        None => return None,
                    }
                }
                _ = async {
                    match interrupt_deadline { Some(deadline) => tokio::time::sleep_until(deadline).await, None => std::future::pending().await }
                } => return Some(BackendEvent::Exited { success: false, message: "Claude interruption timed out".into() }),
                _ = async {
                    match run.idle_deadline { Some(deadline) => tokio::time::sleep_until(deadline).await, None => std::future::pending().await }
                } => return Some(BackendEvent::Exited { success: false, message: "Claude did not become idle after its result".into() }),
                event = async {
                    match replay.pop_front() {
                        Some(frame) => Some(BackendEvent::Message(frame)),
                        None => output.recv().await,
                    }
                } => {
                    let Some(event) = event else {
                        return Some(BackendEvent::Exited { success: false, message: "Claude event stream closed".into() });
                    };
                    match event {
                        BackendEvent::Exited { success, message } => {
                            if let Some(result) = run.result.take() {
                                // Preserve a reported API error instead of reducing it to exit 1.
                                if result["is_error"] == true || (success && run.tasks.is_empty() && run.pending_input.is_empty()) { return Some(BackendEvent::Message(result)); }
                            }
                            return Some(BackendEvent::Exited { success, message });
                        }
                        BackendEvent::Message(message) => {
                            run.observe(&message);
                            if message["type"] == "result" {
                                if message["is_error"] == true || interrupt_deadline.is_some() { return Some(BackendEvent::Message(message)); }
                            } else if !send(BackendEvent::Message(message)).await { return None; }
                            if run.finished() && input_admission.lock().unwrap().close_if_drained() {
                                idle = true;
                                return run.result.take().map(BackendEvent::Message);
                            }
                        }
                    }
                }
            }
        }
    };
    let terminal = tokio::select! { _ = cancel.cancelled() => None, result = run_future => result };
    input_admission.lock().unwrap().closed = true;
    // Background work outlives the turn, so its process is kept even when it
    // cannot serve later turns.
    if (park || !run.background.is_empty()) && idle && !cancel.is_cancelled() {
        let session = crate::warm::WarmSession {
            backend,
            output,
            plugins: plugin_runtime,
            background: std::mem::take(&mut run.background),
            replay: vec![],
        };
        // Reserve first so a cancelled delivery still closes the session gracefully.
        let permit = tokio::select! { _ = cancel.cancelled() => None, permit = events.reserve() => permit.ok() };
        match permit {
            Some(permit) => {
                permit.send(Event::Parked {
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    session,
                });
                if let Some(event) = terminal {
                    send(event).await;
                }
            }
            None => session.close().await,
        }
        return;
    }
    let cleanup = if cancel.is_cancelled()
        || !matches!(&terminal, Some(BackendEvent::Message(v)) if v["type"] == "result")
    {
        backend.terminate().await
    } else {
        backend.shutdown().await
    };
    // Release the process and session lock before exposing a terminal frontend state.
    if let Err(error) = cleanup {
        send(BackendEvent::Exited {
            success: false,
            message: format!("Claude cleanup failed: {error:#}"),
        })
        .await;
    } else if let Some(event) = terminal {
        send(event).await;
    }
}

/// Mirrors the official SDK's result-versus-run distinction. A Codex turn includes
/// any Claude continuation owed by a background agent before the session becomes idle.
#[derive(Default)]
struct RunState {
    state: Option<String>,
    tasks: HashSet<String>,
    /// Every running background task, including ones that do not hold the turn open.
    background: HashSet<String>,
    result: Option<Value>,
    idle_deadline: Option<tokio::time::Instant>,
    pending_input: HashSet<String>,
}
impl RunState {
    fn submitted(&mut self, message: &Value) {
        self.pending_input
            .insert(message["uuid"].as_str().unwrap().to_owned());
        self.result = None;
        self.idle_deadline = None;
    }

    fn observe(&mut self, message: &Value) {
        crate::warm::track_background(&mut self.background, message);
        if !message["parent_tool_use_id"].is_null() {
            return;
        }
        match message["type"].as_str() {
            Some("user") if message["isReplay"] == true => {
                if let Some(id) = message["uuid"].as_str()
                    && self.pending_input.remove(id)
                {
                    // Claude may batch several user messages into one API run.
                    // Only a result after their consumption acknowledgements can
                    // finish this facade turn; counting results would be incorrect.
                    self.result = None;
                }
            }
            Some("system") => match message["subtype"].as_str() {
                Some("session_state_changed") => {
                    self.state = message["state"].as_str().map(str::to_owned)
                }
                Some("task_started")
                    if matches!(
                        message["task_type"].as_str(),
                        Some("local_agent" | "local_workflow")
                    ) =>
                {
                    if let Some(id) = message["task_id"].as_str() {
                        self.tasks.insert(id.to_owned());
                    }
                }
                Some("task_notification") => {
                    if let Some(id) = message["task_id"].as_str() {
                        self.tasks.remove(id);
                    }
                }
                Some("task_updated")
                    if matches!(
                        message["patch"]["status"].as_str(),
                        Some("completed" | "failed" | "killed" | "stopped")
                    ) =>
                {
                    if let Some(id) = message["task_id"].as_str() {
                        self.tasks.remove(id);
                    }
                }
                _ => {}
            },
            Some("result") => self.result = Some(message.clone()),
            Some("assistant" | "stream_event") => {
                self.result = None;
            }
            _ => {}
        }
        if self.result.is_some()
            && (!self.pending_input.is_empty()
                || (self.tasks.is_empty() && self.state.as_deref() == Some("running")))
        {
            self.idle_deadline
                .get_or_insert_with(|| tokio::time::Instant::now() + Duration::from_secs(600));
        } else {
            self.idle_deadline = None;
        }
    }
    fn finished(&self) -> bool {
        self.result.is_some()
            && self.pending_input.is_empty()
            && self.tasks.is_empty()
            && matches!(self.state.as_deref(), None | Some("idle"))
    }
}
