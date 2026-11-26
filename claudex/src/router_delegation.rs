//! Router-owned control plane for both native agent adapters.
use super::*;
use crate::delegation::{self, Agent, Bridge, Harness, McpCall, Registry};
use std::time::Duration;
use tokio::time::Instant;

impl From<Harness> for Side {
    fn from(harness: Harness) -> Self {
        match harness {
            Harness::Codex => Self::Codex,
            Harness::Claude => Self::Claude,
        }
    }
}

#[derive(Clone)]
enum Reply {
    Dynamic {
        side: Side,
        id: Value,
    },
    Mcp {
        id: Value,
        sender: mpsc::Sender<Value>,
    },
}

struct Call {
    parent: String,
    parent_turn: String,
    /// The causal root of `parent_turn`, which child turns are attributed to.
    parent_root: String,
    side: Side,
    name: String,
    arguments: Value,
    reply: Reply,
    deadline: Instant,
    child: Option<String>,
    queued_count: usize,
}

/// A call that arrived before its parent's turn/started notification. The
/// native MCP endpoint and the backend's notification stream are separate
/// channels, so the call can win the race.
struct EarlyCall {
    side: Side,
    parent: String,
    name: String,
    arguments: Value,
    reply: Reply,
    deadline: Instant,
}

/// How long an early call waits for its parent turn to be announced.
const EARLY_CALL_GRACE: Duration = Duration::from_secs(10);

pub(super) enum Step {
    Models {
        call: String,
    },
    Snapshot {
        call: String,
    },
    Started {
        call: String,
        agent: Box<Agent>,
        scope: Option<String>,
        expected: Value,
    },
    TurnStarted {
        call: String,
        child: String,
    },
    Resumed {
        call: String,
        child: String,
        scope: Option<String>,
    },
    Recovered {
        child: String,
    },
    Controlled {
        call: String,
        child: String,
    },
    Interrupted {
        child: String,
    },
}

#[derive(Default)]
pub(super) struct State {
    pub registry: Registry,
    pub calls: Option<mpsc::Sender<McpCall>>,
    pub scopes: HashMap<String, String>,
    pub attached: HashSet<String>,
    pub snapshots: HashMap<String, Value>,
    stale_snapshots: HashSet<String>,
    bridges: HashMap<String, Bridge>,
    active: HashMap<String, String>,
    /// Root turn ids of the `active` turns that reported one.
    roots: HashMap<String, String>,
    pending: HashMap<String, Call>,
    early: Vec<EarlyCall>,
    owned: HashSet<String>,
    shutting_down: HashSet<String>,
    dynamic_replies: VecDeque<(Side, Value, Value)>,
    recovered: bool,
}

impl State {
    pub fn has_shutdown_work(&self) -> bool {
        !self.shutting_down.is_empty()
    }
}

impl Router {
    /// Codex receives a real scoped stdio MCP server; Claude uses the existing
    /// native SDK MCP transport. Neither path replaces native same-harness tools.
    pub(super) fn prepare_agent_tools(
        &mut self,
        side: Side,
        message: &mut Value,
    ) -> Result<Option<String>> {
        if side == Side::Acp
            || !self.facades.contains_key(&Side::Claude)
            || !matches!(
                message["method"].as_str(),
                Some("thread/start" | "thread/resume")
            )
        {
            return Ok(None);
        }
        let resume = message["method"] == "thread/resume";
        let params = &mut message["params"];
        if side == Side::Claude {
            if params["permissions"] == ":read-only"
                || params["sandbox"] == "read-only"
                || params["sandboxPolicy"]["type"] == "readOnly"
            {
                return Ok(None);
            }
            // An absent resume catalog means "preserve", not "replace with only
            // delegation". The facade's attach method merges saved descriptors.
            if resume && !params["dynamicTools"].is_array() {
                return Ok(None);
            }
            let tools = params["dynamicTools"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            if tools
                .iter()
                .any(|tool| tool["name"] == delegation::NAMESPACE)
            {
                anyhow::bail!("The claudex_agents namespace is reserved by the router");
            }
            let mut tools = tools;
            tools.push(delegation::dynamic_spec());
            params["dynamicTools"] = json!(tools);
            return Ok(None);
        }
        // Desktop code-mode clients also need model-facing declarations. Publish
        // the catalog on their dynamic-tool boundary using the same dispatcher.
        // The stdio MCP endpoint remains available to ordinary Codex clients.
        if !resume {
            let mut tools = params["dynamicTools"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            if tools
                .iter()
                .any(|tool| tool["name"] == delegation::NAMESPACE)
            {
                anyhow::bail!("The claudex_agents namespace is reserved by the router");
            }
            tools.push(delegation::dynamic_spec());
            params["dynamicTools"] = json!(tools);
        }
        if let Some(thread) = params["threadId"].as_str()
            && self.agents.attached.contains(thread)
        {
            return Ok(None);
        }
        let Some(calls) = self.agents.calls.clone() else {
            return Ok(None);
        };
        let config = params.get_mut("config");
        if config
            .as_deref()
            .is_some_and(|config| config.get("mcp_servers.claudex_agents").is_some())
        {
            anyhow::bail!("The claudex_agents MCP server name is reserved by the router");
        }
        let scope = uuid::Uuid::new_v4().to_string();
        let bridge = Bridge::bind(scope.clone(), calls)?;
        if !params["config"].is_object() {
            params["config"] = json!({});
        }
        params["config"]["mcp_servers.claudex_agents"] = bridge.config()?;
        self.agents.bridges.insert(scope.clone(), bridge);
        Ok(Some(scope))
    }

    pub(super) fn agent_mcp_call(&mut self, call: McpCall) {
        self.refresh_local_provider_routing();
        let reply = Reply::Mcp {
            id: call.message["id"].clone(),
            sender: call.reply,
        };
        let Some(parent) = self.agents.scopes.get(&call.scope).cloned() else {
            self.agent_reply(reply, Err("Delegation scope has no native thread".into()));
            return;
        };
        // Native Codex supplies this metadata outside model-controlled arguments.
        // A same-harness child may inherit its parent's MCP configuration; that
        // endpoint must not let it delegate with the parent's permissions.
        let native_parent = self
            .local_provider
            .as_ref()
            .map(|local_provider| local_provider.native_id(&parent))
            .unwrap_or(&parent);
        if call.message["params"]["_meta"]["threadId"] != native_parent {
            self.agent_reply(
                reply,
                Err("Native MCP caller does not match its delegation scope".into()),
            );
            return;
        }
        self.agent_call(
            Side::Codex,
            parent,
            call.message["params"]["name"]
                .as_str()
                .unwrap_or("")
                .to_owned(),
            call.message["params"]["arguments"].clone(),
            reply,
        );
    }

    pub(super) fn agent_dynamic_call(&mut self, side: Side, message: &Value) -> bool {
        if side == Side::Acp
            || message["method"] != "item/tool/call"
            || message["params"]["namespace"] != delegation::NAMESPACE
        {
            return false;
        }
        let id = message["id"].clone();
        if let Some((_, _, result)) = self
            .agents
            .dynamic_replies
            .iter()
            .find(|(s, request, _)| *s == side && *request == id)
        {
            self.send_backend(side, json!({"id":id,"result":result.clone()}));
            return true;
        }
        if self.agents.pending.values().any(|call| matches!(&call.reply, Reply::Dynamic{side:s,id:request} if *s == side && *request == id)) { return true; }
        self.agent_call(
            side,
            message["params"]["threadId"]
                .as_str()
                .unwrap_or("")
                .to_owned(),
            message["params"]["tool"].as_str().unwrap_or("").to_owned(),
            message["params"]["arguments"].clone(),
            Reply::Dynamic { side, id },
        );
        true
    }

    fn agent_reply(&mut self, reply: Reply, result: Result<Value, String>) {
        let (success, text) = match result {
            Ok(value) => (true, value.to_string()),
            Err(error) => (false, error),
        };
        match reply {
            Reply::Dynamic { side, id } => {
                let result =
                    json!({"success":success,"contentItems":[{"type":"inputText","text":text}]});
                self.agents
                    .dynamic_replies
                    .push_back((side, id.clone(), result.clone()));
                if self.agents.dynamic_replies.len() > 128 {
                    self.agents.dynamic_replies.pop_front();
                }
                self.send_backend(side, json!({"id":id,"result":result}));
            }
            Reply::Mcp { id, sender } => {
                if sender
                    .try_send(json!({"jsonrpc":"2.0","id":id,"result":{
                    "content":[{"type":"text","text":text}],"isError":!success}}))
                    .is_err()
                {
                    note!("Delegation MCP caller disconnected or stopped reading");
                }
            }
        }
    }

    fn finish_agent_call(&mut self, id: &str, result: Result<Value, String>) {
        if let Some(call) = self.agents.pending.remove(id) {
            self.agent_reply(call.reply, result);
        }
    }

    fn agent_request(&mut self, side: Side, method: &str, params: Value, step: Step) {
        self.internal_request(
            side,
            method,
            params,
            Continuation::Delegation { side, step },
        );
    }

    fn agent_call(
        &mut self,
        side: Side,
        parent: String,
        name: String,
        arguments: Value,
        reply: Reply,
    ) {
        let validation = delegation::validate(&name, &arguments).and_then(|_| {
            if self
                .threads
                .get(&parent)
                .is_some_and(|owner| *owner != side)
            {
                anyhow::bail!("Delegation caller belongs to another harness");
            }
            if side == Side::Claude && !self.claude_available() {
                anyhow::bail!("The Claude service is reconnecting");
            }
            Ok(())
        });
        if let Err(error) = validation {
            self.agent_reply(reply, Err(error.to_string()));
            return;
        }
        if !self.agents.active.contains_key(&parent) {
            self.agents.early.push(EarlyCall {
                side,
                parent,
                name,
                arguments,
                reply,
                deadline: Instant::now() + EARLY_CALL_GRACE,
            });
            return;
        }
        let id = uuid::Uuid::new_v4().to_string();
        let timeout = if name == "wait_agent" {
            arguments["timeout_ms"].as_u64().unwrap_or(60_000)
        } else {
            120_000
        };
        let parent_turn = self.agents.active[&parent].clone();
        // A turn without a reported root (older backends) is its own root.
        let parent_root = self
            .agents
            .roots
            .get(&parent)
            .cloned()
            .unwrap_or_else(|| parent_turn.clone());
        self.agents.pending.insert(
            id.clone(),
            Call {
                parent: parent.clone(),
                parent_turn,
                parent_root,
                side,
                name: name.clone(),
                arguments: arguments.clone(),
                reply,
                deadline: Instant::now() + Duration::from_millis(timeout),
                child: None,
                queued_count: 0,
            },
        );
        if name == "spawn_agent" {
            let root = self.agents.registry.root(&parent);
            let running = self
                .agents
                .registry
                .agents
                .values()
                .filter(|agent| agent.root == root && agent.running())
                .count();
            let starting = self
                .agents
                .pending
                .values()
                .filter(|call| {
                    call.name == "spawn_agent" && self.agents.registry.root(&call.parent) == root
                })
                .count();
            let depth = self
                .agents
                .registry
                .agents
                .get(&parent)
                .map_or(1, |agent| agent.depth + 1);
            if depth > delegation::MAX_DEPTH || running + starting > delegation::MAX_RUNNING {
                self.finish_agent_call(
                    &id,
                    Err("Delegation depth or concurrency limit reached".into()),
                );
                return;
            }
            let target = if arguments["harness"] == "claude" {
                Side::Claude
            } else {
                Side::Codex
            };
            if target == Side::Claude && !self.claude_available() {
                self.finish_agent_call(&id, Err("The Claude service is reconnecting".into()));
                return;
            }
            self.agent_request(
                target,
                "model/list",
                json!({"limit":1000}),
                Step::Models { call: id },
            );
            return;
        }
        if name == "list_agents" {
            let root = self.agents.registry.root(&parent);
            let agents: Vec<Value> = self
                .agents
                .registry
                .agents
                .values()
                .filter(|agent| agent.root == root)
                .map(Agent::state)
                .collect();
            self.finish_agent_call(&id, Ok(json!({"agents":agents})));
            return;
        }
        let target = arguments["agent_id"].as_str().unwrap_or("");
        let agent = match self.agents.registry.target(&parent, target) {
            Ok(agent) => agent.clone(),
            Err(error) => {
                self.finish_agent_call(&id, Err(error.to_string()));
                return;
            }
        };
        if name == "wait_agent" {
            if !agent.running() || timeout == 0 {
                self.finish_agent_call(&id, Ok(agent.state()));
            }
            return;
        }
        if name == "interrupt_agent" {
            self.interrupt_descendants(&agent.id);
            self.interrupt_agent_native(&agent.id);
            self.finish_agent_call(&id, Ok(self.agents.registry.agents[&agent.id].state()));
            return;
        }
        if name == "send_message" && !agent.running() {
            let entry = self.agents.registry.agents.get_mut(&agent.id).unwrap();
            entry
                .queued
                .push(arguments["message"].as_str().unwrap().to_owned());
            let updated = entry.clone();
            let result = self
                .agents
                .registry
                .save(&updated)
                .map(|_| json!({"agent_id":agent.id,"queued":true}))
                .map_err(|e| e.to_string());
            self.finish_agent_call(&id, result);
            return;
        }
        if agent.running() {
            let Some(turn) = &agent.turn_id else {
                self.finish_agent_call(
                    &id,
                    Err("Agent is still starting; retry after it starts".into()),
                );
                return;
            };
            self.agent_request(
                agent.harness.into(),
                "turn/steer",
                json!({"threadId":agent.id,"expectedTurnId":turn,
                "input":text_input(arguments["message"].as_str().unwrap())}),
                Step::Controlled {
                    call: id,
                    child: agent.id,
                },
            );
        } else {
            let mut message = json!({"method":"thread/resume","params":{"threadId":agent.id}});
            let scope = match self.prepare_agent_tools(agent.harness.into(), &mut message) {
                Ok(scope) => scope,
                Err(error) => {
                    self.finish_agent_call(&id, Err(error.to_string()));
                    return;
                }
            };
            self.agent_request(
                agent.harness.into(),
                "thread/resume",
                message["params"].clone(),
                Step::Resumed {
                    call: id,
                    child: agent.id,
                    scope,
                },
            );
        }
    }

    pub(super) fn agent_continue(&mut self, side: Side, step: Step, result: Result<Value, Value>) {
        let result = result.map_err(|error| {
            error["message"]
                .as_str()
                .unwrap_or("Native agent request failed")
                .to_owned()
        });
        match step {
            Step::Models { call } => {
                let Some(pending) = self.agents.pending.get(&call) else {
                    return;
                };
                match result {
                    Ok(catalog)
                        if catalog["data"].as_array().is_some_and(|models| {
                            models.iter().any(|model| {
                                model["id"] == pending.arguments["model"]
                                    || model["model"] == pending.arguments["model"]
                            })
                        }) =>
                    {
                        let parent = pending.parent.clone();
                        let source = pending.side;
                        if let Some(snapshot) = self
                            .agents
                            .snapshots
                            .get(&parent)
                            .filter(|_| !self.agents.stale_snapshots.contains(&parent))
                            .cloned()
                        {
                            self.agent_continue(source, Step::Snapshot { call }, Ok(snapshot));
                        } else if source == Side::Claude {
                            self.agent_request(
                                source,
                                "thread/resume",
                                json!({"threadId":parent,"excludeTurns":true}),
                                Step::Snapshot { call },
                            );
                        } else {
                            self.finish_agent_call(&call,Err("The native server has not confirmed the parent's current permissions; refresh its settings between turns before delegating".into()));
                        }
                    }
                    Ok(_) => self.finish_agent_call(
                        &call,
                        Err(
                            "Requested model is not in the selected native harness's catalog"
                                .into(),
                        ),
                    ),
                    Err(error) => self.finish_agent_call(&call, Err(error)),
                }
            }
            Step::Snapshot { call } => {
                let Some(pending) = self.agents.pending.get(&call) else {
                    return;
                };
                let snapshot = match result {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        self.finish_agent_call(&call, Err(error));
                        return;
                    }
                };
                let harness: Harness =
                    serde_json::from_value(pending.arguments["harness"].clone()).unwrap();
                let params = delegation::child_params(
                    &snapshot,
                    harness,
                    pending.arguments["model"].as_str().unwrap(),
                    pending.arguments["effort"].as_str(),
                );
                let params = match params {
                    Ok(params) => params,
                    Err(error) => {
                        self.finish_agent_call(&call, Err(error.to_string()));
                        return;
                    }
                };
                let agent = Agent {
                    id: String::new(),
                    parent: pending.parent.clone(),
                    root: self.agents.registry.root(&pending.parent).to_owned(),
                    harness,
                    name: pending.arguments["name"]
                        .as_str()
                        .unwrap_or("Delegated agent")
                        .to_owned(),
                    depth: self
                        .agents
                        .registry
                        .agents
                        .get(&pending.parent)
                        .map_or(1, |agent| agent.depth + 1),
                    thread: json!({}),
                    turn_id: None,
                    status: "starting".into(),
                    report: None,
                    queued: vec![],
                    spawn_parent_turn: pending.parent_turn.clone(),
                    spawn_item: json!({}),
                    owner_pid: std::process::id(),
                    archived: false,
                };
                let mut message = json!({"method":"thread/start","params":params});
                let scope = match self.prepare_agent_tools(harness.into(), &mut message) {
                    Ok(scope) => scope,
                    Err(error) => {
                        self.finish_agent_call(&call, Err(error.to_string()));
                        return;
                    }
                };
                let expected = match delegation::permissions(&snapshot) {
                    Ok(expected) => expected,
                    Err(error) => {
                        self.finish_agent_call(&call, Err(error.to_string()));
                        return;
                    }
                };
                self.agent_request(
                    harness.into(),
                    "thread/start",
                    message["params"].clone(),
                    Step::Started {
                        call,
                        agent: Box::new(agent),
                        scope,
                        expected,
                    },
                );
            }
            Step::Started {
                call,
                agent,
                scope,
                expected,
            } => {
                let mut agent = *agent;
                let snapshot = match result {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        self.finish_agent_call(&call, Err(error));
                        return;
                    }
                };
                let Some(thread) = snapshot["thread"]["id"].as_str() else {
                    self.finish_agent_call(&call, Err("Native spawn returned no thread id".into()));
                    return;
                };
                agent.id = thread.to_owned();
                agent.thread = snapshot["thread"].clone();
                agent.spawn_item = json!({"type":"collabAgentToolCall","id":call,"tool":"spawnAgent","status":"completed",
                    "senderThreadId":agent.parent,"receiverThreadIds":[agent.id],"prompt":self.agents.pending.get(&call).map(|p|&p.arguments["message"]),
                    "model":snapshot["model"],"reasoningEffort":snapshot["reasoningEffort"],"agentsStates":{agent.id.clone():{"status":"running","message":null}}});
                if let Some(scope) = scope {
                    self.agents.scopes.insert(scope, agent.id.clone());
                    self.agents.attached.insert(agent.id.clone());
                }
                self.threads.insert(agent.id.clone(), side);
                if side == Side::Claude {
                    self.facade_loaded.insert(agent.id.clone());
                }
                let child = agent.id.clone();
                if let Err(error) = self.agents.registry.save(&agent) {
                    self.finish_agent_call(&call, Err(error.to_string()));
                    return;
                }
                self.agents.registry.agents.insert(child.clone(), agent);
                self.agents.owned.insert(child.clone());
                self.emit_agent_spawn(&child);
                self.internal_request(
                    side,
                    "thread/name/set",
                    json!({"threadId":child,"name":self.agents.registry.agents[&child].name}),
                    Continuation::Ignore,
                );
                if delegation::permissions(&snapshot).ok().as_ref() != Some(&expected) {
                    let failure = "Native child settings differ from the parent's effective permissions; no task was submitted";
                    self.mark_agent(&child, "errored", Some(failure.into()));
                    self.finish_agent_call(&call, Err(failure.into()));
                    return;
                }
                let Some(pending) = self.agents.pending.get_mut(&call) else {
                    self.mark_agent(
                        &child,
                        "interrupted",
                        Some("Parent stopped before child execution began".into()),
                    );
                    return;
                };
                pending.child = Some(child.clone());
                let params =
                    pending.child_turn(&child, pending.arguments["message"].as_str().unwrap());
                self.agent_request(
                    side,
                    "turn/start",
                    params,
                    Step::TurnStarted { call, child },
                );
            }
            Step::TurnStarted { call, child } => match result {
                Ok(result) => {
                    if let Some(turn) = result["turn"]["id"].as_str()
                        && let Some(agent) = self.agents.registry.agents.get_mut(&child)
                    {
                        agent.turn_id = Some(turn.into());
                        if agent.running() {
                            agent.status = "running".into();
                            self.agents.active.insert(child.clone(), turn.into());
                        }
                    }
                    if let Some(pending) = self.agents.pending.get(&call) {
                        if let Some(agent) = self.agents.registry.agents.get_mut(&child) {
                            agent.queued.drain(..pending.queued_count);
                        }
                        if let Some(agent) = self.agents.registry.agents.get(&child) {
                            let _ = self.agents.registry.save(agent);
                        }
                    }
                    if self.agents.pending.contains_key(&call) {
                        self.finish_agent_call(
                            &call,
                            Ok(self.agents.registry.agents[&child].state()),
                        );
                    } else {
                        self.interrupt_agent_native(&child);
                    }
                }
                Err(error) => {
                    self.mark_agent(&child, "errored", Some(error.clone()));
                    self.finish_agent_call(&call, Err(error));
                }
            },
            Step::Resumed { call, child, scope } => {
                let Some(pending) = self.agents.pending.get_mut(&call) else {
                    return;
                };
                if let Err(error) = result {
                    self.finish_agent_call(&call, Err(error));
                    return;
                }
                if let Some(scope) = scope {
                    self.agents.scopes.insert(scope, child.clone());
                    self.agents.attached.insert(child.clone());
                }
                if side == Side::Claude {
                    self.facade_loaded.insert(child.clone());
                }
                let agent = self.agents.registry.agents.get_mut(&child).unwrap();
                let mut messages = agent.queued.clone();
                pending.queued_count = messages.len();
                pending.child = Some(child.clone());
                messages.push(pending.arguments["message"].as_str().unwrap().to_owned());
                agent.status = "starting".into();
                agent.report = None;
                agent.turn_id = None;
                agent.owner_pid = std::process::id();
                let params = pending.child_turn(&child, &messages.join("\n\n"));
                self.agents.owned.insert(child.clone());
                self.agent_request(
                    side,
                    "turn/start",
                    params,
                    Step::TurnStarted { call, child },
                );
            }
            Step::Controlled { call, child } => {
                self.finish_agent_call(
                    &call,
                    result.map(|_| self.agents.registry.agents[&child].state()),
                );
            }
            Step::Interrupted { child } => {
                if let Err(error) = result {
                    self.agents.shutting_down.remove(&child);
                    self.mark_agent(
                        &child,
                        "errored",
                        Some(format!("Native interruption failed: {error}")),
                    );
                }
            }
            Step::Recovered { child } => match result {
                Ok(snapshot) => {
                    let turn = snapshot["thread"]["turns"]
                        .as_array()
                        .and_then(|turns| turns.last());
                    if let Some(turn) = turn {
                        if turn["status"] == "inProgress"
                            && snapshot["thread"]["status"]["type"] == "active"
                        {
                            if let Some(agent) = self.agents.registry.agents.get_mut(&child) {
                                agent.turn_id = turn["id"].as_str().map(str::to_owned);
                            }
                            self.interrupt_agent_native(&child);
                        } else {
                            let status = if turn["status"] == "completed" {
                                "completed"
                            } else {
                                "interrupted"
                            };
                            self.mark_agent(&child, status, Some(turn_report(turn)));
                        }
                    } else {
                        self.mark_agent(
                            &child,
                            "interrupted",
                            Some("Router stopped before a native turn was recorded".into()),
                        );
                    }
                }
                Err(error) => self.mark_agent(&child, "errored", Some(error)),
            },
        }
    }

    fn emit_agent_spawn(&mut self, id: &str) {
        let agent = &self.agents.registry.agents[id];
        let mut thread = agent.thread.clone();
        agent.decorate(&mut thread);
        self.send_client(json!({"method":"thread/started","params":{"thread":thread}}));
        self.send_client(json!({"method":"item/started","params":{"threadId":agent.parent,"turnId":agent.spawn_parent_turn,
            "item":agent.spawn_item,"startedAtMs":crate::protocol::now_ms()}}));
        self.send_client(json!({"method":"item/completed","params":{"threadId":agent.parent,"turnId":agent.spawn_parent_turn,
            "item":agent.spawn_item,"completedAtMs":crate::protocol::now_ms()}}));
    }

    fn mark_agent(&mut self, id: &str, status: &str, report: Option<String>) {
        let Some(agent) = self.agents.registry.agents.get_mut(id) else {
            return;
        };
        agent.status = status.into();
        agent.report = report;
        if !agent.running() {
            agent.thread["status"] = json!({"type":"idle"});
        }
        let snapshot = agent.clone();
        if let Err(error) = self.agents.registry.save(&snapshot) {
            note!("Cannot persist delegation state: {error:#}");
        }
        let waits: Vec<String> = self
            .agents
            .pending
            .iter()
            .filter(|(_, call)| call.name == "wait_agent" && call.arguments["agent_id"] == id)
            .map(|(id, _)| id.clone())
            .collect();
        if !snapshot.running() {
            for wait in waits {
                self.finish_agent_call(&wait, Ok(snapshot.state()));
            }
        }
    }

    pub(super) fn agent_notification(&mut self, _side: Side, message: &Value) {
        let p = &message["params"];
        let thread = p["threadId"].as_str().or(p["thread"]["id"].as_str());
        let Some(thread) = thread else {
            return;
        };
        match message["method"].as_str() {
            Some("thread/settings/updated") => {
                let settings = &p["threadSettings"];
                // Only a complete native report can confirm effective permissions.
                // A missing/malformed report must not revive a stale snapshot.
                if !settings["cwd"].is_string()
                    || settings["approvalPolicy"].is_null()
                    || !settings["approvalsReviewer"].is_string()
                    || !settings["sandboxPolicy"].is_object()
                {
                    return;
                }
                if let Some(snapshot) = self.agents.snapshots.get_mut(thread) {
                    for field in [
                        "cwd",
                        "approvalPolicy",
                        "approvalsReviewer",
                        "disabledPluginIds",
                        "activePermissionProfile",
                    ] {
                        if let Some(value) = settings.get(field) {
                            snapshot[field] = value.clone();
                        }
                    }
                    if let Some(policy) = settings.get("sandboxPolicy") {
                        snapshot["sandbox"] = policy.clone();
                    }
                    self.agents.stale_snapshots.remove(thread);
                }
            }
            Some("turn/started") => {
                if let Some(turn) = p["turn"]["id"].as_str() {
                    self.agents.active.insert(thread.into(), turn.into());
                    match p["turn"]["rootTurnId"].as_str() {
                        Some(root) => self.agents.roots.insert(thread.into(), root.into()),
                        None => self.agents.roots.remove(thread),
                    };
                    let (ready, waiting) = std::mem::take(&mut self.agents.early)
                        .into_iter()
                        .partition(|call| call.parent == thread);
                    self.agents.early = waiting;
                    for call in ready {
                        self.agent_call(
                            call.side,
                            call.parent,
                            call.name,
                            call.arguments,
                            call.reply,
                        );
                    }
                    if let Some(agent) = self.agents.registry.agents.get_mut(thread) {
                        agent.turn_id = Some(turn.into());
                        agent.status = "running".into();
                    }
                    if let Some(agent) = self.agents.registry.agents.get(thread) {
                        let _ = self.agents.registry.save(agent);
                    }
                }
            }
            Some("turn/completed") => {
                self.agents.shutting_down.remove(thread);
                if self
                    .agents
                    .active
                    .get(thread)
                    .is_some_and(|turn| p["turn"]["id"] == *turn)
                {
                    self.agents.active.remove(thread);
                    self.agents.roots.remove(thread);
                }
                if self.agents.registry.agents.contains_key(thread) {
                    let status = match p["turn"]["status"].as_str() {
                        Some("completed") => "completed",
                        Some("interrupted") => "interrupted",
                        _ => "errored",
                    };
                    let text: Vec<&str> = p["turn"]["items"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter(|item| item["type"] == "agentMessage")
                        .filter_map(|item| item["text"].as_str())
                        .collect();
                    let report = if status == "errored" {
                        p["turn"]["error"]["message"].as_str().map(str::to_owned)
                    } else {
                        Some(text.join("\n\n"))
                    };
                    self.mark_agent(thread, status, report);
                }
                self.cancel_parent_calls(thread);
            }
            Some("thread/started") => {
                if let Some(agent) = self.agents.registry.agents.get_mut(thread) {
                    agent.thread = p["thread"].clone();
                }
            }
            Some("thread/status/changed") => {
                if let Some(agent) = self.agents.registry.agents.get_mut(thread) {
                    agent.thread["status"] = p["status"].clone();
                }
            }
            Some(method @ ("thread/archived" | "thread/unarchived")) => {
                if let Some(agent) = self.agents.registry.agents.get_mut(thread) {
                    agent.archived = method == "thread/archived";
                }
                if let Some(agent) = self.agents.registry.agents.get(thread) {
                    let _ = self.agents.registry.save(agent);
                }
            }
            _ => {}
        }
    }

    pub(super) fn decorate_agent_message(&self, message: &mut Value) {
        if let Some(turn) = message.pointer_mut("/params/turn") {
            self.decorate_agent_turn(turn);
        }
        if let Some(thread) = message.pointer_mut("/params/thread") {
            self.agents.registry.decorate(thread);
        }
        if let Some(thread) = message.pointer_mut("/result/thread") {
            self.agents.registry.decorate(thread);
            if let Some(turns) = thread["turns"].as_array_mut() {
                for turn in turns {
                    self.decorate_agent_turn(turn);
                }
            }
        }
    }

    fn decorate_agent_turn(&self, turn: &mut Value) {
        let Some(id) = turn["id"].as_str().map(str::to_owned) else {
            return;
        };
        if let Some(items) = turn["items"].as_array_mut() {
            for agent in self
                .agents
                .registry
                .agents
                .values()
                .filter(|agent| agent.spawn_parent_turn == id)
            {
                if !items
                    .iter()
                    .any(|item| item["id"] == agent.spawn_item["id"])
                {
                    items.push(agent.spawn_item.clone());
                }
            }
        }
    }

    pub(super) fn decorate_agent_pages(&self, method: &str, _params: &Value, message: &mut Value) {
        if method == "thread/turns/list"
            && let Some(turns) = message["result"]["data"].as_array_mut()
        {
            for turn in turns {
                self.decorate_agent_turn(turn);
            }
        }
        if let Some(turns) = message
            .pointer_mut("/result/initialTurnsPage/data")
            .and_then(Value::as_array_mut)
        {
            for turn in turns {
                self.decorate_agent_turn(turn);
            }
        }
    }

    pub(super) fn decorate_agent_listing(&self, params: &Value, result: &mut Value) {
        let parent = params["parentThreadId"].as_str();
        let ancestor = params["ancestorThreadId"].as_str();
        let kinds = params["sourceKinds"]
            .as_array()
            .filter(|kinds| !kinds.is_empty());
        let include_children = kinds.map_or(parent.is_some() || ancestor.is_some(), |kinds| {
            kinds
                .iter()
                .any(|kind| kind == "subAgent" || kind == "subAgentThreadSpawn")
        });
        let Some(data) = result["data"].as_array_mut() else {
            return;
        };
        for item in data.iter_mut() {
            let thread = if item["thread"].is_object() {
                &mut item["thread"]
            } else {
                item
            };
            self.agents.registry.decorate(thread);
        }
        data.retain(|item| {
            let thread = list_thread(item);
            !self
                .agents
                .registry
                .agents
                .contains_key(thread["id"].as_str().unwrap_or(""))
                || include_children
        });
        if !include_children {
            return;
        }
        // Backend relation indexes cannot traverse another harness. The shared
        // registry supplies those edges while native indexes retain native ones.
        for agent in self.agents.registry.agents.values() {
            // Delegated children are ordinary threads, never attached ones.
            if !params["attachedToThreadId"].is_null()
                || params["excludedThreadIds"]
                    .as_array()
                    .is_some_and(|ids| ids.contains(&json!(agent.id)))
                || parent.is_some_and(|parent| agent.parent != parent)
                || ancestor
                    .is_some_and(|ancestor| !self.agents.registry.related(agent, ancestor, true))
                || agent.archived != params["archived"].as_bool().unwrap_or(false)
                || agent.thread["ephemeral"] == true
                || params["projectId"]
                    .as_str()
                    .is_some_and(|project| agent.thread["projectId"] != project)
                || params
                    .as_object()
                    .is_some_and(|p| p.contains_key("projectId"))
                    && params["projectId"].is_null()
                    && !agent.thread["projectId"].is_null()
                || params["cwd"]
                    .as_str()
                    .is_some_and(|cwd| agent.thread["cwd"] != cwd)
                || params["cwd"]
                    .as_array()
                    .is_some_and(|roots| !roots.contains(&agent.thread["cwd"]))
                || params["modelProviders"]
                    .as_array()
                    .is_some_and(|providers| {
                        !providers.is_empty() && !providers.contains(&agent.thread["modelProvider"])
                    })
                || params["originators"].as_array().is_some_and(|originators| {
                    !originators.is_empty() && !originators.contains(&agent.thread["originator"])
                })
                || params
                    .get("sectionId")
                    .is_some_and(|section| *section != agent.thread["section"]["id"])
                || data.iter().any(|item| list_thread(item)["id"] == agent.id)
            {
                continue;
            }
            if params["searchTerm"].as_str().is_some_and(|term| {
                !agent.name.to_lowercase().contains(&term.to_lowercase())
                    && !agent.thread["preview"]
                        .as_str()
                        .unwrap_or("")
                        .to_lowercase()
                        .contains(&term.to_lowercase())
            }) {
                continue;
            }
            let mut thread = agent.thread.clone();
            agent.decorate(&mut thread);
            thread["turns"] = json!([]);
            data.push(thread);
        }
        // A section keeps its server order.
        if params["sortKey"] == "section_position" {
            return;
        }
        let field = sort_field(params);
        data.sort_by_key(|item| {
            if descending(params) {
                -sort_value(item, field)
            } else {
                sort_value(item, field)
            }
        });
    }

    fn interrupt_agent_native(&mut self, id: &str) {
        let Some(agent) = self.agents.registry.agents.get(id).cloned() else {
            return;
        };
        if !agent.running() {
            return;
        }
        let Some(turn) = agent.turn_id else {
            return;
        };
        self.mark_agent(id, "interrupting", None);
        self.agent_request(
            agent.harness.into(),
            "turn/interrupt",
            json!({"threadId":id,"turnId":turn}),
            Step::Interrupted { child: id.into() },
        );
    }

    pub(super) fn interrupt_descendants(&mut self, parent: &str) {
        let children: Vec<String> = self
            .agents
            .registry
            .agents
            .values()
            .filter(|agent| self.agents.registry.related(agent, parent, true) && agent.running())
            .map(|agent| agent.id.clone())
            .collect();
        for child in children {
            self.interrupt_agent_native(&child);
        }
        self.cancel_parent_calls(parent);
    }

    fn cancel_parent_calls(&mut self, parent: &str) {
        let calls: Vec<String> = self
            .agents
            .pending
            .iter()
            .filter(|(_, call)| call.parent == parent)
            .map(|(id, _)| id.clone())
            .collect();
        for call in calls {
            if let Some(child) = self.agents.pending[&call].child.clone() {
                self.interrupt_agent_native(&child);
            }
            self.finish_agent_call(&call, Err("Parent turn stopped during delegation".into()));
        }
    }

    pub(super) fn agent_tick(&mut self) {
        let now = Instant::now();
        let (expired, waiting) = std::mem::take(&mut self.agents.early)
            .into_iter()
            .partition(|call| call.deadline <= now);
        self.agents.early = waiting;
        for call in expired {
            self.agent_reply(
                call.reply,
                Err("Delegation calls require an active parent turn".into()),
            );
        }
        let expired: Vec<String> = self
            .agents
            .pending
            .iter()
            .filter(|(_, call)| {
                call.deadline <= Instant::now()
                    || matches!(&call.reply,Reply::Mcp{sender,..} if sender.is_closed())
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            let call = &self.agents.pending[&id];
            if call.name == "wait_agent" {
                let result = self
                    .agents
                    .registry
                    .agents
                    .get(call.arguments["agent_id"].as_str().unwrap_or(""))
                    .map(Agent::state)
                    .ok_or_else(|| "Unknown delegated agent".into());
                self.finish_agent_call(&id, result);
            } else {
                if let Some(child) = call.child.clone() {
                    self.interrupt_agent_native(&child);
                }
                self.finish_agent_call(
                    &id,
                    Err("Delegation request timed out; native mutations are not replayed".into()),
                );
            }
        }
    }

    pub(super) fn agent_backend_failed(&mut self, side: Side) {
        let affected: Vec<String> = self
            .agents
            .registry
            .agents
            .values()
            .filter(|agent| {
                self.agents.owned.contains(&agent.id)
                    && Side::from(agent.harness) == side
                    && agent.running()
            })
            .map(|agent| agent.id.clone())
            .collect();
        for id in affected {
            self.mark_agent(
                &id,
                "errored",
                Some("Native harness disconnected; work was not replayed".into()),
            );
            self.agents.shutting_down.remove(&id);
        }
        let calls: Vec<String> = self
            .agents
            .pending
            .iter()
            .filter(|(_, call)| call.side == side)
            .map(|(id, _)| id.clone())
            .collect();
        for call in calls {
            self.finish_agent_call(&call, Err("Parent harness disconnected".into()));
        }
    }

    pub(super) fn stop_agents(&mut self) {
        let running: Vec<String> = self
            .agents
            .owned
            .iter()
            .filter(|id| {
                self.agents
                    .registry
                    .agents
                    .get(*id)
                    .is_some_and(Agent::running)
            })
            .cloned()
            .collect();
        for id in running {
            self.agents.shutting_down.insert(id.clone());
            self.interrupt_agent_native(&id);
        }
        let pending: Vec<String> = self.agents.pending.keys().cloned().collect();
        for id in pending {
            self.finish_agent_call(&id, Err("Desktop router stopped".into()));
        }
    }

    pub(super) fn agent_thread_deleted(&mut self, thread: &str) {
        self.agent_thread_unloaded(thread);
        self.agents.snapshots.remove(thread);
        self.agents.stale_snapshots.remove(thread);
        // Shared listings would otherwise keep showing a deleted delegate.
        if self
            .agents
            .registry
            .agents
            .get(thread)
            .is_some_and(|agent| !agent.running())
            && let Err(error) = self.agents.registry.remove(thread)
        {
            note!("Cannot forget deleted agent {thread}: {error:#}");
        }
    }

    pub(super) fn agent_thread_unloaded(&mut self, thread: &str) {
        if self.agents.active.contains_key(thread) {
            return;
        }
        let scopes: Vec<String> = self
            .agents
            .scopes
            .iter()
            .filter(|(_, owner)| *owner == thread)
            .map(|(scope, _)| scope.clone())
            .collect();
        for scope in scopes {
            self.agents.scopes.remove(&scope);
            self.agents.bridges.remove(&scope);
        }
        self.agents.attached.remove(thread);
    }

    pub(super) fn remember_agent_snapshot(&mut self, result: &Value) {
        let Some(id) = result["thread"]["id"].as_str() else {
            return;
        };
        if result["sandbox"].is_object()
            && result.get("approvalPolicy").is_some()
            && result.get("approvalsReviewer").is_some()
        {
            self.agents.snapshots.insert(id.to_owned(), result.clone());
            self.agents.stale_snapshots.remove(id);
        } else if let Some(snapshot) = self.agents.snapshots.get_mut(id) {
            snapshot["thread"] = result["thread"].clone();
        }
    }

    /// Rejoining an active Codex parent can invalidate its tool callbacks. Use
    /// the native snapshot only while its permission settings remain unchanged;
    /// never synthesize effective permissions from requested ones.
    ///
    /// Codex reports settings only when they change, so a request that restates
    /// the effective permissions in another form must not invalidate them: the
    /// desktop resends every turn's settings, including cwd as a writable root.
    pub(super) fn invalidate_agent_snapshot(&mut self, message: &Value) {
        let p = &message["params"];
        let Some(thread) = p["threadId"].as_str() else {
            return;
        };
        if !matches!(
            message["method"].as_str(),
            Some("turn/start" | "thread/settings/update")
        ) {
            return;
        }
        let Some(snapshot) = self.agents.snapshots.get(thread) else {
            return;
        };
        let mut requested = snapshot.clone();
        for (field, saved) in [
            ("cwd", "cwd"),
            ("approvalPolicy", "approvalPolicy"),
            ("approvalsReviewer", "approvalsReviewer"),
            ("sandboxPolicy", "sandbox"),
        ] {
            if let Some(value) = p.get(field).filter(|v| !v.is_null()) {
                requested[saved] = value.clone();
            }
        }
        // A profile selection resolves natively; only its id can be compared.
        let profile_changed = p
            .get("permissions")
            .filter(|v| !v.is_null())
            .is_some_and(|profile| *profile != snapshot["activePermissionProfile"]["id"]);
        if profile_changed
            || delegation::permissions(&requested).ok() != delegation::permissions(snapshot).ok()
        {
            self.agents.stale_snapshots.insert(thread.into());
        }
    }

    pub(super) fn recover_agents(&mut self) {
        if self.agents.recovered {
            return;
        }
        self.agents.recovered = true;
        let agents: Vec<Agent> = self.agents.registry.agents.values().cloned().collect();
        for agent in agents {
            self.threads.insert(agent.id.clone(), agent.harness.into());
            if agent.running() && !process_alive(agent.owner_pid) {
                self.agents.owned.insert(agent.id.clone());
                self.agent_request(
                    agent.harness.into(),
                    "thread/resume",
                    json!({"threadId":agent.id}),
                    Step::Recovered { child: agent.id },
                );
            }
        }
    }
}

fn text_input(text: &str) -> Value {
    json!([{"type":"text","text":text,"text_elements":[]}])
}

impl Call {
    /// A child task is work started on behalf of the calling turn in another
    /// thread, which the native turn lineage records.
    fn child_turn(&self, child: &str, text: &str) -> Value {
        json!({"threadId":child,"input":text_input(text),
            "parentTurnId":self.parent_turn,"rootTurnId":self.parent_root})
    }
}

pub(super) fn shared_listing(params: &Value) -> bool {
    !params["parentThreadId"].is_null()
        || !params["ancestorThreadId"].is_null()
        || params["sourceKinds"].as_array().is_some_and(|kinds| {
            kinds
                .iter()
                .any(|kind| kind == "subAgent" || kind == "subAgentThreadSpawn")
        })
}

/// Relation indexes stop at a harness boundary. Page the collected native and
/// shared edges together, using timestamp + identity so equal timestamps work.
pub(super) fn paginate_listing(params: &Value, result: &mut Value) -> Result<()> {
    const PREFIX: &str = "claudex-agents-list:";
    let mut query = params.clone();
    query.as_object_mut().unwrap().remove("cursor");
    query.as_object_mut().unwrap().remove("limit");
    let field = sort_field(params);
    let descending = descending(params);
    // A section keeps its server order; its cursor resumes after a thread.
    let section_order = params["sortKey"] == "section_position";
    let data = result["data"].as_array_mut().unwrap();
    if !section_order {
        data.sort_by_key(|item| {
            (
                sort_value(item, field),
                list_thread(item)["id"].as_str().unwrap_or("").to_owned(),
            )
        });
        if descending {
            data.reverse();
        }
    }
    if let Some(cursor) = params["cursor"].as_str() {
        let encoded = cursor
            .strip_prefix(PREFIX)
            .context("Invalid shared child-list cursor")?;
        let cursor: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded)?)?;
        if cursor["query"] != query {
            anyhow::bail!("Child-list cursor belongs to another query");
        }
        if section_order {
            let last = data
                .iter()
                .position(|item| list_thread(item)["id"] == cursor["id"])
                .context("Child-list cursor thread is no longer listed")?;
            data.drain(..=last);
        } else {
            let key = (
                cursor["time"]
                    .as_i64()
                    .context("Invalid child-list timestamp")?,
                cursor["id"]
                    .as_str()
                    .context("Invalid child-list identity")?
                    .to_owned(),
            );
            data.retain(|item| {
                let candidate = (
                    sort_value(item, field),
                    list_thread(item)["id"].as_str().unwrap_or("").to_owned(),
                );
                if descending {
                    candidate < key
                } else {
                    candidate > key
                }
            });
        }
    }
    let limit = params["limit"].as_u64().unwrap_or(50).clamp(1, 1000) as usize;
    let next = if data.len() > limit {
        data.truncate(limit);
        let last = data.last().unwrap();
        Some(format!(
            "{PREFIX}{}",
            URL_SAFE_NO_PAD.encode(
                json!({"query":query,"time":sort_value(last,field),"id":list_thread(last)["id"]})
                    .to_string()
            )
        ))
    } else {
        None
    };
    result["nextCursor"] = json!(next);
    Ok(())
}

fn turn_report(turn: &Value) -> String {
    turn["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| item["type"] == "agentMessage")
        .filter_map(|item| item["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn process_alive(pid: u32) -> bool {
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    // ESRCH is evidence that this router owner is gone; permission errors are
    // not evidence of death. Never interrupt another live desktop connection.
    unsafe {
        libc::kill(pid as i32, 0) == 0
            || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_settings_notification_does_not_confirm_stale_permissions() {
        let (client, _client_rx) = mpsc::channel(32);
        let (codex, _codex_rx) = mpsc::channel(32);
        let mut router = Router::new(
            client,
            codex,
            HashMap::from([(Side::Claude, Facade::default())]),
            None,
        );
        router.remember_agent_snapshot(&json!({"thread":{"id":"parent"},"cwd":"/tmp",
            "approvalPolicy":"on-request","approvalsReviewer":"user",
            "sandbox":{"type":"workspaceWrite"}}));
        router.invalidate_agent_snapshot(&json!({"method":"turn/start","params":{
            "threadId":"parent","approvalsReviewer":"auto_review"}}));
        for settings in [Value::Null, json!({"approvalsReviewer":"auto_review"})] {
            router.agent_notification(
                Side::Codex,
                &json!({"method":"thread/settings/updated",
                "params":{"threadId":"parent","threadSettings":settings}}),
            );
            assert!(router.agents.stale_snapshots.contains("parent"));
            assert_eq!(
                router.agents.snapshots["parent"]["approvalsReviewer"],
                "user"
            );
        }
    }

    #[test]
    fn restated_desktop_permissions_keep_the_native_snapshot_confirmed() {
        let (client, _client_rx) = mpsc::channel(32);
        let (codex, _codex_rx) = mpsc::channel(32);
        let mut router = Router::new(
            client,
            codex,
            HashMap::from([(Side::Claude, Facade::default())]),
            None,
        );
        let cwd = tempfile::tempdir().unwrap();
        let extra = tempfile::tempdir().unwrap();
        let (cwd, extra) = (cwd.path().to_str().unwrap(), extra.path().to_str().unwrap());
        // Codex reports writable roots without the implicit cwd.
        router.remember_agent_snapshot(&json!({"thread":{"id":"parent"},"cwd":cwd,
            "approvalPolicy":"on-request","approvalsReviewer":"auto_review",
            "activePermissionProfile":null,"sandbox":{"type":"workspaceWrite",
            "writableRoots":[extra],"networkAccess":true,
            "excludeTmpdirEnvVar":false,"excludeSlashTmp":false}}));
        // The desktop resends each turn's settings with cwd as a writable root.
        let mut desktop = json!({"method":"turn/start","params":{"threadId":"parent",
            "cwd":cwd,"approvalPolicy":"on-request","approvalsReviewer":"guardian_subagent",
            "permissions":null,"sandboxPolicy":{"type":"workspaceWrite",
            "writableRoots":[cwd,extra],"networkAccess":true,
            "excludeTmpdirEnvVar":false,"excludeSlashTmp":false}}});
        router.invalidate_agent_snapshot(&desktop);
        assert!(!router.agents.stale_snapshots.contains("parent"));
        // Native settings are reported only on change, so a real change must
        // still wait for that report.
        desktop["params"]["sandboxPolicy"]["writableRoots"] = json!([cwd]);
        router.invalidate_agent_snapshot(&desktop);
        assert!(router.agents.stale_snapshots.contains("parent"));
        router.agents.stale_snapshots.clear();
        desktop["params"]["sandboxPolicy"]["writableRoots"] = json!([cwd, extra]);
        desktop["params"]["permissions"] = json!(":workspace");
        router.invalidate_agent_snapshot(&desktop);
        assert!(router.agents.stale_snapshots.contains("parent"));
    }

    #[test]
    fn backend_disconnect_preserves_another_routers_running_agent() {
        let (client, _client_rx) = mpsc::channel(32);
        let (codex, _codex_rx) = mpsc::channel(32);
        let mut router = Router::new(
            client,
            codex,
            HashMap::from([(Side::Claude, Facade::default())]),
            None,
        );
        let state = tempfile::tempdir().unwrap();
        router.agents.registry = Registry::open(state.path().join("agents")).unwrap();
        let owned = uuid::Uuid::new_v4().to_string();
        let foreign = uuid::Uuid::new_v4().to_string();
        for id in [&owned, &foreign] {
            let agent = Agent {
                id: id.clone(),
                parent: "parent".into(),
                root: "parent".into(),
                harness: Harness::Claude,
                name: "worker".into(),
                depth: 1,
                thread: json!({"id":id}),
                turn_id: Some("turn".into()),
                status: "running".into(),
                report: None,
                queued: vec![],
                spawn_parent_turn: "parent-turn".into(),
                spawn_item: json!({}),
                owner_pid: std::process::id(),
                archived: false,
            };
            router.agents.registry.save(&agent).unwrap();
            router.agents.registry.agents.insert(id.clone(), agent);
        }
        router.agents.owned.insert(owned.clone());
        router.agent_backend_failed(Side::Claude);
        let persisted = Registry::open(state.path().join("agents")).unwrap();
        assert_eq!(persisted.agents[&owned].status, "errored");
        assert_eq!(persisted.agents[&foreign].status, "running");
        assert_eq!(persisted.agents[&foreign].turn_id.as_deref(), Some("turn"));
    }
}
