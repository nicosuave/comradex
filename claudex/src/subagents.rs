//! Claude subagents as Codex child threads.
//!
//! An `Agent` tool call starts a `spawnAgent` collab item in the calling thread.
//! When Claude reports the task, the facade creates a child thread linked to the
//! caller, completes the spawn item with that receiver, and keeps a `wait` item
//! open until the agent finishes. Subagent frames (`parent_tool_use_id`, `agent_id`)
//! become the child thread's turn. Nested agents emit no frames, only task events,
//! so their threads show the prompt and final report.
use std::collections::HashMap;

use serde_json::{Value, json};

use crate::{
    protocol::{self, now_ms},
    store::Record,
    translate::Translator,
    usage,
};

/// A child turn receiving one run of a subagent.
pub struct ChildTurn {
    pub turn_id: String,
    pub translator: Translator,
    pub usage: usage::Tracker,
    pub started_ms: u64,
}

pub struct AgentRun {
    /// The Agent tool call that owns the spawn and wait items, when there is one.
    /// Continuations of an earlier turn's agent have neither.
    pub spawn: Option<Spawn>,
    pub agent_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn: Option<ChildTurn>,
}

pub struct Spawn {
    pub tool_id: String,
    pub input: Value,
    /// None for the root thread's translator; otherwise the run key of the
    /// parent agent whose child translator owns the items.
    pub owner: Option<String>,
    pub owner_thread: String,
    pub waiting: bool,
}

/// Runs of one root turn, keyed by the Agent tool_use id (or the agent id for
/// continuations without one).
#[derive(Default)]
pub struct Agents {
    pub runs: HashMap<String, AgentRun>,
    by_agent: HashMap<String, String>,
}

impl Agents {
    pub fn spawned(
        &mut self,
        tool_id: &str,
        input: Value,
        owner: Option<String>,
        owner_thread: &str,
    ) {
        self.runs.entry(tool_id.to_owned()).or_insert(AgentRun {
            spawn: Some(Spawn {
                tool_id: tool_id.to_owned(),
                input,
                owner,
                owner_thread: owner_thread.to_owned(),
                waiting: false,
            }),
            agent_id: None,
            thread_id: None,
            turn: None,
        });
    }
    pub fn continued(&mut self, agent_id: &str, thread_id: &str) -> String {
        let key = format!("agent-{agent_id}");
        self.runs.entry(key.clone()).or_insert(AgentRun {
            spawn: None,
            agent_id: Some(agent_id.to_owned()),
            thread_id: Some(thread_id.to_owned()),
            turn: None,
        });
        self.by_agent.insert(agent_id.to_owned(), key.clone());
        key
    }
    pub fn alias(&mut self, agent_id: &str, key: &str) {
        if let Some(run) = self.runs.get_mut(key) {
            run.agent_id = Some(agent_id.to_owned());
            self.by_agent.insert(agent_id.to_owned(), key.to_owned());
        }
    }
    pub fn by_agent(&self, agent_id: &str) -> Option<&str> {
        self.by_agent.get(agent_id).map(String::as_str)
    }
    /// The run for a subagent frame or permission request.
    pub fn find(&self, agent_id: Option<&str>, tool_id: Option<&str>) -> Option<String> {
        agent_id
            .and_then(|id| self.by_agent(id))
            .or_else(|| tool_id.filter(|id| self.runs.contains_key(*id)))
            .map(str::to_owned)
    }
    /// Thread ids of runs with an open child turn.
    pub fn open_threads(&self) -> impl Iterator<Item = &str> {
        self.runs
            .values()
            .filter(|run| run.turn.is_some())
            .filter_map(|run| run.thread_id.as_deref())
    }
}

/// A persisted child thread record, linked to the calling thread.
pub fn child_record(parent: &Record, threads_dir: std::path::PathBuf, input: &Value) -> Record {
    let mut settings = parent.settings.clone();
    if let Some(model) = input["model"].as_str() {
        settings.model = model.to_owned();
    }
    let params = json!({"ephemeral": parent.ephemeral(), "projectId": parent.thread["projectId"],
        "historyMode": parent.thread["historyMode"]});
    let originator = parent.thread["originator"].as_str().unwrap_or("");
    let mut record = Record::new(settings, threads_dir, &params, originator);
    let parent_id = parent.id().to_owned();
    let depth = depth(parent) + 1;
    let nickname = input["name"].as_str().or(input["description"].as_str());
    let role = input["subagent_type"].as_str();
    let thread = &mut record.thread;
    thread["parentThreadId"] = json!(parent_id);
    thread["source"] = json!({"subAgent": {"thread_spawn": {"parent_thread_id": parent_id,
        "depth": depth, "agent_path": null, "agent_nickname": nickname, "agent_role": role}}});
    thread["threadSource"] = json!("subagent");
    thread["agentNickname"] = json!(nickname);
    thread["agentRole"] = json!(role);
    thread["name"] = json!(input["description"].as_str());
    thread["preview"] = json!(
        input["prompt"]
            .as_str()
            .unwrap_or("")
            .chars()
            .take(200)
            .collect::<String>()
    );
    // Claude cannot resume a subagent transcript as its own session.
    thread["canAcceptDirectInput"] = json!(false);
    record
}

pub fn is_child(record: &Record) -> bool {
    !record.thread["parentThreadId"].is_null()
}

fn depth(record: &Record) -> u64 {
    record.thread["source"]["subAgent"]["thread_spawn"]["depth"]
        .as_u64()
        .unwrap_or(0)
}

/// Codex ThreadSourceKind of a facade record.
pub fn source_kind(record: &Record) -> &'static str {
    if is_child(record) {
        "subAgentThreadSpawn"
    } else {
        "appServer"
    }
}

pub fn source_matches(record: &Record, kind: &str) -> bool {
    let own = source_kind(record);
    own == kind || (kind == "subAgent" && own == "subAgentThreadSpawn")
}

/// A child turn does work on behalf of the calling turn, so it shares that
/// turn's causal root.
pub fn new_turn(
    record: &mut Record,
    prompt: Option<&str>,
    root_turn_id: Option<&str>,
) -> (Value, Option<Value>) {
    let turn_id = protocol::id();
    let turn = protocol::new_turn(&turn_id, root_turn_id);
    record.turns.push(turn.clone());
    record.thread["status"] = json!({"type": "active", "activeFlags": []});
    let item = prompt.filter(|p| !p.is_empty()).map(|prompt| {
        json!({"type": "userMessage", "id": protocol::id(), "clientId": null,
            "content": [{"type": "text", "text": prompt, "text_elements": []}]})
    });
    if let Some(item) = &item {
        record.update_item(item.clone(), true);
    }
    record.touch();
    (turn, item)
}

pub fn child_turn(thread_id: &str, turn_id: &str, record: &Record) -> ChildTurn {
    ChildTurn {
        turn_id: turn_id.to_owned(),
        translator: Translator::new(thread_id, turn_id, &record.settings.cwd)
            .with_reasoning(record.settings.reasoning_summary.as_deref() != Some("none")),
        usage: usage::Tracker::default(),
        started_ms: now_ms(),
    }
}

pub fn spawn_completed(child: &str) -> Value {
    json!({"status": "completed", "receiverThreadIds": [child],
        "agentsStates": {child: {"status": "running", "message": null}}})
}

pub fn wait_item(tool_id: &str, sender: &str, child: &str) -> Value {
    json!({"type": "collabAgentToolCall", "id": wait_id(tool_id), "tool": "wait",
        "status": "inProgress", "senderThreadId": sender, "receiverThreadIds": [child],
        "prompt": null, "model": null, "reasoningEffort": null, "agentsStates": {}})
}

pub fn wait_id(tool_id: &str) -> String {
    format!("{tool_id}-wait")
}

/// Final collab item fields for an agent that reached `status`.
pub fn agent_finished(child: &str, status: &str, message: Option<&str>) -> Value {
    let (item_status, agent_status) = match status {
        "completed" => ("completed", "completed"),
        "interrupted" => ("interrupted", "interrupted"),
        _ => ("failed", "errored"),
    };
    json!({"status": item_status, "receiverThreadIds": [child],
        "agentsStates": {child: {"status": agent_status, "message": message}}})
}

/// The report an Agent tool result or task notification carries.
pub fn report(block: &Value, metadata: &Value) -> String {
    let content = metadata
        .get("content")
        .filter(|c| c.is_array())
        .unwrap_or(&block["content"]);
    crate::translate::content_text(content)
}

/// Agent tool results that only acknowledge a background launch.
pub fn launch_ack(metadata: &Value) -> bool {
    metadata["isAsync"] == true
        || matches!(
            metadata["status"].as_str(),
            Some("async_launched" | "remote_launched")
        )
}
