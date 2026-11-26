//! Stateful conversion of Claude SDK messages into Codex item lifecycles.
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

use crate::protocol::{self, RpcError, RpcResult, notification, now_ms};

pub struct Translator {
    thread_id: String,
    turn_id: String,
    cwd: PathBuf,
    message_id: String,
    blocks: HashMap<u64, Value>,
    tool_json: HashMap<u64, String>,
    items: Vec<Value>,
    completed: HashSet<String>,
    started_at: HashMap<String, u64>,
    final_messages: HashSet<String>,
    native_tasks: HashMap<String, String>,
    spawns: Vec<(String, Value)>,
    include_reasoning: bool,
    acp: AcpState,
    /// The compaction item opened by Claude's `compacting` status, until its boundary.
    compaction: Option<String>,
}

/// Item state for ACP session updates.
#[derive(Default)]
struct AcpState {
    /// The open message or thought item: (item id, thinking, agent message id).
    text: Option<(String, bool, Option<String>)>,
    /// Accumulated tool call state by tool call id.
    tools: HashMap<String, Value>,
    /// Bytes of each command's output already streamed as deltas.
    output: HashMap<String, usize>,
    /// Calls whose items the server owns (bridged desktop tools).
    skipped: HashSet<String>,
}

impl Translator {
    pub fn snapshot(&self) -> &[Value] {
        &self.items
    }

    pub fn new(thread_id: &str, turn_id: &str, cwd: &Path) -> Self {
        Self {
            thread_id: thread_id.into(),
            turn_id: turn_id.into(),
            cwd: cwd.into(),
            message_id: protocol::id(),
            blocks: HashMap::new(),
            tool_json: HashMap::new(),
            items: vec![],
            completed: HashSet::new(),
            started_at: HashMap::new(),
            final_messages: HashSet::new(),
            native_tasks: HashMap::new(),
            spawns: Vec::new(),
            include_reasoning: true,
            acp: AcpState::default(),
            compaction: None,
        }
    }
    pub fn with_reasoning(mut self, include: bool) -> Self {
        self.include_reasoning = include;
        self
    }
    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }
    /// Agent calls started since the last call, as (tool_use_id, input). The
    /// server links each to a child thread when Claude reports the task.
    pub fn take_spawns(&mut self) -> Vec<(String, Value)> {
        std::mem::take(&mut self.spawns)
    }
    /// Start an item the server built (collab items for child agents).
    pub fn begin(&mut self, item: Value) -> Vec<Value> {
        let mut events = vec![];
        self.start(item, &mut events);
        events
    }
    /// Apply final fields to an open item and complete it.
    pub fn end(&mut self, id: &str, fields: Value) -> Vec<Value> {
        let mut events = vec![];
        if self.completed.contains(id) {
            return events;
        }
        if let Some(item) = self.items.iter_mut().find(|v| v["id"] == id) {
            for (key, value) in fields.as_object().into_iter().flatten() {
                item[key] = value.clone();
            }
        }
        self.complete(id, &mut events);
        events
    }
    pub fn has_agent_message(&self) -> bool {
        self.items.iter().any(|v| v["type"] == "agentMessage")
    }
    /// A final report for a child whose own text Claude did not forward.
    pub fn final_message(&mut self, text: &str) -> Vec<Value> {
        let mut events = vec![];
        let id = protocol::id();
        self.start(agent_message(&id), &mut events);
        self.text_delta(&id, text, false, &mut events);
        self.complete(&id, &mut events);
        events
    }
    fn event(&self, method: &str, mut p: Value) -> Value {
        p["threadId"] = json!(self.thread_id);
        p["turnId"] = json!(self.turn_id);
        notification(method, p)
    }
    fn start(&mut self, item: Value, events: &mut Vec<Value>) {
        let id = item["id"].as_str().unwrap().to_owned();
        if self.items.iter().any(|v| v["id"] == id) {
            return;
        }
        let time = now_ms();
        self.started_at.insert(id, time);
        events.push(self.event("item/started", json!({"item": item, "startedAtMs": time})));
        self.items.push(item);
    }
    fn complete(&mut self, id: &str, events: &mut Vec<Value>) {
        if !self.completed.insert(id.to_owned()) {
            return;
        }
        if let Some(item) = self.items.iter_mut().find(|v| v["id"] == id)
            && item["type"] == "agentMessage"
            && let Some(text) = item["text"].as_str()
        {
            item["text"] = json!(normalize_visualization(text));
        }
        if let Some(item) = self.items.iter().find(|v| v["id"] == id) {
            events.push(self.event(
                "item/completed",
                json!({"item": item, "completedAtMs": now_ms()}),
            ));
        }
    }
    fn block_id(&self, index: u64) -> String {
        format!("{}-{index}", self.message_id)
    }
    fn text_start(&mut self, index: u64, block: &Value, events: &mut Vec<Value>) {
        let id = self.block_id(index);
        match block["type"].as_str() {
            Some("text") => {
                self.start(agent_message(&id), events);
                if let Some(text) = block["text"].as_str().filter(|s| !s.is_empty()) {
                    self.text_delta(&id, text, false, events);
                }
            }
            Some("thinking") if self.include_reasoning => {
                self.start(
                    json!({"type": "reasoning", "id": id, "summary": [], "content": [""]}),
                    events,
                );
                if let Some(text) = block["thinking"].as_str().filter(|s| !s.is_empty()) {
                    self.text_delta(&id, text, true, events);
                }
            }
            _ => {}
        }
    }
    fn text_delta(&mut self, id: &str, delta: &str, thinking: bool, events: &mut Vec<Value>) {
        if thinking && !self.include_reasoning {
            return;
        }
        if self.completed.contains(id) {
            return;
        }
        if let Some(item) = self.items.iter_mut().find(|v| v["id"] == id) {
            if thinking {
                let text = format!("{}{delta}", item["content"][0].as_str().unwrap_or(""));
                item["content"][0] = json!(text);
            } else {
                let text = format!("{}{delta}", item["text"].as_str().unwrap_or(""));
                item["text"] = json!(text);
            }
        }
        let (method, mut p) = if thinking {
            ("item/reasoning/textDelta", json!({"contentIndex": 0}))
        } else {
            ("item/agentMessage/delta", json!({}))
        };
        p["itemId"] = json!(id);
        p["delta"] = json!(delta);
        events.push(self.event(method, p));
    }

    pub fn tool_start(&mut self, tool_id: &str, name: &str, input: &Value) -> Vec<Value> {
        let mut events = vec![];
        // SDK MCP requests own the desktop dynamic-tool lifecycle and results.
        if name.starts_with("mcp__codex_desktop__") {
            return events;
        }
        if self.items.iter().any(|v| v["id"] == tool_id) {
            return events;
        }
        let item = match name {
            "Bash" => {
                json!({"type": "commandExecution", "id": tool_id, "pluginId": null, "scriptPath": null,
                "command": input["command"].as_str().unwrap_or(""), "cwd": self.cwd, "processId": null,
                "source": "agent", "status": "inProgress", "commandActions": [],
                "aggregatedOutput": null, "exitCode": null, "durationMs": null})
            }
            "Write" | "Edit" | "MultiEdit" => json!({"type": "fileChange", "id": tool_id,
                "changes": file_changes(&self.cwd, name, input), "status": "inProgress"}),
            // Claude lists the tool as Task but calls it Agent.
            "Agent" | "Task" => {
                self.spawns.push((tool_id.to_owned(), input.clone()));
                json!({"type": "collabAgentToolCall", "id": tool_id, "tool": "spawnAgent",
                    "status": "inProgress", "senderThreadId": self.thread_id, "receiverThreadIds": [],
                    "prompt": input["prompt"].as_str(), "model": input["model"].as_str(),
                    "reasoningEffort": null, "agentsStates": {}})
            }
            _ if name.starts_with("mcp__") => {
                let mut parts = name.splitn(3, "__");
                parts.next();
                json!({"type": "mcpToolCall", "id": tool_id, "server": parts.next().unwrap_or("claude"),
                    "tool": parts.next().unwrap_or(name), "status": "inProgress", "arguments": input,
                    "appContext": null, "mcpAppUi": null, "pluginId": null, "readOnlyHint": null,
                    "result": null, "error": null, "durationMs": null})
            }
            _ => {
                json!({"type": "dynamicToolCall", "id": tool_id, "namespace": "claude", "tool": name,
                "arguments": input, "status": "inProgress", "contentItems": null, "success": null, "durationMs": null})
            }
        };
        self.start(item, &mut events);
        if name == "TodoWrite" {
            let plan: Vec<Value> = input["todos"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|todo| {
                    let status = match todo["status"].as_str() {
                        Some("completed") => "completed",
                        Some("in_progress") => "inProgress",
                        _ => "pending",
                    };
                    json!({"step": todo["content"].as_str().unwrap_or(""), "status": status})
                })
                .collect();
            events.push(self.event(
                "turn/plan/updated",
                json!({"explanation": null, "plan": plan}),
            ));
        }
        events
    }

    fn tool_result(&mut self, result: &Value, metadata: &Value, events: &mut Vec<Value>) {
        let Some(id) = result["tool_use_id"].as_str() else {
            return;
        };
        if self.completed.contains(id) {
            return;
        }
        let failed = result["is_error"].as_bool().unwrap_or(false);
        let text = content_text(&result["content"]);
        let mut output_delta = None;
        if let Some(item) = self.items.iter_mut().find(|v| v["id"] == id) {
            item["status"] = json!(if failed { "failed" } else { "completed" });
            let duration = self
                .started_at
                .get(id)
                .map(|start| now_ms().saturating_sub(*start));
            match item["type"].as_str() {
                Some("commandExecution") => {
                    item["aggregatedOutput"] = json!(text);
                    item["exitCode"] = metadata
                        .get("exitCode")
                        .or_else(|| metadata.get("exit_code"))
                        .filter(|v| v.is_i64())
                        .cloned()
                        .unwrap_or(Value::Null);
                    item["durationMs"] = json!(duration);
                    if !text.is_empty() {
                        output_delta = Some(text.clone());
                    }
                }
                Some("dynamicToolCall") => {
                    item["success"] = json!(!failed);
                    item["contentItems"] = json!([{"type": "inputText", "text": text}]);
                    item["durationMs"] = json!(duration);
                }
                Some("mcpToolCall") => {
                    if failed {
                        item["error"] = json!({"message": text});
                    } else {
                        item["result"] = json!({"content": [{"type": "text", "text": text}], "structuredContent": null, "_meta": null});
                    }
                    item["durationMs"] = json!(duration);
                }
                _ => {}
            }
        }
        if let Some(delta) = output_delta {
            events.push(self.event(
                "item/commandExecution/outputDelta",
                json!({"itemId": id, "delta": delta}),
            ));
        }
        self.complete(id, events);
    }

    pub fn receive(&mut self, message: &Value) -> RpcResult<Vec<Value>> {
        let mut events = vec![];
        // Claude may forward subagent messages; these belong to the parent tool's result,
        // not to the top-level assistant response. A subagent tool call that needed
        // approval already has an item here, so its result must still complete it.
        if !message["parent_tool_use_id"].is_null() {
            if message["type"] == "user" {
                let blocks: Vec<&Value> = message["message"]["content"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|block| {
                        block["type"] == "tool_result"
                            && self.items.iter().any(|v| v["id"] == block["tool_use_id"])
                    })
                    .collect();
                for block in blocks {
                    self.tool_result(block, &message["tool_use_result"], &mut events);
                }
            }
            return Ok(events);
        }
        match message["type"].as_str() {
            Some("stream_event") => {
                let event = &message["event"];
                let index = event["index"].as_u64().unwrap_or(0);
                match event["type"].as_str() {
                    Some("message_start") => {
                        self.message_id = event["message"]["id"]
                            .as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(protocol::id);
                        self.blocks.clear();
                        self.tool_json.clear();
                    }
                    Some("content_block_start") => {
                        let block = &event["content_block"];
                        self.blocks.insert(index, block.clone());
                        self.text_start(index, block, &mut events);
                    }
                    Some("content_block_delta") => {
                        let delta = &event["delta"];
                        let id = self.block_id(index);
                        match delta["type"].as_str() {
                            Some("text_delta") => self.text_delta(
                                &id,
                                delta["text"].as_str().unwrap_or(""),
                                false,
                                &mut events,
                            ),
                            Some("thinking_delta") => self.text_delta(
                                &id,
                                delta["thinking"].as_str().unwrap_or(""),
                                true,
                                &mut events,
                            ),
                            Some("input_json_delta") => self
                                .tool_json
                                .entry(index)
                                .or_default()
                                .push_str(delta["partial_json"].as_str().unwrap_or("")),
                            _ => {}
                        }
                    }
                    Some("content_block_stop") => {
                        if let Some(block) = self.blocks.get(&index).cloned() {
                            if block["type"] == "tool_use" {
                                let input =
                                    match self.tool_json.remove(&index).filter(|s| !s.is_empty()) {
                                        Some(text) => {
                                            serde_json::from_str(&text).map_err(|_| {
                                                RpcError::internal(
                                                    "Claude emitted invalid tool argument JSON",
                                                )
                                            })?
                                        }
                                        None => block["input"].clone(),
                                    };
                                events.extend(self.tool_start(
                                    block["id"].as_str().unwrap_or("unknown"),
                                    block["name"].as_str().unwrap_or("unknown"),
                                    &input,
                                ));
                            } else {
                                self.complete(&self.block_id(index), &mut events);
                            }
                        }
                    }
                    _ => {}
                }
            }
            Some("assistant") => {
                let frame_id = message["uuid"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| message.to_string());
                if !self.final_messages.insert(frame_id) {
                    return Ok(events);
                }
                let msg = &message["message"];
                if let Some(message_id) = msg["id"].as_str() {
                    self.message_id = message_id.to_owned();
                }
                for (index, block) in msg["content"].as_array().into_iter().flatten().enumerate() {
                    if block["type"] == "tool_use" {
                        events.extend(self.tool_start(
                            block["id"].as_str().unwrap_or("unknown"),
                            block["name"].as_str().unwrap_or("unknown"),
                            &block["input"],
                        ));
                    } else if block["type"] == "text" || block["type"] == "thinking" {
                        let thinking = block["type"] == "thinking";
                        if thinking && !self.include_reasoning {
                            continue;
                        }
                        let text = block[if thinking { "thinking" } else { "text" }]
                            .as_str()
                            .unwrap_or("");
                        // SDK assistant frames can contain just one block from an API message.
                        // Their array index is not necessarily the streaming block index.
                        let candidate = self
                            .blocks
                            .keys()
                            .filter_map(|i| {
                                let id = self.block_id(*i);
                                self.items.iter().find(|v| v["id"] == id).and_then(|item| {
                                    let existing = if thinking {
                                        item["content"][0].as_str()
                                    } else {
                                        item["text"].as_str()
                                    }?;
                                    if existing == text
                                        || (self.completed.contains(&id)
                                            && existing == normalize_visualization(text))
                                        || (!self.completed.contains(&id)
                                            && text.starts_with(existing))
                                    {
                                        let length = if self.completed.contains(&id) {
                                            text.len()
                                        } else {
                                            existing.len()
                                        };
                                        Some((id, length))
                                    } else {
                                        None
                                    }
                                })
                            })
                            .next();
                        if let Some((id, length)) = candidate {
                            if length < text.len() {
                                self.text_delta(&id, &text[length..], thinking, &mut events);
                            }
                            self.complete(&id, &mut events);
                        } else {
                            let id = format!(
                                "{}-{index}",
                                message["uuid"]
                                    .as_str()
                                    .map(str::to_owned)
                                    .unwrap_or_else(protocol::id)
                            );
                            let item = if thinking {
                                json!({"type": "reasoning", "id": id, "summary": [], "content": [""]})
                            } else {
                                agent_message(&id)
                            };
                            self.start(item, &mut events);
                            self.text_delta(&id, text, thinking, &mut events);
                            self.complete(&id, &mut events);
                        }
                    }
                }
            }
            Some("user") => {
                // A new ACP prompt starts a new message.
                self.acp_close_text(&mut events);
                for block in message["message"]["content"]
                    .as_array()
                    .into_iter()
                    .flatten()
                {
                    if block["type"] == "tool_result" {
                        self.tool_result(block, &message["tool_use_result"], &mut events);
                    }
                }
            }
            Some("system") if message["subtype"] == "task_started" => {
                // Agent's tool result acknowledges launch, not completion. Keep
                // separate native task activity visible while the parent yields.
                if matches!(
                    message["task_type"].as_str(),
                    Some("local_agent" | "local_workflow")
                ) && let Some(task) = message["task_id"].as_str()
                {
                    let id = format!("claude-task-{task}");
                    self.native_tasks.insert(task.to_owned(), id.clone());
                    self.start(json!({"type":"dynamicToolCall","id":id,"namespace":"claude",
                        "tool":if message["task_type"] == "local_workflow" { "Background workflow" } else { "Background agent" },
                        "arguments":{"description":message["description"],"taskId":task},
                        "status":"inProgress","contentItems":null,"success":null,"durationMs":null}), &mut events);
                }
            }
            Some("system")
                if message["subtype"] == "task_notification"
                    || message["subtype"] == "task_updated" =>
            {
                let status = if message["subtype"] == "task_updated" {
                    &message["patch"]["status"]
                } else {
                    &message["status"]
                };
                if matches!(
                    status.as_str(),
                    Some("completed" | "failed" | "killed" | "stopped")
                ) && let Some(task) = message["task_id"].as_str()
                    && let Some(id) = self.native_tasks.remove(task)
                {
                    let text = message["summary"]
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .or_else(|| message["reason"].as_str())
                        .unwrap_or_else(|| status.as_str().unwrap());
                    self.tool_result(
                        &json!({"tool_use_id":id,"content":text,"is_error":status != "completed"}),
                        &Value::Null,
                        &mut events,
                    );
                }
            }
            Some("system") if message["subtype"] == "permission_denied" => {
                let tool = message["tool_name"].as_str().unwrap_or("tool");
                let reason = message["decision_reason"]
                    .as_str()
                    .unwrap_or("Native permission check denied the action");
                let recovery = if message["decision_reason_type"] == "classifier" {
                    " You can explicitly authorize this specific action in chat for a fresh native review. If it remains denied, select 'Ask for approval' in this chat's permissions menu for manual review. Workspace sandbox restrictions remain in place; no global allow rule is required."
                } else {
                    " Explicit deny rules and managed restrictions remain in effect."
                };
                events.push(self.event("warning", json!({"message":format!("Claude denied {tool}: {reason}. The action was not performed and has no pending approval.{recovery}")})));
            }
            // The item stays in progress while Claude summarizes, as Codex's does.
            Some("system")
                if message["subtype"] == "status" && message["status"] == "compacting" =>
            {
                if self.compaction.is_none() {
                    let id = protocol::id();
                    self.start(json!({"type": "contextCompaction", "id": id}), &mut events);
                    self.compaction = Some(id);
                }
            }
            Some("system") if message["subtype"] == "compact_boundary" => {
                let id = match self.compaction.take() {
                    Some(id) => id,
                    None => {
                        let id = protocol::id();
                        self.start(json!({"type": "contextCompaction", "id": id}), &mut events);
                        id
                    }
                };
                self.complete(&id, &mut events);
            }
            Some("acp") => self.acp_update(&message["update"], &mut events),
            Some("result") => {
                self.acp_close_text(&mut events);
                if let Some(output) = message.get("structured_output").filter(|v| !v.is_null()) {
                    let id = protocol::id();
                    self.start(agent_message(&id), &mut events);
                    self.text_delta(&id, &output.to_string(), false, &mut events);
                    if let Some(item) = self.items.iter_mut().find(|v| v["id"] == id) {
                        item["phase"] = json!("final_answer");
                    }
                    self.complete(&id, &mut events);
                }
                if !self.items.iter().any(|v| v["type"] == "agentMessage")
                    && let Some(text) = message["result"].as_str().filter(|s| !s.is_empty())
                {
                    let id = protocol::id();
                    self.start(agent_message(&id), &mut events);
                    self.text_delta(&id, text, false, &mut events);
                    self.complete(&id, &mut events);
                }
            }
            _ => {}
        }
        Ok(events)
    }

    fn acp_update(&mut self, update: &Value, events: &mut Vec<Value>) {
        match update["sessionUpdate"].as_str() {
            Some(kind @ ("agent_message_chunk" | "agent_thought_chunk")) => {
                let thinking = kind == "agent_thought_chunk";
                if thinking && !self.include_reasoning {
                    return;
                }
                let text = acp_content_text(&update["content"]);
                if text.is_empty() {
                    return;
                }
                let message_id = update["messageId"].as_str().map(str::to_owned);
                let reuse = self
                    .acp
                    .text
                    .as_ref()
                    .is_some_and(|(_, open_thinking, open_id)| {
                        *open_thinking == thinking
                            && (message_id.is_none() || *open_id == message_id)
                    });
                if !reuse {
                    self.acp_close_text(events);
                    let id = protocol::id();
                    self.start(
                        if thinking {
                            json!({"type": "reasoning", "id": id, "summary": [], "content": [""]})
                        } else {
                            agent_message(&id)
                        },
                        events,
                    );
                    self.acp.text = Some((id, thinking, message_id));
                }
                let id = self.acp.text.as_ref().unwrap().0.clone();
                self.text_delta(&id, &text, thinking, events);
            }
            Some("tool_call" | "tool_call_update") => self.acp_tool(update, events),
            Some("plan") => {
                let plan: Vec<Value> = update["entries"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|entry| {
                        let status = match entry["status"].as_str() {
                            Some("completed") => "completed",
                            Some("in_progress") => "inProgress",
                            _ => "pending",
                        };
                        json!({"step": entry["content"].as_str().unwrap_or(""), "status": status})
                    })
                    .collect();
                events.push(self.event(
                    "turn/plan/updated",
                    json!({"explanation": null, "plan": plan}),
                ));
            }
            Some("compaction_update") if update["status"] == "completed" => {
                let id = protocol::id();
                self.start(json!({"type": "contextCompaction", "id": id}), events);
                self.complete(&id, events);
            }
            Some("notice") => {
                if let Some(title) = update["title"].as_str() {
                    events.push(self.event("warning", json!({"message": title})));
                }
            }
            // Mode, command, config, session-info and usage updates have no item.
            _ => {}
        }
    }

    fn acp_close_text(&mut self, events: &mut Vec<Value>) {
        if let Some((id, _, _)) = self.acp.text.take() {
            self.complete(&id, events);
        }
    }

    fn acp_tool(&mut self, update: &Value, events: &mut Vec<Value>) {
        let Some(id) = update["toolCallId"].as_str().map(str::to_owned) else {
            return;
        };
        self.acp_close_text(events);
        if update["desktopTool"] == true {
            self.acp.skipped.insert(id);
            return;
        }
        if self.acp.skipped.contains(&id) || self.completed.contains(&id) {
            return;
        }
        let call = self
            .acp
            .tools
            .entry(id.clone())
            .or_insert_with(|| json!({}));
        crate::acp::merge_tool(call, update);
        if call["itemKind"].is_null() {
            call["itemKind"] = json!(call["kind"].as_str().unwrap_or("other"));
        }
        let call = call.clone();
        let item = self.acp_item(&id, &call);
        let output = (item["type"] == "commandExecution")
            .then(|| item["aggregatedOutput"].as_str().unwrap_or("").to_owned());
        if let Some(existing) = self.items.iter_mut().find(|v| v["id"] == id) {
            // Refresh fields as the agent fills in input, content and status.
            *existing = item;
        } else {
            self.start(item, events);
        }
        if let Some(output) = output {
            let sent = self.acp.output.entry(id.clone()).or_default();
            if output.len() > *sent && output.is_char_boundary(*sent) {
                let delta = output[*sent..].to_owned();
                *sent = output.len();
                events.push(self.event(
                    "item/commandExecution/outputDelta",
                    json!({"itemId": id, "delta": delta}),
                ));
            }
        }
        if matches!(call["status"].as_str(), Some("completed" | "failed")) {
            self.complete(&id, events);
        }
    }

    /// The Codex item for an accumulated ACP tool call.
    fn acp_item(&self, id: &str, call: &Value) -> Value {
        let status = match call["status"].as_str() {
            Some("completed") => "completed",
            Some("failed") => "failed",
            _ => "inProgress",
        };
        let text = call["content"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|content| content["type"] == "content")
            .map(|content| acp_content_text(&content["content"]))
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        let duration = self
            .started_at
            .get(id)
            .filter(|_| status != "inProgress")
            .map(|start| now_ms().saturating_sub(*start));
        let diffs: Vec<&Value> = call["content"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|content| content["type"] == "diff")
            .collect();
        // An item keeps the type chosen when it started, even if a later
        // update reports a different kind.
        match call["itemKind"].as_str() {
            Some("execute") => {
                let raw = &call["rawOutput"];
                let output = if text.is_empty() {
                    match raw {
                        Value::String(output) => output.clone(),
                        _ => raw["stdout"]
                            .as_str()
                            .or(raw["output"].as_str())
                            .unwrap_or("")
                            .to_owned(),
                    }
                } else {
                    text
                };
                let exit_code = ["exitCode", "exit_code", "code"]
                    .iter()
                    .find_map(|key| raw[*key].as_i64());
                json!({"type": "commandExecution", "id": id, "pluginId": null, "scriptPath": null,
                    "command": crate::acp::command_text(call),
                    "cwd": call["rawInput"]["cwd"].as_str().map(PathBuf::from).unwrap_or_else(|| self.cwd.clone()),
                    "processId": null, "source": "agent", "status": status, "commandActions": [],
                    "aggregatedOutput": if output.is_empty() && status == "inProgress" { Value::Null } else { json!(output) },
                    "exitCode": exit_code, "durationMs": duration})
            }
            Some(kind @ ("edit" | "delete" | "move")) => {
                let mut changes: Vec<Value> = diffs
                    .iter()
                    .map(|diff| {
                        let path = diff["path"].as_str().unwrap_or("");
                        let before = diff["oldText"].as_str();
                        let after = diff["newText"].as_str().unwrap_or("");
                        let change_kind = if kind == "delete" {
                            json!({"type": "delete"})
                        } else if before.is_none() {
                            json!({"type": "add"})
                        } else {
                            json!({"type": "update", "move_path": null})
                        };
                        let diff = similar::TextDiff::from_lines(before.unwrap_or(""), after)
                            .unified_diff()
                            .header(path, path)
                            .to_string();
                        json!({"path": path, "kind": change_kind, "diff": diff})
                    })
                    .collect();
                if changes.is_empty() {
                    let locations = call["locations"].as_array().cloned().unwrap_or_default();
                    let target = locations.get(1).and_then(|l| l["path"].as_str());
                    changes = locations
                        .iter()
                        .take(1)
                        .filter_map(|location| location["path"].as_str())
                        .map(|path| {
                            let change_kind = if kind == "delete" {
                                json!({"type": "delete"})
                            } else {
                                json!({"type": "update", "move_path": target})
                            };
                            json!({"path": path, "kind": change_kind, "diff": ""})
                        })
                        .collect();
                }
                json!({"type": "fileChange", "id": id, "changes": changes, "status": status})
            }
            _ => {
                let mut content = vec![];
                if !text.is_empty() {
                    content.push(json!({"type": "inputText", "text": text}));
                }
                json!({"type": "dynamicToolCall", "id": id, "namespace": crate::protocol::provider().id,
                    "tool": call["title"].as_str().or(call["name"].as_str()).unwrap_or("tool"),
                    "arguments": if call["rawInput"].is_null() { json!({}) } else { call["rawInput"].clone() },
                    "status": status,
                    "contentItems": if status == "inProgress" && content.is_empty() { Value::Null } else { json!(content) },
                    "success": match status { "completed" => json!(true), "failed" => json!(false), _ => Value::Null },
                    "durationMs": duration})
            }
        }
    }

    pub fn finish(&mut self) -> Vec<Value> {
        let mut events = vec![];
        let ids: Vec<String> = self
            .items
            .iter()
            .map(|v| v["id"].as_str().unwrap().to_owned())
            .collect();
        for id in ids {
            if self.completed.contains(&id) {
                continue;
            }
            if let Some(item) = self.items.iter_mut().find(|v| v["id"] == id) {
                // A result or process exit without a tool_result is not proof a tool succeeded.
                if item["status"] == "inProgress" {
                    item["status"] = json!("failed");
                }
            }
            self.complete(&id, &mut events);
        }
        events
    }
}

// Claude can omit the private-use delimiter glyphs from the desktop skill.
// Accept only a complete standalone visualization reference, outside code fences.
fn normalize_visualization(text: &str) -> String {
    let mut fenced = false;
    text.split_inclusive('\n')
        .map(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
                fenced = !fenced;
            }
            if !fenced
                && let Some(payload) = trimmed.strip_prefix("visualize")
                && let Ok(value) = serde_json::from_str::<Value>(payload)
                && let Some(path) = value["path"].as_str()
                && Path::new(path).is_absolute()
                && path.ends_with(".html")
                && value.as_object().is_some_and(|object| {
                    object
                        .keys()
                        .all(|key| matches!(key.as_str(), "path" | "title" | "mode"))
                })
            {
                return format!(
                    "\u{e200}visualize\u{e202}{payload}\u{e201}{}",
                    if line.ends_with('\n') { "\n" } else { "" }
                );
            }
            line.to_owned()
        })
        .collect()
}

/// Displayable text of one ACP content block.
fn acp_content_text(block: &Value) -> String {
    match block["type"].as_str() {
        Some("text") => block["text"].as_str().unwrap_or("").to_owned(),
        Some("resource_link") => format!(
            "[{}]({})",
            block["title"]
                .as_str()
                .or(block["name"].as_str())
                .unwrap_or("resource"),
            block["uri"].as_str().unwrap_or("")
        ),
        Some("resource") => block["resource"]["text"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| {
                format!(
                    "[{}]",
                    block["resource"]["uri"].as_str().unwrap_or("resource")
                )
            }),
        Some("image") => "[image]".into(),
        Some("audio") => "[audio]".into(),
        _ => String::new(),
    }
}

fn agent_message(id: &str) -> Value {
    json!({"type": "agentMessage", "id": id, "text": "", "phase": null,
        "memoryCitation": null, "delivery": null, "questions": null})
}

pub fn content_text(content: &Value) -> String {
    match content {
        Value::String(s) => s.clone(),
        Value::Array(blocks) => blocks
            .iter()
            .map(|b| {
                b["text"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| b.to_string())
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Null => String::new(),
        v => v.to_string(),
    }
}

fn file_changes(cwd: &Path, name: &str, input: &Value) -> Vec<Value> {
    let path = input["file_path"].as_str().unwrap_or("");
    let full_path = if Path::new(path).is_absolute() {
        PathBuf::from(path)
    } else {
        cwd.join(path)
    };
    // Only inspect small UTF-8 files for preview; never read an unbounded device or pipe.
    let old = std::fs::metadata(&full_path)
        .ok()
        .filter(|m| m.is_file() && m.len() <= 2 * 1024 * 1024)
        .and_then(|_| std::fs::read_to_string(&full_path).ok());
    let (before, after, kind) = if name == "Write" {
        (
            old.clone().unwrap_or_default(),
            input["content"].as_str().unwrap_or("").to_owned(),
            if full_path.exists() {
                json!({"type": "update", "move_path": null})
            } else {
                json!({"type": "add"})
            },
        )
    } else {
        let edits = if name == "MultiEdit" {
            input["edits"].as_array().cloned().unwrap_or_default()
        } else {
            vec![input.clone()]
        };
        let before = old.unwrap_or_else(|| input["old_string"].as_str().unwrap_or("").to_owned());
        let mut after = before.clone();
        for edit in edits {
            let from = edit["old_string"].as_str().unwrap_or("");
            let to = edit["new_string"].as_str().unwrap_or("");
            if edit["replace_all"] == true {
                after = after.replace(from, to);
            } else {
                after = after.replacen(from, to, 1);
            }
        }
        (before, after, json!({"type": "update", "move_path": null}))
    };
    let diff = similar::TextDiff::from_lines(&before, &after)
        .unified_diff()
        .header(path, path)
        .to_string();
    vec![json!({"path": full_path, "kind": kind, "diff": diff})]
}

/// Normalize inputs to canonical Codex records and Anthropic content blocks without flattening images.
pub async fn user_input(input: &Value) -> RpcResult<(Vec<Value>, Vec<Value>)> {
    let input = input
        .as_array()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| RpcError::invalid("input must not be empty"))?;
    let mut codex = Vec::new();
    let mut claude = Vec::new();
    for item in input {
        let mut normalized = item.clone();
        match item["type"].as_str() {
            Some("text") => {
                let text = item["text"]
                    .as_str()
                    .ok_or_else(|| RpcError::invalid("text input requires text"))?;
                if normalized.get("text_elements").is_none() {
                    normalized["text_elements"] = json!([]);
                }
                claude.push(json!({"type": "text", "text": text}));
            }
            Some("skill") => {
                let name = protocol::required_str(item, "name")?;
                let path = Path::new(protocol::required_str(item, "path")?);
                if !path.is_absolute() || path.file_name().is_none_or(|name| name != "SKILL.md") {
                    return Err(RpcError::invalid(
                        "Selected skill must reference an absolute SKILL.md path",
                    ));
                }
                let metadata = tokio::fs::metadata(path)
                    .await
                    .map_err(RpcError::internal)?;
                if !metadata.is_file() || metadata.len() > 1024 * 1024 {
                    return Err(RpcError::invalid(
                        "Selected skill must be a regular file of at most 1 MiB",
                    ));
                }
                let body = tokio::fs::read_to_string(path)
                    .await
                    .map_err(RpcError::internal)?;
                claude.push(json!({"type":"text","text":format!("User selected skill {name} at {}. Apply it to the user's request. Relative references resolve from its containing directory.\n\n{body}",path.display())}));
            }
            Some("mention") => {
                let name = protocol::required_str(item, "name")?;
                let path = protocol::required_str(item, "path")?;
                claude.push(json!({"type":"text","text":format!("User mentioned {name}: {path}")}));
            }
            Some("image") => {
                if item.get("detail").is_some() {
                    return Err(RpcError::invalid(
                        "Claude images do not support Codex detail selection",
                    ));
                }
                let url = item["url"].as_str().ok_or_else(|| {
                    RpcError::invalid("image requires url; OpenAI fileId is unsupported")
                })?;
                let source = if let Some(data) = url.strip_prefix("data:") {
                    let (media_type, data) = data.split_once(";base64,").ok_or_else(|| {
                        RpcError::invalid("image data URL must be base64 encoded")
                    })?;
                    validate_image_type(media_type)?;
                    if data.len() > 28 * 1024 * 1024 || STANDARD.decode(data).is_err() {
                        return Err(RpcError::invalid("Invalid or oversized base64 image"));
                    }
                    json!({"type": "base64", "media_type": media_type, "data": data})
                } else if url.starts_with("https://") || url.starts_with("http://") {
                    json!({"type": "url", "url": url})
                } else {
                    return Err(RpcError::invalid("image URL must use http, https, or data"));
                };
                claude.push(json!({"type": "image", "source": source}));
            }
            Some("localImage") => {
                if item.get("detail").is_some() {
                    return Err(RpcError::invalid(
                        "Claude images do not support Codex detail selection",
                    ));
                }
                let path = item["path"]
                    .as_str()
                    .ok_or_else(|| RpcError::invalid("localImage requires path"))?;
                let (mime, bytes) = read_local_image(Path::new(path))?;
                claude.push(json!({"type": "image", "source": {"type": "base64", "media_type": mime, "data": STANDARD.encode(bytes)}}));
            }
            _ => {
                return Err(RpcError::invalid(
                    "Supported inputs are text, image, localImage, skill, and mention",
                ));
            }
        }
        codex.push(normalized);
    }
    Ok((codex, claude))
}

/// Read a `localImage` attachment, returning its sniffed media type and bytes.
///
/// macOS can refuse a file the desktop itself can read: a screenshot dragged
/// from the capture thumbnail lives under `TemporaryItems`, which only the
/// drop target's process tree may open. The router inlines attachments for
/// that reason; a refusal here names the file rather than surfacing a bare
/// OS error.
pub fn read_local_image(path: &Path) -> RpcResult<(&'static str, Vec<u8>)> {
    if !path.is_absolute() {
        return Err(RpcError::invalid("localImage path must be absolute"));
    }
    let unreadable = |error: std::io::Error| {
        RpcError::invalid(format!(
            "Cannot read attached image {}: {error}",
            path.display()
        ))
    };
    let metadata = std::fs::metadata(path).map_err(unreadable)?;
    if !metadata.is_file() || metadata.len() > 20 * 1024 * 1024 {
        return Err(RpcError::invalid(
            "localImage must be a regular file of at most 20 MiB",
        ));
    }
    let bytes = std::fs::read(path).map_err(unreadable)?;
    let mime = if bytes.starts_with(b"\x89PNG") {
        "image/png"
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        "image/jpeg"
    } else if bytes.starts_with(b"GIF8") {
        "image/gif"
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        "image/webp"
    } else {
        return Err(RpcError::invalid("Unsupported image file type"));
    };
    Ok((mime, bytes))
}

fn validate_image_type(mime: &str) -> RpcResult<()> {
    if ["image/png", "image/jpeg", "image/webp", "image/gif"].contains(&mime) {
        Ok(())
    } else {
        Err(RpcError::invalid("Unsupported image media type"))
    }
}
