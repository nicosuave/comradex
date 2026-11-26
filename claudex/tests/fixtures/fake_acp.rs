//! Deterministic ACP v1 agent for wire tests. No credentials, network, or model calls.
//!
//! Every frame either side sends is validated against the vendored ACP schema
//! (`protocol/acp-v1.unstable.json`, agent-client-protocol 3c229ec, 2026-10-09).
//! A violation exits with status 70 and a stderr diagnostic, which the facade
//! reports as the turn's failure. Behavior is selected by the prompt text and by
//! flags: --store=DIR (required), --no-resume, --no-load, --no-fork, --no-images,
//! --no-models, --legacy-models, --auth-required, --version=N.
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Stdio},
    sync::{Mutex, OnceLock, mpsc},
    time::Duration,
};

fn schema() -> &'static Value {
    static SCHEMA: OnceLock<Value> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        serde_json::from_str(include_str!("../../protocol/acp-v1.unstable.json")).unwrap()
    })
}

fn validators() -> &'static Mutex<HashMap<String, jsonschema::Validator>> {
    static VALIDATORS: OnceLock<Mutex<HashMap<String, jsonschema::Validator>>> = OnceLock::new();
    VALIDATORS.get_or_init(Default::default)
}

/// The definition for a method, by side and suffix (Request/Response/Notification).
fn definition(method: &str, side: &str, suffix: &str) -> Option<String> {
    schema()["$defs"]
        .as_object()
        .unwrap()
        .iter()
        .find(|(name, def)| {
            name.ends_with(suffix) && def["x-method"] == method && def["x-side"] == side
        })
        .map(|(name, _)| name.clone())
}

fn validate(definition: &str, value: &Value, context: &str) {
    let mut validators = validators().lock().unwrap();
    let validator = validators.entry(definition.to_owned()).or_insert_with(|| {
        // Compiling against every definition is slow in debug builds; include
        // only the definitions this one can reach.
        let defs = schema()["$defs"].as_object().unwrap();
        let mut reachable = serde_json::Map::new();
        let mut pending = vec![definition.to_owned()];
        while let Some(name) = pending.pop() {
            if reachable.contains_key(&name) {
                continue;
            }
            let def = defs[&name].clone();
            let mut refs = vec![];
            collect_refs(&def, &mut refs);
            pending.extend(refs);
            reachable.insert(name, def);
        }
        let mut root = defs[definition].clone();
        root["$defs"] = Value::Object(reachable);
        jsonschema::validator_for(&root).unwrap()
    });
    let errors: Vec<String> = validator
        .iter_errors(value)
        .map(|error| format!("{}: {error}", error.instance_path))
        .collect();
    if !errors.is_empty() {
        eprintln!(
            "ACP schema violation ({context}, {definition}): {}\n{value}",
            errors.join("; ")
        );
        std::process::exit(70);
    }
}

fn fail(message: &str) -> ! {
    eprintln!("fake-acp: {message}");
    std::process::exit(71);
}

struct Flags {
    store: PathBuf,
    args: Vec<String>,
}

impl Flags {
    fn has(&self, flag: &str) -> bool {
        self.args.iter().any(|arg| arg == flag)
    }
}

struct Agent {
    flags: Flags,
    incoming: mpsc::Receiver<Value>,
    buffered: VecDeque<Value>,
    next_id: i64,
    cancelled: bool,
}

fn send(message: Value) {
    let mut message = message;
    message["jsonrpc"] = json!("2.0");
    let mut out = std::io::stdout().lock();
    writeln!(out, "{message}").unwrap();
    out.flush().unwrap();
}

impl Agent {
    fn respond(&self, id: &Value, method: &str, result: Value) {
        if let Some(definition) = definition(method, "agent", "Response") {
            validate(&definition, &result, "agent response");
        }
        send(json!({"id": id, "result": result}));
    }

    fn error(&self, id: &Value, code: i64, message: &str) {
        send(json!({"id": id, "error": {"code": code, "message": message}}));
    }

    fn update(&self, session: &str, update: Value) {
        let params = json!({"sessionId": session, "update": update});
        validate("SessionNotification", &params, "agent session/update");
        send(json!({"method": "session/update", "params": params}));
    }

    /// The next client message, validating it on arrival.
    fn recv(&mut self) -> Option<Value> {
        let message = match self.buffered.pop_front() {
            Some(message) => message,
            None => self.incoming.recv().ok()?,
        };
        Some(message)
    }

    /// Send a client-side request and wait for its response. A session/cancel
    /// arriving meanwhile is recorded; other messages are kept for later.
    fn request(&mut self, method: &str, params: Value) -> Value {
        let request_definition = definition(method, "client", "Request")
            .unwrap_or_else(|| fail(&format!("no definition for {method}")));
        validate(&request_definition, &params, "agent request");
        self.next_id += 1;
        let id = format!("agent-{}", self.next_id);
        send(json!({"id": id, "method": method, "params": params}));
        let mut deferred = vec![];
        let response = loop {
            let Ok(message) = self.incoming.recv() else {
                fail("client closed while a request was pending");
            };
            if message["id"] == id.as_str() && message.get("method").is_none() {
                break message;
            }
            if message["method"] == "session/cancel" {
                self.cancelled = true;
                continue;
            }
            deferred.push(message);
        };
        self.buffered.extend(deferred);
        if let Some(result) = response.get("result")
            && let Some(response_definition) = definition(method, "client", "Response")
        {
            validate(&response_definition, result, "client response");
        }
        response
    }

    fn notify(&self, method: &str, params: Value) {
        send(json!({"method": method, "params": params}));
    }

    /// Wait for a session/cancel without blocking other traffic.
    fn wait_for_cancel(&mut self, limit: Duration) -> bool {
        let deadline = std::time::Instant::now() + limit;
        while !self.cancelled {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            match self.incoming.recv_timeout(remaining) {
                Ok(message) if message["method"] == "session/cancel" => self.cancelled = true,
                Ok(message) => self.buffered.push_back(message),
                Err(_) => return false,
            }
        }
        true
    }
}

#[derive(Default, Clone)]
struct Session {
    cwd: String,
    prompts: Vec<Value>,
    replies: Vec<String>,
    model: String,
    effort: String,
    mcp_servers: Vec<Value>,
}

impl Session {
    fn discover_mcp_tools(&self) {
        // Some agents discover MCP tools synchronously before acknowledging the session.
        for server in &self.mcp_servers {
            McpClient::start(server).desktop_catalog();
        }
    }
}

fn session_path(store: &Path, id: &str) -> PathBuf {
    store.join(format!("{id}.json"))
}

fn load_session(store: &Path, id: &str) -> Option<Session> {
    let value: Value =
        serde_json::from_slice(&std::fs::read(session_path(store, id)).ok()?).ok()?;
    Some(Session {
        cwd: value["cwd"].as_str()?.to_owned(),
        prompts: value["prompts"].as_array()?.clone(),
        replies: value["replies"]
            .as_array()?
            .iter()
            .filter_map(|reply| reply.as_str().map(str::to_owned))
            .collect(),
        model: value["model"].as_str()?.to_owned(),
        effort: value["effort"].as_str()?.to_owned(),
        mcp_servers: vec![],
    })
}

fn save_session(store: &Path, id: &str, session: &Session) {
    std::fs::write(
        session_path(store, id),
        json!({"cwd": session.cwd, "prompts": session.prompts, "replies": session.replies,
            "model": session.model, "effort": session.effort})
        .to_string(),
    )
    .unwrap();
}

fn config_options(flags: &Flags, session: &Session) -> Value {
    if flags.has("--no-models") || flags.has("--legacy-models") {
        return json!([]);
    }
    let mut options = json!([
        {"id": "model", "name": "Model", "category": "model", "type": "select",
         "currentValue": session.model,
         "options": [{"group": "main", "name": "Main", "options": [{"value": "fake-a", "name": "Fake A", "description": "First fake model"}]},
                     {"group": "more", "name": "More", "options": [{"value": "fake-b", "name": "Fake B"}]}]},
        {"id": "thinking", "name": "Thinking", "category": "thought_level", "type": "select",
         "currentValue": session.effort,
         "options": [{"value": "low", "name": "Low"}, {"value": "medium", "name": "Medium"},
                     {"value": "high", "name": "High"}]}
    ]);
    if flags.has("--extra-model") {
        options[0]["options"][1]["options"]
            .as_array_mut()
            .unwrap()
            .push(json!({"value":"fake-c","name":"Fake C"}));
    }
    options
}

fn session_response(flags: &Flags, session: &Session) -> Value {
    let mut response = json!({"configOptions": config_options(flags, session)});
    if flags.has("--legacy-models") {
        response["models"] = json!({"currentModelId": session.model,
            "availableModels": [{"modelId": "fake-a", "name": "Fake A"}, {"modelId": "fake-b", "name": "Fake B"}]});
    }
    response
}

fn prompt_text(prompt: &Value) -> String {
    prompt
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|block| block["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

/// The scripted command: the last text block of the prompt.
fn script(prompt: &Value) -> String {
    prompt
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|block| block["text"].as_str())
        .next_back()
        .unwrap_or("")
        .trim()
        .to_owned()
}

fn message(agent: &Agent, session: &str, text: &str) {
    // Two chunks of one message exercise streaming deltas.
    let middle = text.len() / 2;
    let middle = (0..=middle)
        .rev()
        .find(|i| text.is_char_boundary(*i))
        .unwrap();
    for part in [&text[..middle], &text[middle..]] {
        if !part.is_empty() {
            agent.update(
                session,
                json!({"sessionUpdate": "agent_message_chunk", "messageId": "m-1",
                    "content": {"type": "text", "text": part}}),
            );
        }
    }
}

fn permission(agent: &mut Agent, session: &str, call: Value) -> String {
    let response = agent.request(
        "session/request_permission",
        json!({"sessionId": session, "toolCall": call, "options": [
            {"optionId": "yes", "name": "Allow", "kind": "allow_once"},
            {"optionId": "always", "name": "Always", "kind": "allow_always"},
            {"optionId": "no", "name": "Reject", "kind": "reject_once"}]}),
    );
    match response["result"]["outcome"]["outcome"].as_str() {
        Some("selected") => response["result"]["outcome"]["optionId"]
            .as_str()
            .unwrap()
            .to_owned(),
        Some("cancelled") => "cancelled".into(),
        _ => fail(&format!("invalid permission response {response}")),
    }
}

struct McpClient {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}

impl McpClient {
    fn start(server: &Value) -> Self {
        let mut child = std::process::Command::new(server["command"].as_str().unwrap())
            .args(
                server["args"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|arg| arg.as_str().unwrap()),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap_or_else(|error| fail(&format!("cannot start MCP server: {error}")));
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            input,
            output,
        }
    }

    fn call(&mut self, message: Value) -> Option<Value> {
        writeln!(self.input, "{message}").unwrap();
        self.input.flush().unwrap();
        message.get("id")?;
        let mut line = String::new();
        self.output.read_line(&mut line).unwrap();
        Some(serde_json::from_str(&line).unwrap())
    }

    fn desktop_catalog(&mut self) -> Value {
        let init = self
            .call(json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "fake", "version": "1"}}}))
            .unwrap();
        if init["result"]["serverInfo"]["name"] != "codex_desktop" {
            fail(&format!("bad MCP initialize {init}"));
        }
        self.call(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        let response = self
            .call(json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}))
            .unwrap();
        if response["result"]["tools"]
            .as_array()
            .is_none_or(Vec::is_empty)
        {
            fail(&format!("empty MCP catalog {response}"));
        }
        response["result"]["tools"].clone()
    }
}

impl Drop for McpClient {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Run one prompt; returns the stop reason and the reply text recorded.
fn run_prompt(
    agent: &mut Agent,
    session_id: &str,
    session: &mut Session,
    prompt: &Value,
) -> (&'static str, String, Option<Value>) {
    let command = script(prompt);
    let usage = Some(
        json!({"totalTokens": 30, "inputTokens": 20, "outputTokens": 10,
        "cachedReadTokens": 5}),
    );
    let tool = |id: &str, kind: &str, title: &str, extra: Value| {
        let mut call = json!({"sessionUpdate": "tool_call", "toolCallId": id, "title": title,
            "kind": kind, "status": "pending"});
        for (key, value) in extra.as_object().unwrap() {
            call[key] = value.clone();
        }
        call
    };
    // `exec:CMD` (or `exec*N:CMD`) asks permission to run CMD, N times.
    if let Some((count, shell)) = command
        .strip_prefix("exec")
        .and_then(|rest| rest.split_once(':'))
    {
        let count: usize = count.strip_prefix('*').map_or(1, |n| n.parse().unwrap());
        let mut choices = vec![];
        for index in 0..count {
            let id = format!("call-exec-{}-{index}", session.prompts.len());
            let raw = json!({"command": shell});
            agent.update(
                session_id,
                tool(&id, "execute", "Run command", json!({"rawInput": raw})),
            );
            let choice = permission(
                agent,
                session_id,
                json!({"toolCallId": id, "title": "Run command", "kind": "execute", "rawInput": raw}),
            );
            if choice == "cancelled" {
                return ("cancelled", String::new(), None);
            }
            let status = if choice == "no" {
                "failed"
            } else {
                "completed"
            };
            agent.update(
                session_id,
                json!({"sessionUpdate": "tool_call_update", "toolCallId": id, "status": status}),
            );
            choices.push(choice);
        }
        let approved_context = prompt_text(prompt).contains("The user has manually approved");
        let reply = format!(
            "exec {} approved-context={approved_context}",
            choices.join(",")
        );
        message(agent, session_id, &reply);
        return ("end_turn", reply, usage);
    }
    let reply = match command.as_str() {
        "history" => {
            let full = session
                .prompts
                .iter()
                .map(prompt_text)
                .collect::<Vec<_>>()
                .join("\n");
            format!(
                "turns={} instructions={} model={} effort={}",
                session.prompts.len() + 1,
                full.contains("<codex-instructions>")
                    || prompt_text(prompt).contains("<codex-instructions>"),
                session.model,
                session.effort
            )
        }
        "think" => {
            agent.update(
                session_id,
                json!({"sessionUpdate": "agent_thought_chunk", "content": {"type": "text", "text": "pondering"}}),
            );
            "thought".into()
        }
        "tool-exec" | "tool-exec-cancel" => {
            agent.update(
                session_id,
                tool(
                    "call-exec",
                    "execute",
                    "Run echo",
                    json!({"rawInput": {"command": ["echo", "hi"]}}),
                ),
            );
            match permission(
                agent,
                session_id,
                json!({"toolCallId": "call-exec", "title": "Run echo", "kind": "execute",
                    "rawInput": {"command": ["echo", "hi"]}}),
            )
            .as_str()
            {
                "yes" | "always" => {
                    agent.update(session_id, json!({"sessionUpdate": "tool_call_update", "toolCallId": "call-exec",
                        "status": "in_progress", "content": [{"type": "content", "content": {"type": "text", "text": "hi"}}]}));
                    agent.update(session_id, json!({"sessionUpdate": "tool_call_update", "toolCallId": "call-exec",
                        "status": "completed", "content": [{"type": "content", "content": {"type": "text", "text": "hi\nthere"}}],
                        "rawOutput": {"exitCode": 0}}));
                    "ran".into()
                }
                "cancelled" => return ("cancelled", String::new(), None),
                _ => {
                    agent.update(
                        session_id,
                        json!({"sessionUpdate": "tool_call_update", "toolCallId": "call-exec",
                        "status": "failed"}),
                    );
                    "denied".into()
                }
            }
        }
        "tool-edit" => {
            let path = format!("{}/notes.txt", session.cwd);
            let diff =
                json!([{"type": "diff", "path": path, "oldText": "old\n", "newText": "new\n"}]);
            agent.update(
                session_id,
                tool(
                    "call-edit",
                    "edit",
                    "Edit notes",
                    json!({"content": diff, "locations": [{"path": path}]}),
                ),
            );
            let choice = permission(
                agent,
                session_id,
                json!({"toolCallId": "call-edit", "title": "Edit notes", "kind": "edit", "content": diff}),
            );
            let status = if choice == "yes" {
                "completed"
            } else {
                "failed"
            };
            agent.update(session_id, json!({"sessionUpdate": "tool_call_update", "toolCallId": "call-edit", "status": status}));
            format!("edit {choice}")
        }
        "tool-new-file" => {
            let path = format!("{}/created.txt", session.cwd);
            agent.update(
                session_id,
                tool(
                    "call-new",
                    "edit",
                    "Create file",
                    json!({"status": "completed",
                    "content": [{"type": "diff", "path": path, "newText": "fresh\n"}]}),
                ),
            );
            "created".into()
        }
        "tool-read" => {
            let path = format!("{}/notes.txt", session.cwd);
            agent.update(
                session_id,
                tool(
                    "call-read",
                    "read",
                    "Read notes",
                    json!({"locations": [{"path": path, "line": 1}]}),
                ),
            );
            let choice = permission(
                agent,
                session_id,
                json!({"toolCallId": "call-read", "kind": "read", "locations": [{"path": path}]}),
            );
            agent.update(session_id, json!({"sessionUpdate": "tool_call_update", "toolCallId": "call-read",
                "status": "completed", "content": [{"type": "content", "content": {"type": "text", "text": "file body"}}]}));
            format!("read {choice}")
        }
        "tool-fetch" => {
            agent.update(
                session_id,
                tool(
                    "call-fetch",
                    "fetch",
                    "Fetch example",
                    json!({"rawInput": {"url": "https://example.com"}}),
                ),
            );
            let choice = permission(
                agent,
                session_id,
                json!({"toolCallId": "call-fetch", "title": "Fetch example", "kind": "fetch",
                    "rawInput": {"url": "https://example.com"}}),
            );
            agent.update(session_id, json!({"sessionUpdate": "tool_call_update", "toolCallId": "call-fetch",
                "status": "completed", "content": [{"type": "content", "content": {"type": "text", "text": "page"}}]}));
            format!("fetch {choice}")
        }
        "cancel-request" => {
            agent.update(
                session_id,
                tool(
                    "call-gone",
                    "execute",
                    "Vanishing",
                    json!({"rawInput": {"command": "true"}}),
                ),
            );
            // Request permission without waiting, then withdraw it.
            agent.next_id += 1;
            let id = format!("agent-{}", agent.next_id);
            send(
                json!({"id": id, "method": "session/request_permission", "params": {
                "sessionId": session_id, "toolCall": {"toolCallId": "call-gone", "kind": "execute",
                    "rawInput": {"command": "true"}},
                "options": [{"optionId": "yes", "name": "Allow", "kind": "allow_once"}]}}),
            );
            std::thread::sleep(Duration::from_millis(300));
            agent.notify("$/cancel_request", json!({"requestId": id}));
            agent.update(session_id, json!({"sessionUpdate": "tool_call_update", "toolCallId": "call-gone", "status": "failed"}));
            "withdrawn".into()
        }
        "desktop-tool" | "plugin-tool" => {
            let server = session
                .mcp_servers
                .first()
                .cloned()
                .unwrap_or_else(|| fail("no MCP server configured"));
            if server["name"] != "codex_desktop" {
                fail("unexpected MCP server name");
            }
            let mut client = McpClient::start(&server);
            let tools = client.desktop_catalog();
            let (spec, arguments) = if script(prompt) == "plugin-tool" {
                let spec = tools
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|tool| {
                        tool["description"].as_str().is_some_and(|description| {
                            description.starts_with("Codex plugin tool fixture_server/token:")
                        })
                    })
                    .unwrap_or_else(|| fail("missing fixture plugin tool"));
                (spec, json!({"nonce":"acp-catalog"}))
            } else {
                (&tools[0], json!({"query":"abc"}))
            };
            let name = spec["name"].as_str().unwrap().to_owned();
            let mut preview = arguments.clone();
            let tool_name = if script(prompt) == "desktop-tool" {
                preview["toolSummary"] = json!("Look up the fixture");
                format!("mcp_codex_desktop_{name}")
            } else {
                format!("mcp__codex_desktop__{name}")
            };
            agent.update(
                session_id,
                tool(
                    "call-desktop",
                    "other",
                    &tool_name,
                    json!({"name":tool_name,"rawInput":preview}),
                ),
            );
            let choice = permission(
                agent,
                session_id,
                json!({"toolCallId":"call-desktop","name":tool_name,"title":tool_name,"rawInput":preview}),
            );
            if choice != "yes" {
                return ("end_turn", format!("desktop {choice}"), None);
            }
            let result = client
                .call(json!({"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                    "params": {"name": name, "arguments": arguments}}))
                .unwrap();
            agent.update(session_id, json!({"sessionUpdate": "tool_call_update", "toolCallId": "call-desktop", "status": "completed"}));
            format!(
                "desktop result: {}",
                result["result"]["content"][0]["text"]
                    .as_str()
                    .unwrap_or("none")
            )
        }
        "plan" => {
            agent.update(
                session_id,
                json!({"sessionUpdate": "plan", "entries": [
                {"content": "Investigate", "priority": "high", "status": "completed"},
                {"content": "Fix", "priority": "medium", "status": "in_progress"},
                {"content": "Verify", "priority": "low", "status": "pending"}]}),
            );
            "planned".into()
        }
        "notice" => {
            agent.update(
                session_id,
                json!({"sessionUpdate": "notice", "severity": "warning", "title": "Quota nearly exhausted"}),
            );
            agent.update(
                session_id,
                json!({"sessionUpdate": "compaction_update", "compactionId": "c1", "status": "completed"}),
            );
            agent.update(
                session_id,
                json!({"sessionUpdate": "available_commands_update", "availableCommands": []}),
            );
            "noticed".into()
        }
        "usage" => {
            agent.update(
                session_id,
                json!({"sessionUpdate": "usage_update", "used": 1234, "size": 200000}),
            );
            "counted".into()
        }
        "slow" => {
            if agent.wait_for_cancel(Duration::from_secs(20)) {
                agent.cancelled = false;
                return ("cancelled", String::new(), None);
            }
            "slow finished".into()
        }
        "steer" => {
            std::thread::sleep(Duration::from_millis(700));
            "steer acknowledged".into()
        }
        "image" => {
            let images = prompt
                .as_array()
                .unwrap()
                .iter()
                .filter(|block| block["type"] == "image" || block["type"] == "resource_link")
                .map(|block| block["type"].as_str().unwrap())
                .collect::<Vec<_>>()
                .join(",");
            format!("images={images}")
        }
        "fs-request" => {
            let response = agent.request(
                "fs/read_text_file",
                json!({"sessionId": session_id, "path": format!("{}/notes.txt", session.cwd)}),
            );
            format!("fs error {}", response["error"]["code"])
        }
        "refuse" => return ("refusal", String::new(), None),
        "max-tokens" => return ("max_tokens", String::new(), None),
        "error" => return ("error", "scripted failure".into(), None),
        "crash" => {
            eprintln!("fake-acp crashed on purpose");
            std::process::exit(3);
        }
        _ => format!("Echo: {command}"),
    };
    message(agent, session_id, &reply);
    ("end_turn", reply, usage)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let store = args
        .iter()
        .find_map(|arg| arg.strip_prefix("--store="))
        .map(PathBuf::from)
        .unwrap_or_else(|| fail("--store=DIR is required"));
    std::fs::create_dir_all(&store).unwrap();
    let flags = Flags { store, args };
    let (tx, incoming) = mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            let Ok(line) = line else { break };
            let message: Value = serde_json::from_str(&line)
                .unwrap_or_else(|_| fail(&format!("client sent invalid JSON: {line}")));
            if message["jsonrpc"] != "2.0" {
                fail(&format!("client omitted jsonrpc 2.0: {message}"));
            }
            if let Some(method) = message["method"].as_str() {
                let suffix = if message.get("id").is_some() {
                    "Request"
                } else {
                    "Notification"
                };
                let side = if method == "$/cancel_request" {
                    "protocol"
                } else {
                    "agent"
                };
                match definition(method, side, suffix) {
                    Some(definition) => validate(&definition, &message["params"], "client message"),
                    // Removed from the spec, but still used by older agents.
                    None if method == "session/set_model" => {}
                    None => fail(&format!("client used unknown method {method}")),
                }
            }
            if tx.send(message).is_err() {
                break;
            }
        }
    });
    let mut agent = Agent {
        flags,
        incoming,
        buffered: VecDeque::new(),
        next_id: 0,
        cancelled: false,
    };
    let mut sessions: HashMap<String, Session> = HashMap::new();
    while let Some(message) = agent.recv() {
        let Some(method) = message["method"].as_str().map(str::to_owned) else {
            continue;
        };
        let id = message["id"].clone();
        let params = message["params"].clone();
        match method.as_str() {
            "initialize" => {
                if params["protocolVersion"] != 1 {
                    fail("client requested an unexpected protocol version");
                }
                if params["clientCapabilities"]["fs"]["readTextFile"] != false
                    || params["clientCapabilities"]["terminal"] != false
                {
                    fail("client advertised capabilities it does not implement");
                }
                let version: u64 = agent
                    .flags
                    .args
                    .iter()
                    .find_map(|arg| arg.strip_prefix("--version="))
                    .map(|v| v.parse().unwrap())
                    .unwrap_or(1);
                let mut session_capabilities = json!({"list": {}, "close": {}, "delete": {}});
                if !agent.flags.has("--no-resume") {
                    session_capabilities["resume"] = json!({});
                }
                if !agent.flags.has("--no-fork") {
                    session_capabilities["fork"] = json!({});
                }
                agent.respond(
                    &id,
                    "initialize",
                    json!({"protocolVersion": version,
                        "agentCapabilities": {"loadSession": !agent.flags.has("--no-load"),
                            "promptCapabilities": {"image": !agent.flags.has("--no-images"), "audio": false, "embeddedContext": false},
                            "mcpCapabilities": {"http": false, "sse": false},
                            "sessionCapabilities": session_capabilities},
                        "authMethods": [{"id": "login", "name": "Fake login"}],
                        "agentInfo": {"name": "fake-acp", "title": "Fake ACP", "version": "1.0.0"}}),
                );
            }
            "session/new" => {
                if agent.flags.has("--auth-required") {
                    agent.error(&id, -32000, "Authentication required");
                    continue;
                }
                let session_id = format!("sess-{}", uuid::Uuid::new_v4().simple());
                let session = Session {
                    cwd: params["cwd"].as_str().unwrap().to_owned(),
                    model: "fake-a".into(),
                    effort: "medium".into(),
                    mcp_servers: params["mcpServers"].as_array().cloned().unwrap_or_default(),
                    ..Default::default()
                };
                session.discover_mcp_tools();
                save_session(&agent.flags.store, &session_id, &session);
                let mut response = session_response(&agent.flags, &session);
                response["sessionId"] = json!(session_id);
                agent.respond(&id, "session/new", response);
                sessions.insert(session_id.clone(), session);
            }
            "session/resume" | "session/load" | "session/fork" => {
                let source = params["sessionId"].as_str().unwrap();
                let Some(mut session) = load_session(&agent.flags.store, source) else {
                    agent.error(&id, -32002, "Session not found");
                    continue;
                };
                session.cwd = params["cwd"].as_str().unwrap().to_owned();
                session.mcp_servers = params["mcpServers"].as_array().cloned().unwrap_or_default();
                session.discover_mcp_tools();
                if method == "session/load" {
                    // Replay history before answering, as the spec requires.
                    for (prompt, reply) in session.prompts.iter().zip(&session.replies) {
                        agent.update(
                            source,
                            json!({"sessionUpdate": "user_message_chunk",
                            "content": {"type": "text", "text": prompt_text(prompt)}}),
                        );
                        agent.update(
                            source,
                            json!({"sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": reply}}),
                        );
                    }
                }
                let session_id = if method == "session/fork" {
                    let forked = format!("sess-{}", uuid::Uuid::new_v4().simple());
                    save_session(&agent.flags.store, &forked, &session);
                    forked
                } else {
                    source.to_owned()
                };
                let mut response = session_response(&agent.flags, &session);
                if method == "session/fork" {
                    response["sessionId"] = json!(session_id);
                }
                agent.respond(&id, &method, response);
                sessions.insert(session_id.clone(), session);
            }
            "session/set_config_option" | "session/set_model" => {
                let session_id = params["sessionId"].as_str().unwrap().to_owned();
                let session = sessions
                    .get_mut(&session_id)
                    .unwrap_or_else(|| fail("unknown session"));
                let (config, value) = if method == "session/set_model" {
                    ("model", params["modelId"].as_str().unwrap())
                } else {
                    (
                        params["configId"].as_str().unwrap(),
                        params["value"].as_str().unwrap(),
                    )
                };
                match config {
                    "model" => session.model = value.into(),
                    "thinking" => session.effort = value.into(),
                    _ => fail("unknown config option"),
                }
                save_session(&agent.flags.store, &session_id, session);
                let session = session.clone();
                if method == "session/set_model" {
                    send(json!({"id": id, "result": {}}));
                } else {
                    agent.respond(
                        &id,
                        &method,
                        json!({"configOptions": config_options(&agent.flags, &session)}),
                    );
                }
            }
            "session/prompt" => {
                let session_id = params["sessionId"].as_str().unwrap().to_owned();
                let mut session = sessions
                    .get(&session_id)
                    .cloned()
                    .unwrap_or_else(|| fail("prompt for an unknown session"));
                agent.cancelled = false;
                let (stop, reply, usage) =
                    run_prompt(&mut agent, &session_id, &mut session, &params["prompt"]);
                if stop == "error" {
                    agent.error(&id, -32603, &reply);
                    continue;
                }
                session.prompts.push(params["prompt"].clone());
                session.replies.push(reply);
                save_session(&agent.flags.store, &session_id, &session);
                sessions.insert(session_id, session);
                let mut response = json!({"stopReason": stop});
                if let Some(usage) = usage {
                    response["usage"] = usage;
                }
                agent.respond(&id, "session/prompt", response);
            }
            "session/delete" => {
                let session_id = params["sessionId"].as_str().unwrap();
                let _ = std::fs::remove_file(session_path(&agent.flags.store, session_id));
                std::fs::write(agent.flags.store.join(format!("{session_id}.deleted")), "")
                    .unwrap();
                agent.respond(&id, "session/delete", json!({}));
            }
            "session/cancel" => {}
            other => {
                if !id.is_null() {
                    agent.error(&id, -32601, &format!("unsupported {other}"));
                }
            }
        }
    }
}

fn collect_refs(value: &Value, refs: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            if let Some(name) = object
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|reference| reference.strip_prefix("#/$defs/"))
            {
                refs.push(name.to_owned());
            }
            object.values().for_each(|value| collect_refs(value, refs));
        }
        Value::Array(items) => items.iter().for_each(|value| collect_refs(value, refs)),
        _ => {}
    }
}
