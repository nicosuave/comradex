//! Deterministic stand-in for genuine `codex app-server`, used to test the
//! local router. Threads live in memory; `FAKE_CODEX_THREADS=n` seeds `n`
//! persisted threads with descending timestamps. `fake/state` reports what the
//! router asked of this backend. Sections start with Codex's built-in pinned
//! section; `threadSection/list` pages two sections at a time.
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    io::{self, BufRead, BufReader, Write},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

struct Mcp {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    serial: u64,
}

impl Mcp {
    fn start(config: &Value) -> Self {
        let mut child = Command::new(config["command"].as_str().unwrap())
            .args(
                config["args"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|arg| arg.as_str().unwrap()),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut mcp = Self {
            input: child.stdin.take().unwrap(),
            output: BufReader::new(child.stdout.take().unwrap()),
            child,
            serial: 0,
        };
        mcp.request("initialize",json!({"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"fake-codex","version":"1"}}));
        writeln!(
            mcp.input,
            "{}",
            json!({"jsonrpc":"2.0","method":"notifications/initialized"})
        )
        .unwrap();
        let tools = mcp.request("tools/list", json!({}));
        assert!(
            tools["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| tool["name"] == "spawn_agent")
        );
        mcp
    }
    fn request(&mut self, method: &str, params: Value) -> Value {
        self.serial += 1;
        writeln!(
            self.input,
            "{}",
            json!({"jsonrpc":"2.0","id":self.serial,"method":method,"params":params})
        )
        .unwrap();
        self.input.flush().unwrap();
        loop {
            let mut line = String::new();
            assert!(
                self.output.read_line(&mut line).unwrap() > 0,
                "MCP closed before replying"
            );
            let reply: Value = serde_json::from_str(&line).unwrap();
            if reply["id"] == self.serial {
                return reply;
            }
        }
    }
}
impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

type Histories = Arc<Mutex<HashMap<String, Vec<Value>>>>;

fn complete(thread: &str, turn: &str, text: &str, status: &str, histories: &Histories) {
    let turn = json!({"id":turn,"status":status,"items":[{"type":"agentMessage","id":uuid::Uuid::new_v4().to_string(),"text":text,"phase":"final_answer"}],"error":null});
    let mut history = histories.lock().unwrap();
    let turns = history.entry(thread.into()).or_default();
    if let Some(entry) = turns.iter_mut().find(|entry| entry["id"] == turn["id"]) {
        *entry = turn.clone();
    } else {
        turns.push(turn.clone());
    }
    drop(history);
    emit(json!({"method":"turn/completed","params":{"threadId":thread,"turn":turn}}));
}

fn substitute(value: &mut Value, agent: &str) {
    match value {
        Value::String(text) if text == "$agent" => *text = agent.into(),
        Value::Array(values) => {
            for value in values {
                substitute(value, agent);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                substitute(value, agent);
            }
        }
        _ => {}
    }
}

fn script(
    thread: String,
    turn: String,
    actions: Vec<Value>,
    mcp: Arc<Mutex<Mcp>>,
    histories: Histories,
    cancel: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        let mut agent = String::new();
        let mut results = vec![];
        for mut action in actions {
            if cancel.load(Ordering::Acquire) {
                return;
            }
            substitute(&mut action, &agent);
            let reply = mcp.lock().unwrap().request(
                "tools/call",
                json!({"name":action["tool"],"arguments":action["arguments"],
                    "_meta":action.get("meta").cloned().unwrap_or_else(||json!({"threadId":thread}))}),
            );
            let result = reply["result"].clone();
            if let Some(text) = result["content"][0]["text"].as_str()
                && let Ok(value) = serde_json::from_str::<Value>(text)
                && let Some(id) = value["agent_id"].as_str()
            {
                agent = id.into();
            }
            results.push(result);
        }
        if !cancel.load(Ordering::Acquire) {
            complete(
                &thread,
                &turn,
                &json!(results).to_string(),
                "completed",
                &histories,
            );
        }
    });
}

fn settings(params: &Value) -> Value {
    let sandbox = match params["sandbox"].as_str().unwrap_or("danger-full-access") {
        "workspace-write" => {
            let config = &params["config"]["sandbox_workspace_write"];
            json!({"type":"workspaceWrite","writableRoots":config["writable_roots"].as_array().cloned().unwrap_or_default(),
                "networkAccess":config["network_access"].as_bool().unwrap_or(false),"excludeSlashTmp":config["exclude_slash_tmp"].as_bool().unwrap_or(false),
                "excludeTmpdirEnvVar":config["exclude_tmpdir_env_var"].as_bool().unwrap_or(false)})
        }
        "read-only" => json!({"type":"readOnly","networkAccess":false}),
        _ => json!({"type":"dangerFullAccess"}),
    };
    json!({"cwd":params["cwd"].as_str().map(str::to_owned).unwrap_or_else(||std::env::current_dir().unwrap().to_string_lossy().into()),
        "approvalPolicy":params["approvalPolicy"].as_str().unwrap_or("on-request"),
        "approvalsReviewer":params["approvalsReviewer"].as_str().unwrap_or("user"),
        "sandbox":sandbox,"disabledPluginIds":params["disabledPluginIds"].as_array().cloned().unwrap_or_default(),
        "runtimeWorkspaceRoots":params["runtimeWorkspaceRoots"].as_array().cloned().unwrap_or_default(),
        "reasoningEffort":params["config"]["model_reasoning_effort"],"activePermissionProfile":null})
}

fn emit(value: Value) {
    let mut out = io::stdout().lock();
    writeln!(out, "{value}").unwrap();
    out.flush().unwrap();
}

fn update_settings(thread: &str, params: &Value, effective: &mut HashMap<String, Value>) {
    let Some(settings) = effective.get_mut(thread) else {
        return;
    };
    let previous = settings.clone();
    for field in [
        "cwd",
        "approvalPolicy",
        "approvalsReviewer",
        "disabledPluginIds",
    ] {
        if let Some(value) = params.get(field).filter(|value| !value.is_null()) {
            settings[field] = value.clone();
        }
    }
    if let Some(policy) = params.get("sandboxPolicy").filter(|value| !value.is_null()) {
        settings["sandbox"] = policy.clone();
    }
    if *settings != previous {
        let mut reported = settings.clone();
        reported["sandboxPolicy"] = reported.as_object_mut().unwrap().remove("sandbox").unwrap();
        emit(json!({"method":"thread/settings/updated","params":{
            "threadId":thread,"threadSettings":reported}}));
    }
}

fn thread(id: &str, created: i64, model: &str, turns: usize) -> Value {
    json!({"id": id, "preview": "", "modelProvider": "openai", "model": model,
        "createdAt": created, "updatedAt": created, "recencyAt": created, "turns": [],
        "status": {"type": "idle"}, "fakeTurns": turns})
}

const PINNED_SECTION: &str = "01984de2-8f74-7c91-a3b2-5c5e937cf318";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|arg| arg == "--version") {
        println!("codex-cli 9.9.9-fake");
        return;
    }
    assert!(
        args.iter().any(|arg| arg == "app-server"),
        "unexpected fake codex invocation: {args:?}"
    );
    let seeded: i64 = std::env::var("FAKE_CODEX_THREADS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    // Seeded threads predate everything the test creates afterwards.
    let mut threads: Vec<Value> = (0..seeded)
        .map(|n| thread(&format!("codex-seed-{n:03}"), 1_000 + n * 10, "gpt-fake", 1))
        .collect();
    let mut unsubscribed: Vec<String> = vec![];
    let mut requests: Vec<String> = vec![];
    let mut app_params: Vec<Value> = vec![];
    let mut thread_configs = HashMap::<String, Value>::new();
    let mut sections = vec![json!({"id": PINNED_SECTION, "name": "Pinned", "appearance": null})];
    // Section id -> member thread ids in position order.
    let mut members = HashMap::<String, Vec<String>>::new();
    let mut serial = 0u64;
    let mut default_model = "gpt-fake".to_owned();
    let mut effective = HashMap::<String, Value>::new();
    let mut mcps = HashMap::<String, Arc<Mutex<Mcp>>>::new();
    let histories: Histories = Arc::new(Mutex::new(HashMap::new()));
    let mut active = HashMap::<String, (String, Arc<AtomicBool>)>::new();
    let mut permissions = HashMap::<String, (String, String)>::new();
    for line in io::stdin().lock().lines() {
        let message: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let Some(method) = message["method"].as_str() else {
            if let Some((thread, turn)) = permissions.remove(message["id"].as_str().unwrap_or("")) {
                let allowed = message["result"]["decision"] == "accept"
                    || message["result"]["decision"] == "acceptForSession";
                complete(
                    &thread,
                    &turn,
                    if allowed {
                        "Native Codex approved"
                    } else {
                        "Native Codex denied"
                    },
                    "completed",
                    &histories,
                );
            }
            continue;
        };
        let Some(id) = message.get("id").cloned() else {
            continue;
        };
        requests.push(method.to_owned());
        let p = &message["params"];
        let result = match method {
            "initialize" => Ok(
                json!({"userAgent": "codex_cli_rs/9.9.9-fake", "codexHome": "/fake",
                "platformFamily": "unix", "platformOs": "macos", "args": args}),
            ),
            "model/list" => Ok(json!({"data": [
                {"id": "gpt-fake", "model": "gpt-fake", "displayName": "GPT Fake", "isDefault": true,
                    "serviceTiers": [{"id":"ultrafast", "name":"Ultrafast", "description":"Fixture speed tier"}]},
                {"id": "gpt-fake-mini", "model": "gpt-fake-mini", "displayName": "GPT Fake Mini", "isDefault": false}
            ], "nextCursor": null})),
            "config/read" => Ok(json!({"config":{"model":default_model}})),
            "config/batchWrite" => {
                for edit in p["edits"].as_array().into_iter().flatten() {
                    if edit["keyPath"] == "model" {
                        default_model = edit["value"].as_str().unwrap().to_owned();
                    }
                }
                Ok(
                    json!({"status":"ok","version":"fake-config-version","filePath":"/fake/config.toml","overriddenMetadata":null}),
                )
            }
            "thread/start" => {
                let provider = p["modelProvider"].as_str().unwrap_or("openai");
                if p["model"] == "mlx-start-fails" {
                    emit(
                        json!({"id":id,"error":{"code":-32600,"message":"fixture refused MLX start"}}),
                    );
                    continue;
                }
                serial += 1;
                let id = format!("0199{serial:04}-0000-7000-8000-000000000000");
                let model = p["model"].as_str().unwrap_or("gpt-fake");
                let created = 2_000_000_000 + serial as i64;
                let mut started = thread(&id, created, model, 0);
                started["modelProvider"] = json!(provider);
                thread_configs.insert(id.clone(), p["config"].clone());
                effective.insert(id.clone(), settings(p));
                if p["config"]["mcp_servers.claudex_agents"].is_object() {
                    mcps.insert(
                        id.clone(),
                        Arc::new(Mutex::new(Mcp::start(
                            &p["config"]["mcp_servers.claudex_agents"],
                        ))),
                    );
                }
                threads.push(started.clone());
                emit(json!({"method": "thread/started", "params": {"thread": started}}));
                let mut result = effective[&id].clone();
                result["thread"] = started;
                result["model"] = json!(model);
                result["modelProvider"] = json!(provider);
                Ok(result)
            }
            "turn/start" => {
                if p["input"].as_array().into_iter().flatten().any(|part| {
                    part["text"]
                        .as_str()
                        .is_some_and(|text| text.contains("fail-turn-start"))
                }) {
                    emit(
                        json!({"id":id,"error":{"code":-32603,"message":"fixture refused turn start"}}),
                    );
                    continue;
                }
                let thread_id = p["threadId"].as_str().unwrap_or("");
                update_settings(thread_id, p, &mut effective);
                match threads.iter_mut().find(|t| t["id"] == thread_id) {
                    Some(thread) => {
                        thread["fakeTurns"] = json!(thread["fakeTurns"].as_u64().unwrap() + 1);
                        if let Some(model) = p["model"].as_str() {
                            thread["model"] = json!(model);
                        }
                        serial += 1;
                        let turn = json!({"id": format!("codex-turn-{serial}"), "items": [], "status": "inProgress"});
                        let turn_id = turn["id"].as_str().unwrap().to_owned();
                        let cancel = Arc::new(AtomicBool::new(false));
                        active.insert(thread_id.into(), (turn_id.clone(), cancel.clone()));
                        histories
                            .lock()
                            .unwrap()
                            .entry(thread_id.into())
                            .or_default()
                            .push(turn.clone());
                        emit(json!({"id": id, "result": {"turn": turn}}));
                        emit(
                            json!({"method": "turn/started", "params": {"threadId": thread_id, "turn": turn}}),
                        );
                        let text = p["input"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(|part| part["text"].as_str())
                            .collect::<Vec<_>>()
                            .join("\n");
                        if let Some(script_text) = text.strip_prefix("delegate-script:") {
                            let actions: Vec<Value> = serde_json::from_str(script_text).unwrap();
                            script(
                                thread_id.into(),
                                turn_id,
                                actions,
                                mcps[thread_id].clone(),
                                histories.clone(),
                                cancel,
                            );
                        } else if text == "hang" {
                            // Await native steering or interruption from the router.
                        } else if text == "permission"
                            && effective[thread_id]["approvalPolicy"] != "never"
                        {
                            let request = format!("native-codex-approval-{serial}");
                            permissions
                                .insert(request.clone(), (thread_id.into(), turn_id.clone()));
                            emit(
                                json!({"id":request,"method":"item/commandExecution/requestApproval","params":{
                                "threadId":thread_id,"turnId":turn_id,"itemId":"native-command","command":"printf fixture","cwd":effective[thread_id]["cwd"]}}),
                            );
                        } else {
                            let report = if text == "permission" {
                                "Native Codex denied without prompting".to_owned()
                            } else if text == "report service tier" {
                                p["serviceTierForTurn"]
                                    .as_str()
                                    .unwrap_or("default")
                                    .to_owned()
                            } else {
                                format!("Codex: {text}")
                            };
                            complete(thread_id, &turn_id, &report, "completed", &histories);
                        }
                        continue;
                    }
                    None => Err("thread not found"),
                }
            }
            "thread/read" | "thread/resume" => {
                let thread_id = p["threadId"].as_str().unwrap();
                if method == "thread/resume"
                    && p["config"]["mcp_servers.claudex_agents"].is_object()
                {
                    mcps.insert(
                        thread_id.into(),
                        Arc::new(Mutex::new(Mcp::start(
                            &p["config"]["mcp_servers.claudex_agents"],
                        ))),
                    );
                }
                threads
                    .iter()
                    .find(|t| t["id"] == thread_id)
                    .map(|t| {
                        let mut thread = t.clone();
                        thread["turns"] = json!(
                            histories
                                .lock()
                                .unwrap()
                                .get(thread_id)
                                .cloned()
                                .unwrap_or_default()
                        );
                        let mut result = effective
                            .get(thread_id)
                            .cloned()
                            .unwrap_or_else(|| json!({}));
                        result["thread"] = thread;
                        result["model"] = t["model"].clone();
                        result["modelProvider"] = t["modelProvider"].clone();
                        result
                    })
                    .ok_or("thread not found")
            }
            "turn/interrupt" => match active.get(p["threadId"].as_str().unwrap()) {
                Some((turn, cancel)) if *turn == p["turnId"] => {
                    cancel.store(true, Ordering::Release);
                    complete(
                        p["threadId"].as_str().unwrap(),
                        turn,
                        "Native Codex interrupted",
                        "interrupted",
                        &histories,
                    );
                    Ok(json!({}))
                }
                _ => Err("no matching native turn"),
            },
            "turn/steer" => {
                let thread_id = p["threadId"].as_str().unwrap();
                match active.get(thread_id) {
                    Some((turn, _)) if *turn == p["expectedTurnId"] => {
                        complete(
                            thread_id,
                            turn,
                            &format!("Codex steered: {}", p["input"][0]["text"].as_str().unwrap()),
                            "completed",
                            &histories,
                        );
                        Ok(json!({"turnId":turn}))
                    }
                    _ => Err("no matching native turn"),
                }
            }
            "thread/name/set" => {
                if let Some(thread) = threads.iter_mut().find(|t| t["id"] == p["threadId"]) {
                    thread["name"] = p["name"].clone();
                }
                Ok(json!({}))
            }
            "thread/metadata/update" => match threads.iter_mut().find(|t| t["id"] == p["threadId"])
            {
                Some(thread) => {
                    for field in ["projectId", "gitInfo", "daybreakEnabled"] {
                        if let Some(value) = p.get(field) {
                            thread[field] = value.clone();
                        }
                    }
                    Ok(json!({"thread":thread}))
                }
                None => Err("thread not found"),
            },
            "thread/settings/update" => match threads.iter_mut().find(|t| t["id"] == p["threadId"])
            {
                Some(thread) => {
                    if let Some(model) = p.get("model") {
                        thread["model"] = model.clone();
                    }
                    update_settings(p["threadId"].as_str().unwrap(), p, &mut effective);
                    Ok(json!({}))
                }
                None => Err("thread not found"),
            },
            "thread/unsubscribe" => {
                unsubscribed.push(p["threadId"].as_str().unwrap_or("").to_owned());
                emit(json!({"method":"thread/closed","params":{"threadId":p["threadId"]}}));
                threads.retain(|t| t["id"] != p["threadId"] || t["fakeTurns"] != 0);
                Ok(json!({"status": "unsubscribed"}))
            }
            "thread/loaded/list" => Ok(
                json!({"data": threads.iter().map(|t| t["id"].clone()).collect::<Vec<_>>(), "nextCursor": null}),
            ),
            // Fake Codex threads own no attachments.
            "thread/attachmentOwner/list" => Ok(json!({"data": [], "nextCursor": null})),
            "app/list" | "app/installed" | "app/read" => {
                app_params.push(p.clone());
                Ok(json!({"data": [], "nextCursor": null}))
            }
            "threadSection/list" => {
                let start: usize = p["cursor"]
                    .as_str()
                    .map(|c| c.strip_prefix("fake-section:").unwrap().parse().unwrap())
                    .unwrap_or(0);
                let end = (start + 2).min(sections.len());
                let next = (end < sections.len()).then(|| format!("fake-section:{end}"));
                Ok(json!({"data": &sections[start..end], "nextCursor": next}))
            }
            "threadSection/create" => {
                serial += 1;
                let section = json!({"id": format!("0199{serial:04}-0000-7000-8000-5ec710000000"),
                    "name": p["name"], "appearance": null});
                sections.push(section.clone());
                Ok(json!({"section": section}))
            }
            "threadSection/update" => {
                match sections.iter_mut().find(|s| s["id"] == p["sectionId"]) {
                    Some(section) => {
                        section["name"] = p["name"].clone();
                        Ok(json!({"section": section}))
                    }
                    None => Err("thread section not found"),
                }
            }
            "threadSection/delete" => {
                // As in Codex's state DB, members become unsectioned.
                sections.retain(|s| s["id"] != p["sectionId"]);
                members.remove(p["sectionId"].as_str().unwrap_or(""));
                for thread in threads.iter_mut() {
                    if thread["section"]["id"] == p["sectionId"] {
                        thread["section"] = Value::Null;
                    }
                }
                Ok(json!({}))
            }
            "thread/section/move" => {
                let thread_id = p["threadId"].as_str().unwrap_or("").to_owned();
                let section = sections.iter().find(|s| s["id"] == p["sectionId"]).cloned();
                let before = p["beforeThreadId"].as_str();
                if !threads.iter().any(|t| t["id"] == thread_id.as_str()) {
                    Err("thread not found")
                } else if !p["sectionId"].is_null() && section.is_none() {
                    Err("section does not exist")
                } else if before.is_some_and(|before| {
                    !members
                        .get(p["sectionId"].as_str().unwrap_or(""))
                        .is_some_and(|list| list.iter().any(|id| id == before))
                }) {
                    Err("before thread is not in section")
                } else {
                    for list in members.values_mut() {
                        list.retain(|id| *id != thread_id);
                    }
                    if let Some(section) = &section {
                        let list = members
                            .entry(section["id"].as_str().unwrap().to_owned())
                            .or_default();
                        let index = before
                            .and_then(|before| list.iter().position(|id| id == before))
                            .unwrap_or(list.len());
                        list.insert(index, thread_id.clone());
                    }
                    let thread = threads
                        .iter_mut()
                        .find(|t| t["id"] == thread_id.as_str())
                        .unwrap();
                    thread["section"] = section.unwrap_or(Value::Null);
                    Ok(json!({}))
                }
            }
            "thread/list" | "thread/search" => {
                let field = match p["sortKey"].as_str() {
                    Some("updated_at") => "updatedAt",
                    _ => "createdAt",
                };
                let mut listed: Vec<Value> = threads
                    .iter()
                    .filter(|t| {
                        p["modelProviders"].as_array().map_or(
                            t["modelProvider"] == "openai",
                            |providers| {
                                providers.is_empty() || providers.contains(&t["modelProvider"])
                            },
                        ) && (p["parentThreadId"].is_null()
                            || t["parentThreadId"] == p["parentThreadId"])
                            && (p["ancestorThreadId"].is_null()
                                || t["parentThreadId"] == p["ancestorThreadId"])
                            && p["searchTerm"]
                                .as_str()
                                .is_none_or(|term| t["id"].as_str().unwrap().contains(term))
                            && p.get("sectionId")
                                .is_none_or(|section| t["section"]["id"] == *section)
                    })
                    .cloned()
                    .collect();
                if p["sortKey"] == "section_position" {
                    let order = members
                        .get(p["sectionId"].as_str().unwrap_or(""))
                        .cloned()
                        .unwrap_or_default();
                    listed.sort_by_key(|t| order.iter().position(|id| t["id"] == id.as_str()));
                } else {
                    listed.sort_by_key(|t| -t[field].as_i64().unwrap());
                }
                let start: usize = p["cursor"]
                    .as_str()
                    .map(|c| c.strip_prefix("fake:").unwrap().parse().unwrap())
                    .unwrap_or(0);
                let limit = p["limit"].as_u64().unwrap_or(50) as usize;
                let end = (start + limit).min(listed.len());
                let page: Vec<Value> = listed[start.min(end)..end]
                    .iter()
                    .map(|t| {
                        if method == "thread/search" {
                            json!({"thread": t, "snippet": t["preview"]})
                        } else {
                            t.clone()
                        }
                    })
                    .collect();
                let next = (end < listed.len()).then(|| format!("fake:{end}"));
                Ok(json!({"data": page, "nextCursor": next, "backwardsCursor": "fake-backwards"}))
            }
            "fake/state" => Ok(
                json!({"unsubscribed": unsubscribed, "requests": requests, "apps": app_params, "threadConfigs":thread_configs,
                    "pid":std::process::id(), "parentPid":unsafe { libc::getppid() }}),
            ),
            _ => Err("unsupported"),
        };
        emit(match result {
            Ok(result) => json!({"id": id, "result": result}),
            Err(message) => json!({"id": id, "error": {"code": -32600, "message": message}}),
        });
    }
}
