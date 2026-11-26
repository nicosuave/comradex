//! Deterministic CLI stand-in. No credentials, network, or Claude calls.
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::io::{self, BufRead, Write};

fn substitute_agent(value: &mut Value, agent: &str) {
    match value {
        Value::String(text) if text == "$agent" => *text = agent.into(),
        Value::Array(values) => {
            for value in values {
                substitute_agent(value, agent);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                substitute_agent(value, agent);
            }
        }
        _ => {}
    }
}

fn delegation_action(action: &Value, agent: &str) {
    let mut arguments = action["arguments"].clone();
    substitute_agent(&mut arguments, agent);
    emit(
        json!({"type":"control_request","request_id":"delegation_call","request":{
        "subtype":"mcp_message","server_name":"codex_desktop","message":{"jsonrpc":"2.0","id":99,
            "method":"tools/call","params":{"name":format!("claudex_agents__{}",action["tool"].as_str().unwrap()),"arguments":arguments}}}}),
    );
}

fn emit(value: Value) {
    let mut out = io::stdout().lock();
    writeln!(out, "{value}").unwrap();
    out.flush().unwrap();
}

fn finish(session: &str, text: &str, error: bool) {
    finish_with(session, text, error, json!({}));
}

/// `result` fields are merged into the result message.
fn finish_with(session: &str, text: &str, error: bool, result: Value) {
    let message_id = format!("msg_{}", uuid::Uuid::new_v4());
    let assistant_id = uuid::Uuid::new_v4().to_string();
    let input_tokens = if text == "Echo: cold-cache" {
        30_000
    } else {
        4
    };
    emit(
        json!({"type":"stream_event","session_id":session,"event":{"type":"message_start","message":{"id":message_id,"type":"message","role":"assistant","content":[],"model":"fake-claude","usage":{"input_tokens":input_tokens,"output_tokens":0}}}}),
    );
    emit(
        json!({"type":"stream_event","session_id":session,"event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}}),
    );
    emit(
        json!({"type":"stream_event","session_id":session,"event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}}),
    );
    emit(
        json!({"type":"stream_event","session_id":session,"event":{"type":"content_block_stop","index":0}}),
    );
    emit(
        json!({"type":"assistant","session_id":session,"uuid":assistant_id,"parent_tool_use_id":null,"message":{"id":message_id,"type":"message","role":"assistant","model":"fake-claude","content":[{"type":"text","text":text}],"stop_reason":"end_turn","usage":{"input_tokens":input_tokens,"output_tokens":3}}}),
    );
    let mut message = json!({"type":"result","subtype":if error {"error_during_execution"} else {"success"},"is_error":error,"result":text,"errors":if error {vec![text]} else {vec![]},"session_id":session,"duration_ms":1,"duration_api_ms":1,"num_turns":1,"total_cost_usd":0.0,"usage":{"input_tokens":if text == "Echo: usage-probe" {40} else {4},"output_tokens":if text == "Echo: usage-probe" {30} else {3}},"modelUsage":{"fake-claude":{"contextWindow":200000},"auxiliary-model":{"contextWindow":1000000}},"permission_denials":[]});
    message
        .as_object_mut()
        .unwrap()
        .extend(result.as_object().unwrap().clone());
    emit(message);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let read_only_helper = args.iter().any(|arg| arg == "--tools=");
    if read_only_helper {
        for required in [
            "--safe-mode",
            "--strict-mcp-config",
            "--disable-slash-commands",
            "--no-chrome",
            "--permission-mode=dontAsk",
            "--no-session-persistence",
        ] {
            assert!(
                args.iter().any(|arg| arg == required),
                "missing helper boundary: {required}"
            );
        }
        assert!(
            !args
                .iter()
                .any(|arg| arg == "--mcp-config" || arg.starts_with("--resume="))
        );
    }
    if args.get(1).is_some_and(|arg| arg == "plugin") {
        emit(json!({"installed":[],"available":[]}));
        return;
    }
    let session = args
        .iter()
        .find_map(|arg| arg.strip_prefix("--session-id="))
        .or_else(|| args.iter().find_map(|arg| arg.strip_prefix("--resume=")))
        .unwrap_or("fake-session");
    // Model the native control responses for the explicit managed policy
    // supplied by the test client. This fixture does not enforce an OS sandbox;
    // real native enforcement is verified separately by integration probes.
    let managed: Option<Value> = args
        .iter()
        .find_map(|arg| arg.strip_prefix("--managed-settings="))
        .map(|raw| serde_json::from_str(raw).unwrap());
    let mut hanging = false;
    let mut background = false;
    let mut prompts = 0;
    let mut predictions = 0;
    let mut pending_prediction: Option<String> = None;
    // With --fake-mcp-pending, a server reports connecting for the first status polls.
    let mut mcp_polls = 0;
    let mut prompted_while_connecting = false;
    let mut steering_inputs: Option<Vec<String>> = None;
    let mut delegation_actions: VecDeque<Value> = VecDeque::new();
    let mut delegation_results = Vec::<Value>::new();
    let mut delegation_agent = String::new();
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else {
            break;
        };
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        match value["type"].as_str() {
            Some("control_request") => {
                let request_id = &value["request_id"];
                match value["request"]["subtype"].as_str() {
                    Some("initialize") => {
                        if args.iter().any(|arg| arg == "--fake-init-delay") {
                            std::thread::sleep(std::time::Duration::from_millis(200));
                        }
                        if args.iter().any(|arg| arg == "--fake-init-crash") {
                            eprintln!("fake initialization failure");
                            std::process::exit(23);
                        }
                        if args.iter().any(|arg| arg == "--fake-init-hang") {
                            continue;
                        }
                        println!("fake diagnostic before initialization");
                        let mut models = vec![
                            json!({"value":"fake-claude","displayName":"Fake Claude","description":"Deterministic test backend"}),
                        ];
                        for (value, display) in [
                            ("opus", "Opus test"),
                            ("fable", "Fable test"),
                            ("sonnet", "Sonnet test"),
                            ("haiku", "Haiku test"),
                        ] {
                            let mut model = json!({"value":value,"displayName":display,"description":"Fixture model",
                                "supportedEffortLevels":["low","medium","high","xhigh","max"]});
                            if value == "opus" {
                                model["supportsFastMode"] = json!(true);
                            }
                            models.push(model);
                        }
                        emit(
                            json!({"type":"control_response","response":{"subtype":"success","request_id":request_id,"response":{"commands":[],"models":models}}}),
                        );
                        emit(
                            json!({"type":"system","subtype":"init","session_id":session,"cwd":std::env::current_dir().unwrap(),"tools":if read_only_helper {vec![]} else {vec!["Bash"]},"mcp_servers":[],"model":"fake-claude","permissionMode":if read_only_helper {"dontAsk"} else {"default"}}),
                        );
                    }
                    Some("mcp_status") if args.iter().any(|arg| arg == "--fake-mcp-pending") => {
                        mcp_polls += 1;
                        let status = if mcp_polls < 3 {
                            "pending"
                        } else {
                            "connected"
                        };
                        emit(
                            json!({"type":"control_response","response":{"subtype":"success","request_id":request_id,"response":{"mcpServers":[{"name":"slow","status":status}]}}}),
                        );
                    }
                    Some(
                        subtype @ ("get_settings" | "get_sandbox_dialog" | "list_permission_rules"),
                    ) if managed.is_some() => {
                        let policy = managed.as_ref().unwrap();
                        let response = match subtype {
                            "get_settings" => json!({"effective":policy}),
                            "get_sandbox_dialog" => json!({
                                "supported":true,"enabled":true,"enabled_in_settings":true,
                                "locked":true,"overrides_locked":true,
                                "excluded_commands":[],
                                "no_sandbox_allowed":policy["sandbox"]["allowUnsandboxedCommands"],
                                "restrictions":{
                                    "network_managed":true,"allow_all_unix_sockets":false,"unix_sockets":[],
                                    "fs_allow_write":policy["sandbox"]["filesystem"]["allowWrite"],
                                    "fs_deny_write":policy["sandbox"]["filesystem"]["denyWrite"],
                                    "network_allowed_domains":policy["sandbox"]["network"]["allowedDomains"]
                                }
                            }),
                            _ => {
                                let mut rules = Vec::new();
                                for behavior in ["allow", "deny", "ask"] {
                                    for rule in policy["permissions"][behavior]
                                        .as_array()
                                        .into_iter()
                                        .flatten()
                                    {
                                        rules.push(json!({"rule":rule,"behavior":behavior,"notInEffect":false}));
                                    }
                                }
                                json!({"state":{"managedOnly":true,"rules":rules}})
                            }
                        };
                        emit(
                            json!({"type":"control_response","response":{"subtype":"success","request_id":request_id,"response":response}}),
                        );
                    }
                    Some("side_question") => {
                        predictions += 1;
                        if let Some(path) = args
                            .iter()
                            .find_map(|arg| arg.strip_prefix("--fake-prediction-log="))
                        {
                            let mut log = std::fs::OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open(path)
                                .unwrap();
                            writeln!(
                                log,
                                "{}",
                                json!({"kind":"start","requestId":request_id,
                                    "question":value["request"]["question"]})
                            )
                            .unwrap();
                        }
                        let question = value["request"]["question"].as_str().unwrap();
                        if question.starts_with("hang-prediction") {
                            pending_prediction = request_id.as_str().map(str::to_owned);
                            emit(json!({"type":"system","subtype":"control_request_progress",
                                "request_id":request_id,"status":"started"}));
                        } else {
                            let response = json!({"suggestion":format!("process {} prediction {predictions} prompt {prompts}",std::process::id())}).to_string();
                            emit(
                                json!({"type":"control_response","response":{"subtype":"success",
                                "request_id":request_id,"response":{"response":response,"synthetic":false}}}),
                            );
                        }
                    }
                    Some("interrupt") => {
                        emit(
                            json!({"type":"control_response","response":{"subtype":"success","request_id":request_id,"response":{}}}),
                        );
                        if hanging {
                            hanging = false;
                            finish(session, "Interrupted", true);
                        }
                    }
                    _ => emit(
                        json!({"type":"control_response","response":{"subtype":"error","request_id":request_id,"error":"unsupported fake control request"}}),
                    ),
                }
            }
            Some("control_cancel_request") => {
                if pending_prediction.as_deref() == value["request_id"].as_str() {
                    if let Some(path) = args
                        .iter()
                        .find_map(|arg| arg.strip_prefix("--fake-prediction-log="))
                    {
                        let mut log = std::fs::OpenOptions::new().append(true).open(path).unwrap();
                        writeln!(
                            log,
                            "{}",
                            json!({"kind":"cancel","requestId":value["request_id"]})
                        )
                        .unwrap();
                    }
                    pending_prediction = None;
                }
            }
            Some("user") => {
                prompts += 1;
                prompted_while_connecting |=
                    args.iter().any(|arg| arg == "--fake-mcp-pending") && mcp_polls < 3;
                let content = &value["message"]["content"];
                let text = content.as_str().map(str::to_owned).unwrap_or_else(|| {
                    content
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|part| part["text"].as_str())
                        .collect::<Vec<_>>()
                        .join("\n")
                });
                if let Some(inputs) = &mut steering_inputs {
                    if inputs.is_empty() {
                        finish(
                            session,
                            "First result before queued input is consumed",
                            false,
                        );
                        emit(
                            json!({"type":"system","subtype":"session_state_changed","state":"idle"}),
                        );
                    }
                    inputs.push(text.clone());
                    emit(
                        json!({"type":"user","uuid":value["uuid"],"isReplay":true,"message":value["message"],"session_id":session,"parent_tool_use_id":null}),
                    );
                    if inputs.len() == 2 {
                        finish(session, &format!("Steered: {}", inputs.join(" + ")), false);
                        emit(
                            json!({"type":"system","subtype":"session_state_changed","state":"idle"}),
                        );
                        steering_inputs = None;
                    }
                    continue;
                }
                emit(
                    json!({"type":"user","uuid":value["uuid"],"isReplay":true,"message":value["message"],"session_id":session,"parent_tool_use_id":null}),
                );
                match text.as_str() {
                    text if text.starts_with("delegate-script:") => {
                        delegation_actions=serde_json::from_str(text.strip_prefix("delegate-script:").unwrap()).unwrap();
                        delegation_results.clear(); delegation_agent.clear();
                        let action=delegation_actions.pop_front().unwrap(); delegation_action(&action,&delegation_agent);
                    }
                    "steer-busy" | "steer-closed-input" => {
                        if text == "steer-closed-input" {
                            unsafe { libc::close(0); }
                        }
                        emit(json!({"type":"assistant","session_id":session,"message":{"id":"busy","role":"assistant","content":[{"type":"text","text":"Backend input paused"}]}}));
                        std::thread::sleep(std::time::Duration::from_secs(2));
                    }
                    text if text.starts_with("large-steer:") => finish(session, "Large input consumed", false),
                    "steer-wait" => {
                        steering_inputs = Some(Vec::new());
                        emit(json!({"type":"system","subtype":"session_state_changed","state":"running"}));
                        emit(json!({"type":"assistant","session_id":session,"message":{"id":"waiting","role":"assistant","content":[{"type":"text","text":"Waiting for steering"}]}}));
                    }
                    "question" => {
                        emit(json!({"type":"control_request","request_id":"question_fake","request":{"subtype":"can_use_tool","tool_name":"AskUserQuestion","tool_use_id":"question_tool","input":{"questions":[{"header":"Choice","question":"Which value?","options":[{"label":"One","description":"First"},{"label":"Two","description":"Second"}],"multiSelect":false}]}}}));
                    }
                    "desktop-tool" | "desktop-invalid" | "desktop-unknown" => {
                        assert!(args.iter().any(|a| a.contains("\"codex_desktop\"")), "missing SDK MCP registration");
                        let name = if text == "desktop-unknown" {"missing"} else {"desktop__echo"};
                        let arguments = if text == "desktop-invalid" {json!({"value":42})} else {json!({"value":"from Claude"})};
                        emit(json!({"type":"control_request","request_id":"desktop_call",
                            "request":{"subtype":"mcp_message","server_name":"codex_desktop",
                                "message":{"jsonrpc":"2.0","id":73,"method":"tools/call","params":{"name":name,"arguments":arguments}}}}));
                    }
                    "hang" => {
                        hanging = true;
                        emit(json!({"type":"stream_event","session_id":session,"event":{"type":"message_start","message":{"id":"working"}}}));
                        emit(json!({"type":"stream_event","session_id":session,"event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}}));
                        emit(json!({"type":"stream_event","session_id":session,"event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Working"}}}));
                    }
                    "process-info" => finish(session, &format!("process {} prompt {prompts}", std::process::id()), false),
                    "mcp-info" => finish(session, &format!("prompted while MCP connecting: {prompted_while_connecting}"), false),
                    "fast-info" => {
                        // Claude reads fast mode for SDK sessions only from flag settings.
                        let requested = args
                            .iter()
                            .filter_map(|a| a.strip_prefix("--settings="))
                            .any(|raw| serde_json::from_str::<Value>(raw).unwrap()["fastMode"] == true);
                        let state = if requested {
                            json!({"fast_mode_state":"off","fast_mode_disabled_reason":"extra_usage_disabled"})
                        } else {
                            json!({"fast_mode_state":"off","fast_mode_disabled_reason":"sdk_opt_in_required"})
                        };
                        finish_with(session, &format!("fast mode {requested}"), false, state);
                    }
                    "resume-info" => finish(session, if args.iter().any(|a| a.starts_with("--resume=")) { "resumed" } else { "new" }, false),
                    "fork-info" => finish(session, &json!({
                        "resume":args.iter().find_map(|a| a.strip_prefix("--resume=")),
                        "resumeAt":args.iter().find_map(|a| a.strip_prefix("--resume-session-at=")),
                        "fork":args.iter().any(|a| a=="--fork-session"),
                        "systemPrompt":args.iter().find_map(|a| a.strip_prefix("--system-prompt=")),
                        "appendSystemPrompt":args.iter().find_map(|a| a.strip_prefix("--append-system-prompt=")),
                        "ephemeral":args.iter().any(|a| a=="--no-session-persistence"),
                        "session":session
                    }).to_string(), false),
                    text if text.starts_with("background-bash") => {
                        // An optional suffix sets how long the command runs, in milliseconds.
                        let delay = text
                            .strip_prefix("background-bash ")
                            .map_or(300, |ms| ms.parse().unwrap());
                        // Claude Code 2.1.296: a background command outlives its turn,
                        // and Claude answers its notification without a new prompt.
                        emit(json!({"type":"system","subtype":"task_started","task_id":"bash-1","task_type":"local_bash","description":"Sleep then echo"}));
                        finish(session, "Started", false);
                        let session = session.to_owned();
                        std::thread::spawn(move || {
                            std::thread::sleep(std::time::Duration::from_millis(delay));
                            emit(json!({"type":"system","subtype":"task_notification","task_id":"bash-1","status":"completed","summary":"Background command completed (exit code 0)"}));
                            emit(json!({"type":"system","subtype":"init","session_id":session,"model":"fake-claude"}));
                            finish(&session, "Background output: BGDONE", false);
                        });
                    }
                    "background" => {
                        background = true;
                        emit(json!({"type":"system","subtype":"session_state_changed","state":"running"}));
                        emit(json!({"type":"system","subtype":"task_started","task_id":"bg","task_type":"local_agent","description":"Background fixture"}));
                        finish(session, "Parent waiting", false);
                        emit(json!({"type":"control_request","request_id":"permission_fake","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"printf fake"},"tool_use_id":"tool_fake"}}));
                    }
                    "subagent" => {
                        // Foreground Agent call, frames shaped like Claude Code 2.1.294.
                        let child = |kind: &str, content: Value| json!({"type":kind,"session_id":session,
                            "uuid":uuid::Uuid::new_v4().to_string(),"parent_tool_use_id":"agent_call","agent_id":"agent-1",
                            "message":{"id":format!("msg_{}", uuid::Uuid::new_v4()),"role":if kind == "user" {"user"} else {"assistant"},
                                "model":"fake-child","content":content,"usage":{"input_tokens":7,"output_tokens":2}}});
                        emit(json!({"type":"assistant","session_id":session,"uuid":uuid::Uuid::new_v4().to_string(),"parent_tool_use_id":null,
                            "message":{"id":"msg_agent","role":"assistant","content":[{"type":"tool_use","id":"agent_call","name":"Agent",
                                "input":{"description":"Inspect","prompt":"Look around","subagent_type":"general-purpose","name":"scout"}}]}}));
                        emit(json!({"type":"system","subtype":"task_started","task_id":"agent-1","tool_use_id":"agent_call",
                            "task_type":"local_agent","is_backgrounded":false,"description":"Inspect"}));
                        emit(child("user", json!([{"type":"text","text":"Look around"}])));
                        emit(child("assistant", json!([{"type":"text","text":"Child working"},
                            {"type":"tool_use","id":"child_bash","name":"Bash","input":{"command":"ls"}}])));
                        emit(child("user", json!([{"type":"tool_result","tool_use_id":"child_bash","content":"listing"}])));
                        // A nested agent emits no frames of its own, only task events.
                        emit(child("assistant", json!([{"type":"tool_use","id":"nested_call","name":"Agent",
                            "input":{"description":"Nested","prompt":"Go deeper","subagent_type":"Explore"}}])));
                        emit(json!({"type":"system","subtype":"task_started","task_id":"agent-1b","tool_use_id":"nested_call",
                            "task_type":"local_agent","is_backgrounded":false,"spawn_depth":2,"parent_task_id":"agent-1"}));
                        let mut nested = child("user", json!([{"type":"tool_result","tool_use_id":"nested_call","content":"Nested report"}]));
                        nested["tool_use_result"] = json!({"status":"completed","agentId":"agent-1b","content":[{"type":"text","text":"Nested report"}]});
                        emit(nested);
                        emit(json!({"type":"system","subtype":"task_notification","task_id":"agent-1b","tool_use_id":"nested_call",
                            "status":"completed","summary":"Nested report"}));
                        emit(child("assistant", json!([{"type":"text","text":"Child done"}])));
                        emit(json!({"type":"user","session_id":session,"parent_tool_use_id":null,
                            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"agent_call","content":"Report: all good"}]},
                            "tool_use_result":{"status":"completed","agentId":"agent-1","content":[{"type":"text","text":"Report: all good"}],
                                "usage":{"input_tokens":10,"output_tokens":5}}}));
                        emit(json!({"type":"system","subtype":"task_updated","task_id":"agent-1","patch":{"status":"completed"}}));
                        emit(json!({"type":"system","subtype":"task_notification","task_id":"agent-1","tool_use_id":"agent_call",
                            "status":"completed","summary":"Report: all good"}));
                        finish(session, "Parent done", false);
                    }
                    "subagent-continue" => {
                        // SendMessage resumes agent-1 from an earlier turn.
                        emit(json!({"type":"assistant","session_id":session,"uuid":uuid::Uuid::new_v4().to_string(),"parent_tool_use_id":null,
                            "message":{"id":"msg_send","role":"assistant","content":[{"type":"tool_use","id":"send_call","name":"SendMessage",
                                "input":{"to":"agent-1","summary":"More","message":"More please"}}]}}));
                        emit(json!({"type":"user","session_id":session,"parent_tool_use_id":null,
                            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"send_call","content":"{\"success\":true}"}]}}));
                        emit(json!({"type":"user","session_id":session,"uuid":uuid::Uuid::new_v4().to_string(),
                            "parent_tool_use_id":"send_call","agent_id":"agent-1","message":{"role":"user","content":[{"type":"text","text":"More please"}]}}));
                        emit(json!({"type":"assistant","session_id":session,"uuid":uuid::Uuid::new_v4().to_string(),
                            "parent_tool_use_id":"send_call","agent_id":"agent-1",
                            "message":{"id":"msg_continued","role":"assistant","content":[{"type":"text","text":"Continued"}]}}));
                        emit(json!({"type":"system","subtype":"task_notification","task_id":"agent-1","tool_use_id":"send_call",
                            "status":"completed","summary":"Continued"}));
                        finish(session, "Parent continued", false);
                    }
                    "subagent-background" => {
                        emit(json!({"type":"system","subtype":"session_state_changed","state":"running"}));
                        emit(json!({"type":"assistant","session_id":session,"uuid":uuid::Uuid::new_v4().to_string(),"parent_tool_use_id":null,
                            "message":{"id":"msg_bg","role":"assistant","content":[{"type":"tool_use","id":"agent_bg","name":"Agent",
                                "input":{"description":"Research","prompt":"Dig in","run_in_background":true}}]}}));
                        emit(json!({"type":"system","subtype":"task_started","task_id":"agent-2","tool_use_id":"agent_bg",
                            "task_type":"local_agent","is_backgrounded":true,"description":"Research"}));
                        emit(json!({"type":"user","session_id":session,"parent_tool_use_id":null,
                            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"agent_bg","content":"Async agent launched successfully"}]},
                            "tool_use_result":{"isAsync":true,"status":"async_launched","agentId":"agent-2"}}));
                        finish(session, "Parent waiting", false);
                        emit(json!({"type":"assistant","session_id":session,"uuid":uuid::Uuid::new_v4().to_string(),
                            "parent_tool_use_id":"agent_bg","agent_id":"agent-2",
                            "message":{"id":"msg_bg_child","role":"assistant","content":[{"type":"text","text":"Background report"}]}}));
                        emit(json!({"type":"system","subtype":"task_updated","task_id":"agent-2","patch":{"status":"completed"}}));
                        emit(json!({"type":"system","subtype":"task_notification","task_id":"agent-2","tool_use_id":"agent_bg",
                            "status":"completed","summary":"Background report"}));
                        finish(session, "Parent saw report", false);
                        emit(json!({"type":"system","subtype":"session_state_changed","state":"idle"}));
                    }
                    // Claude Code 2.1's frames for a manual compaction. The pause
                    // keeps the compaction running while a test acts on it.
                    "/compact" => {
                        emit(json!({"type":"system","subtype":"status","status":"compacting","session_id":session}));
                        std::thread::sleep(std::time::Duration::from_secs(1));
                        emit(json!({"type":"system","subtype":"status","status":null,"session_id":session}));
                        emit(json!({"type":"system","subtype":"compact_boundary","session_id":session,
                            "compact_metadata":{"trigger":"manual","pre_tokens":50000,"post_tokens":7500}}));
                        emit(json!({"type":"user","uuid":uuid::Uuid::new_v4().to_string(),"isSynthetic":true,"session_id":session,"parent_tool_use_id":null,
                            "message":{"role":"user","content":"This session is being continued from a previous conversation."}}));
                        emit(json!({"type":"result","subtype":"success","is_error":false,"result":"","session_id":session,"duration_ms":1,"duration_api_ms":0,
                            "num_turns":0,"total_cost_usd":0.0,"usage":{"input_tokens":0,"output_tokens":0},"modelUsage":{},"permission_denials":[]}));
                    }
                    "crash" => {
                        eprintln!("fake backend crashed deliberately");
                        std::process::exit(17);
                    }
                    "malformed" => {
                        println!("{{not valid json");
                        io::stdout().flush().unwrap();
                    }
                    "tool" => {
                        emit(
                            json!({"type":"assistant","session_id":session,"message":{"id":"msg_tool","role":"assistant","content":[{"type":"tool_use","id":"tool_fake","name":"Bash","input":{"command":"printf fake"}}],"usage":{"input_tokens":2,"output_tokens":1}}}),
                        );
                        emit(
                            json!({"type":"control_request","request_id":"permission_fake","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"printf fake"},"tool_use_id":"tool_fake","permission_suggestions":[]}}),
                        );
                    }
                    _ => finish(session, &format!("Echo: {text}"), false),
                }
            }
            Some("control_response") if value["response"]["request_id"] == "question_fake" => {
                let data = &value["response"]["response"];
                finish(session, &data.to_string(), data["behavior"] != "allow");
            }
            Some("control_response") if value["response"]["request_id"] == "desktop_call" => {
                let response = &value["response"]["response"]["mcp_response"];
                assert_eq!(response["id"], 73);
                finish(session, &response.to_string(), false);
            }
            Some("control_response") if value["response"]["request_id"] == "delegation_call" => {
                let result = value["response"]["response"]["mcp_response"]["result"].clone();
                if let Some(text) = result["content"][0]["text"].as_str()
                    && let Ok(parsed) = serde_json::from_str::<Value>(text)
                    && let Some(id) = parsed["agent_id"].as_str()
                {
                    delegation_agent = id.into();
                }
                delegation_results.push(result);
                if let Some(action) = delegation_actions.pop_front() {
                    delegation_action(&action, &delegation_agent);
                } else {
                    finish(session, &json!(delegation_results).to_string(), false);
                }
            }
            Some("control_response") if value["response"]["request_id"] == "permission_fake" => {
                let allowed = value["response"]["response"]["behavior"] == "allow";
                let text = if allowed {
                    "fake tool output"
                } else {
                    "Tool denied"
                };
                emit(
                    json!({"type":"user","session_id":session,"message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool_fake","content":text,"is_error":!allowed}]}}),
                );
                if background {
                    background = false;
                    emit(
                        json!({"type":"system","subtype":"task_notification","task_id":"bg","status":"completed","summary":"Background fixture finished"}),
                    );
                    finish(session, "Background complete", false);
                    emit(json!({"type":"system","subtype":"session_state_changed","state":"idle"}));
                } else {
                    finish(session, text, false);
                }
            }
            _ => {}
        }
    }
    // A process that takes a while to persist its transcript after EOF.
    if args.iter().any(|arg| arg == "--fake-slow-exit") {
        std::thread::sleep(std::time::Duration::from_secs(3));
    }
}
