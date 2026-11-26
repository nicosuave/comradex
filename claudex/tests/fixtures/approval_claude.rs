//! Native-process fixture that reports exactly what the facade authorized.
use serde_json::{Value, json};
use std::io::{self, BufRead, Write};

fn emit(value: Value) {
    let mut out = io::stdout().lock();
    writeln!(out, "{value}").unwrap();
    out.flush().unwrap();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).is_some_and(|arg| arg == "--mcp-fixture") {
        mcp_fixture(&args[2]);
        return;
    }
    let session = args
        .iter()
        .find_map(|a| a.strip_prefix("--session-id="))
        .or_else(|| args.iter().find_map(|a| a.strip_prefix("--resume=")))
        .unwrap_or("approval-fixture");
    // The "subagent" prompt routes the approved tool call through a child agent.
    let mut subagent = false;
    for line in io::stdin().lock().lines() {
        let value: Value = serde_json::from_str(&line.unwrap()).unwrap();
        match value["type"].as_str() {
            Some("control_request") if value["request"]["subtype"] == "initialize" => {
                emit(
                    json!({"type":"control_response","response":{"subtype":"success","request_id":value["request_id"],"response":{"commands":[],"models":[{"value":"fake-claude","displayName":"Fixture","description":"Fixture"}]}}}),
                );
                emit(
                    json!({"type":"system","subtype":"init","session_id":session,"cwd":std::env::current_dir().unwrap(),"tools":["Bash","AskUserQuestion"],"model":"fake-claude"}),
                );
            }
            Some("user") => {
                let content = &value["message"]["content"];
                let text = content.as_str().map(str::to_owned).unwrap_or_else(|| {
                    content
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter_map(|p| p["text"].as_str())
                        .collect::<String>()
                });
                emit(
                    json!({"type":"user","uuid":value["uuid"],"isReplay":true,"message":value["message"],"session_id":session,"parent_tool_use_id":null}),
                );
                let (tool, input) = if text == "questions" {
                    (
                        "AskUserQuestion",
                        json!({"questions":[
                            {"question":"Which features?","header":"Features","multiSelect":true,"options":[{"label":"Search","description":"Find"},{"label":"Export","description":"Save"}]},
                            {"question":"Which theme?","header":"Theme","multiSelect":false,"options":[{"label":"Light","description":"Bright"},{"label":"Dark","description":"Dim"}]}
                        ]}),
                    )
                } else if text == "mcp-mutation" {
                    (
                        "mcp__fixture__save_note",
                        json!({"note":"synthetic fixture only"}),
                    )
                } else if text == "subagent" {
                    ("Bash", json!({"command":"echo child"}))
                } else if let Some(path) = text.strip_prefix("read:") {
                    ("Read", json!({"file_path":path}))
                } else {
                    ("Bash", json!({"command":text}))
                };
                subagent = text == "subagent";
                if subagent {
                    emit(
                        json!({"type":"assistant","session_id":session,"parent_tool_use_id":null,"message":{"id":"agent-message","role":"assistant","content":[{"type":"tool_use","id":"agent-tool","name":"Agent","input":{"description":"Child","prompt":"Run it"}}]}}),
                    );
                    emit(
                        json!({"type":"system","subtype":"task_started","task_id":"child-agent","tool_use_id":"agent-tool","task_type":"local_agent","is_backgrounded":false}),
                    );
                }
                emit(
                    json!({"type":"assistant","session_id":session,"parent_tool_use_id":if subagent { json!("agent-tool") } else { Value::Null },"agent_id":if subagent { json!("child-agent") } else { Value::Null },"message":{"id":"permission-message","role":"assistant","content":[{"type":"tool_use","id":"permission-tool","name":tool,"input":input}]}}),
                );
                if text == "mcp-mutation" && args.iter().any(|a| a == "--permission-mode=auto") {
                    emit(
                        json!({"type":"system","subtype":"permission_denied","tool_name":tool,"tool_use_id":"permission-tool","decision_reason_type":"classifier","decision_reason":"Shared-resource write","message":"Permission denied by native auto classifier"}),
                    );
                    emit(
                        json!({"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"permission-tool","content":"Permission denied","is_error":true}]}}),
                    );
                    emit(
                        json!({"type":"result","subtype":"success","is_error":false,"result":"{\"behavior\":\"deny\"}","session_id":session,"usage":{"input_tokens":1,"output_tokens":1}}),
                    );
                    continue;
                }
                emit(
                    json!({"type":"control_request","request_id":"native-permission","request":{"subtype":"can_use_tool","tool_name":tool,"input":input,"tool_use_id":"permission-tool","agent_id":if subagent { json!("child-agent") } else { Value::Null },"permission_suggestions":[{"type":"setMode","mode":"acceptEdits","destination":"session"}]}}),
                );
            }
            Some("control_response") if value["response"]["request_id"] == "native-permission" => {
                let data = &value["response"]["response"];
                let text = data.to_string();
                emit(
                    json!({"type":"user","session_id":session,"parent_tool_use_id":if subagent { json!("agent-tool") } else { Value::Null },"agent_id":if subagent { json!("child-agent") } else { Value::Null },"message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"permission-tool","content":text,"is_error":data["behavior"]!="allow"}]}}),
                );
                if subagent {
                    emit(
                        json!({"type":"user","session_id":session,"parent_tool_use_id":null,"message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"agent-tool","content":"child done"}]}}),
                    );
                    emit(
                        json!({"type":"system","subtype":"task_notification","task_id":"child-agent","tool_use_id":"agent-tool","status":"completed","summary":"child done"}),
                    );
                }
                emit(
                    json!({"type":"assistant","session_id":session,"uuid":uuid::Uuid::new_v4().to_string(),"message":{"id":uuid::Uuid::new_v4().to_string(),"role":"assistant","model":"fake-claude","content":[{"type":"text","text":text}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}}),
                );
                emit(
                    json!({"type":"result","subtype":"success","is_error":false,"result":text,"session_id":session,"duration_ms":1,"num_turns":1,"usage":{"input_tokens":1,"output_tokens":1}}),
                );
            }
            _ => {}
        }
    }
}

/// A harmless local MCP server for opt-in real-Claude permission tests.
fn mcp_fixture(output: &str) {
    for line in io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let Some(id) = request.get("id") else {
            continue;
        };
        let result = match request["method"].as_str() {
            Some("initialize") => {
                json!({"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"approval-fixture","version":"1"}})
            }
            Some("tools/list") => {
                json!({"tools":[{"name":"save_note","description":"Save a synthetic integration-test note to the fixture's private temporary file.","inputSchema":{"type":"object","properties":{"note":{"type":"string"}},"required":["note"],"additionalProperties":false},"annotations":{"readOnlyHint":false,"destructiveHint":false,"openWorldHint":false}}]})
            }
            Some("tools/call") if request["params"]["name"] == "save_note" => {
                let note = request["params"]["arguments"]["note"].as_str().unwrap();
                std::fs::write(output, note).unwrap();
                json!({"content":[{"type":"text","text":note}]})
            }
            _ => json!({}),
        };
        emit(json!({"jsonrpc":"2.0","id":id,"result":result}));
    }
}
