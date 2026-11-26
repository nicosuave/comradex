//! Verify an installed SSH connection using the desktop's WebSocket proxy.
//! Usage: desktop_smoke [SSH alias] [model] (defaults: claude-codex-local opus).
use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{process::Stdio, time::Duration};
use tokio::process::Command;
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() -> Result<()> {
    let alias = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "claude-codex-local".into());
    let workspace = tempfile::tempdir()?;
    let cwd = workspace.path().join("desktop-task");
    let model = std::env::args().nth(2).unwrap_or_else(|| "opus".into());
    let tool_token = format!("DESKTOP_TOOL_{}", uuid::Uuid::new_v4());
    let mut tool_called = false;
    let mut completed_turns = 0;
    let mut saved_path = Value::Null;
    let mut proxy = Command::new("ssh")
        .args([
            "-T",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            &alias,
            "exec \"$CODEX_INSTALL_DIR/codex\" app-server proxy",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let stream = tokio::io::join(
        proxy.stdout.take().context("stdout")?,
        proxy.stdin.take().context("stdin")?,
    );
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(15),
        tokio_tungstenite::client_async("ws://codex-app-server/rpc", stream),
    )
    .await??;
    ws.send(Message::Text(
        json!({"id":1,"method":"initialize","params":{
        "clientInfo":{"name":"desktop-ssh-smoke","version":"1"},
        "capabilities":{"experimentalApi":true}}})
        .to_string()
        .into(),
    ))
    .await?;
    let mut thread = None;
    let mut browse_root = None;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(180), ws.next())
            .await?
            .context("proxy disconnected")??;
        let Message::Text(text) = frame else { continue };
        let value: Value = serde_json::from_str(&text)?;
        if let Some(error) = value.get("error") {
            bail!("RPC failed: {error}");
        }
        let request = match value["id"].as_u64() {
            Some(1) => {
                println!("Connected: {}", value["result"]["userAgent"]);
                ws.send(Message::Text(
                    json!({"method":"initialized"}).to_string().into(),
                ))
                .await?;
                let home = std::path::PathBuf::from(
                    value["result"]["codexHome"].as_str().context("codexHome")?,
                );
                browse_root = Some(home.parent().context("home parent")?.to_owned());
                Some(json!({"id":13,"method":"fs/getMetadata","params":{"path":browse_root}}))
            }
            Some(13) => {
                ensure!(
                    value["result"]["isDirectory"] == true,
                    "picker root is not a directory"
                );
                Some(json!({"id":14,"method":"fs/readDirectory","params":{"path":browse_root}}))
            }
            Some(14) => {
                ensure!(
                    value["result"]["entries"].is_array(),
                    "missing directory entries"
                );
                println!("Folder picker: metadata and directory listing passed");
                Some(json!({"id":10,"method":"config/read","params":{"includeLayers":false}}))
            }
            Some(10) => {
                let config = &value["result"]["config"];
                let provider = config["model_provider"]
                    .as_str()
                    .context("model provider")?;
                ensure!(
                    config["model_providers"][provider].is_object(),
                    "desktop custom provider is missing"
                );
                Some(
                    json!({"id":11,"method":"model/list","params":{"includeHidden":true,"limit":100}}),
                )
            }
            Some(11) => {
                let models = value["result"]["data"]
                    .as_array()
                    .context("model catalog")?;
                for alias in ["opus", "fable", "sonnet", "haiku"] {
                    let entry = models
                        .iter()
                        .find(|m| m["model"] == alias && m["hidden"] == false)
                        .with_context(|| format!("missing visible {alias}"))?;
                    let efforts = entry["supportedReasoningEfforts"]
                        .as_array()
                        .context("effort list")?;
                    for level in ["low", "medium", "high", "xhigh", "max"] {
                        ensure!(
                            efforts.iter().any(|e| e["reasoningEffort"] == level),
                            "missing {alias} effort {level}"
                        );
                    }
                    println!(
                        "Catalog: {} ({alias}), low/medium/high/xhigh/max",
                        entry["displayName"]
                    );
                }
                Some(
                    json!({"id":12,"method":"fs/createDirectory","params":{"path":cwd,"recursive":true}}),
                )
            }
            Some(12) => {
                ensure!(cwd.is_dir(), "workspace was not created");
                Some(json!({"id":2,"method":"thread/start","params":{
                    "cwd":cwd,"threadSource":"user","model":model,
                    "approvalPolicy":"on-request","approvalsReviewer":"user","sandbox":"danger-full-access",
                    "runtimeWorkspaceRoots":[cwd],"experimentalRawEvents":false,"personality":"pragmatic",
                    "dynamicTools":[{"type":"namespace","name":"desktop","description":"Smoke test tools","tools":[
                        {"type":"function","name":"probe","description":"Return the verification token", "inputSchema":{"type":"object","properties":{},"additionalProperties":false}}
                    ]}],
                    "historyMode":"paginated","config":{"features.request_permissions_tool":true,
                    "model_reasoning_effort":"max"}}}))
            }
            Some(2) => {
                thread = Some(value["result"]["thread"]["id"].clone());
                saved_path = value["result"]["thread"]["path"].clone();
                let fixture: Value =
                    serde_json::from_str(include_str!("../tests/fixtures/desktop-turn.json"))?;
                let mut params = fixture["request"].clone();
                params["threadId"] = json!(thread);
                params["cwd"] = json!(cwd);
                params["runtimeWorkspaceRoots"] = json!([cwd]);
                params["model"] = json!(model);
                params["collaborationMode"]["settings"]["model"] = json!(model);
                params["input"] = json!([{"type":"text","text":"Call the desktop probe MCP tool exactly once, then reply with only the exact token it returns. Do not use any other tools.","text_elements":[]}]);
                Some(json!({"id":3,"method":"turn/start","params":params}))
            }
            Some(20) => {
                ensure!(
                    value["result"]["turnsBackwardsCursor"].is_string(),
                    "resume did not provide history cursor"
                );
                Some(
                    json!({"id":21,"method":"thread/timeline/list","params":{"threadId":thread,"limit":500}}),
                )
            }
            Some(21) => {
                ensure!(
                    value["result"]["data"]
                        .as_array()
                        .is_some_and(|a| a.iter().any(|v| v["type"] == "turnCompleted")),
                    "timeline missing completed turn"
                );
                Some(
                    json!({"id":22,"method":"command/exec","params":{"command":["/bin/sh","-c","printf desktop-command"],"cwd":cwd}}),
                )
            }
            Some(22) => {
                ensure!(
                    value["result"]["stdout"] == "desktop-command",
                    "host command failed"
                );
                Some(
                    json!({"id":23,"method":"thread/queue/add","params":{"threadId":thread,
                    "clientUserMessageId":"desktop-probe-followup","input":[{"type":"text","text":"Reply with only the exact verification token from your previous answer. Do not call any tools.","text_elements":[]}]}}),
                )
            }
            Some(23) => Some(
                json!({"id":24,"method":"thread/queue/start","params":{"threadId":thread,"queuedSubmissionId":value["result"]["queuedSubmission"]["id"]}}),
            ),
            Some(4) => break,
            _ => None,
        };
        if let Some(request) = request {
            ws.send(Message::Text(request.to_string().into())).await?;
        }
        // Only the in-memory probe is authorized; it has no external effects.
        if value.get("id").is_some() && value.get("method").is_some() {
            let result = if value["method"] == "item/tool/call"
                && value["params"]["namespace"] == "desktop"
                && value["params"]["tool"] == "probe"
                && value["params"]["arguments"] == json!({})
            {
                tool_called = true;
                json!({"id":value["id"],"result":{"success":true,"contentItems":[{"type":"inputText","text":tool_token}]}})
            } else {
                json!({"id":value["id"],"error":{"code":-32601,"message":"Only desktop probe is authorized"}})
            };
            ws.send(Message::Text(result.to_string().into())).await?;
        }
        if value["method"] == "turn/completed" {
            ensure!(tool_called, "Claude did not call the desktop tool");
            let turn = &value["params"]["turn"];
            ensure!(
                turn["status"] == "completed",
                "turn failed: {}",
                turn["error"]
            );
            ensure!(
                turn["items"]
                    .as_array()
                    .context("items")?
                    .iter()
                    .any(|item| item["type"] == "agentMessage"
                        && item["text"]
                            .as_str()
                            .is_some_and(|s| s.trim() == tool_token)),
                "unexpected response"
            );
            completed_turns += 1;
            let request = if completed_turns == 1 {
                json!({"id":20,"method":"thread/resume","params":{"threadId":thread,"path":saved_path,"cwd":cwd,"modelProvider":"anthropic","excludeTurns":true}})
            } else {
                json!({"id":4,"method":"thread/archive","params":{"threadId":thread}})
            };
            ws.send(Message::Text(request.to_string().into())).await?;
        }
    }
    ws.close(None).await?;
    drop(ws);
    ensure!(
        tokio::time::timeout(Duration::from_secs(10), proxy.wait())
            .await??
            .success(),
        "SSH proxy failed"
    );
    println!(
        "PASS: SSH proxy, native catalog, folder picker, desktop request options, dynamic tool roundtrip, live {model} max-effort turn, path resume, timeline, host command, queued context-preserving followup; probe thread archived"
    );
    Ok(())
}
