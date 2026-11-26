//! Probe the ordinary desktop SSH login-shell path after `claudex install`.
//! Usage: remote_router_smoke SSH_ALIAS [MODEL]. Makes one model request and
//! archives its own chat; it does not restart the desktop or other conversations.
use anyhow::{Context, Result, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{process::Stdio, time::Duration};
use tokio::{
    io::Join,
    process::{ChildStdin, ChildStdout, Command},
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

type Socket = WebSocketStream<Join<ChildStdout, ChildStdin>>;
const DEADLINE: Duration = Duration::from_secs(60);

async fn next(socket: &mut Socket) -> Result<Value> {
    loop {
        let frame = tokio::time::timeout(DEADLINE, socket.next())
            .await?
            .context("SSH connection closed")??;
        if let Message::Text(text) = frame {
            return Ok(serde_json::from_str(&text)?);
        }
    }
}

async fn rpc(socket: &mut Socket, id: u64, method: &str, params: Value) -> Result<Value> {
    socket
        .send(Message::Text(
            json!({"id":id,"method":method,"params":params})
                .to_string()
                .into(),
        ))
        .await?;
    loop {
        let value = next(socket).await?;
        if value["id"] == id && value.get("method").is_none() {
            ensure!(value.get("error").is_none(), "{method}: {}", value["error"]);
            return Ok(value["result"].clone());
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let alias = std::env::args()
        .nth(1)
        .context("Usage: remote_router_smoke SSH_ALIAS [MODEL]")?;
    let model = std::env::args().nth(2).unwrap_or_else(|| "opus".into());
    // The desktop exports its payload before starting an interactive login
    // shell, then selects codex through CODEX_INSTALL_DIR. Do the same here.
    let command = r#"sh -c 'CODEX_REMOTE_PAYLOAD="$1"; export CODEX_REMOTE_PAYLOAD; exec "$SHELL" -l -i -c '\''CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"; export CODEX_HOME; exec /bin/sh -c "$CODEX_REMOTE_PAYLOAD"'\''' sh 'PATH="${CODEX_INSTALL_DIR:-$HOME/.local/bin}:$PATH"; export PATH; exec codex app-server proxy'"#;
    let mut child = Command::new("ssh")
        .args([
            "-T",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            &alias,
            command,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let stream = tokio::io::join(
        child.stdout.take().context("stdout")?,
        child.stdin.take().context("stdin")?,
    );
    let (mut socket, _) = tokio::time::timeout(
        DEADLINE,
        tokio_tungstenite::client_async("ws://codex-app-server/rpc", stream),
    )
    .await??;
    let init = rpc(&mut socket, 1, "initialize", json!({"clientInfo":{"name":"remote-router-smoke","version":"1"},"capabilities":{"experimentalApi":true}})).await?;
    println!("SSH WebSocket connected: {}", init["userAgent"]);
    socket
        .send(Message::Text(
            json!({"method":"initialized"}).to_string().into(),
        ))
        .await?;
    let catalog = rpc(
        &mut socket,
        2,
        "model/list",
        json!({"includeHidden":true,"limit":100}),
    )
    .await?;
    let models = catalog["data"].as_array().context("model catalog")?;
    ensure!(
        models
            .iter()
            .any(|m| m["id"].as_str().is_some_and(|id| id.starts_with("gpt-"))),
        "GPT models missing"
    );
    for name in ["opus", "fable", "sonnet", "haiku"] {
        ensure!(
            models
                .iter()
                .any(|m| m["id"] == name && m["hidden"] == false),
            "Visible {name} missing"
        );
    }
    println!("Mixed GPT/Claude catalog: OK");
    let thread = rpc(&mut socket, 3, "thread/start", json!({"cwd":"/tmp","model":model,"sandbox":"workspace-write","approvalPolicy":"on-request"})).await?;
    let id = thread["thread"]["id"].clone();
    let token = format!("REMOTE_ROUTER_{}", uuid::Uuid::new_v4());
    let result: Result<()> = async {
        rpc(&mut socket, 4, "turn/start", json!({"threadId":id,"input":[{"type":"text","text":format!("Reply exactly {token}. Do not use tools."),"text_elements":[]}]})).await?;
        let mut text = String::new();
        loop {
            let value = next(&mut socket).await?;
            if value["method"] == "item/agentMessage/delta" {
                text.push_str(value["params"]["delta"].as_str().unwrap_or_default());
            }
            if value["method"] == "turn/completed" {
                ensure!(value["params"]["turn"]["status"] == "completed", "{value}");
                ensure!(text.contains(&token), "Expected token missing: {text}");
                println!("Live routed {model} response: OK");
                break;
            }
        }
        Ok(())
    }.await;
    rpc(&mut socket, 5, "thread/archive", json!({"threadId":id})).await?;
    println!("Verification chat archived");
    result?;
    socket.close(None).await?;
    Ok(())
}
