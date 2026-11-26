//! Live SSH recovery of a pending native Claude desktop-tool call.
//! Usage: reconnect_smoke [SSH alias] [model]. Creates and archives its own chat.
use anyhow::{Context, Result, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{process::Stdio, time::Duration};
use tokio::{
    io::Join,
    process::{Child, ChildStdin, ChildStdout, Command},
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

type Socket = WebSocketStream<Join<ChildStdout, ChildStdin>>;
struct Peer {
    proxy: Child,
    socket: Socket,
    serial: u64,
    backlog: Vec<Value>,
}
impl Peer {
    async fn connect(alias: &str) -> Result<Self> {
        let mut proxy = Command::new("ssh")
            .args([
                "-T",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=5",
                alias,
                "exec \"$CODEX_INSTALL_DIR/codex\" app-server proxy",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let io = tokio::io::join(
            proxy.stdout.take().context("stdout")?,
            proxy.stdin.take().context("stdin")?,
        );
        let (socket, _) = tokio::time::timeout(
            Duration::from_secs(15),
            tokio_tungstenite::client_async("ws://codex-app-server/rpc", io),
        )
        .await??;
        let mut peer = Self {
            proxy,
            socket,
            serial: 0,
            backlog: vec![],
        };
        peer.rpc("initialize",json!({"clientInfo":{"name":"ssh-reconnect-proof","version":"1"},"capabilities":{"experimentalApi":true}})).await?;
        peer.send(json!({"method":"initialized"})).await?;
        Ok(peer)
    }
    async fn send(&mut self, value: Value) -> Result<()> {
        self.socket
            .send(Message::Text(value.to_string().into()))
            .await?;
        Ok(())
    }
    async fn until(&mut self, matches: impl Fn(&Value) -> bool) -> Result<Value> {
        if let Some(index) = self.backlog.iter().position(&matches) {
            return Ok(self.backlog.remove(index));
        }
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(180), self.socket.next())
                .await?
                .context("SSH proxy closed")??;
            if let Message::Text(text) = frame {
                let value: Value = serde_json::from_str(&text)?;
                if matches(&value) {
                    return Ok(value);
                }
                self.backlog.push(value);
            }
        }
    }
    async fn rpc(&mut self, method: &str, params: Value) -> Result<Value> {
        self.serial += 1;
        let id = self.serial;
        self.send(json!({"id":id,"method":method,"params":params}))
            .await?;
        let value = self
            .until(|v| v["id"] == id && v.get("method").is_none())
            .await?;
        ensure!(value.get("error").is_none(), "{method}: {}", value["error"]);
        Ok(value["result"].clone())
    }
    async fn disconnect(mut self) -> Result<()> {
        self.proxy.kill().await?;
        self.proxy.wait().await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let alias = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "claude-codex-local".into());
    let model = std::env::args().nth(2).unwrap_or_else(|| "opus".into());
    let workspace = tempfile::tempdir()?;
    let token = format!("RECONNECTED_{}", uuid::Uuid::new_v4());
    let mut first = Peer::connect(&alias).await?;
    let started=first.rpc("thread/start",json!({"cwd":workspace.path(),"model":model,"sandbox":"danger-full-access","approvalPolicy":"on-request",
        "historyMode":"paginated","dynamicTools":[{"type":"function","name":"reconnect_probe","description":"Returns the verification token","inputSchema":{"type":"object","properties":{},"additionalProperties":false}}]})).await?;
    let thread = started["thread"]["id"].clone();
    let turn=first.rpc("turn/start",json!({"threadId":thread,"effort":"low","input":[{"type":"text","text":"Call the reconnect_probe MCP tool exactly once. Reply only with its returned verification token. Do not use other tools."}]})).await?;
    let pending = first.until(|v| v["method"] == "item/tool/call").await?;
    ensure!(
        pending["params"]["tool"] == "reconnect_probe",
        "wrong tool: {pending}"
    );
    first.disconnect().await?;
    let mut recovered = Peer::connect(&alias).await?;
    let resumed = recovered
        .rpc(
            "thread/resume",
            json!({"threadId":thread,"excludeTurns":true}),
        )
        .await?;
    ensure!(
        resumed["thread"]["status"]["type"] == "active",
        "native turn did not survive: {resumed}"
    );
    let replay = recovered.until(|v| v["method"] == "item/tool/call").await?;
    ensure!(replay == pending, "replay changed request identity");
    recovered.send(json!({"id":replay["id"],"result":{"success":true,"contentItems":[{"type":"inputText","text":token}]}})).await?;
    let completed = recovered
        .until(|v| {
            v["method"] == "turn/completed" && v["params"]["turn"]["id"] == turn["turn"]["id"]
        })
        .await?;
    let final_turn = &completed["params"]["turn"];
    ensure!(
        final_turn["status"] == "completed",
        "native failure: {completed}"
    );
    let items = final_turn["items"].as_array().context("turn items")?;
    ensure!(
        items.iter().any(|item| item["type"] == "agentMessage"
            && item["text"]
                .as_str()
                .is_some_and(|text| text.contains(&token))),
        "native model did not receive recovered tool result"
    );
    ensure!(
        items
            .iter()
            .filter(|item| item["type"] == "dynamicToolCall" && item["tool"] == "reconnect_probe")
            .count()
            == 1,
        "tool ran more than once"
    );
    recovered
        .rpc("thread/archive", json!({"threadId":thread}))
        .await?;
    recovered.disconnect().await?;
    println!(
        "{alias}: native turn survived SSH replacement; exact request/call IDs replayed; one tool result reached Claude; probe archived"
    );
    Ok(())
}
