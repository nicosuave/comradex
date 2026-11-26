//! Exercise the installed desktop's actual remote file hydration protocol.
//! Usage: visualization_smoke <SSH alias> <absolute HTML path>
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::process::Command;
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() -> Result<()> {
    let alias = std::env::args().nth(1).context("SSH alias required")?;
    let file = std::env::args().nth(2).context("HTML path required")?;
    ensure!(Path::new(&file).is_absolute(), "absolute path required");
    let directory = Path::new(&file).parent().context("parent")?;
    let mut proxy = Command::new("ssh")
        .args([
            "-T",
            "-o",
            "BatchMode=yes",
            &alias,
            "exec \"$CODEX_INSTALL_DIR/codex\" app-server proxy",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let stream = tokio::io::join(proxy.stdout.take().unwrap(), proxy.stdin.take().unwrap());
    let (mut ws, _) = tokio_tungstenite::client_async("ws://codex-app-server/rpc", stream).await?;
    ws.send(Message::Text(json!({"id":1,"method":"initialize","params":{"clientInfo":{"name":"visualization-hydration-proof","version":"1"},"capabilities":{"experimentalApi":true}}}).to_string().into())).await?;
    let mut bytes = Vec::new();
    let mut ack = false;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(30), ws.next())
            .await?
            .context("disconnected")??;
        let Message::Text(text) = frame else { continue };
        let message: Value = serde_json::from_str(&text)?;
        ensure!(message.get("error").is_none(), "{message}");
        let next = if message["id"] == 1 {
            Some(
                json!({"id":2,"method":"process/spawn","params":{"processHandle":"realpath","command":["/bin/pwd","-P"],"cwd":directory,"streamStdin":true,"streamStdoutStderr":true,"timeoutMs":5000,"outputBytesCap":16384}}),
            )
        } else if message["id"] == 2 || message["id"] == 3 {
            ensure!(message["result"] == json!({}), "spawn acknowledgement");
            ack = true;
            None
        } else if message["method"] == "process/outputDelta" {
            ensure!(ack, "output preceded spawn acknowledgement");
            if message["params"]["stream"] == "stdout" {
                bytes.extend(
                    STANDARD.decode(message["params"]["deltaBase64"].as_str().context("bytes")?)?,
                );
            }
            None
        } else if message["method"] == "process/exited" {
            ensure!(
                message["params"]["exitCode"] == 0,
                "host process failed: {message}"
            );
            if message["params"]["processHandle"] == "realpath" {
                ensure!(
                    String::from_utf8(bytes.clone())?.trim() == directory.to_str().unwrap(),
                    "canonical path differs"
                );
                bytes.clear();
                ack = false;
                Some(
                    json!({"id":3,"method":"process/spawn","params":{"processHandle":"read","command":["/bin/cat",file],"cwd":"/","streamStdin":true,"streamStdoutStderr":true,"timeoutMs":null,"outputBytesCap":null}}),
                )
            } else {
                ensure!(!bytes.is_empty(), "empty visualization");
                ensure!(
                    String::from_utf8(bytes.clone())?.contains("<div"),
                    "expected HTML fragment"
                );
                println!(
                    "PASS: installed SSH process/spawn canonical path and uncapped file hydration ({} bytes)",
                    bytes.len()
                );
                break;
            }
        } else {
            None
        };
        if let Some(next) = next {
            ws.send(Message::Text(next.to_string().into())).await?;
        }
    }
    ws.close(None).await?;
    proxy.kill().await.ok();
    Ok(())
}
