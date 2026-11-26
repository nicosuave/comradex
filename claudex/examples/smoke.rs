//! Live inference, process-restart recall, and fork-snapshot smoke test.
//! Run after building the server: cargo run --example smoke -- /path/to/server
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
};

struct Client {
    child: Child,
    input: Option<ChildStdin>,
    output: Lines<BufReader<ChildStdout>>,
    serial: u64,
}

impl Client {
    async fn start(binary: &Path, state: &Path, cwd: &Path) -> Result<Self> {
        let mut child = Command::new(binary)
            .arg("--state-dir")
            .arg(state)
            .arg("--claude-arg=--safe-mode")
            // Read-only Bash commands may otherwise be auto-allowed. Request an
            // explicit prompt even for the harmless printf used by this probe.
            .arg("--claude-arg=--settings={\"permissions\":{\"ask\":[\"Bash\"]}}")
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let input = child.stdin.take();
        let output = BufReader::new(child.stdout.take().context("missing stdout")?).lines();
        let mut client = Self {
            child,
            input,
            output,
            serial: 0,
        };
        client.request("initialize", json!({"clientInfo":{"name":"live-smoke","version":"1"},"capabilities":{"experimentalApi":true}})).await?;
        client.send(json!({"method":"initialized"})).await?;
        Ok(client)
    }

    async fn send(&mut self, value: Value) -> Result<()> {
        let input = self.input.as_mut().context("client closed")?;
        input.write_all(format!("{value}\n").as_bytes()).await?;
        input.flush().await?;
        Ok(())
    }

    async fn read(&mut self) -> Result<Value> {
        let line = tokio::time::timeout(Duration::from_secs(180), self.output.next_line())
            .await
            .context("facade response timed out")??
            .context("facade closed stdout")?;
        Ok(serde_json::from_str(&line)?)
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        self.serial += 1;
        let id = self.serial;
        self.send(json!({"id":id,"method":method,"params":params}))
            .await?;
        loop {
            let response = self.read().await?;
            if response["id"] == id && response.get("method").is_none() {
                if let Some(error) = response.get("error") {
                    bail!("{method}: {error}");
                }
                return Ok(response["result"].clone());
            }
        }
    }

    async fn turn(
        &mut self,
        thread: &str,
        prompt: &str,
        exercise_approval: bool,
    ) -> Result<String> {
        let started = self.request("turn/start", json!({"threadId":thread,"input":[{"type":"text","text":prompt,"text_elements":[]}]})).await?;
        let mut approved = false;
        loop {
            let message = self.read().await?;
            if message.get("id").is_some() && message.get("method").is_some() {
                // This example approves only this exact, side-effect-free command.
                let allow = exercise_approval
                    && message["method"] == "item/commandExecution/requestApproval"
                    && message["params"]["command"] == "printf FACADE_TOOL_OK";
                approved |= allow;
                self.send(json!({"id":message["id"],"result":{"decision":if allow {"accept"} else {"decline"}}})).await?;
            }
            if message["method"] == "turn/completed"
                && message["params"]["turn"]["id"] == started["turn"]["id"]
            {
                let turn = &message["params"]["turn"];
                ensure!(
                    turn["status"] == "completed",
                    "Claude turn failed: {}",
                    turn["error"]
                );
                let items = turn["items"].as_array().context("missing items")?;
                if exercise_approval {
                    ensure!(
                        approved,
                        "Claude did not request the expected command approval"
                    );
                    ensure!(
                        items.iter().any(|item| item["type"] == "commandExecution"
                            && item["status"] == "completed"
                            && item["aggregatedOutput"]
                                .as_str()
                                .is_some_and(|s| s.contains("FACADE_TOOL_OK"))),
                        "approved tool output was not observed"
                    );
                }
                return Ok(items
                    .iter()
                    .filter(|item| item["type"] == "agentMessage")
                    .filter_map(|item| item["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n"));
            }
        }
    }

    async fn close(mut self) -> Result<()> {
        self.input.take();
        let status = tokio::time::timeout(Duration::from_secs(15), self.child.wait()).await??;
        ensure!(status.success(), "facade exit: {status}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let binary = std::env::args_os()
        .nth(1)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| "target/debug/claude-codex-server".into())
        .canonicalize()
        .context("build claude-codex-server first, or pass its binary path")?;
    let exercise_approval = std::env::args().any(|arg| arg == "--exercise-approval");
    let temp = tempfile::tempdir()?;
    let state = temp.path().join("state");
    let cwd = temp.path().join("workspace");
    std::fs::create_dir(&cwd)?;
    let token = uuid::Uuid::new_v4().to_string();
    let mut client = Client::start(&binary, &state, &cwd).await?;
    let thread = client
        .request("thread/start", json!({"sandbox":"danger-full-access"}))
        .await?["thread"]["id"]
        .as_str()
        .context("missing thread id")?
        .to_owned();
    client.turn(&thread, &format!("Remember this token for this conversation: {token}. Reply with ACK only. Do not use tools."), false).await?;
    client.close().await?;
    eprintln!("First turn completed; restarting the facade.");

    let mut client = Client::start(&binary, &state, &cwd).await?;
    client
        .request("thread/resume", json!({"threadId":thread}))
        .await?;
    let reply = client
        .turn(
            &thread,
            "What token did I ask you to remember? Reply with only the token. Do not use tools.",
            false,
        )
        .await?;
    ensure!(
        reply.contains(&token),
        "recall after process restart failed: {reply}"
    );

    let fork = client
        .request("thread/fork", json!({"threadId":thread}))
        .await?["thread"]["id"]
        .as_str()
        .context("missing fork id")?
        .to_owned();
    client
        .turn(
            &thread,
            "Replace the remembered token with REPLACED. Reply with ACK only. Do not use tools.",
            false,
        )
        .await?;
    let reply = client
        .turn(
            &fork,
            "What token did I ask you to remember? Reply with only the token. Do not use tools.",
            false,
        )
        .await?;
    ensure!(
        reply.contains(&token),
        "fork did not preserve its snapshot: {reply}"
    );
    if exercise_approval {
        client
            .turn(
                &thread,
                "Use Bash to run exactly: printf FACADE_TOOL_OK\nThen report the output.",
                true,
            )
            .await?;
    }
    client.close().await?;
    println!(
        "PASS: live inference, restart recall, fork snapshot{}",
        if exercise_approval {
            ", tool approval"
        } else {
            ""
        }
    );
    Ok(())
}
