#![cfg(feature = "test-backend")]

use serde_json::{Value, json};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
};

struct Peer {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    received: Vec<Value>,
    serial: u64,
}

impl Peer {
    async fn start(state: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
            .args([
                "app-server",
                "--stdio",
                "--claude",
                env!("CARGO_BIN_EXE_fake-claude"),
            ])
            .arg("--state-dir")
            .arg(state)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        Self {
            stdin: child.stdin.take().unwrap(),
            stdout: BufReader::new(child.stdout.take().unwrap()).lines(),
            child,
            received: vec![],
            serial: 0,
        }
    }

    async fn until(&mut self, predicate: impl Fn(&Value) -> bool) -> Value {
        if let Some(value) = self.received.iter().find(|value| predicate(value)) {
            return value.clone();
        }
        loop {
            let line = tokio::time::timeout(Duration::from_secs(10), self.stdout.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let value: Value = serde_json::from_str(&line).unwrap();
            self.received.push(value.clone());
            if predicate(&value) {
                return value;
            }
        }
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        self.serial += 1;
        let id = self.serial;
        let message = json!({"id":id,"method":method,"params":params});
        self.stdin
            .write_all(format!("{message}\n").as_bytes())
            .await
            .unwrap();
        let reply = self
            .until(|value| value["id"] == id && value.get("method").is_none())
            .await;
        assert!(reply.get("error").is_none(), "{reply}");
        reply["result"].clone()
    }

    async fn close(mut self) {
        drop(self.stdin);
        tokio::time::timeout(Duration::from_secs(10), self.child.wait())
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn steering_admission_does_not_block_the_actor_on_backend_stdin() {
    for prompt in ["steer-busy", "steer-closed-input"] {
        let state = tempfile::tempdir().unwrap();
        let mut peer = Peer::start(state.path()).await;
        peer.request(
            "initialize",
            json!({"clientInfo":{"name":"steering-concurrency","version":"1"}}),
        )
        .await;
        let thread = peer
            .request("thread/start", json!({"sandbox":"danger-full-access"}))
            .await["thread"]["id"]
            .clone();
        let turn = peer
            .request(
                "turn/start",
                json!({"threadId":thread,"input":[{"type":"text","text":prompt}]}),
            )
            .await["turn"]["id"]
            .clone();
        peer.until(|value| {
            value["method"] == "item/completed"
                && value["params"]["item"]["text"] == "Backend input paused"
        })
        .await;
        // Exceeds the backend pipe capacity. Its deliberate two-second read
        // pause must not stall the independent server request loop.
        tokio::time::timeout(Duration::from_secs(1), async {
            peer.request("turn/steer", json!({"threadId":thread,"expectedTurnId":turn,
                "input":[{"type":"text","text":format!("large-steer:{}", "x".repeat(512 * 1024))}]})).await;
            peer.request("thread/loaded/list", json!({})).await;
        }).await.expect("backend stdin blocked the server actor");
        let completed = peer
            .until(|value| {
                value["method"] == "turn/completed" && value["params"]["turn"]["id"] == turn
            })
            .await;
        assert_eq!(
            completed["params"]["turn"]["status"],
            if prompt == "steer-busy" {
                "completed"
            } else {
                "failed"
            }
        );
        if prompt == "steer-busy" {
            assert!(
                completed["params"]["turn"]["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|item| item["text"] == "Large input consumed")
            );
        }
        peer.close().await;
    }
}
