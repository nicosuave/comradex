#![cfg(all(feature = "test-backend", unix))]

use serde_json::{Value, json};
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    net::{
        UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    process::{Child, Command},
};

const DEADLINE: Duration = Duration::from_secs(12);

struct Server {
    child: Child,
    socket: PathBuf,
    _state: tempfile::TempDir,
}

impl Server {
    async fn start() -> Self {
        Self::with_approval_timeout(1).await
    }

    async fn with_approval_timeout(seconds: u64) -> Self {
        let state = tempfile::Builder::new()
            .prefix("reconnect-")
            .tempdir_in("/tmp")
            .unwrap();
        let socket = state.path().join("rpc.sock");
        let mut child = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
            .args(["app-server", "--claude", env!("CARGO_BIN_EXE_fake-claude")])
            .arg("--approval-timeout-seconds")
            .arg(seconds.to_string())
            .arg("--listen")
            .arg(format!("unix-lines://{}", socket.display()))
            .arg("--state-dir")
            .arg(state.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stderr = BufReader::new(child.stderr.take().unwrap()).lines();
        let ready = tokio::time::timeout(DEADLINE, stderr.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(ready.starts_with("Listening on "), "{ready}");
        Self {
            child,
            socket,
            _state: state,
        }
    }

    async fn connect(&self) -> Peer {
        let (read, write) = UnixStream::connect(&self.socket)
            .await
            .unwrap()
            .into_split();
        let mut peer = Peer {
            read: BufReader::new(read).lines(),
            write,
            history: vec![],
            serial: 0,
        };
        peer.ok(
            "initialize",
            json!({"clientInfo":{"name":"reconnect-test","version":"1"},
            "capabilities":{"experimentalApi":true}}),
        )
        .await;
        peer
    }

    async fn stop(mut self) {
        unsafe {
            libc::kill(self.child.id().unwrap() as i32, libc::SIGTERM);
        }
        assert!(
            tokio::time::timeout(DEADLINE, self.child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }
}

struct Peer {
    read: Lines<BufReader<OwnedReadHalf>>,
    write: OwnedWriteHalf,
    history: Vec<Value>,
    serial: u64,
}

impl Peer {
    async fn send(&mut self, value: Value) {
        self.write
            .write_all(format!("{value}\n").as_bytes())
            .await
            .unwrap();
    }

    async fn until(&mut self, predicate: impl Fn(&Value) -> bool) -> Value {
        if let Some(value) = self.history.iter().find(|value| predicate(value)) {
            return value.clone();
        }
        loop {
            let line = tokio::time::timeout(DEADLINE, self.read.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let value: Value = serde_json::from_str(&line).unwrap();
            self.history.push(value.clone());
            if predicate(&value) {
                return value;
            }
        }
    }

    async fn ok(&mut self, method: &str, params: Value) -> Value {
        self.serial += 1;
        let id = format!("rpc-{}", self.serial);
        self.send(json!({"id":id,"method":method,"params":params}))
            .await;
        let response = self
            .until(|value| value.get("method").is_none() && value["id"] == id)
            .await;
        assert!(response.get("error").is_none(), "{method}: {response}");
        response["result"].clone()
    }

    async fn thread(&mut self) -> Value {
        self.ok("thread/start", json!({"sandbox":"danger-full-access"}))
            .await["thread"]["id"]
            .clone()
    }

    async fn turn(&mut self, thread: &Value, text: &str) -> Value {
        self.ok(
            "turn/start",
            json!({"threadId":thread,"input":[{"type":"text","text":text}]}),
        )
        .await["turn"]["id"]
            .clone()
    }

    async fn completed(&mut self, turn: &Value) -> Value {
        self.until(|value| {
            value["method"] == "turn/completed" && value["params"]["turn"]["id"] == *turn
        })
        .await["params"]["turn"]
            .clone()
    }

    async fn close(mut self) {
        self.write.shutdown().await.unwrap();
    }
}

fn last_turn(snapshot: &Value) -> &Value {
    snapshot["thread"]["turns"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
}

#[tokio::test]
async fn reconnect_recovers_partial_stream_and_control_of_the_same_turn() {
    let server = Server::start().await;
    let mut owner = server.connect().await;
    let thread = owner.thread().await;
    let turn = owner.turn(&thread, "hang").await;
    owner
        .until(|value| {
            value["method"] == "item/agentMessage/delta" && value["params"]["delta"] == "Working"
        })
        .await;
    owner.close().await;
    let mut replacement = server.connect().await;
    let snapshot = replacement
        .ok("thread/resume", json!({"threadId":thread}))
        .await;
    let active = last_turn(&snapshot);
    assert_eq!(active["id"], turn);
    assert_eq!(active["status"], "inProgress");
    assert!(
        active["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["text"] == "Working")
    );
    assert_eq!(snapshot["thread"]["status"]["type"], "active");
    replacement
        .ok("turn/interrupt", json!({"threadId":thread,"turnId":turn}))
        .await;
    assert_eq!(replacement.completed(&turn).await["status"], "interrupted");
    replacement.close().await;
    server.stop().await;
}

#[tokio::test]
async fn approval_and_question_replay_keep_ids_and_pause_disconnected_time() {
    for (prompt, method, result) in [
        (
            "tool",
            "item/commandExecution/requestApproval",
            json!({"decision":"accept"}),
        ),
        (
            "question",
            "item/tool/requestUserInput",
            json!({"answers":{"q0":{"answers":["Two"]}}}),
        ),
    ] {
        let server = Server::start().await;
        let mut owner = server.connect().await;
        let thread = owner.thread().await;
        let turn = owner.turn(&thread, prompt).await;
        let original = owner.until(|value| value["method"] == method).await;
        owner.close().await;
        tokio::time::sleep(Duration::from_millis(1300)).await;
        let mut replacement = server.connect().await;
        let snapshot = replacement
            .ok("thread/resume", json!({"threadId":thread}))
            .await;
        assert_eq!(last_turn(&snapshot)["status"], "inProgress");
        let replay = replacement.until(|value| value["method"] == method).await;
        assert_eq!(
            replay, original,
            "replay changed request identity or payload"
        );
        let response_position = replacement
            .history
            .iter()
            .position(|value| value["result"]["thread"]["id"] == thread)
            .unwrap();
        let replay_position = replacement
            .history
            .iter()
            .position(|value| value["method"] == method)
            .unwrap();
        assert!(
            response_position < replay_position,
            "request replay preceded resume snapshot"
        );
        replacement
            .send(json!({"id":replay["id"],"result":result}))
            .await;
        let completed = replacement.completed(&turn).await;
        assert_eq!(completed["status"], "completed");
        if prompt == "tool" {
            assert!(
                completed["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|item| item["aggregatedOutput"] == "fake tool output")
            );
        } else {
            let text = completed["items"]
                .as_array()
                .unwrap()
                .iter()
                .find(|item| item["type"] == "agentMessage")
                .unwrap()["text"]
                .as_str()
                .unwrap();
            let answer: Value = serde_json::from_str(text).unwrap();
            assert_eq!(answer["updatedInput"]["answers"]["Which value?"], "Two");
        }
        replacement.close().await;
        server.stop().await;
    }
}

#[tokio::test]
async fn reconnect_resumes_the_remaining_approval_budget_instead_of_resetting_it() {
    let server = Server::with_approval_timeout(4).await;
    let mut owner = server.connect().await;
    let thread = owner.thread().await;
    let turn = owner.turn(&thread, "tool").await;
    let original = owner
        .until(|value| value["method"] == "item/commandExecution/requestApproval")
        .await;
    // Keep a wide gap between the remaining budget (1.5s) and a reset (4s),
    // allowing CI scheduling and backend completion without accepting a reset.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    owner.close().await;
    tokio::time::sleep(Duration::from_millis(4300)).await;
    let mut replacement = server.connect().await;
    let snapshot = replacement
        .ok("thread/resume", json!({"threadId":thread}))
        .await;
    assert_eq!(last_turn(&snapshot)["status"], "inProgress");
    let replay = replacement
        .until(|value| value["method"] == "item/commandExecution/requestApproval")
        .await;
    assert_eq!(replay, original);
    let completed = tokio::time::timeout(Duration::from_secs(3), replacement.completed(&turn))
        .await
        .expect("reconnect reset the approval timeout instead of retaining its remaining budget");
    assert!(
        completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["type"] == "commandExecution" && item["status"] == "failed")
    );
    replacement.close().await;
    server.stop().await;
}

#[tokio::test]
async fn cached_dynamic_result_before_resume_is_accepted_without_observer_takeover() {
    let server = Server::start().await;
    let mut owner = server.connect().await;
    let thread = owner.ok("thread/start",json!({"sandbox":"danger-full-access","dynamicTools":[
        {"type":"namespace","name":"desktop","description":"Desktop","tools":[
            {"type":"function","name":"echo","description":"Echo","inputSchema":{"type":"object"}}]}]})).await["thread"]["id"].clone();
    let turn = owner.turn(&thread, "desktop-tool").await;
    let call = owner
        .until(|value| value["method"] == "item/tool/call")
        .await;
    let mut observer = server.connect().await;
    observer
        .ok("thread/resume", json!({"threadId":thread}))
        .await;
    observer.send(json!({"id":call["id"],"result":{"success":true,"contentItems":[{"type":"inputText","text":"observer must not win"}]}})).await;
    let snapshot = observer
        .ok(
            "thread/read",
            json!({"threadId":thread,"includeTurns":true}),
        )
        .await;
    assert_eq!(last_turn(&snapshot)["status"], "inProgress");
    assert!(
        !observer
            .history
            .iter()
            .any(|value| value["method"] == "item/tool/call"),
        "live observer received an executable request"
    );
    owner.close().await;

    let mut replacement = server.connect().await;
    replacement.send(json!({"id":call["id"],"result":{"success":true,"contentItems":[{"type":"inputText","text":"cached client result"}]}})).await;
    let completed = replacement.completed(&turn).await;
    assert_eq!(completed["status"], "completed");
    let calls: Vec<_> = completed["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["type"] == "dynamicToolCall")
        .collect();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["id"], call["params"]["callId"]);
    assert_eq!(calls[0]["success"], true);
    assert_eq!(calls[0]["contentItems"][0]["text"], "cached client result");
    assert!(
        !replacement
            .history
            .iter()
            .any(|value| value["method"] == "item/tool/call"),
        "cached result triggered duplicate execution"
    );
    assert_eq!(observer.completed(&turn).await, completed);
    let snapshot = replacement
        .ok("thread/resume", json!({"threadId":thread}))
        .await;
    assert_eq!(last_turn(&snapshot), &completed);
    replacement.close().await;
    observer.close().await;
    server.stop().await;
}

#[tokio::test]
async fn detached_turn_finishes_and_reopens_with_its_completed_items() {
    let server = Server::start().await;
    let mut owner = server.connect().await;
    let thread = owner.thread().await;
    let mut observer = server.connect().await;
    observer
        .ok("thread/resume", json!({"threadId":thread}))
        .await;
    let turn = owner.turn(&thread, "steer-busy").await;
    owner
        .until(|value| {
            value["method"] == "item/completed"
                && value["params"]["item"]["text"] == "Backend input paused"
        })
        .await;
    owner.ok("turn/steer",json!({"threadId":thread,"expectedTurnId":turn,"input":[{"type":"text","text":"finish while detached"}]})).await;
    owner.close().await;
    let completed = observer.completed(&turn).await;
    assert_eq!(completed["status"], "completed");
    assert!(
        completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["text"] == "Echo: finish while detached")
    );
    let mut replacement = server.connect().await;
    let snapshot = replacement
        .ok("thread/resume", json!({"threadId":thread}))
        .await;
    assert_eq!(last_turn(&snapshot), &completed);
    assert_eq!(snapshot["thread"]["status"]["type"], "idle");
    replacement.close().await;
    observer.close().await;
    server.stop().await;
}
