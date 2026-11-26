#![cfg(all(feature = "test-backend", unix))]
//! The local router between genuine Codex (a deterministic stand-in here) and
//! the Claude facade, exercised end to end through its stdio transport.

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{collections::HashSet, path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
};

const DEADLINE: Duration = Duration::from_secs(15);
/// Deployment settings of an enclosing facade session must not leak into the
/// fixtures (for example, a real plugin sidecar).
const DEPLOYMENT_ENV: &[&str] = &[
    "CLAUDE_CODEX_HOME",
    "CLAUDE_CODEX_PLUGIN_EXECUTABLE",
    "CLAUDE_CODEX_PLUGIN_HOME",
    "CLAUDE_CODEX_SOCKET_DIR",
    "CODEX_HOME",
];
const DESKTOP_ARGS: &[&str] = &[
    "-c",
    "features.code_mode_host=true",
    "app-server",
    "--analytics-default-enabled",
];

struct MlxFixture(tokio::task::JoinHandle<()>);

impl MlxFixture {
    async fn enable(facade: &Facade) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1", listener.local_addr().unwrap());
        let models = ["mlx-test", "mlx-start-fails", "gpt-fake"].map(|id| {
            json!({
            "id":id,"context_length":32768,"capabilities":["chat","tool_use","streaming"],
            "input_modalities":["text"]})
        });
        let body = json!({"data":models}).to_string();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                use tokio::io::AsyncReadExt;
                let mut request = [0; 8192];
                if stream.read(&mut request).await.is_err() {
                    continue;
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        let config = claude_codex_server::local_provider::Config::discover(&url)
            .await
            .unwrap();
        config.write(facade._state.path()).unwrap();
        Self(task)
    }
}

impl Drop for MlxFixture {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The facade's login-session service, listening on its private socket.
struct Facade {
    _child: Child,
    socket: std::path::PathBuf,
    _state: tempfile::TempDir,
    process_group: Option<u32>,
}

impl Drop for Facade {
    fn drop(&mut self) {
        if let Some(group) = self.process_group {
            // This test-owned service and its supervised ACP worker share a
            // private process group, so a failed test cannot leave workers.
            unsafe {
                libc::kill(-(group as i32), libc::SIGTERM);
            }
        }
    }
}

impl Facade {
    async fn start(claude: &str) -> Self {
        Self::start_with_model(claude, "fake-claude").await
    }

    async fn start_with_model(claude: &str, model: &str) -> Self {
        let state = tempfile::tempdir().unwrap();
        let socket = state.path().join("f.sock");
        let child = Self::launch_with_model(&state, &socket, claude, model).await;
        Self {
            _child: child,
            socket,
            _state: state,
            process_group: None,
        }
    }

    async fn start_with_local_acp() -> Self {
        let state = tempfile::tempdir_in("/tmp").unwrap();
        let root = state.path();
        let sockets = root.join("sockets");
        let socket = sockets.join("app-server-control.sock");
        std::fs::create_dir(&sockets).unwrap();
        let config = claude_codex_server::local_acp::LocalAcp {
            executable: env!("CARGO_BIN_EXE_fake-acp").into(),
            args: vec![
                format!("--store={}", root.join("native-acp").display()),
                "--extra-model".into(),
            ],
            provider_id: "fake-provider".into(),
            provider_name: "Fake Provider".into(),
            model: "fake-b".into(),
            include_models: vec!["fake-a".into()],
            initialize_timeout_seconds: 3,
        };
        std::fs::write(
            claude_codex_server::local_acp::LocalAcp::config_path(root),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"));
        use std::os::unix::process::CommandExt;
        command.as_std_mut().process_group(0);
        for name in DEPLOYMENT_ENV {
            command.env_remove(name);
        }
        let child = command
            .args([
                "app-server",
                "--listen",
                "unix://",
                "--claude",
                env!("CARGO_BIN_EXE_fake-claude"),
                "--model",
                "opus",
            ])
            .args(["--initialize-timeout-seconds", "3"])
            .env("CLAUDE_CODEX_SOCKET_DIR", &sockets)
            .arg("--state-dir")
            .arg(root.join("state"))
            .current_dir(root)
            .stdin(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let acp_socket = claude_codex_server::local_acp::LocalAcp::socket(root);
        let process_group = child.id();
        tokio::time::timeout(DEADLINE, async {
            for path in [&socket, &acp_socket] {
                while tokio::net::UnixStream::connect(path).await.is_err() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        })
        .await
        .expect("both local facades are listening");
        Self {
            _child: child,
            socket,
            _state: state,
            process_group,
        }
    }

    async fn launch(state: &tempfile::TempDir, socket: &Path, claude: &str) -> Child {
        Self::launch_with_model(state, socket, claude, "fake-claude").await
    }

    async fn launch_with_model(
        state: &tempfile::TempDir,
        socket: &Path,
        claude: &str,
        model: &str,
    ) -> Child {
        let mut command = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"));
        for name in DEPLOYMENT_ENV {
            command.env_remove(name);
        }
        let child = command
            .args(["app-server", "--claude", claude, "--model", model])
            .args(["--initialize-timeout-seconds", "3"])
            .arg("--listen")
            .arg(format!("unix://{}", socket.display()))
            .arg("--state-dir")
            .arg(state.path().join("state"))
            .current_dir(state.path())
            .stdin(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        tokio::time::timeout(DEADLINE, async {
            // A restarted service can leave the old socket path behind before
            // rebinding it. Wait for a live listener, not just the path.
            while tokio::net::UnixStream::connect(socket).await.is_err() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("facade socket");
        child
    }
}

struct Desktop {
    _child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    history: Vec<Value>,
    serial: u64,
    deadline: Duration,
}

impl Desktop {
    async fn connect(socket: &Path, seeded_codex_threads: usize) -> Self {
        Self::connect_with_codex(
            socket,
            seeded_codex_threads,
            env!("CARGO_BIN_EXE_fake-codex"),
            DEADLINE,
        )
        .await
    }

    async fn connect_with_codex(
        socket: &Path,
        seeded_codex_threads: usize,
        codex: &str,
        deadline: Duration,
    ) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"));
        for name in DEPLOYMENT_ENV {
            command.env_remove(name);
        }
        let mut child = command
            .arg("--sock")
            .arg(socket)
            .args(["route", "--codex", codex, "--"])
            .args(DESKTOP_ARGS)
            .env("FAKE_CODEX_THREADS", seeded_codex_threads.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut desktop = Self {
            input: child.stdin.take().unwrap(),
            output: BufReader::new(child.stdout.take().unwrap()).lines(),
            _child: child,
            history: vec![],
            serial: 0,
            deadline,
        };
        let init = desktop
            .ok(
                "initialize",
                json!({"clientInfo":{"name":"codex_desktop","version":"1"},
                    "capabilities":{"experimentalApi":true}}),
            )
            .await;
        assert!(
            init["userAgent"]
                .as_str()
                .is_some_and(|agent| agent.contains("codex")),
            "{init}"
        );
        desktop
            .send(json!({"method":"initialized","params":{}}))
            .await;
        desktop
    }

    async fn send(&mut self, value: Value) {
        let line = format!("{value}\n");
        tokio::time::timeout(DEADLINE, self.input.write_all(line.as_bytes()))
            .await
            .unwrap()
            .unwrap();
    }

    async fn next(&mut self) -> Value {
        let line = tokio::time::timeout(self.deadline, self.output.next_line())
            .await
            .expect("router output")
            .unwrap()
            .expect("router stdout open");
        serde_json::from_str(&line).unwrap()
    }

    /// Returns the next matching message, keeping the others for later.
    async fn until(&mut self, predicate: impl Fn(&Value) -> bool) -> Value {
        if let Some(position) = self.history.iter().position(&predicate) {
            return self.history.remove(position);
        }
        loop {
            let message = self.next().await;
            if predicate(&message) {
                return message;
            }
            self.history.push(message);
        }
    }

    async fn call(&mut self, method: &str, params: Value) -> Value {
        self.serial += 1;
        let id = self.serial;
        self.send(json!({"id": id, "method": method, "params": params}))
            .await;
        self.until(|v| v["id"] == id && v.get("method").is_none())
            .await
    }

    async fn ok(&mut self, method: &str, params: Value) -> Value {
        let reply = self.call(method, params).await;
        assert!(reply.get("error").is_none(), "{method} failed: {reply}");
        reply["result"].clone()
    }

    async fn thread(&mut self, params: Value) -> String {
        self.ok("thread/start", params).await["thread"]["id"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    async fn turn(&mut self, thread: &str, model: &str, text: &str) -> Value {
        self.call(
            "turn/start",
            json!({"threadId": thread, "model": model,
                "input": [{"type": "text", "text": text, "text_elements": []}]}),
        )
        .await
    }

    async fn completed(&mut self, thread: &str) -> Value {
        self.until(|v| v["method"] == "turn/completed" && v["params"]["threadId"] == thread)
            .await
    }

    async fn crash_router(&mut self) {
        #[cfg(target_os = "macos")]
        {
            // The desktop's direct child is now Codex. Crash the actual router
            // worker, preserving coverage of recovery without orderly shutdown.
            let children = Command::new("pgrep")
                .args(["-P", &self._child.id().unwrap().to_string()])
                .output()
                .await
                .unwrap();
            for pid in String::from_utf8_lossy(&children.stdout).split_whitespace() {
                let args = Command::new("ps")
                    .args(["-p", pid, "-o", "args="])
                    .output()
                    .await
                    .unwrap();
                if String::from_utf8_lossy(&args.stdout).contains(" route --codex ") {
                    let status = Command::new("kill")
                        .args(["-KILL", pid])
                        .status()
                        .await
                        .unwrap();
                    assert!(status.success());
                    tokio::time::timeout(DEADLINE, self._child.wait())
                        .await
                        .expect("Codex exited after router crash")
                        .unwrap();
                    return;
                }
            }
            panic!("router worker was not found");
        }
        #[cfg(not(target_os = "macos"))]
        self._child.kill().await.unwrap();
    }
}

fn ids(page: &Value) -> Vec<String> {
    page["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| {
            item["thread"]["id"]
                .as_str()
                .or_else(|| item["id"].as_str())
                .unwrap()
                .to_owned()
        })
        .collect()
}

fn script(actions: Value) -> String {
    format!("delegate-script:{actions}")
}

fn reports(completed: &Value) -> Vec<Value> {
    let text = completed["params"]["turn"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "agentMessage")
        .unwrap()["text"]
        .as_str()
        .unwrap();
    serde_json::from_str(text).unwrap()
}

fn streamed_answer(desktop: &Desktop, completed: &Value) -> String {
    // Current Codex can return a completed turn with itemsView=notLoaded.
    // The subscribed stream remains the source of its actual answer text.
    let params = &completed["params"];
    let turn = &params["turn"]["id"];
    let messages: Vec<&Value> = desktop
        .history
        .iter()
        .filter(|message| {
            message["params"]["threadId"] == params["threadId"]
                && message["params"]["turnId"] == *turn
        })
        .collect();
    let final_text: String = messages
        .iter()
        .filter(|message| {
            message["method"] == "item/completed"
                && message["params"]["item"]["type"] == "agentMessage"
        })
        .filter_map(|message| message["params"]["item"]["text"].as_str())
        .collect();
    if !final_text.is_empty() {
        return final_text;
    }
    messages
        .iter()
        .filter(|message| message["method"] == "item/agentMessage/delta")
        .filter_map(|message| message["params"]["delta"].as_str())
        .collect()
}

fn result(report: &Value) -> Value {
    assert_eq!(report["isError"], false, "{report}");
    serde_json::from_str(report["content"][0]["text"].as_str().unwrap()).unwrap()
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn local_codex_keeps_the_desktops_child_pid_and_parent() {
    let missing = tempfile::tempdir().unwrap();
    let mut desktop = Desktop::connect(&missing.path().join("absent.sock"), 0).await;
    let backend = desktop.ok("fake/state", json!({})).await;
    assert_eq!(backend["pid"], desktop._child.id().unwrap());
    assert_eq!(backend["parentPid"], std::process::id());

    // Killing the app's direct child must also release the router's inherited
    // stdout, even while the desktop's stdin remains open.
    desktop._child.kill().await.unwrap();
    tokio::time::timeout(DEADLINE, async {
        while desktop.output.next_line().await.unwrap().is_some() {}
    })
    .await
    .expect("router exited after Codex was killed");
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn local_router_closes_codex_when_desktop_input_closes() {
    let missing = tempfile::tempdir().unwrap();
    let desktop = Desktop::connect(&missing.path().join("absent.sock"), 0).await;
    let Desktop {
        mut _child,
        input,
        mut output,
        ..
    } = desktop;
    drop(input);
    let status = tokio::time::timeout(DEADLINE, _child.wait())
        .await
        .expect("Codex exited on desktop EOF")
        .unwrap();
    assert!(status.success());
    tokio::time::timeout(DEADLINE, async {
        while output.next_line().await.unwrap().is_some() {}
    })
    .await
    .expect("router released desktop stdout");
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn failed_local_codex_exec_does_not_leave_the_router_running() {
    let temp = tempfile::tempdir().unwrap();
    let output = tokio::time::timeout(
        DEADLINE,
        Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
            .arg("--sock")
            .arg(temp.path().join("absent.sock"))
            .args(["route", "--codex"])
            .arg(temp.path().join("missing-codex"))
            .arg("--")
            .args(DESKTOP_ARGS)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("failed exec released all inherited pipes")
    .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Cannot run"));
}

#[tokio::test]
async fn inherited_mcp_endpoint_cannot_use_another_native_callers_permissions() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let parent = desktop
        .thread(json!({"model":"gpt-fake","approvalPolicy":"never"}))
        .await;
    let actions = json!([
        {"tool":"spawn_agent","arguments":{"harness":"claude","model":"opus","message":"task"},"meta":{"threadId":"native-child"}},
        {"tool":"spawn_agent","arguments":{"harness":"claude","model":"opus","message":"task"},"meta":{}},
        {"tool":"list_agents","arguments":{}}
    ]);
    desktop.turn(&parent, "gpt-fake", &script(actions)).await;
    let completed = desktop.completed(&parent).await;
    let reports = reports(&completed);
    for report in &reports[..2] {
        assert_eq!(report["isError"], true);
        assert!(
            report["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("does not match")
        );
    }
    assert!(result(&reports[2])["agents"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn refused_followup_preserves_queued_messages_for_the_next_native_turn() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let parent = desktop
        .thread(json!({"model":"opus","approvalPolicy":"never"}))
        .await;
    let actions = json!([
        {"tool":"spawn_agent","arguments":{"harness":"codex","model":"gpt-fake","message":"task"}},
        {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}},
        {"tool":"send_message","arguments":{"agent_id":"$agent","message":"queued context"}},
        {"tool":"followup_task","arguments":{"agent_id":"$agent","message":"fail-turn-start"}},
        {"tool":"followup_task","arguments":{"agent_id":"$agent","message":"retry task"}},
        {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}}
    ]);
    desktop.turn(&parent, "opus", &script(actions)).await;
    let completed = desktop.completed(&parent).await;
    let reports = reports(&completed);
    assert_eq!(reports[3]["isError"], true);
    assert_eq!(
        result(&reports[5])["report"],
        "Codex: queued context\n\nretry task"
    );
}

#[tokio::test]
async fn native_mcp_delegation_works_in_both_directions_with_linked_history() {
    for (parent_model, harness, child_model, expected) in [
        ("gpt-fake", "claude", "opus", "Echo: delegated task"),
        ("opus", "codex", "gpt-fake", "Codex: delegated task"),
    ] {
        let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
        let mut desktop = Desktop::connect(&facade.socket, 0).await;
        let parent=desktop.thread(json!({"model":parent_model,"cwd":facade._state.path(),
            "sandbox":"workspace-write","approvalPolicy":"never","approvalsReviewer":"auto_review"})).await;
        let actions = json!([
            {"tool":"spawn_agent","arguments":{"harness":harness,"model":child_model,"message":"delegated task","name":"Cross-harness worker"}},
            {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}},
            {"tool":"list_agents","arguments":{}}
        ]);
        assert!(
            desktop
                .turn(&parent, parent_model, &script(actions))
                .await
                .get("error")
                .is_none()
        );
        let completed = desktop.completed(&parent).await;
        let reports = reports(&completed);
        let child = result(&reports[0])["agent_id"].as_str().unwrap().to_owned();
        let waited = result(&reports[1]);
        assert_eq!(waited["status"], "completed");
        assert_eq!(waited["report"], expected);
        assert_eq!(result(&reports[2])["agents"].as_array().unwrap().len(), 1);
        let list = desktop
            .ok("thread/list", json!({"parentThreadId":parent}))
            .await;
        assert_eq!(ids(&list).as_slice(), std::slice::from_ref(&child));
        assert_eq!(
            list["data"][0]["source"]["subAgent"]["thread_spawn"]["parent_thread_id"],
            parent
        );
        assert_eq!(list["data"][0]["agentNickname"], "Cross-harness worker");
        let normal = desktop.ok("thread/list", json!({})).await;
        assert!(
            !ids(&normal).contains(&child),
            "Delegates appeared as unrelated top-level chats"
        );
        let read = desktop
            .ok("thread/read", json!({"threadId":child,"includeTurns":true}))
            .await;
        assert_eq!(read["thread"]["parentThreadId"], parent);
        assert_eq!(read["thread"]["canAcceptDirectInput"], true);
        let permissions = desktop.ok("thread/resume", json!({"threadId":child})).await;
        assert_eq!(permissions["approvalPolicy"], "never");
        assert_eq!(permissions["approvalsReviewer"], "auto_review");
        assert_eq!(permissions["sandbox"]["type"], "workspaceWrite");
        assert_eq!(permissions["sandbox"]["networkAccess"], false);
        let history = desktop
            .ok(
                "thread/read",
                json!({"threadId":parent,"includeTurns":true}),
            )
            .await;
        let spawn = history["thread"]["turns"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|turn| turn["items"].as_array().unwrap())
            .find(|item| item["type"] == "collabAgentToolCall")
            .unwrap();
        assert_eq!(spawn["receiverThreadIds"], json!([child]));
        assert_eq!(spawn["tool"], "spawnAgent");
    }
}

#[tokio::test]
async fn codex_parent_delegates_after_switching_to_approve_for_me() {
    for method in ["thread/settings/update", "turn/start"] {
        let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
        let mut desktop = Desktop::connect(&facade.socket, 0).await;
        let parent = desktop
            .thread(json!({"model":"gpt-fake","cwd":facade._state.path(),
            "sandbox":"workspace-write","approvalPolicy":"on-request","approvalsReviewer":"user"}))
            .await;
        let actions = script(json!([
            {"tool":"spawn_agent","arguments":{"harness":"claude","model":"opus","message":"delegated task"}},
            {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}}
        ]));
        if method == "thread/settings/update" {
            desktop
                .ok(
                    method,
                    json!({"threadId":parent,"approvalsReviewer":"auto_review"}),
                )
                .await;
        }
        desktop
            .ok(
                "turn/start",
                json!({"threadId":parent,"model":"gpt-fake",
            "approvalsReviewer":"auto_review",
            "input":[{"type":"text","text":actions,"text_elements":[]}]}),
            )
            .await;
        let completed = desktop.completed(&parent).await;
        let reports = reports(&completed);
        assert_ne!(reports[0]["isError"], true, "{method}: {reports:?}");
        assert_eq!(result(&reports[1])["report"], "Echo: delegated task");
        let child = result(&reports[0])["agent_id"].clone();
        let settings = desktop.ok("thread/resume", json!({"threadId":child})).await;
        assert_eq!(settings["approvalsReviewer"], "auto_review");
        assert_eq!(settings["approvalPolicy"], "on-request");
        assert_eq!(settings["sandbox"]["type"], "workspaceWrite");
        let state = desktop.ok("fake/state", json!({})).await;
        assert!(
            !state["requests"]
                .as_array()
                .unwrap()
                .contains(&json!("thread/resume")),
            "Delegation must not resume the active Codex parent: {state}"
        );
    }
}

#[tokio::test]
async fn desktop_can_open_running_delegate_without_reconfiguring_it() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let parent = desktop
        .thread(json!({"model":"gpt-fake","cwd":facade._state.path(),
            "sandbox":"workspace-write","approvalPolicy":"never","approvalsReviewer":"auto_review"}))
        .await;
    desktop.turn(&parent, "gpt-fake", &script(json!([
        {"tool":"spawn_agent","arguments":{"harness":"claude","model":"opus","message":"hang"}}
    ]))).await;
    let completed = desktop.completed(&parent).await;
    let child = result(&reports(&completed)[0])["agent_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let before = desktop.ok("thread/resume", json!({"threadId":child})).await;
    assert_eq!(before["thread"]["turns"][0]["status"], "inProgress");
    let open = json!({"threadId":child,"path":before["thread"]["path"],
        "excludeTurns":true,"initialTurnsPage":{"limit":10},
        "cwd":facade._state.path(),"model":"fable","effort":"high",
        "approvalPolicy":"on-request","approvalsReviewer":"user","sandbox":"danger-full-access",
        "developerInstructions":"Desktop opening context","baseInstructions":"Desktop base context",
        "dynamicTools":[]});
    let opened = desktop.ok("thread/resume", open.clone()).await;
    assert_eq!(opened["model"], "opus");
    assert_eq!(opened["approvalPolicy"], before["approvalPolicy"]);
    assert_eq!(opened["approvalsReviewer"], before["approvalsReviewer"]);
    assert_eq!(opened["sandbox"], before["sandbox"]);
    assert_eq!(opened["thread"]["turns"], json!([]));
    assert_eq!(
        opened["initialTurnsPage"]["data"][0]["status"],
        "inProgress"
    );
    assert_eq!(opened["thread"]["parentThreadId"], parent);
    // Direct control still reaches the running child after hydration.
    desktop
        .ok(
            "turn/interrupt",
            json!({"threadId":child,"turnId":before["thread"]["turns"][0]["id"]}),
        )
        .await;
    let completed = desktop.completed(&child).await;
    assert_eq!(completed["params"]["turn"]["status"], "interrupted");
    let idle = desktop.ok("thread/resume", open).await;
    assert_eq!(idle["model"], "opus");
    assert_eq!(idle["sandbox"], before["sandbox"]);
    let saved: Value =
        serde_json::from_slice(&std::fs::read(before["thread"]["path"].as_str().unwrap()).unwrap())
            .unwrap();
    assert!(
        saved["settings"]["developer_instructions"]
            .as_str()
            .unwrap()
            .starts_with("You are a delegated child agent")
    );
    assert_eq!(saved["settings"]["base_instructions"], Value::Null);
    assert!(
        !saved["settings"]["dynamic_tools"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    desktop
        .ok(
            "thread/settings/update",
            json!({"threadId":child,"model":"fable"}),
        )
        .await;
    let changed = desktop.ok("thread/resume", json!({"threadId":child})).await;
    assert_eq!(changed["model"], "fable");
}

#[tokio::test]
async fn delegation_wait_steer_queue_and_followup_preserve_one_native_child() {
    for (parent_model, harness, child_model) in [
        ("gpt-fake", "claude", "opus"),
        ("opus", "codex", "gpt-fake"),
    ] {
        let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
        let mut desktop = Desktop::connect(&facade.socket, 0).await;
        let parent = desktop
            .thread(json!({"model":parent_model,"approvalPolicy":"never"}))
            .await;
        let actions = json!([
            {"tool":"spawn_agent","arguments":{"harness":harness,"model":child_model,"message":"hang"}},
            {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":0}},
            {"tool":"send_message","arguments":{"agent_id":"$agent","message":"finish task"}},
            {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}},
            {"tool":"send_message","arguments":{"agent_id":"$agent","message":"queued context"}},
            {"tool":"followup_task","arguments":{"agent_id":"$agent","message":"follow-up task"}},
            {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}},
            {"tool":"list_agents","arguments":{}}
        ]);
        desktop.turn(&parent, parent_model, &script(actions)).await;
        let completed = desktop.completed(&parent).await;
        let reports = reports(&completed);
        let child = result(&reports[0])["agent_id"].clone();
        assert_eq!(result(&reports[1])["status"], "running");
        assert!(
            result(&reports[3])["report"]
                .as_str()
                .unwrap()
                .contains("finish task")
        );
        assert_eq!(result(&reports[4])["queued"], true);
        assert_eq!(result(&reports[5])["agent_id"], child);
        assert!(
            result(&reports[6])["report"]
                .as_str()
                .unwrap()
                .contains("queued context\n\nfollow-up task")
        );
        assert_eq!(result(&reports[7])["agents"].as_array().unwrap().len(), 1);
        let read = desktop
            .ok("thread/read", json!({"threadId":child,"includeTurns":true}))
            .await;
        assert_eq!(read["thread"]["turns"].as_array().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn delegated_native_approvals_round_trip_on_the_child_thread() {
    for (parent_model, harness, child_model) in [
        ("gpt-fake", "claude", "opus"),
        ("opus", "codex", "gpt-fake"),
    ] {
        let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
        let mut desktop = Desktop::connect(&facade.socket, 0).await;
        let parent=desktop.thread(json!({"model":parent_model,"approvalPolicy":"on-request","approvalsReviewer":"user"})).await;
        let message = if harness == "claude" {
            "tool"
        } else {
            "permission"
        };
        desktop.turn(&parent,parent_model,&script(json!([
            {"tool":"spawn_agent","arguments":{"harness":harness,"model":child_model,"message":message}},
            {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}}
        ]))).await;
        let approval = desktop
            .until(|message| message["method"] == "item/commandExecution/requestApproval")
            .await;
        let child = approval["params"]["threadId"].as_str().unwrap().to_owned();
        assert_ne!(child, parent);
        desktop
            .send(json!({"id":approval["id"],"result":{"decision":"decline"}}))
            .await;
        let completed = desktop.completed(&parent).await;
        let reports = reports(&completed);
        assert_eq!(result(&reports[0])["agent_id"], child);
        assert!(
            result(&reports[1])["report"]
                .as_str()
                .unwrap()
                .to_lowercase()
                .contains("denied")
        );
    }
}

/// Opt-in model-backed verification. Uses normal native authentication, creates
/// only test threads, archives them afterwards, and never installs or restarts
/// the user's service.
#[tokio::test]
#[ignore = "requires CLAUDEX_NATIVE_CODEX, CLAUDEX_NATIVE_CLAUDE and native authentication; makes model calls"]
async fn native_shared_delegation() {
    let codex = std::env::var("CLAUDEX_NATIVE_CODEX").expect("genuine Codex executable");
    let claude = std::env::var("CLAUDEX_NATIVE_CLAUDE").expect("native Claude executable");
    let gpt = std::env::var("CLAUDEX_NATIVE_GPT_MODEL").unwrap_or_else(|_| "gpt-6-luna".into());
    let facade = if std::env::var_os("CLAUDEX_NATIVE_FACADE_SOCKET").is_none() {
        Some(Facade::start_with_model(&claude, "opus").await)
    } else {
        None
    };
    let scratch = tempfile::tempdir().unwrap();
    let socket = std::env::var_os("CLAUDEX_NATIVE_FACADE_SOCKET")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| facade.as_ref().unwrap().socket.clone());
    let mut desktop =
        Desktop::connect_with_codex(&socket, 0, &codex, Duration::from_secs(120)).await;
    for (parent_model, harness, child_model, reviewer) in [
        (gpt.as_str(), "claude", "opus", "user"),
        ("opus", "codex", gpt.as_str(), "user"),
        ("gpt-6.1-sol", "claude", "opus", "auto_review"),
    ] {
        let parent = desktop
            .thread(json!({"model":parent_model,"cwd":scratch.path(),
            "sandbox":"workspace-write","approvalPolicy":"on-request","approvalsReviewer":"user"}))
            .await;
        let prompt = format!(
            "Verify cross-harness delegation. Use the claudex_agents MCP tools, not native subagent tools or shell subprocesses. Call spawn_agent with harness={harness}, model={child_model}, name=Native verification, and message=Reply exactly CROSS_HARNESS_CHILD_OK without using other tools. Then call wait_agent on the returned agent_id until it finishes. Finally reply CROSS_HARNESS_PARENT_OK. Do not edit files or run other tools."
        );
        // Change the review mode on the turn, as the desktop composer does. It
        // also restates the workspace with cwd as a writable root, which the
        // native settings report omits and never re-sends while unchanged.
        let reply = desktop
            .call(
                "turn/start",
                json!({"threadId":parent,"model":parent_model,
            "cwd":scratch.path(),"approvalPolicy":"on-request","approvalsReviewer":reviewer,
            "sandboxPolicy":{"type":"workspaceWrite","writableRoots":[scratch.path()],
                "networkAccess":false,"excludeTmpdirEnvVar":false,"excludeSlashTmp":false},
            "input":[{"type":"text","text":prompt,"text_elements":[]}]}),
            )
            .await;
        assert!(reply.get("error").is_none(), "{reply}");
        let completed = loop {
            let message = desktop
                .until(|message| {
                    message["method"] == "turn/completed" && message["params"]["threadId"] == parent
                        || message.get("method").is_some() && message.get("id").is_some()
                })
                .await;
            if message["method"] == "turn/completed" {
                break message;
            }
            if message["method"] == "mcpServer/elicitation/request"
                && message["params"]["serverName"] == "claudex_agents"
                && message["params"]["_meta"]["codex_approval_kind"] == "mcp_tool_call"
            {
                let arguments = &message["params"]["_meta"]["tool_params"];
                if arguments.get("harness").is_some() {
                    assert_eq!(arguments["harness"], harness);
                    assert_eq!(arguments["model"], child_model);
                }
                desktop
                    .send(json!({"id":message["id"],"result":{"action":"accept","content":{}}}))
                    .await;
                continue;
            }
            panic!("Unexpected approval outside the native delegation probe: {message}");
        };
        assert_eq!(
            completed["params"]["turn"]["status"], "completed",
            "{completed}"
        );
        let spawn = completed["params"]["turn"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "collabAgentToolCall" && item["tool"] == "spawnAgent")
            .unwrap_or_else(||panic!("Native model did not spawn through the shared tool: {completed}; MCP startup: {:?}",desktop.history.iter().filter(|message|message["method"] == "mcpServer/startupStatus/updated").collect::<Vec<_>>()));
        let child = spawn["receiverThreadIds"][0].as_str().unwrap();
        let child = desktop
            .ok("thread/read", json!({"threadId":child,"includeTurns":true}))
            .await;
        assert_eq!(child["thread"]["parentThreadId"], parent);
        // The child's task is attributed to the turn that delegated it.
        let root = &completed["params"]["turn"]["rootTurnId"];
        assert!(root.is_string(), "{completed}");
        assert_eq!(child["thread"]["turns"][0]["rootTurnId"], *root, "{child}");
        let settings = desktop
            .ok("thread/resume", json!({"threadId":child["thread"]["id"]}))
            .await;
        assert_eq!(settings["approvalsReviewer"], reviewer);
        let report = child["thread"]["turns"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|turn| turn["items"].as_array().unwrap())
            .filter(|item| item["type"] == "agentMessage")
            .filter_map(|item| item["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(report.contains("CROSS_HARNESS_CHILD_OK"), "{report}");
        eprintln!(
            "Verified native {parent_model} -> {harness}/{child_model}: child {child_id}",
            child_id = child["thread"]["id"]
        );
        desktop
            .ok("thread/archive", json!({"threadId":child["thread"]["id"]}))
            .await;
        desktop
            .ok("thread/archive", json!({"threadId":parent}))
            .await;
    }
}

#[tokio::test]
async fn attachment_owners_include_claude_threads() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let owner = desktop
        .thread(json!({"model":"fake-claude","sandbox":"danger-full-access"}))
        .await;
    desktop
        .ok(
            "thread/attachment/add",
            json!({"threadId":owner,"attachmentType":"worktree","identityKey":"/repo/worktree",
                "payload":{"root":"/repo/worktree"}}),
        )
        .await;
    // The desktop's archive cleanup query for a managed worktree.
    let query = json!({"attachmentType":"worktree","identityKey":"/repo/worktree",
        "archived":false,"cursor":null,"limit":100});
    let owners = desktop
        .ok("thread/attachmentOwner/list", query.clone())
        .await;
    assert_eq!(
        owners,
        json!({"data":[{"threadId":owner,"archived":false}],"nextCursor":null})
    );
    let mut other = query;
    other["identityKey"] = json!("/repo/other");
    let none = desktop.ok("thread/attachmentOwner/list", other).await;
    assert_eq!(none["data"], json!([]));
}

#[tokio::test]
async fn desktop_samples_claude_threads_on_their_warm_process() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let parent = desktop
        .thread(json!({"model":"fake-claude","sandbox":"danger-full-access"}))
        .await;
    assert!(
        desktop
            .turn(&parent, "fake-claude", "parent context")
            .await
            .get("error")
            .is_none()
    );
    let turn = desktop.completed(&parent).await["params"]["turn"]["id"].clone();
    let stale = desktop
        .call(
            "thread/sample",
            json!({"threadId":parent,"expectedTurnId":"earlier","prompt":"next?"}),
        )
        .await;
    assert!(
        stale["error"]["message"]
            .as_str()
            .unwrap()
            .contains("stale"),
        "{stale}"
    );
    // The desktop 0.165 composer request, including Codex-only options.
    let request = json!({"threadId":parent,"expectedTurnId":turn,"prompt":"next?",
        "effort":"low","threadSource":"composer_predictions",
        "turnTrigger":"composer_predictions","disableMcp":true,
        "responsesapiClientMetadata":{"prediction_trigger":"live"}});
    let sample = desktop.ok("thread/sample", request.clone()).await;
    assert_eq!(sample["sourceTurnId"], turn);
    assert!(sample["responseId"].is_string());
    let payload: Value = serde_json::from_str(sample["text"].as_str().unwrap()).unwrap();
    assert!(
        payload["suggestion"]
            .as_str()
            .unwrap()
            .contains("prediction 1")
    );

    let mut hanging = request.clone();
    hanging["prompt"] = json!("hang-prediction");
    desktop
        .send(json!({"id":"hanging-sample","method":"thread/sample","params":hanging}))
        .await;
    // Give the side question time to start, so cancel interrupts it.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let cancel = desktop
        .ok(
            "thread/sample/cancel",
            json!({"requestId":"hanging-sample"}),
        )
        .await;
    assert_eq!(cancel, json!({}));
    let cancelled = desktop
        .until(|message| message["id"] == "hanging-sample")
        .await;
    assert!(
        cancelled["error"]["message"]
            .as_str()
            .unwrap()
            .contains("cancelled"),
        "{cancelled}"
    );
    // The parent stays sampleable after a cancelled question.
    let again = desktop.ok("thread/sample", request).await;
    assert!(again["text"].as_str().unwrap().contains("prediction"));
    // A sample creates no thread, so the desktop hears only about the parent.
    for message in &desktop.history {
        if let Some(thread) = message["params"]["threadId"]
            .as_str()
            .or_else(|| message["params"]["thread"]["id"].as_str())
        {
            assert_eq!(thread, parent, "{message}");
        }
    }
}

#[tokio::test]
async fn desktop_prediction_requests_keep_the_claude_provider_and_route_the_temporary_fork() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let parent = desktop
        .thread(json!({"model":"fake-claude","sandbox":"danger-full-access"}))
        .await;
    assert!(
        desktop
            .turn(&parent, "fake-claude", "parent context")
            .await
            .get("error")
            .is_none()
    );
    let completed = desktop.completed(&parent).await;
    let resumed = desktop
        .ok(
            "thread/resume",
            json!({"threadId":parent,"excludeTurns":true}),
        )
        .await;
    assert_eq!(resumed["modelProvider"], "openai");
    assert_eq!(resumed["thread"]["modelProvider"], "anthropic");
    assert_eq!(resumed["claudexPredictionFork"], true);
    // A renderer may reuse the top-level compatibility provider on later resume.
    let echoed = desktop
        .ok(
            "thread/resume",
            json!({"threadId":parent,"modelProvider":resumed["modelProvider"],"excludeTurns":true}),
        )
        .await;
    assert_eq!(echoed["thread"]["modelProvider"], "anthropic");
    // Desktop 0.165 adds the MCP pruning flag and an unload delay.
    let fork = desktop
        .ok(
            "thread/fork",
            json!({
                "threadId":parent,"experimentalPredictionMode":true,"ephemeral":true,
                "excludeTurns":true,"threadSource":"composer_predictions",
                "experimentalDisablePredictionMcp":true,
                "config":{"thread_unload_delay_secs":10}
            }),
        )
        .await;
    let prediction = fork["thread"]["id"].as_str().unwrap();
    // The desktop supplies no model override on this special fork's turn.
    desktop
        .ok(
            "turn/start",
            json!({
                "threadId":prediction,"turnTrigger":"composer_predictions",
                "responsesapiClientMetadata":{
                    "prediction_source_thread_id":parent,
                    "prediction_source_turn_id":completed["params"]["turn"]["id"],
                    "prediction_trigger":"live"
                },
                "clientUserMessageId":uuid::Uuid::new_v4().to_string(),
                "input":[{"type":"text","text":"fork-info","text_elements":[]}],
                "effort":"low","outputSchema":null
            }),
        )
        .await;
    let result = desktop.completed(prediction).await;
    assert_eq!(result["params"]["turn"]["status"], "completed");
    let text = result["params"]["turn"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "agentMessage")
        .unwrap()["text"]
        .as_str()
        .unwrap();
    let payload: Value = serde_json::from_str(text).unwrap();
    assert!(
        payload["suggestion"]
            .as_str()
            .unwrap()
            .contains("prediction 1 prompt 1")
    );
    let retained = desktop
        .ok(
            "thread/read",
            json!({"threadId":parent,"includeTurns":true}),
        )
        .await;
    assert_eq!(retained["thread"]["modelProvider"], "anthropic");
    assert_eq!(retained["thread"]["turns"].as_array().unwrap().len(), 1);
    desktop
        .ok("thread/unsubscribe", json!({"threadId":prediction}))
        .await;
    assert!(
        desktop
            .call("thread/read", json!({"threadId":prediction}))
            .await
            .get("error")
            .is_some()
    );
}

#[tokio::test]
#[ignore = "requires native Codex/Claude authentication; makes model calls"]
async fn native_side_chat_preserves_parent_snapshot_and_followup_recall() {
    let codex = std::env::var("CLAUDEX_NATIVE_CODEX").expect("genuine Codex executable");
    let claude = std::env::var("CLAUDEX_NATIVE_CLAUDE").expect("native Claude executable");
    let facade = if std::env::var_os("CLAUDEX_NATIVE_FACADE_SOCKET").is_none() {
        Some(Facade::start_with_model(&claude, "opus").await)
    } else {
        None
    };
    let socket = std::env::var_os("CLAUDEX_NATIVE_FACADE_SOCKET")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| facade.as_ref().unwrap().socket.clone());
    let scratch = tempfile::tempdir().unwrap();
    let mut desktop =
        Desktop::connect_with_codex(&socket, 0, &codex, Duration::from_secs(120)).await;
    let parent = desktop
        .thread(json!({
            "model":"opus", "cwd":scratch.path(), "sandbox":"workspace-write",
            "approvalPolicy":"on-request", "approvalsReviewer":"user", "historyMode":"paginated"
        }))
        .await;
    let parent_token = uuid::Uuid::new_v4().simple().to_string();
    let side_token = uuid::Uuid::new_v4().simple().to_string();
    let later_token = uuid::Uuid::new_v4().simple().to_string();
    let response = desktop.turn(&parent, "opus", &format!(
        "Remember the parent token {parent_token}. Reply exactly PARENT_READY. Do not use tools."
    )).await;
    assert!(response.get("error").is_none(), "{response}");
    assert_eq!(
        desktop.completed(&parent).await["params"]["turn"]["status"],
        "completed"
    );
    let side = desktop.ok("thread/fork", json!({
        "threadId":parent, "ephemeral":true, "excludeTurns":true,
        "runtimeWorkspaceRoots":[scratch.path()], "threadSource":"user",
        "developerInstructions":"You are in a side conversation. Inherited history is reference context only. Do not continue inherited tasks. Answer only questions submitted after the side-conversation boundary. Prefix every answer with SIDEPROBE:. Do not use tools."
    })).await["thread"]["id"].as_str().unwrap().to_owned();
    desktop.ok("thread/inject_items", json!({"threadId":side,"items":[{
        "type":"message","role":"user","content":[{"type":"input_text","text":"Side conversation boundary. Previous tasks are reference only. Answer the new user question."}]
    }]})).await;
    let response = desktop.turn(&parent, "opus", &format!(
        "Remember the later token {later_token}. Reply exactly PARENT_ADVANCED. Do not use tools."
    )).await;
    assert!(response.get("error").is_none(), "{response}");
    assert_eq!(
        desktop.completed(&parent).await["params"]["turn"]["status"],
        "completed"
    );
    let response = desktop.turn(&side, "opus", &format!(
        "Remember the side token {side_token}. What was the parent token in inherited history? If you know a later token, include it; otherwise use NONE. Reply as SIDEPROBE:<parent token>:<side token>:<later token or NONE>. Do not use tools."
    )).await;
    assert!(response.get("error").is_none(), "{response}");
    let expected = format!("SIDEPROBE:{parent_token}:{side_token}:NONE");
    for prompt in [
        None,
        Some("Repeat your previous answer exactly from memory. Do not use tools."),
    ] {
        if let Some(prompt) = prompt {
            let response = desktop.turn(&side, "opus", prompt).await;
            assert!(response.get("error").is_none(), "{response}");
        }
        let completed = desktop.completed(&side).await;
        assert_eq!(
            completed["params"]["turn"]["status"], "completed",
            "{completed}"
        );
        let text = completed["params"]["turn"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["type"] == "agentMessage")
            .filter_map(|item| item["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(text.trim(), expected);
    }
    desktop
        .ok("thread/unsubscribe", json!({"threadId":side}))
        .await;
    desktop
        .ok("thread/archive", json!({"threadId":parent}))
        .await;
    eprintln!(
        "Verified native side chat: instruction override, boundary injection, parent snapshot and second-turn recall"
    );
}

#[tokio::test]
async fn delegated_interruption_and_scope_boundaries_are_enforced() {
    for (parent_model, harness, child_model) in [
        ("gpt-fake", "claude", "opus"),
        ("opus", "codex", "gpt-fake"),
    ] {
        let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
        let mut desktop = Desktop::connect(&facade.socket, 0).await;
        let parent = desktop
            .thread(json!({"model":parent_model,"approvalPolicy":"never"}))
            .await;
        desktop.turn(&parent,parent_model,&script(json!([
            {"tool":"spawn_agent","arguments":{"harness":harness,"model":child_model,"message":"hang"}}
        ]))).await;
        let completed = desktop.completed(&parent).await;
        let child = result(&reports(&completed)[0])["agent_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let unrelated = desktop.thread(json!({"model":parent_model})).await;
        desktop
            .turn(
                &unrelated,
                parent_model,
                &script(json!([
                    {"tool":"interrupt_agent","arguments":{"agent_id":child}}
                ])),
            )
            .await;
        let rejected = desktop.completed(&unrelated).await;
        let denied = reports(&rejected);
        assert_eq!(denied[0]["isError"], true);
        assert!(
            denied[0]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("outside the caller's task tree")
        );
        desktop
            .turn(
                &parent,
                parent_model,
                &script(json!([
                    {"tool":"interrupt_agent","arguments":{"agent_id":child}},
                    {"tool":"wait_agent","arguments":{"agent_id":child,"timeout_ms":10_000}}
                ])),
            )
            .await;
        let stopped = desktop.completed(&parent).await;
        assert_eq!(result(&reports(&stopped)[1])["status"], "interrupted");
    }
}

#[tokio::test]
async fn delegated_children_survive_router_reload_and_paginate_without_duplicates() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let parent = desktop
        .thread(json!({"model":"gpt-fake","approvalPolicy":"never"}))
        .await;
    let actions:Vec<Value>=(0..3).flat_map(|index|[
        json!({"tool":"spawn_agent","arguments":{"harness":"claude","model":"opus","message":"task","name":format!("Worker {index}")}}),
        json!({"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}})
    ]).collect();
    desktop
        .turn(&parent, "gpt-fake", &script(json!(actions)))
        .await;
    let completed = desktop.completed(&parent).await;
    let reports = reports(&completed);
    let expected: HashSet<String> = reports
        .iter()
        .step_by(2)
        .map(|report| result(report)["agent_id"].as_str().unwrap().to_owned())
        .collect();
    drop(desktop);
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let mut seen = HashSet::new();
    let mut cursor = Value::Null;
    for _ in 0..5 {
        let page = desktop
            .ok(
                "thread/list",
                json!({"parentThreadId":parent,"limit":1,"cursor":cursor}),
            )
            .await;
        assert_eq!(page["data"].as_array().unwrap().len(), 1);
        for id in ids(&page) {
            assert!(seen.insert(id), "duplicate child on a later page");
        }
        cursor = page["nextCursor"].clone();
        if cursor.is_null() {
            break;
        }
    }
    assert_eq!(seen, expected);
    let filtered = desktop
        .ok(
            "thread/list",
            json!({"parentThreadId":parent,"sourceKinds":["appServer"]}),
        )
        .await;
    assert!(filtered["data"].as_array().unwrap().is_empty());
    for child in expected {
        let read = desktop
            .ok("thread/read", json!({"threadId":child,"includeTurns":true}))
            .await;
        assert_eq!(read["thread"]["parentThreadId"], parent);
    }
}

#[tokio::test]
async fn router_disconnect_and_crash_recovery_stop_native_claude_children() {
    for graceful in [true, false] {
        let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
        let mut desktop = Desktop::connect(&facade.socket, 0).await;
        let parent = desktop
            .thread(json!({"model":"gpt-fake","approvalPolicy":"never"}))
            .await;
        desktop.turn(&parent,"gpt-fake",&script(json!([
            {"tool":"spawn_agent","arguments":{"harness":"claude","model":"opus","message":"hang"}}
        ]))).await;
        let completed = desktop.completed(&parent).await;
        let child = result(&reports(&completed)[0])["agent_id"]
            .as_str()
            .unwrap()
            .to_owned();
        if graceful {
            let Desktop {
                mut _child, input, ..
            } = desktop;
            drop(input);
            tokio::time::timeout(DEADLINE, _child.wait())
                .await
                .unwrap()
                .unwrap();
        } else {
            desktop.crash_router().await;
            drop(desktop);
        }
        let mut desktop = Desktop::connect(&facade.socket, 0).await;
        if !graceful {
            desktop.completed(&child).await;
        }
        let read = desktop
            .ok("thread/read", json!({"threadId":child,"includeTurns":true}))
            .await;
        assert_eq!(
            read["thread"]["turns"].as_array().unwrap().last().unwrap()["status"],
            "interrupted"
        );
        assert_eq!(read["thread"]["parentThreadId"], parent);
    }
}

#[tokio::test]
async fn nested_delegation_keeps_one_tree_across_both_harnesses() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let parent = desktop
        .thread(json!({"model":"gpt-fake","approvalPolicy":"never"}))
        .await;
    let nested = script(json!([
        {"tool":"spawn_agent","arguments":{"harness":"codex","model":"gpt-fake","message":"nested task"}},
        {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}}
    ]));
    desktop.turn(&parent,"gpt-fake",&script(json!([
        {"tool":"spawn_agent","arguments":{"harness":"claude","model":"opus","message":nested}},
        {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}},
        {"tool":"list_agents","arguments":{}}
    ]))).await;
    let completed = desktop.completed(&parent).await;
    let reports = reports(&completed);
    let outer = result(&reports[0])["agent_id"].clone();
    assert_eq!(result(&reports[2])["agents"].as_array().unwrap().len(), 2);
    let all = desktop
        .ok("thread/list", json!({"ancestorThreadId":parent}))
        .await;
    assert_eq!(all["data"].as_array().unwrap().len(), 2);
    assert!(
        all["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|child| child["parentThreadId"] == outer)
    );
    let direct = desktop
        .ok("thread/list", json!({"parentThreadId":parent}))
        .await;
    assert_eq!(direct["data"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn ssh_proxy_lists_both_backends_and_runs_claude_over_websocket() {
    use tokio_tungstenite::tungstenite::Message;
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut command = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"));
    for name in DEPLOYMENT_ENV {
        command.env_remove(name);
    }
    let mut child = command
        .arg("--sock")
        .arg(&facade.socket)
        .args([
            "route",
            "--codex",
            env!("CARGO_BIN_EXE_fake-codex"),
            "--",
            "app-server",
            "proxy",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let stream = tokio::io::join(child.stdout.take().unwrap(), child.stdin.take().unwrap());
    let (mut socket, _) = tokio::time::timeout(
        DEADLINE,
        tokio_tungstenite::client_async("ws://codex-app-server/rpc", stream),
    )
    .await
    .unwrap()
    .expect("desktop SSH WebSocket handshake");
    async fn rpc<S>(
        socket: &mut tokio_tungstenite::WebSocketStream<S>,
        id: u64,
        method: &str,
        params: Value,
    ) -> Value
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        socket
            .send(Message::Text(
                json!({"id":id,"method":method,"params":params})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        loop {
            let message = tokio::time::timeout(DEADLINE, socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let Message::Text(text) = message else {
                continue;
            };
            let value: Value = serde_json::from_str(&text).unwrap();
            if value["id"] == id {
                assert!(value.get("error").is_none(), "{value}");
                return value["result"].clone();
            }
        }
    }
    rpc(&mut socket, 1, "initialize", json!({"clientInfo":{"name":"codex_desktop","version":"1"},"capabilities":{"experimentalApi":true}})).await;
    socket
        .send(Message::Text(
            json!({"method":"initialized"}).to_string().into(),
        ))
        .await
        .unwrap();
    let models = rpc(
        &mut socket,
        2,
        "model/list",
        json!({"includeHidden":true,"limit":100}),
    )
    .await;
    let models = models["data"].as_array().unwrap();
    assert!(models.iter().any(|model| model["id"] == "gpt-fake"));
    assert!(models.iter().any(|model| model["id"] == "opus"));
    let thread = rpc(
        &mut socket,
        3,
        "thread/start",
        json!({"model":"opus","sandbox":"workspace-write","approvalPolicy":"on-request"}),
    )
    .await;
    let id = &thread["thread"]["id"];
    rpc(&mut socket, 4, "turn/start", json!({"threadId":id,"input":[{"type":"text","text":"hello through SSH","text_elements":[]}]})).await;
    loop {
        let message = tokio::time::timeout(DEADLINE, socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Message::Text(text) = message else {
            continue;
        };
        let value: Value = serde_json::from_str(&text).unwrap();
        if value["method"] == "turn/completed" {
            assert_eq!(value["params"]["turn"]["status"], "completed", "{value}");
            break;
        }
    }
    socket.close(None).await.unwrap();
}

#[tokio::test]
async fn native_ultrafast_capability_and_turn_override_survive_routing() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;

    let merged = desktop.ok("model/list", json!({})).await;
    assert_eq!(merged["data"][0]["serviceTiers"][0]["id"], "ultrafast");
    assert!(
        merged["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["id"] == "opus")
    );

    let native = desktop
        .ok("model/list", json!({"modelProvider":"openai"}))
        .await;
    assert_eq!(
        native["data"],
        json!(merged["data"].as_array().unwrap()[..2])
    );

    let thread = desktop.thread(json!({"model":"gpt-fake"})).await;
    desktop
        .ok(
            "turn/start",
            json!({
                "threadId":thread,"serviceTierForTurn":"ultrafast",
                "input":[{"type":"text","text":"report service tier","text_elements":[]}]
            }),
        )
        .await;
    let completed = desktop.completed(&thread).await;
    assert_eq!(completed["params"]["turn"]["items"][0]["text"], "ultrafast");
}

#[tokio::test]
async fn provider_scoped_model_lists_reach_the_backend_that_owns_the_provider() {
    let mut facade = Facade::start_with_local_acp().await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    // The facade connections are up once the combined catalog lists both.
    desktop.ok("model/list", json!({})).await;
    let names = |page: &Value| -> Vec<String> {
        page["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|model| model["id"].as_str().unwrap().to_owned())
            .collect()
    };
    let query = |provider: &str| json!({"modelProvider":provider,"includeHidden":true,"limit":100});

    // The desktop scopes an existing chat's picker to its thread's provider.
    let claude = desktop.ok("model/list", query("anthropic")).await;
    let claude_names = names(&claude);
    for model in ["opus", "sonnet"] {
        assert!(claude_names.contains(&model.to_owned()), "{claude_names:?}");
    }
    for other in ["default", "gpt-fake", "fake-a", "fake-b"] {
        assert!(
            !claude_names.contains(&other.to_owned()),
            "{claude_names:?}"
        );
    }
    let default = claude["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["isDefault"] == true)
        .unwrap();
    assert_eq!(default["id"], "opus");

    // Only configured ACP models are selectable, with the primary model as default.
    let acp = desktop.ok("model/list", query("fake-provider")).await;
    let mut acp_names = names(&acp);
    acp_names.sort();
    assert_eq!(acp_names, ["fake-a", "fake-b"]);
    for model in acp["data"].as_array().unwrap() {
        assert_eq!(model["isDefault"], model["id"] == "fake-b", "{model}");
    }

    for provider in ["anthropic", "fake-provider"] {
        let failure = desktop
            .call("model/list", json!({"modelProvider":provider,"limit":0}))
            .await;
        assert_eq!(failure["error"]["code"], -32602, "{failure}");
        assert!(failure.get("result").is_none(), "{failure}");
    }

    // Every other provider belongs to Codex.
    let native = names(&desktop.ok("model/list", query("openai")).await);
    assert_eq!(native, ["gpt-fake", "gpt-fake-mini"]);

    // Let the service stop its ACP worker.
    let pid = facade._child.id().unwrap();
    // SAFETY: the live child PID belongs to this unreaped process.
    assert_eq!(unsafe { libc::kill(pid as i32, libc::SIGTERM) }, 0);
    tokio::time::timeout(DEADLINE, facade._child.wait())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn one_picker_and_one_sidebar_span_codex_and_claude() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;

    let models = desktop.ok("model/list", json!({})).await;
    let names: Vec<&str> = models["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert_eq!(&names[..2], ["gpt-fake", "gpt-fake-mini"]);
    for claude in ["fake-claude", "opus", "sonnet"] {
        assert!(names.contains(&claude), "{names:?}");
    }
    let defaults: Vec<_> = models["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["isDefault"] == true)
        .collect();
    assert_eq!(defaults.len(), 1);
    assert_eq!(defaults[0]["id"], "gpt-fake");

    let desktop_mcp = json!({
        "mcp_servers.artifact_session":{"command":"/desktop/node_repl", "enabled":true},
        "mcp_servers.cua_repl":{"command":"/desktop/node", "enabled":true}
    });
    let codex = desktop
        .thread(json!({"model": "gpt-fake", "config":desktop_mcp}))
        .await;
    let reply = desktop.turn(&codex, "gpt-fake", "hello codex").await;
    assert!(reply.get("error").is_none(), "{reply}");
    desktop.completed(&codex).await;
    let state = desktop.ok("fake/state", json!({})).await;
    for (key, value) in desktop_mcp.as_object().unwrap() {
        assert_eq!(state["threadConfigs"][&codex][key], *value);
    }

    let claude = desktop
        .thread(json!({"model": "opus", "config": {
            "mcp_servers.node_repl":{"command":"node", "args":["desktop-repl.js"]},
            "mcp_servers.artifact_session":{"command":"/desktop/node_repl", "env":{"NODE_REPL_TRUSTED_SERVICES":"{\"artifact_session\":\"@oai/artifact-tool/artifact-session/service\"}"}},
            "mcp_servers.cua_repl":{"command":"/desktop/node", "args":["/desktop/@oai/cua-repl/bin/cua-repl.mjs"], "enabled":true},
            "memories.dual_write":true, "memories.version":"v2"
        }}))
        .await;
    assert_ne!(claude, codex);
    let reply = desktop.turn(&claude, "opus", "hello claude").await;
    assert!(reply.get("error").is_none(), "{reply}");
    desktop.completed(&claude).await;
    let read = desktop.ok("thread/read", json!({"threadId": claude})).await;
    assert_eq!(read["thread"]["modelProvider"], "anthropic");

    // Each backend keeps its own threads; switching families needs a new chat.
    let wrong = desktop.turn(&claude, "gpt-fake", "again").await;
    assert!(
        wrong["error"]["message"]
            .as_str()
            .unwrap()
            .contains("runs on Claude")
    );
    let wrong = desktop.turn(&codex, "opus", "again").await;
    assert!(
        wrong["error"]["message"]
            .as_str()
            .unwrap()
            .contains("runs on Codex")
    );

    let listed = desktop
        .ok("thread/list", json!({"limit": 50, "sortKey": "created_at"}))
        .await;
    let listed = ids(&listed);
    assert!(
        listed.contains(&codex) && listed.contains(&claude),
        "{listed:?}"
    );
    // Codex threads created later sort ahead of the Claude thread.
    assert!(listed.iter().position(|id| *id == codex) < listed.iter().position(|id| *id == claude));
    let loaded = desktop.ok("thread/loaded/list", json!({})).await;
    let loaded: Vec<&str> = loaded["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(loaded.contains(&codex.as_str()) && loaded.contains(&claude.as_str()));
}

#[tokio::test]
async fn selected_local_acp_models_join_the_picker_and_adopt_only_their_own_chats() {
    let mut facade = Facade::start_with_local_acp().await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let catalog = desktop.ok("model/list", json!({})).await;
    let models = catalog["data"].as_array().unwrap();
    for model in ["gpt-fake", "opus", "fake-a", "fake-b"] {
        assert_eq!(
            models.iter().filter(|entry| entry["id"] == model).count(),
            1
        );
    }
    assert!(models.iter().all(|entry| entry["id"] != "fake-c"));
    assert!(
        models
            .iter()
            .any(|entry| entry["id"] == "fake-b" && entry["displayName"] == "Fake B")
    );
    let defaults: Vec<_> = models
        .iter()
        .filter(|entry| entry["isDefault"] == true)
        .collect();
    assert_eq!(defaults.len(), 1);
    assert_eq!(defaults[0]["id"], "gpt-fake");

    let claude = desktop.thread(json!({"model":"opus"})).await;
    let denied = desktop
        .call(
            "thread/start",
            json!({"model":"fake-b","permissions":":workspace"}),
        )
        .await;
    assert_eq!(
        denied["error"]["message"],
        "Local ACP chats require Full access"
    );
    let prewarmed = desktop
        .thread(json!({"sandbox":"workspace-write","approvalPolicy":"on-request"}))
        .await;
    // A permission selection can be acknowledged by Codex before the model
    // changes. The adoption must respect that selection over stale prewarm data.
    desktop
        .ok(
            "thread/settings/update",
            json!({
                "threadId":prewarmed, "permissions":":danger-full-access"
            }),
        )
        .await;
    desktop
        .ok(
            "thread/settings/update",
            json!({
                "threadId":prewarmed, "model":"fake-b"
            }),
        )
        .await;
    let turn = desktop.turn(&prewarmed, "fake-b", "history").await;
    assert!(turn.get("error").is_none(), "{turn}");
    let reply = desktop
        .until(|frame| {
            frame["method"] == "item/completed"
                && frame["params"]["threadId"] == prewarmed
                && frame["params"]["item"]["type"] == "agentMessage"
        })
        .await;
    assert!(
        reply["params"]["item"]["text"]
            .as_str()
            .unwrap()
            .contains("model=fake-b"),
        "{reply}"
    );
    let completed = desktop.completed(&prewarmed).await;
    assert_eq!(
        completed["params"]["turn"]["status"], "completed",
        "{completed}"
    );
    let native = desktop.ok("fake/state", json!({})).await;
    assert_eq!(native["unsubscribed"], json!([prewarmed]));
    assert!(
        !native["requests"]
            .as_array()
            .unwrap()
            .contains(&json!("turn/start"))
    );

    let wrong = desktop.turn(&prewarmed, "opus", "stay on ACP").await;
    assert!(
        wrong["error"]["message"]
            .as_str()
            .unwrap()
            .contains("start a new chat")
    );
    let wrong = desktop.turn(&claude, "fake-b", "stay on Claude").await;
    assert!(
        wrong["error"]["message"]
            .as_str()
            .unwrap()
            .contains("runs on Claude")
    );

    // A second picked model can use the same ACP provider in the same chat.
    let turn = desktop.turn(&prewarmed, "fake-a", "history").await;
    assert!(turn.get("error").is_none(), "{turn}");
    let completed = desktop.completed(&prewarmed).await;
    assert_eq!(
        completed["params"]["turn"]["status"], "completed",
        "{completed}"
    );
    let reply = desktop
        .until(|frame| {
            frame["method"] == "item/completed"
                && frame["params"]["threadId"] == prewarmed
                && frame["params"]["turnId"] == turn["result"]["turn"]["id"]
                && frame["params"]["item"]["type"] == "agentMessage"
        })
        .await;
    assert!(
        reply["params"]["item"]["text"]
            .as_str()
            .unwrap()
            .contains("model=fake-a"),
        "{reply}"
    );

    // A new desktop connection has no memory of the adoption. Resolve the
    // persisted thread's owner before consulting any merged thread list.
    let mut reopened = Desktop::connect(&facade.socket, 0).await;
    let thread = reopened
        .ok("thread/read", json!({"threadId":prewarmed}))
        .await;
    assert_eq!(thread["thread"]["modelProvider"], "fake-provider");
    assert_eq!(thread["thread"]["model"], "fake-a");
    let threads = reopened.ok("thread/list", json!({})).await;
    let threads = ids(&threads);
    assert_eq!(threads.iter().filter(|id| **id == prewarmed).count(), 1);
    assert!(threads.contains(&claude));

    // Stop the parent as launchd would, so the supervised worker also exits.
    let pid = facade._child.id().unwrap();
    assert_eq!(unsafe { libc::kill(pid as i32, libc::SIGTERM) }, 0);
    assert!(
        tokio::time::timeout(DEADLINE, facade._child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    let socket = claude_codex_server::local_acp::LocalAcp::socket(facade._state.path());
    assert!(tokio::net::UnixStream::connect(socket).await.is_err());
}

#[tokio::test]
async fn a_prewarmed_codex_thread_moves_to_claude_on_its_first_claude_turn() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    // The desktop prewarms without a model, then submits with the picked one.
    let thread = desktop
        .thread(
            json!({"cwd": "/tmp", "sandbox": "danger-full-access", "config": {
                "mcp_servers.node_repl":{"command":"node", "args":["desktop-repl.js"]},
                "mcp_servers.artifact_session":{"command":"", "enabled":false},
                "mcp_servers.cua_repl":{"command":"/desktop/electron", "enabled":false},
                "memories.dual_write":true, "memories.version":"v2"
            }}),
        )
        .await;
    let reply = desktop.turn(&thread, "sonnet", "first message").await;
    assert!(reply.get("error").is_none(), "{reply}");
    let completed = desktop.completed(&thread).await;
    assert_eq!(
        completed["params"]["turn"]["status"], "completed",
        "{completed}"
    );
    let read = desktop.ok("thread/read", json!({"threadId": thread})).await;
    assert_eq!(read["thread"]["id"], thread.as_str());
    assert_eq!(read["thread"]["modelProvider"], "anthropic");
    assert_eq!(read["thread"]["model"], "sonnet");
    let state = desktop.ok("fake/state", json!({})).await;
    assert_eq!(state["unsubscribed"], json!([thread]));
    assert!(
        !state["requests"]
            .as_array()
            .unwrap()
            .contains(&json!("turn/start")),
        "the Claude turn must not reach Codex: {state}"
    );
    // A second router learns ownership from the facade itself.
    let mut fresh = Desktop::connect(&facade.socket, 0).await;
    let read = fresh.ok("thread/read", json!({"threadId": thread})).await;
    assert_eq!(read["thread"]["modelProvider"], "anthropic");
    fresh
        .ok(
            "thread/resume",
            json!({"threadId": thread, "model": "sonnet"}),
        )
        .await;
    let followup = fresh.turn(&thread, "sonnet", "second").await;
    assert!(followup.get("error").is_none(), "{followup}");
    fresh.completed(&thread).await;
}

#[tokio::test]
async fn merged_pages_keep_every_thread_exactly_once_in_order() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 7).await;
    let mut claude = HashSet::new();
    for n in 0..3 {
        let thread = desktop.thread(json!({"model": "haiku"})).await;
        desktop.turn(&thread, "haiku", &format!("c{n}")).await;
        desktop.completed(&thread).await;
        claude.insert(thread);
    }
    let codex_new = desktop.thread(json!({})).await;
    desktop.turn(&codex_new, "gpt-fake", "x").await;
    desktop.completed(&codex_new).await;

    for method in ["thread/list", "thread/search"] {
        let mut cursor = Value::Null;
        let mut seen = vec![];
        let mut created = vec![];
        loop {
            let mut params = json!({"limit": 3, "cursor": cursor, "sortKey": "created_at"});
            if method == "thread/search" {
                params["searchTerm"] = json!("");
            }
            let page = desktop.ok(method, params).await;
            for item in page["data"].as_array().unwrap() {
                let thread = if method == "thread/search" {
                    assert!(item["snippet"].is_string(), "{item}");
                    &item["thread"]
                } else {
                    item
                };
                created.push(thread["createdAt"].as_i64().unwrap());
            }
            seen.extend(ids(&page));
            assert_eq!(page["backwardsCursor"], Value::Null);
            cursor = page["nextCursor"].clone();
            if cursor.is_null() {
                break;
            }
        }
        let unique: HashSet<_> = seen.iter().cloned().collect();
        assert_eq!(unique.len(), seen.len(), "{method} duplicated: {seen:?}");
        assert_eq!(seen.len(), 7 + 1 + 3, "{method}: {seen:?}");
        assert!(claude.is_subset(&unique));
        assert!(created.windows(2).all(|w| w[0] >= w[1]), "{created:?}");
    }
}

/// Every source kind, as the desktop lists a section's members.
const ALL_SOURCE_KINDS: &[&str] = &[
    "cli",
    "vscode",
    "exec",
    "appServer",
    "subAgent",
    "subAgentReview",
    "subAgentCompact",
    "subAgentThreadSpawn",
    "subAgentOther",
    "unknown",
];

/// Thread ids in a section, paging the way the desktop does.
async fn section_members(desktop: &mut Desktop, section: &str, limit: u64) -> Vec<String> {
    let mut members = vec![];
    let mut cursor = Value::Null;
    loop {
        let page = desktop
            .ok(
                "thread/list",
                json!({"cursor":cursor,"limit":limit,"modelProviders":[],"sectionId":section,
                    "sortKey":"section_position","sourceKinds":ALL_SOURCE_KINDS,"useStateDbOnly":true}),
            )
            .await;
        members.extend(ids(&page));
        cursor = page["nextCursor"].clone();
        if cursor.is_null() {
            return members;
        }
    }
}

#[tokio::test]
async fn claude_threads_share_codex_sections_through_the_router() {
    const PINNED: &str = "01984de2-8f74-7c91-a3b2-5c5e937cf318";
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let codex = desktop.thread(json!({"model": "gpt-fake"})).await;
    let claude = desktop.thread(json!({"model": "opus"})).await;
    let second = desktop.thread(json!({"model": "opus"})).await;
    desktop
        .ok(
            "thread/section/move",
            json!({"threadId": codex, "sectionId": PINNED, "beforeThreadId": null}),
        )
        .await;
    // Neither backend can order its threads around the other's: a Claude
    // thread placed before a Codex one follows Codex's members.
    desktop
        .ok(
            "thread/section/move",
            json!({"threadId": claude, "sectionId": PINNED, "beforeThreadId": codex}),
        )
        .await;
    desktop
        .ok(
            "thread/section/move",
            json!({"threadId": second, "sectionId": PINNED, "beforeThreadId": claude}),
        )
        .await;
    let read = desktop.ok("thread/read", json!({"threadId": claude})).await;
    assert_eq!(
        read["thread"]["section"],
        json!({"id": PINNED, "name": "Pinned", "appearance": null})
    );
    let pinned = section_members(&mut desktop, PINNED, 100).await;
    assert_eq!(pinned, [codex.clone(), second.clone(), claude.clone()]);
    assert_eq!(section_members(&mut desktop, PINNED, 1).await, pinned);
    // Codex's sections page two at a time; the router reads every page to
    // find a destination.
    desktop
        .ok("threadSection/create", json!({"name": "Someday"}))
        .await;
    let work = desktop
        .ok("threadSection/create", json!({"name": "Work"}))
        .await["section"]
        .clone();
    let work_id = work["id"].as_str().unwrap();
    desktop
        .ok(
            "thread/section/move",
            json!({"threadId": second, "sectionId": work_id}),
        )
        .await;
    assert_eq!(
        section_members(&mut desktop, PINNED, 100).await,
        [codex.clone(), claude.clone()]
    );
    assert_eq!(
        section_members(&mut desktop, work_id, 100).await.as_slice(),
        std::slice::from_ref(&second)
    );
    let missing = desktop
        .call(
            "thread/section/move",
            json!({"threadId": claude, "sectionId": "0199ffff-0000-7000-8000-000000000000"}),
        )
        .await;
    assert!(
        missing["error"]["message"]
            .as_str()
            .unwrap()
            .contains("does not exist"),
        "{missing}"
    );
    // Codex renames and deletions reach the Claude thread's copy.
    desktop
        .ok(
            "threadSection/update",
            json!({"sectionId": work_id, "name": "Renamed"}),
        )
        .await;
    let read = desktop.ok("thread/read", json!({"threadId": second})).await;
    assert_eq!(read["thread"]["section"]["name"], "Renamed");
    desktop
        .ok("threadSection/delete", json!({"sectionId": work_id}))
        .await;
    let read = desktop.ok("thread/read", json!({"threadId": second})).await;
    assert_eq!(read["thread"]["section"], Value::Null);
    let unsectioned = desktop
        .ok("thread/list", json!({"limit": 100, "sectionId": null}))
        .await;
    assert!(ids(&unsectioned).contains(&second), "{unsectioned}");
    assert!(!ids(&unsectioned).contains(&claude), "{unsectioned}");
    desktop
        .ok(
            "thread/section/move",
            json!({"threadId": claude, "sectionId": null}),
        )
        .await;
    assert_eq!(section_members(&mut desktop, PINNED, 100).await, [codex]);
}

#[tokio::test]
async fn deleting_a_claude_thread_forgets_it_and_apps_go_to_codex() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let codex = desktop.thread(json!({"model": "gpt-fake"})).await;
    let claude = desktop.thread(json!({"model": "opus"})).await;
    // Apps do not depend on the chat's model; Codex never sees the Claude id.
    desktop
        .ok("app/list", json!({"threadId": claude, "limit": 10}))
        .await;
    desktop
        .ok(
            "app/read",
            json!({"threadId": claude, "appIds": ["fixture"]}),
        )
        .await;
    desktop
        .ok("app/installed", json!({"threadId": codex}))
        .await;
    let state = desktop.ok("fake/state", json!({})).await;
    assert_eq!(
        state["apps"],
        json!([{"limit": 10}, {"appIds": ["fixture"]}, {"threadId": codex}])
    );

    let goal = desktop
        .ok("thread/goal/get", json!({"threadId": claude}))
        .await;
    assert_eq!(goal, json!({"goal": null}));
    desktop
        .ok("thread/delete", json!({"threadId": claude}))
        .await;
    desktop
        .until(|v| v["method"] == "thread/deleted" && v["params"]["threadId"] == claude)
        .await;
    let listed = desktop.ok("thread/list", json!({"limit": 100})).await;
    assert_eq!(ids(&listed).as_slice(), std::slice::from_ref(&codex));
    // The router no longer assumes Claude owns the id: it asks the facade,
    // which no longer knows it, and then genuine Codex.
    let read = desktop
        .call("thread/read", json!({"threadId": claude}))
        .await;
    assert!(read["error"].is_object(), "{read}");
    let state = desktop.ok("fake/state", json!({})).await;
    assert!(
        state["requests"]
            .as_array()
            .unwrap()
            .contains(&json!("thread/read")),
        "{state}"
    );
}

#[tokio::test]
async fn claude_server_requests_round_trip_with_namespaced_ids() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_approval-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let thread = desktop
        .thread(json!({"model": "fake-claude", "sandbox": "danger-full-access"}))
        .await;
    let reply = desktop.turn(&thread, "fake-claude", "echo routed").await;
    assert!(reply.get("error").is_none(), "{reply}");
    let request = desktop
        .until(|v| v["method"] == "item/commandExecution/requestApproval")
        .await;
    assert!(request["id"].as_str().unwrap().starts_with("claudex:"));
    desktop
        .send(json!({"id": request["id"], "result": {"decision": "accept"}}))
        .await;
    let resolved = desktop
        .until(|v| v["method"] == "serverRequest/resolved")
        .await;
    assert_eq!(resolved["params"]["requestId"], request["id"]);
    desktop.completed(&thread).await;
}

#[tokio::test]
async fn losing_the_facade_ends_its_running_turns_for_the_desktop() {
    let mut facade = Facade::start(env!("CARGO_BIN_EXE_approval-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let thread = desktop
        .thread(json!({"model": "fake-claude", "sandbox": "danger-full-access"}))
        .await;
    let reply = desktop.turn(&thread, "fake-claude", "echo routed").await;
    assert!(reply.get("error").is_none(), "{reply}");
    let request = desktop
        .until(|v| v["method"] == "item/commandExecution/requestApproval")
        .await;
    // The desktop's own connection stays up across a facade crash or restart,
    // so the router must end what the desktop is showing as running.
    facade._child.kill().await.unwrap();
    let resolved = desktop
        .until(|v| v["method"] == "serverRequest/resolved")
        .await;
    assert_eq!(resolved["params"]["requestId"], request["id"]);
    assert_eq!(resolved["params"]["threadId"], thread);
    let completed = desktop.completed(&thread).await;
    let turn = &completed["params"]["turn"];
    assert_eq!(turn["id"], reply["result"]["turn"]["id"]);
    assert_eq!(turn["status"], "interrupted");
    assert!(turn["error"]["message"].is_string(), "{completed}");
    desktop
        .until(|v| {
            v["method"] == "thread/status/changed"
                && v["params"]["threadId"] == thread
                && v["params"]["status"]["type"] == "idle"
        })
        .await;
}

#[tokio::test]
async fn without_the_facade_the_router_is_plain_codex() {
    let missing = tempfile::tempdir().unwrap();
    let mut desktop = Desktop::connect(&missing.path().join("absent.sock"), 2).await;
    let models = desktop.ok("model/list", json!({})).await;
    assert_eq!(models["data"].as_array().unwrap().len(), 2);
    let rejected = desktop.call("thread/start", json!({"model":"opus"})).await;
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap()
            .contains("reconnecting")
    );
    let thread = desktop.thread(json!({"model": "gpt-fake"})).await;
    let read = desktop.ok("thread/read", json!({"threadId": thread})).await;
    assert_eq!(read["thread"]["modelProvider"], "openai");
    let listed = desktop.ok("thread/list", json!({"limit": 1})).await;
    assert_eq!(listed["nextCursor"], "fake:1");
    assert_eq!(listed["backwardsCursor"], "fake-backwards");
}

#[tokio::test]
async fn desktop_creation_metadata_settings_and_collaboration_mode_reach_claude() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    // The desktop may prewarm on Codex, assign a project, then pick Claude
    // through settings before submitting a collaboration-mode turn.
    let thread = desktop
        .thread(json!({"cwd":"/tmp","sandbox":"danger-full-access"}))
        .await;
    desktop.ok("thread/metadata/update", json!({"threadId":thread,
        "projectId":"desktop-project", "gitInfo":{"branch":"main","sha":"abc","originUrl":"https://example.test/repo"},
        "daybreakEnabled":false})).await;
    desktop.ok("thread/settings/update", json!({"threadId":thread,
        "collaborationMode":{"mode":"default","settings":{"model":"opus","reasoning_effort":"medium","developer_instructions":null}}})).await;
    let read = desktop.ok("thread/read", json!({"threadId":thread})).await;
    assert_eq!(read["thread"]["modelProvider"], "anthropic");
    assert_eq!(read["thread"]["projectId"], "desktop-project");
    assert_eq!(read["thread"]["gitInfo"]["branch"], "main");
    let turn = desktop.call("turn/start",json!({"threadId":thread,
        "model":null, "input":[{"type":"text","text":"desktop first message","text_elements":[]}],
        "collaborationMode":{"mode":"default","settings":{"model":"opus","reasoning_effort":"medium","developer_instructions":null}}})).await;
    assert!(turn.get("error").is_none(), "{turn}");
    desktop.completed(&thread).await;
    // Metadata patches and subsequent settings persist without erasing omitted fields.
    desktop
        .ok(
            "thread/metadata/update",
            json!({"threadId":thread,"gitInfo":{"branch":"feature","sha":null}}),
        )
        .await;
    desktop
        .ok(
            "thread/settings/update",
            json!({"threadId":thread,"model":"sonnet","effort":"high"}),
        )
        .await;
    let mut resumed = Desktop::connect(&facade.socket, 0).await;
    let read = resumed
        .ok("thread/resume", json!({"threadId":thread}))
        .await;
    assert_eq!(read["thread"]["projectId"], "desktop-project");
    assert_eq!(
        read["thread"]["gitInfo"],
        json!({"branch":"feature","sha":null,"originUrl":"https://example.test/repo"})
    );
    assert_eq!(read["model"], "sonnet");
    assert_eq!(read["reasoningEffort"], "high");
    let listed = desktop.ok("thread/list", json!({"limit":100})).await;
    let copies: Vec<_> = listed["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["id"] == thread)
        .collect();
    assert_eq!(
        copies.len(),
        1,
        "Both prewarm copies appeared in the desktop list: {listed}"
    );
    assert_eq!(copies[0]["modelProvider"], "anthropic");
    let state = desktop.ok("fake/state", json!({})).await;
    assert!(
        !state["requests"]
            .as_array()
            .unwrap()
            .contains(&json!("turn/start")),
        "Claude turn reached Codex: {state}"
    );
}

#[tokio::test]
async fn desktop_read_only_helper_does_not_receive_delegation_tools() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let thread = desktop
        .thread(json!({"model":"opus", "ephemeral":true,
        "permissions":":read-only", "approvalPolicy":"never",
        "allowProviderModelFallback":true, "dynamicTools":null}))
        .await;
    let reply = desktop
        .call(
            "turn/start",
            json!({"threadId":thread,
        "permissions":":read-only", "approvalPolicy":"never",
        "input":[{"type":"text","text":"helper","text_elements":[]}]}),
        )
        .await;
    assert!(reply.get("error").is_none(), "{reply}");
    let completed = desktop.completed(&thread).await;
    assert_eq!(
        completed["params"]["turn"]["status"], "completed",
        "{completed}"
    );
    let read = desktop
        .ok(
            "thread/read",
            json!({"threadId":thread,"includeTurns":true}),
        )
        .await;
    assert!(read.to_string().contains("Echo: helper"), "{read}");
    let state = desktop.ok("fake/state", json!({})).await;
    assert!(
        !state["requests"]
            .as_array()
            .unwrap()
            .contains(&json!("turn/start")),
        "{state}"
    );
}

#[tokio::test]
async fn current_desktop_projectless_task_submits_first_and_followup_messages() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let cwd = tempfile::tempdir().unwrap();
    let thread=desktop.thread(json!({"cwd":cwd.path(),"model":"opus",
        "daybreakEnabled":false,"daybreakProgramPropagationEnrolled":true,
        "permissions":":workspace", "approvalPolicy":"on-request","approvalsReviewer":"auto_review",
        "historyMode":"paginated","threadSource":"user","dynamicTools":[],"runtimeWorkspaceRoots":[cwd.path()],
        "config":{"mcp_servers.node_repl":{"command":"node","args":["desktop-repl.js"]},
            "mcp_servers.artifact_session":{"command":"", "enabled":false},
            "mcp_servers.cua_repl":{"command":"/desktop/electron", "enabled":false},
            "memories.version":"v2","memories.dual_write":true,"features.request_permissions_tool":true}})).await;
    // main's project-assignment write must complete before the desktop sends
    // the first message, including projectless tasks (empty projectId).
    desktop
        .ok(
            "thread/metadata/update",
            json!({"threadId":thread,"projectId":""}),
        )
        .await;
    let fixture: Value = serde_json::from_str(include_str!("fixtures/desktop-turn.json")).unwrap();
    for _ in 0..2 {
        let mut request = fixture["request"].clone();
        request["threadId"] = json!(thread);
        request["cwd"] = json!(cwd.path());
        request["runtimeWorkspaceRoots"] = json!([cwd.path()]);
        request["sandboxPolicy"]["writableRoots"] = json!([cwd.path()]);
        let reply = desktop.call("turn/start", request).await;
        assert!(reply.get("error").is_none(), "{reply}");
        let completed = desktop.completed(&thread).await;
        assert_eq!(
            completed["params"]["turn"]["status"], "completed",
            "{completed}"
        );
    }
    let read = desktop
        .ok(
            "thread/read",
            json!({"threadId":thread,"includeTurns":true}),
        )
        .await;
    assert_eq!(read["thread"]["turns"].as_array().unwrap().len(), 2);
    assert_eq!(read["thread"]["modelProvider"], "anthropic");
    assert_eq!(read["thread"]["daybreakEnabled"], false);
    let state = desktop.ok("fake/state", json!({})).await;
    assert!(
        !state["requests"]
            .as_array()
            .unwrap()
            .contains(&json!("turn/start")),
        "{state}"
    );
}

#[tokio::test]
async fn model_less_prewarm_uses_the_effective_saved_claude_default() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    desktop
        .ok(
            "config/batchWrite",
            json!({"edits":[{"keyPath":"model","value":"opus","mergeStrategy":"upsert"}]}),
        )
        .await;
    let thread = desktop
        .thread(json!({"cwd":"/tmp","sandbox":"danger-full-access"}))
        .await;
    desktop
        .ok(
            "thread/metadata/update",
            json!({"threadId":thread,"projectId":""}),
        )
        .await;
    let reply = desktop
        .call(
            "turn/start",
            json!({"threadId":thread,
        "input":[{"type":"text","text":"inherit saved model","text_elements":[]}]}),
        )
        .await;
    assert!(reply.get("error").is_none(), "{reply}");
    desktop.completed(&thread).await;
    let read = desktop.ok("thread/read", json!({"threadId":thread})).await;
    assert_eq!(read["thread"]["modelProvider"], "anthropic");
    assert_eq!(read["thread"]["model"], "opus");
    let state = desktop.ok("fake/state", json!({})).await;
    assert!(
        !state["requests"]
            .as_array()
            .unwrap()
            .contains(&json!("thread/start")),
        "Claude default reached Codex: {state}"
    );
}

#[tokio::test]
async fn service_restart_reconnects_without_sending_claude_requests_to_codex() {
    let mut facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    desktop.ok("model/list", json!({})).await;
    let thread = desktop
        .thread(json!({"model":"opus","sandbox":"danger-full-access"}))
        .await;
    desktop.turn(&thread, "opus", "first").await;
    desktop.completed(&thread).await;
    facade._child.kill().await.unwrap();
    // Observe the disconnect through the merged catalog, rather than racing
    // the operating system's close notification with a fixed sleep.
    tokio::time::timeout(DEADLINE, async {
        loop {
            let models = desktop.ok("model/list", json!({})).await;
            if models["data"].as_array().unwrap().len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let failed = desktop.call("thread/start", json!({"model":"opus"})).await;
    assert!(
        failed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("reconnecting"),
        "{failed}"
    );
    let failed = desktop
        .call("model/list", json!({"modelProvider":"anthropic"}))
        .await;
    assert!(
        failed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("reconnecting"),
        "{failed}"
    );
    let failed = desktop.turn(&thread, "opus", "while disconnected").await;
    assert!(
        failed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("reconnecting"),
        "{failed}"
    );
    facade._child = Facade::launch(
        &facade._state,
        &facade.socket,
        env!("CARGO_BIN_EXE_fake-claude"),
    )
    .await;
    tokio::time::timeout(DEADLINE, async {
        loop {
            let models = desktop.ok("model/list", json!({})).await;
            if models["data"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["id"] == "opus")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // The router restores the thread subscription on the new facade connection.
    let reply = desktop.turn(&thread, "opus", "after reconnect").await;
    assert!(reply.get("error").is_none(), "{reply}");
    desktop.completed(&thread).await;
    let state = desktop.ok("fake/state", json!({})).await;
    assert!(
        !state["requests"]
            .as_array()
            .unwrap()
            .contains(&json!("turn/start")),
        "Claude turn reached Codex: {state}"
    );
}

#[tokio::test]
async fn dropped_images_reach_claude_inline_when_steering_and_queueing() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    // Stands in for a screenshot dragged from the capture thumbnail, which
    // only the desktop's process tree (where the router runs) may open.
    let dropped = tempfile::tempdir().unwrap();
    let image = dropped.path().join("Screenshot.png");
    let png = b"\x89PNG\r\n\x1a\nscreenshot";
    std::fs::write(&image, png).unwrap();
    let attachment = json!({"type":"localImage","path":image});
    let inlined =
        json!({"type":"image","url":format!("data:image/png;base64,{}", STANDARD.encode(png))});

    let thread = desktop
        .thread(json!({"model": "fake-claude", "sandbox": "danger-full-access"}))
        .await;
    let reply = desktop.turn(&thread, "fake-claude", "steer-wait").await;
    let turn = reply["result"]["turn"]["id"].clone();
    desktop
        .until(|v| {
            v["method"] == "item/completed" && v["params"]["item"]["text"] == "Waiting for steering"
        })
        .await;

    let missing = desktop
        .call(
            "turn/steer",
            json!({"threadId":thread,"expectedTurnId":turn,
        "input":[{"type":"localImage","path":dropped.path().join("gone.png")}]}),
        )
        .await;
    assert!(
        missing["error"]["message"]
            .as_str()
            .is_some_and(|message| message.starts_with("Cannot read attached image")),
        "{missing}"
    );
    desktop
        .ok(
            "turn/steer",
            json!({"threadId":thread,"expectedTurnId":turn,
        "input":[{"type":"text","text":"look","text_elements":[]},attachment]}),
        )
        .await;
    let steered = desktop
        .until(|v| {
            v["method"] == "item/started"
                && v["params"]["item"]["type"] == "userMessage"
                && v["params"]["item"]["content"][0]["text"] == "look"
        })
        .await;
    assert_eq!(steered["params"]["item"]["content"][1], inlined);
    desktop
        .ok(
            "turn/steer",
            json!({"threadId":thread,"expectedTurnId":turn,
        "input":[{"type":"text","text":"done","text_elements":[]}]}),
        )
        .await;
    let completed = desktop.completed(&thread).await;
    assert!(
        completed["params"]["turn"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["text"] == "Steered: look + done"),
        "{completed}"
    );

    // Queued input is replayed by the facade later, so it must be stored inline.
    let queued = desktop
        .ok(
            "thread/queue/add",
            json!({"threadId":thread,
        "clientUserMessageId":"queued-image","input":[attachment]}),
        )
        .await;
    assert_eq!(queued["queuedSubmission"]["input"], json!([inlined]));
}

#[tokio::test]
async fn other_codex_invocations_run_genuine_codex() {
    let output = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
        .args([
            "route",
            "--codex",
            env!("CARGO_BIN_EXE_fake-codex"),
            "--",
            "--version",
        ])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "codex-cli 9.9.9-fake\n"
    );
}

#[tokio::test]
async fn mlx_picker_and_native_threads_preserve_gpt_claude_and_acp_routes() {
    use claude_codex_server::local_provider::PROVIDER;
    let facade = Facade::start_with_local_acp().await;
    let _mlx = MlxFixture::enable(&facade).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let models = desktop.ok("model/list", json!({})).await;
    let data = models["data"].as_array().unwrap();
    for model in ["gpt-fake", "opus", "fake-a", "fake-b", "mlx-test"] {
        assert!(data.iter().any(|entry| entry["id"] == model), "{models}");
    }
    // A server advertising a native model id cannot take over the GPT entry.
    assert_eq!(
        data.iter()
            .filter(|entry| entry["id"] == "gpt-fake")
            .count(),
        1
    );
    assert_eq!(
        data.iter().find(|entry| entry["id"] == "gpt-fake").unwrap()["isDefault"],
        true
    );
    let scoped = desktop
        .ok("model/list", json!({"modelProvider":PROVIDER}))
        .await;
    let mut scoped_models: Vec<_> = scoped["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|model| model["id"].as_str().unwrap())
        .collect();
    scoped_models.sort_unstable();
    assert_eq!(scoped_models, ["mlx-start-fails", "mlx-test"]);
    let mlx = desktop
        .thread(json!({"model":"mlx-test","sandbox":"workspace-write","approvalPolicy":"never"}))
        .await;
    desktop.turn(&mlx, "mlx-test", "local first turn").await;
    desktop.completed(&mlx).await;
    let read = desktop.ok("thread/read", json!({"threadId":mlx})).await;
    assert_eq!(read["thread"]["modelProvider"], PROVIDER);
    assert_eq!(read["sandbox"]["type"], "workspaceWrite");
    let resumed = desktop
        .ok(
            "thread/resume",
            json!({"threadId":mlx,"modelProvider":"openai"}),
        )
        .await;
    assert_eq!(resumed["thread"]["modelProvider"], PROVIDER);

    let gpt = desktop.thread(json!({"model":"gpt-fake"})).await;
    desktop.turn(&gpt, "gpt-fake", "native GPT").await;
    desktop.completed(&gpt).await;
    let claude = desktop
        .thread(json!({"model":"opus","sandbox":"danger-full-access"}))
        .await;
    desktop.turn(&claude, "opus", "native Claude").await;
    desktop.completed(&claude).await;
    let acp = desktop
        .thread(json!({"model":"fake-b","sandbox":"danger-full-access"}))
        .await;
    desktop.turn(&acp, "fake-b", "native ACP").await;
    desktop.completed(&acp).await;
    for (id, provider) in [
        (&gpt, "openai"),
        (&claude, "anthropic"),
        (&acp, "fake-provider"),
    ] {
        assert_eq!(
            desktop.ok("thread/read", json!({"threadId":id})).await["thread"]["modelProvider"],
            provider
        );
    }
    let state = desktop.ok("fake/state", json!({})).await;
    assert!(state["threadConfigs"][&mlx][format!("model_providers.{PROVIDER}")].is_object());
    assert!(state["threadConfigs"][&gpt]["model_catalog_json"].is_null());
    let listed = desktop.ok("thread/list", json!({})).await;
    for id in [&mlx, &gpt, &claude, &acp] {
        assert!(ids(&listed).contains(id), "{listed}");
    }
    assert!(desktop.turn(&gpt, "mlx-test", "wrong provider").await["error"].is_object());
    assert!(desktop.turn(&mlx, "gpt-fake", "wrong provider").await["error"].is_object());
}

#[tokio::test]
async fn mlx_prewarm_keeps_public_identity_metadata_permissions_and_notifications() {
    use claude_codex_server::local_provider::{Integration, PROVIDER};
    let facade = Facade::start_with_local_acp().await;
    let _mlx = MlxFixture::enable(&facade).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    desktop.ok("model/list", json!({})).await;
    let thread = desktop
        .thread(
            json!({"model":"gpt-fake","sandbox":"workspace-write","approvalPolicy":"on-request"}),
        )
        .await;
    desktop
        .ok(
            "thread/metadata/update",
            json!({"threadId":thread,"projectId":"my-project"}),
        )
        .await;
    desktop
        .ok(
            "thread/settings/update",
            json!({"threadId":thread,"approvalPolicy":"never"}),
        )
        .await;
    desktop
        .ok(
            "thread/settings/update",
            json!({"threadId":thread,"model":"mlx-test"}),
        )
        .await;
    let routing = Integration::read(facade._state.path()).unwrap().unwrap();
    let native = routing.native_id(&thread).to_owned();
    assert_ne!(native, thread);
    let read = desktop.ok("thread/read", json!({"threadId":thread})).await;
    assert_eq!(read["thread"]["id"], thread);
    assert_eq!(read["thread"]["modelProvider"], PROVIDER);
    assert_eq!(read["thread"]["projectId"], "my-project");
    assert_eq!(read["approvalPolicy"], "never");
    assert_eq!(read["sandbox"]["type"], "workspaceWrite");
    desktop.turn(&thread, "mlx-test", "first").await;
    desktop.completed(&thread).await;
    desktop.turn(&thread, "mlx-test", "follow up").await;
    desktop.completed(&thread).await;
    let loaded = desktop.ok("thread/loaded/list", json!({})).await;
    assert!(loaded["data"].as_array().unwrap().contains(&json!(thread)));
    assert!(!loaded["data"].as_array().unwrap().contains(&json!(native)));
    let listed = desktop.ok("thread/list", json!({})).await;
    assert_eq!(
        ids(&listed).iter().filter(|id| **id == thread).count(),
        1,
        "{listed}"
    );
    assert!(!ids(&listed).contains(&native));
    let resumed = desktop
        .ok(
            "thread/resume",
            json!({"threadId":thread,"modelProvider":"openai"}),
        )
        .await;
    assert_eq!(resumed["thread"]["id"], thread);
    assert_eq!(resumed["thread"]["modelProvider"], PROVIDER);
    assert!(
        !desktop
            .history
            .iter()
            .any(|message| message["method"] == "thread/closed"
                && message["params"]["threadId"] == thread)
    );
    assert!(
        !desktop
            .history
            .iter()
            .any(|message| message["params"]["thread"]["id"] == native)
    );
}

#[tokio::test]
async fn mlx_empty_selection_can_return_to_gpt_and_failed_replacement_keeps_original() {
    let facade = Facade::start_with_local_acp().await;
    let _mlx = MlxFixture::enable(&facade).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    desktop.ok("model/list", json!({})).await;
    let thread = desktop.thread(json!({"model":"gpt-fake"})).await;
    let failed = desktop
        .call(
            "thread/settings/update",
            json!({"threadId":thread,"model":"mlx-start-fails"}),
        )
        .await;
    assert!(
        failed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("refused MLX start")
    );
    assert_eq!(
        desktop.ok("thread/read", json!({"threadId":thread})).await["thread"]["modelProvider"],
        "openai"
    );
    desktop
        .ok(
            "thread/settings/update",
            json!({"threadId":thread,"model":"mlx-test"}),
        )
        .await;
    desktop
        .ok(
            "thread/settings/update",
            json!({"threadId":thread,"model":"gpt-fake"}),
        )
        .await;
    let read = desktop.ok("thread/read", json!({"threadId":thread})).await;
    assert_eq!(read["thread"]["id"], thread);
    assert_eq!(read["thread"]["modelProvider"], "openai");
    desktop
        .turn(&thread, "gpt-fake", "GPT after changing selection")
        .await;
    desktop.completed(&thread).await;
}

#[tokio::test]
async fn mlx_offline_discovery_keeps_cached_models_and_existing_routes_available() {
    let facade = Facade::start_with_local_acp().await;
    let mlx = MlxFixture::enable(&facade).await;
    drop(mlx);
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let models = desktop.ok("model/list", json!({})).await;
    for model in ["gpt-fake", "opus", "fake-b", "mlx-test"] {
        assert!(
            models["data"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["id"] == model),
            "{models}"
        );
    }
    let thread = desktop.thread(json!({"model":"gpt-fake"})).await;
    desktop
        .turn(&thread, "gpt-fake", "works with MLX offline")
        .await;
    desktop.completed(&thread).await;
}

#[tokio::test]
async fn mlx_empty_selection_uses_existing_claude_and_acp_adoption() {
    let facade = Facade::start_with_local_acp().await;
    let _mlx = MlxFixture::enable(&facade).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    desktop.ok("model/list", json!({})).await;
    for (model, provider) in [("opus", "anthropic"), ("fake-b", "fake-provider")] {
        let thread = desktop
            .thread(json!({"model":"gpt-fake","sandbox":"danger-full-access"}))
            .await;
        desktop
            .ok(
                "thread/settings/update",
                json!({"threadId":thread,"model":"mlx-test"}),
            )
            .await;
        desktop
            .ok(
                "thread/settings/update",
                json!({"threadId":thread,"model":model}),
            )
            .await;
        desktop
            .turn(&thread, model, "uses the selected native facade")
            .await;
        desktop.completed(&thread).await;
        assert_eq!(
            desktop.ok("thread/read", json!({"threadId":thread})).await["thread"]["modelProvider"],
            provider
        );
    }
    let state = desktop.ok("fake/state", json!({})).await;
    assert!(
        !state["requests"]
            .as_array()
            .unwrap()
            .contains(&json!("turn/start")),
        "A facade model was sent to native Codex: {state}"
    );
}

#[tokio::test]
async fn mlx_aliased_mcp_parent_is_authenticated_without_authorizing_an_inherited_child() {
    let facade = Facade::start_with_local_acp().await;
    let _mlx = MlxFixture::enable(&facade).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    desktop.ok("model/list", json!({})).await;
    let thread = desktop
        .thread(json!({"model":"gpt-fake","sandbox":"workspace-write"}))
        .await;
    desktop
        .ok(
            "thread/settings/update",
            json!({"threadId":thread,"model":"mlx-test"}),
        )
        .await;
    let text = script(json!([
        {"tool":"list_agents","arguments":{}},
        {"tool":"list_agents","arguments":{},"meta":{"threadId":"a-different-native-child"}}
    ]));
    desktop.turn(&thread, "mlx-test", &text).await;
    let results = reports(&desktop.completed(&thread).await);
    assert_ne!(results[0]["isError"], true, "{results:?}");
    assert_eq!(results[1]["isError"], true, "{results:?}");
    assert!(
        results[1]
            .to_string()
            .contains("does not match its delegation scope")
    );
}

/// Opt-in end-to-end check against the installed genuine Codex and MLX-Serve.
/// Creates only its own disposable chats and executes printf / sleep in a temp
/// workspace. Run with CLAUDEX_LIVE_CODEX and, optionally, CLAUDEX_MLX_MODEL.
#[tokio::test]
#[ignore = "requires real Codex authentication and a running MLX-Serve"]
async fn mlx_live_native_tools_stream_interrupt_fork_and_cold_resume() {
    use claude_codex_server::local_provider::{Config, PROVIDER};
    let codex = std::env::var("CLAUDEX_LIVE_CODEX")
        .expect("Set CLAUDEX_LIVE_CODEX to the genuine Codex executable");
    let url = std::env::var("CLAUDEX_MLX_URL")
        .unwrap_or_else(|_| claude_codex_server::local_provider::MLX_URL.into());
    let model = std::env::var("CLAUDEX_MLX_MODEL")
        .unwrap_or_else(|_| "ddalcu/Inkling-2-Mini-MLX-8bit".into());
    let gpt_model = std::env::var("CLAUDEX_GPT_MODEL").unwrap_or_else(|_| "gpt-6.2-sol".into());
    let facade = Facade::start_with_local_acp().await;
    let mut options = Config::new(&url, "MLX-Serve");
    options.reasoning = true;
    let config = options.refresh().await.expect("MLX-Serve model discovery");
    assert!(
        config.model(&model).is_some(),
        "MLX-Serve does not advertise {model}"
    );
    config.write(facade._state.path()).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let local_test_config = json!({"model_reasoning_effort":"low"});
    let mut desktop =
        Desktop::connect_with_codex(&facade.socket, 0, &codex, Duration::from_secs(180)).await;
    let models = desktop.ok("model/list", json!({})).await;
    for id in [&model, &gpt_model, &"opus".to_owned(), &"fake-b".to_owned()] {
        assert!(
            models["data"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["model"] == *id),
            "{id} absent from merged catalog"
        );
    }
    let thread = desktop
        .thread(json!({"cwd":workspace.path(),"model":gpt_model,
        "sandbox":"workspace-write","approvalPolicy":"never","historyMode":"paginated",
        "config":local_test_config}))
        .await;
    desktop
        .ok(
            "thread/settings/update",
            json!({"threadId":thread,"model":model,"effort":"low"}),
        )
        .await;
    desktop.turn(&thread,&model,"Use the shell tool to run `printf MLX_TOOL_OK`. You must actually run it. Then reply with the command output.").await;
    let completed = desktop.completed(&thread).await;
    assert_eq!(
        completed["params"]["turn"]["status"], "completed",
        "{completed}"
    );
    let command = desktop.history.iter().find(|message| {
        message["method"] == "item/completed"
            && message["params"]["threadId"] == thread
            && message["params"]["item"]["type"] == "commandExecution"
            && message["params"]["item"]["aggregatedOutput"]
                .as_str()
                .is_some_and(|s| s.contains("MLX_TOOL_OK"))
    });
    assert!(
        command.is_some(),
        "No real command execution returned MLX_TOOL_OK: {completed}"
    );
    assert!(
        desktop
            .history
            .iter()
            .any(|message| message["method"] == "item/agentMessage/delta"
                && message["params"]["threadId"] == thread),
        "No streamed answer delta"
    );
    eprintln!("MLX live: native shell execution and streaming passed");
    // This native Codex version creates the durable rollout on the first
    // turn; metadata writes to an empty native prewarm are not supported.
    desktop
        .ok(
            "thread/metadata/update",
            json!({"threadId":thread,"projectId":""}),
        )
        .await;
    // The post-restart turn below also verifies a real history-dependent follow-up.
    let interrupted = desktop.turn(&thread,&model,"Run the shell command `sleep 30; printf INTERRUPT_PROBE_DONE` now. Do not answer before it finishes.").await;
    let turn_id = interrupted["result"]["turn"]["id"].clone();
    desktop
        .until(|message| {
            message["method"] == "item/started"
                && message["params"]["threadId"] == thread
                && message["params"]["item"]["type"] == "commandExecution"
                && message["params"]["item"]["command"]
                    .as_str()
                    .is_some_and(|s| s.contains("sleep 30"))
        })
        .await;
    desktop
        .ok(
            "turn/interrupt",
            json!({"threadId":thread,"turnId":turn_id}),
        )
        .await;
    assert_eq!(
        desktop.completed(&thread).await["params"]["turn"]["status"],
        "interrupted"
    );
    eprintln!("MLX live: follow-up and native interruption passed");
    desktop._child.kill().await.unwrap();
    desktop._child.wait().await.unwrap();
    drop(desktop);
    let mut desktop =
        Desktop::connect_with_codex(&facade.socket, 0, &codex, Duration::from_secs(180)).await;
    let listed = desktop
        .ok("thread/list", json!({"cwd":workspace.path(),"limit":100}))
        .await;
    assert!(
        ids(&listed).contains(&thread),
        "Cold list lost the public prewarm identity: {listed}"
    );
    let resumed = desktop
        .ok(
            "thread/resume",
            json!({"threadId":thread,"modelProvider":"openai","excludeTurns":true,"config":local_test_config}),
        )
        .await;
    assert_eq!(resumed["thread"]["id"], thread);
    assert_eq!(resumed["thread"]["modelProvider"], PROVIDER);
    desktop
        .turn(
            &thread,
            &model,
            "Recall the first successful shell command's output. Reply with that marker only.",
        )
        .await;
    let resumed_turn = desktop.completed(&thread).await;
    assert_eq!(
        resumed_turn["params"]["turn"]["status"], "completed",
        "{resumed_turn}"
    );
    assert!(
        streamed_answer(&desktop, &resumed_turn).contains("MLX_TOOL_OK"),
        "{resumed_turn}"
    );
    let fork = desktop
        .ok(
            "thread/fork",
            json!({"threadId":thread,"excludeTurns":true}),
        )
        .await;
    let fork_id = fork["thread"]["id"].as_str().unwrap().to_owned();
    assert_eq!(fork["thread"]["forkedFromId"], thread, "{fork}");
    assert_eq!(fork["thread"]["modelProvider"], PROVIDER);
    eprintln!("MLX live: cold list/resume and native fork identity passed");
    let gpt = desktop.thread(json!({"cwd":workspace.path(),"model":gpt_model,
        "sandbox":"workspace-write","approvalPolicy":"never","config":{"model_reasoning_effort":"low"}})).await;
    let gpt_read = desktop.ok("thread/read", json!({"threadId":gpt})).await;
    assert_eq!(gpt_read["thread"]["modelProvider"], "openai");
    desktop
        .turn(
            &gpt,
            &gpt_model,
            "Reply with exactly GPT_ROUTE_OK. Do not use tools.",
        )
        .await;
    let gpt_turn = desktop.completed(&gpt).await;
    assert_eq!(
        gpt_turn["params"]["turn"]["status"], "completed",
        "{gpt_turn}"
    );
    assert!(
        streamed_answer(&desktop, &gpt_turn).contains("GPT_ROUTE_OK"),
        "{gpt_turn}"
    );
    eprintln!("MLX live: real GPT completion through the existing provider passed");
    for id in [fork_id, thread, gpt] {
        desktop.ok("thread/delete", json!({"threadId":id})).await;
    }
}
