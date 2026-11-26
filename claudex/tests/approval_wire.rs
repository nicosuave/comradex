#![cfg(feature = "test-backend")]

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
    history: Vec<Value>,
    serial: u64,
}

impl Client {
    async fn spawn(state: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
            .args([
                "app-server",
                "--stdio",
                "--claude",
                env!("CARGO_BIN_EXE_approval-claude"),
                "--model",
                "fake-claude",
                "--approval-timeout-seconds",
                "8",
            ])
            .arg("--state-dir")
            .arg(state)
            .current_dir(state)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let input = child.stdin.take();
        let output = BufReader::new(child.stdout.take().unwrap()).lines();
        let mut client = Self {
            child,
            input,
            output,
            history: vec![],
            serial: 0,
        };
        client.ok("initialize",json!({"clientInfo":{"name":"approval-wire","version":"1"},"capabilities":{"experimentalApi":true}})).await;
        client.send(json!({"method":"initialized"})).await;
        client
    }

    async fn send(&mut self, value: Value) {
        self.input
            .as_mut()
            .unwrap()
            .write_all(format!("{value}\n").as_bytes())
            .await
            .unwrap();
    }

    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(15), self.output.next_line())
            .await
            .expect("wire deadline")
            .unwrap()
            .expect("server stdout closed");
        let value: Value = serde_json::from_str(&line).unwrap();
        self.history.push(value.clone());
        value
    }

    async fn ok(&mut self, method: &str, params: Value) -> Value {
        self.serial += 1;
        let id = json!(self.serial);
        self.send(json!({"id":id,"method":method,"params":params}))
            .await;
        loop {
            let value = self.read().await;
            if value["id"] == id && value.get("method").is_none() {
                assert!(value.get("error").is_none(), "{method}: {value}");
                return value["result"].clone();
            }
        }
    }

    async fn until(&mut self, predicate: impl Fn(&Value) -> bool) -> Value {
        if let Some(found) = self.history.iter().find(|v| predicate(v)) {
            return found.clone();
        }
        loop {
            let value = self.read().await;
            if predicate(&value) {
                return value;
            }
        }
    }

    async fn start(&mut self, thread: &str, command: &str, settings: Value) -> String {
        let mut params =
            json!({"threadId":thread,"input":[{"type":"text","text":command,"text_elements":[]}]});
        params
            .as_object_mut()
            .unwrap()
            .extend(settings.as_object().unwrap().clone());
        self.ok("turn/start", params).await["turn"]["id"]
            .as_str()
            .unwrap()
            .into()
    }

    async fn request(&mut self, turn: &str) -> Value {
        self.until(|v| {
            v.get("id").is_some() && v.get("method").is_some() && v["params"]["turnId"] == turn
        })
        .await
    }

    async fn finish(&mut self, turn: &str) -> Value {
        let completed = self
            .until(|v| v["method"] == "turn/completed" && v["params"]["turn"]["id"] == turn)
            .await;
        assert_eq!(
            completed["params"]["turn"]["status"], "completed",
            "{completed}"
        );
        let item = completed["params"]["turn"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["type"] == "agentMessage")
            .unwrap();
        serde_json::from_str(item["text"].as_str().unwrap()).unwrap()
    }

    fn prompt_count(&self, turn: &str) -> usize {
        self.history
            .iter()
            .filter(|v| {
                v.get("id").is_some() && v.get("method").is_some() && v["params"]["turnId"] == turn
            })
            .count()
    }

    async fn close(mut self) {
        self.input.take();
        assert!(
            tokio::time::timeout(Duration::from_secs(15), self.child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }
}

#[tokio::test]
async fn classifier_denial_stays_denied_until_user_switches_and_approves_retry() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    let thread = client
        .ok(
            "thread/start",
            json!({"sandbox":"danger-full-access","approvalsReviewer":"guardian_subagent"}),
        )
        .await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let denied = client.start(&thread, "mcp-mutation", json!({})).await;
    assert_eq!(client.finish(&denied).await["behavior"], "deny");
    assert_eq!(client.prompt_count(&denied), 0);
    assert!(client.history.iter().any(|v| v["method"] == "warning"
        && v["params"]["message"].as_str().is_some_and(
            |text| text.contains("Ask for approval") && text.contains("Shared-resource write")
        )));
    let retry = client
        .start(&thread, "mcp-mutation", json!({"approvalsReviewer":"user"}))
        .await;
    let request = client.request(&retry).await;
    assert_eq!(request["method"], "item/tool/requestUserInput");
    assert!(
        request["params"]["questions"][0]["question"]
            .as_str()
            .unwrap()
            .contains("synthetic fixture only")
    );
    client
        .send(json!({"id":request["id"],"result":{"answers":{"permission":{"answers":["Allow"]}}}}))
        .await;
    let native = client.finish(&retry).await;
    assert_eq!(native["behavior"], "allow");
    assert!(native.get("updatedPermissions").is_none());
    assert_eq!(client.prompt_count(&retry), 1);
    client.close().await;
}

#[tokio::test]
async fn native_multi_select_uses_fresh_ids_and_ignores_stale_answers() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    let thread = client
        .ok("thread/start", json!({"sandbox":"danger-full-access"}))
        .await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let turn = client.start(&thread, "questions", json!({})).await;
    let first = client.request(&turn).await;
    assert_eq!(first["method"], "item/tool/requestOptionPicker");
    assert_eq!(first["params"]["allowMultiple"], true);
    client.send(json!({"id":first["id"],"result":{"action":"submit","selectedOptions":["Export","Search"],"freeformAnswer":"Custom"}})).await;
    let second = client
        .until(|v| {
            v["method"] == "item/tool/requestOptionPicker"
                && v["params"]["question"] == "Which theme?"
        })
        .await;
    assert_ne!(first["id"], second["id"]);
    assert_eq!(second["params"]["allowMultiple"], false);
    client
        .send(json!({"id":first["id"],"result":{"action":"dismiss","selectedOptions":[]}}))
        .await;
    client.send(json!({"id":second["id"],"result":{"action":"submit","selectedOptions":["Dark"],"freeformAnswer":null}})).await;
    let native = client.finish(&turn).await;
    assert_eq!(native["behavior"], "allow");
    assert_eq!(
        native["updatedInput"]["answers"],
        json!({"Which features?":"Export, Search, Custom","Which theme?":"Dark"})
    );
    assert_eq!(client.prompt_count(&turn), 2);
    client.close().await;
}

#[tokio::test]
async fn skipping_later_question_denies_without_submitting_partial_answers() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    let thread = client
        .ok(
            "thread/start",
            json!({"approvalPolicy":"never","sandbox":"danger-full-access"}),
        )
        .await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let turn = client.start(&thread, "questions", json!({})).await;
    let first = client.request(&turn).await;
    client
        .send(json!({"id":first["id"],"result":{"action":"submit","selectedOptions":["Search"]}}))
        .await;
    let second = client
        .until(|v| {
            v["method"] == "item/tool/requestOptionPicker"
                && v["params"]["question"] == "Which theme?"
        })
        .await;
    client.send(json!({"id":second["id"],"result":{"action":"skip","selectedOptions":[],"freeformAnswer":null}})).await;
    let native = client.finish(&turn).await;
    assert_eq!(native["behavior"], "deny");
    assert!(
        native.get("updatedInput").is_none(),
        "partial answers leaked: {native}"
    );
    client.close().await;
}

#[tokio::test]
async fn session_approval_survives_restart_but_respects_scope_fork_and_never() {
    let state = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    let thread = client
        .ok("thread/start", json!({"sandbox":"danger-full-access"}))
        .await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let turn = client.start(&thread, "printf approved", json!({})).await;
    let prompt = client.request(&turn).await;
    assert!(
        prompt["params"]["availableDecisions"]
            .as_array()
            .unwrap()
            .contains(&json!("acceptForSession"))
    );
    client
        .send(json!({"id":prompt["id"],"result":{"decision":"acceptForSession"}}))
        .await;
    let native = client.finish(&turn).await;
    assert_eq!(native["behavior"], "allow");
    assert!(
        native.get("updatedPermissions").is_none(),
        "broad native suggestions must not be applied"
    );
    let repeat = client.start(&thread, "printf approved", json!({})).await;
    assert_eq!(client.finish(&repeat).await["behavior"], "allow");
    assert_eq!(client.prompt_count(&repeat), 0);
    client.close().await;

    let mut client = Client::spawn(state.path()).await;
    client.ok("thread/resume", json!({"threadId":thread})).await;
    let repeat = client.start(&thread, "printf approved", json!({})).await;
    assert_eq!(client.finish(&repeat).await["behavior"], "allow");
    assert_eq!(client.prompt_count(&repeat), 0);

    let fork = client.ok("thread/fork", json!({"threadId":thread})).await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    for (target, command, settings) in [
        (fork.as_str(), "printf approved", json!({})),
        (thread.as_str(), "printf different", json!({})),
        (
            thread.as_str(),
            "printf approved",
            json!({"cwd":other.path()}),
        ),
    ] {
        let turn = client.start(target, command, settings).await;
        let prompt = client.request(&turn).await;
        client
            .send(json!({"id":prompt["id"],"result":{"decision":"decline"}}))
            .await;
        assert_eq!(client.finish(&turn).await["behavior"], "deny");
    }
    let never = client
        .start(
            &thread,
            "printf approved",
            json!({"cwd":state.path(),"approvalPolicy":"never"}),
        )
        .await;
    assert_eq!(client.finish(&never).await["behavior"], "deny");
    assert_eq!(client.prompt_count(&never), 0);
    client.close().await;
}

#[tokio::test]
async fn stale_session_response_cannot_upgrade_an_allow_once_decision() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    let thread = client
        .ok("thread/start", json!({"sandbox":"danger-full-access"}))
        .await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let first = client.start(&thread, "printf once", json!({})).await;
    let prompt = client.request(&first).await;
    client
        .send(json!({"id":prompt["id"],"result":{"decision":"accept"}}))
        .await;
    assert_eq!(client.finish(&first).await["behavior"], "allow");
    client
        .send(json!({"id":prompt["id"],"result":{"decision":"acceptForSession"}}))
        .await;
    let second = client.start(&thread, "printf once", json!({})).await;
    let next = client.request(&second).await;
    assert_ne!(prompt["id"], next["id"]);
    client
        .send(json!({"id":next["id"],"result":{"decision":"decline"}}))
        .await;
    assert_eq!(client.finish(&second).await["behavior"], "deny");
    client.close().await;
}

#[tokio::test]
async fn file_read_uses_native_permissions_and_requires_requested_path() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    let thread = client
        .ok("thread/start", json!({"sandbox":"danger-full-access"}))
        .await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    for (path, grant, expected) in [("one.txt", true, "allow"), ("two.txt", false, "deny")] {
        let turn = client
            .start(&thread, &format!("read:{path}"), json!({}))
            .await;
        let request = client.request(&turn).await;
        assert_eq!(request["method"], "item/permissions/requestApproval");
        assert_eq!(
            request["params"]["permissions"]["fileSystem"]["read"][0],
            state
                .path()
                .canonicalize()
                .unwrap()
                .join(path)
                .to_str()
                .unwrap()
        );
        let permissions = if grant {
            request["params"]["permissions"].clone()
        } else {
            json!({"fileSystem":{"read":["/unrelated"]}})
        };
        client
            .send(
                json!({"id":request["id"],"result":{"permissions":permissions,"scope":"session"}}),
            )
            .await;
        assert_eq!(client.finish(&turn).await["behavior"], expected);
    }
    let turn = client.start(&thread, "read:one.txt", json!({})).await;
    assert_eq!(client.finish(&turn).await["behavior"], "allow");
    assert_eq!(client.prompt_count(&turn), 0);
    client.close().await;
}

#[tokio::test]
#[ignore = "requires authenticated native Claude; exercises only a synthetic local MCP server"]
async fn native_user_review_after_auto_selection_prompts_before_mcp_execution() {
    let fixture = tempfile::tempdir().unwrap();
    let note_path = fixture.path().join("approved-note.txt");
    let nonce = uuid::Uuid::new_v4().to_string();
    let mcp = json!({"mcpServers":{"approval_fixture":{"command":env!("CARGO_BIN_EXE_approval-claude"),"args":["--mcp-fixture",note_path]}}});
    let mut child = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
        .args(["app-server", "--stdio", "--model", "opus", "--state-dir"])
        .arg(fixture.path().join("state"))
        .args([
            "--claude-arg=--setting-sources=",
            "--claude-arg=--strict-mcp-config",
            "--claude-arg=--tools=",
        ])
        .arg(format!("--claude-arg=--mcp-config={mcp}"))
        .env_remove("CLAUDE_CODEX_PLUGIN_EXECUTABLE")
        .current_dir(fixture.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap()).lines();
    input.write_all(b"{\"id\":1,\"method\":\"initialize\",\"params\":{\"clientInfo\":{\"name\":\"native-mcp-approval\",\"version\":\"1\"},\"capabilities\":{\"experimentalApi\":true}}}\n").await.unwrap();
    let mut approvals = 0;
    tokio::time::timeout(Duration::from_secs(120), async {
        while let Some(line) = output.next_line().await.unwrap() {
            let value: Value = serde_json::from_str(&line).unwrap();
            assert!(value.get("error").is_none(), "{value}");
            let request = match value["id"].as_u64() {
                Some(1) => Some(json!({"id":2,"method":"thread/start","params":{"cwd":fixture.path(),"sandbox":"workspace-write","approvalPolicy":"on-request","approvalsReviewer":"guardian_subagent","model":"opus","effort":"low","ephemeral":true}})),
                Some(2) => Some(json!({"id":3,"method":"turn/start","params":{"threadId":value["result"]["thread"]["id"],"approvalsReviewer":"user","input":[{"type":"text","text":format!("Call approval_fixture/save_note exactly once with note {nonce}. It saves a synthetic integration-test note in a disposable local fixture. Wait for the user approval prompt before execution, then report the tool result.")} ]}})),
                _ => None,
            };
            if let Some(request) = request { input.write_all(format!("{request}\n").as_bytes()).await.unwrap(); }
            if value.get("id").is_some() && value.get("method").is_some() {
                assert_eq!(value["method"], "item/tool/requestUserInput", "{value}");
                let question = value["params"]["questions"][0]["question"].as_str().unwrap();
                assert!(question.contains("mcp__approval_fixture__save_note") && question.contains(&nonce), "{value}");
                assert!(!note_path.exists(), "MCP executed before approval");
                approvals += 1;
                let reply = json!({"id":value["id"],"result":{"answers":{"permission":{"answers":["Allow"]}}}});
                input.write_all(format!("{reply}\n").as_bytes()).await.unwrap();
            }
            if value["method"] == "turn/completed" {
                assert_eq!(value["params"]["turn"]["status"], "completed", "{value}");
                break;
            }
        }
    }).await.expect("native MCP approval deadline");
    assert_eq!(approvals, 1);
    assert_eq!(std::fs::read_to_string(note_path).unwrap(), nonce);
    child.kill().await.unwrap();
}

#[tokio::test]
async fn approved_subagent_tool_call_completes_with_its_native_result() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    let thread = client
        .ok("thread/start", json!({"sandbox":"danger-full-access"}))
        .await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let turn = client.start(&thread, "subagent", json!({})).await;
    let request = client
        .until(|v| v["method"] == "item/commandExecution/requestApproval")
        .await;
    // The request belongs to the subagent's child thread and turn.
    let child = request["params"]["threadId"].as_str().unwrap().to_owned();
    assert_ne!(child, thread);
    assert_ne!(request["params"]["turnId"], turn.as_str());
    assert_eq!(request["params"]["itemId"], "permission-tool");
    let waiting = client
        .until(|v| {
            v["method"] == "thread/status/changed"
                && v["params"]["threadId"] == child.as_str()
                && v["params"]["status"]["activeFlags"] == json!(["waitingOnApproval"])
        })
        .await;
    assert_eq!(waiting["params"]["status"]["type"], "active");
    client
        .send(json!({"id":request["id"],"result":{"decision":"accept"}}))
        .await;
    assert_eq!(client.finish(&turn).await["behavior"], "allow");
    let resolved = client
        .until(|v| {
            v["method"] == "serverRequest/resolved" && v["params"]["requestId"] == request["id"]
        })
        .await;
    assert_eq!(resolved["params"]["threadId"], child.as_str());
    let completed = client
        .until(|v| {
            v["method"] == "item/completed" && v["params"]["item"]["id"] == "permission-tool"
        })
        .await;
    assert_eq!(completed["params"]["threadId"], child.as_str());
    assert_eq!(completed["params"]["item"]["status"], "completed");
    assert!(
        completed["params"]["item"]["aggregatedOutput"]
            .as_str()
            .unwrap()
            .contains("allow")
    );
    client.close().await;
}
