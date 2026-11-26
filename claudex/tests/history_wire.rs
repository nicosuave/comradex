#![cfg(feature = "test-backend")]

use serde_json::{Value, json};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
};

struct Peer {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    received: Vec<Value>,
    serial: u64,
    timeout: Duration,
}

impl Peer {
    async fn open(state: &Path) -> Self {
        Self::open_with_args(state, &[]).await
    }

    async fn open_with_args(state: &Path, extra: &[&str]) -> Self {
        Self::open_backend(state, env!("CARGO_BIN_EXE_fake-claude"), extra).await
    }

    async fn open_backend(state: &Path, backend: &str, extra: &[&str]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
            .args(["app-server", "--stdio", "--claude", backend])
            .args(extra)
            .arg("--state-dir")
            .arg(state)
            .current_dir(state)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut peer = Self {
            input: child.stdin.take().unwrap(),
            output: BufReader::new(child.stdout.take().unwrap()).lines(),
            child,
            received: vec![],
            serial: 0,
            timeout: Duration::from_secs(15),
        };
        peer.ok("initialize", json!({"clientInfo":{"name":"history-wire","version":"1"},"capabilities":{"experimentalApi":true}})).await;
        peer
    }

    async fn until(&mut self, predicate: impl Fn(&Value) -> bool) -> Value {
        if let Some(found) = self.received.iter().find(|value| predicate(value)) {
            return found.clone();
        }
        loop {
            let line = tokio::time::timeout(self.timeout, self.output.next_line())
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
        self.input
            .write_all(format!("{message}\n").as_bytes())
            .await
            .unwrap();
        self.until(|value| value["id"] == id && value.get("method").is_none())
            .await
    }

    async fn ok(&mut self, method: &str, params: Value) -> Value {
        let reply = self.request(method, params).await;
        assert!(reply.get("error").is_none(), "{method}: {reply}");
        reply["result"].clone()
    }

    async fn thread(&mut self, mode: &str) -> String {
        self.ok(
            "thread/start",
            json!({"sandbox":"danger-full-access","historyMode":mode}),
        )
        .await["thread"]["id"]
            .as_str()
            .unwrap()
            .into()
    }

    async fn start(&mut self, thread: &str, text: &str) -> String {
        self.ok(
            "turn/start",
            json!({"threadId":thread,"input":[{"type":"text","text":text}]}),
        )
        .await["turn"]["id"]
            .as_str()
            .unwrap()
            .into()
    }

    async fn complete(&mut self, turn: &str) -> Value {
        let notification = self
            .until(|value| {
                value["method"] == "turn/completed" && value["params"]["turn"]["id"] == turn
            })
            .await;
        notification["params"]["turn"].clone()
    }

    async fn say(&mut self, thread: &str, text: &str) -> Value {
        let turn = self.start(thread, text).await;
        let result = self.complete(&turn).await;
        assert_eq!(result["status"], "completed", "{result}");
        result
    }

    async fn args(&mut self, thread: &str) -> Value {
        let turn = self.say(thread, "fork-info").await;
        let text = turn["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "agentMessage")
            .unwrap()["text"]
            .as_str()
            .unwrap();
        serde_json::from_str(text).unwrap()
    }

    async fn predict(&mut self, parent: &str, prompt: &str) -> Value {
        let fork = self.ok("thread/fork", json!({
            "threadId":parent,"experimentalPredictionMode":true,"ephemeral":true,"excludeTurns":true
        })).await;
        let id = fork["thread"]["id"].as_str().unwrap();
        let turn = self.say(id, prompt).await;
        self.ok("thread/unsubscribe", json!({"threadId":id})).await;
        serde_json::from_str(answer(&turn)).unwrap()
    }

    async fn close(mut self) {
        drop(self.input);
        let status = tokio::time::timeout(Duration::from_secs(15), self.child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success());
    }
}

fn record(state: &Path, thread: &str) -> Value {
    serde_json::from_slice(
        &std::fs::read(state.join("threads").join(format!("{thread}.json"))).unwrap(),
    )
    .unwrap()
}

fn assert_fork(args: &Value, checkpoint: &Value) {
    assert_eq!(args["fork"], true);
    assert_eq!(args["resume"], checkpoint["session_id"]);
    assert_eq!(args["resumeAt"], checkpoint["message_id"]);
    assert_ne!(args["session"], checkpoint["session_id"]);
}

fn answer(turn: &Value) -> &str {
    turn["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "agentMessage")
        .unwrap()["text"]
        .as_str()
        .unwrap()
}

#[tokio::test]
async fn desktop_prediction_reuses_the_warm_parent_and_coalesces_identical_requests() {
    let state = tempfile::tempdir().unwrap();
    let mut peer = Peer::open(state.path()).await;
    let source = peer
        .ok("thread/start", json!({
            "sandbox":"danger-full-access", "model":"opus", "config":{"model_reasoning_effort":"high"},
            "baseInstructions":"Parent base", "developerInstructions":"Parent developer",
            "dynamicTools":[{"type":"function","name":"parent_tool","description":"Only the parent has this tool", "inputSchema":{"type":"object"}}]
        }))
        .await["thread"]["id"]
        .as_str().unwrap().to_owned();
    let first = peer.say(&source, "process-info").await;
    let process = answer(&first).strip_suffix(" prompt 1").unwrap();
    let resumed = peer
        .ok(
            "thread/resume",
            json!({"threadId":source,"excludeTurns":true}),
        )
        .await;
    assert_eq!(resumed["claudexPredictionFork"], true);
    assert_eq!(resumed["modelProvider"], "anthropic");
    let original = record(state.path(), &source);
    let fork = peer
        .ok(
            "thread/fork",
            json!({
                "threadId":source, "experimentalPredictionMode":true,
                "ephemeral":true, "excludeTurns":true, "threadSource":"composer_predictions"
            }),
        )
        .await;
    let prediction = fork["thread"]["id"].as_str().unwrap();
    assert_eq!(fork["modelProvider"], "anthropic");
    assert_eq!(fork["model"], "opus");
    assert_eq!(fork["reasoningEffort"], "high");
    assert_eq!(fork["thread"]["ephemeral"], true);
    assert!(fork["thread"]["path"].is_null());
    assert_eq!(fork["thread"]["turns"], json!([]));
    let turn = peer.say(prediction, "predict-next").await;
    let result: Value = serde_json::from_str(answer(&turn)).unwrap();
    assert_eq!(
        result["suggestion"],
        format!("{process} prediction 1 prompt 1")
    );
    assert_eq!(record(state.path(), &source), original);
    assert!(
        !state
            .path()
            .join("threads")
            .join(format!("{prediction}.json"))
            .exists()
    );
    let repeated = peer
        .request(
            "turn/start",
            json!({
                "threadId":prediction,"input":[{"type":"text","text":"another prediction"}]
            }),
        )
        .await;
    assert!(
        repeated["error"]["message"]
            .as_str()
            .unwrap()
            .contains("one turn")
    );
    peer.ok("thread/unsubscribe", json!({"threadId":prediction}))
        .await;
    assert!(
        peer.request("thread/read", json!({"threadId":prediction}))
            .await
            .get("error")
            .is_some()
    );
    let repeated = peer.predict(&source, "predict-next").await;
    assert_eq!(
        repeated, result,
        "identical forks must share one native request"
    );
    let different = peer.predict(&source, "predict-different").await;
    assert_eq!(
        different["suggestion"],
        format!("{process} prediction 2 prompt 1")
    );
    assert_eq!(record(state.path(), &source), original);
    let after = peer.say(&source, "process-info").await;
    assert_eq!(answer(&after), format!("{process} prompt 2"));
    peer.close().await;
}

#[tokio::test]
async fn predictions_skip_cold_stale_and_oversized_context_without_a_new_process() {
    let state = tempfile::tempdir().unwrap();
    let mut peer = Peer::open(state.path()).await;
    let parent = peer.thread("legacy").await;
    peer.say(&parent, "cold-cache").await;
    assert_eq!(
        peer.predict(&parent, "predict-next").await["suggestion"],
        Value::Null
    );
    let warm = peer.say(&parent, "process-info").await;
    let process = answer(&warm).strip_suffix(" prompt 2").unwrap();
    assert_eq!(
        peer.predict(&parent, &"x".repeat(9 * 1024)).await["suggestion"],
        Value::Null
    );
    let fork = peer.ok("thread/fork",json!({
        "threadId":parent,"experimentalPredictionMode":true,"ephemeral":true,"excludeTurns":true
    })).await;
    let invalid = peer
        .request(
            "turn/start",
            json!({
                "threadId":fork["thread"]["id"],
                "input":[{"type":"localImage","path":"/unread-prediction-image.png"}]
            }),
        )
        .await;
    assert!(
        invalid["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Prediction input")
    );
    let invalid = peer
        .request(
            "turn/start",
            json!({
                "threadId":fork["thread"]["id"],"model":"haiku",
                "input":[{"type":"text","text":"predict-next"}]
            }),
        )
        .await;
    assert!(
        invalid.get("error").is_some(),
        "prediction must retain the cached model"
    );
    peer.say(&parent, "process-info").await;
    let stale = peer
        .say(fork["thread"]["id"].as_str().unwrap(), "predict-next")
        .await;
    assert_eq!(
        serde_json::from_str::<Value>(answer(&stale)).unwrap()["suggestion"],
        Value::Null
    );
    let prediction = peer.predict(&parent, "predict-next").await;
    assert_eq!(
        prediction["suggestion"],
        format!("{process} prediction 1 prompt 3")
    );
    peer.close().await;
    let mut peer = Peer::open(state.path()).await;
    peer.ok("thread/resume", json!({"threadId":parent})).await;
    assert_eq!(
        peer.predict(&parent, "predict-next").await["suggestion"],
        Value::Null
    );
    peer.close().await;
}

#[tokio::test]
async fn structured_prediction_turns_answer_in_their_schema() {
    let state = tempfile::tempdir().unwrap();
    let log = state.path().join("prediction.jsonl");
    let arg = format!("--claude-arg=--fake-prediction-log={}", log.display());
    let mut peer = Peer::open_with_args(state.path(), &[&arg]).await;
    let parent = peer.thread("legacy").await;
    peer.say(&parent, "parent history").await;
    // The desktop's title refresh: a prediction fork whose turn has a schema.
    let fork = peer
        .ok(
            "thread/fork",
            json!({
                "threadId":parent,"experimentalPredictionMode":true,"ephemeral":true,
                "excludeTurns":true,"threadSource":"thread_title"
            }),
        )
        .await;
    let fork = fork["thread"]["id"].as_str().unwrap();
    let schema = json!({"type":"object","properties":{"suggestion":{"type":"string"}},
        "required":["suggestion"],"additionalProperties":false});
    let turn = peer
        .ok(
            "turn/start",
            json!({"threadId":fork,"outputSchema":schema,
                "input":[{"type":"text","text":"Title this chat","text_elements":[]}]}),
        )
        .await["turn"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let completed = peer.complete(&turn).await;
    let value: Value = serde_json::from_str(answer(&completed)).unwrap();
    assert!(value["suggestion"].as_str().unwrap().contains("prediction"));
    let question = std::fs::read_to_string(&log).unwrap();
    let question: Value = serde_json::from_str(question.lines().next().unwrap()).unwrap();
    let question = question["question"].as_str().unwrap();
    assert!(question.starts_with("Title this chat"), "{question}");
    assert!(question.contains(&schema.to_string()), "{question}");
    peer.close().await;
}

#[tokio::test]
async fn cancelling_a_prediction_keeps_the_parent_usable() {
    let state = tempfile::tempdir().unwrap();
    let log = state.path().join("prediction.jsonl");
    let arg = format!("--claude-arg=--fake-prediction-log={}", log.display());
    let mut peer = Peer::open_with_args(state.path(), &[&arg]).await;
    let parent = peer.thread("legacy").await;
    let first = peer.say(&parent, "process-info").await;
    let process = answer(&first).strip_suffix(" prompt 1").unwrap();
    let fork = peer.ok("thread/fork",json!({
        "threadId":parent,"experimentalPredictionMode":true,"ephemeral":true,"excludeTurns":true
    })).await;
    let prediction = fork["thread"]["id"].as_str().unwrap();
    let pending = peer.start(prediction, "hang-prediction").await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while !log.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    // A real parent turn cancels only the side question, and uses the same process.
    let after = peer.say(&parent, "process-info").await;
    assert_eq!(answer(&after), format!("{process} prompt 2"));
    assert_eq!(peer.complete(&pending).await["status"], "interrupted");
    let events: Vec<Value> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["kind"], "start");
    assert_eq!(events[1]["kind"], "cancel");
    assert_eq!(events[0]["requestId"], events[1]["requestId"]);
    let next = peer.predict(&parent, "predict-next").await;
    assert!(next["suggestion"].as_str().unwrap().starts_with(process));
    peer.close().await;
}

#[tokio::test]
async fn prediction_forks_reject_overrides_unloaded_and_temporary_parents() {
    let state = tempfile::tempdir().unwrap();
    let mut peer = Peer::open(state.path()).await;
    let source = peer.thread("legacy").await;
    peer.say(&source, "parent history").await;
    for override_params in [
        json!({"ephemeral":false}),
        json!({"experimentalPredictionMode":"yes"}),
        json!({"model":"haiku"}),
        json!({"config":{"model_reasoning_effort":"high"}}),
        json!({"baseInstructions":"replace parent"}),
        json!({"beforeTurnId":"earlier"}),
        json!({"permissions":":danger-full-access"}),
    ] {
        let mut params =
            json!({"threadId":source,"experimentalPredictionMode":true,"ephemeral":true});
        params
            .as_object_mut()
            .unwrap()
            .extend(override_params.as_object().unwrap().clone());
        let reply = peer.request("thread/fork", params).await;
        assert_eq!(reply["error"]["code"], -32602, "{reply}");
    }
    peer.ok("thread/unsubscribe", json!({"threadId":source}))
        .await;
    let unloaded = peer
        .request(
            "thread/fork",
            json!({
                "threadId":source,"experimentalPredictionMode":true,"ephemeral":true
            }),
        )
        .await;
    assert!(
        unloaded["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Resume the parent")
    );
    let temporary =
        peer.ok("thread/start", json!({"ephemeral":true})).await["thread"]["id"].clone();
    assert!(
        peer.request(
            "thread/fork",
            json!({
                "threadId":temporary,"experimentalPredictionMode":true,"ephemeral":true
            })
        )
        .await["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Ephemeral")
    );
    peer.close().await;
}

#[tokio::test]
#[ignore = "requires authenticated native Claude; verifies cached input, recall, and tool/hook isolation"]
async fn native_prediction_reuses_cache_without_tools_or_hooks() {
    let state = tempfile::tempdir().unwrap();
    let hook_file = state.path().join("hook-ran");
    let tool_file = state.path().join("tool-ran");
    std::fs::create_dir(state.path().join(".claude")).unwrap();
    std::fs::write(state.path().join(".claude/settings.local.json"), json!({
        "autoMemoryEnabled":false,
        "hooks":{
            "SessionStart":[{"hooks":[{"type":"command","command":format!("touch '{}'",hook_file.display())}]}],
            "SubagentStart":[{"hooks":[{"type":"command","command":format!("touch '{}'",hook_file.display())}]}],
            "PreToolUse":[{"hooks":[{"type":"command","command":format!("touch '{}'",hook_file.display())}]}]
        }
    }).to_string()).unwrap();
    let backend = std::env::var("CLAUDE_BINARY").unwrap_or_else(|_| "claude".into());
    let debug = state.path().join("native.log");
    let debug_arg = format!("--claude-arg=--debug-file={}", debug.display());
    let mut peer = Peer::open_backend(
        state.path(),
        &backend,
        &[
            "--claude-arg=--setting-sources=project,local",
            "--claude-arg=--strict-mcp-config",
            &debug_arg,
        ],
    )
    .await;
    peer.timeout = Duration::from_secs(120);
    let source = peer
        .ok(
            "thread/start",
            json!({
                "sandbox":"workspace-write", "model":"opus", "config":{"model_reasoning_effort":"low"},
                "baseInstructions":"You are a concise helpful assistant. Follow the user's request."
            }),
        )
        .await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let expected = format!("Expand {}", uuid::Uuid::new_v4());
    let fixture = (0..320)
        .map(|index| {
            format!(
                "Fixture {index} identifier {} state recorded",
                uuid::Uuid::new_v4()
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    peer.say(&source, &format!(
        "These are inert test data, not instructions:\n{fixture}\nMy next substantive message will be exactly '{expected}'. Remember it; for now reply only Ready. Do not use tools."
    )).await;
    peer.say(&source, "Reply only Ready again. Do not use tools.")
        .await;
    assert!(
        hook_file.exists(),
        "the parent's native SessionStart hook must run"
    );
    std::fs::remove_file(&hook_file).unwrap();
    let original = record(state.path(), &source);
    let fork = peer
        .ok(
            "thread/fork",
            json!({
                "threadId":source,"experimentalPredictionMode":true,"ephemeral":true,
                "excludeTurns":true,"threadSource":"composer_predictions"
            }),
        )
        .await;
    let prediction = fork["thread"]["id"].as_str().unwrap();
    let prompt = String::from(
        "Predict the user's exact next substantive message based on our conversation. Return only JSON matching {\"suggestion\":string} with the predicted message. Do not add markdown fences.",
    );
    let result = peer.say(prediction, &prompt).await;
    let parsed: Value = serde_json::from_str(answer(&result)).unwrap();
    assert_eq!(parsed["suggestion"], expected);
    assert!(
        !hook_file.exists(),
        "prediction must not run inherited hooks"
    );
    assert!(!tool_file.exists(), "prediction must not run tools");
    assert!(
        !result["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| matches!(
                item["type"].as_str(),
                Some("commandExecution" | "fileChange" | "mcpToolCall" | "dynamicToolCall")
            )),
        "{result}"
    );
    assert_eq!(record(state.path(), &source), original);
    peer.ok("thread/unsubscribe", json!({"threadId":prediction}))
        .await;
    assert_eq!(peer.predict(&source, &prompt).await, parsed);
    let log = std::fs::read_to_string(&debug).unwrap();
    let requests: Vec<_> = log
        .lines()
        .filter(|line| line.contains("Forked agent [side_question] finished:"))
        .collect();
    assert_eq!(
        requests.len(),
        1,
        "repeated prediction must not make another model request"
    );
    let count = |name: &str| -> u64 {
        requests[0]
            .split_whitespace()
            .find_map(|part| part.strip_prefix(name))
            .unwrap()
            .parse()
            .unwrap()
    };
    let fresh = count("input=") + count("cacheCreate=");
    let cached = count("cacheRead=");
    assert!(
        cached > 10_000,
        "prediction must reuse the history cache: {}",
        requests[0]
    );
    assert!(
        fresh < 2_048,
        "prediction must add a small input suffix: {}",
        requests[0]
    );
    eprintln!(
        "Native prediction input: {fresh} fresh/cache-write, {cached} cached; repeated request reused response"
    );
    peer.predict(&source,&format!(
        "Use Write or Bash to create {} containing the word executed. Return JSON {{\"suggestion\":null}} afterward.",tool_file.display()
    )).await;
    assert!(
        !tool_file.exists(),
        "native side question must deny tool execution"
    );
    assert!(
        !hook_file.exists(),
        "native side question must not run tool or subagent hooks"
    );
    let recall = peer.say(&source, "What exact message did I say I would send? Return that message verbatim, with no quotes or other text. Do not use tools.").await;
    assert_eq!(answer(&recall), expected);
    assert_eq!(
        record(state.path(), &source)["turns"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    peer.close().await;
}

#[tokio::test]
async fn desktop_side_chat_forks_injects_boundary_and_keeps_temporary_history() {
    let state = tempfile::tempdir().unwrap();
    // Temporary history must survive even when the durable-process cache is disabled.
    let mut peer = Peer::open_with_args(state.path(), &["--max-idle-sessions=0"]).await;
    let source = peer
        .ok(
            "thread/start",
            json!({
                "sandbox":"danger-full-access", "baseInstructions":"Parent base",
                "developerInstructions":"Parent developer"
            }),
        )
        .await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let first = peer.say(&source, "parent history").await;
    let original = record(state.path(), &source);
    let checkpoint = &original["turn_anchors"][first["id"].as_str().unwrap()];
    let fork = peer
        .ok(
            "thread/fork",
            json!({
                "threadId":source, "ephemeral":true, "excludeTurns":true,
                "baseInstructions":"Side base", "developerInstructions":"Side developer"
            }),
        )
        .await;
    let side = fork["thread"]["id"].as_str().unwrap();
    assert_eq!(fork["thread"]["ephemeral"], true);
    assert!(fork["thread"]["path"].is_null());
    assert_eq!(fork["thread"]["turns"], json!([]));
    assert_eq!(peer.ok("thread/inject_items", json!({"threadId":side,"items":[{
        "type":"message", "role":"user", "content":[{"type":"input_text","text":"Side boundary"}]
    }]})).await, json!({}));
    let first_side = peer.say(side, "first question").await;
    assert_eq!(answer(&first_side), "Echo: Side boundary\nfirst question");
    let args = peer.args(side).await;
    assert_fork(&args, checkpoint);
    assert_eq!(args["systemPrompt"], "Side base");
    assert!(
        args["appendSystemPrompt"]
            .as_str()
            .unwrap()
            .starts_with("Side developer\n")
    );
    assert_eq!(args["ephemeral"], true);
    let process = peer.say(side, "process-info").await;
    assert!(answer(&process).ends_with("prompt 3"), "{process}");
    let changed = peer.request("turn/start", json!({
        "threadId":side,"input":[{"type":"text","text":"discard history"}],"model":"different-model"
    })).await;
    assert!(
        changed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Cannot restart a temporary chat")
    );
    let continued = peer.say(side, "process-info").await;
    assert_eq!(
        answer(&continued).split(" prompt ").next(),
        answer(&process).split(" prompt ").next()
    );
    assert!(answer(&continued).ends_with("prompt 4"));
    let list = peer.ok("thread/list", json!({})).await;
    assert!(
        !list["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|thread| thread["id"] == side)
    );
    assert!(
        !state
            .path()
            .join("threads")
            .join(format!("{side}.json"))
            .exists()
    );
    assert_eq!(record(state.path(), &source), original);
    peer.ok("thread/unsubscribe", json!({"threadId":side}))
        .await;
    assert!(
        peer.request("thread/read", json!({"threadId":side,"includeTurns":false}))
            .await
            .get("error")
            .is_some()
    );
    peer.close().await;
}

#[tokio::test]
async fn side_chat_snapshots_completed_history_without_interrupting_active_parent() {
    let state = tempfile::tempdir().unwrap();
    let mut peer = Peer::open(state.path()).await;
    let source = peer.thread("legacy").await;
    let first = peer.say(&source, "completed parent history").await;
    let checkpoint =
        record(state.path(), &source)["turn_anchors"][first["id"].as_str().unwrap()].clone();
    let active = peer.start(&source, "hang").await;
    let fork = peer
        .ok("thread/fork", json!({"threadId":source,"ephemeral":true}))
        .await;
    assert_eq!(fork["thread"]["turns"], json!([first]));
    let side = fork["thread"]["id"].as_str().unwrap();
    assert_fork(&peer.args(side).await, &checkpoint);
    assert_eq!(
        peer.ok(
            "thread/read",
            json!({"threadId":source,"includeTurns":true})
        )
        .await["thread"]["status"]["type"],
        "active"
    );
    assert!(
        peer.request(
            "thread/fork",
            json!({"threadId":source,"lastTurnId":active})
        )
        .await
        .get("error")
        .is_some()
    );
    peer.ok("turn/interrupt", json!({"threadId":source,"turnId":active}))
        .await;
    peer.complete(&active).await;
    peer.close().await;
}

#[tokio::test]
async fn injected_context_rejects_unsupported_items_without_partial_mutation() {
    let state = tempfile::tempdir().unwrap();
    let mut peer = Peer::open(state.path()).await;
    let thread = peer.thread("legacy").await;
    let error = peer.request("thread/inject_items", json!({"threadId":thread,"items":[
        {"type":"message","role":"user","content":[{"type":"input_text","text":"must not be queued"}]},
        {"type":"message","role":"assistant","content":[{"type":"output_text","text":"unsupported"}]}
    ]})).await;
    assert!(error.get("error").is_some());
    assert_eq!(
        answer(&peer.say(&thread, "ordinary prompt").await),
        "Echo: ordinary prompt"
    );
    peer.close().await;
}

fn validate_revert(value: &Value) {
    let root: Value = serde_json::from_str(include_str!("../protocol/codex-0.170.0.json")).unwrap();
    let mut schema = root["definitions"]["v2"]["ThreadRevertResponse"].clone();
    schema["definitions"] = root["definitions"].clone();
    let validator = jsonschema::validator_for(&schema).unwrap();
    assert!(validator.is_valid(value), "{value}");
}

#[tokio::test]
async fn forks_use_exact_native_turn_anchors_and_leave_source_unchanged() {
    let state = tempfile::tempdir().unwrap();
    let mut peer = Peer::open(state.path()).await;
    let source = peer.thread("legacy").await;
    let first = peer.say(&source, "retained").await;
    let second = peer.say(&source, "discarded").await;
    let original = record(state.path(), &source);
    assert_eq!(original["turn_anchors"].as_object().unwrap().len(), 2);
    let checkpoint = original["turn_anchors"][first["id"].as_str().unwrap()].clone();
    assert_ne!(checkpoint["message_id"], original["backend_message_id"]);
    for params in [
        json!({"threadId":source,"beforeTurnId":second["id"]}),
        json!({"threadId":source,"lastTurnId":first["id"]}),
    ] {
        let fork = peer.ok("thread/fork", params).await;
        assert_eq!(fork["thread"]["turns"], json!([first]));
        let child = fork["thread"]["id"].as_str().unwrap();
        assert_fork(&peer.args(child).await, &checkpoint);
    }
    assert_eq!(record(state.path(), &source), original);
    let empty = peer
        .ok(
            "thread/fork",
            json!({"threadId":source,"beforeTurnId":first["id"]}),
        )
        .await;
    assert_eq!(empty["thread"]["turns"], json!([]));
    let args = peer.args(empty["thread"]["id"].as_str().unwrap()).await;
    assert_eq!(args["fork"], false);
    assert!(args["resume"].is_null());
    assert!(args["resumeAt"].is_null());
    let rollback = peer
        .ok("thread/rollback", json!({"threadId":source,"numTurns":1}))
        .await;
    assert_eq!(rollback["thread"]["turns"], json!([first]));
    assert_fork(&peer.args(&source).await, &checkpoint);
    peer.close().await;
}

#[tokio::test]
async fn reverted_prefix_survives_restart_and_rollback_can_clear_everything() {
    let state = tempfile::tempdir().unwrap();
    let mut peer = Peer::open(state.path()).await;
    let thread = peer.thread("paginated").await;
    let first = peer.say(&thread, "retained").await;
    let second = peer.say(&thread, "discarded").await;
    let before = record(state.path(), &thread);
    let checkpoint = before["turn_anchors"][first["id"].as_str().unwrap()].clone();
    let response = peer
        .ok(
            "thread/revert",
            json!({"threadId":thread,"beforeTurnId":second["id"]}),
        )
        .await;
    validate_revert(&response);
    assert_eq!(response["thread"]["turns"], json!([]));
    assert!(response["turnsBackwardsCursor"].is_string());
    assert!(response["itemsBackwardsCursor"].is_string());
    peer.until(|value| {
        value["method"] == "thread/reverted" && value["params"]["threadId"] == thread
    })
    .await;
    let page = peer.ok("thread/turns/list", json!({"threadId":thread,"cursor":response["turnsBackwardsCursor"],"sortDirection":"desc"})).await;
    assert_eq!(page["data"][0]["id"], first["id"]);
    let recovered = record(state.path(), &thread);
    // The record keeps item times apart from its stored items.
    let mut stored = first.clone();
    for item in stored["items"].as_array_mut().unwrap() {
        let item = item.as_object_mut().unwrap();
        item.remove("startedAtMs");
        item.remove("completedAtMs");
    }
    assert_eq!(recovered["turns"], json!([stored]));
    assert_eq!(recovered["turn_anchors"].as_object().unwrap().len(), 1);
    assert_eq!(recovered["has_session"], false);
    assert_ne!(recovered["session_id"], before["session_id"]);
    peer.close().await;

    let mut peer = Peer::open(state.path()).await;
    let unloaded = peer
        .request("thread/rollback", json!({"threadId":thread,"numTurns":1}))
        .await;
    assert!(
        unloaded["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Resume")
    );
    peer.ok("thread/resume", json!({"threadId":thread})).await;
    assert_fork(&peer.args(&thread).await, &checkpoint);
    let clear = peer
        .ok("thread/rollback", json!({"threadId":thread,"numTurns":99}))
        .await;
    assert_eq!(clear["thread"]["turns"], json!([]));
    let cleared = record(state.path(), &thread);
    assert_eq!(cleared["turn_anchors"], json!({}));
    assert_eq!(cleared["item_times"], json!({}));
    let args = peer.args(&thread).await;
    assert_eq!(args["fork"], false);
    assert!(args["resume"].is_null());
    peer.close().await;
}

#[tokio::test]
async fn invalid_recovery_is_atomic_and_legacy_records_fail_closed() {
    let state = tempfile::tempdir().unwrap();
    let mut peer = Peer::open(state.path()).await;
    let thread = peer.thread("legacy").await;
    let first = peer.say(&thread, "first").await;
    peer.say(&thread, "second").await;
    for (method, params) in [
        (
            "thread/revert",
            json!({"threadId":thread,"beforeTurnId":first["id"]}),
        ),
        ("thread/rollback", json!({"threadId":thread,"numTurns":0})),
        (
            "thread/fork",
            json!({"threadId":thread,"beforeTurnId":"unknown"}),
        ),
        (
            "thread/fork",
            json!({"threadId":thread,"beforeTurnId":first["id"],"lastTurnId":first["id"]}),
        ),
    ] {
        let before = record(state.path(), &thread);
        assert!(peer.request(method, params).await.get("error").is_some());
        assert_eq!(record(state.path(), &thread), before);
    }
    let active = peer.start(&thread, "hang").await;
    assert!(
        peer.request("thread/rollback", json!({"threadId":thread,"numTurns":1}))
            .await
            .get("error")
            .is_some()
    );
    peer.ok("turn/interrupt", json!({"threadId":thread,"turnId":active}))
        .await;
    peer.complete(&active).await;
    peer.close().await;

    let path = state.path().join("threads").join(format!("{thread}.json"));
    let mut legacy = record(state.path(), &thread);
    legacy.as_object_mut().unwrap().remove("turn_anchors");
    legacy
        .as_object_mut()
        .unwrap()
        .remove("tracks_turn_anchors");
    std::fs::write(&path, serde_json::to_vec_pretty(&legacy).unwrap()).unwrap();
    let mut peer = Peer::open(state.path()).await;
    peer.ok("thread/resume", json!({"threadId":thread})).await;
    let before = record(state.path(), &thread);
    let error = peer
        .request("thread/rollback", json!({"threadId":thread,"numTurns":1}))
        .await;
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("exact Claude transcript anchor")
    );
    assert_eq!(record(state.path(), &thread), before);
    let fork = peer.ok("thread/fork", json!({"threadId":thread})).await;
    let checkpoint =
        json!({"session_id":before["session_id"],"message_id":before["backend_message_id"]});
    assert_fork(
        &peer.args(fork["thread"]["id"].as_str().unwrap()).await,
        &checkpoint,
    );
    peer.close().await;
}

#[tokio::test]
async fn native_startup_failure_cannot_reuse_the_previous_turn_anchor() {
    let state = tempfile::tempdir().unwrap();
    let mut peer = Peer::open(state.path()).await;
    let thread = peer.thread("paginated").await;
    let first = peer.say(&thread, "retained native history").await;
    let successful = record(state.path(), &thread);
    peer.close().await;

    // Crash before native initialization or prompt consumption, so the failed
    // facade turn has no native UUID. The preceding UUID still exists globally.
    let mut peer = Peer::open_with_args(state.path(), &["--claude-arg=--fake-init-crash"]).await;
    peer.ok("thread/resume", json!({"threadId":thread})).await;
    let failed_id = peer.start(&thread, "never consumed by Claude").await;
    let failed = peer.complete(&failed_id).await;
    assert_eq!(failed["status"], "failed");
    let crashed = record(state.path(), &thread);
    assert_eq!(crashed["tracks_turn_anchors"], true);
    assert_eq!(
        crashed["backend_message_id"],
        successful["backend_message_id"]
    );
    assert!(crashed["turn_anchors"].get(&failed_id).is_none());
    for params in [
        json!({"threadId":thread}),
        json!({"threadId":thread,"lastTurnId":failed_id}),
    ] {
        let rejected = peer.request("thread/fork", params).await;
        assert!(
            rejected["error"]["message"]
                .as_str()
                .unwrap()
                .contains("exact Claude transcript anchor")
        );
        assert_eq!(record(state.path(), &thread), crashed);
    }
    peer.close().await;

    let mut peer = Peer::open(state.path()).await;
    peer.ok("thread/resume", json!({"threadId":thread})).await;
    let next = peer.say(&thread, "later native turn").await;
    let before_revert = record(state.path(), &thread);
    let rejected = peer
        .request(
            "thread/revert",
            json!({"threadId":thread,"beforeTurnId":next["id"]}),
        )
        .await;
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap()
            .contains("exact Claude transcript anchor")
    );
    assert_eq!(record(state.path(), &thread), before_revert);
    // Excluding the failed turn itself retains a known valid native prefix.
    peer.ok(
        "thread/revert",
        json!({"threadId":thread,"beforeTurnId":failed_id}),
    )
    .await;
    let checkpoint = &successful["turn_anchors"][first["id"].as_str().unwrap()];
    assert_fork(&peer.args(&thread).await, checkpoint);
    peer.close().await;
}
