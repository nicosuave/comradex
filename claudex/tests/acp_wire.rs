#![cfg(feature = "test-backend")]
//! The Codex app-server surface driven by an ACP agent. The fake agent checks
//! every frame the facade sends against the ACP schema; these tests check every
//! frame the facade sends to Codex against the Codex schema.

use serde_json::{Value, json};
use std::{collections::HashMap, path::Path, process::Stdio, sync::OnceLock, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
};

const DEADLINE: Duration = Duration::from_secs(20);

fn schemas() -> &'static HashMap<String, jsonschema::Validator> {
    static SCHEMAS: OnceLock<HashMap<String, jsonschema::Validator>> = OnceLock::new();
    SCHEMAS.get_or_init(|| {
        let root: Value =
            serde_json::from_str(include_str!("../protocol/codex-0.170.0.json")).unwrap();
        let mut validators = HashMap::new();
        for name in [
            "ServerNotification",
            "ServerRequest",
            "JSONRPCResponse",
            "JSONRPCErrorError",
        ] {
            let mut schema = root["definitions"][name].clone();
            schema["definitions"] = root["definitions"].clone();
            validators.insert(name.to_owned(), jsonschema::validator_for(&schema).unwrap());
        }
        for name in [
            "InitializeResponse",
            "ThreadStartResponse",
            "ThreadResumeResponse",
            "ThreadForkResponse",
            "ThreadReadResponse",
            "ThreadListResponse",
            "TurnStartResponse",
            "TurnInterruptResponse",
            "TurnSteerResponse",
            "ModelListResponse",
            "ConfigReadResponse",
            "ConfigRequirementsReadResponse",
            "PluginListResponse",
            "PluginInstalledResponse",
            "PluginSearchResponse",
            "ThreadApproveGuardianDeniedActionResponse",
        ] {
            let mut schema = root["definitions"]["v2"][name].clone();
            if schema.is_null() {
                schema = root["definitions"][name].clone();
            }
            assert!(schema.is_object(), "missing upstream definition {name}");
            schema["definitions"] = root["definitions"].clone();
            validators.insert(name.to_owned(), jsonschema::validator_for(&schema).unwrap());
        }
        validators
    })
}

fn validate(name: &str, value: &Value) {
    let errors: Vec<_> = schemas()[name]
        .iter_errors(value)
        .map(|error| format!("{}: {error}", error.instance_path))
        .collect();
    assert!(
        errors.is_empty(),
        "{name} violated:\n{}\nWire value: {value}",
        errors.join("\n")
    );
}

fn response_schema(method: &str) -> &'static str {
    match method {
        "initialize" => "InitializeResponse",
        "thread/start" => "ThreadStartResponse",
        "thread/resume" => "ThreadResumeResponse",
        "thread/fork" => "ThreadForkResponse",
        "thread/read" => "ThreadReadResponse",
        "thread/list" => "ThreadListResponse",
        "turn/start" => "TurnStartResponse",
        "turn/interrupt" => "TurnInterruptResponse",
        "turn/steer" => "TurnSteerResponse",
        "model/list" => "ModelListResponse",
        "config/read" => "ConfigReadResponse",
        "configRequirements/read" => "ConfigRequirementsReadResponse",
        "plugin/list" => "PluginListResponse",
        "plugin/installed" => "PluginInstalledResponse",
        "plugin/search" => "PluginSearchResponse",
        "thread/approveGuardianDeniedAction" => "ThreadApproveGuardianDeniedActionResponse",
        _ => panic!("unexpected success for {method}"),
    }
}

struct Client {
    child: Child,
    input: Option<ChildStdin>,
    output: Lines<BufReader<ChildStdout>>,
    methods: HashMap<String, String>,
    history: Vec<Value>,
    serial: u64,
}

impl Client {
    /// A facade serving the fake ACP agent, which keeps sessions in `state`.
    async fn spawn(state: &Path, agent_flags: &[&str]) -> Self {
        Self::spawn_with(state, agent_flags, &[]).await
    }

    /// `facade_args` are passed to the facade itself.
    async fn spawn_with(state: &Path, agent_flags: &[&str], facade_args: &[&str]) -> Self {
        Self::spawn_with_env(state, agent_flags, facade_args, &[]).await
    }

    async fn spawn_with_env(
        state: &Path,
        agent_flags: &[&str],
        facade_args: &[&str],
        env: &[(&str, &Path)],
    ) -> Self {
        schemas();
        let mut command = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"));
        command
            .args([
                "app-server",
                "--stdio",
                "--acp-agent",
                env!("CARGO_BIN_EXE_fake-acp"),
                "--provider-id",
                "fake",
                "--provider-name",
                "Fake ACP",
                "--initialize-timeout-seconds",
                "5",
                "--approval-timeout-seconds",
                "10",
                "--claude",
                env!("CARGO_BIN_EXE_fake-reviewer"),
                "--auto-review-timeout-seconds",
                "3",
            ])
            .arg(format!(
                "--acp-arg=--store={}",
                state.join("agent").display()
            ));
        for flag in agent_flags {
            command.arg(format!("--acp-arg={flag}"));
        }
        command.args(facade_args);
        // Opt into a fixture plugin runtime without inheriting the host's Codex.
        command
            .env_remove("CLAUDE_CODEX_PLUGIN_EXECUTABLE")
            .envs(env.iter().copied());
        let mut child = command
            .arg("--state-dir")
            .arg(state.join("facade"))
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
            methods: HashMap::new(),
            history: vec![],
            serial: 0,
        };
        client
            .ok(
                "initialize",
                json!({"clientInfo":{"name":"acp-wire","version":"1"},"capabilities":{"experimentalApi":true}}),
            )
            .await;
        client.send(json!({"method":"initialized"})).await;
        client
    }

    async fn send(&mut self, value: Value) {
        tokio::time::timeout(
            DEADLINE,
            self.input
                .as_mut()
                .unwrap()
                .write_all(format!("{value}\n").as_bytes()),
        )
        .await
        .unwrap()
        .unwrap();
    }

    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(DEADLINE, self.output.next_line())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "facade response timed out; last frames: {:?}",
                    self.history.iter().rev().take(3).collect::<Vec<_>>()
                )
            })
            .unwrap()
            .expect("facade unexpectedly closed stdout");
        let value: Value = serde_json::from_str(&line).expect("stdout must carry JSON frames");
        if value.get("method").is_some() {
            validate(
                if value.get("id").is_some() {
                    "ServerRequest"
                } else {
                    "ServerNotification"
                },
                &value,
            );
        } else if value.get("error").is_some() {
            validate("JSONRPCErrorError", &value["error"]);
        } else {
            validate("JSONRPCResponse", &value);
            let method = self
                .methods
                .get(&value["id"].to_string())
                .expect("response ID must belong to a request");
            validate(response_schema(method), &value["result"]);
        }
        self.history.push(value.clone());
        value
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        self.serial += 1;
        let id = json!(format!("request-{}", self.serial));
        self.methods.insert(id.to_string(), method.to_owned());
        self.send(json!({"id":id,"method":method,"params":params}))
            .await;
        loop {
            let value = self.read().await;
            if value.get("method").is_none() && value["id"] == id {
                return value;
            }
        }
    }

    async fn ok(&mut self, method: &str, params: Value) -> Value {
        let value = self.request(method, params).await;
        assert!(value.get("error").is_none(), "{method} failed: {value}");
        value["result"].clone()
    }

    async fn error(&mut self, method: &str, params: Value) -> String {
        let value = self.request(method, params).await;
        value["error"]["message"]
            .as_str()
            .unwrap_or_else(|| panic!("{method} unexpectedly succeeded: {value}"))
            .to_owned()
    }

    async fn thread(&mut self, params: Value) -> String {
        self.ok("thread/start", params).await["thread"]["id"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    async fn start(&mut self, thread: &str, text: &str) -> String {
        self.start_with(thread, text, json!({})).await
    }

    async fn start_with(&mut self, thread: &str, text: &str, extra: Value) -> String {
        let mut params =
            json!({"threadId":thread,"input":[{"type":"text","text":text,"text_elements":[]}]});
        for (key, value) in extra.as_object().unwrap() {
            params[key] = value.clone();
        }
        self.ok("turn/start", params).await["turn"]["id"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    async fn until(&mut self, predicate: impl Fn(&Value) -> bool) -> Value {
        if let Some(value) = self.history.iter().find(|value| predicate(value)) {
            return value.clone();
        }
        loop {
            let value = self.read().await;
            if predicate(&value) {
                return value;
            }
        }
    }

    async fn completed(&mut self, turn: &str) -> Value {
        self.until(|v| v["method"] == "turn/completed" && v["params"]["turn"]["id"] == turn)
            .await["params"]["turn"]
            .clone()
    }

    async fn server_request(&mut self, method: &str, turn: &str) -> Value {
        self.until(|v| {
            v["method"] == method && v.get("id").is_some() && v["params"]["turnId"] == turn
        })
        .await
    }

    async fn answer(&mut self, request: &Value, result: Value) {
        self.send(json!({"id":request["id"],"result":result})).await;
    }

    /// The final text of the turn's agent messages.
    fn reply(turn: &Value) -> String {
        turn["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["type"] == "agentMessage")
            .filter_map(|item| item["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn item<'a>(turn: &'a Value, kind: &str) -> &'a Value {
        turn["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == kind)
            .unwrap_or_else(|| panic!("no {kind} item in {turn}"))
    }

    fn turn_events(&self, turn: &str, method: &str) -> Vec<Value> {
        self.history
            .iter()
            .filter(|v| v["method"] == method && v["params"]["turnId"] == turn)
            .cloned()
            .collect()
    }

    async fn close(mut self) {
        self.input.take();
        let status = tokio::time::timeout(DEADLINE, self.child.wait())
            .await
            .expect("facade did not stop after stdin EOF")
            .unwrap();
        assert!(status.success(), "facade exit: {status}");
    }
}

fn full_access() -> Value {
    json!({"sandbox":"danger-full-access"})
}

#[tokio::test]
async fn turns_stream_messages_reasoning_usage_and_identify_the_provider() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path(), &[]).await;
    // The default thread uses full access: the facade cannot confine agent tools.
    let started = client.ok("thread/start", json!({})).await;
    assert_eq!(started["sandbox"]["type"], "dangerFullAccess");
    assert_eq!(started["modelProvider"], "fake");
    assert_eq!(started["model"], "default");
    let thread = started["thread"]["id"].as_str().unwrap().to_owned();

    let turn = client.start(&thread, "hello").await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "completed", "{completed}");
    assert_eq!(Client::reply(&completed), "Echo: hello");
    let deltas: Vec<String> = client
        .turn_events(&turn, "item/agentMessage/delta")
        .iter()
        .map(|v| v["params"]["delta"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(deltas.concat(), "Echo: hello");
    assert!(deltas.len() > 1, "chunks stream as separate deltas");
    // Prompt usage reaches the thread's billing total.
    let usage = client
        .until(|v| {
            v["method"] == "thread/tokenUsage/updated" && v["params"]["turnId"] == turn.as_str()
        })
        .await;
    assert_eq!(usage["params"]["tokenUsage"]["total"]["outputTokens"], 10);
    assert_eq!(
        usage["params"]["tokenUsage"]["total"]["cachedInputTokens"],
        5
    );

    let turn = client.start(&thread, "think").await;
    let completed = client.completed(&turn).await;
    assert_eq!(
        Client::item(&completed, "reasoning")["content"][0],
        "pondering"
    );

    let turn = client.start(&thread, "usage").await;
    client.completed(&turn).await;
    let usage = client
        .until(|v| {
            v["method"] == "thread/tokenUsage/updated"
                && v["params"]["turnId"] == turn.as_str()
                && v["params"]["tokenUsage"]["last"]["inputTokens"] == 1234
        })
        .await;
    assert_eq!(usage["params"]["tokenUsage"]["modelContextWindow"], 200000);

    let config = client.ok("config/read", json!({})).await;
    assert_eq!(config["config"]["model_provider"], "fake");
    assert_eq!(
        config["config"]["model_providers"]["fake"]["name"],
        "Fake ACP"
    );
    assert_eq!(config["config"]["sandbox_mode"], "danger-full-access");
    let requirements = client.ok("configRequirements/read", Value::Null).await;
    assert_eq!(
        requirements["requirements"]["allowedSandboxModes"],
        json!(["danger-full-access"])
    );
    let listed = client
        .ok("thread/list", json!({"modelProviders":["fake"]}))
        .await;
    assert_eq!(listed["data"].as_array().unwrap().len(), 1);
    let listed = client
        .ok("thread/list", json!({"modelProviders":["anthropic"]}))
        .await;
    assert!(listed["data"].as_array().unwrap().is_empty());
    for method in ["plugin/list", "plugin/installed"] {
        assert_eq!(
            client.ok(method, json!({})).await["marketplaces"],
            json!([])
        );
    }
    client.close().await;
}

#[tokio::test]
async fn later_turns_resume_or_load_the_session_and_instructions_arrive_once() {
    // Without kept processes every turn starts the agent and resumes or loads.
    let cold = ["--idle-session-timeout-seconds", "0"];
    for flags in [&[][..], &["--no-resume"][..]] {
        let state = tempfile::tempdir().unwrap();
        let mut client = Client::spawn_with(state.path(), flags, &cold).await;
        let thread = client
            .thread(json!({"sandbox":"danger-full-access","developerInstructions":"Be terse."}))
            .await;
        let turn = client.start(&thread, "hello").await;
        assert_eq!(client.completed(&turn).await["status"], "completed");
        let turn = client.start(&thread, "history").await;
        let completed = client.completed(&turn).await;
        assert_eq!(
            Client::reply(&completed),
            "turns=2 instructions=true model=fake-a effort=medium",
            "flags {flags:?}"
        );
        // A load replay must not reappear as items in the resumed turn.
        assert_eq!(
            completed["items"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|item| item["type"] == "agentMessage")
                .count(),
            1,
            "flags {flags:?}: {completed}"
        );
        client.close().await;
    }
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn_with(state.path(), &["--no-resume", "--no-load"], &cold).await;
    let thread = client.thread(full_access()).await;
    let turn = client.start(&thread, "hello").await;
    assert_eq!(client.completed(&turn).await["status"], "completed");
    let turn = client.start(&thread, "hello").await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "failed");
    assert!(
        completed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("can neither resume nor load sessions"),
        "{completed}"
    );
    client.close().await;
}

#[tokio::test]
async fn model_list_and_turn_settings_use_session_config_options() {
    for flags in [&[][..], &["--legacy-models"][..]] {
        let state = tempfile::tempdir().unwrap();
        let mut client = Client::spawn(state.path(), flags).await;
        let models = client.ok("model/list", json!({})).await["data"].clone();
        let ids: Vec<&str> = models
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["default", "fake-a", "fake-b"], "flags {flags:?}");
        assert_eq!(models[0]["isDefault"], true);
        assert_eq!(models[0]["displayName"], "Agent default");
        if flags.is_empty() {
            let efforts: Vec<&str> = models[1]["supportedReasoningEfforts"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e["reasoningEffort"].as_str().unwrap())
                .collect();
            assert_eq!(efforts, ["low", "medium", "high"]);
        }
        // The probe session is ephemeral and deleted afterwards.
        let deleted = std::fs::read_dir(state.path().join("agent"))
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().ends_with(".deleted"));
        assert!(deleted, "model/list probe session was not deleted");

        let thread = client
            .thread(json!({"sandbox":"danger-full-access","model":"fake-b","effort":"high"}))
            .await;
        let turn = client.start(&thread, "history").await;
        let completed = client.completed(&turn).await;
        let expected_effort = if flags.is_empty() { "high" } else { "medium" };
        assert_eq!(
            Client::reply(&completed),
            // The desktop visualization guidance is always delivered as instructions.
            format!("turns=1 instructions=true model=fake-b effort={expected_effort}"),
            "flags {flags:?}"
        );
        // A later turn can switch models on the resumed session.
        let turn = client
            .start_with(&thread, "history", json!({"model":"fake-a"}))
            .await;
        let completed = client.completed(&turn).await;
        assert!(
            Client::reply(&completed).contains("model=fake-a"),
            "{completed}"
        );
        let turn = client
            .start_with(&thread, "hello", json!({"model":"missing-model"}))
            .await;
        let completed = client.completed(&turn).await;
        assert_eq!(completed["status"], "failed");
        assert!(
            completed["error"]["message"]
                .as_str()
                .unwrap()
                .contains("not offered by this ACP agent"),
            "{completed}"
        );
        client.close().await;
    }
}

#[tokio::test]
async fn agent_permission_requests_use_codex_approvals() {
    let state = tempfile::tempdir().unwrap();
    std::fs::write(state.path().join("notes.txt"), "old\n").unwrap();
    let mut client = Client::spawn(state.path(), &[]).await;
    let thread = client
        .thread(json!({"sandbox":"danger-full-access","cwd":state.path()}))
        .await;

    let turn = client.start(&thread, "tool-exec").await;
    let request = client
        .server_request("item/commandExecution/requestApproval", &turn)
        .await;
    assert_eq!(request["params"]["command"], "echo hi");
    client.answer(&request, json!({"decision":"accept"})).await;
    let completed = client.completed(&turn).await;
    let command = Client::item(&completed, "commandExecution");
    assert_eq!(command["status"], "completed");
    assert_eq!(command["command"], "echo hi");
    assert_eq!(command["aggregatedOutput"], "hi\nthere");
    assert_eq!(command["exitCode"], 0);
    let output: String = client
        .turn_events(&turn, "item/commandExecution/outputDelta")
        .iter()
        .map(|v| v["params"]["delta"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(output, "hi\nthere");
    assert_eq!(Client::reply(&completed), "ran");

    let turn = client.start(&thread, "tool-exec").await;
    let request = client
        .server_request("item/commandExecution/requestApproval", &turn)
        .await;
    client.answer(&request, json!({"decision":"decline"})).await;
    let completed = client.completed(&turn).await;
    assert_eq!(
        Client::item(&completed, "commandExecution")["status"],
        "failed"
    );
    assert_eq!(Client::reply(&completed), "denied");

    // A session grant answers the identical later request without prompting.
    let turn = client.start(&thread, "tool-exec").await;
    let request = client
        .server_request("item/commandExecution/requestApproval", &turn)
        .await;
    client
        .answer(&request, json!({"decision":"acceptForSession"}))
        .await;
    assert_eq!(Client::reply(&client.completed(&turn).await), "ran");
    let turn = client.start(&thread, "tool-exec").await;
    let completed = client.completed(&turn).await;
    assert_eq!(Client::reply(&completed), "ran");
    assert!(
        client
            .turn_events(&turn, "item/commandExecution/requestApproval")
            .is_empty()
    );

    let turn = client.start(&thread, "tool-edit").await;
    let request = client
        .server_request("item/fileChange/requestApproval", &turn)
        .await;
    client.answer(&request, json!({"decision":"accept"})).await;
    let completed = client.completed(&turn).await;
    let change = Client::item(&completed, "fileChange");
    assert_eq!(change["status"], "completed");
    let diff = change["changes"][0]["diff"].as_str().unwrap();
    assert!(diff.contains("-old") && diff.contains("+new"), "{diff}");
    assert_eq!(change["changes"][0]["kind"]["type"], "update");

    let turn = client.start(&thread, "tool-new-file").await;
    let completed = client.completed(&turn).await;
    assert_eq!(
        Client::item(&completed, "fileChange")["changes"][0]["kind"]["type"],
        "add"
    );

    let turn = client.start(&thread, "tool-read").await;
    let request = client
        .server_request("item/permissions/requestApproval", &turn)
        .await;
    let path = state.path().canonicalize().unwrap().join("notes.txt");
    assert_eq!(
        request["params"]["permissions"]["fileSystem"]["read"],
        json!([path])
    );
    client
        .answer(
            &request,
            json!({"permissions":{"fileSystem":{"read":[path]}},"scope":"turn"}),
        )
        .await;
    let completed = client.completed(&turn).await;
    assert_eq!(Client::reply(&completed), "read yes");

    let turn = client.start(&thread, "tool-fetch").await;
    let request = client
        .server_request("item/tool/requestUserInput", &turn)
        .await;
    client
        .answer(
            &request,
            json!({"answers":{"permission":{"answers":["Deny"]}}}),
        )
        .await;
    let completed = client.completed(&turn).await;
    assert_eq!(Client::reply(&completed), "fetch no");
    let fetch = Client::item(&completed, "dynamicToolCall");
    assert_eq!(fetch["namespace"], "fake");
    assert_eq!(fetch["tool"], "Fetch example");
    assert_eq!(fetch["contentItems"][0]["text"], "page");

    // approvalPolicy=never denies without prompting, as for Claude.
    let turn = client
        .start_with(&thread, "tool-edit", json!({"approvalPolicy":"never"}))
        .await;
    let completed = client.completed(&turn).await;
    assert_eq!(Client::reply(&completed), "edit no");
    assert!(
        client
            .turn_events(&turn, "item/fileChange/requestApproval")
            .is_empty()
    );
    client.close().await;
}

#[tokio::test]
async fn interrupts_cancel_prompts_and_outstanding_approvals() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path(), &[]).await;
    let thread = client.thread(full_access()).await;
    let turn = client.start(&thread, "slow").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    client
        .ok("turn/interrupt", json!({"threadId":thread,"turnId":turn}))
        .await;
    assert_eq!(client.completed(&turn).await["status"], "interrupted");

    let turn = client.start(&thread, "tool-exec-cancel").await;
    let request = client
        .server_request("item/commandExecution/requestApproval", &turn)
        .await;
    client
        .ok("turn/interrupt", json!({"threadId":thread,"turnId":turn}))
        .await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "interrupted");
    client
        .until(|v| {
            v["method"] == "serverRequest/resolved" && v["params"]["requestId"] == request["id"]
        })
        .await;

    // The session still works after a cancelled turn.
    let turn = client.start(&thread, "hello").await;
    assert_eq!(Client::reply(&client.completed(&turn).await), "Echo: hello");

    // An agent withdrawing its own request resolves the approval.
    let turn = client.start(&thread, "cancel-request").await;
    let request = client
        .server_request("item/commandExecution/requestApproval", &turn)
        .await;
    client
        .until(|v| {
            v["method"] == "serverRequest/resolved" && v["params"]["requestId"] == request["id"]
        })
        .await;
    assert_eq!(Client::reply(&client.completed(&turn).await), "withdrawn");
    client.close().await;
}

#[tokio::test]
async fn steering_input_runs_as_a_follow_up_prompt_in_the_same_turn() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path(), &[]).await;
    let thread = client.thread(full_access()).await;
    let turn = client.start(&thread, "steer").await;
    let steered = client
        .ok(
            "turn/steer",
            json!({"threadId":thread,"expectedTurnId":turn,
                "input":[{"type":"text","text":"after","text_elements":[]}]}),
        )
        .await;
    assert_eq!(steered["turnId"], turn);
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "completed");
    assert_eq!(Client::reply(&completed), "steer acknowledged\nEcho: after");
    assert_eq!(
        completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["type"] == "userMessage")
            .count(),
        2
    );
    client.close().await;
}

#[tokio::test]
async fn forks_copy_the_session_and_history_rewinds_are_refused() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path(), &[]).await;
    let thread = client.thread(full_access()).await;
    let first = client.start(&thread, "hello").await;
    client.completed(&first).await;

    assert!(
        client
            .error("thread/rollback", json!({"threadId":thread,"numTurns":1}))
            .await
            .contains("cannot rewind")
    );
    assert!(
        client
            .error(
                "thread/fork",
                json!({"threadId":thread,"beforeTurnId":first})
            )
            .await
            .contains("only at its latest turn")
    );

    let fork = client
        .ok("thread/fork", json!({"threadId":thread,"lastTurnId":first}))
        .await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let turn = client.start(&fork, "history").await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "completed", "{completed}");
    assert!(
        Client::reply(&completed).starts_with("turns=2"),
        "{completed}"
    );
    // The source keeps its own session.
    let turn = client.start(&thread, "history").await;
    assert!(Client::reply(&client.completed(&turn).await).starts_with("turns=2"));

    // A fork whose source advanced before its first turn would not be exact.
    let stale = client.ok("thread/fork", json!({"threadId":thread})).await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let turn = client.start(&thread, "hello").await;
    client.completed(&turn).await;
    assert!(
        client
            .error(
                "turn/start",
                json!({"threadId":stale,"input":[{"type":"text","text":"hello","text_elements":[]}]})
            )
            .await
            .contains("continued after this fork")
    );
    client.close().await;

    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path(), &["--no-fork"]).await;
    let thread = client.thread(full_access()).await;
    let turn = client.start(&thread, "hello").await;
    client.completed(&turn).await;
    assert!(
        client
            .error("thread/fork", json!({"threadId":thread}))
            .await
            .contains("does not support forking")
    );
    client.close().await;
}

#[tokio::test]
async fn settings_an_agent_cannot_honor_are_rejected() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path(), &["--no-images"]).await;
    for params in [
        json!({"sandbox":"workspace-write"}),
        json!({"permissions":":workspace"}),
    ] {
        let message = client.error("thread/start", params.clone()).await;
        assert!(message.contains("cannot confine"), "{params}: {message}");
    }
    let thread = client.thread(full_access()).await;
    assert!(
        client
            .error(
                "turn/start",
                json!({"threadId":thread,"outputSchema":{"type":"object"},
                    "input":[{"type":"text","text":"hello","text_elements":[]}]})
            )
            .await
            .contains("structured output")
    );
    let image = "data:image/png;base64,iVBORw0KGgo=";
    let turn = client
        .ok(
            "turn/start",
            json!({"threadId":thread,"input":[{"type":"image","url":image},
                {"type":"text","text":"image","text_elements":[]}]}),
        )
        .await["turn"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "failed");
    assert!(
        completed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("does not accept image input")
    );
    client.close().await;

    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path(), &[]).await;
    let thread = client.thread(full_access()).await;
    let turn = client
        .ok(
            "turn/start",
            json!({"threadId":thread,"input":[{"type":"image","url":image},
                {"type":"image","url":"https://example.com/cat.png"},
                {"type":"text","text":"image","text_elements":[]}]}),
        )
        .await["turn"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        Client::reply(&client.completed(&turn).await),
        "images=image,resource_link"
    );
    client.close().await;
}

#[tokio::test]
async fn desktop_tools_reach_the_agent_through_the_mcp_bridge() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path(), &[]).await;
    let thread = client
        .thread(json!({"sandbox":"danger-full-access","dynamicTools":[
            {"type":"namespace","name":"desktop","description":"Desktop tools","tools":[
                {"type":"function","name":"lookup","description":"Look something up",
                 "inputSchema":{"type":"object","properties":{"query":{"type":"string"}},
                                "required":["query"],"additionalProperties":false}}]}]}))
        .await;
    let turn = client.start(&thread, "desktop-tool").await;
    let call = client.server_request("item/tool/call", &turn).await;
    assert_eq!(call["params"]["namespace"], "desktop");
    assert_eq!(call["params"]["tool"], "lookup");
    assert_eq!(call["params"]["arguments"], json!({"query":"abc"}));
    // The facade validated and allowed the agent's permission request itself.
    assert!(
        client
            .history
            .iter()
            .all(|v| v["method"] != "item/tool/requestUserInput")
    );
    client
        .answer(
            &call,
            json!({"success":true,"contentItems":[{"type":"inputText","text":"found it"}]}),
        )
        .await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "completed", "{completed}");
    assert_eq!(Client::reply(&completed), "desktop result: found it");
    // Only the facade's own item represents the desktop call.
    let dynamic: Vec<&Value> = completed["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["type"] == "dynamicToolCall")
        .collect();
    assert_eq!(dynamic.len(), 1, "{completed}");
    assert_eq!(dynamic[0]["success"], true);
    client.close().await;
}

#[tokio::test]
async fn codex_connector_tools_are_discoverable_in_new_and_resumed_acp_sessions() {
    let state = tempfile::tempdir().unwrap();
    let codex = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/plugin_runtime.sh");
    let mut client = Client::spawn_with_env(
        state.path(),
        &[],
        &["--idle-session-timeout-seconds", "0"],
        &[
            ("CLAUDE_CODEX_PLUGIN_EXECUTABLE", &codex),
            ("CLAUDE_CODEX_PLUGIN_HOME", state.path()),
        ],
    )
    .await;
    let thread = client.thread(full_access()).await;
    for _ in 0..2 {
        let turn = client.start(&thread, "plugin-tool").await;
        let approval = client
            .server_request("item/tool/requestUserInput", &turn)
            .await;
        assert!(
            approval["params"]["questions"][0]["question"]
                .as_str()
                .unwrap()
                .contains("codex_plugin_")
        );
        client
            .answer(
                &approval,
                json!({"answers":{"permission":{"answers":["Allow"]}}}),
            )
            .await;
        let completed = client.completed(&turn).await;
        assert_eq!(completed["status"], "completed", "{completed}");
        assert_eq!(
            Client::reply(&completed),
            "desktop result: Executed fixture"
        );
        assert!(
            completed["items"].as_array().unwrap().iter().any(|item| {
                item["type"] == "mcpToolCall"
                    && item["server"] == "fixture_server"
                    && item["tool"] == "token"
                    && item["status"] == "completed"
            }),
            "{completed}"
        );
    }
    client.close().await;
}

#[tokio::test]
async fn agent_updates_stop_reasons_and_failures_are_reported() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path(), &[]).await;
    let thread = client.thread(full_access()).await;

    let turn = client.start(&thread, "plan").await;
    client.completed(&turn).await;
    let plan = client.turn_events(&turn, "turn/plan/updated");
    assert_eq!(
        plan[0]["params"]["plan"],
        json!([{"step":"Investigate","status":"completed"},{"step":"Fix","status":"inProgress"},
            {"step":"Verify","status":"pending"}])
    );

    let turn = client.start(&thread, "notice").await;
    let completed = client.completed(&turn).await;
    Client::item(&completed, "contextCompaction");
    assert!(
        client
            .turn_events(&turn, "warning")
            .iter()
            .any(|v| v["params"]["message"] == "Quota nearly exhausted")
    );

    let turn = client.start(&thread, "fs-request").await;
    assert_eq!(
        Client::reply(&client.completed(&turn).await),
        "fs error -32601"
    );

    for (script, expected) in [
        ("refuse", "refused to continue"),
        ("max-tokens", "maximum token limit"),
        ("error", "scripted failure"),
        ("crash", "crashed on purpose"),
    ] {
        let turn = client.start(&thread, script).await;
        let completed = client.completed(&turn).await;
        assert_eq!(completed["status"], "failed", "{script}: {completed}");
        assert!(
            completed["error"]["message"]
                .as_str()
                .unwrap()
                .contains(expected),
            "{script}: {completed}"
        );
    }
    // A crashed agent process does not poison the thread.
    let turn = client.start(&thread, "hello").await;
    assert_eq!(client.completed(&turn).await["status"], "completed");
    client.close().await;

    for (flag, expected) in [
        ("--auth-required", "requires authentication (Fake login)"),
        ("--version=2", "negotiated protocol version 2"),
    ] {
        let state = tempfile::tempdir().unwrap();
        let mut client = Client::spawn(state.path(), &[flag]).await;
        let thread = client.thread(full_access()).await;
        let turn = client.start(&thread, "hello").await;
        let completed = client.completed(&turn).await;
        assert_eq!(completed["status"], "failed");
        assert!(
            completed["error"]["message"]
                .as_str()
                .unwrap()
                .contains(expected),
            "{flag}: {completed}"
        );
        client.close().await;
    }
}

fn auto_review() -> Value {
    json!({"sandbox":"danger-full-access","approvalsReviewer":"auto_review"})
}

/// The completed review notifications of a turn, in order.
fn reviews(client: &Client, turn: &str) -> Vec<Value> {
    client.turn_events(turn, "item/autoApprovalReview/completed")
}

#[tokio::test]
async fn automatic_review_approves_or_denies_agent_requests_without_prompting() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path(), &[]).await;
    let requirements = client.ok("configRequirements/read", Value::Null).await;
    assert_eq!(
        requirements["requirements"]["allowedApprovalsReviewers"],
        json!(["user", "auto_review", "guardian_subagent"])
    );
    let thread = client.thread(auto_review()).await;

    let turn = client.start(&thread, "exec:ls -la").await;
    let completed = client.completed(&turn).await;
    assert_eq!(Client::reply(&completed), "exec yes approved-context=false");
    let started = client.turn_events(&turn, "item/autoApprovalReview/started");
    assert_eq!(started.len(), 1);
    assert_eq!(started[0]["params"]["review"]["status"], "inProgress");
    assert_eq!(
        started[0]["params"]["action"],
        json!({"type":"command","source":"shell","command":"ls -la",
            "cwd":started[0]["params"]["action"]["cwd"]})
    );
    let review = &reviews(&client, &turn)[0]["params"];
    assert_eq!(review["review"]["status"], "approved");
    assert_eq!(review["review"]["riskLevel"], "low");
    assert_eq!(review["decisionSource"], "agent");
    assert_eq!(review["reviewId"], started[0]["params"]["reviewId"]);
    assert_eq!(
        review["targetItemId"],
        Client::item(&completed, "commandExecution")["id"]
    );
    // The reviewer decides; the user is never asked.
    assert!(
        client
            .turn_events(&turn, "item/commandExecution/requestApproval")
            .is_empty()
    );

    let turn = client.start(&thread, "exec:rm -rf build").await;
    let completed = client.completed(&turn).await;
    assert_eq!(Client::reply(&completed), "exec no approved-context=false");
    let denied = reviews(&client, &turn)[0]["params"].clone();
    assert_eq!(denied["review"]["status"], "denied");
    assert_eq!(denied["review"]["riskLevel"], "high");
    assert!(
        denied["review"]["rationale"]
            .as_str()
            .unwrap()
            .contains("not requested")
    );

    // The user approves the denied action: the agent learns of it with its
    // next input, and that exact action is then allowed without another review.
    let event = json!({"id":denied["reviewId"],"turn_id":turn,"status":"denied",
        "action":denied["action"]});
    client
        .ok(
            "thread/approveGuardianDeniedAction",
            json!({"threadId":thread,"event":event}),
        )
        .await;
    assert!(
        client
            .error(
                "thread/approveGuardianDeniedAction",
                json!({"threadId":thread,"event":event})
            )
            .await
            .contains("not found")
    );
    let turn = client.start(&thread, "exec:rm -rf build").await;
    let completed = client.completed(&turn).await;
    assert_eq!(Client::reply(&completed), "exec yes approved-context=true");
    assert!(
        reviews(&client, &turn).is_empty(),
        "the session grant applies"
    );
    // A different payload is reviewed again, and later reviews see the approval.
    let turn = client.start(&thread, "exec:rm -rf dist").await;
    let completed = client.completed(&turn).await;
    assert_eq!(Client::reply(&completed), "exec yes approved-context=false");
    assert_eq!(
        reviews(&client, &turn)[0]["params"]["review"]["userAuthorization"],
        "high"
    );
    client.close().await;
}

#[tokio::test]
async fn automatic_review_fails_closed_and_stops_repeated_denials() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path(), &[]).await;
    let thread = client.thread(auto_review()).await;

    let turn = client.start(&thread, "exec:crash-review").await;
    let completed = client.completed(&turn).await;
    assert_eq!(Client::reply(&completed), "exec no approved-context=false");
    let review = &reviews(&client, &turn)[0]["params"]["review"];
    assert_eq!(review["status"], "denied");
    assert!(
        review["rationale"]
            .as_str()
            .unwrap()
            .contains("reviewer exploded"),
        "{review}"
    );

    let turn = client.start(&thread, "exec:slow-review").await;
    let completed = client.completed(&turn).await;
    assert_eq!(Client::reply(&completed), "exec no approved-context=false");
    assert_eq!(
        reviews(&client, &turn)[0]["params"]["review"]["status"],
        "timedOut"
    );

    // Three consecutive denials in a turn interrupt it, as in Codex.
    let turn = client.start(&thread, "exec*4:rm -rf /").await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "interrupted", "{completed}");
    assert_eq!(reviews(&client, &turn).len(), 3);
    assert!(
        client
            .history
            .iter()
            .any(|v| v["method"] == "guardianWarning" && v["params"]["threadId"] == thread)
    );

    // An interrupted review is reported as aborted.
    let turn = client.start(&thread, "exec:slow-review").await;
    client
        .until(|v| {
            v["method"] == "item/autoApprovalReview/started" && v["params"]["turnId"] == turn
        })
        .await;
    client
        .ok("turn/interrupt", json!({"threadId":thread,"turnId":turn}))
        .await;
    assert_eq!(client.completed(&turn).await["status"], "interrupted");
    assert_eq!(
        reviews(&client, &turn)[0]["params"]["review"]["status"],
        "aborted"
    );
    client.close().await;
}

#[tokio::test]
async fn idle_agent_processes_serve_later_turns_until_settings_change() {
    // An agent that can neither resume nor load still continues a thread
    // while the facade keeps its process between turns.
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path(), &["--no-resume", "--no-load"]).await;
    let thread = client.thread(full_access()).await;
    let turn = client.start(&thread, "hello").await;
    assert_eq!(client.completed(&turn).await["status"], "completed");
    let turn = client.start(&thread, "history").await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "completed", "{completed}");
    assert!(
        Client::reply(&completed).starts_with("turns=2 "),
        "{completed}"
    );
    // A different model needs a new launch, which this agent cannot resume into.
    let turn = client
        .start_with(&thread, "hello", json!({"model":"fake-b"}))
        .await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "failed");
    assert!(
        completed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("can neither resume nor load sessions"),
        "{completed}"
    );
    client.close().await;
}
