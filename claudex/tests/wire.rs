#![cfg(feature = "test-backend")]

use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    process::Stdio,
    sync::OnceLock,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
};

const DEADLINE: Duration = Duration::from_secs(15);

/// Compile independent upstream contracts once; do not derive expected fields
/// from the facade's constructors or reuse its request-only validators.
fn schemas() -> &'static HashMap<String, jsonschema::Validator> {
    static SCHEMAS: OnceLock<HashMap<String, jsonschema::Validator>> = OnceLock::new();
    SCHEMAS.get_or_init(|| {
        let root: Value =
            serde_json::from_str(include_str!("../protocol/codex-0.170.0.json")).unwrap();
        let mut validators = HashMap::new();
        for name in [
            "InitializeResponse",
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
            "ThreadTimelineListResponse",
            "FsReadFileResponse",
            "FsWriteFileResponse",
            "FsCopyResponse",
            "FsRemoveResponse",
            "CommandExecResponse",
            "ThreadStartResponse",
            "ThreadAttachmentAddResponse",
            "ThreadAttachmentListResponse",
            "ThreadAttachmentRemoveResponse",
            "ThreadResumeResponse",
            "ThreadForkResponse",
            "ThreadReadResponse",
            "ThreadListResponse",
            "ThreadLoadedListResponse",
            "ThreadTurnsListResponse",
            "ThreadItemsListResponse",
            "ThreadItemsReadResponse",
            "HooksListResponse",
            "FeedbackSaveResponse",
            "ThreadMemoryReadResponse",
            "ThreadAttachmentOwnerListResponse",
            "ListMcpServerStatusResponse",
            "ThreadReadStateUpdateResponse",
            "ThreadSetNameResponse",
            "ThreadMetadataUpdateResponse",
            "ThreadSettingsUpdateResponse",
            "ThreadArchiveResponse",
            "ThreadUnarchiveResponse",
            "ThreadDeleteResponse",
            "ThreadGoalGetResponse",
            "ThreadSearchOccurrencesResponse",
            "ThreadSectionMoveResponse",
            "TurnStartResponse",
            "TurnInterruptResponse",
            "TurnSteerResponse",
            "ThreadCompactStartResponse",
            "ThreadInjectItemsResponse",
            "ThreadQueueAddResponse",
            "ThreadQueueListResponse",
            "ThreadQueueUpdateResponse",
            "ThreadQueueDeleteResponse",
            "ThreadQueueReorderResponse",
            "ThreadQueueStartResponse",
            "ModelListResponse",
            "GetAccountResponse",
            "ConfigReadResponse",
            "ConfigWriteResponse",
            "FsCreateDirectoryResponse",
            "FsGetMetadataResponse",
            "FsReadDirectoryResponse",
            "ConfigRequirementsReadResponse",
            "CollaborationModeListResponse",
            "ExperimentalFeatureListResponse",
        ] {
            let mut schema = root["definitions"]["v2"][name].clone();
            assert!(schema.is_object(), "missing upstream definition {name}");
            schema["definitions"] = root["definitions"].clone();
            validators.insert(name.to_owned(), jsonschema::validator_for(&schema).unwrap());
        }
        // Legacy v1 response is not included in the schema export.
        validators.insert(
            "GetAuthStatusResponse".into(),
            jsonschema::validator_for(&json!({
                "type": "object", "required": ["authMethod", "authToken", "requiresOpenaiAuth"],
                "properties": {
                    "authMethod": {"type": "null"}, "authToken": {"type": "null"},
                    "requiresOpenaiAuth": {"const": false}
                }
            }))
            .unwrap(),
        );
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
        "thread/timeline/list" => "ThreadTimelineListResponse",
        "fs/readFile" => "FsReadFileResponse",
        "fs/writeFile" => "FsWriteFileResponse",
        "fs/copy" => "FsCopyResponse",
        "fs/remove" => "FsRemoveResponse",
        "command/exec" => "CommandExecResponse",
        "initialize" => "InitializeResponse",
        "thread/start" => "ThreadStartResponse",
        "thread/attachment/add" => "ThreadAttachmentAddResponse",
        "thread/attachment/list" => "ThreadAttachmentListResponse",
        "thread/attachment/remove" => "ThreadAttachmentRemoveResponse",
        "thread/resume" => "ThreadResumeResponse",
        "thread/fork" => "ThreadForkResponse",
        "thread/read" => "ThreadReadResponse",
        "thread/list" => "ThreadListResponse",
        "thread/loaded/list" => "ThreadLoadedListResponse",
        "thread/turns/list" => "ThreadTurnsListResponse",
        "thread/items/list" => "ThreadItemsListResponse",
        "thread/items/read" => "ThreadItemsReadResponse",
        "hooks/list" => "HooksListResponse",
        "feedback/save" => "FeedbackSaveResponse",
        "thread/memory/read" => "ThreadMemoryReadResponse",
        "thread/attachmentOwner/list" => "ThreadAttachmentOwnerListResponse",
        "mcpServerStatus/list" => "ListMcpServerStatusResponse",
        "thread/readState/update" => "ThreadReadStateUpdateResponse",
        "thread/name/set" => "ThreadSetNameResponse",
        "thread/metadata/update" => "ThreadMetadataUpdateResponse",
        "thread/settings/update" => "ThreadSettingsUpdateResponse",
        "thread/archive" => "ThreadArchiveResponse",
        "thread/unarchive" => "ThreadUnarchiveResponse",
        "thread/delete" => "ThreadDeleteResponse",
        "thread/goal/get" => "ThreadGoalGetResponse",
        "thread/searchOccurrences" => "ThreadSearchOccurrencesResponse",
        "thread/section/move" => "ThreadSectionMoveResponse",
        "turn/start" => "TurnStartResponse",
        "turn/interrupt" => "TurnInterruptResponse",
        "turn/steer" => "TurnSteerResponse",
        "thread/compact/start" => "ThreadCompactStartResponse",
        "thread/inject_items" => "ThreadInjectItemsResponse",
        "thread/queue/add" => "ThreadQueueAddResponse",
        "thread/queue/list" => "ThreadQueueListResponse",
        "thread/queue/update" => "ThreadQueueUpdateResponse",
        "thread/queue/delete" => "ThreadQueueDeleteResponse",
        "thread/queue/reorder" => "ThreadQueueReorderResponse",
        "thread/queue/start" => "ThreadQueueStartResponse",
        "model/list" => "ModelListResponse",
        "account/read" => "GetAccountResponse",
        "getAuthStatus" => "GetAuthStatusResponse",
        "config/read" => "ConfigReadResponse",
        "config/batchWrite" => "ConfigWriteResponse",
        "fs/createDirectory" => "FsCreateDirectoryResponse",
        "fs/getMetadata" => "FsGetMetadataResponse",
        "fs/readDirectory" => "FsReadDirectoryResponse",
        "configRequirements/read" => "ConfigRequirementsReadResponse",
        "collaborationMode/list" => "CollaborationModeListResponse",
        "experimentalFeature/list" => "ExperimentalFeatureListResponse",
        _ => panic!("unexpected success for unsupported method {method}"),
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

#[tokio::test]
async fn configured_workspace_defaults_are_visible_and_explicit_policies_win() {
    let state = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    std::fs::write(
        state.path().join("workspace-defaults.json"),
        json!({"writable_roots":[cache.path()],"network_access":true}).to_string(),
    )
    .unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let config = client.ok("config/read", json!({})).await;
    assert_eq!(
        config["config"]["sandbox_workspace_write"]["network_access"],
        true
    );
    assert_eq!(
        config["config"]["sandbox_workspace_write"]["writable_roots"],
        json!([cache.path()])
    );
    for params in [
        json!({}),
        json!({"permissions":":workspace"}),
        json!({"sandbox":"workspace-write"}),
    ] {
        let started = client.ok("thread/start", params).await;
        assert_eq!(started["sandbox"]["networkAccess"], true);
        assert_eq!(started["sandbox"]["writableRoots"], json!([cache.path()]));
    }
    let restricted = json!({"type":"workspaceWrite","networkAccess":false,"writableRoots":[],
        "excludeSlashTmp":false,"excludeTmpdirEnvVar":false});
    let started = client
        .ok("thread/start", json!({"sandboxPolicy":restricted}))
        .await;
    assert_eq!(started["sandbox"], restricted);
    let combined = client
        .ok(
            "thread/start",
            json!({"sandbox":"workspace-write","sandboxPolicy":restricted}),
        )
        .await;
    assert_eq!(combined["sandbox"], restricted);
    let id = &started["thread"]["id"];
    let resumed = client.ok("thread/resume", json!({"threadId":id})).await;
    assert_eq!(resumed["sandbox"], restricted);
    let resumed = client
        .ok(
            "thread/resume",
            json!({"threadId":id,"permissions":":workspace"}),
        )
        .await;
    assert_eq!(resumed["sandbox"]["networkAccess"], true);
    let forked = client
        .ok(
            "thread/fork",
            json!({"threadId":id,"sandboxPolicy":restricted}),
        )
        .await;
    assert_eq!(forked["sandbox"], restricted);
    for (selection, expected_network) in [
        (json!({"permissions":":workspace"}), true),
        (json!({"sandboxPolicy":restricted}), false),
    ] {
        let mut params = selection;
        params["threadId"] = id.clone();
        params["input"] = json!([{"type":"text","text":"hello","text_elements":[]}]);
        let turn = client.ok("turn/start", params).await;
        client.completed(turn["turn"]["id"].as_str().unwrap()).await;
        let resumed = client.ok("thread/resume", json!({"threadId":id})).await;
        assert_eq!(resumed["sandbox"]["networkAccess"], expected_network);
    }
    let full = client
        .ok("thread/start", json!({"permissions":":danger-full-access"}))
        .await;
    assert_eq!(full["sandbox"]["type"], "dangerFullAccess");
    client.close().await;
}

impl Client {
    async fn spawn(state: &Path) -> Self {
        Self::spawn_with_args(state, &[]).await
    }

    async fn spawn_with_args(state: &Path, extra: &[&str]) -> Self {
        Self::spawn_with_env(state, extra, &[]).await
    }

    async fn spawn_with_env(state: &Path, extra: &[&str], env: &[(&str, &Path)]) -> Self {
        schemas();
        let mut child = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
            .args([
                "app-server",
                "--stdio",
                "--claude",
                env!("CARGO_BIN_EXE_fake-claude"),
                "--model",
                "fake-claude",
                "--initialize-timeout-seconds",
                "3",
                "--approval-timeout-seconds",
                "5",
            ])
            .args(extra)
            .arg("--state-dir")
            .arg(state)
            .envs(env.iter().copied())
            .current_dir(state)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let input = child.stdin.take();
        let output = BufReader::new(child.stdout.take().unwrap()).lines();
        Self {
            child,
            input,
            output,
            methods: HashMap::new(),
            history: vec![],
            serial: 0,
        }
    }

    async fn send(&mut self, value: Value) {
        self.raw(&format!("{value}\n")).await;
    }

    async fn raw(&mut self, text: &str) {
        tokio::time::timeout(
            DEADLINE,
            self.input.as_mut().unwrap().write_all(text.as_bytes()),
        )
        .await
        .unwrap()
        .unwrap();
    }

    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(DEADLINE, self.output.next_line())
            .await
            .expect("facade response timed out")
            .unwrap()
            .expect("facade unexpectedly closed stdout");
        let value: Value =
            serde_json::from_str(&line).expect("stdout must contain only JSON frames");
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
            // Parse errors have no request ID; Codex's RequestId schema excludes
            // null, so validate the owned error payload and test null explicitly.
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

    async fn request_id(&mut self, id: Value, method: &str, params: Value) -> Value {
        assert!(
            self.methods
                .insert(id.to_string(), method.to_owned())
                .is_none(),
            "test reused a request ID"
        );
        self.send(json!({"id":id,"method":method,"params":params}))
            .await;
        loop {
            let value = self.read().await;
            if value.get("method").is_none() && value["id"] == id {
                return value;
            }
        }
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        self.serial += 1;
        self.request_id(json!(format!("request-{}", self.serial)), method, params)
            .await
    }

    async fn ok(&mut self, method: &str, params: Value) -> Value {
        let value = self.request(method, params).await;
        assert!(value.get("error").is_none(), "{method} failed: {value}");
        value["result"].clone()
    }

    async fn initialize(&mut self) {
        self.ok("initialize", json!({"clientInfo":{"name":"wire-tests","version":"1"},"capabilities":{"experimentalApi":true}})).await;
        self.send(json!({"method":"initialized"})).await;
    }

    async fn thread(&mut self) -> String {
        self.ok("thread/start", json!({"sandbox":"danger-full-access"}))
            .await["thread"]["id"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    async fn start(&mut self, thread: &str, text: &str) -> String {
        self.ok(
            "turn/start",
            json!({"threadId":thread,"input":[{"type":"text","text":text,"text_elements":[]}]}),
        )
        .await["turn"]["id"]
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
        self.until(|value| {
            value["method"] == "turn/completed" && value["params"]["turn"]["id"] == turn
        })
        .await["params"]["turn"]
            .clone()
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

fn assert_echo_lifecycle(client: &Client, turn: &str, expected: &str) {
    let events: Vec<_> = client
        .history
        .iter()
        .filter(|value| value["params"]["turnId"] == turn || value["params"]["turn"]["id"] == turn)
        .collect();
    assert_eq!(
        events
            .iter()
            .filter(|value| value["method"] == "turn/started")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|value| value["method"] == "turn/completed")
            .count(),
        1
    );
    let started = events
        .iter()
        .position(|value| value["method"] == "turn/started")
        .unwrap();
    let completed = events
        .iter()
        .position(|value| value["method"] == "turn/completed")
        .unwrap();
    assert!(started < completed);
    let mut open = HashSet::new();
    let mut closed = HashSet::new();
    for value in &events {
        if value["method"] == "item/started" {
            assert!(
                open.insert(value["params"]["item"]["id"].to_string()),
                "duplicate item start"
            );
        }
        if value["method"] == "item/completed" {
            let id = value["params"]["item"]["id"].to_string();
            assert!(open.contains(&id), "item completed before start");
            assert!(closed.insert(id), "duplicate item completion");
        }
    }
    assert_eq!(open, closed, "terminal turn left incomplete items");
    let delta = events
        .iter()
        .filter(|value| value["method"] == "item/agentMessage/delta")
        .map(|value| value["params"]["delta"].as_str().unwrap())
        .collect::<String>();
    assert_eq!(
        delta, expected,
        "partial and final Claude messages must not double text"
    );
    let messages: Vec<_> = events
        .iter()
        .filter(|value| {
            value["method"] == "item/completed" && value["params"]["item"]["type"] == "agentMessage"
        })
        .collect();
    assert_eq!(messages.len(), 1, "expected one final assistant item");
    assert_eq!(messages[0]["params"]["item"]["text"], expected);
    let terminal = &events[completed]["params"]["turn"];
    assert_eq!(terminal["status"], "completed");
    assert_eq!(
        terminal["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["type"] == "agentMessage")
            .count(),
        1
    );
}

#[tokio::test]
async fn handshake_ids_and_malformed_input_recovery() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    let before = client.request_id(json!(17), "thread/list", json!({})).await;
    assert_eq!(before["id"], 17);
    assert_eq!(before["error"]["code"], -32000);
    let init = client
        .request_id(
            json!("initialize-string-id"),
            "initialize",
            json!({"clientInfo":{"name":"wire-tests","version":"1"}}),
        )
        .await;
    assert_eq!(init["id"], "initialize-string-id");
    assert!(
        init["result"]["userAgent"]
            .as_str()
            .unwrap()
            .contains("claude-codex-server")
    );
    assert_eq!(client.ok("thread/list", json!({})).await["data"], json!([]));
    client.ok("config/read", json!({})).await;
    assert_eq!(
        client
            .ok(
                "getAuthStatus",
                json!({"includeToken": true, "refreshToken": false})
            )
            .await,
        json!({"authMethod": null, "authToken": null, "requiresOpenaiAuth": false})
    );
    client.send(json!({"method":"initialized"})).await;
    let experimental = client
        .request("thread/start", json!({"historyMode":"legacy"}))
        .await;
    assert_eq!(experimental["error"]["code"], -32602);
    assert!(
        experimental["error"]["message"]
            .as_str()
            .unwrap()
            .contains("experimentalApi")
    );
    assert_eq!(
        client
            .request(
                "initialize",
                json!({"clientInfo":{"name":"again","version":"1"}})
            )
            .await["error"]["code"],
        -32600
    );
    client.raw("{bad json\n").await;
    let error = client.read().await;
    assert_eq!(error["error"]["code"], -32700);
    assert!(error["id"].is_null());
    let list = client
        .request_id(json!(9007199254740991_u64), "thread/list", json!({}))
        .await;
    assert_eq!(list["id"], json!(9007199254740991_u64));
    assert_eq!(list["result"]["data"], json!([]));
    for method in ["model/list", "account/read", "config/read"] {
        client.ok(method, json!({})).await;
    }
    client.ok("configRequirements/read", Value::Null).await;
    client.close().await;
}

#[tokio::test]
async fn desktop_workspace_and_model_selection_survive_restart() {
    let state = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let cwd = workspace.path().join("new-task/nested");
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let config = client
        .ok("config/read", json!({"includeLayers": false}))
        .await;
    let provider = config["config"]["model_provider"].as_str().unwrap();
    // Desktop requires this map entry to bypass its OpenAI model allowlist.
    assert!(config["config"]["model_providers"][provider].is_object());
    let catalog = client
        .ok("model/list", json!({"includeHidden": true, "limit": 100}))
        .await;
    for model in ["opus", "fable", "sonnet", "haiku"] {
        let entry = catalog["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["model"] == model)
            .unwrap();
        assert_eq!(entry["hidden"], false);
        assert!(entry["displayName"].as_str().unwrap().ends_with(" test"));
        assert_eq!(
            entry["supportedReasoningEfforts"].as_array().unwrap().len(),
            5
        );
    }
    client.ok("fs/createDirectory", json!({"path": cwd})).await;
    client
        .ok(
            "fs/createDirectory",
            json!({"path": cwd, "recursive": true}),
        )
        .await;
    assert!(cwd.is_dir());
    let metadata = client.ok("fs/getMetadata", json!({"path": cwd})).await;
    assert_eq!(metadata["isDirectory"], true);
    assert_eq!(metadata["isFile"], false);
    assert_eq!(metadata["isSymlink"], false);
    std::fs::write(cwd.join("file.txt"), "content").unwrap();
    std::fs::create_dir(cwd.join("folder")).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(cwd.join("folder"), cwd.join("link")).unwrap();
    let entries = client.ok("fs/readDirectory", json!({"path": cwd})).await;
    let entries = entries["entries"].as_array().unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e["fileName"] == "file.txt" && e["isFile"] == true)
    );
    assert!(
        entries
            .iter()
            .any(|e| e["fileName"] == "folder" && e["isDirectory"] == true)
    );
    #[cfg(unix)]
    {
        assert!(
            entries
                .iter()
                .any(|e| e["fileName"] == "link" && e["isDirectory"] == true)
        );
        let link = client
            .ok("fs/getMetadata", json!({"path": cwd.join("link")}))
            .await;
        assert_eq!(link["isSymlink"], true);
        assert_eq!(link["isDirectory"], true);
    }
    assert!(
        client
            .request("fs/getMetadata", json!({"path": cwd.join("absent")}))
            .await
            .get("error")
            .is_some()
    );
    assert!(
        client
            .request("fs/readDirectory", json!({"path": cwd.join("file.txt")}))
            .await
            .get("error")
            .is_some()
    );
    assert!(
        client
            .request("fs/createDirectory", json!({"path": "relative"}))
            .await
            .get("error")
            .is_some()
    );
    assert!(
        client
            .request(
                "fs/createDirectory",
                json!({"path": workspace.path().join("missing/child"), "recursive": false})
            )
            .await
            .get("error")
            .is_some()
    );
    let write = client
        .ok(
            "config/batchWrite",
            json!({"edits": [
        {"keyPath": "model", "value": "sonnet", "mergeStrategy": "upsert"},
        {"keyPath": "model_reasoning_effort", "value": "medium", "mergeStrategy": "upsert"}
    ], "filePath": null, "expectedVersion": null, "reloadUserConfig": true}),
        )
        .await;
    assert_eq!(write["status"], "ok");
    // Invalid batches must not partially apply earlier valid edits.
    assert!(client.request("config/batchWrite", json!({"edits": [
        {"keyPath": "model", "value": "opus", "mergeStrategy": "upsert"},
        {"keyPath": "sandbox_mode", "value": "workspace-write", "mergeStrategy": "upsert"}
    ]})).await.get("error").is_some());
    client.close().await;
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let config = client.ok("config/read", json!({})).await;
    assert_eq!(config["config"]["model"], "sonnet");
    assert_eq!(config["config"]["model_reasoning_effort"], "medium");
    let thread = client
        .ok(
            "thread/start",
            json!({"cwd": cwd, "threadSource": "user", "sandbox":"danger-full-access"}),
        )
        .await;
    assert_eq!(thread["model"], "sonnet");
    assert_eq!(thread["reasoningEffort"], "medium");
    let id = thread["thread"]["id"].as_str().unwrap();
    let turn = client.start(id, "desktop creation").await;
    assert_eq!(client.completed(&turn).await["status"], "completed");
    client.close().await;
}

#[tokio::test]
async fn desktop_thread_and_turn_options_reach_claude() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.ok("thread/start", json!({
        "cwd":state.path(), "model":"opus", "modelProvider":null,
        "approvalPolicy":"on-request", "approvalsReviewer":"user",
        "sandbox":"danger-full-access", "runtimeWorkspaceRoots":[state.path()],
        "historyMode":"paginated", "threadSource":"user", "ephemeral":null,
        "serviceTier":null, "personality":"pragmatic", "mockExperimentalField":null,
        "experimentalRawEvents":false, "dynamicTools":[],
        "baseInstructions":null, "developerInstructions":"Answer concisely.",
        "config":{"model_reasoning_effort":"xhigh", "features.request_permissions_tool":true,
            "apps.connector_openai_pages.tools":{"chatgpt_space.create_canvas":{"enabled":false}}}
    })).await;
    assert_eq!(thread["model"], "opus");
    assert_eq!(thread["reasoningEffort"], "xhigh");
    let id = &thread["thread"]["id"];
    let turn = client.ok("turn/start", json!({
        "threadId":id, "input":[{"type":"text","text":"desktop payload","text_elements":[]}],
        "approvalsReviewer":"user", "approvalPolicy":"on-request",
        "sandboxPolicy":{"type":"dangerFullAccess"}, "runtimeWorkspaceRoots":[state.path()],
        "turnTrigger":"submit", "responsesapiClientMetadata":{}, "multiAgentMode":"explicitRequestOnly",
        "disabledPluginIds":[], "collaborationMode":{"mode":"default",
            "settings":{"model":"fable","reasoning_effort":"max","developer_instructions":null}},
        "additionalContext":{"test":{"kind":"untrusted","value":"Desktop reference context"}}
    })).await;
    assert_eq!(
        client.completed(turn["turn"]["id"].as_str().unwrap()).await["status"],
        "completed"
    );
    let resumed = client
        .ok(
            "thread/resume",
            json!({"threadId":id,"approvalsReviewer":"user",
        "experimentalRawEvents":false,"config":{},"runtimeWorkspaceRoots":[state.path()]}),
        )
        .await;
    assert_eq!(resumed["model"], "fable");
    assert_eq!(resumed["reasoningEffort"], "max");
    let rejected = client
        .request(
            "thread/start",
            json!({"approvalsReviewer":"not-a-reviewer"}),
        )
        .await;
    assert_eq!(rejected["error"]["code"], -32602);
    assert_eq!(
        client
            .request("thread/start", json!({"experimentalRawEvents":true}))
            .await["error"]["code"],
        -32602
    );
    client.close().await;
}

#[tokio::test]
async fn installed_desktop_turn_builder_payload_submits_first_and_followup_turns() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    client
        .ok(
            "thread/metadata/update",
            json!({"threadId":thread,"projectId":""}),
        )
        .await;
    let fixture: Value = serde_json::from_str(include_str!("fixtures/desktop-turn.json")).unwrap();
    for summary in ["detailed", "none", "auto", "concise"] {
        let mut request = fixture["request"].clone();
        request["threadId"] = json!(thread);
        request["cwd"] = json!(state.path());
        request["runtimeWorkspaceRoots"] = json!([state.path()]);
        if request["sandboxPolicy"]["type"] == "workspaceWrite" {
            request["sandboxPolicy"]["writableRoots"] = json!([state.path()]);
        }
        request["summary"] = json!(summary);
        let turn = client.ok("turn/start", request).await;
        let id = turn["turn"]["id"].as_str().unwrap();
        assert_eq!(client.completed(id).await["status"], "completed");
        assert_echo_lifecycle(&client, id, "Echo: desktop payload");
    }
    client.close().await;
}

#[tokio::test]
async fn read_only_desktop_helpers_are_tool_free_and_cannot_change_mode() {
    let state = tempfile::tempdir().unwrap();
    let home = state.path().join("codex-home");
    std::fs::create_dir(&home).unwrap();
    let codex = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex_scope.sh");
    let mut client = Client::spawn_with_env(
        state.path(),
        &[],
        &[
            ("CLAUDE_CODEX_PLUGIN_EXECUTABLE", &codex),
            ("CLAUDE_CODEX_PLUGIN_HOME", &home),
        ],
    )
    .await;
    client.initialize().await;
    let helper = json!({"permissions":":read-only","ephemeral":true,
        "approvalPolicy":"never","allowProviderModelFallback":true,
        "dynamicTools":[claude_codex_server::delegation::dynamic_spec()]});
    for invalid in [
        json!({"ephemeral":false}),
        json!({"approvalPolicy":"on-request"}),
        json!({"dynamicTools":[{"name":"write","description":"Write","inputSchema":{"type":"object"}}]}),
        json!({"sandboxPolicy":{"type":"readOnly","networkAccess":true},"permissions":null}),
    ] {
        let mut params = helper.clone();
        params
            .as_object_mut()
            .unwrap()
            .extend(invalid.as_object().unwrap().clone());
        let reply = client.request("thread/start", params).await;
        assert_eq!(reply["error"]["code"], -32602, "{reply}");
    }
    let started = client.ok("thread/start", helper.clone()).await;
    assert_eq!(
        started["sandbox"],
        json!({"type":"readOnly","networkAccess":false})
    );
    let thread = &started["thread"]["id"];
    let plugin = client
        .request(
            "mcpServer/tool/call",
            json!({
                "threadId":thread,"server":"fixture","tool":"write","arguments":{}
            }),
        )
        .await;
    assert_eq!(plugin["error"]["code"], -32602, "{plugin}");
    assert!(
        plugin["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Read-only helpers")
    );
    for params in [
        json!({"sandboxPolicy":{"type":"workspaceWrite"}}),
        json!({"sandboxPolicy":{"type":"dangerFullAccess"}}),
        json!({"approvalPolicy":"on-request"}),
    ] {
        let mut params = params;
        params["threadId"] = thread.clone();
        let reply = client.request("thread/settings/update", params).await;
        assert_eq!(reply["error"]["code"], -32602, "{reply}");
    }
    assert_eq!(
        client
            .request("thread/resume", json!({"threadId":thread}))
            .await["error"]["code"],
        -32602
    );
    assert_eq!(
        client
            .request("thread/fork", json!({"threadId":thread,"ephemeral":true}))
            .await["error"]["code"],
        -32602
    );
    let turn = client
        .ok(
            "turn/start",
            json!({"threadId":thread,"permissions":":read-only",
        "input":[{"type":"text","text":"helper","text_elements":[]}]}),
        )
        .await;
    let turn = turn["turn"]["id"].as_str().unwrap();
    let completed = client.completed(turn).await;
    assert_eq!(completed["status"], "completed", "{completed}");
    assert_echo_lifecycle(&client, turn, "Echo: helper");
    assert_eq!(
        client
            .request(
                "turn/start",
                json!({"threadId":thread,
        "input":[{"type":"text","text":"again","text_elements":[]}]})
            )
            .await["error"]["code"],
        -32602
    );

    let unsafe_helper = client.ok("thread/start", helper).await;
    let turn = client
        .start(unsafe_helper["thread"]["id"].as_str().unwrap(), "tool")
        .await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "failed", "{completed}");
    assert!(
        completed
            .to_string()
            .contains("Read-only helper attempted tool execution"),
        "{completed}"
    );
    client.close().await;
}

#[tokio::test]
async fn desktop_initial_daybreak_choice_survives_restart() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let mut threads = Vec::new();
    for enabled in [json!(true), json!(false), json!(null)] {
        let started = client
            .ok(
                "thread/start",
                json!({
                    "daybreakEnabled":enabled, "daybreakProgramPropagationEnrolled":true
                }),
            )
            .await;
        assert_eq!(started["thread"]["daybreakEnabled"], enabled);
        threads.push((started["thread"]["id"].clone(), enabled));
    }
    client.close().await;
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    for (thread, enabled) in threads {
        let resumed = client.ok("thread/resume", json!({"threadId":thread})).await;
        assert_eq!(resumed["thread"]["daybreakEnabled"], enabled);
    }
    client.close().await;
}

#[tokio::test]
async fn desktop_metadata_and_next_turn_settings_persist_with_schema_checked_notifications() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let updated = client
        .ok(
            "thread/metadata/update",
            json!({"threadId":thread,
        "projectId":"desktop-project", "daybreakEnabled":false,
        "gitInfo":{"sha":"abc","branch":"main","originUrl":"https://example.test/repo"}}),
        )
        .await;
    assert_eq!(updated["thread"]["projectId"], "desktop-project");
    client
        .ok(
            "thread/settings/update",
            json!({"threadId":thread,"model":"opus","effort":"high",
        "approvalsReviewer":"auto_review","permissions":":workspace","disabledPluginIds":[]}),
        )
        .await;
    let invalid = client
        .request(
            "thread/metadata/update",
            json!({"threadId":thread,"projectId":"different","gitInfo":{"branch":""}}),
        )
        .await;
    assert_eq!(invalid["error"]["code"], -32602);
    client.close().await;
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let resumed = client.ok("thread/resume", json!({"threadId":thread})).await;
    assert_eq!(resumed["thread"]["projectId"], "desktop-project");
    assert_eq!(resumed["thread"]["gitInfo"]["sha"], "abc");
    assert_eq!(resumed["model"], "opus");
    assert_eq!(resumed["reasoningEffort"], "high");
    assert_eq!(resumed["approvalsReviewer"], "auto_review");
    assert_eq!(resumed["sandbox"]["type"], "workspaceWrite");
    client
        .ok(
            "thread/metadata/update",
            json!({"threadId":thread,"projectId":"","gitInfo":{"sha":null,"branch":"feature"}}),
        )
        .await;
    let read = client.ok("thread/read", json!({"threadId":thread})).await;
    assert!(read["thread"]["projectId"].is_null());
    assert_eq!(
        read["thread"]["gitInfo"],
        json!({"sha":null,"branch":"feature","originUrl":"https://example.test/repo"})
    );
    client.close().await;
}

#[tokio::test]
async fn desktop_dynamic_tools_roundtrip_and_survive_restart() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.ok("thread/start", json!({"sandbox":"danger-full-access","personality":"friendly",
        "dynamicTools":[{"type":"namespace","name":"desktop","description":"Desktop tools","tools":[
            {"type":"function","name":"echo","description":"Echo a value","inputSchema":{
                "type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}}
        ]}]})).await["thread"]["id"].as_str().unwrap().to_owned();
    client.close().await;
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    client
        .ok(
            "thread/resume",
            json!({"threadId":thread,"personality":"none"}),
        )
        .await;
    for reply in [
        json!({"result":{"success":true,"contentItems":[{"type":"inputText","text":"desktop result"}]}}),
        json!({"result":{"success":false,"contentItems":[{"type":"inputText","text":"declined"}]}}),
        json!({"error":{"code":-32603,"message":"client unavailable"}}),
        json!({"result":{"success":true,"contentItems":[{"type":"bogus"}]}}),
    ] {
        let turn = client.start(&thread, "desktop-tool").await;
        let call = client
            .until(|v| v["method"] == "item/tool/call" && v["params"]["turnId"] == turn)
            .await;
        assert_eq!(call["params"]["namespace"], "desktop");
        assert_eq!(call["params"]["tool"], "echo");
        assert_eq!(call["params"]["arguments"], json!({"value":"from Claude"}));
        let mut response = reply.clone();
        response["id"] = call["id"].clone();
        client.send(response).await;
        let completed = client.completed(&turn).await;
        let items = completed["items"].as_array().unwrap();
        let dynamic: Vec<_> = items
            .iter()
            .filter(|i| i["type"] == "dynamicToolCall")
            .collect();
        assert_eq!(dynamic.len(), 1);
        assert_eq!(dynamic[0]["id"], call["params"]["callId"]);
        let success = reply["result"]["success"] == true
            && reply["result"]["contentItems"][0]["type"] == "inputText";
        assert_eq!(dynamic[0]["success"], success);
        assert_eq!(
            dynamic[0]["status"],
            if success { "completed" } else { "failed" }
        );
        let text = items.iter().find(|i| i["type"] == "agentMessage").unwrap()["text"]
            .as_str()
            .unwrap();
        let returned: Value = serde_json::from_str(text).unwrap();
        assert_eq!(returned["result"]["isError"], !success);
        assert_eq!(returned["result"]["content"][0]["type"], "text");
    }
    for prompt in ["desktop-invalid", "desktop-unknown"] {
        let turn = client.start(&thread, prompt).await;
        client.completed(&turn).await;
        assert!(
            !client
                .history
                .iter()
                .any(|v| v["method"] == "item/tool/call" && v["params"]["turnId"] == turn)
        );
    }
    let turn = client.start(&thread, "desktop-tool").await;
    let call = client
        .until(|v| v["method"] == "item/tool/call" && v["params"]["turnId"] == turn)
        .await;
    client
        .ok("turn/interrupt", json!({"threadId":thread,"turnId":turn}))
        .await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "interrupted");
    assert!(
        completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["id"] == call["params"]["callId"] && i["status"] == "failed")
    );
    client.close().await;
}

#[tokio::test]
async fn text_turn_has_exactly_one_complete_lifecycle() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    client
        .ok("thread/start", json!({"historyMode":"legacy"}))
        .await;
    let thread = client.thread().await;
    let turn = client.start(&thread, "hello").await;
    let completed = client.completed(&turn).await;
    assert!(completed["error"].is_null());
    assert_echo_lifecycle(&client, &turn, "Echo: hello");
    let read = client
        .ok(
            "thread/read",
            json!({"threadId":thread,"includeTurns":true}),
        )
        .await;
    assert_eq!(read["thread"]["turns"], json!([completed]));
    let turns = client
        .ok(
            "thread/turns/list",
            json!({"threadId":thread,"itemsView":"full"}),
        )
        .await;
    assert_eq!(turns["data"].as_array().unwrap().len(), 1);
    let items = client
        .ok(
            "thread/items/list",
            json!({"threadId":thread,"turnId":turn}),
        )
        .await;
    assert_eq!(items["data"].as_array().unwrap().len(), 2);
    client.close().await;
}

#[tokio::test]
async fn items_are_timed_and_read_state_follows_terminal_results() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let read = client.ok("thread/read", json!({"threadId":thread})).await;
    assert_eq!(
        read["readState"],
        json!({"firstUnread":null,"revision":format!("initial:{thread}")})
    );
    let turn = client.start(&thread, "hello").await;
    let completed = client.completed(&turn).await;
    let items = completed["items"].as_array().unwrap().clone();
    for item in &items {
        assert!(item["startedAtMs"].is_u64(), "{item}");
        assert!(item["completedAtMs"].is_u64(), "{item}");
    }
    // A completed answer needs attention until the user marks it read.
    let changed = client
        .until(|v| v["method"] == "thread/readState/changed" && v["params"]["threadId"] == thread)
        .await["params"]["readState"]
        .clone();
    assert_eq!(changed["firstUnread"], json!({"type":"turn","turnId":turn}));
    let list = client.ok("thread/list", json!({})).await;
    assert_eq!(list["readStates"][&thread], changed);
    let read = client.ok("thread/read", json!({"threadId":thread})).await;
    assert_eq!(read["readState"], changed);

    // Stored order, unknown ids omitted, with the item's times.
    let (first, last) = (&items[0], &items[items.len() - 1]);
    let entries = client
        .ok(
            "thread/items/read",
            json!({"threadId":thread,"turnId":turn,"itemIds":[last["id"],"missing",first["id"]]}),
        )
        .await["data"]
        .clone();
    assert_eq!(
        entries,
        json!([
            {"turnId":turn,"startedAtMs":first["startedAtMs"],"completedAtMs":first["completedAtMs"],"item":first},
            {"turnId":turn,"startedAtMs":last["startedAtMs"],"completedAtMs":last["completedAtMs"],"item":last}
        ])
    );
    for ids in [
        json!([]),
        json!((0..101).map(|i| i.to_string()).collect::<Vec<_>>()),
    ] {
        let reply = client
            .request(
                "thread/items/read",
                json!({"threadId":thread,"turnId":turn,"itemIds":ids}),
            )
            .await;
        assert!(reply.get("error").is_some(), "{reply}");
    }

    // An edit made against an older snapshot reports the current state.
    let stale = client
        .request(
            "thread/readState/update",
            json!({"threadId":thread,"operation":{"type":"read"},
                "expectedRevision":format!("initial:{thread}")}),
        )
        .await;
    assert_eq!(stale["error"]["data"]["reason"], "readStateConflict");
    assert_eq!(stale["error"]["data"]["readState"], changed);
    let marked = client
        .ok(
            "thread/readState/update",
            json!({"threadId":thread,"operation":{"type":"read"},
                "expectedRevision":changed["revision"]}),
        )
        .await["readState"]
        .clone();
    assert!(marked["firstUnread"].is_null());
    assert_ne!(marked["revision"], changed["revision"]);
    let unread = client
        .ok(
            "thread/readState/update",
            json!({"threadId":thread,"operation":{"type":"unread"},
                "expectedRevision":marked["revision"]}),
        )
        .await["readState"]
        .clone();
    assert_eq!(unread["firstUnread"], json!({"type":"threadStart"}));
    client.close().await;

    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let read = client.ok("thread/read", json!({"threadId":thread})).await;
    assert_eq!(read["readState"], unread);
    client.close().await;
}

#[tokio::test]
async fn attached_side_chats_are_listed_under_their_source_and_deleted_first() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let source = client
        .ok(
            "thread/start",
            json!({"sandbox":"danger-full-access","historyMode":"paginated"}),
        )
        .await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let turn = client.start(&source, "hello").await;
    client.completed(&turn).await;
    // The desktop's persistent side chat request.
    let side = client
        .ok(
            "thread/fork",
            json!({"threadId":source,"attachToSource":true,"excludeTurns":true}),
        )
        .await["thread"]
        .clone();
    assert_eq!(side["forkedFromId"], source);
    assert_eq!(side["attachedToThreadId"], source);
    let side = side["id"].as_str().unwrap().to_owned();

    let ordinary = client.ok("thread/list", json!({})).await;
    assert!(ids(&ordinary).contains(&source));
    assert!(!ids(&ordinary).contains(&side));
    client
        .ok(
            "thread/name/set",
            json!({"threadId":side,"name":"Attached draft"}),
        )
        .await;
    let search = json!({"includeAttachedThreads":true,"searchTerm":"Attached draft"});
    let including_attached = client.ok("thread/list", search).await;
    assert_eq!(ids(&including_attached), vec![side.clone()]);
    let unfiltered = client
        .request("thread/list", json!({"includeAttachedThreads":true}))
        .await;
    assert_eq!(unfiltered["error"]["code"], -32600);
    let attached = client
        .ok(
            "thread/list",
            json!({"attachedToThreadId":source,"archived":false,"limit":100,"useStateDbOnly":true}),
        )
        .await;
    assert_eq!(ids(&attached), vec![side.clone()]);
    let mixed = client
        .request(
            "thread/list",
            json!({"attachedToThreadId":source,"parentThreadId":source}),
        )
        .await;
    assert!(mixed.get("error").is_some(), "{mixed}");

    for params in [
        json!({"threadId":source,"attachToSource":true,"ephemeral":true}),
        json!({"threadId":client.thread().await,"attachToSource":true}),
    ] {
        let reply = client.request("thread/fork", params).await;
        assert!(reply.get("error").is_some(), "{reply}");
    }

    let blocked = client
        .request("thread/delete", json!({"threadId":source}))
        .await;
    assert!(blocked.get("error").is_some(), "{blocked}");
    client.ok("thread/delete", json!({"threadId":side})).await;
    client.ok("thread/delete", json!({"threadId":source})).await;
    client.close().await;
}

#[tokio::test]
async fn thread_scoped_hooks_and_mcp_status_use_the_thread_workspace() {
    let state = tempfile::tempdir().unwrap();
    let home = state.path().join("codex-home");
    std::fs::create_dir(&home).unwrap();
    let codex = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex_scope.sh");
    let mut client = Client::spawn_with_env(
        state.path(),
        &[],
        &[
            ("CLAUDE_CODEX_PLUGIN_EXECUTABLE", &codex),
            ("CLAUDE_CODEX_PLUGIN_HOME", &home),
        ],
    )
    .await;
    client.initialize().await;
    let workspace = tempfile::tempdir().unwrap();
    let thread = client
        .ok(
            "thread/start",
            json!({"sandbox":"danger-full-access","cwd":workspace.path()}),
        )
        .await["thread"]["id"]
        .clone();
    // Desktop 0.166 scopes these to the open chat.
    client.ok("hooks/list", json!({"threadId":thread})).await;
    client
        .ok(
            "mcpServerStatus/list",
            json!({"threadId":thread,"detail":"full"}),
        )
        .await;
    let unknown = client
        .request("hooks/list", json!({"threadId":"missing"}))
        .await;
    assert!(unknown.get("error").is_some(), "{unknown}");
    let requests: Vec<Value> = std::fs::read_to_string(home.join("requests"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert!(request["params"].get("threadId").is_none(), "{request}");
    }
    assert_eq!(
        requests[0]["params"]["cwds"],
        json!([workspace.path().canonicalize().unwrap()])
    );
    assert_eq!(requests[1]["params"]["detail"], "full");

    // Genuine Codex writes the archive; the thread contributes its record.
    let archive = state.path().join("diagnostics.zip");
    let saved = client
        .ok(
            "feedback/save",
            json!({"threadId":thread,"classification":"bug","includeLogs":true,
                "savePath":archive}),
        )
        .await;
    assert_eq!(saved["savedPath"], json!(archive));
    assert_eq!(saved["threadId"], thread);
    let requests = std::fs::read_to_string(home.join("requests")).unwrap();
    let feedback: Value = serde_json::from_str(requests.lines().last().unwrap()).unwrap();
    assert!(feedback["params"].get("threadId").is_none(), "{feedback}");
    let record = state
        .path()
        .canonicalize()
        .unwrap()
        .join("threads")
        .join(format!("{}.json", thread.as_str().unwrap()));
    assert_eq!(feedback["params"]["extraLogFiles"], json!([record]));
    client.close().await;
}

#[tokio::test]
async fn codex_0_166_thread_methods_answer_like_codex() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let memory = client
        .ok("thread/memory/read", json!({"threadId":thread}))
        .await;
    assert_eq!(memory, json!({"summary":null}));
    // Genuine Codex 0.166 has not implemented this either.
    let prediction = client
        .request(
            "thread/prediction/request",
            json!({"threadId":thread,"sourceTurnId":"turn"}),
        )
        .await;
    assert_eq!(prediction["error"]["code"], -32601);
    client
        .ok(
            "thread/attachment/add",
            json!({"threadId":thread,"attachmentType":"worktree","identityKey":"/w",
                "payload":{}}),
        )
        .await;
    client
        .ok("thread/archive", json!({"threadId":thread}))
        .await;
    let owners = client
        .ok(
            "thread/attachmentOwner/list",
            json!({"attachmentType":"worktree","identityKey":"/w"}),
        )
        .await;
    assert_eq!(
        owners,
        json!({"data":[{"threadId":thread,"archived":true}],"nextCursor":null})
    );
    let active_only = client
        .ok(
            "thread/attachmentOwner/list",
            json!({"attachmentType":"worktree","identityKey":"/w","archived":false}),
        )
        .await;
    assert_eq!(active_only["data"], json!([]));
    client.close().await;
}

#[tokio::test]
async fn steering_waits_for_consumed_inputs_and_handles_backend_startup() {
    for during_startup in [false, true] {
        let state = tempfile::tempdir().unwrap();
        let args = if during_startup {
            vec!["--claude-arg=--fake-init-delay"]
        } else {
            vec![]
        };
        let mut client = Client::spawn_with_args(state.path(), &args).await;
        client.initialize().await;
        let thread = client.thread().await;
        let turn = client.start(&thread, "steer-wait").await;
        if !during_startup {
            client
                .until(|v| {
                    v["method"] == "item/completed"
                        && v["params"]["item"]["text"] == "Waiting for steering"
                })
                .await;
        }
        let mismatch = client.request("turn/steer", json!({"threadId":thread,"expectedTurnId":"wrong", "input":[{"type":"text","text":"ignored"}]})).await;
        assert!(
            mismatch["error"]["message"]
                .as_str()
                .unwrap()
                .contains("expectedTurnId")
        );
        for (index, text) in ["one", "two"].into_iter().enumerate() {
            let result = client.ok("turn/steer", json!({"threadId":thread,"expectedTurnId":turn,
                "clientUserMessageId":format!("steer-{index}"),"input":[{"type":"text","text":text}]})).await;
            assert_eq!(result["turnId"], turn);
            assert!(
                !client
                    .history
                    .iter()
                    .any(|v| v["method"] == "turn/completed" && v["params"]["turn"]["id"] == turn)
            );
        }
        let completed = client.completed(&turn).await;
        assert_eq!(completed["status"], "completed");
        let items = completed["items"].as_array().unwrap();
        assert!(
            items
                .iter()
                .any(|item| item["text"] == "Steered: one + two")
        );
        for index in 0..2 {
            assert_eq!(
                items
                    .iter()
                    .filter(|item| item["type"] == "userMessage"
                        && item["clientId"] == format!("steer-{index}"))
                    .count(),
                1
            );
        }
        assert_eq!(
            client
                .history
                .iter()
                .filter(|v| v["method"] == "turn/completed" && v["params"]["turn"]["id"] == turn)
                .count(),
            1
        );
        let inactive = client.request("turn/steer", json!({"threadId":thread,"expectedTurnId":turn,"input":[{"type":"text","text":"late"}]})).await;
        assert!(inactive.get("error").is_some());
        client.close().await;
    }
}

#[tokio::test]
async fn manual_compaction_runs_claude_compact_as_its_own_turn() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let empty = client
        .request("thread/compact/start", json!({"threadId":thread}))
        .await;
    assert!(
        empty["error"]["message"]
            .as_str()
            .unwrap()
            .contains("no history")
    );
    let first = client.start(&thread, "hello").await;
    client.completed(&first).await;
    // Context queued for the next user turn must not be folded into `/compact`.
    client.ok("thread/inject_items", json!({"threadId":thread,"items":[{
        "type":"message", "role":"user", "content":[{"type":"input_text","text":"Side boundary"}]
    }]})).await;

    let result = client
        .ok("thread/compact/start", json!({"threadId":thread}))
        .await;
    assert_eq!(result, json!({}));
    let turn = client
        .until(|v| v["method"] == "turn/started" && v["params"]["turn"]["id"] != first)
        .await["params"]["turn"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let started = client
        .until(|v| {
            v["method"] == "item/started" && v["params"]["item"]["type"] == "contextCompaction"
        })
        .await;
    // Steering a manual compaction fails as it does in Codex.
    let steer = client
        .request(
            "turn/steer",
            json!({"threadId":thread,"expectedTurnId":turn,
        "input":[{"type":"text","text":"more"}]}),
        )
        .await;
    assert_eq!(
        steer["error"]["data"]["codexErrorInfo"],
        json!({"activeTurnNotSteerable":{"turnKind":"compact"}})
    );
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "completed");
    let items = completed["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["type"], "contextCompaction");
    assert_eq!(items[0]["id"], started["params"]["item"]["id"]);
    // As in Codex 0.166, the completed item carries its recorded times.
    assert!(items[0]["startedAtMs"].is_u64() && items[0]["completedAtMs"].is_u64());
    let lifecycle: Vec<_> = client
        .history
        .iter()
        .filter(|v| v["params"]["item"]["type"] == "contextCompaction")
        .map(|v| v["method"].as_str().unwrap())
        .collect();
    assert_eq!(lifecycle, ["item/started", "item/completed"]);

    let next = client.start(&thread, "after").await;
    let next = client.completed(&next).await;
    assert!(
        next["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["text"] == "Echo: Side boundary\nafter"),
        "{next}"
    );
    client.close().await;
}

#[tokio::test]
async fn queue_mutations_persist_and_start_consumes_only_on_success() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let mut entries = Vec::new();
    for index in 0..3 {
        let params = json!({"threadId":thread,"clientUserMessageId":format!("queued-{index}"),
            "input":[{"type":"text","text":format!("message-{index}")} ]});
        let entry = client.ok("thread/queue/add", params.clone()).await["queuedSubmission"].clone();
        assert_eq!(
            client.ok("thread/queue/add", params).await["queuedSubmission"],
            entry
        );
        entries.push(entry);
    }
    client
        .ok(
            "thread/queue/update",
            json!({"threadId":thread,"queuedSubmissionId":entries[1]["id"],
        "input":[{"type":"text","text":"updated"}]}),
        )
        .await;
    let invalid = client
        .request(
            "thread/queue/reorder",
            json!({"threadId":thread,
        "queuedSubmissionIds":[entries[1]["id"], entries[1]["id"], entries[0]["id"]]}),
        )
        .await;
    assert!(invalid.get("error").is_some());
    client
        .ok(
            "thread/queue/reorder",
            json!({"threadId":thread,
        "queuedSubmissionIds":[entries[1]["id"], entries[2]["id"], entries[0]["id"]]}),
        )
        .await;
    assert_eq!(
        client
            .ok(
                "thread/queue/delete",
                json!({"threadId":thread,"queuedSubmissionId":entries[2]["id"]})
            )
            .await["deleted"],
        true
    );
    assert_eq!(
        client
            .ok(
                "thread/queue/delete",
                json!({"threadId":thread,"queuedSubmissionId":entries[2]["id"]})
            )
            .await["deleted"],
        false
    );
    client.close().await;

    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    client.ok("thread/resume", json!({"threadId":thread})).await;
    let page = client
        .ok("thread/queue/list", json!({"threadId":thread,"limit":1}))
        .await;
    assert_eq!(page["data"][0]["id"], entries[1]["id"]);
    assert_eq!(page["data"][0]["input"][0]["text"], "updated");
    assert_eq!(
        client
            .ok(
                "thread/queue/list",
                json!({"threadId":thread,"limit":1,"cursor":page["nextCursor"]})
            )
            .await["data"][0]["id"],
        entries[0]["id"]
    );
    let active = client.start(&thread, "hang").await;
    assert!(
        client
            .request("thread/queue/start", json!({"threadId":thread}))
            .await
            .get("error")
            .is_some()
    );
    assert_eq!(
        client
            .ok("thread/queue/list", json!({"threadId":thread}))
            .await["data"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    client
        .ok("turn/interrupt", json!({"threadId":thread,"turnId":active}))
        .await;
    client.completed(&active).await;
    let turn = client
        .ok("thread/queue/start", json!({"threadId":thread}))
        .await["turn"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let completed = client.completed(&turn).await;
    assert!(
        completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["clientId"] == "queued-1")
    );
    assert_echo_lifecycle(&client, &turn, "Echo: updated");
    assert_eq!(
        client
            .ok("thread/queue/list", json!({"threadId":thread}))
            .await["data"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(
        client
            .history
            .iter()
            .any(|v| v["method"] == "thread/queue/changed")
    );
    let turn = client
        .ok(
            "thread/queue/start",
            json!({"threadId":thread,"queuedSubmissionId":entries[0]["id"]}),
        )
        .await["turn"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    client.completed(&turn).await;
    assert!(
        client
            .request("thread/queue/start", json!({"threadId":thread}))
            .await
            .get("error")
            .is_some()
    );
    client.close().await;
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    assert_eq!(
        client
            .ok("thread/queue/list", json!({"threadId":thread}))
            .await["data"],
        json!([])
    );
    client.close().await;
}

#[tokio::test]
async fn user_questions_remain_available_when_permission_prompts_are_disabled() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client
        .ok(
            "thread/start",
            json!({"sandbox":"danger-full-access","approvalPolicy":"never"}),
        )
        .await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let turn = client.start(&thread, "question").await;
    let question = client
        .until(|v| v["method"] == "item/tool/requestUserInput")
        .await;
    client
        .send(json!({"id":question["id"],"result":{"answers":{"q0":{"answers":["Two"]}}}}))
        .await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "completed");
    let text = completed["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "agentMessage")
        .unwrap()["text"]
        .as_str()
        .unwrap();
    let permission: Value = serde_json::from_str(text).unwrap();
    assert_eq!(permission["behavior"], "allow");
    assert_eq!(permission["updatedInput"]["answers"]["Which value?"], "Two");
    client.close().await;
}

#[tokio::test]
async fn desktop_tool_execution_can_outlive_the_permission_timeout() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.ok("thread/start", json!({"sandbox":"danger-full-access","dynamicTools":[
        {"type":"namespace","name":"desktop","description":"Desktop tools","tools":[{"type":"function","name":"echo","description":"Echo","inputSchema":{"type":"object"}}]}]})).await["thread"]["id"].as_str().unwrap().to_owned();
    let turn = client.start(&thread, "desktop-tool").await;
    let call = client.until(|v| v["method"] == "item/tool/call").await;
    tokio::time::sleep(Duration::from_millis(5200)).await;
    client.send(json!({"id":call["id"],"result":{"success":true,"contentItems":[{"type":"inputText","text":"late result"}]}})).await;
    let completed = client.completed(&turn).await;
    assert!(
        completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == call["params"]["callId"] && item["success"] == true)
    );
    client.close().await;
}

#[tokio::test]
async fn tool_permissions_round_trip_allow_and_deny() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    for (decision, expected, status) in [
        ("accept", "fake tool output", "completed"),
        ("decline", "Tool denied", "failed"),
    ] {
        let turn = client.start(&thread, "tool").await;
        let approval = client
            .until(|value| {
                value["method"] == "item/commandExecution/requestApproval"
                    && value["params"]["turnId"] == turn
            })
            .await;
        assert_eq!(approval["params"]["threadId"], thread);
        assert_eq!(approval["params"]["command"], "printf fake");
        assert!(
            !client
                .history
                .iter()
                .any(|value| value["method"] == "turn/completed"
                    && value["params"]["turn"]["id"] == turn)
        );
        client
            .send(json!({"id":approval["id"],"result":{"decision":decision}}))
            .await;
        let completed = client.completed(&turn).await;
        let command = completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "commandExecution")
            .unwrap();
        assert_eq!(command["status"], status);
        assert_eq!(command["aggregatedOutput"], expected);
        assert_echo_lifecycle(&client, &turn, expected);
        assert!(
            client
                .history
                .iter()
                .any(|value| value["method"] == "serverRequest/resolved"
                    && value["params"]["requestId"] == approval["id"])
        );
    }
    client.close().await;
}

#[tokio::test]
async fn interruption_crash_and_malformed_backend_are_terminal() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let hanging = client.start(&thread, "hang").await;
    client
        .ok(
            "turn/interrupt",
            json!({"threadId":thread,"turnId":hanging}),
        )
        .await;
    assert_eq!(client.completed(&hanging).await["status"], "interrupted");
    for (text, diagnostic) in [
        ("crash", "fake backend crashed deliberately"),
        ("malformed", "malformed Claude JSON frame"),
    ] {
        let turn = client.start(&thread, text).await;
        let completed = client.completed(&turn).await;
        assert_eq!(completed["status"], "failed");
        assert!(
            completed["error"]["message"]
                .as_str()
                .unwrap()
                .contains(diagnostic),
            "{completed}"
        );
        assert!(client.history.iter().any(|value| value["method"] == "error"
            && value["params"]["turnId"] == turn
            && value["params"]["willRetry"] == false));
    }
    let recovery = client.start(&thread, "recovered").await;
    client.completed(&recovery).await;
    assert_echo_lifecycle(&client, &recovery, "Echo: recovered");
    client.close().await;
}

#[tokio::test]
async fn persistence_pagination_archive_and_fork_metadata() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let source = client.thread().await;
    let turn = client.start(&source, "persist me").await;
    let terminal = client.completed(&turn).await;
    client
        .ok(
            "thread/name/set",
            json!({"threadId":source,"name":"Saved source"}),
        )
        .await;
    let second = client.thread().await;
    let first_page = client
        .ok("thread/list", json!({"limit":1,"sortDirection":"asc"}))
        .await;
    assert_eq!(first_page["data"].as_array().unwrap().len(), 1);
    assert!(first_page["nextCursor"].is_string());
    let second_page = client
        .ok(
            "thread/list",
            json!({"limit":1,"sortDirection":"asc","cursor":first_page["nextCursor"]}),
        )
        .await;
    let listed: HashSet<_> = [
        first_page["data"][0]["id"].as_str().unwrap(),
        second_page["data"][0]["id"].as_str().unwrap(),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        listed,
        [source.as_str(), second.as_str()].into_iter().collect()
    );
    assert!(second_page["nextCursor"].is_null());
    assert_eq!(
        client
            .request(
                "thread/loaded/list",
                json!({"cursor":first_page["nextCursor"]})
            )
            .await["error"]["code"],
        -32602
    );
    client.close().await;

    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let read = client
        .ok(
            "thread/read",
            json!({"threadId":source,"includeTurns":true}),
        )
        .await;
    assert_eq!(read["thread"]["name"], "Saved source");
    assert_eq!(read["thread"]["turns"], json!([terminal]));
    assert_eq!(read["thread"]["status"]["type"], "notLoaded");
    assert_eq!(
        client.ok("thread/loaded/list", json!({})).await["data"],
        json!([])
    );
    let resumed = client.ok("thread/resume", json!({"threadId":source})).await;
    assert_eq!(resumed["thread"]["turns"].as_array().unwrap().len(), 1);
    let next = client.start(&source, "after restart").await;
    client.completed(&next).await;
    let fork = client.ok("thread/fork", json!({"threadId":source})).await;
    let fork_id = fork["thread"]["id"].as_str().unwrap().to_owned();
    assert_ne!(fork_id, source);
    assert_eq!(fork["thread"]["forkedFromId"], source);
    assert_eq!(fork["thread"]["sessionId"], resumed["thread"]["sessionId"]);
    assert_eq!(fork["thread"]["turns"].as_array().unwrap().len(), 2);
    let source_record: Value = serde_json::from_slice(
        &std::fs::read(state.path().join("threads").join(format!("{source}.json"))).unwrap(),
    )
    .unwrap();
    let advanced = client.start(&source, "source advanced after fork").await;
    client.completed(&advanced).await;
    let fork_turn = client.start(&fork_id, "fork-info").await;
    let fork_result = client.completed(&fork_turn).await;
    let backend_args: Value = serde_json::from_str(
        fork_result["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "agentMessage")
            .unwrap()["text"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(backend_args["fork"], true);
    assert_eq!(backend_args["resume"], source_record["session_id"]);
    assert_eq!(
        backend_args["resumeAt"],
        source_record["backend_message_id"]
    );
    assert_eq!(
        client
            .ok(
                "thread/read",
                json!({"threadId":source,"includeTurns":true})
            )
            .await["thread"]["turns"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    client
        .ok("thread/archive", json!({"threadId":source}))
        .await;
    let archived = client.ok("thread/list", json!({"archived":true})).await;
    assert_eq!(archived["data"].as_array().unwrap().len(), 1);
    assert_eq!(archived["data"][0]["id"], source);
    assert_eq!(
        client
            .request("thread/resume", json!({"threadId":source}))
            .await["error"]["code"],
        -32602
    );
    client
        .ok("thread/unarchive", json!({"threadId":source}))
        .await;
    assert_eq!(
        client.ok("thread/list", json!({"archived":true})).await["data"],
        json!([])
    );
    client.close().await;
}

#[tokio::test]
async fn independent_threads_progress_while_another_turn_waits() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let blocked = client.thread().await;
    let fast = client.thread().await;
    let waiting = client.start(&blocked, "tool").await;
    let approval = client
        .until(|value| {
            value["method"] == "item/commandExecution/requestApproval"
                && value["params"]["turnId"] == waiting
        })
        .await;
    let quick = client.start(&fast, "concurrent").await;
    client.completed(&quick).await;
    assert_echo_lifecycle(&client, &quick, "Echo: concurrent");
    assert!(!client.history.iter().any(
        |value| value["method"] == "turn/completed" && value["params"]["turn"]["id"] == waiting
    ));
    client
        .send(json!({"id":approval["id"],"result":{"decision":"accept"}}))
        .await;
    client.completed(&waiting).await;
    client.close().await;
}

#[tokio::test]
async fn unsupported_sandbox_options_and_invalid_inputs_are_rejected() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    for params in [
        json!({"sandbox":"read-only"}),
        json!({"sandbox":"external-sandbox"}),
        json!({"approvalPolicy":"untrusted"}),
        json!({"modelProvider":"openai"}),
        json!({"serviceTier":"flex"}),
        json!({"config":{"model":"other"}}),
    ] {
        let error = client.request("thread/start", params).await;
        assert_eq!(error["error"]["code"], -32602, "{error}");
    }
    let thread = client.thread().await;
    let invalid = [
        json!({"threadId":thread,"input":[]}),
        json!({"threadId":thread,"input":[{"type":"text","text":"hello","text_elements":[]}],"sandboxPolicy":{"type":"readOnly"}}),
        json!({"threadId":thread,"input":[{"type":"text","text":"hello","text_elements":[]}],"serviceTier":"flex"}),
        json!({"threadId":thread,"input":[{"type":"text","text":"hello","text_elements":[]}],"serviceTierForTurn":"flex"}),
    ];
    for params in invalid {
        assert_eq!(
            client.request("turn/start", params).await["error"]["code"],
            -32602
        );
    }
    assert_eq!(
        client
            .request("turn/steer", json!({"threadId":thread,"input":[]}))
            .await["error"]["code"],
        -32602
    );
    assert_eq!(
        client
            .ok(
                "thread/read",
                json!({"threadId":thread,"includeTurns":true})
            )
            .await["thread"]["turns"],
        json!([]),
        "rejected requests must not create turns"
    );
    client.close().await;
}

#[tokio::test]
async fn background_agent_continuations_keep_stdin_open_until_idle() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let turn = client.start(&thread, "background").await;
    let approval = client
        .until(|v| v["method"] == "item/commandExecution/requestApproval")
        .await;
    assert!(
        !client
            .history
            .iter()
            .any(|v| v["method"] == "turn/completed")
    );
    let pending = client
        .ok(
            "thread/read",
            json!({"threadId":thread,"includeTurns":true}),
        )
        .await;
    let task = pending["thread"]["turns"][0]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == "claude-task-bg")
        .expect("background work must stay visible");
    assert_eq!(task["status"], "inProgress");
    client
        .send(json!({"id":approval["id"], "result":{"decision":"accept"}}))
        .await;
    let turn = client.completed(&turn).await;
    assert_eq!(turn["status"], "completed");
    let task = turn["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == "claude-task-bg")
        .unwrap();
    assert_eq!(task["status"], "completed");
    assert_eq!(task["success"], true);
    let messages: Vec<&str> = turn["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["type"] == "agentMessage")
        .filter_map(|i| i["text"].as_str())
        .collect();
    assert_eq!(messages, ["Parent waiting", "Background complete"]);
    client.close().await;
}

#[tokio::test]
async fn interruption_drains_final_transcript_and_next_turn_resumes() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let turn = client.start(&thread, "hang").await;
    client
        .until(|v| v["method"] == "item/agentMessage/delta" && v["params"]["delta"] == "Working")
        .await;
    client
        .ok("turn/interrupt", json!({"threadId":thread,"turnId":turn}))
        .await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "interrupted");
    assert!(
        completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["text"] == "Interrupted")
    );
    let next = client.start(&thread, "resume-info").await;
    let completed = client.completed(&next).await;
    assert!(
        completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["text"] == "resumed")
    );
    client.close().await;
}

#[tokio::test]
async fn desktop_discovery_reports_only_facade_capabilities() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let config = client
        .ok(
            "config/read",
            json!({"cwd":state.path(),"includeLayers":true}),
        )
        .await;
    assert_eq!(config["config"]["model_provider"], "anthropic");
    assert_eq!(config["config"]["sandbox_mode"], "workspace-write");
    assert_eq!(config["config"]["approval_policy"], "on-request");
    assert_eq!(config["config"]["approvals_reviewer"], "user");
    assert_eq!(
        config["config"]["sandbox_workspace_write"],
        json!({
            "writable_roots": [], "network_access": false,
            "exclude_slash_tmp": false, "exclude_tmpdir_env_var": false
        })
    );
    let started = client.ok("thread/start", json!({"cwd":state.path()})).await;
    assert_eq!(
        started["sandbox"],
        json!({
            "type": "workspaceWrite", "writableRoots": [], "networkAccess": false,
            "excludeSlashTmp": false, "excludeTmpdirEnvVar": false
        })
    );
    assert_eq!(config["layers"], json!([]));
    let requirements = client.ok("configRequirements/read", Value::Null).await;
    assert_eq!(
        requirements["requirements"]["allowedSandboxModes"],
        json!(["workspace-write", "danger-full-access"])
    );
    assert_eq!(
        requirements["requirements"]["allowedApprovalPolicies"],
        json!(["on-request", "never"])
    );
    assert_eq!(
        requirements["requirements"]["allowedApprovalsReviewers"],
        json!(["user", "auto_review", "guardian_subagent"])
    );
    assert_eq!(
        client.ok("collaborationMode/list", json!({})).await["data"],
        json!([])
    );
    let features = client
        .ok("experimentalFeature/list", json!({"limit":100}))
        .await;
    assert_eq!(features["data"], json!([]));
    assert!(features["nextCursor"].is_null());
    client.close().await;
}

#[tokio::test]
async fn desktop_reopen_hydrates_snapshot_without_early_history_fetch() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let created = client
        .ok(
            "thread/start",
            json!({"historyMode":"paginated","sandbox":"danger-full-access"}),
        )
        .await;
    let id = created["thread"]["id"].as_str().unwrap();
    let turn = client.start(id, "persisted history").await;
    client.completed(&turn).await;
    client.close().await;

    // Seed a large valid persisted transcript from real wire-produced items.
    // Reopening, paging, and resuming still run through the public transport.
    let path = created["thread"]["path"].as_str().unwrap();
    let mut record: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let template = record["turns"][0].clone();
    record["turns"] = json!(
        (0..7)
            .map(|index| {
                let mut turn = template.clone();
                turn["id"] = json!(format!("saved-turn-{index}"));
                turn["items"] = json!(
                    (0..105)
                        .map(|item_index| {
                            let mut item = template["items"][1].clone();
                            item["id"] = json!(format!("saved-item-{index}-{item_index}"));
                            item
                        })
                        .collect::<Vec<_>>()
                );
                turn
            })
            .collect::<Vec<_>>()
    );
    std::fs::write(path, serde_json::to_vec(&record).unwrap()).unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let metadata = client
        .ok("thread/read", json!({"threadId":id,"includeTurns":false}))
        .await;
    let request = json!({"threadId":id,"history":null,"path":metadata["thread"]["path"],
        "model":null,"modelProvider":"anthropic","cwd":metadata["thread"]["cwd"],
        "personality":"friendly","excludeTurns":true,"config":{},
        "initialTurnsPage":{"limit":5,"itemsView":"full","sortDirection":"desc"}});
    let resumed = client.ok("thread/resume", request.clone()).await;
    assert_eq!(resumed["thread"]["turns"], json!([]));
    assert_eq!(
        resumed["initialTurnsPage"]["data"]
            .as_array()
            .unwrap()
            .len(),
        5
    );
    assert_eq!(resumed["initialTurnsPage"]["data"][0]["id"], "saved-turn-6");
    let older = client
        .ok(
            "thread/turns/list",
            json!({"threadId":id,
        "cursor":resumed["initialTurnsPage"]["nextCursor"], "limit":5,
        "itemsView":"notLoaded", "sortDirection":"desc"}),
        )
        .await;
    assert_eq!(older["data"].as_array().unwrap().len(), 2);
    assert_eq!(older["data"][0]["id"], "saved-turn-1");
    assert_eq!(older["data"][0]["items"], json!([]));
    // A newer turn must not move the inclusive snapshot boundary.
    let new_turn = client.start(id, "newer than resume").await;
    client.completed(&new_turn).await;
    let mut cursor = resumed["turnsBackwardsCursor"].clone();
    assert!(cursor.is_string());
    let mut turn_ids = vec![];
    loop {
        let page = client
            .ok(
                "thread/turns/list",
                json!({"threadId":id,"cursor":cursor,
            "limit":5,"itemsView":"notLoaded","sortDirection":"desc"}),
            )
            .await;
        for turn in page["data"].as_array().unwrap() {
            turn_ids.push(turn["id"].as_str().unwrap().to_owned());
            let mut item_cursor = resumed["itemsBackwardsCursor"].clone();
            let mut items = vec![];
            loop {
                let page = client
                    .ok(
                        "thread/items/list",
                        json!({"threadId":id,"turnId":turn["id"],
                    "cursor":item_cursor,"limit":100,"sortDirection":"desc"}),
                    )
                    .await;
                items.extend(
                    page["data"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|item| item["item"]["id"].clone()),
                );
                item_cursor = page["nextCursor"].clone();
                if item_cursor.is_null() {
                    break;
                }
            }
            assert_eq!(items.len(), 105);
            assert_eq!(
                items
                    .iter()
                    .map(Value::to_string)
                    .collect::<HashSet<_>>()
                    .len(),
                105
            );
        }
        cursor = page["nextCursor"].clone();
        if cursor.is_null() {
            break;
        }
    }
    assert_eq!(
        turn_ids,
        (0..7)
            .rev()
            .map(|index| format!("saved-turn-{index}"))
            .collect::<Vec<_>>()
    );
    let mut by_path = request.clone();
    by_path["threadId"] = json!("ignored-for-path-resume");
    assert_eq!(
        client.ok("thread/resume", by_path).await["thread"]["id"],
        id
    );
    let mut invalid = request;
    invalid["path"] = json!(state.path().join("not-a-thread.json"));
    assert_eq!(
        client.request("thread/resume", invalid).await["error"]["code"],
        -32602
    );
    client.close().await;
}

#[cfg(unix)]
#[tokio::test]
async fn desktop_active_resume_from_second_client_keeps_running_settings() {
    use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
    async fn request(
        read: &mut Lines<BufReader<OwnedReadHalf>>,
        write: &mut OwnedWriteHalf,
        id: u64,
        method: &str,
        params: Value,
    ) -> Value {
        write
            .write_all(format!("{}\n", json!({"id":id,"method":method,"params":params})).as_bytes())
            .await
            .unwrap();
        loop {
            let line = tokio::time::timeout(DEADLINE, read.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let value: Value = serde_json::from_str(&line).unwrap();
            if value["id"] == id && value.get("method").is_none() {
                return value;
            }
        }
    }
    let state = tempfile::Builder::new()
        .prefix("facade-resume-")
        .tempdir_in("/tmp")
        .unwrap();
    let endpoint = format!("unix-lines://{}", state.path().join("rpc.sock").display());
    let mut child = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
        .args([
            "--listen",
            &endpoint,
            "--claude",
            env!("CARGO_BIN_EXE_fake-claude"),
            "--state-dir",
        ])
        .arg(state.path())
        .current_dir(state.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut log = BufReader::new(child.stderr.take().unwrap()).lines();
    assert!(
        tokio::time::timeout(DEADLINE, log.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .starts_with("Listening on")
    );
    let (ar, mut aw) = tokio::net::UnixStream::connect(state.path().join("rpc.sock"))
        .await
        .unwrap()
        .into_split();
    let (br, mut bw) = tokio::net::UnixStream::connect(state.path().join("rpc.sock"))
        .await
        .unwrap()
        .into_split();
    let mut ar = BufReader::new(ar).lines();
    let mut br = BufReader::new(br).lines();
    let initialize = json!({"clientInfo":{"name":"wire-resume","version":"1"}});
    assert!(
        request(&mut ar, &mut aw, 1, "initialize", initialize.clone())
            .await
            .get("error")
            .is_none()
    );
    assert!(
        request(&mut br, &mut bw, 1, "initialize", initialize)
            .await
            .get("error")
            .is_none()
    );
    let created = request(
        &mut ar,
        &mut aw,
        2,
        "thread/start",
        json!({"baseInstructions":"Be precise","developerInstructions":"Be concise","sandbox":"danger-full-access"}),
    )
    .await;
    let id = &created["result"]["thread"]["id"];
    let started = request(
        &mut ar,
        &mut aw,
        3,
        "turn/start",
        json!({"threadId":id,"input":[{"type":"text","text":"hang","text_elements":[]}]}),
    )
    .await;
    assert!(started.get("error").is_none(), "{started}");
    let echo = json!({"threadId":id,"path":created["result"]["thread"]["path"],
        "cwd":created["result"]["cwd"],"modelProvider":"anthropic","excludeTurns":true,
        "baseInstructions":"Be precise","developerInstructions":"Be concise",
        "approvalPolicy":"on-request","sandbox":"danger-full-access"});
    let resumed = request(&mut br, &mut bw, 2, "thread/resume", echo.clone()).await;
    assert!(resumed.get("error").is_none(), "{resumed}");
    validate("ThreadResumeResponse", &resumed["result"]);
    // Like Codex, opening a running chat attaches to it; differing launch
    // settings (such as the desktop's sandbox preset) are ignored, not refused.
    let mut changed = echo.clone();
    changed["model"] = json!("different-model");
    changed["developerInstructions"] = json!("Changed");
    changed["sandbox"] = json!("workspace-write");
    let attached = request(&mut br, &mut bw, 3, "thread/resume", changed).await;
    assert!(attached.get("error").is_none(), "{attached}");
    validate("ThreadResumeResponse", &attached["result"]);
    assert_eq!(attached["result"]["model"], resumed["result"]["model"]);
    assert_eq!(attached["result"]["sandbox"], resumed["result"]["sandbox"]);
    let mut stale = echo.clone();
    stale["path"] = json!("/not/the/active/path");
    assert_eq!(
        request(&mut br, &mut bw, 4, "thread/resume", stale).await["error"]["code"],
        -32602
    );
    let interrupted = request(
        &mut br,
        &mut bw,
        6,
        "turn/interrupt",
        json!({"threadId":id,"turnId":started["result"]["turn"]["id"]}),
    )
    .await;
    assert!(interrupted.get("error").is_none());
    // Receiving terminal notifications proves the second client subscribed.
    loop {
        let line = tokio::time::timeout(DEADLINE, br.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let value: Value = serde_json::from_str(&line).unwrap();
        if value["method"] == "turn/completed" {
            break;
        }
    }
    let idle_resume = request(&mut br, &mut bw, 7, "thread/resume", echo.clone()).await;
    assert!(idle_resume.get("error").is_none(), "{idle_resume}");
    let mut changed = echo;
    changed["developerInstructions"] = json!("Different after session creation");
    changed["baseInstructions"] = json!("Updated base instructions");
    let updated = request(&mut br, &mut bw, 8, "thread/resume", changed).await;
    assert!(updated.get("error").is_none(), "{updated}");
    validate("ThreadResumeResponse", &updated["result"]);
    child.kill().await.unwrap();
}

#[tokio::test]
async fn desktop_host_services_and_timeline_are_routed_with_schema_valid_results() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let path = state.path().join("binary.dat");
    let copy = state.path().join("copied.dat");
    client
        .ok("fs/writeFile", json!({"path":path,"dataBase64":"AP9BQgo="}))
        .await;
    assert_eq!(
        client.ok("fs/readFile", json!({"path":path})).await["dataBase64"],
        "AP9BQgo="
    );
    client
        .ok("fs/copy", json!({"sourcePath":path,"destinationPath":copy}))
        .await;
    assert_eq!(std::fs::read(&copy).unwrap(), b"\0\xffAB\n");
    client.ok("fs/remove", json!({"path":copy})).await;
    assert!(!copy.exists());
    let result = client
        .ok(
            "command/exec",
            json!({"command":["/bin/sh","-c","printf routed"],"cwd":state.path()}),
        )
        .await;
    assert_eq!(result["stdout"], "routed");
    assert_eq!(result["exitCode"], 0);
    let thread = client.thread().await;
    let turn = client.start(&thread, "hello").await;
    client.completed(&turn).await;
    let page = client
        .ok(
            "thread/timeline/list",
            json!({"threadId":thread,"limit":100}),
        )
        .await;
    let entries = page["data"].as_array().unwrap();
    assert_eq!(entries.first().unwrap()["type"], "turnStarted");
    assert_eq!(entries.last().unwrap()["type"], "turnCompleted");
    assert!(entries.iter().all(|entry| entry["turnId"] == turn));
    client.close().await;
}

#[tokio::test]
async fn workspace_attachment_metadata_and_environment_survive_restart() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let started = client.ok("thread/start",json!({"sandbox":"danger-full-access",
        "config":{"shell_environment_policy.inherit":"all","shell_environment_policy.set":{"FACADE_WORKTREE":"yes"}},
        "disabledPluginIds":["sample@local"]})).await;
    let id = started["thread"]["id"].as_str().unwrap().to_owned();
    assert_eq!(started["disabledPluginIds"], json!(["sample@local"]));
    let added = client.ok("thread/attachment/add",json!({"threadId":id,"attachmentType":"archived_worktree",
        "identityKey":"/tmp/worktree-metadata","payload":{"worktree":{"snapshot":"preserved"},"pullRequests":[]}})).await;
    assert_eq!(added["outcome"], "created");
    client
        .until(|v| v["method"] == "thread/attachment/updated")
        .await;
    let descendants = client
        .ok(
            "thread/list",
            json!({"ancestorThreadId":id,"sourceKinds":["subAgentThreadSpawn"]}),
        )
        .await;
    assert!(descendants["data"].as_array().unwrap().is_empty());
    client.close().await;
    let persisted: Value = serde_json::from_slice(
        &std::fs::read(state.path().join("threads").join(format!("{id}.json"))).unwrap(),
    )
    .unwrap();
    assert_eq!(
        persisted["settings"]["environment_overrides"]["FACADE_WORKTREE"],
        "yes"
    );
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let resumed = client.ok("thread/resume", json!({"threadId":id})).await;
    assert_eq!(resumed["disabledPluginIds"], json!(["sample@local"]));
    let listed = client
        .ok("thread/attachment/list", json!({"threadId":id}))
        .await;
    assert_eq!(
        listed["data"][0]["payload"]["worktree"]["snapshot"],
        "preserved"
    );
    client.ok("thread/attachment/remove",json!({"threadId":id,"attachmentType":"archived_worktree","identityKey":"/tmp/worktree-metadata"})).await;
    client.close().await;
}

#[tokio::test]
async fn context_usage_is_latest_request_and_replays_after_restart() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let turn = client.start(&thread, "usage-probe").await;
    client.completed(&turn).await;
    let usage = client
        .history
        .iter()
        .rev()
        .find(|m| m["method"] == "thread/tokenUsage/updated")
        .unwrap()["params"]["tokenUsage"]
        .clone();
    assert_eq!(usage["last"]["totalTokens"], 7);
    assert_eq!(usage["total"]["totalTokens"], 70);
    assert_eq!(usage["modelContextWindow"], 200000);
    client.close().await;
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    client.ok("thread/resume", json!({"threadId":thread})).await;
    let replay = client
        .until(|m| m["method"] == "thread/tokenUsage/updated")
        .await;
    assert_eq!(replay["params"]["tokenUsage"], usage);
    client.close().await;
    let path = state.path().join("threads").join(format!("{thread}.json"));
    let mut legacy: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    legacy
        .as_object_mut()
        .unwrap()
        .remove("tracks_request_usage");
    legacy["token_usage"]["last"] = legacy["token_usage"]["total"].clone();
    std::fs::write(&path, serde_json::to_vec(&legacy).unwrap()).unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    client.ok("thread/resume", json!({"threadId":thread})).await;
    let replay = client
        .until(|m| m["method"] == "thread/tokenUsage/updated")
        .await;
    assert!(replay["params"]["tokenUsage"]["modelContextWindow"].is_null());
    assert_eq!(replay["params"]["tokenUsage"]["total"], usage["total"]);
    client.close().await;
}

fn item<'a>(turn: &'a Value, id: &str) -> &'a Value {
    turn["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == id)
        .unwrap_or_else(|| panic!("missing item {id} in {turn}"))
}

fn agent_texts(turn: &Value) -> Vec<String> {
    turn["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["type"] == "agentMessage")
        .map(|item| item["text"].as_str().unwrap().to_owned())
        .collect()
}

fn ids(list: &Value) -> Vec<String> {
    list["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|thread| thread["id"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn foreground_subagent_streams_into_a_linked_child_thread() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let turn = client.start(&thread, "subagent").await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "completed", "{completed}");
    let spawn = item(&completed, "agent_call");
    assert_eq!(spawn["type"], "collabAgentToolCall");
    assert_eq!(spawn["tool"], "spawnAgent");
    assert_eq!(spawn["status"], "completed");
    assert_eq!(spawn["senderThreadId"], thread.as_str());
    assert_eq!(spawn["prompt"], "Look around");
    let child = spawn["receiverThreadIds"][0].as_str().unwrap().to_owned();
    assert_ne!(child, thread);
    let wait = item(&completed, "agent_call-wait");
    assert_eq!(wait["tool"], "wait");
    assert_eq!(wait["status"], "completed");
    assert_eq!(wait["agentsStates"][&child]["status"], "completed");
    assert_eq!(wait["agentsStates"][&child]["message"], "Report: all good");
    assert_eq!(agent_texts(&completed), ["Parent done"]);

    let child_turn = client
        .until(|v| v["method"] == "turn/completed" && v["params"]["threadId"] == child.as_str())
        .await["params"]["turn"]
        .clone();
    assert_eq!(child_turn["status"], "completed");
    assert_eq!(child_turn["items"][0]["type"], "userMessage");
    assert_eq!(child_turn["items"][0]["content"][0]["text"], "Look around");
    assert_eq!(agent_texts(&child_turn), ["Child working", "Child done"]);
    let bash = item(&child_turn, "child_bash");
    assert_eq!(bash["status"], "completed");
    assert_eq!(bash["aggregatedOutput"], "listing");
    // The child's events arrive on the child thread, never the parent's.
    assert!(
        client
            .history
            .iter()
            .all(|v| v["params"]["item"]["id"] != "child_bash"
                || v["params"]["threadId"] == child.as_str())
    );

    let read = client
        .ok("thread/read", json!({"threadId":child,"includeTurns":true}))
        .await;
    let view = &read["thread"];
    assert_eq!(view["parentThreadId"], thread.as_str());
    assert_eq!(view["threadSource"], "subagent");
    assert_eq!(view["agentNickname"], "scout");
    assert_eq!(view["agentRole"], "general-purpose");
    let spawned = &view["source"]["subAgent"]["thread_spawn"];
    assert_eq!(spawned["parent_thread_id"], thread.as_str());
    assert_eq!(spawned["depth"], 1);
    assert_eq!(view["turns"].as_array().unwrap().len(), 1);

    // The nested agent's thread hangs off the child and shows its report.
    let nested = item(&child_turn, "nested_call");
    assert_eq!(nested["tool"], "spawnAgent");
    let grandchild = nested["receiverThreadIds"][0].as_str().unwrap().to_owned();
    assert_eq!(
        item(&child_turn, "nested_call-wait")["agentsStates"][&grandchild]["message"],
        "Nested report"
    );
    let read = client
        .ok(
            "thread/read",
            json!({"threadId":grandchild,"includeTurns":true}),
        )
        .await;
    assert_eq!(read["thread"]["parentThreadId"], child.as_str());
    assert_eq!(
        read["thread"]["source"]["subAgent"]["thread_spawn"]["depth"],
        2
    );
    assert_eq!(read["thread"]["turns"][0]["status"], "completed");
    assert_eq!(agent_texts(&read["thread"]["turns"][0]), ["Nested report"]);

    let sorted = |mut ids: Vec<String>| {
        ids.sort();
        ids
    };
    assert_eq!(
        ids(&client.ok("thread/list", json!({})).await),
        [thread.as_str()]
    );
    assert_eq!(
        ids(&client
            .ok("thread/list", json!({"parentThreadId": thread}))
            .await),
        [child.as_str()]
    );
    let descendants = sorted(vec![child.clone(), grandchild.clone()]);
    for filter in [
        json!({"ancestorThreadId": thread}),
        json!({"sourceKinds": ["subAgentThreadSpawn"]}),
        json!({"sourceKinds": ["subAgent"]}),
    ] {
        assert_eq!(
            sorted(ids(&client.ok("thread/list", filter).await)),
            descendants
        );
    }
    let rejected = client
        .request(
            "turn/start",
            json!({"threadId":child,"input":[{"type":"text","text":"more","text_elements":[]}]}),
        )
        .await;
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap()
            .contains("read-only"),
        "{rejected}"
    );
    client.close().await;

    // After a restart, a continued agent resumes the same child thread.
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    client.ok("thread/resume", json!({"threadId":thread})).await;
    let read = client
        .ok("thread/read", json!({"threadId":child,"includeTurns":true}))
        .await;
    assert_eq!(read["thread"]["parentThreadId"], thread.as_str());
    assert_eq!(read["thread"]["turns"][0]["status"], "completed");
    let turn = client.start(&thread, "subagent-continue").await;
    let completed = client.completed(&turn).await;
    assert_eq!(agent_texts(&completed), ["Parent continued"]);
    let continued = client
        .until(|v| v["method"] == "turn/completed" && v["params"]["threadId"] == child.as_str())
        .await["params"]["turn"]
        .clone();
    assert_eq!(continued["status"], "completed");
    assert_eq!(continued["items"][0]["content"][0]["text"], "More please");
    assert_eq!(agent_texts(&continued), ["Continued"]);
    let read = client
        .ok("thread/read", json!({"threadId":child,"includeTurns":true}))
        .await;
    assert_eq!(read["thread"]["turns"].as_array().unwrap().len(), 2);
    client.close().await;
}

#[tokio::test]
async fn background_subagent_keeps_its_child_turn_open_past_the_launch_ack() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let turn = client.start(&thread, "subagent-background").await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "completed", "{completed}");
    assert_eq!(
        agent_texts(&completed),
        ["Parent waiting", "Parent saw report"]
    );
    let spawn = item(&completed, "agent_bg");
    assert_eq!(spawn["status"], "completed");
    let child = spawn["receiverThreadIds"][0].as_str().unwrap().to_owned();
    let wait = item(&completed, "agent_bg-wait");
    assert_eq!(wait["status"], "completed");
    assert_eq!(wait["agentsStates"][&child]["message"], "Background report");
    assert!(
        completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["type"] != "dynamicToolCall"),
        "linked agents need no placeholder: {completed}"
    );
    let child_turn = client
        .until(|v| v["method"] == "turn/completed" && v["params"]["threadId"] == child.as_str())
        .await["params"]["turn"]
        .clone();
    assert_eq!(child_turn["status"], "completed");
    assert_eq!(agent_texts(&child_turn), ["Background report"]);
    // The child turn ends with the task notification, after the parent's first
    // result answered the launch acknowledgement.
    let position =
        |predicate: &dyn Fn(&Value) -> bool| client.history.iter().position(predicate).unwrap();
    let parent_waiting = position(&|v| {
        v["method"] == "item/completed" && v["params"]["item"]["text"] == "Parent waiting"
    });
    let child_done =
        position(&|v| v["method"] == "turn/completed" && v["params"]["threadId"] == child.as_str());
    assert!(parent_waiting < child_done);
    client.close().await;
}

/// The fake backend's process ID and how many prompts that process has seen,
/// from a completed `process-info` turn.
fn process_info(completed: &Value) -> (i32, u32) {
    assert_eq!(completed["status"], "completed");
    let text = agent_texts(completed).pop().unwrap();
    let words: Vec<&str> = text.split(' ').collect();
    (words[1].parse().unwrap(), words[3].parse().unwrap())
}

async fn probe_process(client: &mut Client, thread: &str, options: Value) -> (i32, u32) {
    let mut params = json!({"threadId":thread,
        "input":[{"type":"text","text":"process-info","text_elements":[]}]});
    params
        .as_object_mut()
        .unwrap()
        .extend(options.as_object().unwrap().clone());
    let turn = client.ok("turn/start", params).await["turn"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    process_info(&client.completed(&turn).await)
}

fn running(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

async fn wait_for_exit(pid: i32) {
    tokio::time::timeout(DEADLINE, async {
        while running(pid) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Claude process did not exit");
}

#[tokio::test]
async fn idle_claude_process_serves_later_turns_until_its_launch_changes() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let (first, prompts) = probe_process(&mut client, &thread, json!({})).await;
    assert_eq!(prompts, 1);
    // An unchanged launch reuses the process, keeping Claude's request prefix cached.
    assert_eq!(
        probe_process(&mut client, &thread, json!({})).await,
        (first, 2)
    );
    // A changed launch closes the idle process and resumes the session in a new one.
    let (second, prompts) = probe_process(&mut client, &thread, json!({"model":"opus"})).await;
    assert_ne!(second, first);
    assert_eq!(prompts, 1);
    assert!(!running(first));
    assert!(running(second));
    // Archiving acknowledges at once; the idle process closes in the background.
    client
        .ok("thread/archive", json!({"threadId":thread}))
        .await;
    wait_for_exit(second).await;
    client.close().await;

    let state = tempfile::tempdir().unwrap();
    let mut client =
        Client::spawn_with_args(state.path(), &["--idle-session-timeout-seconds", "0"]).await;
    client.initialize().await;
    let thread = client.thread().await;
    let (first, _) = probe_process(&mut client, &thread, json!({})).await;
    let (second, prompts) = probe_process(&mut client, &thread, json!({})).await;
    assert_ne!(second, first);
    assert_eq!(prompts, 1);
    client.close().await;
}

#[tokio::test]
async fn closing_a_claude_process_does_not_hold_up_other_threads() {
    let state = tempfile::tempdir().unwrap();
    let mut client =
        Client::spawn_with_args(state.path(), &["--claude-arg=--fake-slow-exit"]).await;
    client.initialize().await;
    let thread = client.thread().await;
    let other = client.thread().await;
    let (first, _) = probe_process(&mut client, &thread, json!({})).await;
    // A changed launch closes the idle process, which takes seconds here.
    let sent = std::time::Instant::now();
    client
        .methods
        .insert(json!("relaunch").to_string(), "turn/start".into());
    client
        .send(
            json!({"id":"relaunch","method":"turn/start","params":{"threadId":thread,
            "model":"opus","input":[{"type":"text","text":"process-info","text_elements":[]}]}}),
        )
        .await;
    client.ok("thread/read", json!({"threadId":other})).await;
    assert!(
        sent.elapsed() < Duration::from_millis(1500),
        "another thread waited {:?} for the closing process",
        sent.elapsed()
    );
    let reply = client.until(|value| value["id"] == "relaunch").await;
    let turn = reply["result"]["turn"]["id"].as_str().unwrap().to_owned();
    let (second, prompts) = process_info(&client.completed(&turn).await);
    // The new process still resumed the session only after the old one exited.
    assert_ne!(second, first);
    assert_eq!(prompts, 1);
    assert!(!running(first));
    client.close().await;
}

#[tokio::test]
async fn probing_the_model_catalog_does_not_hold_up_other_requests() {
    let state = tempfile::tempdir().unwrap();
    let mut client =
        Client::spawn_with_args(state.path(), &["--claude-arg=--fake-init-hang"]).await;
    client.initialize().await;
    client
        .methods
        .insert(json!("catalog").to_string(), "model/list".into());
    client
        .send(json!({"id":"catalog","method":"model/list","params":{}}))
        .await;
    client.thread().await;
    assert!(
        !client.history.iter().any(|value| value["id"] == "catalog"),
        "thread/start waited for the model catalog probe"
    );
    // The probe's initialization times out, and the request reports it.
    let catalog = client.until(|value| value["id"] == "catalog").await;
    assert!(catalog["error"]["message"].is_string(), "{catalog}");
    client.close().await;
}

#[tokio::test]
async fn equivalent_workspace_policies_keep_one_claude_process() {
    let state = tempfile::tempdir().unwrap();
    let project = state.path().join("project");
    let cache = state.path().join("cache");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::create_dir_all(&cache).unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client
        .ok(
            "thread/start",
            json!({"cwd":project,"sandbox":"workspace-write"}),
        )
        .await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let policy = |roots: Value| {
        json!({"sandboxPolicy":{"type":"workspaceWrite","writableRoots":roots,
            "networkAccess":true,"excludeSlashTmp":false,"excludeTmpdirEnvVar":false}})
    };
    // The desktop's first turn names the temporary directories (which
    // workspace-write keeps writable anyway) and repeats roots; later turns don't.
    let (first, prompts) = probe_process(
        &mut client,
        &thread,
        policy(json!([cache, "/tmp", std::env::temp_dir(), cache, project])),
    )
    .await;
    assert_eq!(prompts, 1);
    assert_eq!(
        probe_process(&mut client, &thread, policy(json!([cache]))).await,
        (first, 2)
    );
    // A policy that grants something else still needs its own launch.
    let (second, prompts) = probe_process(&mut client, &thread, policy(json!([]))).await;
    assert_ne!(second, first);
    assert_eq!(prompts, 1);
    client.close().await;
}

#[tokio::test]
async fn fast_service_tier_selects_claude_fast_mode() {
    async fn probe(client: &mut Client, thread: &str, options: Value) -> String {
        let mut params = json!({"threadId":thread,
            "input":[{"type":"text","text":"fast-info","text_elements":[]}]});
        params
            .as_object_mut()
            .unwrap()
            .extend(options.as_object().unwrap().clone());
        let turn = client.ok("turn/start", params).await["turn"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        agent_texts(&client.completed(&turn).await).pop().unwrap()
    }

    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let models = client.ok("model/list", json!({})).await["data"].clone();
    let tiers = |id: &str| {
        models
            .as_array()
            .unwrap()
            .iter()
            .find(|model| model["id"] == id)
            .unwrap()["serviceTiers"]
            .clone()
    };
    assert_eq!(tiers("opus")[0]["id"], "priority");
    assert_eq!(tiers("sonnet"), json!([]));

    let started = client
        .ok(
            "thread/start",
            json!({"sandbox":"danger-full-access","model":"opus","serviceTier":"priority"}),
        )
        .await;
    assert_eq!(started["serviceTier"], "priority");
    let thread = started["thread"]["id"].as_str().unwrap().to_owned();
    assert_eq!(
        probe(&mut client, &thread, json!({})).await,
        "fast mode true"
    );
    // Claude ran at standard speed despite the request, so the client is told why.
    let warning = client.until(|v| v["method"] == "warning").await;
    assert!(
        warning["params"]["message"]
            .as_str()
            .unwrap()
            .contains("extra_usage_disabled"),
        "{warning}"
    );
    // A per-turn tier does not change the thread's tier.
    assert_eq!(
        probe(
            &mut client,
            &thread,
            json!({"serviceTierForTurn":"default"})
        )
        .await,
        "fast mode false"
    );
    assert_eq!(
        probe(&mut client, &thread, json!({})).await,
        "fast mode true"
    );
    // Desktop sends null for standard speed.
    assert_eq!(
        probe(&mut client, &thread, json!({"serviceTier":null})).await,
        "fast mode false"
    );
    assert_eq!(
        probe(&mut client, &thread, json!({"serviceTierForTurn":"fast"})).await,
        "fast mode true"
    );
    client
        .ok(
            "thread/settings/update",
            json!({"threadId":thread,"serviceTier":"priority"}),
        )
        .await;
    let settings = client
        .until(|v| v["method"] == "thread/settings/updated")
        .await;
    assert_eq!(
        settings["params"]["threadSettings"]["serviceTier"],
        "priority"
    );
    assert_eq!(
        probe(&mut client, &thread, json!({})).await,
        "fast mode true"
    );
    client.close().await;
}

/// Waits for the turn Claude starts by itself after `after`, and returns its text.
async fn woken_turn(client: &mut Client, thread: &str, after: &str) -> Vec<String> {
    let started = client
        .until(|v| {
            v["method"] == "turn/started"
                && v["params"]["threadId"] == thread
                && v["params"]["turn"]["id"] != after
        })
        .await;
    let woke = started["params"]["turn"]["id"].as_str().unwrap().to_owned();
    let completed = client.completed(&woke).await;
    assert_eq!(completed["status"], "completed");
    agent_texts(&completed)
}

#[tokio::test]
async fn idle_claude_work_after_a_turn_arrives_as_a_new_turn() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let first = client.start(&thread, "background-bash").await;
    assert_eq!(agent_texts(&client.completed(&first).await), ["Started"]);
    // Claude answers the background command's notification on its own.
    assert_eq!(
        woken_turn(&mut client, &thread, &first).await,
        ["Background output: BGDONE"]
    );
    let turns = client
        .ok(
            "thread/read",
            json!({"threadId":thread,"includeTurns":true}),
        )
        .await["thread"]["turns"]
        .clone();
    assert_eq!(turns.as_array().unwrap().len(), 2);
    // The woken process is parked again and serves the next turn.
    let next = client.start(&thread, "process-info").await;
    let text = agent_texts(&client.completed(&next).await).pop().unwrap();
    assert!(text.ends_with("prompt 2"), "{text}");
    client.close().await;
}

#[tokio::test]
async fn background_work_survives_idle_expiry_eviction_and_disabled_reuse() {
    // Running work outlives an idle timeout shorter than the work itself.
    let state = tempfile::tempdir().unwrap();
    let mut client =
        Client::spawn_with_args(state.path(), &["--idle-session-timeout-seconds", "1"]).await;
    client.initialize().await;
    let thread = client.thread().await;
    let first = client.start(&thread, "background-bash 1600").await;
    client.completed(&first).await;
    assert_eq!(
        woken_turn(&mut client, &thread, &first).await,
        ["Background output: BGDONE"]
    );
    client.close().await;

    // With process reuse disabled, the process stays only for its background work.
    let state = tempfile::tempdir().unwrap();
    let mut client =
        Client::spawn_with_args(state.path(), &["--idle-session-timeout-seconds", "0"]).await;
    client.initialize().await;
    let thread = client.thread().await;
    let first = client.start(&thread, "background-bash").await;
    client.completed(&first).await;
    assert_eq!(
        woken_turn(&mut client, &thread, &first).await,
        ["Background output: BGDONE"]
    );
    let next = client.start(&thread, "process-info").await;
    let text = agent_texts(&client.completed(&next).await).pop().unwrap();
    assert!(
        text.ends_with("prompt 1"),
        "a kept process never serves a new turn: {text}"
    );
    client.close().await;

    // Another thread's idle process does not evict one running background work.
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn_with_args(state.path(), &["--max-idle-sessions", "1"]).await;
    client.initialize().await;
    let working = client.thread().await;
    let first = client.start(&working, "background-bash 800").await;
    client.completed(&first).await;
    let other = client.thread().await;
    let turn = client.start(&other, "hello").await;
    client.completed(&turn).await;
    assert_eq!(
        woken_turn(&mut client, &working, &first).await,
        ["Background output: BGDONE"]
    );
    client.close().await;
}

#[tokio::test]
async fn archiving_a_chat_with_running_background_work_warns() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let first = client.start(&thread, "background-bash 5000").await;
    client.completed(&first).await;
    client
        .ok("thread/archive", json!({"threadId":thread}))
        .await;
    let warning = client.until(|v| v["method"] == "warning").await;
    assert!(
        warning["params"]["message"]
            .as_str()
            .unwrap()
            .contains("1 background task(s)"),
        "{warning}"
    );
    client.close().await;
}

#[tokio::test]
async fn first_prompt_waits_for_connecting_mcp_servers() {
    let state = tempfile::tempdir().unwrap();
    let mut client =
        Client::spawn_with_args(state.path(), &["--claude-arg=--fake-mcp-pending"]).await;
    client.initialize().await;
    let thread = client.thread().await;
    let turn = client.start(&thread, "mcp-info").await;
    let completed = client.completed(&turn).await;
    assert_eq!(
        agent_texts(&completed),
        ["prompted while MCP connecting: false"]
    );
    client.close().await;
}

/// A thread whose subagent spawned a nested agent: (root, child, grandchild).
async fn subagent_tree(client: &mut Client) -> (String, String, String) {
    let root = client.thread().await;
    let turn = client.start(&root, "subagent").await;
    let completed = client.completed(&turn).await;
    let child = item(&completed, "agent_call")["receiverThreadIds"][0]
        .as_str()
        .unwrap()
        .to_owned();
    let child_turn = client
        .until(|v| v["method"] == "turn/completed" && v["params"]["threadId"] == child.as_str())
        .await;
    let grandchild = item(&child_turn["params"]["turn"], "nested_call")["receiverThreadIds"][0]
        .as_str()
        .unwrap()
        .to_owned();
    (root, child, grandchild)
}

/// Thread ids announced deleted so far, in order.
fn deleted(client: &Client) -> Vec<String> {
    client
        .history
        .iter()
        .filter(|v| v["method"] == "thread/deleted")
        .map(|v| v["params"]["threadId"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn deleting_a_thread_removes_its_subagents_deepest_first() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let (root, child, grandchild) = subagent_tree(&mut client).await;
    let (other_root, other_child, other_grandchild) = subagent_tree(&mut client).await;
    let ephemeral =
        client.ok("thread/start", json!({"ephemeral":true})).await["thread"]["id"].clone();
    let refused = client
        .request("thread/delete", json!({"threadId":ephemeral}))
        .await;
    assert!(
        refused["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not persisted"),
        "{refused}"
    );

    client.ok("thread/delete", json!({"threadId":root})).await;
    client
        .until(|v| v["method"] == "thread/deleted" && v["params"]["threadId"] == root.as_str())
        .await;
    assert_eq!(
        deleted(&client),
        [grandchild.clone(), child.clone(), root.clone()]
    );
    let threads = state.path().join("threads");
    for id in [&root, &child, &grandchild] {
        let read = client.request("thread/read", json!({"threadId":id})).await;
        assert_eq!(read["error"]["code"], -32602, "{read}");
        assert!(!threads.join(format!("{id}.json")).exists());
    }
    assert_eq!(
        client
            .request("thread/delete", json!({"threadId":root}))
            .await["error"]["code"],
        -32602
    );

    // Deleting a subagent's own thread keeps its parent, which then no longer
    // links a continuation of that agent to the deleted thread.
    client
        .ok("thread/delete", json!({"threadId":other_child}))
        .await;
    client
        .until(|v| {
            v["method"] == "thread/deleted" && v["params"]["threadId"] == other_child.as_str()
        })
        .await;
    assert_eq!(
        deleted(&client)[3..],
        [other_grandchild.clone(), other_child.clone()]
    );
    let parent: Value =
        serde_json::from_slice(&std::fs::read(threads.join(format!("{other_root}.json"))).unwrap())
            .unwrap();
    let linked: Vec<&Value> = parent["agents"].as_object().unwrap().values().collect();
    assert!(
        !linked.contains(&&json!(other_child)) && !linked.contains(&&json!(other_grandchild)),
        "{parent}"
    );

    // As Codex shuts a running thread down, a running turn ends first.
    let busy = client.thread().await;
    let hanging = client.start(&busy, "hang").await;
    client.ok("thread/delete", json!({"threadId":busy})).await;
    assert_eq!(client.completed(&hanging).await["status"], "interrupted");
    client
        .until(|v| v["method"] == "thread/deleted" && v["params"]["threadId"] == busy.as_str())
        .await;
    client.close().await;

    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    assert_eq!(
        ids(&client.ok("thread/list", json!({})).await),
        [other_root.as_str()]
    );
    let read = client
        .request("thread/read", json!({"threadId":other_grandchild}))
        .await;
    assert_eq!(read["error"]["code"], -32602, "{read}");
    client.close().await;
}

#[tokio::test]
async fn search_occurrences_finds_visible_messages_and_opens_their_turns() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let long = format!("{}Needle{}", "é".repeat(60), "x".repeat(100));
    let mut turns = vec![];
    for text in ["Alpha beta ALPHA", "gamma alpha", long.as_str()] {
        let turn = client.start(&thread, text).await;
        client.completed(&turn).await;
        turns.push(turn);
    }
    let all = client
        .ok(
            "thread/searchOccurrences",
            json!({"threadId":thread,"searchTerm":"alpha"}),
        )
        .await;
    assert!(all["nextCursor"].is_null());
    let found: Vec<(&str, &str, Value)> = all["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|occurrence| {
            (
                occurrence["turnId"].as_str().unwrap(),
                occurrence["snippet"].as_str().unwrap(),
                occurrence["snippetMatchRange"].clone(),
            )
        })
        .collect();
    // User messages and each turn's final answer, case-insensitively, in order.
    assert_eq!(
        found,
        [
            (
                turns[0].as_str(),
                "Alpha beta ALPHA",
                json!({"start":0,"end":5})
            ),
            (
                turns[0].as_str(),
                "Alpha beta ALPHA",
                json!({"start":11,"end":16})
            ),
            (
                turns[0].as_str(),
                "Echo: Alpha beta ALPHA",
                json!({"start":6,"end":11})
            ),
            (
                turns[0].as_str(),
                "Echo: Alpha beta ALPHA",
                json!({"start":17,"end":22})
            ),
            (
                turns[1].as_str(),
                "gamma alpha",
                json!({"start":6,"end":11})
            ),
            (
                turns[1].as_str(),
                "Echo: gamma alpha",
                json!({"start":12,"end":17})
            ),
        ]
    );
    // One page per occurrence resumes within an item, too.
    let mut paged = vec![];
    let mut cursor = Value::Null;
    loop {
        let page = client
            .ok(
                "thread/searchOccurrences",
                json!({"threadId":thread,"searchTerm":"alpha","limit":1,"cursor":cursor}),
            )
            .await;
        paged.extend(page["data"].as_array().unwrap().iter().cloned());
        cursor = page["nextCursor"].clone();
        if cursor.is_null() {
            break;
        }
    }
    assert_eq!(json!(paged), all["data"]);
    let first_page = client
        .ok(
            "thread/searchOccurrences",
            json!({"threadId":thread,"searchTerm":"alpha","limit":1}),
        )
        .await;
    let foreign = client
        .request(
            "thread/searchOccurrences",
            json!({"threadId":thread,"searchTerm":"beta","cursor":first_page["nextCursor"]}),
        )
        .await;
    assert_eq!(foreign["error"]["code"], -32602, "{foreign}");
    let blank = client
        .request(
            "thread/searchOccurrences",
            json!({"threadId":thread,"searchTerm":"  "}),
        )
        .await;
    assert_eq!(blank["error"]["code"], -32602, "{blank}");

    // Snippets keep 49 characters before and 96 after, with UTF-16 ranges.
    let needle = client
        .ok(
            "thread/searchOccurrences",
            json!({"threadId":thread,"searchTerm":"NEEDLE"}),
        )
        .await;
    let snippet = format!("... {}Needle{} ...", "é".repeat(49), "x".repeat(96));
    for occurrence in needle["data"].as_array().unwrap() {
        assert_eq!(occurrence["snippet"], snippet.as_str());
        assert_eq!(
            occurrence["snippetMatchRange"],
            json!({"start":53,"end":59})
        );
    }
    assert_eq!(needle["data"].as_array().unwrap().len(), 2);

    // A match's turn cursor opens its turn in both directions.
    let turn_cursor = &all["data"][4]["turnCursor"];
    let older = client
        .ok(
            "thread/turns/list",
            json!({"threadId":thread,"cursor":turn_cursor,"limit":5,"sortDirection":"desc"}),
        )
        .await;
    assert_eq!(ids(&older), [turns[1].clone(), turns[0].clone()]);
    let newer = client
        .ok(
            "thread/turns/list",
            json!({"threadId":thread,"cursor":turn_cursor,"limit":5,"sortDirection":"asc"}),
        )
        .await;
    assert_eq!(ids(&newer), [turns[1].clone(), turns[2].clone()]);

    // Claude threads have no Codex goals.
    assert_eq!(
        client
            .ok("thread/goal/get", json!({"threadId":thread}))
            .await,
        json!({"goal":null})
    );
    let set = client
        .request(
            "thread/goal/set",
            json!({"threadId":thread,"objective":"ship it"}),
        )
        .await;
    assert_eq!(set["error"]["code"], -32601, "{set}");
    client.close().await;
}

#[tokio::test]
async fn section_moves_order_and_persist_claude_threads() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let first = client.thread().await;
    let second = client.thread().await;
    let other = client.thread().await;
    let section =
        json!({"id":"01984de2-8f74-7c91-a3b2-5c5e937cf318","name":"Pinned","appearance":null});
    let section_id = section["id"].clone();
    let move_to = |thread: &str, before: Value| json!({"threadId":thread,"sectionId":section_id,"beforeThreadId":before,"claudexSection":section});
    // Sections belong to genuine Codex; the router supplies the destination.
    let unknown = client
        .request(
            "thread/section/move",
            json!({"threadId":first,"sectionId":section_id}),
        )
        .await;
    assert_eq!(unknown["error"]["code"], -32602, "{unknown}");
    client
        .ok("thread/section/move", move_to(first.as_str(), Value::Null))
        .await;
    let moved = client
        .until(|v| v["method"] == "thread/section/changed")
        .await;
    assert_eq!(
        moved["params"],
        json!({"threadId":first,"sectionId":section_id})
    );
    client
        .ok(
            "thread/section/move",
            move_to(second.as_str(), json!(first)),
        )
        .await;
    for invalid in [
        move_to(first.as_str(), json!(first)),
        move_to(first.as_str(), json!(other)),
    ] {
        let refused = client.request("thread/section/move", invalid).await;
        assert_eq!(refused["error"]["code"], -32602, "{refused}");
    }
    let read = client.ok("thread/read", json!({"threadId":first})).await;
    assert_eq!(read["thread"]["section"], section);
    assert!(read["thread"]["sectionEnteredAt"].is_u64());
    let members = json!({"sectionId":section_id,"sortKey":"section_position"});
    assert_eq!(
        ids(&client.ok("thread/list", members.clone()).await),
        [second.as_str(), first.as_str()]
    );
    assert_eq!(
        ids(&client.ok("thread/list", json!({"sectionId":null})).await),
        [other.as_str()]
    );
    assert_eq!(ids(&client.ok("thread/list", json!({})).await).len(), 3);
    let unfiltered = client
        .request("thread/list", json!({"sortKey":"section_position"}))
        .await;
    assert_eq!(unfiltered["error"]["code"], -32602, "{unfiltered}");
    client.close().await;

    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    assert_eq!(
        ids(&client.ok("thread/list", members.clone()).await),
        [second.as_str(), first.as_str()]
    );
    client
        .ok(
            "thread/section/move",
            json!({"threadId":second,"sectionId":null}),
        )
        .await;
    let read = client.ok("thread/read", json!({"threadId":second})).await;
    assert!(read["thread"]["section"].is_null());
    assert!(read["thread"]["sectionEnteredAt"].is_null());
    assert_eq!(
        ids(&client.ok("thread/list", members).await),
        [first.as_str()]
    );
    client.close().await;
}
