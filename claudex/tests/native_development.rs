//! Opt-in coverage of macOS developer tools inside the real Claude sandbox.
#![cfg(target_os = "macos")]
use claude_codex_server::{
    backend::{Backend, BackendConfig, BackendEvent, SessionOptions},
    sandbox::WorkspaceDefaults,
};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
#[ignore = "requires authenticated Claude and gh, macOS sandbox access, and Ruby"]
async fn native_workspace_tls_cache_and_local_server() {
    // Keep the denied file outside native always-writable temporary directories.
    let fixture = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let workspace = fixture.path().join("workspace");
    let cache = fixture.path().join("cache");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&cache).unwrap();
    let cache_file = cache.join("probe.txt");
    let outside_file = fixture.path().join("denied.txt");
    let config = BackendConfig {
        executable: std::env::var_os("CLAUDE_BINARY")
            .map(Into::into)
            .unwrap_or_else(|| "claude".into()),
        extra_args: vec![
            "--setting-sources=".into(),
            "--strict-mcp-config".into(),
            "--tools=Bash".into(),
        ],
        initialize_timeout: Duration::from_secs(30),
        protocol: Default::default(),
    };
    let options = SessionOptions {
        state_dir: fixture.path().join("state"),
        cwd: workspace,
        session_id: uuid::Uuid::new_v4().to_string(),
        resume: false,
        fork_from: None,
        resume_at: None,
        model: "opus".into(),
        permission_mode: "acceptEdits".into(),
        system_prompt: None,
        append_system_prompt: None,
        effort: Some("low".into()),
        output_schema: None,
        ephemeral: true,
        dynamic_tools: vec![],
        native_settings: json!({"sandbox":{
            "allowUnsandboxedCommands":false,
            "enableWeakerNetworkIsolation":true,
            "network":{"allowLocalBinding":true}
        }}),
        sandbox: WorkspaceDefaults {
            writable_roots: vec![cache],
            network_access: true,
        }
        .policy(),
    };
    let command = format!(
        "gh api rate_limit --jq '.resources.core.limit' && printf 'TLS_OK\\n'; \
         /usr/bin/ruby -rsocket -e 's = TCPServer.new(\"127.0.0.1\", 0); puts \"BIND_OK\"; s.close'; \
         printf CACHE_OK > '{}'; printf DENIED > '{}'",
        cache_file.display(),
        outside_file.display()
    );
    let (mut backend, mut events) = Backend::spawn(&config, &options).await.unwrap();
    backend.send(json!({"type":"user","message":{"role":"user","content":format!(
        "Run this exact Bash command once to validate sandbox behavior. The final write is intentionally expected to fail. Do not retry, modify the command, disable the sandbox, or use other tools. Report the output. Command: {command}"
    )}})).await.unwrap();
    let mut output = String::new();
    let mut completed = false;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(90), events.recv())
        .await
        .unwrap()
    {
        match event {
            BackendEvent::Message(value) => {
                if value["type"] == "control_request" {
                    // Manual mode may ask about the compound command, including
                    // its deliberately out-of-root write. Approve only that
                    // exact command; the OS sandbox must still block the write.
                    let request = &value["request"];
                    assert_eq!(request["subtype"], "can_use_tool");
                    assert_eq!(request["tool_name"], "Bash");
                    assert_eq!(request["input"]["command"], command);
                    assert_ne!(request["input"]["dangerouslyDisableSandbox"], true);
                    backend
                        .send(json!({"type":"control_response","response":{
                            "subtype":"success","request_id":value["request_id"],
                            "response":{"behavior":"allow","updatedInput":request["input"]}
                        }}))
                        .await
                        .unwrap();
                }
                // Inspect tool results, never the model's narration or command text.
                if value["type"] == "user" {
                    for block in value["message"]["content"].as_array().into_iter().flatten() {
                        if block["type"] == "tool_result" {
                            output.push_str(&block["content"].to_string());
                        }
                    }
                }
                if value["type"] == "result" {
                    completed = true;
                    break;
                }
            }
            BackendEvent::Exited { success, message } => panic!("native exit {success}: {message}"),
        }
    }
    backend.terminate().await.unwrap();
    assert!(completed);
    assert!(output.contains("TLS_OK"), "gh failed: {output}");
    assert!(output.contains("BIND_OK"), "local bind failed: {output}");
    assert_eq!(std::fs::read_to_string(cache_file).unwrap(), "CACHE_OK");
    assert!(!outside_file.exists(), "outside write must remain blocked");
    assert!(
        output.to_lowercase().contains("operation not permitted"),
        "missing OS denial: {output}"
    );
}
