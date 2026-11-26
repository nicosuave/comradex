use claude_codex_server::{
    desktop::normalize,
    store::{Record, Settings},
};
use serde_json::json;

#[test]
fn explicit_auto_reviewer_round_trips_without_claiming_a_sandbox() {
    for method in ["thread/start", "thread/resume", "thread/fork", "turn/start"] {
        for reviewer in ["auto_review", "guardian_subagent"] {
            let (params, warnings) = normalize(
                method,
                &json!({
                    "approvalsReviewer": reviewer, "sandbox": "danger-full-access"
                }),
            )
            .unwrap();
            let mut settings = Settings::new(std::env::temp_dir(), "opus".into());
            settings.apply(&params).unwrap();
            assert_eq!(settings.native_permission_mode(), "auto");
            assert!(
                warnings
                    .iter()
                    .any(|warning| warning.contains("unrestricted"))
            );
            let stored = serde_json::to_value(&settings).unwrap();
            let restored: Settings = serde_json::from_value(stored).unwrap();
            assert_eq!(restored.native_permission_mode(), "auto");
            let record = Record::new(restored, std::env::temp_dir(), &json!({}), "test");
            let response = record.response(false, false);
            assert_eq!(response["approvalsReviewer"], "auto_review");
            assert_eq!(response["sandbox"]["type"], "dangerFullAccess");
            assert!(response["activePermissionProfile"].is_null());
        }
    }
}

#[test]
fn auto_is_opt_in_and_never_bypasses_only_with_full_access() {
    let mut settings = Settings::new(std::env::temp_dir(), "opus".into());
    assert_eq!(settings.native_permission_mode(), "manual");
    let mut legacy = serde_json::to_value(&settings).unwrap();
    legacy.as_object_mut().unwrap().remove("approvals_reviewer");
    let legacy: Settings = serde_json::from_value(legacy).unwrap();
    assert_eq!(legacy.native_permission_mode(), "manual");
    settings
        .apply(&json!({"approvalsReviewer":"auto_review"}))
        .unwrap();
    // The desktop's Full access preset.
    settings
        .apply(&json!({"approvalPolicy":"never", "sandbox":"danger-full-access"}))
        .unwrap();
    assert_eq!(settings.native_permission_mode(), "bypassPermissions");
    // Inside the workspace sandbox, never denies whatever would prompt.
    settings
        .apply(&json!({"sandbox":"workspace-write"}))
        .unwrap();
    assert_eq!(settings.native_permission_mode(), "dontAsk");
    settings
        .apply(
            &json!({"approvalPolicy":"on-request", "approvalsReviewer":"user",
            "sandbox":"danger-full-access"}),
        )
        .unwrap();
    assert_eq!(settings.native_permission_mode(), "manual");
}

#[test]
fn unsupported_reviewers_and_sandbox_claims_fail_closed() {
    for params in [
        json!({"approvalsReviewer":"bypassPermissions"}),
        json!({"approvalsReviewer":true}),
        json!({"approvalsReviewer":"auto_review", "sandboxPolicy":{"type":"readOnly","networkAccess":true}}),
    ] {
        let mut settings = Settings::new(std::env::temp_dir(), "opus".into());
        assert!(settings.apply(&params).is_err());
    }
    for params in [
        json!({"approvalsReviewer":"auto_review", "sandbox":"read-only"}),
        json!({"approvalsReviewer":"auto_review", "sandboxPolicy":{"type":"readOnly"}}),
    ] {
        let mut settings = Settings::new(std::env::temp_dir(), "opus".into());
        settings.apply(&params).unwrap();
        let record = Record::new(settings.clone(), std::env::temp_dir(), &json!({}), "test");
        assert!(record.validate_read_only_helper().is_err());
        settings.apply(&json!({"approvalPolicy":"never"})).unwrap();
        assert_eq!(settings.native_permission_mode(), "dontAsk");
        let helper = Record::new(
            settings,
            std::env::temp_dir(),
            &json!({"ephemeral":true}),
            "test",
        );
        helper.validate_read_only_helper().unwrap();
    }
}

#[test]
fn workspace_profile_uses_scoped_native_modes_and_reports_actual_policy() {
    for reviewer in ["user", "auto_review"] {
        let (params, warnings) = normalize(
            "thread/start",
            &json!({"permissions":":workspace", "approvalsReviewer":reviewer}),
        )
        .unwrap();
        let mut settings = Settings::new(std::env::temp_dir(), "opus".into());
        settings.apply(&params).unwrap();
        assert_eq!(
            settings.native_permission_mode(),
            if reviewer == "user" {
                "acceptEdits"
            } else {
                "auto"
            }
        );
        assert!(settings.sandbox.is_workspace());
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("OS sandbox"))
        );
        let record = Record::new(settings, std::env::temp_dir(), &json!({}), "test");
        assert_eq!(
            record.response(false, false)["sandbox"]["type"],
            "workspaceWrite"
        );
    }
}

#[test]
fn native_settings_keep_restrictions_and_unrelated_settings_without_widening_roots() {
    use claude_codex_server::sandbox::{Policy, load_settings_from};
    let fixture = tempfile::tempdir().unwrap();
    let user = fixture.path().join("user");
    let project = fixture.path().join("project");
    std::fs::create_dir_all(&user).unwrap();
    std::fs::create_dir_all(project.join(".claude")).unwrap();
    std::fs::write(user.join("settings.json"), json!({
        "permissions":{"allow":["Bash(*)","Edit(//**)"],"deny":["Read(/private/**)"],"ask":["WebFetch"]},
        "sandbox":{"allowUnsandboxedCommands":false,"filesystem":{"allowWrite":["/"],"denyWrite":["./protected"]}},
        "env":{"KEEP_EXISTING":"yes"},"enabledPlugins":{"native@example":true}
    }).to_string()).unwrap();
    std::fs::write(
        project.join(".claude/settings.json"),
        json!({
            "permissions":{"ask":["Edit(/specific/**)"],"allow":["WebFetch(domain:example.com)"]},
            "sandbox":{"allowUnsandboxedCommands":true,"excludedCommands":["*"]},
            "hooks":{"PreToolUse":[]}, "language":"English"
        })
        .to_string(),
    )
    .unwrap();
    let mut settings =
        load_settings_from(&project, &user, &project, &["user", "project", "local"]).unwrap();
    Policy::workspace()
        .apply_native(&mut settings, &project, "acceptEdits")
        .unwrap();
    assert_eq!(
        settings["permissions"]["deny"][0],
        format!("Read(/{}/private/**)", user.display())
    );
    assert!(
        settings["permissions"]["ask"]
            .as_array()
            .unwrap()
            .contains(&json!(format!("Edit(/{}/specific/**)", project.display())))
    );
    assert!(
        settings["permissions"]["allow"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        settings["sandbox"]["filesystem"]["allowWrite"],
        json!([project.canonicalize().unwrap()])
    );
    assert_eq!(
        settings["sandbox"]["filesystem"]["denyWrite"][0],
        json!(user.join("./protected"))
    );
    assert_eq!(settings["sandbox"]["allowUnsandboxedCommands"], false);
    assert_eq!(settings["sandbox"]["excludedCommands"], json!([]));
    assert_eq!(settings["sandbox"]["enabled"], true);
    assert_eq!(settings["sandbox"]["failIfUnavailable"], true);
    assert_eq!(settings["sandbox"]["network"]["allowedDomains"], json!([]));
    assert_eq!(settings["env"]["KEEP_EXISTING"], "yes");
    assert_eq!(settings["enabledPlugins"]["native@example"], true);
    assert_eq!(settings["language"], "English");
    assert!(settings["hooks"]["PreToolUse"].is_array());
}

#[test]
fn resolved_native_sandbox_is_checked_before_any_user_work() {
    use claude_codex_server::sandbox::Policy;
    let fixture = tempfile::tempdir().unwrap();
    let policy = Policy::workspace();
    let mut effective = json!({});
    policy
        .apply_native(&mut effective, fixture.path(), "acceptEdits")
        .unwrap();
    let settings = json!({"effective":effective});
    let status = json!({"supported":true,"enabled":true,"enabled_in_settings":true,
        "excluded_commands":[],"restrictions":{"fs_allow_write":[fixture.path().canonicalize().unwrap()],
        "unix_sockets":[],"allow_all_unix_sockets":false,"network_allowed_domains":[]}});
    policy
        .verify_native(&settings, &status, fixture.path())
        .unwrap();
    let mut bad = status.clone();
    bad["enabled"] = json!(false);
    assert!(
        policy
            .verify_native(&settings, &bad, fixture.path())
            .is_err()
    );
    let mut bad = status.clone();
    bad["restrictions"]["fs_allow_write"] = json!(["/"]);
    assert!(
        policy
            .verify_native(&settings, &bad, fixture.path())
            .is_err()
    );
    let mut bad = status.clone();
    bad["restrictions"]["network_allowed_domains"] = json!(["*"]);
    assert!(
        policy
            .verify_native(&settings, &bad, fixture.path())
            .is_err()
    );
    let mut bad = settings.clone();
    bad["effective"]["sandbox"]["filesystem"]["disabled"] = json!(true);
    assert!(policy.verify_native(&bad, &status, fixture.path()).is_err());
}

#[test]
fn auto_review_does_not_inject_manual_escape_approvals() {
    use claude_codex_server::sandbox::Policy;
    let fixture = tempfile::tempdir().unwrap();
    let escape_rule = json!("Bash(dangerouslyDisableSandbox:true)");
    for mode in ["auto", "acceptEdits", "dontAsk"] {
        let mut settings = json!({});
        Policy::workspace()
            .apply_native(&mut settings, fixture.path(), mode)
            .unwrap();
        assert_eq!(
            settings["permissions"]["ask"]
                .as_array()
                .unwrap()
                .contains(&escape_rule),
            mode != "auto"
        );
    }
    // Explicit operator asks/denies still apply in auto mode.
    let mut settings = json!({"permissions":{"ask":[escape_rule],"deny":["Bash(rm:*)"]}});
    Policy::workspace()
        .apply_native(&mut settings, fixture.path(), "auto")
        .unwrap();
    assert!(
        settings["permissions"]["ask"]
            .as_array()
            .unwrap()
            .contains(&escape_rule)
    );
    assert_eq!(settings["permissions"]["deny"], json!(["Bash(rm:*)"]));
}

#[test]
fn workspace_defaults_validate_paths_and_reject_unknown_settings() {
    use claude_codex_server::sandbox::WorkspaceDefaults;
    let state = tempfile::tempdir().unwrap();
    assert!(
        !WorkspaceDefaults::load(state.path())
            .unwrap()
            .network_access
    );
    let path = state.path().join("workspace-defaults.json");
    for invalid in [
        json!({"writable_roots":["relative"]}),
        json!({"writable_roots":[state.path().join("missing")]}),
        json!({"network_access": "true"}),
        json!({"network_acess": true}),
    ] {
        std::fs::write(&path, invalid.to_string()).unwrap();
        assert!(WorkspaceDefaults::load(state.path()).is_err());
    }
    std::fs::write(
        &path,
        json!({"writable_roots":[state.path()],"network_access":true}).to_string(),
    )
    .unwrap();
    let defaults = WorkspaceDefaults::load(state.path()).unwrap();
    assert!(defaults.network_access);
    assert_eq!(defaults.writable_roots, vec![state.path()]);
}

#[test]
fn local_servers_require_network_access_and_an_explicit_native_setting() {
    use claude_codex_server::sandbox::{Policy, WorkspaceDefaults};
    let fixture = tempfile::tempdir().unwrap();
    for network_access in [false, true] {
        for allow_binding in [false, true] {
            let policy = WorkspaceDefaults {
                network_access,
                ..Default::default()
            }
            .policy();
            let mut native = json!({"sandbox":{
                "enableWeakerNetworkIsolation":true,
                "network":{"allowLocalBinding":allow_binding}
            }});
            policy
                .apply_native(&mut native, fixture.path(), "acceptEdits")
                .unwrap();
            assert_eq!(
                native["sandbox"]["network"]["allowLocalBinding"],
                network_access && allow_binding
            );
            assert_eq!(native["sandbox"]["enableWeakerNetworkIsolation"], true);
            let status = json!({"supported":true,"enabled":true,"enabled_in_settings":true,
                "excluded_commands":[],"restrictions":{"fs_allow_write":[fixture.path().canonicalize().unwrap()],
                "unix_sockets":[],"allow_all_unix_sockets":false,
                "network_allowed_domains": if network_access { json!(["*"]) } else { json!([]) }}});
            let mut settings = json!({"effective":native});
            policy
                .verify_native(&settings, &status, fixture.path())
                .unwrap();
            settings["effective"]["sandbox"]["network"]["allowLocalBinding"] = json!(true);
            if !network_access {
                assert!(
                    policy
                        .verify_native(&settings, &status, fixture.path())
                        .is_err()
                );
            }
        }
    }
    let mut native = json!({});
    let policy: Policy = WorkspaceDefaults {
        network_access: true,
        ..Default::default()
    }
    .policy();
    policy
        .apply_native(&mut native, fixture.path(), "acceptEdits")
        .unwrap();
    assert_ne!(native["sandbox"]["network"]["allowLocalBinding"], true);
}

/// Runs the installed native runtime and consumes model tokens. Kept opt-in so
/// ordinary unit tests do not depend on authentication or host Seatbelt access.
#[tokio::test]
#[ignore = "requires installed authenticated Claude and host sandbox access"]
async fn native_workspace_boundary_and_file_approval() {
    use claude_codex_server::{
        backend::{Backend, BackendConfig, BackendEvent, SessionOptions},
        sandbox::Policy,
    };
    use std::time::Duration;
    let parent = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let workspace = parent.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let outside_shell = parent.path().join("outside-shell.txt");
    let outside_write = parent.path().join("outside-write.txt");
    let config = BackendConfig {
        executable: std::env::var_os("CLAUDE_BINARY")
            .map(Into::into)
            .unwrap_or_else(|| "claude".into()),
        extra_args: vec![
            "--setting-sources".into(),
            "".into(),
            "--strict-mcp-config".into(),
            "--tools".into(),
            "Bash,Write,Read".into(),
        ],
        initialize_timeout: Duration::from_secs(30),
        protocol: Default::default(),
    };
    let options = SessionOptions {
        state_dir: parent.path().join("state"),
        cwd: workspace.clone(),
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
        native_settings: json!({"sandbox":{"allowUnsandboxedCommands":false}}),
        sandbox: Policy::workspace(),
    };
    let (mut backend, mut events) = Backend::spawn(&config, &options).await.unwrap();
    let prompt = format!(
        "Perform these three disposable file operations exactly once each. First use Write to create inside-write.txt in the current directory with content INSIDE_WRITE. Second use Bash to run `printf INSIDE_SHELL > inside-shell.txt; printf OUTSIDE_SHELL > {}`. Third use Write to create {} with content OUTSIDE_WRITE. If a tool is denied, report it and continue the other operations. Do not retry or ask for other permissions.",
        outside_shell.display(),
        outside_write.display()
    );
    backend
        .send(json!({"type":"user","message":{"role":"user","content":prompt}}))
        .await
        .unwrap();
    let mut file_denial = false;
    let mut shell_denial = false;
    let mut completed = false;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(90), events.recv())
        .await
        .unwrap()
    {
        match event {
            BackendEvent::Message(value) => {
                if value["type"] == "control_request" {
                    let request = &value["request"];
                    if request["subtype"] == "can_use_tool" && request["tool_name"] == "Write" {
                        panic!(
                            "outside direct writes must be denied by the SDK guard before approval"
                        );
                    }
                    backend.send(json!({"type":"control_response","response":{"subtype":"success","request_id":value["request_id"],"response":{"behavior":"deny","message":"Denied by the test host"}}})).await.unwrap();
                }
                if value["type"] == "user" {
                    shell_denial |= value.to_string().contains("operation not permitted");
                    file_denial |= value.to_string().contains("Host workspace boundary");
                }
                if value["type"] == "result" {
                    completed = true;
                    break;
                }
            }
            BackendEvent::Exited { success, message } => {
                panic!("native runtime exited {success}: {message}")
            }
        }
    }
    backend.terminate().await.unwrap();
    assert!(completed);
    assert!(
        file_denial,
        "outside native file write must be denied by the SDK guard"
    );
    assert!(shell_denial, "outside shell write must be denied by the OS");
    assert!(!outside_shell.exists());
    assert!(!outside_write.exists());
    assert_eq!(
        std::fs::read_to_string(workspace.join("inside-write.txt"))
            .unwrap()
            .trim(),
        "INSIDE_WRITE"
    );
    assert_eq!(
        std::fs::read_to_string(workspace.join("inside-shell.txt")).unwrap(),
        "INSIDE_SHELL"
    );
}
