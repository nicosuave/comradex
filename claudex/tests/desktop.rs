use claude_codex_server::desktop::normalize;
use serde_json::json;

#[test]
fn desktop_daybreak_enrollment_does_not_change_claude_execution() {
    for enabled in [json!(true), json!(false), json!(null)] {
        for enrolled in [json!(true), json!(false), json!(null)] {
            let (params, warnings) = normalize(
                "thread/start",
                &json!({
                    "model":"opus", "daybreakEnabled":enabled,
                    "daybreakProgramPropagationEnrolled":enrolled
                }),
            )
            .unwrap();
            assert_eq!(params["model"], "opus");
            assert_eq!(params["daybreakEnabled"], enabled);
            assert!(params.get("daybreakProgramPropagationEnrolled").is_none());
            assert_eq!(!warnings.is_empty(), enrolled == true);
        }
    }
    assert!(
        normalize(
            "thread/start",
            &json!({
                "daybreakProgramPropagationEnrolled":"true"
            })
        )
        .is_err()
    );
    assert!(
        normalize(
            "thread/start",
            &json!({
                "ephemeral":true, "daybreakEnabled":false
            })
        )
        .is_err()
    );
}

#[test]
fn workspace_overlay_and_disabled_plugin_selection_survive_normalization() {
    for method in ["thread/start", "thread/resume", "thread/fork", "turn/start"] {
        let (params, _) = normalize(
            method,
            &json!({
                "disabledPluginIds":["example@market"],
                "config":{
                    "shell_environment_policy.inherit":"all",
                    "shell_environment_policy.set":{"WORKTREE_LABEL":"task"},
                    "shell_environment_policy.exclude":[],
                    "features.native_feature":true
                }
            }),
        )
        .unwrap();
        assert_eq!(
            params["environmentOverrides"],
            json!({"WORKTREE_LABEL":"task"})
        );
        assert_eq!(params["disabledPluginIds"], json!(["example@market"]));
        assert!(params.get("config").is_none());
        let (params, _) = normalize(
            method,
            &json!({"disabledPluginIds":[], "config":{"shell_environment_policy.set":{}}}),
        )
        .unwrap();
        assert_eq!(params["environmentOverrides"], json!({}));
        assert_eq!(params["disabledPluginIds"], json!([]));
        let (params, _) = normalize(method, &json!({})).unwrap();
        assert!(params.get("environmentOverrides").is_none());
        assert!(params.get("disabledPluginIds").is_none());
    }
}

#[test]
fn incompatible_shell_restrictions_are_not_silently_discarded() {
    for config in [
        json!({"shell_environment_policy.inherit":"none"}),
        json!({"shell_environment_policy.exclude":["SECRET"]}),
        json!({"shell_environment_policy.experimental_use_profile":true}),
    ] {
        let error = normalize("thread/start", &json!({"config":config})).unwrap_err();
        assert!(error.message.contains("shell_environment_policy"));
    }
}

#[test]
fn codex_desktop_runtime_overrides_do_not_block_claude_requests() {
    for method in [
        "thread/start",
        "thread/resume",
        "thread/fork",
        "thread/settings/update",
        "turn/start",
    ] {
        let (params, warnings) = normalize(
            method,
            &json!({
                "approvalPolicy":"on-request", "sandbox":"workspace-write",
                "config":{
                    "mcp_servers.node_repl":{"command":"node", "args":["desktop-repl.js"]},
                    "mcp_servers.artifact_session":{"command":"/desktop/node_repl", "env":{"NODE_REPL_TRUSTED_SERVICES":"{\"artifact_session\":\"@oai/artifact-tool/artifact-session/service\"}"}},
                    "mcp_servers.cua_repl":{"command":"/desktop/node", "args":["/desktop/@oai/cua-repl/bin/cua-repl.mjs"], "enabled":true},
                    "memories.dual_write":true, "memories.version":"v2",
                    "model_reasoning_effort":"high"
                }
            }),
        )
        .unwrap();
        assert!(params.get("config").is_none());
        assert_eq!(params["effort"], "high");
        assert_eq!(params["approvalPolicy"], "on-request");
        assert_eq!(params["sandbox"], "workspace-write");
        assert!(warnings.iter().any(|w| w.contains(
            "desktop MCP overrides (node_repl, artifact_session, cua_repl) are not applied"
        )));
        let (disabled, _) = normalize(
            method,
            &json!({"config":{
                "mcp_servers.artifact_session":{"command":"", "enabled":false},
                "mcp_servers.cua_repl":{"command":"/desktop/electron", "enabled":false}
            }}),
        )
        .unwrap();
        assert!(disabled.get("config").is_none());
    }
    // Unrecognized settings are reported by name rather than silently dropped.
    for key in ["mcp_servers.other", "memories.unknown"] {
        let (_, warnings) =
            normalize("thread/start", &json!({"config":{key:{"enabled":true}}})).unwrap();
        assert!(warnings.iter().any(|w| w.contains(key)), "{warnings:?}");
    }
}

#[test]
fn new_desktop_config_keys_do_not_block_resuming_chats() {
    for method in ["thread/start", "thread/resume", "thread/fork", "turn/start"] {
        let (params, warnings) = normalize(
            method,
            &json!({"config":{
                "apps.connector_openai_search_service.omit_tools_from":["codex"],
                "web_search":"live",
                "model_reasoning_effort":"high"
            }}),
        )
        .unwrap();
        assert!(params.get("config").is_none());
        assert_eq!(params["effort"], "high");
        let warning = warnings
            .iter()
            .find(|w| w.starts_with("Codex configuration not applied"))
            .unwrap();
        assert!(warning.contains("apps.connector_openai_search_service.omit_tools_from"));
        assert!(warning.contains("web_search"));
    }
}

#[test]
fn config_overrides_of_model_sandbox_or_approvals_still_fail() {
    for key in [
        "model",
        "model_provider",
        "model_providers.openai.base_url",
        "sandbox_mode",
        "sandbox_workspace_write.network_access",
        "approval_policy",
        "approvals_reviewer",
    ] {
        let error = normalize("thread/resume", &json!({"config":{key:"x"}})).unwrap_err();
        assert!(error.message.contains(key), "{}", error.message);
    }
    // Prefix matching stays on key-segment boundaries.
    normalize(
        "thread/resume",
        &json!({"config":{"model_verbosity":"low"}}),
    )
    .unwrap();
}
