//! Translate desktop request options into the subset implemented by Claude.
use serde_json::{Value, json};

use crate::protocol::{RpcError, RpcResult};

pub fn normalize(method: &str, params: &Value) -> RpcResult<(Value, Vec<String>)> {
    if !matches!(
        method,
        "thread/start" | "thread/resume" | "thread/fork" | "thread/settings/update" | "turn/start"
    ) {
        return Ok((params.clone(), vec![]));
    }
    let mut p = params.clone();
    let object = p
        .as_object_mut()
        .ok_or_else(|| RpcError::invalid("params must be an object"))?;
    if method == "thread/fork" && object.get("experimentalPredictionMode") == Some(&json!(true)) {
        crate::protocol::supported_fields(
            params,
            &[
                "threadId",
                "experimentalPredictionMode",
                "experimentalDisablePredictionMcp",
                "ephemeral",
                "excludeTurns",
                "threadSource",
                "config",
            ],
        )?;
        if object.get("ephemeral") != Some(&json!(true)) {
            return Err(RpcError::invalid("Prediction forks require ephemeral=true"));
        }
        // A prediction fork keeps its parent's native tools so it reuses the
        // parent's prompt cache, and is discarded after its one turn. Codex's
        // Responses-specific MCP pruning and unload delay do not apply.
        object.remove("experimentalDisablePredictionMcp");
        if let Some(config) = object.remove("config").filter(|v| !v.is_null())
            && config
                .as_object()
                .is_none_or(|config| config.keys().any(|key| key != "thread_unload_delay_secs"))
        {
            return Err(RpcError::invalid(
                "Prediction forks keep their parent's configuration",
            ));
        }
    }
    let mut warnings = Vec::new();
    if let Some(fallback) = object
        .remove("allowProviderModelFallback")
        .filter(|value| !value.is_null())
    {
        if !fallback.is_boolean() {
            return Err(RpcError::invalid(
                "allowProviderModelFallback must be a boolean",
            ));
        }
        if fallback == true {
            warnings.push("Claude uses native model resolution; Codex model-provider fallback is not applied.".into());
        }
    }
    if method == "thread/start" {
        // Desktop enrolls new threads in Codex's Daybreak propagation program.
        // This is distinct from the saved choice and cannot select a Claude model.
        if let Some(enrolled) = object
            .remove("daybreakProgramPropagationEnrolled")
            .filter(|value| !value.is_null())
        {
            if !enrolled.is_boolean() {
                return Err(RpcError::invalid(
                    "daybreakProgramPropagationEnrolled must be a boolean",
                ));
            }
            if enrolled == true {
                warnings.push("Daybreak program propagation is not available for Claude; the selected native Claude model is used.".into());
            }
        }
        if object.get("ephemeral") == Some(&json!(true))
            && object
                .get("daybreakEnabled")
                .is_some_and(|value| !value.is_null())
        {
            return Err(RpcError::invalid(
                "daybreakEnabled is not supported for ephemeral threads",
            ));
        }
    }
    if let Some(profile) = object
        .remove("permissions")
        .filter(|value| !value.is_null())
    {
        let sandbox = match profile.as_str() {
            Some(":workspace") => "workspace-write",
            Some(":danger-full-access") => "danger-full-access",
            Some(":read-only") => "read-only",
            _ => {
                return Err(RpcError::invalid(
                    "Supported permission profiles are :workspace, :danger-full-access, and :read-only for tool-free ephemeral helpers",
                ));
            }
        };
        if object.get("sandbox").is_some_and(|value| !value.is_null())
            || object
                .get("sandboxPolicy")
                .is_some_and(|value| !value.is_null())
        {
            return Err(RpcError::invalid(
                "Select either permissions or a sandbox policy, not both",
            ));
        }
        if method == "turn/start" || method == "thread/settings/update" {
            object.insert(
                "sandboxPolicy".into(),
                match sandbox {
                    "workspace-write" => json!({"type":"workspaceWrite"}),
                    "read-only" => json!({"type":"readOnly","networkAccess":false}),
                    _ => json!({"type":"dangerFullAccess"}),
                },
            );
        } else {
            object.insert("sandbox".into(), json!(sandbox));
        }
    }
    if object.get("sandbox") == Some(&json!("read-only"))
        || object
            .get("sandboxPolicy")
            .is_some_and(|policy| policy["type"] == "readOnly")
    {
        if method == "thread/fork" {
            return Err(RpcError::invalid(
                "Read-only Claude helpers cannot fork native sessions",
            ));
        }
        // A running older router still injects its delegation catalog into
        // helper requests after the facade is upgraded. Remove only that exact
        // router-owned descriptor; caller-supplied tools remain an error.
        #[cfg(unix)]
        if let Some(tools) = object.get_mut("dynamicTools").and_then(Value::as_array_mut) {
            tools.retain(|tool| *tool != crate::delegation::dynamic_spec());
        }
        warnings.push("Read-only Claude helpers have no tools, hooks, skills or plugins. General read-only agent sessions are unsupported.".into());
    }
    if object
        .get("approvalsReviewer")
        .is_some_and(|reviewer| reviewer == "auto_review" || reviewer == "guardian_subagent")
    {
        warnings.push("With on-request approvals, automated review uses Claude Code's native auto permission mode. The never policy still denies permission prompts. A danger-full-access profile remains unrestricted.".into());
    }
    if object.get("sandbox") == Some(&json!("workspace-write"))
        || object
            .get("sandboxPolicy")
            .is_some_and(|p| p["type"] == "workspaceWrite")
    {
        warnings.push("Workspace commands use Claude's native OS sandbox. File tools use native scoped permissions; MCP servers and hooks remain trusted integrations. Existing deny/ask rules and other native settings are preserved; preapproved command/write/network grants and sandbox exclusions are replaced by this workspace's roots and approval policy.".into());
    }
    if let Some(raw) = object
        .remove("experimentalRawEvents")
        .filter(|v| !v.is_null())
        && raw != false
    {
        return Err(RpcError::invalid(
            "Raw Codex events are not available from Claude",
        ));
    }
    // Workspace roots describe desktop context. Explicit sandboxPolicy writable
    // roots select any additional write access.
    if let Some(roots) = object
        .remove("runtimeWorkspaceRoots")
        .filter(|v| !v.is_null())
        && !roots.as_array().is_some_and(|roots| {
            roots.iter().all(|root| {
                root.as_str()
                    .is_some_and(|root| std::path::Path::new(root).is_absolute())
            })
        })
    {
        return Err(RpcError::invalid(
            "runtimeWorkspaceRoots must contain absolute paths",
        ));
    }
    // Client analytics do not affect Claude execution.
    object.remove("turnTrigger");
    object.remove("responsesapiClientMetadata");
    object.remove("responsesapiClientMetadataPatch");
    // Deprecated by the pinned protocol: friendly/pragmatic no longer select a style.
    object.remove("personality");
    if object
        .get("summary")
        .and_then(Value::as_str)
        .is_some_and(|s| s == "concise" || s == "detailed")
    {
        warnings.push("Claude returns its native reasoning text; Codex reasoning-summary length preferences are not applied.".into());
    }
    if object
        .get("environments")
        .is_some_and(|v| v.as_array().is_some_and(Vec::is_empty))
    {
        object.remove("environments");
    }
    if let Some(config) = object.remove("config").filter(|v| !v.is_null()) {
        let config = config
            .as_object()
            .ok_or_else(|| RpcError::invalid("config must be an object"))?;
        if let Some(overrides) = crate::workspaces::shell_environment_overrides(config)? {
            object.insert("environmentOverrides".into(), json!(overrides));
        }
        let mut unsupported = Vec::new();
        let mut ignored = Vec::new();
        let mut native_features = false;
        for (key, value) in config {
            if value.is_null() {
                continue;
            }
            match key.as_str() {
                key if crate::workspaces::is_shell_environment_key(key) => {}
                "model_reasoning_effort" => {
                    object.entry("effort").or_insert_with(|| value.clone());
                }
                // Desktop unconditionally supplies Codex runtime feature flags.
                // They are not Claude settings. Warn instead of pretending to
                // enable Codex-only execution features on the native backend.
                key if key.starts_with("features.") => native_features = true,
                // The local desktop injects Codex memory and desktop-owned MCP
                // runtimes, including disabled placeholders. Claude keeps its
                // native memory/MCP configuration and its separate tool bridge.
                "memories.dual_write"
                | "memories.version"
                | "mcp_servers.node_repl"
                | "mcp_servers.artifact_session"
                | "mcp_servers.cua_repl" => {
                    native_features = true;
                }
                "apps.connector_openai_pages.tools"
                    if value.as_object().is_some_and(|tools| {
                        tools.values().all(|tool| tool["enabled"] == false)
                    }) => {}
                "plugins.codex-app-tools@openai-bundled.mcp_servers.codex_app.enabled_tools"
                | "mcp_servers.codex_app.enabled_tools"
                    if value.is_array() =>
                {
                    if value.as_array().is_some_and(|tools| !tools.is_empty()) {
                        warnings.push("Codex tool allowlists do not configure Claude MCP servers. Desktop tools supplied in dynamicTools are bridged separately; Claude's native tools and configured MCP servers remain available.".into());
                    }
                }
                // Config-layer overrides of model identity, sandboxing or
                // approvals would silently diverge from the request's typed
                // fields, so they still fail.
                key if is_execution_policy_key(key) => unsupported.push(key),
                // Desktop releases add Codex-only config keys without notice.
                // Rejecting them breaks resuming existing chats, so report
                // them instead.
                _ => {
                    eprintln!(
                        "Ignoring desktop {method} config {key}={}",
                        loggable_config_value(key, value)
                    );
                    ignored.push(key.as_str());
                }
            }
        }
        if !unsupported.is_empty() {
            return Err(RpcError::invalid(format!(
                "Unsupported Claude configuration: {}",
                unsupported.join(", ")
            )));
        }
        if !ignored.is_empty() {
            warnings.push(format!(
                "Codex configuration not applied to Claude: {}",
                ignored.join(", ")
            ));
        }
        if native_features {
            warnings.push("Claude uses its native runtime, memory and tools; Codex runtime feature, memory and desktop MCP overrides (node_repl, artifact_session, cua_repl) are not applied. Existing desktop tool forwarding is separate.".into());
        }
    }
    if let Some(mode) = object.remove("collaborationMode").filter(|v| !v.is_null()) {
        if mode["mode"] != "default" {
            return Err(RpcError::invalid(
                "Only the default collaboration mode is supported by Claude",
            ));
        }
        for (source, target) in [("model", "model"), ("reasoning_effort", "effort")] {
            if !mode["settings"][source].is_null() {
                object.insert(target.into(), mode["settings"][source].clone());
            }
        }
        if let Some(instructions) = mode["settings"]["developer_instructions"].as_str()
            && !instructions.is_empty()
        {
            object.insert("developerInstructions".into(), json!(instructions));
        }
    }
    // Deprecated and ignored by the pinned Codex protocol as well.
    object.remove("multiAgentMode");
    Ok((p, warnings))
}

fn is_execution_policy_key(key: &str) -> bool {
    [
        "model",
        "model_provider",
        "model_providers",
        "sandbox_mode",
        "sandbox_workspace_write",
        "approval_policy",
        "approvals_reviewer",
    ]
    .iter()
    .any(|policy| {
        key.strip_prefix(policy)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
    })
}

/// Enough of an ignored config value to support it later without copying
/// credentials into the server log: objects list their field names only.
fn loggable_config_value(key: &str, value: &Value) -> String {
    const SENSITIVE: [&str; 7] = [
        "auth", "env", "header", "key", "password", "secret", "token",
    ];
    const MAX_LEN: usize = 300;
    let lowered = key.to_ascii_lowercase();
    if SENSITIVE.iter().any(|word| lowered.contains(word)) {
        return "<redacted>".into();
    }
    let text = match value {
        Value::Object(fields) => format!(
            "{{{}}}",
            fields
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Array(items) if items.iter().any(|item| item.is_object()) => {
            format!("[{} entries]", items.len())
        }
        _ => value.to_string(),
    };
    match text.char_indices().nth(MAX_LEN) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text,
    }
}
