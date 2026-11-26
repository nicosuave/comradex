use std::{ffi::OsString, fs, path::Path};

use claude_codex_server::{native_profile::NativeProfile, sandbox::Policy};
use serde_json::{Value, json};

fn write(path: &Path, value: &Value) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
}

fn profile(root: &Path, session: &str) -> NativeProfile {
    fs::create_dir_all(root.join("workspace")).unwrap();
    NativeProfile::prepare_from(
        &root.join("state"),
        &root.join("workspace"),
        session,
        &root.join("original"),
        &root.join("original/.claude.json"),
        OsString::from("original-auth-namespace"),
        &root.join("original/plugins"),
    )
    .unwrap()
}

#[test]
fn refresh_preserves_native_features_and_session_state_without_copying_auth_or_grants() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let original = root.join("original");
    let cwd = root.join("workspace");
    let session = uuid::Uuid::new_v4().to_string();
    let settings = json!({
        "enabledPlugins":{"example@personal":true},"env":{"NATIVE_CUSTOM":"present"},
        "permissions":{"additionalDirectories":["/"],"allow":["Edit", "Bash"],"deny":["Read(/private/**)"]},
        "sandbox":{"filesystem":{"allowWrite":["/"],"denyRead":["private"]},"allowUnsandboxedCommands":false}
    });
    write(&original.join("settings.json"), &settings);
    write(
        &original.join(".claude.json"),
        &json!({
            "oauthAccount":{"fake":"never copy"},"primaryApiKey":"fake-test-key",
            "mcpServers":{"test":{"type":"stdio","command":"test-server"}},
            "projects":{cwd.to_str().unwrap():{
                "hasTrustDialogAccepted":true,"mcpServers":{"local":{"command":"local-server"}},
                "enabledMcpjsonServers":["local"],"allowedTools":["Bash"]
            }}
        }),
    );
    fs::create_dir_all(original.join("skills/example")).unwrap();
    fs::write(original.join("skills/example/SKILL.md"), "native body").unwrap();
    let first = profile(&root, &session);
    assert_eq!(
        fs::read_to_string(first.config_dir.join("skills/example/SKILL.md")).unwrap(),
        "native body"
    );
    assert_eq!(
        first.user_settings["enabledPlugins"],
        settings["enabledPlugins"]
    );
    assert!(first.user_settings["permissions"]["additionalDirectories"].is_null());
    assert_eq!(first.user_settings["permissions"]["allow"], json!([]));
    assert!(first.user_settings["sandbox"]["filesystem"]["allowWrite"].is_null());
    assert_eq!(
        first.user_settings["sandbox"]["allowUnsandboxedCommands"],
        false
    );
    assert_eq!(
        first
            .environment
            .get(&OsString::from("CLAUDE_SECURESTORAGE_CONFIG_DIR")),
        Some(&OsString::from("original-auth-namespace"))
    );
    let state_path = first.config_dir.join(".claude.json");
    let mut state: Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    assert!(state.get("oauthAccount").is_none());
    assert!(state.get("primaryApiKey").is_none());
    assert_eq!(state["mcpServers"]["test"]["command"], "test-server");
    assert_eq!(
        state["projects"][cwd.to_str().unwrap()]["hasTrustDialogAccepted"],
        true
    );
    assert!(state["projects"][cwd.to_str().unwrap()]["allowedTools"].is_null());
    state["nativeSessionMarker"] = json!("preserved");
    write(&state_path, &state);
    write(&original.join(".claude.json"), &json!({"projects":{}}));
    fs::write(
        first.config_dir.join("projects/existing-session"),
        "transcript",
    )
    .unwrap();
    let second = profile(&root, &session);
    assert_eq!(first.config_dir, second.config_dir);
    let state: Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    assert_eq!(state["nativeSessionMarker"], "preserved");
    assert!(state["projects"][cwd.to_str().unwrap()]["hasTrustDialogAccepted"].is_null());
    assert!(state["mcpServers"].is_null());
    assert_eq!(
        fs::read_to_string(original.join("projects/existing-session")).unwrap(),
        "transcript"
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(original.join("settings.json")).unwrap())
            .unwrap(),
        settings
    );
    let other = profile(&root, &uuid::Uuid::new_v4().to_string());
    assert_ne!(first.config_dir, other.config_dir);
    assert_eq!(
        fs::read_to_string(other.config_dir.join("projects/existing-session")).unwrap(),
        "transcript"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&state_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(&first.config_dir)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }
}

fn runtime(policy: &Value) -> (Value, Value, Value) {
    let settings = json!({"effective":policy});
    let status = json!({
        "supported":true,"enabled":true,"enabled_in_settings":true,"locked":true,"overrides_locked":true,
        "no_sandbox_allowed":policy["sandbox"]["allowUnsandboxedCommands"],"excluded_commands":[],
        "restrictions":{
            "fs_allow_write":policy["sandbox"]["filesystem"]["allowWrite"],
            "fs_deny_write":policy["sandbox"]["filesystem"]["denyWrite"],
            "network_managed":true,"network_allowed_domains":[],"unix_sockets":[],"allow_all_unix_sockets":false
        }
    });
    let mut rules = Vec::new();
    for behavior in ["allow", "ask", "deny"] {
        for rule in policy["permissions"][behavior].as_array().unwrap() {
            rules.push(json!({"behavior":behavior,"source":"policySettings","rule":rule}));
        }
    }
    (
        settings,
        status,
        json!({"state":{"managedOnly":true,"rules":rules}}),
    )
}

#[test]
fn runtime_checks_active_rules_and_os_roots_instead_of_inactive_merged_grants() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let profile = profile(&root, &uuid::Uuid::new_v4().to_string());
    let cwd = root.join("workspace");
    let policy = Policy::workspace();
    let managed = policy
        .managed_settings(&json!({}), &cwd, "dontAsk", &profile)
        .unwrap();
    let (mut settings, status, mut rules) = runtime(&managed);
    settings["effective"]["permissions"]["allow"] = json!(["Edit", "Bash"]);
    settings["effective"]["permissions"]["additionalDirectories"] = json!([root]);
    rules["state"]["rules"].as_array_mut().unwrap().push(
        json!({"behavior":"allow","source":"projectSettings","rule":"Bash","notInEffect":true}),
    );
    policy
        .verify_native_with_rules(&settings, &status, &rules, &managed, &cwd)
        .unwrap();
    rules["state"]["rules"]
        .as_array_mut()
        .unwrap()
        .last_mut()
        .unwrap()["notInEffect"] = json!(false);
    assert!(
        policy
            .verify_native_with_rules(&settings, &status, &rules, &managed, &cwd)
            .is_err()
    );
    rules["state"]["rules"].as_array_mut().unwrap().pop();
    let mut outside = status.clone();
    outside["restrictions"]["fs_allow_write"] = json!([root]);
    assert!(
        policy
            .verify_native_with_rules(&settings, &outside, &rules, &managed, &cwd)
            .is_err()
    );
    let mut unprotected = status.clone();
    unprotected["restrictions"]["fs_deny_write"] = json!([]);
    assert!(
        policy
            .verify_native_with_rules(&settings, &unprotected, &rules, &managed, &cwd)
            .is_err()
    );
    let mut unmanaged = rules.clone();
    unmanaged["state"]["managedOnly"] = json!(false);
    assert!(
        policy
            .verify_native_with_rules(&settings, &status, &unmanaged, &managed, &cwd)
            .is_err()
    );
    rules["state"]["rules"].as_array_mut().unwrap().remove(0);
    assert!(
        policy
            .verify_native_with_rules(&settings, &status, &rules, &managed, &cwd)
            .is_err()
    );
}

#[test]
fn managed_policy_keeps_reviewer_choice_and_preserves_restrictive_rules() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let profile = profile(&root, &uuid::Uuid::new_v4().to_string());
    let settings = json!({"permissions":{"allow":["Bash"],"ask":["Read(secret)"],"deny":["Edit(secret)"]},"sandbox":{"allowUnsandboxedCommands":false}});
    for mode in ["auto", "acceptEdits", "dontAsk"] {
        let managed = Policy::workspace()
            .managed_settings(&settings, &root.join("workspace"), mode, &profile)
            .unwrap();
        assert_eq!(managed["sandbox"]["allowUnsandboxedCommands"], false);
        assert_eq!(managed["allowManagedPermissionRulesOnly"], true);
        assert_eq!(
            managed["sandbox"]["network"]["allowManagedDomainsOnly"],
            true
        );
        assert!(
            !managed["permissions"]["allow"]
                .as_array()
                .unwrap()
                .contains(&json!("Bash"))
        );
        assert!(
            managed["permissions"]["ask"]
                .as_array()
                .unwrap()
                .contains(&json!("Read(secret)"))
        );
        assert!(
            managed["permissions"]["deny"]
                .as_array()
                .unwrap()
                .contains(&json!("Edit(secret)"))
        );
        assert_eq!(
            managed["permissions"]["ask"]
                .as_array()
                .unwrap()
                .contains(&json!("Bash(dangerouslyDisableSandbox:true)")),
            mode != "auto"
        );
    }
}

#[test]
fn configuration_is_protected_except_the_transcript_and_memory_namespace() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let original = root.join("original");
    fs::create_dir_all(original.join("output-styles")).unwrap();
    fs::create_dir_all(original.join("todos")).unwrap();
    fs::write(original.join("output-styles/terse.md"), "style").unwrap();
    let profile = profile(&root, &uuid::Uuid::new_v4().to_string());
    assert_eq!(
        fs::read_to_string(profile.config_dir.join("output-styles/terse.md")).unwrap(),
        "style"
    );
    assert_eq!(profile.transcripts, original.join("projects"));
    for path in [
        original.join("settings.json"),
        original.join("todos"),
        original.join("skills"),
        original.join("output-styles"),
        original.join("workflows"),
    ] {
        assert!(
            profile.protected_paths.contains(&path),
            "{}",
            path.display()
        );
    }
    assert!(!profile.protected_paths.contains(&original));
    assert!(!profile.protected_paths.contains(&original.join("projects")));
}

#[cfg(unix)]
#[test]
fn stale_idle_profiles_are_removed() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let stale = profile(&root, &uuid::Uuid::new_v4().to_string());
    let recent = profile(&root, &uuid::Uuid::new_v4().to_string());
    let old = std::time::SystemTime::now() - std::time::Duration::from_secs(31 * 24 * 60 * 60);
    fs::File::options()
        .write(true)
        .open(stale.config_dir.join("source.json"))
        .unwrap()
        .set_modified(old)
        .unwrap();
    profile(&root, &uuid::Uuid::new_v4().to_string());
    assert!(!stale.config_dir.exists());
    assert!(recent.config_dir.exists());
    // Shared native content behind the removed links is untouched.
    assert!(root.join("original/projects").exists());
}

#[cfg(unix)]
#[test]
fn unexpected_profile_links_fail_closed_without_overwriting_content() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let session = uuid::Uuid::new_v4().to_string();
    let first = profile(&root, &session);
    let link = first.config_dir.join("skills");
    fs::remove_file(&link).unwrap();
    fs::write(&link, "unexpected native content").unwrap();
    let result = NativeProfile::prepare_from(
        &root.join("state"),
        &root.join("workspace"),
        &session,
        &root.join("original"),
        &root.join("original/.claude.json"),
        OsString::new(),
        &root.join("original/plugins"),
    );
    assert!(result.is_err());
    assert_eq!(
        fs::read_to_string(link).unwrap(),
        "unexpected native content"
    );
}

#[test]
#[ignore = "requires installed authenticated Claude; reads auth status without model calls"]
fn native_profile_preserves_configured_authentication() {
    let dir = tempfile::tempdir().unwrap();
    let profile = NativeProfile::prepare(
        &dir.path().join("state"),
        dir.path(),
        &uuid::Uuid::new_v4().to_string(),
    )
    .unwrap();
    let executable = std::env::var_os("CLAUDE_BINARY").unwrap_or_else(|| "claude".into());
    let status = |isolated: bool| {
        let mut command = std::process::Command::new(&executable);
        command.args(["auth", "status", "--json"]);
        if isolated {
            command.envs(&profile.environment);
        }
        let output = command.output().unwrap();
        assert!(output.status.success(), "native auth status failed");
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };
    let original = status(false);
    assert_eq!(
        original["loggedIn"], true,
        "normal Claude profile must be authenticated for this test"
    );
    let isolated = status(true);
    for field in ["loggedIn", "authMethod", "apiProvider"] {
        assert_eq!(
            isolated[field], original[field],
            "native auth continuity: {field}"
        );
    }
}
