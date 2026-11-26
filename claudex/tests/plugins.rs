use claude_codex_server::plugins::{PluginService, disabled_settings, enabled_key_id};
use serde_json::{Value, json};
use std::{fs, os::unix::fs::PermissionsExt};

struct Fixture {
    dir: tempfile::TempDir,
    service: PluginService,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("plugin");
        fs::create_dir_all(root.join(".claude-plugin")).unwrap();
        fs::create_dir_all(root.join("skills/example")).unwrap();
        fs::create_dir_all(root.join("hooks")).unwrap();
        fs::write(
            root.join(".claude-plugin/plugin.json"),
            r#"{"name":"sample","description":"Installed description"}"#,
        )
        .unwrap();
        fs::write(
            root.join("skills/example/SKILL.md"),
            "---\nname: example\ndescription: Example native skill\n---\nInstructions\n",
        )
        .unwrap();
        fs::write(
            root.join("hooks/hooks.json"),
            r#"{"hooks":{"PreToolUse":[{"hooks":[]}]}}"#,
        )
        .unwrap();
        fs::create_dir_all(dir.path().join("plugins")).unwrap();
        fs::write(
            dir.path().join("plugins/known_marketplaces.json"),
            json!({"test-market":{"installLocation":dir.path().join("market")}}).to_string(),
        )
        .unwrap();
        fs::write(dir.path().join("inventory.json"), json!({"installed":[
            {"id":"sample@test-market","enabled":true,"scope":"user","installPath":root,"version":"1.2","mcpServers":{"example":{}}},
            {"id":"unrelated@test-market","enabled":true,"scope":"project","installPath":root,"projectPath":"/unrelated/project"}
        ],"available":[{"pluginId":"new@test-market","name":"new","marketplaceName":"test-market","description":"A new plugin","source":{"source":"url","url":"https://example.test/plugin.git"}}]}).to_string()).unwrap();
        let executable = dir.path().join("claude");
        fs::write(&executable, r#"#!/bin/sh
printf '%s\n' "$@" >> "$CLAUDE_CONFIG_DIR/commands"
if [ -f "$CLAUDE_CONFIG_DIR/fail" ]; then printf 'native denied\n' >&2; exit 7; fi
if [ "$2" = list ]; then cat "$CLAUDE_CONFIG_DIR/inventory.json"; else printf '{"success":true}\n'; fi
"#).unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        let service = PluginService::new(
            executable,
            Some(dir.path().to_owned()),
            dir.path().to_owned(),
        );
        Self { dir, service }
    }
}

fn validate(definition: &str, value: &Value) {
    let schema: Value = serde_json::from_str(claude_codex_server::protocol::SCHEMA).unwrap();
    let mut root = schema.clone();
    root["$ref"] = json!(format!("#/definitions/v2/{definition}"));
    let validator = jsonschema::validator_for(&root).unwrap();
    let errors: Vec<_> = validator
        .iter_errors(value)
        .map(|e| e.to_string())
        .collect();
    assert!(errors.is_empty(), "{errors:?}");
}

#[tokio::test]
async fn discovers_native_catalog_and_component_details() {
    let f = Fixture::new();
    let listed = f.service.handle("plugin/list", &json!({})).await.unwrap();
    validate("PluginListResponse", &listed);
    assert_eq!(
        listed["marketplaces"][0]["plugins"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    // The fixture deliberately lacks its registered catalog manifest. A native
    // CLI that suppresses this error must not cause a false healthy result.
    assert_eq!(listed["marketplaceLoadErrors"].as_array().unwrap().len(), 1);
    let installed = f
        .service
        .handle("plugin/installed", &json!({}))
        .await
        .unwrap();
    validate("PluginInstalledResponse", &installed);
    assert_eq!(
        installed["marketplaces"][0]["plugins"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let details = f.service.handle("plugin/read",&json!({"pluginName":"sample","marketplacePath":f.dir.path().join("market/.claude-plugin/marketplace.json")})).await.unwrap();
    validate("PluginReadResponse", &details);
    assert_eq!(details["plugin"]["skills"][0]["name"], "example");
    assert_eq!(details["plugin"]["hooks"][0]["eventName"], "preToolUse");
    assert_eq!(details["plugin"]["mcpServers"], json!(["example"]));
    assert_eq!(
        f.service.enabled_config().await.unwrap(),
        json!({"sample@test-market":{"enabled":true}})
    );
    let search = f
        .service
        .handle("plugin/search", &json!({"searchTerm":"new","limit":1}))
        .await
        .unwrap();
    validate("PluginSearchResponse", &search);
    assert_eq!(search["data"][0]["plugin"]["id"], "new@test-market");
}

#[tokio::test]
async fn delegates_mutations_without_accepting_plugin_commands() {
    let f = Fixture::new();
    let result = f
        .service
        .handle(
            "plugin/install",
            &json!({"pluginName":"new","remoteMarketplaceName":"test-market"}),
        )
        .await
        .unwrap();
    validate("PluginInstallResponse", &result);
    f.service
        .set_enabled("sample@test-market", false)
        .await
        .unwrap();
    f.service
        .handle(
            "plugin/uninstall",
            &json!({"pluginId":"sample@test-market"}),
        )
        .await
        .unwrap();
    let args = fs::read_to_string(f.dir.path().join("commands")).unwrap();
    assert!(args.contains("install\nnew@test-market\n--scope\nuser\n--json"));
    assert!(args.contains("disable\nsample@test-market\n--scope\nuser\n--json"));
    assert!(args.contains("--keep-data"));
    assert!(!args.contains("--yes"));
    fs::write(f.dir.path().join("fail"), "").unwrap();
    let error = f
        .service
        .set_enabled("sample@test-market", true)
        .await
        .unwrap_err();
    assert_eq!(error.code, -32000);
    assert!(error.message.contains("native denied"));
}

#[tokio::test]
async fn rejects_codex_cloud_and_unknown_plugins() {
    let f = Fixture::new();
    assert!(
        f.service
            .handle("plugin/list", &json!({"marketplaceKinds":["vertical"]}))
            .await
            .is_err()
    );
    assert!(
        f.service
            .handle(
                "plugin/read",
                &json!({"pluginName":"codex-app-tools","remoteMarketplaceName":"openai-bundled"})
            )
            .await
            .is_err()
    );
    assert_eq!(
        f.service
            .handle("plugin/share/list", &json!({}))
            .await
            .unwrap_err()
            .code,
        -32601
    );
    assert!(f.service.set_enabled("--help", true).await.is_err());
    assert!(
        f.service
            .handle("plugin/uninstall", &json!({"pluginId":"a@b; echo bad"}))
            .await
            .is_err()
    );
}

#[test]
fn session_disable_settings_do_not_mutate_global_config() {
    assert_eq!(
        enabled_key_id("plugins.sample@test-market.enabled").unwrap(),
        "sample@test-market"
    );
    assert!(enabled_key_id("plugins.sample@test-market.config").is_err());
    assert_eq!(
        disabled_settings(&["sample@test-market".into()]).unwrap(),
        json!({"enabledPlugins":{"sample@test-market":false}})
    );
}
