//! Actual facade RPC transport with an independent native CLI fixture. Every
//! native write is confined to this test's CLAUDE_CONFIG_DIR.
use serde_json::{Value, json};
use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
};

struct Client {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    serial: u64,
}
impl Client {
    async fn start(root: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
            .args(["app-server", "--stdio", "--claude"])
            .arg(root.join("claude"))
            .arg("--state-dir")
            .arg(root.join("state"))
            .current_dir(root)
            .env("CLAUDE_CONFIG_DIR", root.join("native"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap()).lines();
        let mut client = Self {
            child,
            input,
            output,
            serial: 0,
        };
        client.ok("initialize",json!({"clientInfo":{"name":"plugin-wire","version":"1"},"capabilities":{"experimentalApi":true}})).await;
        client
            .input
            .write_all(b"{\"method\":\"initialized\"}\n")
            .await
            .unwrap();
        client
    }
    async fn rpc(&mut self, method: &str, params: Value) -> Value {
        self.serial += 1;
        self.input
            .write_all(
                format!(
                    "{}\n",
                    json!({"id":self.serial,"method":method,"params":params})
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let line = self
                    .output
                    .next_line()
                    .await
                    .unwrap()
                    .expect("facade closed");
                let value: Value = serde_json::from_str(&line).unwrap();
                if value["id"] == self.serial {
                    return value;
                }
            }
        })
        .await
        .expect("RPC timeout")
    }
    async fn ok(&mut self, method: &str, params: Value) -> Value {
        let response = self.rpc(method, params).await;
        assert!(response.get("error").is_none(), "{method}: {response}");
        response["result"].clone()
    }
    async fn stop(mut self) {
        drop(self.input);
        tokio::time::timeout(Duration::from_secs(10), self.child.wait())
            .await
            .unwrap()
            .unwrap();
    }
}

fn fixture() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let native = root.join("native");
    let plugin = native.join("bundle");
    let market = native.join("market/.claude-plugin");
    fs::create_dir_all(&market).unwrap();
    fs::create_dir_all(native.join("plugins")).unwrap();
    fs::create_dir_all(plugin.join(".claude-plugin")).unwrap();
    fs::create_dir_all(plugin.join("skills/example")).unwrap();
    fs::write(
        plugin.join(".claude-plugin/plugin.json"),
        r#"{"name":"sample","description":"Native fixture"}"#,
    )
    .unwrap();
    fs::write(
        plugin.join("skills/example/SKILL.md"),
        "---\nname: example\ndescription: fixture skill\n---\nTest",
    )
    .unwrap();
    fs::write(
        market.join("marketplace.json"),
        r#"{"name":"fixture","plugins":[]}"#,
    )
    .unwrap();
    fs::write(
        native.join("plugins/known_marketplaces.json"),
        json!({"fixture":{"installLocation":native.join("market")}}).to_string(),
    )
    .unwrap();
    for enabled in [true, false] {
        fs::write(native.join(if enabled {"enabled.json"} else {"disabled.json"}),json!({"installed":[{"id":"sample@fixture","scope":"user","enabled":enabled,"installPath":plugin,"version":"1"}],"available":[]}).to_string()).unwrap();
    }
    let executable = root.join("claude");
    fs::write(&executable,r#"#!/bin/sh
set -eu
test "$1" = plugin
printf '%s\n' "$@" >> "$CLAUDE_CONFIG_DIR/calls"
case "$2" in
list)
  if [ -e "$CLAUDE_CONFIG_DIR/disabled" ]; then cat "$CLAUDE_CONFIG_DIR/disabled.json"; else cat "$CLAUDE_CONFIG_DIR/enabled.json"; fi
  ;;
disable|enable)
  test "$3" = sample@fixture
  test "$4" = --scope
  test "$5" = user
  test "$6" = --json
  if [ -e "$CLAUDE_CONFIG_DIR/deny" ]; then printf 'native policy denied\n' >&2; exit 7; fi
  if [ "$2" = disable ]; then : > "$CLAUDE_CONFIG_DIR/disabled"; else rm -f "$CLAUDE_CONFIG_DIR/disabled"; fi
  printf '{"success":true}\n'
  ;;
*) exit 9 ;;
esac
"#).unwrap();
    fs::set_permissions(executable, fs::Permissions::from_mode(0o755)).unwrap();
    dir
}
fn validate(name: &str, value: &Value) {
    let schema: Value = serde_json::from_str(claude_codex_server::protocol::SCHEMA).unwrap();
    let mut root = schema["definitions"]["v2"][name].clone();
    root["definitions"] = schema["definitions"].clone();
    let validator = jsonschema::validator_for(&root).unwrap();
    let errors: Vec<_> = validator
        .iter_errors(value)
        .map(|e| e.to_string())
        .collect();
    assert!(errors.is_empty(), "{name}: {errors:?}\n{value}");
}
fn edit(enabled: bool) -> Value {
    json!({"keyPath":"plugins.sample@fixture.enabled","value":enabled,"mergeStrategy":"upsert"})
}

#[tokio::test]
async fn plugin_discovery_and_native_enablement_roundtrip() {
    let dir = fixture();
    let mut client = Client::start(dir.path()).await;
    let listed = client
        .ok("plugin/list", json!({"marketplaceKinds":["local"]}))
        .await;
    validate("PluginListResponse", &listed);
    assert_eq!(
        listed["marketplaces"][0]["plugins"][0]["id"],
        "sample@fixture"
    );
    assert_eq!(listed["marketplaceLoadErrors"], json!([]));
    let details=client.ok("plugin/read",json!({"pluginName":"sample","marketplacePath":dir.path().join("native/market/.claude-plugin/marketplace.json")})).await;
    validate("PluginReadResponse", &details);
    assert_eq!(details["plugin"]["skills"][0]["name"], "example");
    let config = client
        .ok("config/read", json!({"includeLayers":true}))
        .await;
    validate("ConfigReadResponse", &config);
    assert_eq!(
        config["config"]["plugins"]["sample@fixture"]["enabled"],
        true
    );
    let written = client
        .ok("config/batchWrite", json!({"edits":[edit(false)]}))
        .await;
    validate("ConfigWriteResponse", &written);
    assert_eq!(
        written["filePath"],
        json!(dir.path().join("native/settings.json"))
    );
    assert!(dir.path().join("native/disabled").exists());
    let config = client.ok("config/read", json!({})).await;
    assert_eq!(
        config["config"]["plugins"]["sample@fixture"]["enabled"],
        false
    );
    let installed = client.ok("plugin/installed", json!({})).await;
    assert_eq!(installed["marketplaces"][0]["plugins"][0]["enabled"], false);
    client
        .ok("config/batchWrite", json!({"edits":[edit(true)]}))
        .await;
    assert!(!dir.path().join("native/disabled").exists());
    client.stop().await;
}

#[tokio::test]
async fn invalid_transactions_and_native_failures_do_not_report_success() {
    let dir = fixture();
    let mut client = Client::start(dir.path()).await;
    for params in [
        json!({"edits":[edit(false),{"keyPath":"model","value":"opus","mergeStrategy":"upsert"}]}),
        json!({"edits":[edit(false),edit(true)]}),
        json!({"edits":[edit(false)],"expectedVersion":"native"}),
        json!({"edits":[{"keyPath":"plugins.sample@fixture.enabled","value":"false","mergeStrategy":"upsert"}]}),
    ] {
        assert_eq!(
            client.rpc("config/batchWrite", params).await["error"]["code"],
            -32602
        );
    }
    assert!(
        !dir.path().join("native/calls").exists(),
        "invalid edits invoked native mutation"
    );
    fs::write(dir.path().join("native/deny"), "").unwrap();
    let failed = client
        .rpc("config/batchWrite", json!({"edits":[edit(false)]}))
        .await;
    assert_eq!(failed["error"]["code"], -32000);
    assert!(
        failed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("native policy denied")
    );
    assert!(!dir.path().join("native/disabled").exists());
    let thread = client
        .ok(
            "thread/start",
            json!({"disabledPluginIds":["sample@fixture"]}),
        )
        .await;
    assert_eq!(thread["disabledPluginIds"], json!(["sample@fixture"]));
    let id = thread["thread"]["id"].clone();
    client.stop().await;
    let mut client = Client::start(dir.path()).await;
    let resumed = client.ok("thread/resume", json!({"threadId":id})).await;
    assert_eq!(resumed["disabledPluginIds"], json!(["sample@fixture"]));
    assert!(
        !dir.path().join("native/disabled").exists(),
        "per-thread override mutated native settings"
    );
    client.stop().await;
}
