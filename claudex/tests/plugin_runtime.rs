use claude_codex_server::{codex_plugins::Config, dynamic_tools::Tool, plugin_runtime::Runtime};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) {
    fs::create_dir_all(path.as_ref().parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn fixture() -> (tempfile::TempDir, Config) {
    let dir = tempfile::tempdir().unwrap();
    let executable = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/plugin_runtime.sh");
    fs::create_dir(dir.path().join("home")).unwrap();
    let config = Config {
        executable,
        home: dir.path().join("home"),
        cwd: dir.path().to_owned(),
        overrides: vec![],
    };
    (dir, config)
}

#[tokio::test]
async fn only_additional_plugin_skills_are_injected_with_full_descriptions() {
    let (_dir, config) = fixture();
    let long_description = "Complete plugin applicability and guidance. ".repeat(12);
    fs::write(config.home.join("description"), &long_description).unwrap();
    let native = ["sample@fixture".to_owned()].into_iter().collect();
    let runtime = Runtime::start_with_native_plugins(config, &[], &[], &native)
        .await
        .unwrap();
    assert!(!runtime.instructions.contains("standalone/SKILL.md"));
    assert!(!runtime.instructions.contains("fixture-skill"));
    assert!(runtime.instructions.contains("sites:sites-building"));
    assert!(runtime.instructions.contains(&long_description));
    assert!(runtime.instructions.contains("/sites/SKILL.md"));
    assert!(runtime.instructions.contains("other:retained"));
    // Skill de-duplication must not remove the Codex MCP execution path.
    let (name, _) = runtime
        .tools
        .iter()
        .find(|(_, (_, server, _))| server == "fixture_server")
        .unwrap();
    let result = runtime
        .call(name, json!({"nonce":"still-routed"}))
        .await
        .unwrap();
    assert_eq!(
        result["structuredContent"]["params"]["arguments"]["nonce"],
        "still-routed"
    );
    runtime.client.shutdown().await;
}

#[tokio::test]
async fn runtime_preserves_input_schema_names_and_routes_actual_calls() {
    let (dir, config) = fixture();
    let runtime = Runtime::start(config, &[], &[]).await.unwrap();
    assert!(
        runtime
            .instructions
            .contains("fixture-skill: Read fixture instructions (file: /fixture/SKILL.md)")
    );
    assert_eq!(
        runtime.tools.len(),
        4,
        "desktop-bound tool leaked into sidecar"
    );
    let (name, (tool, server, native)) = runtime
        .tools
        .iter()
        .find(|(_, (_, server, _))| server == "fixture_server")
        .unwrap();
    assert_eq!(server, "fixture_server");
    assert_eq!(
        native, "token",
        "must route by spec.name, not qualified inventory key"
    );
    assert_eq!(tool.input_schema["required"], json!(["nonce"]));
    assert!(runtime.call(name, json!({})).await.is_err());
    assert!(
        !dir.path().join("home/calls").exists(),
        "invalid arguments reached MCP"
    );
    let result = runtime
        .call(name, json!({"nonce":"caller-token"}))
        .await
        .unwrap();
    assert_eq!(
        result["structuredContent"]["params"],
        json!({"threadId":"real-sidecar-id","server":"fixture_server","tool":"token","arguments":{"nonce":"caller-token"}})
    );
    runtime.client.shutdown().await;
}

#[tokio::test]
async fn thread_disabled_local_plugin_preserves_unrelated_capabilities() {
    let (dir, config) = fixture();
    let runtime = Runtime::start(config, &["sample@fixture".into()], &[])
        .await
        .unwrap();
    assert_eq!(runtime.tools.len(), 3);
    assert!(
        !runtime
            .tools
            .values()
            .any(|(_, server, _)| server == "fixture_server")
    );
    assert!(!runtime.instructions.contains("fixture-skill"));
    assert!(runtime.instructions.contains("sites:sites-building"));
    assert!(runtime.instructions.contains("other:retained"));
    assert!(!dir.path().join("home/overrides").exists());
    runtime.client.shutdown().await;
}

#[tokio::test]
async fn remote_disabled_plugin_filters_connector_tools_and_qualified_skills() {
    for id in ["sites@remote", "remote-sites"] {
        let (dir, config) = fixture();
        let runtime = Runtime::start(config, &[id.into()], &[]).await.unwrap();
        assert_eq!(runtime.tools.len(), 2);
        assert!(
            !runtime
                .tools
                .values()
                .any(|(_, _, name)| name.starts_with("sites."))
        );
        assert!(!runtime.instructions.contains("sites-building"));
        assert!(!runtime.instructions.contains("Sites guidance"));
        assert!(runtime.instructions.contains("fixture-skill"));
        assert!(runtime.instructions.contains("other:retained"));
        let reads: Value = serde_json::from_str(
            fs::read_to_string(dir.path().join("home/reads"))
                .unwrap()
                .trim(),
        )
        .unwrap();
        assert_eq!(
            reads["params"],
            json!({"remoteMarketplaceName":"remote","pluginName":"remote-sites"})
        );
        assert_eq!(runtime.descriptors.len(), 2);
        runtime.client.shutdown().await;
    }
}

#[tokio::test]
async fn connector_guidance_and_ui_metadata_survive_without_app_only_tools() {
    let (_dir, config) = fixture();
    let runtime = Runtime::start(config, &[], &[]).await.unwrap();
    assert_eq!(
        runtime
            .instructions
            .matches("Sites guidance exactly once")
            .count(),
        1
    );
    assert!(
        !runtime
            .tools
            .values()
            .any(|(_, _, name)| name == "app_only")
    );
    let (name, _) = runtime
        .tools
        .iter()
        .find(|(_, (_, _, name))| name == "sites.list_sites")
        .unwrap();
    assert_eq!(
        runtime.descriptors[name]["_meta"]["ui"]["resourceUri"],
        "ui://sites"
    );
    assert_eq!(
        runtime.descriptors[name]["_meta"]["connector_id"],
        "sites-connector"
    );
    runtime.client.shutdown().await;
}

#[tokio::test]
async fn desktop_provided_tool_keeps_its_original_execution_path() {
    let (_dir, config) = fixture();
    let desktop = Tool {
        name: "token".into(),
        namespace: Some("mcp__fixture_server".into()),
        mcp_name: "desktop_token".into(),
        description: "desktop".into(),
        input_schema: json!({"type":"object"}),
    };
    let runtime = Runtime::start(config, &[], &[desktop]).await.unwrap();
    assert_eq!(runtime.tools.len(), 3);
    assert!(
        !runtime
            .tools
            .values()
            .any(|(_, server, _)| server == "fixture_server")
    );
    runtime.client.shutdown().await;
}

/// Real Codex loads, installs, and executes a local plugin in a fresh private
/// CODEX_HOME. The home has no copied auth/config; no model turn is submitted.
/// Run with CODEX_PLUGIN_SMOKE_EXECUTABLE=/absolute/path/to/genuine/codex and
/// CODEX_PLUGIN_SMOKE_BUN=/absolute/path/to/bun, then cargo test --test
/// plugin_runtime genuine_codex_local_plugin -- --ignored --nocapture.
/// Also set CLAUDE_PLUGIN_SMOKE_EXECUTABLE to verify a real Claude Opus turn
/// through the facade using ordinary native Claude authentication.
#[tokio::test]
#[ignore = "requires explicitly selected genuine Codex and Bun executables"]
async fn genuine_codex_local_plugin() {
    let executable = PathBuf::from(
        std::env::var_os("CODEX_PLUGIN_SMOKE_EXECUTABLE")
            .expect("set CODEX_PLUGIN_SMOKE_EXECUTABLE"),
    );
    let bun = PathBuf::from(
        std::env::var_os("CODEX_PLUGIN_SMOKE_BUN").expect("set CODEX_PLUGIN_SMOKE_BUN"),
    );
    assert!(executable.is_absolute() && bun.is_absolute());
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("codex-home");
    let cwd = dir.path().join("workspace");
    let marketplace = dir.path().join("marketplace");
    let plugin = marketplace.join("local-token");
    fs::create_dir_all(&home).unwrap();
    fs::create_dir_all(&cwd).unwrap();
    write(
        home.join("config.toml"),
        format!(
            r#"cli_auth_credentials_store = "file"
model = "fixture-model"
model_provider = "fixture"

[model_providers.fixture]
name = "No model calls"
base_url = "http://127.0.0.1:9"
wire_api = "responses"
requires_openai_auth = false

[features]
plugins = true
remote_plugin = false
apps = false

[marketplaces.runtime-fixture]
source_type = "local"
source = {}

[plugins."already-disabled@runtime-fixture"]
enabled = false

[projects.{}]
trust_level = "trusted"
"#,
            serde_json::to_string(&marketplace).unwrap(),
            serde_json::to_string(&cwd).unwrap()
        ),
    );
    let manifest = marketplace.join(".agents/plugins/marketplace.json");
    write(&manifest,json!({"name":"runtime-fixture","plugins":[{"name":"local-token","source":{"source":"local","path":"./local-token"}}]}).to_string());
    write(plugin.join(".codex-plugin/plugin.json"),json!({"name":"local-token","version":"1.0.0","description":"Isolated actual Codex execution fixture"}).to_string());
    write(
        plugin.join("skills/local-token/SKILL.md"),
        "---\nname: codex-local-token-fixture\ndescription: Use the token MCP tool to echo a nonce.\n---\nCall the token tool with the requested nonce.\n",
    );
    let mcp = dir.path().join("token-mcp.mjs");
    write(
        &mcp,
        r#"import { createInterface } from 'node:readline';
const lines = createInterface({ input: process.stdin });
lines.on('line', line => {
  const request = JSON.parse(line);
  if (request.id === undefined) return;
  let result;
  switch (request.method) {
    case 'initialize': result = { protocolVersion: request.params.protocolVersion, capabilities: { tools: {} }, serverInfo: { name: 'codex-local-token-fixture', version: '1.0.0' } }; break;
    case 'ping': result = {}; break;
    case 'tools/list': result = { tools: [{ name: 'token', description: 'Return the exact caller nonce', inputSchema: { type: 'object', properties: { nonce: { type: 'string' } }, required: ['nonce'], additionalProperties: false } }] }; break;
    case 'tools/call': result = { content: [{ type: 'text', text: JSON.stringify({ marker: 'actual-local-mcp', nonce: request.params.arguments.nonce }) }], isError: false }; break;
    case 'resources/list': result = { resources: [] }; break;
    case 'resources/templates/list': result = { resourceTemplates: [] }; break;
    default: process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: request.id, error: { code: -32601, message: 'Unknown fixture method' } }) + '\n'); return;
  }
  process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: request.id, result }) + '\n');
});
"#,
    );
    write(
        plugin.join(".mcp.json"),
        json!({"mcpServers":{"local_token_fixture":{"command":bun,"args":[mcp]}}}).to_string(),
    );
    let config = Config {
        executable: executable.clone(),
        home: home.clone(),
        cwd: cwd.clone(),
        overrides: vec![],
    };
    let management = claude_codex_server::plugin_runtime::spawn(config.clone())
        .await
        .unwrap();
    let catalog = management
        .request(
            "plugin/list",
            json!({"marketplaceKinds":["local"],"cwds":[cwd]}),
        )
        .await
        .unwrap();
    let catalog_plugin = catalog["marketplaces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["name"] == "runtime-fixture")
        .expect("fixture marketplace absent")["plugins"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "local-token")
        .cloned()
        .expect("fixture plugin absent");
    assert_eq!(catalog_plugin["installed"], false);
    management
        .request(
            "plugin/install",
            json!({"marketplacePath":manifest,"pluginName":"local-token"}),
        )
        .await
        .unwrap();
    let details = management
        .request(
            "plugin/read",
            json!({"marketplacePath":manifest,"pluginName":"local-token"}),
        )
        .await
        .unwrap();
    assert_eq!(details["plugin"]["summary"]["installed"], true);
    assert!(
        details["plugin"]["skills"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["name"] == "local-token:codex-local-token-fixture"),
        "actual fixture plugin details: {details}"
    );
    let installed_config = fs::read_to_string(home.join("config.toml")).unwrap();
    management.shutdown().await;
    let runtime = Runtime::start(config.clone(), &[], &[]).await.unwrap();
    assert!(runtime.instructions.contains("codex-local-token-fixture"));
    let (name, (tool, _, _)) = runtime
        .tools
        .iter()
        .find(|(_, (_, server, native))| {
            server.contains("local_token_fixture") && native == "token"
        })
        .expect("actual plugin MCP tool absent");
    assert_eq!(tool.input_schema["required"], json!(["nonce"]));
    let nonce = uuid::Uuid::new_v4().to_string();
    let result = runtime.call(name, json!({"nonce":nonce})).await.unwrap();
    assert_ne!(result["isError"], true, "{result}");
    let token: Value =
        serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(token, json!({"marker":"actual-local-mcp","nonce":nonce}));
    runtime.client.shutdown().await;
    let disabled = Runtime::start(config, &["local-token@runtime-fixture".into()], &[])
        .await
        .unwrap();
    let effective = disabled
        .client
        .request("config/read", json!({}))
        .await
        .unwrap();
    assert_eq!(
        effective["config"]["plugins"]["already-disabled@runtime-fixture"]["enabled"], false,
        "runtime changed unrelated plugin configuration"
    );
    assert!(
        !disabled.instructions.contains("codex-local-token-fixture"),
        "disabled plugin still in skill inventory; effective plugin config: {}; tools: {:?}",
        effective["config"]["plugins"],
        disabled
            .tools
            .values()
            .map(|(_, server, tool)| (server, tool))
            .collect::<Vec<_>>()
    );
    assert!(
        !disabled
            .tools
            .values()
            .any(|(_, server, _)| server.contains("local_token_fixture"))
    );
    disabled.client.shutdown().await;
    assert_eq!(
        fs::read_to_string(home.join("config.toml")).unwrap(),
        installed_config,
        "runtime loading or session disable changed persistent plugin configuration"
    );
    println!(
        "Genuine Codex local plugin catalog/install/read/skill/MCP nonce/thread disable passed in isolated CODEX_HOME"
    );
    if let Some(claude) = std::env::var_os("CLAUDE_PLUGIN_SMOKE_EXECUTABLE") {
        claude_facade_smoke(dir.path(), &home, &cwd, &executable, &PathBuf::from(claude))
            .await
            .unwrap();
    }
}

async fn claude_facade_smoke(
    root: &Path,
    home: &Path,
    cwd: &Path,
    codex: &Path,
    claude: &Path,
) -> anyhow::Result<()> {
    use anyhow::{Context, ensure};
    ensure!(
        claude.is_absolute(),
        "CLAUDE_PLUGIN_SMOKE_EXECUTABLE must be absolute"
    );
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
        .args(["app-server", "--stdio", "--claude"])
        .arg(claude)
        .args([
            "--model",
            "opus",
            "--initialize-timeout-seconds",
            "90",
            "--approval-timeout-seconds",
            "120",
        ])
        .arg("--state-dir")
        .arg(root.join("facade-state"))
        .env("CLAUDE_CODEX_PLUGIN_EXECUTABLE", codex)
        .env("CLAUDE_CODEX_PLUGIN_HOME", home)
        .env("CODEX_HOME", root.join("facade-codex-home"))
        .env_remove("CLAUDECODE")
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .context("starting isolated facade")?;
    let mut input = child.stdin.take().context("facade stdin")?;
    let mut output = BufReader::new(child.stdout.take().context("facade stdout")?).lines();
    let nonce = uuid::Uuid::new_v4().to_string();
    let prompt = format!(
        "Use the Codex plugin skill local-token:codex-local-token-fixture. Read its SKILL.md using the Read tool, then call its local_token_fixture/token MCP tool exactly once with nonce {nonce}. Return only the exact text returned by that tool. Do not use Bash or any other tools."
    );
    let init = json!({"id":1,"method":"initialize","params":{"clientInfo":{"name":"codex-plugin-claude-smoke","version":"1"},"capabilities":{"experimentalApi":true}}});
    input.write_all(format!("{init}\n").as_bytes()).await?;
    let mut saw_skill_read = false;
    let mut observed_token = false;
    let mut approval_count = 0;
    let result=tokio::time::timeout(Duration::from_secs(240),async {
        loop {
            let line=output.next_line().await?.context("facade closed before completion")?;
            let message:Value=serde_json::from_str(&line)?;
            ensure!(message.get("error").is_none(),"Facade RPC failed: {}",message["error"]);
            let request=match message["id"].as_u64() {
                Some(1)=>{
                    input.write_all(b"{\"method\":\"initialized\"}\n").await?;
                    Some(json!({"id":2,"method":"thread/start","params":{"cwd":cwd,"model":"opus","ephemeral":true,"approvalPolicy":"on-request","sandbox":"danger-full-access"}}))
                },
                Some(2)=>Some(json!({"id":3,"method":"turn/start","params":{"threadId":message["result"]["thread"]["id"],"input":[{"type":"text","text":prompt,"text_elements":[]}]}})),
                _=>None,
            };
            if let Some(request)=request {input.write_all(format!("{request}\n").as_bytes()).await?;}
            if message.get("id").is_some() && message.get("method").is_some() {
                let question=message["params"]["questions"][0]["question"].as_str().unwrap_or_default();
                let allowed=message["method"]=="item/tool/requestUserInput" && (
                    (question.contains("tool Read ") && question.contains("SKILL.md") && question.contains(root.file_name().unwrap().to_str().unwrap())) ||
                    (question.contains("tool mcp__codex_desktop__") && question.contains(&nonce)));
                let response=if allowed {
                    approval_count+=1;
                    json!({"id":message["id"],"result":{"answers":{"permission":{"answers":["Allow"]}}}})
                } else {
                    json!({"id":message["id"],"error":{"code":-32601,"message":"Only fixture skill Read and token MCP execution are authorized"}})
                };
                input.write_all(format!("{response}\n").as_bytes()).await?;
            }
            if message["method"]=="item/completed" {
                let item=&message["params"]["item"];
                if item["type"]=="dynamicToolCall" && item["tool"]=="Read" && item["arguments"]["file_path"].as_str().is_some_and(|s|s.contains("local-token/SKILL.md")) {
                    saw_skill_read=true;
                }
                if item["type"]=="mcpToolCall" && item["server"].as_str().is_some_and(|s|s.contains("local_token_fixture")) {
                    ensure!(item["status"]=="completed","Plugin execution failed: {item}");
                    let token:Value=serde_json::from_str(item["result"]["content"][0]["text"].as_str().context("plugin result text")?)?;
                    ensure!(token==json!({"marker":"actual-local-mcp","nonce":nonce}),"Plugin nonce mismatch: {token}");
                    observed_token=true;
                }
            }
            if message["method"]=="turn/completed" {
                let turn=&message["params"]["turn"];
                ensure!(turn["status"]=="completed","Claude turn failed: {}",turn["error"]);
                ensure!(saw_skill_read,"Claude did not read the actual Codex plugin skill");
                ensure!(observed_token,"Claude did not execute the actual Codex plugin MCP");
                ensure!(turn["items"].as_array().context("turn items")?.iter().any(|item|item["type"]=="agentMessage" && item["text"].as_str().is_some_and(|text|text.contains("actual-local-mcp") && text.contains(&nonce))),"Claude final response omitted actual plugin token");
                return Ok::<_,anyhow::Error>(());
            }
        }
    }).await;
    drop(input);
    if tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .is_err()
    {
        let _ = child.kill().await;
    }
    result.context("Claude plugin smoke timed out")??;
    println!(
        "Actual Claude Opus facade turn read Codex skill and executed real plugin MCP nonce; {approval_count} scoped approvals"
    );
    Ok(())
}
