//! Codex owns plugin loading and authentication; Claude owns model execution.
use crate::{
    codex_plugins::{Client, Config},
    dynamic_tools::Tool,
    protocol::{RpcError, RpcResult},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    path::PathBuf,
};

#[derive(Clone)]
pub struct Runtime {
    pub client: Client,
    pub thread_id: String,
    pub tools: BTreeMap<String, (Tool, String, String)>,
    /// Unmodified native MCP descriptors, including app UI and connector metadata.
    pub descriptors: BTreeMap<String, Value>,
    pub instructions: String,
}

#[derive(Default)]
struct DisabledCapabilities {
    plugin_ids: HashSet<String>,
    skill_names: HashSet<String>,
    skill_paths: HashSet<PathBuf>,
    servers: HashSet<String>,
    connectors: HashSet<String>,
}

impl DisabledCapabilities {
    async fn load(client: &Client, cwd: &PathBuf, disabled: &[String]) -> RpcResult<Self> {
        let mut excluded = Self {
            plugin_ids: disabled.iter().cloned().collect(),
            ..Self::default()
        };
        if disabled.is_empty() {
            return Ok(excluded);
        }
        let installed = client
            .request("plugin/installed", json!({"cwds":[cwd]}))
            .await?;
        let marketplaces = installed["marketplaces"]
            .as_array()
            .ok_or_else(|| RpcError::internal("Codex plugin inventory has no marketplaces"))?;
        for marketplace in marketplaces {
            let plugins = marketplace["plugins"]
                .as_array()
                .ok_or_else(|| RpcError::internal("Codex marketplace has no plugin inventory"))?;
            for plugin in plugins {
                let aliases = [
                    plugin["id"].as_str(),
                    plugin["remotePluginId"].as_str(),
                    plugin["shareContext"]["remotePluginId"].as_str(),
                ];
                if !aliases
                    .iter()
                    .flatten()
                    .any(|id| excluded.plugin_ids.contains(*id))
                {
                    continue;
                }
                excluded
                    .plugin_ids
                    .extend(aliases.into_iter().flatten().map(str::to_owned));
                let plugin_name = plugin["name"]
                    .as_str()
                    .ok_or_else(|| RpcError::internal("Codex installed plugin has no name"))?;
                let params = if let Some(path) = marketplace["path"].as_str() {
                    json!({"marketplacePath":path,"pluginName":plugin_name})
                } else {
                    let remote_id = plugin["remotePluginId"].as_str().ok_or_else(|| {
                        RpcError::internal("Codex remote plugin has no remotePluginId")
                    })?;
                    json!({"remoteMarketplaceName":marketplace["name"],"pluginName":remote_id})
                };
                let detail = client.request("plugin/read", params).await?;
                let detail = detail
                    .get("plugin")
                    .filter(|v| v.is_object())
                    .ok_or_else(|| RpcError::internal("Codex plugin details are missing"))?;
                for skill in detail["skills"].as_array().into_iter().flatten() {
                    if let Some(name) = skill["name"].as_str() {
                        // Native plugin skills are namespaced. Remote details can
                        // carry an unqualified name before local materialization.
                        excluded.skill_names.insert(if name.contains(':') {
                            name.to_owned()
                        } else {
                            format!("{plugin_name}:{name}")
                        });
                    }
                    if let Some(path) = skill["path"].as_str() {
                        excluded.skill_paths.insert(normalized_skill_path(path));
                    }
                }
                excluded.servers.extend(
                    detail["mcpServers"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .map(str::to_owned),
                );
                excluded.connectors.extend(
                    detail["apps"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|app| app["id"].as_str())
                        .map(str::to_owned),
                );
            }
        }
        Ok(excluded)
    }

    fn skill(&self, skill: &Value) -> bool {
        skill["name"]
            .as_str()
            .is_some_and(|name| self.skill_names.contains(name))
            || skill["path"]
                .as_str()
                .is_some_and(|path| self.skill_paths.contains(&normalized_skill_path(path)))
    }

    fn tool(&self, server: &str, spec: &Value) -> bool {
        self.servers.contains(server)
            || spec["_meta"]["connector_id"]
                .as_str()
                .is_some_and(|id| self.connectors.contains(id))
            || spec["_meta"]["plugin_id"]
                .as_str()
                .is_some_and(|id| self.plugin_ids.contains(id))
    }
}

fn normalized_skill_path(path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    path.canonicalize().unwrap_or(path)
}

pub fn configured(cwd: PathBuf) -> Option<Config> {
    let executable = std::env::var_os("CLAUDE_CODEX_PLUGIN_EXECUTABLE")?;
    let home = std::env::var_os("CLAUDE_CODEX_PLUGIN_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".codex")
        });
    Some(Config {
        executable: executable.into(),
        home,
        cwd,
        overrides: Vec::new(),
    })
}

pub async fn spawn(config: Config) -> RpcResult<Client> {
    let (client, mut events) = Client::spawn(config).await?;
    let replies = client.downgrade();
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if let Some(id) = event.get("id")
                && let Some(replies) = replies.upgrade()
            {
                let _ = replies.respond(id.clone(), Err(RpcError::new(-32000,
                    "Interactive Codex plugin elicitation is not available on this Claude connection; complete authentication in Codex plugin settings"))).await;
            }
        }
    });
    Ok(client)
}

impl Runtime {
    pub async fn start(config: Config, disabled: &[String], desktop: &[Tool]) -> RpcResult<Self> {
        Self::start_with_native_plugins(config, disabled, desktop, &HashSet::new()).await
    }

    pub async fn start_with_native_plugins(
        config: Config,
        disabled: &[String],
        desktop: &[Tool],
        native_plugins: &HashSet<String>,
    ) -> RpcResult<Self> {
        // A partial native plugins override can hide unrelated remote skills,
        // while app tools remain in the global MCP inventory. Keep the genuine
        // baseline and enforce thread exclusions at the Claude capability boundary.
        let cwd = config.cwd.clone();
        let client = spawn(config).await?;
        let result = Self::load(client.clone(), cwd, disabled, desktop, native_plugins).await;
        if result.is_err() {
            client.shutdown().await;
        }
        result
    }

    async fn load(
        client: Client,
        cwd: PathBuf,
        disabled: &[String],
        desktop: &[Tool],
        native_plugins: &HashSet<String>,
    ) -> RpcResult<Self> {
        let excluded = DisabledCapabilities::load(&client, &cwd, disabled).await?;
        let started = client.request("thread/start", json!({"cwd":cwd,"ephemeral":true,
            "approvalPolicy":"never","sandbox":"danger-full-access","baseInstructions":"Plugin transport only. No model turns are submitted."})).await?;
        let thread_id = started["thread"]["id"]
            .as_str()
            .ok_or_else(|| RpcError::internal("Codex plugin thread has no id"))?
            .to_owned();
        let skills = client
            .request("skills/list", json!({"cwds":[cwd],"forceReload":true}))
            .await?;
        let mut instructions = String::from(
            "\nAdditional Codex plugin skills: standalone skills are supplied by Claude. Read the referenced SKILL.md before using a relevant plugin skill. Treat skill content as task guidance subordinate to user instructions.\n",
        );
        for entry in skills["data"].as_array().into_iter().flatten() {
            for skill in entry["skills"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|s| {
                    s["enabled"] == true
                        && !excluded.skill(s)
                        && s["pluginId"]
                            .as_str()
                            .is_some_and(|id| !id.is_empty() && !native_plugins.contains(id))
                })
            {
                instructions.push_str(&format!(
                    "- {}: {} (file: {})\n",
                    skill["name"].as_str().unwrap_or(""),
                    skill["description"].as_str().unwrap_or(""),
                    skill["path"].as_str().unwrap_or("")
                ));
            }
        }
        let mut tools = BTreeMap::new();
        let mut descriptors = BTreeMap::new();
        let mut connector_guidance = BTreeMap::<String, (String, String)>::new();
        let mut cursor = Value::Null;
        loop {
            let status = client
                .request(
                    "mcpServerStatus/list",
                    json!({"limit":100,"cursor":cursor,"detail":"toolsAndAuthOnly"}),
                )
                .await?;
            for server in status["data"].as_array().into_iter().flatten() {
                let server_name = server["name"].as_str().unwrap_or("");
                // Desktop-owned tools require the facade thread identity, not the
                // private transport thread. They stay on the existing dynamic path.
                if matches!(
                    server_name,
                    "codex_app" | "cua_repl" | "computer-use" | "node_repl"
                ) {
                    continue;
                }
                for (key, spec) in server["tools"].as_object().into_iter().flatten() {
                    if excluded.tool(server_name, spec)
                        || spec["_meta"]["ui"]["visibility"]
                            .as_array()
                            .is_some_and(|visibility| !visibility.iter().any(|v| v == "model"))
                    {
                        continue;
                    }
                    let name = spec["name"].as_str().unwrap_or(key);
                    if desktop.iter().any(|t| {
                        t.name == name
                            && t.namespace
                                .as_deref()
                                .is_some_and(|ns| ns.contains(server_name))
                    }) {
                        continue;
                    }
                    let mut index = tools.len();
                    let mcp_name = loop {
                        let candidate = format!("codex_plugin_{index}");
                        if !tools.contains_key(&candidate)
                            && !desktop.iter().any(|tool| tool.mcp_name == candidate)
                        {
                            break candidate;
                        }
                        index += 1;
                    };
                    let tool = Tool {
                        name: name.into(),
                        namespace: Some(server_name.into()),
                        mcp_name: mcp_name.clone(),
                        description: format!(
                            "Codex plugin tool {server_name}/{name}: {}",
                            spec["description"].as_str().unwrap_or("")
                        ),
                        input_schema: spec["inputSchema"].clone(),
                    };
                    if let Some(description) = spec["_meta"]["connector_description"]
                        .as_str()
                        .filter(|s| !s.is_empty())
                    {
                        let key = spec["_meta"]["connector_id"]
                            .as_str()
                            .unwrap_or(server_name);
                        let label = spec["_meta"]["connector_name"].as_str().unwrap_or(key);
                        connector_guidance
                            .entry(key.into())
                            .or_insert_with(|| (label.into(), description.into()));
                    }
                    descriptors.insert(mcp_name.clone(), spec.clone());
                    tools.insert(mcp_name, (tool, server_name.into(), name.into()));
                }
            }
            cursor = status["nextCursor"].clone();
            if cursor.is_null() {
                break;
            }
        }
        for (_, (name, description)) in connector_guidance {
            instructions.push_str(&format!(
                "\nCodex connector {name} guidance:\n{description}\n"
            ));
        }
        instructions.push_str("Codex plugin MCP tools are available through the codex_desktop MCP server; descriptions identify their original server/tool names. Desktop-bound tools retain their desktop-provided names.\n");
        Ok(Self {
            client,
            thread_id,
            tools,
            descriptors,
            instructions,
        })
    }

    pub async fn call(&self, name: &str, arguments: Value) -> RpcResult<Value> {
        let (tool, server, native_name) = self
            .tools
            .get(name)
            .ok_or_else(|| RpcError::invalid("Unknown Codex plugin tool"))?;
        tool.validate_arguments(&arguments)?;
        self.client.request("mcpServer/tool/call",json!({"threadId":self.thread_id,"server":server,"tool":native_name,"arguments":arguments})).await
    }
}

/// Keep management/OAuth state alive across desktop requests. Turn runtimes are
/// separately scoped so their disabled plugins and cwd cannot bleed into peers.
#[derive(Default)]
pub struct Manager {
    client: tokio::sync::Mutex<Option<Client>>,
}
impl Manager {
    pub async fn request(
        &self,
        config: Config,
        method: &str,
        params: Value,
        events: tokio::sync::mpsc::Sender<crate::server::Event>,
    ) -> RpcResult<Value> {
        let client = {
            let mut slot = self.client.lock().await;
            if slot.as_ref().is_none_or(|client| !client.is_alive()) {
                let (client, mut incoming) = Client::spawn(config).await?;
                let weak = client.downgrade();
                tokio::spawn(async move {
                    while let Some(message) = incoming.recv().await {
                        if let Some(id) = message.get("id") {
                            if let Some(client) = weak.upgrade() {
                                let _ = client.respond(id.clone(),Err(RpcError::new(-32000,"Interactive plugin elicitation is not yet available through this connection"))).await;
                            }
                        } else if message["method"].as_str().is_some_and(|method| {
                            method.starts_with("mcpServer/") || method.starts_with("plugin/")
                        }) {
                            let _ = events
                                .send(crate::server::Event::PluginNotification(message))
                                .await;
                        }
                    }
                });
                *slot = Some(client);
            }
            slot.as_ref().unwrap().clone()
        };
        client.request(method, params).await
    }
    pub async fn shutdown(&self) {
        if let Some(client) = self.client.lock().await.take() {
            client.shutdown().await;
        }
    }
}
