//! Codex plugin discovery backed exclusively by the configured Claude installation.
//! OpenAI catalog/sharing and bundled Codex tools are not Claude plugins.
use crate::protocol::{RpcError, RpcResult};
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{io::AsyncReadExt, process::Command};

#[derive(Clone, Debug)]
pub struct PluginService {
    executable: PathBuf,
    config_dir: PathBuf,
    cwd: PathBuf,
}

#[derive(Clone)]
struct Plugin {
    summary: Value,
    marketplace: String,
    marketplace_path: Option<PathBuf>,
    root: Option<PathBuf>,
    native: Value,
}

impl PluginService {
    pub fn new(executable: PathBuf, config_dir: Option<PathBuf>, cwd: PathBuf) -> Self {
        let config_dir = config_dir
            .or_else(|| std::env::var_os("CLAUDE_CONFIG_DIR").map(PathBuf::from))
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".claude")
            });
        Self {
            executable,
            config_dir,
            cwd,
        }
    }

    pub fn settings_path(&self) -> PathBuf {
        self.config_dir.join("settings.json")
    }

    // Bound output and wall time: plugin loaders can execute configured MCP helpers.
    async fn run(&self, args: &[String], cwd: &Path) -> RpcResult<Value> {
        let mut cmd = Command::new(&self.executable);
        cmd.arg("plugin")
            .args(args)
            .current_dir(cwd)
            .env("CLAUDE_CONFIG_DIR", &self.config_dir)
            .env_remove("CLAUDECODE")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd.spawn().map_err(RpcError::internal)?;
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let result = tokio::time::timeout(Duration::from_secs(120), async {
            let read = |stream: Box<dyn tokio::io::AsyncRead + Unpin + Send>| async move {
                let mut bytes = Vec::new();
                stream
                    .take(8 * 1024 * 1024 + 1)
                    .read_to_end(&mut bytes)
                    .await?;
                if bytes.len() > 8 * 1024 * 1024 {
                    return Err(std::io::Error::other("Claude plugin output exceeded 8 MiB"));
                }
                Ok::<_, std::io::Error>(bytes)
            };
            tokio::try_join!(child.wait(), read(Box::new(stdout)), read(Box::new(stderr)))
        })
        .await
        .map_err(|_| RpcError::internal("Claude plugin command timed out"))?
        .map_err(RpcError::internal)?;
        let (status, stdout, stderr) = result;
        if !status.success() {
            return Err(RpcError::new(
                -32000,
                format!(
                    "Claude plugin command failed: {}",
                    String::from_utf8_lossy(if stderr.is_empty() { &stdout } else { &stderr })
                        .trim()
                ),
            ));
        }
        if stdout.iter().all(u8::is_ascii_whitespace) {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&stdout)
            .or_else(|_| {
                // Some native mutation commands print progress before their final JSON line.
                String::from_utf8_lossy(&stdout)
                    .lines()
                    .rev()
                    .find_map(|line| serde_json::from_str(line).ok())
                    .ok_or_else(|| {
                        <serde_json::Error as serde::de::Error>::custom(
                            "Claude plugin command did not return JSON",
                        )
                    })
            })
            .map_err(RpcError::internal)
    }

    fn marketplaces(&self) -> RpcResult<BTreeMap<String, PathBuf>> {
        let path = self.config_dir.join("plugins/known_marketplaces.json");
        let value = read_json_optional(&path)?;
        Ok(value
            .as_object()
            .into_iter()
            .flatten()
            .filter_map(|(name, entry)| {
                entry["installLocation"].as_str().map(|p| {
                    let p = PathBuf::from(p);
                    let path = if p.is_file() {
                        p
                    } else {
                        p.join(".claude-plugin/marketplace.json")
                    };
                    (name.clone(), path)
                })
            })
            .collect())
    }

    async fn inventory(&self, params: &Value) -> RpcResult<Vec<Plugin>> {
        let marketplaces = self.marketplaces()?;
        let cwds: Vec<PathBuf> = params["cwds"]
            .as_array()
            .filter(|v| !v.is_empty())
            .map(|v| {
                v.iter()
                    .filter_map(Value::as_str)
                    .map(PathBuf::from)
                    .collect()
            })
            .unwrap_or_else(|| vec![self.cwd.clone()]);
        let mut plugins = BTreeMap::<String, Plugin>::new();
        for cwd in cwds {
            let native = self
                .run(&strings(&["list", "--json", "--available"]), &cwd)
                .await?;
            let installed = native["installed"].as_array().ok_or_else(|| {
                RpcError::internal("Claude plugin list is missing installed array")
            })?;
            let available = native["available"].as_array().ok_or_else(|| {
                RpcError::internal("Claude plugin list is missing available array")
            })?;
            for entry in available.iter().chain(installed.iter()) {
                let is_installed = entry.get("id").is_some();
                if is_installed
                    && entry["projectPath"]
                        .as_str()
                        .is_some_and(|p| !cwd.starts_with(p))
                {
                    continue;
                }
                let id = entry[if is_installed { "id" } else { "pluginId" }]
                    .as_str()
                    .ok_or_else(|| RpcError::internal("Claude plugin is missing id"))?;
                let (name, market) = validate_plugin_id(id)?;
                let marketplace_path = marketplaces.get(market).cloned();
                let root = entry["installPath"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .map(PathBuf::from)
                    .or_else(|| {
                        entry["source"].as_str().and_then(|p| {
                            marketplace_path
                                .as_ref()
                                .and_then(|m| m.parent()?.parent().map(|r| r.join(p)))
                        })
                    });
                let manifest = root
                    .as_ref()
                    .map(|r| read_json_optional(&r.join(".claude-plugin/plugin.json")))
                    .transpose()?
                    .unwrap_or(Value::Null);
                let description = entry
                    .get("description")
                    .or_else(|| manifest.get("description"))
                    .cloned()
                    .unwrap_or(Value::Null);
                let source = if let Some(root) = &root {
                    json!({"type":"local","path":root})
                } else if let Some(url) = entry["source"]["url"].as_str() {
                    json!({"type":"git","url":url,"path":entry["source"]["path"],"refName":entry["source"]["ref"],"sha":entry["source"]["sha"]})
                } else if entry["source"]["source"] == "github" {
                    json!({"type":"git","url":format!("https://github.com/{}",entry["source"]["repo"].as_str().unwrap_or_default())})
                } else if entry["source"]["source"] == "npm" {
                    json!({"type":"npm","package":entry["source"]["package"],"version":entry["source"]["version"]})
                } else {
                    return Err(RpcError::new(
                        -32000,
                        format!("Claude plugin {id} uses a source not representable by Codex"),
                    ));
                };
                let enabled = is_installed && entry["enabled"].as_bool().unwrap_or(false);
                let summary = json!({"id":id,"name":name,"source":source,"installed":is_installed,"enabled":enabled,
                    "authPolicy":"ON_USE","installPolicy":"AVAILABLE","availability":"AVAILABLE",
                    "localVersion":if is_installed { entry["version"].clone() } else { Value::Null },"version":entry["version"],
                    "interface":{"displayName":name,"shortDescription":description,"capabilities":[],"screenshots":[],"screenshotUrls":[]}});
                let plugin = Plugin {
                    summary,
                    marketplace: market.to_string(),
                    marketplace_path,
                    root,
                    native: entry.clone(),
                };
                if !plugins
                    .get(id)
                    .is_some_and(|p| p.summary["installed"] == true && !is_installed)
                {
                    plugins.insert(id.to_string(), plugin);
                }
            }
        }
        Ok(plugins.into_values().collect())
    }

    pub async fn enabled_config(&self) -> RpcResult<Value> {
        let mut config = Map::new();
        for plugin in self.inventory(&json!({})).await? {
            if plugin.summary["installed"] == true {
                config.insert(
                    plugin.summary["id"].as_str().unwrap().into(),
                    json!({"enabled":plugin.summary["enabled"]}),
                );
            }
        }
        Ok(Value::Object(config))
    }

    pub async fn set_enabled(&self, id: &str, enabled: bool) -> RpcResult<()> {
        validate_plugin_id(id)?;
        self.run(
            &strings(&[
                if enabled { "enable" } else { "disable" },
                id,
                "--scope",
                "user",
                "--json",
            ]),
            &self.cwd,
        )
        .await?;
        Ok(())
    }

    pub async fn handle(&self, method: &str, params: &Value) -> RpcResult<Value> {
        if !matches!(
            method,
            "plugin/list"
                | "plugin/installed"
                | "plugin/search"
                | "plugin/read"
                | "plugin/install"
                | "plugin/uninstall"
        ) {
            return Err(RpcError::unsupported(method));
        }
        if params["marketplaceKinds"]
            .as_array()
            .is_some_and(|v| v.iter().any(|k| k != "local"))
        {
            return Err(RpcError::invalid(
                "Claude exposes configured local marketplaces; OpenAI remote catalogs and sharing are unavailable",
            ));
        }
        if method == "plugin/uninstall" {
            let id = required(params, "pluginId")?;
            validate_plugin_id(id)?;
            self.run(
                &strings(&["uninstall", id, "--scope", "user", "--json", "--keep-data"]),
                &self.cwd,
            )
            .await?;
            return Ok(json!({}));
        }
        if params["forceRefetch"] == true {
            // Native update has no JSON mode for all marketplaces. Update each known name.
            for name in self.marketplaces()?.keys() {
                self.run(
                    &strings(&["marketplace", "update", name, "--json"]),
                    &self.cwd,
                )
                .await?;
            }
        }
        let plugins = self.inventory(params).await?;
        if method == "plugin/read" || method == "plugin/install" {
            let plugin = resolve(&plugins, params)?;
            if method == "plugin/install" {
                self.run(
                    &strings(&[
                        "install",
                        plugin.summary["id"].as_str().unwrap(),
                        "--scope",
                        "user",
                        "--json",
                    ]),
                    &self.cwd,
                )
                .await?;
                return Ok(json!({"appsNeedingAuth":[],"authPolicy":"ON_USE"}));
            }
            return Ok(json!({"plugin": detail(plugin)?}));
        }
        if method == "plugin/search" {
            if params["scope"].as_str().is_some_and(|s| s != "global") {
                return Err(RpcError::invalid(
                    "Claude plugin search supports only the configured global catalog",
                ));
            }
            let term = required(params, "searchTerm")?.to_lowercase();
            let start = match params["cursor"].as_str() {
                Some(v) => v
                    .parse::<usize>()
                    .map_err(|_| RpcError::invalid("Invalid plugin cursor"))?,
                None => 0,
            };
            let limit = params["limit"].as_u64().unwrap_or(50).clamp(1, 200) as usize;
            let matches: Vec<_> = plugins
                .iter()
                .filter(|p| {
                    p.summary["name"]
                        .as_str()
                        .unwrap_or_default()
                        .to_lowercase()
                        .contains(&term)
                        || p.summary["interface"]["shortDescription"]
                            .as_str()
                            .unwrap_or_default()
                            .to_lowercase()
                            .contains(&term)
                })
                .collect();
            let data:Vec<_> = matches.iter().skip(start).take(limit).map(|p| json!({"plugin":p.summary,"marketplaceName":p.marketplace,"marketplacePath":p.marketplace_path})).collect();
            let next = (start.saturating_add(data.len()) < matches.len())
                .then(|| (start + data.len()).to_string());
            return Ok(json!({"data":data,"nextCursor":next}));
        }
        let mut groups = BTreeMap::<String, Value>::new();
        let mut load_errors = Vec::new();
        // Claude's catalog CLI suppresses marketplace read failures. Inspect the
        // configured manifests too so that unavailable catalogs are not reported
        // as a successful empty inventory.
        for path in self.marketplaces()?.values() {
            let error = match std::fs::read(path) {
                Ok(bytes) => serde_json::from_slice::<Value>(&bytes)
                    .err()
                    .map(|e| e.to_string()),
                Err(error) => Some(error.to_string()),
            };
            if let Some(message) = error {
                load_errors.push(json!({"marketplacePath":path,"message":message}));
            }
        }
        let suggestions = params["installSuggestionPluginNames"].as_array();
        for p in plugins {
            if let (Some(errors), Some(path)) = (p.native["errors"].as_array(), &p.marketplace_path)
            {
                for error in errors {
                    load_errors.push(json!({"marketplacePath":path,"message":format!("{}: {}",p.summary["id"].as_str().unwrap(),error.as_str().unwrap_or("Native plugin load failure"))}));
                }
            }
            if method == "plugin/installed"
                && p.summary["installed"] != true
                && !suggestions.is_some_and(|s| s.contains(&p.summary["name"]))
            {
                continue;
            }
            let group = groups.entry(p.marketplace.clone()).or_insert_with(
                || json!({"name":p.marketplace,"path":p.marketplace_path,"plugins":[]}),
            );
            group["plugins"].as_array_mut().unwrap().push(p.summary);
        }
        Ok(
            json!({"marketplaces":groups.into_values().collect::<Vec<_>>(),"marketplaceLoadErrors":load_errors,"featuredPluginIds":[]}),
        )
    }
}

pub fn validate_plugin_id(id: &str) -> RpcResult<(&str, &str)> {
    let (name, marketplace) = id
        .rsplit_once('@')
        .ok_or_else(|| RpcError::invalid("Plugin id must be name@marketplace"))?;
    if [name, marketplace].iter().any(|s| {
        s.is_empty()
            || s.starts_with('-')
            || !s
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    }) {
        return Err(RpcError::invalid("Invalid native Claude plugin id"));
    }
    Ok((name, marketplace))
}

pub fn enabled_key_id(key: &str) -> RpcResult<&str> {
    let id = key
        .strip_prefix("plugins.")
        .and_then(|k| k.strip_suffix(".enabled"))
        .ok_or_else(|| RpcError::invalid("Expected plugins.<id>.enabled"))?;
    validate_plugin_id(id)?;
    Ok(id)
}

/// Merge this into existing --settings JSON; do not add a second --settings flag.
pub fn disabled_settings(ids: &[String]) -> RpcResult<Value> {
    let mut enabled = Map::new();
    for id in ids {
        validate_plugin_id(id)?;
        enabled.insert(id.clone(), Value::Bool(false));
    }
    Ok(json!({"enabledPlugins":enabled}))
}

fn required<'a>(params: &'a Value, key: &str) -> RpcResult<&'a str> {
    params[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| RpcError::invalid(format!("Missing {key}")))
}
fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| (*s).into()).collect()
}
fn read_json_optional(path: &Path) -> RpcResult<Value> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|e| RpcError::internal(format!("{}: {e}", path.display()))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Value::Null),
        Err(e) => Err(RpcError::internal(e)),
    }
}
fn resolve<'a>(plugins: &'a [Plugin], params: &Value) -> RpcResult<&'a Plugin> {
    let name = required(params, "pluginName")?;
    let matches: Vec<_> = plugins
        .iter()
        .filter(|p| {
            (p.summary["name"] == name || p.summary["id"] == name)
                && params["remoteMarketplaceName"]
                    .as_str()
                    .is_none_or(|m| p.marketplace == m)
                && params["marketplacePath"]
                    .as_str()
                    .is_none_or(|m| p.marketplace_path.as_deref() == Some(Path::new(m)))
        })
        .collect();
    match matches.as_slice() {
        [plugin] => Ok(plugin),
        [] => Err(RpcError::invalid(
            "Plugin not found in configured Claude marketplaces",
        )),
        _ => Err(RpcError::invalid(
            "Ambiguous plugin name; provide marketplacePath",
        )),
    }
}
fn detail(plugin: &Plugin) -> RpcResult<Value> {
    let mut skills = Vec::new();
    let mut hooks = Vec::new();
    if let Some(root) = &plugin.root {
        let manifest = read_json_optional(&root.join(".claude-plugin/plugin.json"))?;
        let mut skill_roots = vec![root.join("skills")];
        if let Some(path) = manifest["skills"].as_str() {
            skill_roots.push(root.join(path));
        }
        if let Some(paths) = manifest["skills"].as_array() {
            skill_roots.extend(paths.iter().filter_map(Value::as_str).map(|p| root.join(p)));
        }
        for dir in skill_roots {
            let mut paths = Vec::new();
            if dir.join("SKILL.md").is_file() {
                paths.push(dir.join("SKILL.md"));
            }
            if let Ok(entries) = std::fs::read_dir(&dir) {
                paths.extend(
                    entries
                        .flatten()
                        .map(|e| e.path().join("SKILL.md"))
                        .filter(|p| p.is_file()),
                );
            }
            for path in paths {
                if skills.iter().any(|s: &Value| s["path"] == json!(path)) {
                    continue;
                }
                let text = std::fs::read_to_string(&path).map_err(RpcError::internal)?;
                let field = |name: &str| {
                    text.strip_prefix("---")
                        .and_then(|s| s.split("---").next())
                        .and_then(|s| s.lines().find_map(|l| l.strip_prefix(&format!("{name}:"))))
                        .map(|s| s.trim().trim_matches(['\'', '"']).to_string())
                };
                let name = field("name").unwrap_or_else(|| {
                    path.parent()
                        .unwrap()
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .into()
                });
                skills.push(json!({"name":name,"description":field("description").unwrap_or_default(),"path":path,"enabled":plugin.summary["enabled"]}));
            }
        }
        let mut hook_config = read_json_optional(&root.join("hooks/hooks.json"))?;
        if let Some(path) = manifest["hooks"].as_str() {
            hook_config = read_json_optional(&root.join(path))?;
        } else if manifest["hooks"].is_object() {
            hook_config = manifest["hooks"].clone();
        }
        for event in hook_config["hooks"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, _)| k)
        {
            let mut chars = event.chars();
            let mapped = format!("{}{}", chars.next().unwrap().to_lowercase(), chars.as_str());
            if [
                "preToolUse",
                "permissionRequest",
                "postToolUse",
                "preCompact",
                "postCompact",
                "sessionStart",
                "sessionEnd",
                "userPromptSubmit",
                "subagentStart",
                "subagentStop",
                "stop",
                "interrupt",
            ]
            .contains(&mapped.as_str())
            {
                hooks.push(json!({"eventName":mapped,"key":format!("{}:{event}",plugin.summary["id"].as_str().unwrap())}));
            }
        }
    }
    let mcps: Vec<_> = plugin.native["mcpServers"]
        .as_object()
        .into_iter()
        .flatten()
        .map(|(key, _)| key.clone())
        .collect();
    Ok(
        json!({"summary":plugin.summary,"description":plugin.summary["interface"]["shortDescription"],"marketplaceName":plugin.marketplace,"marketplacePath":plugin.marketplace_path,"skills":skills,"hooks":hooks,"mcpServers":mcps,"apps":[],"appTemplates":[]}),
    )
}
