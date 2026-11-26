//! Opt-in self-hosted Responses providers using native Codex tools.
//!
//! Inference goes directly from genuine Codex to the configured endpoint. This module only
//! supplies model metadata and remembers the native identity of a replacement
//! for a still-empty desktop prewarm.
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const PROVIDER: &str = "claudex-local";
pub const MLX_URL: &str = "http://127.0.0.1:11234/v1";
const INSTRUCTIONS: &str = "You are Codex, a coding agent working with the user in a shared workspace. Follow the session and project instructions. Investigate the relevant files, make focused changes, and verify the requested behavior. Use only the tools declared for this session and their actual schemas. When work is requested, call the appropriate tools rather than ending with a description of the next step. Preserve user work, respect the configured permissions, and report the result concisely.";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub base_url: String,
    pub name: String,
    #[serde(default)]
    pub context_window: Option<u64>,
    #[serde(default)]
    pub selected_models: Vec<String>,
    #[serde(default)]
    pub env_key: Option<String>,
    /// Opt in only for servers implementing Codex custom tools and tool search.
    #[serde(default)]
    pub codex_tools: bool,
    #[serde(default)]
    pub reasoning: bool,
    pub models: Vec<Model>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Model {
    pub id: String,
    pub context_window: u64,
    pub input_modalities: Vec<String>,
}

impl Config {
    pub fn new(base_url: &str, name: &str) -> Self {
        Self {
            base_url: base_url.trim().trim_end_matches('/').into(),
            name: name.into(),
            context_window: None,
            selected_models: vec![],
            env_key: None,
            codex_tools: false,
            reasoning: false,
            models: vec![],
        }
    }

    pub fn path(root: &Path) -> PathBuf {
        root.join("local-provider.json")
    }
    pub fn catalog_path(root: &Path) -> PathBuf {
        root.join("local-provider-models.json")
    }

    pub fn read(root: &Path) -> Result<Option<Self>> {
        let bytes = match fs::read(Self::path(root)) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let config: Self =
            serde_json::from_slice(&bytes).context("Read local provider configuration")?;
        config.validate()?;
        Ok(Some(config))
    }

    fn validate(&self) -> Result<()> {
        self.validate_endpoint()?;
        if self.name.trim().is_empty() {
            bail!("Give the self-hosted provider a name");
        }
        let mut ids = std::collections::HashSet::new();
        for model in &self.models {
            if model.id.trim().is_empty()
                || !ids.insert(&model.id)
                || crate::router::native_claude_alias(&model.id)
                || model.context_window < 1024
                || model.context_window > i64::MAX as u64
                || !model.input_modalities.iter().any(|m| m == "text")
                || model
                    .input_modalities
                    .iter()
                    .any(|m| !matches!(m.as_str(), "text" | "image"))
            {
                bail!(
                    "Invalid or conflicting self-hosted model metadata: {}",
                    model.id
                );
            }
        }
        if self.models.is_empty() {
            bail!(
                "No usable chat models discovered. Set --context-window if the server does not advertise its working context window"
            );
        }
        for selected in &self.selected_models {
            if self.model(selected).is_none() {
                bail!("Server did not advertise the selected chat model: {selected}");
            }
        }
        Ok(())
    }

    fn validate_endpoint(&self) -> Result<()> {
        let url = reqwest::Url::parse(&self.base_url).context("Invalid self-hosted server URL")?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            bail!("Use an HTTP(S) base URL without credentials, query or fragment");
        }
        if self.env_key.as_deref().is_some_and(|key| {
            key.is_empty()
                || !key.bytes().enumerate().all(|(i, c)| {
                    c == b'_' || c.is_ascii_alphabetic() || i > 0 && c.is_ascii_digit()
                })
        }) {
            bail!("env_key must name an environment variable");
        }
        Ok(())
    }

    pub async fn discover(base_url: &str) -> Result<Self> {
        Self::new(base_url, "Self-hosted").refresh().await
    }

    pub async fn refresh(&self) -> Result<Self> {
        self.validate_endpoint()?;
        let mut config = self.clone();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let mut request = client.get(format!("{}/models", config.base_url));
        if let Some(key) = &config.env_key {
            request = request.bearer_auth(
                std::env::var(key)
                    .with_context(|| format!("Set {key} for the self-hosted provider"))?,
            );
        }
        let mut response = request.send().await?.error_for_status()?;
        if response
            .content_length()
            .is_some_and(|size| size > 4 * 1024 * 1024)
        {
            bail!("Self-hosted model list is too large");
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if bytes.len() + chunk.len() > 4 * 1024 * 1024 {
                bail!("Self-hosted model list is too large");
            }
            bytes.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&bytes).context("Read self-hosted model list")?;
        config.models = Self::parse_models(&value, config.context_window, &config.selected_models)?;
        config.validate()?;
        Ok(config)
    }

    fn parse_models(
        value: &Value,
        context: Option<u64>,
        selected: &[String],
    ) -> Result<Vec<Model>> {
        let rows = value["data"]
            .as_array()
            .context("Self-hosted model list has no data array")?;
        let mut models = Vec::new();
        for row in rows {
            let capabilities = row["capabilities"].as_array();
            if capabilities.is_some_and(|c| {
                !c.iter().any(|v| v == "chat") || !c.iter().any(|v| v == "tool_use")
            }) {
                continue;
            }
            let Some(id) = row["id"].as_str() else {
                continue;
            };
            if !selected.is_empty() && !selected.iter().any(|model| model == id) {
                continue;
            }
            let Some(context_window) = row["context_length"]
                .as_u64()
                .or_else(|| row["max_model_len"].as_u64())
                .or_else(|| row["meta"]["context_length"].as_u64())
                .or(context)
            else {
                continue;
            };
            let mut input_modalities: Vec<String> = row["input_modalities"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .filter(|m| matches!(*m, "text" | "image"))
                .map(str::to_owned)
                .collect();
            if row.get("input_modalities").is_none() {
                input_modalities.push("text".into());
            }
            models.push(Model {
                id: id.to_owned(),
                context_window,
                input_modalities,
            });
        }
        Ok(models)
    }

    pub fn model(&self, id: &str) -> Option<&Model> {
        self.models.iter().find(|m| m.id == id)
    }

    pub fn write(&self, root: &Path) -> Result<()> {
        self.validate()?;
        crate::install::atomic_write(
            &Self::catalog_path(root),
            &serde_json::to_vec_pretty(&self.catalog())?,
            0o600,
        )?;
        crate::install::atomic_write(&Self::path(root), &serde_json::to_vec_pretty(self)?, 0o600)
    }

    pub fn catalog(&self) -> Value {
        json!({"models":self.models.iter().map(|model| json!({
            "slug":model.id,"display_name":model.display_name(&self.name),
            "description":format!("{} · native Codex tools and permissions",self.name),
            "base_instructions":INSTRUCTIONS,"shell_type":"unified_exec",
            "visibility":"list","supported_in_api":true,"priority":100,
            "default_reasoning_level":if self.reasoning { Some("medium") } else { None },
            "supported_reasoning_levels":if self.reasoning { json!([
                {"effort":"low","description":"Lighter reasoning"},
                {"effort":"medium","description":"Balanced reasoning"},
                {"effort":"high","description":"Deeper reasoning"}]) } else { json!([]) },
            "support_verbosity":false,"supports_reasoning_summaries":false,
            "apply_patch_tool_type":null,"supports_parallel_tool_calls":true,
            "truncation_policy":{"mode":"tokens","limit":10000},
            "experimental_supported_tools":[],"input_modalities":model.input_modalities,
            "supports_search_tool":self.codex_tools,
            "tool_mode":if self.codex_tools { "code_mode_only" } else { "direct" },
            "use_responses_lite":false,
            "context_window":model.context_window,
            "effective_context_window_percent":95
        })).collect::<Vec<_>>()})
    }

    pub fn apply(&self, root: &Path, params: &mut Value) -> Result<()> {
        let object = params
            .as_object_mut()
            .context("self-hosted thread parameters must be an object")?;
        object.insert("modelProvider".into(), json!(PROVIDER));
        let config = object.entry("config").or_insert_with(|| json!({}));
        if config.is_null() {
            *config = json!({});
        }
        let config = config
            .as_object_mut()
            .context("self-hosted thread config must be an object")?;
        let mut provider = json!({
            "name":self.name,"base_url":self.base_url,"wire_api":"responses",
            "requires_openai_auth":false,"supports_websockets":false,
            "capabilities":{"remote_compaction":"unsupported"}
        });
        if let Some(key) = &self.env_key {
            provider["env_key"] = json!(key);
        }
        config.insert(format!("model_providers.{PROVIDER}"), provider);
        config.insert("model_catalog_json".into(), json!(Self::catalog_path(root)));
        config.insert("web_search".into(), json!("disabled"));
        // Ordinary Responses endpoints need not implement Codex's specialized
        // tool-search/custom-code wire types. Without compatible deferral,
        // cloud apps inline their complete catalog (over a million characters).
        // Keep native tools and configured MCP servers, scoped to this chat.
        if self.codex_tools {
            config.insert("features.code_mode_tool_search".into(), json!(true));
        } else {
            config.insert("features.apps".into(), json!(false));
        }
        Ok(())
    }
}

impl Model {
    fn display_name(&self, name: &str) -> String {
        format!(
            "{} ({name})",
            self.id.rsplit('/').next().unwrap_or(&self.id)
        )
    }

    pub fn picker(&self, name: &str, reasoning: bool) -> Value {
        json!({"id":self.id,"model":self.id,"displayName":self.display_name(name),
            "description":format!("{name} · native Codex agent"),
            "hidden":false,"modelSpecialty":null,"isDefault":false,
            "defaultReasoningEffort":"medium","supportedReasoningEfforts":if reasoning { json!([
                {"reasoningEffort":"low","description":"Lighter reasoning"},
                {"reasoningEffort":"medium","description":"Balanced reasoning"},
                {"reasoningEffort":"high","description":"Deeper reasoning"}]) } else { json!([]) },
            "inputModalities":self.input_modalities,"supportsPersonality":false,
            "additionalSpeedTiers":[],"serviceTiers":[],"defaultServiceTier":null})
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Thread {
    pub native_id: String,
    pub model: String,
    pub provider: String,
}

/// Local routing state, separate from native Codex history. The lock protects
/// simultaneous desktop app-server processes adding their own self-hosted chats.
pub struct Integration {
    pub root: PathBuf,
    pub config: Config,
    pub threads: HashMap<String, Thread>,
    state_stamp: Option<(std::time::SystemTime, u64, u64)>,
}

impl Integration {
    pub fn read(root: &Path) -> Result<Option<Self>> {
        let Some(config) = Config::read(root)? else {
            return Ok(None);
        };
        let mut integration = Self {
            root: root.to_owned(),
            config,
            threads: HashMap::new(),
            state_stamp: None,
        };
        integration.refresh_threads()?;
        Ok(Some(integration))
    }

    fn state_path(&self) -> PathBuf {
        self.root.join("local-provider-threads.json")
    }

    pub fn refresh_threads(&mut self) -> Result<()> {
        use std::os::unix::fs::MetadataExt;
        let metadata = match fs::metadata(self.state_path()) {
            Ok(metadata) => metadata,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                self.threads.clear();
                self.state_stamp = None;
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        let stamp = (metadata.modified()?, metadata.len(), metadata.ino());
        if self.state_stamp == Some(stamp) {
            return Ok(());
        }
        self.threads = serde_json::from_slice(&fs::read(self.state_path())?)
            .context("Read self-hosted thread routing")?;
        self.state_stamp = Some(stamp);
        Ok(())
    }

    pub fn remember(
        &mut self,
        public: &str,
        native: &str,
        model: &str,
        provider: &str,
    ) -> Result<()> {
        self.save_thread(
            public,
            None,
            Thread {
                native_id: native.into(),
                model: model.into(),
                provider: provider.into(),
            },
        )
    }

    pub fn replace(
        &mut self,
        public: &str,
        expected: &str,
        native: &str,
        model: &str,
        provider: &str,
    ) -> Result<()> {
        self.save_thread(
            public,
            Some(expected),
            Thread {
                native_id: native.into(),
                model: model.into(),
                provider: provider.into(),
            },
        )
    }

    fn save_thread(&mut self, public: &str, expected: Option<&str>, thread: Thread) -> Result<()> {
        let lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.root.join("local-provider-threads.lock"))?;
        lock.lock_exclusive()?;
        self.refresh_threads()?;
        let owner = self
            .threads
            .iter()
            .find(|(_, saved)| saved.native_id == thread.native_id)
            .map(|(id, _)| id.clone());
        let public = if let Some(owner) = owner.as_deref().filter(|owner| *owner != public) {
            if public != thread.native_id {
                bail!("Native self-hosted identity already belongs to another public chat");
            }
            owner
        } else {
            public
        };
        if self.threads.get(public).is_some_and(|previous| {
            previous.native_id != thread.native_id && expected != Some(previous.native_id.as_str())
        }) {
            bail!("self-hosted chat already has a different native identity");
        }
        let mut threads = self.threads.clone();
        threads.insert(public.to_owned(), thread);
        crate::install::atomic_write(&self.state_path(), &serde_json::to_vec(&threads)?, 0o600)?;
        self.threads = threads;
        self.state_stamp = None;
        Ok(())
    }

    pub fn owns(&self, public: &str) -> bool {
        self.threads
            .get(public)
            .is_some_and(|thread| thread.provider == PROVIDER)
    }

    pub fn native_id<'a>(&'a self, public: &'a str) -> &'a str {
        self.threads
            .get(public)
            .map(|thread| thread.native_id.as_str())
            .unwrap_or(public)
    }

    pub fn public_id<'a>(&'a self, native: &'a str) -> &'a str {
        self.threads
            .iter()
            .find(|(_, thread)| thread.native_id == native)
            .map(|(public, _)| public.as_str())
            .unwrap_or(native)
    }

    pub fn to_native(&self, message: &mut Value) {
        self.translate(message, false);
    }
    pub fn to_public(&self, message: &mut Value) {
        self.translate(message, true);
    }

    fn translate(&self, message: &mut Value, outbound: bool) {
        let ids: HashMap<&str, &str> = self
            .threads
            .iter()
            .filter(|(public, thread)| **public != thread.native_id)
            .map(|(public, thread)| {
                if outbound {
                    (thread.native_id.as_str(), public.as_str())
                } else {
                    (public.as_str(), thread.native_id.as_str())
                }
            })
            .collect();
        translate_ids(message, &ids, false);
    }
}

/// Only protocol identity fields are translated. Text, tool arguments, paths,
/// turn/item ids and arbitrary string content must retain their original bytes.
fn translate_ids(value: &mut Value, ids: &HashMap<&str, &str>, thread_object: bool) {
    match value {
        Value::Object(object) => {
            let is_thread = thread_object
                || object.contains_key("modelProvider")
                    && (object.contains_key("preview") || object.contains_key("createdAt"));
            for (key, value) in object {
                if matches!(
                    key.as_str(),
                    "threadId"
                        | "sessionId"
                        | "parentThreadId"
                        | "parent_thread_id"
                        | "ancestorThreadId"
                        | "forkedFromId"
                        | "attachedToThreadId"
                        | "beforeThreadId"
                ) || key == "id" && is_thread
                {
                    if let Some(replacement) = value.as_str().and_then(|id| ids.get(id)) {
                        *value = json!(replacement);
                    }
                } else if matches!(
                    key.as_str(),
                    "threadIds" | "includedThreadIds" | "excludedThreadIds"
                ) {
                    if let Some(values) = value.as_array_mut() {
                        for id in values {
                            if let Some(replacement) = id.as_str().and_then(|id| ids.get(id)) {
                                *id = json!(replacement);
                            }
                        }
                    }
                } else if !matches!(key.as_str(), "input" | "arguments" | "output" | "config") {
                    translate_ids(value, ids, key == "thread");
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                translate_ids(value, ids, false);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_uses_working_context_and_excludes_media_models() {
        let rows = json!({"data":[
            {"id":"local/vision","context_length":32768,"meta":{"context_length":1048576},
                "capabilities":["chat","tool_use"],"input_modalities":["text","image","video"]},
            {"id":"image-model","capabilities":["image"],"input_modalities":["text"]},
            {"id":"missing-context","capabilities":["chat","tool_use"],"input_modalities":["text"]}]});
        let models = Config::parse_models(&rows, None, &[]).unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].context_window, 32768);
        assert_eq!(models[0].input_modalities, ["text", "image"]);
    }

    #[test]
    fn standard_openai_discovery_accepts_explicit_context_without_mlx_metadata() {
        let data = json!({"data":[{"id":"local/qwen","object":"model","owned_by":"vllm"}]});
        assert!(Config::parse_models(&data, None, &[]).unwrap().is_empty());
        let models = Config::parse_models(&data, Some(32768), &[]).unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].context_window, 32768);
        assert_eq!(models[0].input_modalities, ["text"]);
        assert!(
            Config::parse_models(&data, Some(32768), &["another-model".into()])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn aliases_persist_and_only_translate_protocol_identities() {
        let root = tempfile::tempdir().unwrap();
        let config = Config {
            base_url: MLX_URL.into(),
            name: "MLX-Serve".into(),
            context_window: None,
            selected_models: vec![],
            env_key: None,
            codex_tools: false,
            reasoning: false,
            models: vec![Model {
                id: "local/test".into(),
                context_window: 32768,
                input_modalities: vec!["text".into()],
            }],
        };
        config.write(root.path()).unwrap();
        let mut integration = Integration::read(root.path()).unwrap().unwrap();
        integration
            .remember("public", "native", "local/test", PROVIDER)
            .unwrap();
        let integration = Integration::read(root.path()).unwrap().unwrap();
        let mut message = json!({"id":"native","params":{"threadId":"native","thread":{"id":"native","sessionId":"native","forkedFromId":"native"},
            "source":{"subAgent":{"thread_spawn":{"parent_thread_id":"native"}}},
            "turn":{"id":"native"},"input":[{"text":"native"}],"arguments":{"threadId":"native"},"path":"native"}});
        integration.to_public(&mut message);
        assert_eq!(message["params"]["threadId"], "public");
        assert_eq!(message["params"]["thread"]["id"], "public");
        assert_eq!(message["params"]["thread"]["sessionId"], "public");
        assert_eq!(message["params"]["thread"]["forkedFromId"], "public");
        assert_eq!(
            message["params"]["source"]["subAgent"]["thread_spawn"]["parent_thread_id"],
            "public"
        );
        assert_eq!(message["id"], "native");
        assert_eq!(message["params"]["turn"]["id"], "native");
        assert_eq!(message["params"]["input"][0]["text"], "native");
        assert_eq!(message["params"]["arguments"]["threadId"], "native");
        integration.to_native(&mut message);
        assert_eq!(message["params"]["threadId"], "native");
    }

    #[test]
    fn running_instances_refresh_shared_aliases_without_duplicate_native_owners() {
        let root = tempfile::tempdir().unwrap();
        let config = Config {
            base_url: MLX_URL.into(),
            name: "MLX-Serve".into(),
            context_window: None,
            selected_models: vec![],
            env_key: None,
            codex_tools: false,
            reasoning: false,
            models: vec![Model {
                id: "local/test".into(),
                context_window: 32768,
                input_modalities: vec!["text".into()],
            }],
        };
        config.write(root.path()).unwrap();
        let mut first = Integration::read(root.path()).unwrap().unwrap();
        let mut second = Integration::read(root.path()).unwrap().unwrap();
        first
            .remember("public", "native", "local/test", PROVIDER)
            .unwrap();
        second.refresh_threads().unwrap();
        assert_eq!(second.native_id("public"), "native");
        assert_eq!(second.public_id("native"), "public");
        second
            .remember("native", "native", "local/test", PROVIDER)
            .unwrap();
        first.refresh_threads().unwrap();
        assert_eq!(first.threads.len(), 1);
        assert_eq!(first.public_id("native"), "public");
    }
}
