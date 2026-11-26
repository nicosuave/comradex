use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::protocol::{self, RpcError, RpcResult, now, now_ms};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Settings {
    pub cwd: PathBuf,
    pub model: String,
    pub approval_policy: String,
    #[serde(default = "default_approvals_reviewer")]
    pub approvals_reviewer: String,
    #[serde(default)]
    pub sandbox: crate::sandbox::Policy,
    pub base_instructions: Option<String>,
    pub developer_instructions: Option<String>,
    pub effort: Option<String>,
    /// Codex service tier id; any value selects Claude fast mode.
    #[serde(default)]
    pub service_tier: Option<String>,
    #[serde(default)]
    pub dynamic_tools: Vec<crate::dynamic_tools::Tool>,
    #[serde(default)]
    pub reasoning_summary: Option<String>,
    #[serde(default)]
    pub environment_overrides: BTreeMap<String, String>,
    #[serde(default)]
    pub disabled_plugin_ids: Vec<String>,
}

impl Settings {
    pub fn new(cwd: PathBuf, model: String) -> Self {
        Self {
            cwd,
            model,
            approval_policy: "on-request".into(),
            approvals_reviewer: default_approvals_reviewer(),
            sandbox: Default::default(),
            base_instructions: None,
            developer_instructions: None,
            effort: None,
            service_tier: None,
            dynamic_tools: vec![],
            reasoning_summary: None,
            environment_overrides: BTreeMap::new(),
            disabled_plugin_ids: vec![],
        }
    }

    pub fn apply(&mut self, p: &Value) -> RpcResult<()> {
        if let Some(reviewer) = p.get("approvalsReviewer").filter(|v| !v.is_null()) {
            self.approvals_reviewer = match reviewer.as_str() {
                Some("user") => "user",
                Some("auto_review" | "guardian_subagent") => "auto_review",
                _ => {
                    return Err(RpcError::invalid(
                        "approvalsReviewer must be user, auto_review, or guardian_subagent",
                    ));
                }
            }
            .into();
        }
        if let Some(value) = p.get("environmentOverrides") {
            self.environment_overrides =
                serde_json::from_value(value.clone()).map_err(RpcError::internal)?;
        }
        if let Some(value) = p.get("disabledPluginIds").filter(|v| !v.is_null()) {
            let ids: Vec<String> =
                serde_json::from_value(value.clone()).map_err(RpcError::internal)?;
            crate::plugins::disabled_settings(&ids)?;
            self.disabled_plugin_ids = ids;
        }
        if let Some(summary) = p["summary"].as_str() {
            self.reasoning_summary = Some(summary.into());
        }
        if let Some(tools) = p.get("dynamicTools").filter(|v| !v.is_null()) {
            self.dynamic_tools = crate::dynamic_tools::parse(tools)?;
        }
        if let Some(cwd) = p["cwd"].as_str() {
            let path = Path::new(cwd);
            if !path.is_absolute() || !path.is_dir() {
                return Err(RpcError::invalid(
                    "cwd must be an existing absolute directory",
                ));
            }
            self.cwd = path.canonicalize().map_err(RpcError::internal)?;
        }
        if let Some(model) = p["model"].as_str() {
            if model.trim().is_empty() || model.starts_with('-') {
                return Err(RpcError::invalid(
                    "model must be a nonempty Claude model name or alias",
                ));
            }
            self.model = model.to_owned();
        }
        if let Some(provider) = p["modelProvider"].as_str()
            && provider != protocol::provider().id
        {
            return Err(RpcError::invalid(format!(
                "Only modelProvider={} is supported",
                protocol::provider().id
            )));
        }
        if let Some(policy) = p.get("approvalPolicy").filter(|p| !p.is_null()) {
            let Some(policy @ ("on-request" | "never")) = policy.as_str() else {
                return Err(RpcError::invalid(
                    "Claude supports on-request and never approval policies; untrusted and granular policies cannot be reproduced",
                ));
            };
            self.approval_policy = policy.to_owned();
        }
        if let Some(sandbox) = p.get("sandbox").filter(|v| !v.is_null()) {
            self.sandbox = match sandbox.as_str() {
                Some("danger-full-access") => crate::sandbox::Policy::DangerFullAccess,
                Some("workspace-write") => crate::sandbox::Policy::workspace(),
                Some("read-only") => crate::sandbox::Policy::ReadOnly {
                    network_access: false,
                },
                _ => return Err(sandbox_error()),
            };
        }
        if let Some(sandbox) = p.get("sandboxPolicy").filter(|v| !v.is_null()) {
            self.sandbox = serde_json::from_value(sandbox.clone()).map_err(|_| sandbox_error())?;
        }
        self.sandbox
            .validate()
            .map_err(|error| RpcError::invalid(error.to_string()))?;
        if let Some(effort) = p["effort"].as_str() {
            if !["none", "low", "medium", "high", "xhigh", "max"].contains(&effort) {
                return Err(RpcError::invalid(
                    "Claude effort must be low, medium, high, xhigh, or max",
                ));
            }
            self.effort = (effort != "none").then(|| effort.to_owned());
        }
        // Present-but-null clears the tier, matching Codex settings updates.
        if let Some(tier) = p.get("serviceTier") {
            self.service_tier = service_tier(tier)?;
        }
        if let Some(instructions) = p["baseInstructions"].as_str() {
            self.base_instructions = Some(instructions.to_owned());
        }
        if let Some(instructions) = p["developerInstructions"].as_str() {
            self.developer_instructions = Some(instructions.to_owned());
        }
        Ok(())
    }

    /// Codex's Full access (danger-full-access with approvalPolicy=never) runs
    /// every action without asking, which is Claude's bypassPermissions. Inside
    /// the workspace sandbox, never keeps native dontAsk: actions the sandbox
    /// and scoped permissions allow run, and anything that would prompt is
    /// denied, as Codex reports a sandbox failure instead of asking.
    pub fn native_permission_mode(&self) -> &'static str {
        if self.sandbox.is_read_only() {
            return "dontAsk";
        }
        if self.approval_policy == "never" {
            if self.sandbox.is_workspace() {
                "dontAsk"
            } else {
                "bypassPermissions"
            }
        } else if self.approvals_reviewer == "auto_review" {
            "auto"
        } else if self.sandbox.is_workspace() {
            "acceptEdits"
        } else {
            "manual"
        }
    }
}

/// Codex advertises fast mode as the `priority` tier; `fast` is its legacy
/// name. Claude has one fast mode, so either selects it and `default` clears it.
pub fn service_tier(value: &Value) -> RpcResult<Option<String>> {
    match value.as_str() {
        None if value.is_null() => Ok(None),
        Some("default") => Ok(None),
        Some(tier @ ("priority" | "fast")) => Ok(Some(tier.to_owned())),
        _ => Err(RpcError::invalid(
            "Claude supports only the priority (fast) and default service tiers",
        )),
    }
}

fn default_approvals_reviewer() -> String {
    "user".into()
}

fn sandbox_error() -> RpcError {
    RpcError::invalid(
        "Supported sandbox profiles are danger-full-access and workspace-write. Workspace commands use Claude's native OS sandbox; read-only and external sandbox profiles are unsupported.",
    )
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Record {
    pub format_version: u32,
    pub thread: Value,
    pub settings: Settings,
    pub session_id: String,
    pub has_session: bool,
    pub fork_from: Option<String>,
    /// One-shot, read-only question against the parent's warm native process.
    #[serde(default)]
    pub prediction_mode: bool,
    #[serde(default)]
    pub backend_message_id: Option<String>,
    #[serde(default)]
    pub turn_anchors: BTreeMap<String, crate::history_recovery::NativeAnchor>,
    #[serde(default)]
    pub tracks_turn_anchors: bool,
    #[serde(default)]
    pub attachments: Vec<crate::attachments::Attachment>,
    #[serde(default)]
    pub session_grants: Vec<crate::approvals::SessionGrant>,
    /// Claude agent id -> child thread id, so a continued agent keeps its thread.
    #[serde(default)]
    pub agents: BTreeMap<String, String>,
    pub archived: bool,
    pub turns: Vec<Value>,
    #[serde(default)]
    pub queued_submissions: Vec<Value>,
    pub item_times: BTreeMap<String, (u64, Option<u64>)>,
    pub token_usage: Value,
    #[serde(default)]
    pub tracks_request_usage: bool,
    /// Recent automatic review denials: {reviewId, tool, input, action}.
    #[serde(default)]
    pub auto_review_denials: Vec<Value>,
    /// Denied actions the user then approved; shown to later reviews.
    #[serde(default)]
    pub approved_actions: Vec<Value>,
    /// Text delivered ahead of the next turn's input.
    #[serde(default)]
    pub pending_context: Vec<String>,
    /// Order within `thread.section`; lower positions list first.
    #[serde(default)]
    pub section_position: Option<i64>,
    /// Codex's durable read receipt: `None` is read, and an empty turn id
    /// marks the whole thread unread.
    #[serde(default)]
    pub first_unread_turn: Option<String>,
    /// Guards read-state edits against results the editor has not seen.
    #[serde(default)]
    pub read_revision: Option<String>,
    /// The last terminal result published as needing attention.
    #[serde(default)]
    pub last_published_turn: Option<String>,
    /// The source of a persistent side chat (`thread/fork` `attachToSource`),
    /// which ordinary listings hide.
    #[serde(default)]
    pub attached_to: Option<String>,
}

impl Record {
    pub fn validate_read_only_helper(&self) -> RpcResult<()> {
        if self.settings.sandbox.is_read_only()
            && (!self.ephemeral()
                || self.settings.approval_policy != "never"
                || !self.settings.dynamic_tools.is_empty()
                || !self.settings.environment_overrides.is_empty()
                || self.fork_from.is_some())
        {
            return Err(RpcError::invalid(
                "Read-only Claude helpers require ephemeral=true, approvalPolicy=never, no dynamic tools or environment overrides, and no source session; general read-only chats are unsupported",
            ));
        }
        Ok(())
    }

    pub fn apply_settings(&mut self, params: &Value) -> RpcResult<()> {
        let was_read_only = self.settings.sandbox.is_read_only();
        self.settings.apply(params)?;
        if was_read_only != self.settings.sandbox.is_read_only() {
            return Err(RpcError::invalid(
                "Read-only helper mode cannot be changed on an existing thread",
            ));
        }
        self.validate_read_only_helper()
    }

    pub fn new(settings: Settings, path: PathBuf, p: &Value, originator: &str) -> Self {
        Self::with_id(protocol::id(), settings, path, p, originator)
    }
    pub fn with_id(
        thread_id: String,
        settings: Settings,
        path: PathBuf,
        p: &Value,
        originator: &str,
    ) -> Self {
        let ephemeral = p["ephemeral"].as_bool().unwrap_or(false);
        let timestamp = now();
        let thread = json!({
            "id": thread_id, "sessionId": thread_id, "forkedFromId": null, "parentThreadId": null,
            "environments": null, "extra": null, "preview": "", "ephemeral": ephemeral,
            "section": null, "sectionEnteredAt": null, "projectId": p["projectId"],
            "historyMode": p["historyMode"].as_str().unwrap_or("legacy"),
            "modelProvider": protocol::provider().id, "model": settings.model, "reasoningEffort": settings.effort,
            "createdAt": timestamp, "updatedAt": timestamp, "recencyAt": timestamp, "status": {"type": "idle"},
            "path": if ephemeral { None } else { Some(path.join(format!("{thread_id}.json"))) },
            "cwd": settings.cwd, "cliVersion": protocol::VERSION, "originator": originator,
            "source": "appServer", "canAcceptDirectInput": true, "threadSource": p["threadSource"],
            "agentNickname": null, "agentRole": null, "gitInfo": null, "name": null,
            "daybreakEnabled": p["daybreakEnabled"], "turns": []
        });
        Self {
            format_version: 1,
            thread,
            settings,
            session_id: uuid::Uuid::new_v4().to_string(),
            has_session: false,
            fork_from: None,
            prediction_mode: false,
            backend_message_id: None,
            turn_anchors: BTreeMap::new(),
            tracks_turn_anchors: true,
            attachments: vec![],
            session_grants: vec![],
            agents: BTreeMap::new(),
            archived: false,
            turns: vec![],
            queued_submissions: vec![],
            item_times: BTreeMap::new(),
            token_usage: json!({"total": protocol::usage(0,0,0,0), "last": protocol::usage(0,0,0,0), "modelContextWindow": null}),
            tracks_request_usage: true,
            auto_review_denials: vec![],
            approved_actions: vec![],
            pending_context: vec![],
            section_position: None,
            first_unread_turn: None,
            read_revision: None,
            last_published_turn: None,
            attached_to: None,
        }
    }
    pub fn id(&self) -> &str {
        self.thread["id"].as_str().unwrap()
    }
    pub fn recover_history(
        &mut self,
        boundary: &crate::history_recovery::HistoryBoundary,
    ) -> RpcResult<()> {
        use crate::history_recovery::{NativeAnchor, plan_prefix};
        // A new ephemeral fork can be initialized from its durable parent's
        // anchors. The server rejects recovery of existing ephemeral history.
        let latest = self
            .backend_message_id
            .as_ref()
            .filter(|_| !self.tracks_turn_anchors)
            .map(|message_id| NativeAnchor {
                session_id: if self.has_session {
                    self.session_id.clone()
                } else {
                    self.fork_from
                        .clone()
                        .unwrap_or_else(|| self.session_id.clone())
                },
                message_id: message_id.clone(),
            });
        let plan = plan_prefix(&self.turns, &self.turn_anchors, latest.as_ref(), boundary)?;
        self.turns.truncate(plan.retained_len);
        self.turn_anchors
            .retain(|id, _| self.turns.iter().any(|turn| turn["id"] == *id));
        self.item_times.retain(|id, _| {
            self.turns.iter().any(|turn| {
                turn["items"]
                    .as_array()
                    .is_some_and(|items| items.iter().any(|item| item["id"] == *id))
            })
        });
        self.session_id = uuid::Uuid::new_v4().to_string();
        self.has_session = false;
        self.fork_from = plan.anchor.as_ref().map(|anchor| anchor.session_id.clone());
        self.backend_message_id = plan.anchor.map(|anchor| anchor.message_id);
        self.thread["preview"] = json!(
            self.turns
                .iter()
                .flat_map(|turn| turn["items"].as_array().into_iter().flatten())
                .find(|item| item["type"] == "userMessage")
                .and_then(|item| item["content"].as_array())
                .map(|content| content
                    .iter()
                    .filter_map(|part| part["text"].as_str())
                    .collect::<Vec<_>>()
                    .join(" "))
                .unwrap_or_default()
        );
        // Removed native usage cannot be reconstructed from historical display
        // items. Reset the counter rather than retaining removed-turn totals.
        self.token_usage = json!({"total":protocol::usage(0,0,0,0),"last":protocol::usage(0,0,0,0),"modelContextWindow":null});
        self.touch();
        Ok(())
    }
    pub fn ephemeral(&self) -> bool {
        self.thread["ephemeral"].as_bool().unwrap_or(false)
    }
    /// The turns with each item's recorded lifecycle times.
    pub fn timed_turns(&self) -> Vec<Value> {
        self.turns
            .iter()
            .map(|turn| self.timed_turn(turn))
            .collect()
    }
    pub fn timed_turn(&self, turn: &Value) -> Value {
        let mut turn = turn.clone();
        if let Some(items) = turn["items"].as_array_mut() {
            for item in items {
                *item = self.timed_item(item);
            }
        }
        turn
    }
    pub fn view(&self, include_turns: bool, loaded: bool) -> Value {
        let mut thread = self.thread.clone();
        thread["attachedToThreadId"] = json!(self.attached_to);
        thread["turns"] = if include_turns {
            json!(self.timed_turns())
        } else {
            json!([])
        };
        if !loaded {
            thread["status"] = json!({"type": "notLoaded"});
            thread["canAcceptDirectInput"] = Value::Null;
        }
        thread
    }
    pub fn touch(&mut self) {
        self.thread["updatedAt"] = json!(now());
        self.thread["recencyAt"] = self.thread["updatedAt"].clone();
        self.thread["model"] = json!(self.settings.model);
        self.thread["reasoningEffort"] = json!(self.settings.effort);
        self.thread["cwd"] = json!(self.settings.cwd);
    }

    /// Patch desktop-owned metadata without changing execution settings.
    pub fn patch_metadata(&mut self, params: &Value) -> RpcResult<()> {
        // Validate before mutating any field, including project placement.
        if let Some(git) = params["gitInfo"].as_object() {
            for (key, value) in git {
                if !["sha", "branch", "originUrl"].contains(&key.as_str())
                    || !(value.is_null() || value.as_str().is_some_and(|s| !s.trim().is_empty()))
                {
                    return Err(RpcError::invalid(
                        "Git metadata fields must be null or nonempty strings",
                    ));
                }
            }
        }
        if let Some(project) = params["projectId"].as_str() {
            self.thread["projectId"] = if project.is_empty() {
                Value::Null
            } else {
                json!(project)
            };
        }
        if let Some(enabled) = params["daybreakEnabled"].as_bool() {
            self.thread["daybreakEnabled"] = json!(enabled);
        }
        if let Some(git) = params["gitInfo"].as_object()
            && !git.is_empty()
        {
            if !self.thread["gitInfo"].is_object() {
                self.thread["gitInfo"] = json!({"sha":null,"branch":null,"originUrl":null});
            }
            for (key, value) in git {
                self.thread["gitInfo"][key] = value.clone();
            }
        }
        Ok(())
    }

    pub fn thread_settings(&self) -> Value {
        json!({
            "model":self.settings.model, "modelProvider":protocol::provider().id,
            "cwd":self.settings.cwd, "approvalPolicy":self.settings.approval_policy,
            "approvalsReviewer":self.settings.approvals_reviewer,
            "sandboxPolicy":self.settings.sandbox, "activePermissionProfile":null,
            "disabledPluginIds":self.settings.disabled_plugin_ids, "effort":self.settings.effort,
            "summary":self.settings.reasoning_summary, "serviceTier":self.settings.service_tier, "personality":null,
            "multiAgentMode":"explicitRequestOnly",
            "collaborationMode":{"mode":"default","settings":{
                "model":self.settings.model,"reasoning_effort":self.settings.effort,
                "developer_instructions":self.settings.developer_instructions
            }}
        })
    }
    pub fn response(&self, include_turns: bool, resume: bool) -> Value {
        let mut response = json!({"thread": self.view(include_turns, true), "model": self.settings.model,
            "modelProvider": protocol::provider().id, "serviceTier": self.settings.service_tier, "disabledPluginIds": self.settings.disabled_plugin_ids,
            "cwd": self.settings.cwd, "runtimeWorkspaceRoots": [self.settings.cwd], "instructionSources": [],
            "approvalPolicy": self.settings.approval_policy, "approvalsReviewer": self.settings.approvals_reviewer,
            "sandbox": self.settings.sandbox, "activePermissionProfile": null,
            "reasoningEffort": self.settings.effort, "multiAgentMode": "explicitRequestOnly"});
        if resume {
            response["collaborationMode"] = Value::Null;
            response["initialTurnsPage"] = Value::Null;
            response["turnsBackwardsCursor"] = Value::Null;
            response["itemsBackwardsCursor"] = Value::Null;
        }
        response
    }
    /// As in Codex, only durable user threads have read state; subagent and
    /// other-source threads have no notification policy to apply.
    pub fn read_state(&self) -> Option<Value> {
        let user = matches!(self.thread["threadSource"].as_str(), None | Some("user"));
        if self.ephemeral() || crate::subagents::is_child(self) || !user {
            return None;
        }
        let first_unread = self.first_unread_turn.as_deref().map(|turn| match turn {
            "" => json!({"type": "threadStart"}),
            turn => json!({"type": "turn", "turnId": turn}),
        });
        // An unedited thread's revision is bound to it, so it cannot edit another.
        let revision = self
            .read_revision
            .clone()
            .unwrap_or_else(|| format!("initial:{}", self.id()));
        Some(json!({"firstUnread": first_unread, "revision": revision}))
    }
    /// Marks a newly delivered terminal result as needing attention, once.
    /// Every new result changes the revision, so older acknowledgements fail.
    pub fn publish_attention(&mut self, turn_id: &str) -> bool {
        if self.read_state().is_none() || self.last_published_turn.as_deref() == Some(turn_id) {
            return false;
        }
        self.first_unread_turn
            .get_or_insert_with(|| turn_id.to_owned());
        self.read_revision = Some(uuid::Uuid::new_v4().to_string());
        self.last_published_turn = Some(turn_id.to_owned());
        true
    }
    /// Applies a read or unread mark made against `expected_revision`. A stale
    /// edit returns the current state and is never retried.
    pub fn mark_read_state(
        &mut self,
        expected_revision: &str,
        unread: bool,
    ) -> Result<Value, Value> {
        let current = self.read_state().ok_or(Value::Null)?;
        if current["revision"] != expected_revision {
            return Err(current);
        }
        self.first_unread_turn = unread.then(String::new);
        self.read_revision = Some(uuid::Uuid::new_v4().to_string());
        Ok(self.read_state().unwrap())
    }
    /// As in Codex 0.166, an item carries its recorded lifecycle times.
    pub fn timed_item(&self, item: &Value) -> Value {
        let mut item = item.clone();
        let times = item["id"].as_str().and_then(|id| self.item_times.get(id));
        item["startedAtMs"] = json!(times.map(|t| t.0));
        item["completedAtMs"] = json!(times.and_then(|t| t.1));
        item
    }
    /// A `ThreadItemEntry`: the timed item and its turn.
    pub fn item_entry(&self, turn_id: &Value, item: &Value) -> Value {
        let item = self.timed_item(item);
        json!({"turnId": turn_id, "startedAtMs": item["startedAtMs"],
            "completedAtMs": item["completedAtMs"], "item": item})
    }
    pub fn update_item(&mut self, item: Value, completed: bool) {
        let Some(turn) = self.turns.last_mut() else {
            return;
        };
        let id = item["id"].as_str().unwrap().to_owned();
        let times = self
            .item_times
            .entry(id.clone())
            .or_insert((now_ms(), None));
        if completed {
            times.1 = Some(now_ms());
        }
        let items = turn["items"].as_array_mut().unwrap();
        match items.iter_mut().find(|v| v["id"] == id) {
            Some(existing) => *existing = item,
            None => items.push(item),
        }
    }
}

/// A process-exclusive state directory. Each thread is replaced atomically, with private permissions.
pub struct Store {
    root: PathBuf,
    writer: Option<Writer>,
    write_failures: Option<tokio::sync::mpsc::UnboundedReceiver<WriteFailure>>,
    _lock: File,
}

/// Thread records are written in order by one background thread. The facade
/// serves every thread from one event loop, which must not wait on the disk
/// (each write ends with full flushes) unless the caller needs the outcome.
struct Writer {
    jobs: std::sync::mpsc::Sender<WriteJob>,
    thread: std::thread::JoinHandle<()>,
}

struct WriteJob {
    path: PathBuf,
    /// The record's new contents, or None to remove it.
    bytes: Option<Vec<u8>>,
    done: Option<std::sync::mpsc::Sender<Result<(), String>>>,
}

/// A deferred thread record write that failed.
pub struct WriteFailure {
    pub thread_id: String,
    pub error: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ModelDefaults {
    pub model: String,
    pub effort: Option<String>,
    #[serde(default)]
    pub service_tier: Option<String>,
    pub version: String,
}

impl Store {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        let root = root.as_ref().canonicalize()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        }
        let lock = private_open(&root.join("server.lock"), false)?;
        lock.try_lock_exclusive()
            .context("Another facade process is using this state directory")?;
        fs::create_dir_all(root.join("threads"))?;
        let (jobs, queue) = std::sync::mpsc::channel();
        let (failures, write_failures) = tokio::sync::mpsc::unbounded_channel();
        let thread = std::thread::Builder::new()
            .name("thread-record-writer".into())
            .spawn(move || write_records(queue, failures))?;
        Ok(Self {
            root,
            writer: Some(Writer { jobs, thread }),
            write_failures: Some(write_failures),
            _lock: lock,
        })
    }
    /// Failures of deferred writes, for the one owner that reports them.
    pub fn take_write_failures(
        &mut self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<WriteFailure>> {
        self.write_failures.take()
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn threads_dir(&self) -> PathBuf {
        self.root.join("threads")
    }
    pub fn defaults_path(&self) -> PathBuf {
        self.root.join("model-defaults.json")
    }
    pub fn load_defaults(&self, model: String) -> Result<ModelDefaults> {
        match File::open(self.defaults_path()) {
            Ok(file) => Ok(serde_json::from_reader(file)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(ModelDefaults {
                model,
                effort: None,
                service_tier: None,
                version: "0".into(),
            }),
            Err(error) => Err(error.into()),
        }
    }
    pub fn save_defaults(&self, defaults: &ModelDefaults) -> Result<()> {
        self.write_json(&self.defaults_path(), defaults)
    }
    /// Write a record and wait until it is durable.
    pub fn save(&self, record: &Record) -> Result<()> {
        match self.record_job(record)? {
            Some(job) => self.queue_and_wait(job),
            None => Ok(()),
        }
    }
    /// Remove a thread's record and wait until the removal is durable. It is
    /// ordered with queued writes, so an earlier deferred save cannot restore it.
    pub fn delete(&self, thread_id: &str) -> Result<()> {
        uuid::Uuid::parse_str(thread_id).context("Invalid persisted thread id")?;
        self.queue_and_wait(WriteJob {
            path: self.threads_dir().join(format!("{thread_id}.json")),
            bytes: None,
            done: None,
        })
    }
    fn queue_and_wait(&self, mut job: WriteJob) -> Result<()> {
        let (done, outcome) = std::sync::mpsc::channel();
        job.done = Some(done);
        self.queue(job)?;
        outcome
            .recv()
            .context("The thread record writer stopped")?
            .map_err(anyhow::Error::msg)
    }
    /// Queue a record write without waiting for it. Writes stay in order with
    /// `save`, and a failure arrives through `take_write_failures`.
    pub fn save_deferred(&self, record: &Record) -> Result<()> {
        match self.record_job(record)? {
            Some(job) => self.queue(job),
            None => Ok(()),
        }
    }
    fn record_job(&self, record: &Record) -> Result<Option<WriteJob>> {
        if record.ephemeral() {
            return Ok(None);
        }
        let id = record.id();
        uuid::Uuid::parse_str(id).context("Invalid persisted thread id")?;
        let mut bytes = serde_json::to_vec(record)?;
        bytes.push(b'\n');
        Ok(Some(WriteJob {
            path: self.threads_dir().join(format!("{id}.json")),
            bytes: Some(bytes),
            done: None,
        }))
    }
    fn queue(&self, job: WriteJob) -> Result<()> {
        self.writer
            .as_ref()
            .context("The thread record writer stopped")?
            .jobs
            .send(job)
            .map_err(|_| anyhow::anyhow!("The thread record writer stopped"))
    }
    fn write_json(&self, path: &Path, value: &impl Serialize) -> Result<()> {
        let mut bytes = serde_json::to_vec(value)?;
        bytes.push(b'\n');
        write_atomic(path, &bytes)
    }
    pub fn load(&self) -> Result<BTreeMap<String, Record>> {
        let mut records = BTreeMap::new();
        for entry in fs::read_dir(self.threads_dir())? {
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            // serde_json does not buffer Read inputs: parsing a File directly
            // makes a syscall per byte and delays readiness for large histories.
            let mut record: Record = serde_json::from_slice(&fs::read(&path)?)
                .with_context(|| format!("Cannot load {}", path.display()))?;
            if !record.tracks_request_usage {
                // Older records stored an entire turn's billing total as context.
                // Preserve billing; leave occupancy unknown until a native request.
                record.token_usage["last"] = protocol::usage(0, 0, 0, 0);
                record.token_usage["modelContextWindow"] = Value::Null;
            }
            if record.format_version != 1 {
                bail!("Unsupported state format in {}", path.display());
            }
            if path.file_stem().and_then(|v| v.to_str()) != Some(record.id()) {
                bail!("Thread id does not match state filename");
            }
            uuid::Uuid::parse_str(record.id())?;
            let mut recovered = false;
            for turn in &mut record.turns {
                if turn["status"] == "inProgress" {
                    turn["status"] = json!("interrupted");
                    turn["completedAt"] = json!(now());
                    turn["error"] = json!({"message": "Facade stopped while this turn was active", "codexErrorInfo": null, "additionalDetails": null, "misalignment": null});
                    for item in turn["items"].as_array_mut().unwrap() {
                        if item["status"] == "inProgress" {
                            item["status"] = json!("failed");
                        }
                    }
                    recovered = true;
                }
            }
            record.thread["status"] = json!({"type": "idle"});
            if recovered {
                self.save(&record)?;
            }
            records.insert(record.id().to_owned(), record);
        }
        Ok(records)
    }
}

impl Drop for Store {
    /// Finish queued writes before the state directory lock is released.
    fn drop(&mut self) {
        if let Some(Writer { jobs, thread }) = self.writer.take() {
            drop(jobs);
            let _ = thread.join();
        }
    }
}

fn write_records(
    queue: std::sync::mpsc::Receiver<WriteJob>,
    failures: tokio::sync::mpsc::UnboundedSender<WriteFailure>,
) {
    while let Ok(first) = queue.recv() {
        // Only the newest queued content of each record needs to be written.
        let mut batch: Vec<WriteJob> = Vec::new();
        let mut waiting: Vec<Vec<std::sync::mpsc::Sender<Result<(), String>>>> = Vec::new();
        for job in std::iter::once(first).chain(queue.try_iter()) {
            match batch.iter().position(|queued| queued.path == job.path) {
                Some(index) => {
                    batch[index].bytes = job.bytes;
                    waiting[index].extend(job.done);
                }
                None => {
                    waiting.push(job.done.into_iter().collect());
                    batch.push(WriteJob { done: None, ..job });
                }
            }
        }
        for (job, waiting) in batch.into_iter().zip(waiting) {
            let outcome = match &job.bytes {
                Some(bytes) => write_atomic(&job.path, bytes),
                None => remove_durably(&job.path),
            }
            .map_err(|error| format!("{error:#}"));
            if let Err(error) = &outcome
                && waiting.is_empty()
            {
                let thread_id = job
                    .path
                    .file_stem()
                    .map(|stem| stem.to_string_lossy().into_owned())
                    .unwrap_or_default();
                eprintln!("Cannot save thread {thread_id}: {error}");
                let _ = failures.send(WriteFailure {
                    thread_id,
                    error: error.clone(),
                });
            }
            for done in waiting {
                let _ = done.send(outcome.clone());
            }
        }
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().context("State file needs a parent")?;
    let temp = dir.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = private_open(&temp, true)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        #[cfg(unix)]
        File::open(dir)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn remove_durably(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    #[cfg(unix)]
    File::open(path.parent().context("State file needs a parent")?)?.sync_all()?;
    Ok(())
}

fn private_open(path: &Path, exclusive: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if exclusive {
        options.create_new(true);
    } else {
        options.create(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    Ok(options.open(path)?)
}
