//! Shared, parent-scoped MCP delegation. Execution stays in the native harnesses.
use std::{collections::BTreeMap, fs, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{io::AsyncWriteExt, sync::mpsc};
use tokio_util::{
    codec::{FramedRead, LinesCodec},
    sync::CancellationToken,
};

pub const NAMESPACE: &str = "claudex_agents";
pub const MAX_DEPTH: u64 = 8;
pub const MAX_RUNNING: usize = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Harness {
    Codex,
    Claude,
}

fn tool(name: &str, description: &str, properties: Value, required: &[&str]) -> Value {
    json!({"name":name,"description":description,"inputSchema":{
        "type":"object","properties":properties,"required":required,"additionalProperties":false}})
}

/// One catalog for genuine stdio MCP and Claude's native SDK MCP transport.
pub fn catalog() -> Vec<Value> {
    let text = json!({"type":"string","minLength":1,"maxLength":100_000});
    let target = json!({"type":"string","minLength":1});
    vec![
        tool(
            "spawn_agent",
            "Start a visible child agent in Codex or Claude. Supply a self-contained task briefing; history is not forked. Workspace and native approvals are inherited. Returns agent_id immediately; use wait_agent for the result.",
            json!({"harness":{"type":"string","enum":["codex","claude"]},"model":target,"message":text,
                "name":{"type":"string","minLength":1,"maxLength":120},
                "effort":{"type":"string","enum":["low","medium","high","xhigh","max","ultra"]}}),
            &["harness", "model", "message"],
        ),
        tool(
            "list_agents",
            "List this task's delegated agents and their current status and final reports.",
            json!({}),
            &[],
        ),
        tool(
            "wait_agent",
            "Wait for a delegated agent to finish or fail. A timeout returns its current state and does not cancel it. Maximum timeout is 60 seconds.",
            json!({"agent_id":target,"timeout_ms":{"type":"integer","minimum":0,"maximum":60_000}}),
            &["agent_id"],
        ),
        tool(
            "send_message",
            "Steer a running delegated agent without interrupting it, or queue a message for its next follow-up. Does not start an idle agent.",
            json!({"agent_id":target,"message":text}),
            &["agent_id", "message"],
        ),
        tool(
            "followup_task",
            "Start a new task in an idle delegated agent's existing native session. If it is running, steer its current task. Preserves its harness, model and permissions.",
            json!({"agent_id":target,"message":text}),
            &["agent_id", "message"],
        ),
        tool(
            "interrupt_agent",
            "Interrupt a delegated agent and its running descendants through their native turn controls. Does not delete its thread or edits.",
            json!({"agent_id":target}),
            &["agent_id"],
        ),
    ]
}

pub fn dynamic_spec() -> Value {
    let tools: Vec<Value> = catalog()
        .into_iter()
        .map(|mut t| {
            t["type"] = json!("function");
            t
        })
        .collect();
    json!({"type":"namespace","name":NAMESPACE,"description":"Cross-harness child agents with native workspace permissions.","tools":tools})
}

pub fn validate(name: &str, arguments: &Value) -> Result<()> {
    let spec = catalog()
        .into_iter()
        .find(|t| t["name"] == name)
        .context("Unknown delegation tool")?;
    let validator = jsonschema::validator_for(&spec["inputSchema"])?;
    if !validator.is_valid(arguments) {
        bail!("Delegation arguments do not match the tool schema");
    }
    Ok(())
}

/// Translate an effective native snapshot, never guessed defaults or model arguments.
pub fn child_params(
    snapshot: &Value,
    harness: Harness,
    model: &str,
    effort: Option<&str>,
) -> Result<Value> {
    let cwd = snapshot["cwd"]
        .as_str()
        .context("Parent has no effective cwd")?;
    let policy = snapshot
        .get("approvalPolicy")
        .filter(|v| !v.is_null())
        .context("Parent has no effective approval policy")?;
    let reviewer = snapshot
        .get("approvalsReviewer")
        .filter(|v| !v.is_null())
        .context("Parent has no effective approval reviewer")?;
    let sandbox = snapshot
        .get("sandbox")
        .filter(|v| v.is_object())
        .context("Parent has no effective sandbox policy")?;
    if let Some(profile) = snapshot["activePermissionProfile"]["id"].as_str()
        && ![":workspace", ":danger-full-access", ":read-only"].contains(&profile)
    {
        bail!("Custom permission profile {profile} cannot be inherited across harnesses");
    }
    if snapshot["thread"]["environments"]
        .as_array()
        .is_some_and(|environments| {
            environments
                .iter()
                .any(|environment| environment["environmentId"] != "local")
        })
    {
        bail!("Cross-harness agents require the parent's local execution environment");
    }
    let mut params = json!({"cwd":cwd,"model":model,"approvalPolicy":policy,"approvalsReviewer":reviewer,
        "ephemeral":snapshot["thread"]["ephemeral"].as_bool().unwrap_or(false),
        "projectId":snapshot["thread"]["projectId"],"historyMode":snapshot["thread"]["historyMode"],
        "disabledPluginIds":snapshot["disabledPluginIds"].as_array().cloned().unwrap_or_default(),
        "developerInstructions":"You are a delegated child agent. Complete the supplied task within its scope, preserve user work, and return a concise result with evidence and validation. Use claudex_agents for cross-harness delegation. Do not commit, push, deploy or send messages to others unless the task explicitly authorizes it."});
    match harness {
        Harness::Claude => {
            if ![json!("on-request"), json!("never")].contains(policy) {
                bail!("Claude cannot reproduce the parent's approval policy");
            }
            let native: crate::sandbox::Policy = serde_json::from_value(sandbox.clone())
                .context("Claude cannot reproduce the parent's sandbox policy")?;
            native.validate()?;
            if native.is_read_only() {
                bail!(
                    "Read-only Claude sessions are limited to standalone ephemeral helpers, not delegated agents"
                );
            }
            params["sandboxPolicy"] = sandbox.clone();
            if let Some(effort) = effort {
                params["effort"] = json!(effort);
            }
        }
        Harness::Codex => {
            let mode = match sandbox["type"].as_str() {
                Some("dangerFullAccess") => "danger-full-access",
                Some("workspaceWrite") => "workspace-write",
                Some("readOnly") => "read-only",
                _ => bail!("Codex cannot reproduce the parent's sandbox policy"),
            };
            params["sandbox"] = json!(mode);
            if mode == "workspace-write" {
                params["config"] = json!({"sandbox_workspace_write":{
                    "writable_roots":sandbox["writableRoots"].as_array().cloned().unwrap_or_default(),
                    "network_access":sandbox["networkAccess"].as_bool().unwrap_or(false),
                    "exclude_slash_tmp":sandbox["excludeSlashTmp"].as_bool().unwrap_or(false),
                    "exclude_tmpdir_env_var":sandbox["excludeTmpdirEnvVar"].as_bool().unwrap_or(false)}});
            } else if mode == "read-only" && sandbox["networkAccess"] == true {
                bail!("Read-only network access cannot be inherited by this Codex start contract");
            }
            if let Some(effort) = effort {
                params["config"]["model_reasoning_effort"] = json!(effort);
            }
            if let Some(roots) = snapshot.get("runtimeWorkspaceRoots") {
                params["runtimeWorkspaceRoots"] = roots.clone();
            }
        }
    }
    Ok(params)
}

/// Check the backend's effective result before submitting any child task. A
/// machine policy may reject or alter requested settings; requests are not proof.
pub fn permissions(snapshot: &Value) -> Result<Value> {
    let policy = snapshot
        .get("approvalPolicy")
        .context("Missing effective approval policy")?;
    let reviewer = snapshot["approvalsReviewer"]
        .as_str()
        .context("Missing effective approval reviewer")?;
    let reviewer = if reviewer == "guardian_subagent" {
        "auto_review"
    } else {
        reviewer
    };
    let cwd = std::path::Path::new(snapshot["cwd"].as_str().context("Missing effective cwd")?)
        .canonicalize()?;
    let sandbox = &snapshot["sandbox"];
    let sandbox = match sandbox["type"].as_str() {
        Some("dangerFullAccess") => json!({"type":"dangerFullAccess"}),
        Some("workspaceWrite") => {
            let mut roots = sandbox["writableRoots"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            for root in &mut roots {
                if let Some(path) = root.as_str() {
                    *root = json!(std::path::Path::new(path).canonicalize()?);
                }
            }
            // Both harnesses always grant cwd and omit it from reported roots,
            // while desktop requests list it explicitly.
            roots.push(json!(cwd));
            roots.sort_by_key(Value::to_string);
            roots.dedup();
            json!({"type":"workspaceWrite","writableRoots":roots,
                "networkAccess":sandbox["networkAccess"].as_bool().unwrap_or(false),
                "excludeSlashTmp":sandbox["excludeSlashTmp"].as_bool().unwrap_or(false),
                "excludeTmpdirEnvVar":sandbox["excludeTmpdirEnvVar"].as_bool().unwrap_or(false)})
        }
        Some("readOnly") => {
            json!({"type":"readOnly","networkAccess":sandbox["networkAccess"].as_bool().unwrap_or(false)})
        }
        _ => bail!("Missing or unsupported effective sandbox policy"),
    };
    Ok(json!({"cwd":cwd,"approvalPolicy":policy,
        "approvalsReviewer":reviewer,"sandbox":sandbox}))
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: String,
    pub parent: String,
    pub root: String,
    pub harness: Harness,
    pub name: String,
    pub depth: u64,
    pub thread: Value,
    pub turn_id: Option<String>,
    pub status: String,
    pub report: Option<String>,
    pub queued: Vec<String>,
    pub spawn_parent_turn: String,
    pub spawn_item: Value,
    pub owner_pid: u32,
    pub archived: bool,
}

impl Agent {
    pub fn state(&self) -> Value {
        json!({"agent_id":self.id,"parent_thread_id":self.parent,"harness":self.harness,
            "name":self.name,"status":self.status,"report":self.report})
    }
    pub fn running(&self) -> bool {
        matches!(
            self.status.as_str(),
            "running" | "starting" | "interrupting"
        )
    }
    pub fn decorate(&self, thread: &mut Value) {
        thread["parentThreadId"] = json!(self.parent);
        thread["source"] = json!({"subAgent":{"thread_spawn":{"parent_thread_id":self.parent,
            "depth":self.depth,"agent_path":null,"agent_nickname":self.name,"agent_role":"delegate"}}});
        thread["threadSource"] = json!("subagent");
        thread["agentNickname"] = json!(self.name);
        thread["agentRole"] = json!("delegate");
        thread["name"] = json!(self.name);
        // These are real native sessions, unlike Claude's embedded Agent transcripts.
        thread["canAcceptDirectInput"] = json!(true);
    }
}

#[derive(Default)]
pub struct Registry {
    pub agents: BTreeMap<String, Agent>,
    directory: Option<PathBuf>,
}

impl Registry {
    pub fn open(directory: PathBuf) -> Result<Self> {
        fs::create_dir_all(&directory)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
        }
        let mut registry = Self {
            directory: Some(directory.clone()),
            ..Self::default()
        };
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.extension().and_then(|v| v.to_str()) != Some("json") {
                continue;
            }
            let agent: Agent = serde_json::from_slice(&fs::read(&path)?)?;
            if uuid::Uuid::parse_str(&agent.id).is_err()
                || path.file_stem().and_then(|v| v.to_str()) != Some(&agent.id)
            {
                bail!("Invalid persisted delegation identity");
            }
            registry.agents.insert(agent.id.clone(), agent);
        }
        Ok(registry)
    }
    pub fn save(&self, agent: &Agent) -> Result<()> {
        if agent.thread["ephemeral"] == true {
            return Ok(());
        }
        let Some(directory) = &self.directory else {
            return Ok(());
        };
        let path = directory.join(format!("{}.json", agent.id));
        let temporary = directory.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
        let write = || -> Result<()> {
            use std::io::Write;
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
            }
            let mut file = options.open(&temporary)?;
            file.write_all(&serde_json::to_vec(agent)?)?;
            file.sync_all()?;
            fs::rename(&temporary, path)?;
            Ok(())
        };
        let result = write();
        if result.is_err() {
            let _ = fs::remove_file(temporary);
        }
        result
    }
    /// Forget a delegated agent whose thread was deleted.
    pub fn remove(&mut self, id: &str) -> Result<()> {
        if self.agents.remove(id).is_none() {
            return Ok(());
        }
        let Some(directory) = &self.directory else {
            return Ok(());
        };
        match fs::remove_file(directory.join(format!("{id}.json"))) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error.into()),
            _ => Ok(()),
        }
    }
    pub fn root<'a>(&'a self, caller: &'a str) -> &'a str {
        self.agents.get(caller).map_or(caller, |agent| &agent.root)
    }
    pub fn target(&self, caller: &str, id: &str) -> Result<&Agent> {
        let agent = self.agents.get(id).context("Unknown delegated agent")?;
        if caller == id || self.root(caller) != agent.root {
            bail!("Agent is outside the caller's task tree");
        }
        Ok(agent)
    }
    pub fn related(&self, agent: &Agent, ancestor: &str, any_depth: bool) -> bool {
        if !any_depth {
            return agent.parent == ancestor;
        }
        let mut parent = agent.parent.as_str();
        for _ in 0..=MAX_DEPTH {
            if parent == ancestor {
                return true;
            }
            match self.agents.get(parent) {
                Some(agent) => parent = &agent.parent,
                None => return false,
            }
        }
        false
    }
    pub fn decorate(&self, thread: &mut Value) {
        if let Some(agent) = thread["id"].as_str().and_then(|id| self.agents.get(id)) {
            agent.decorate(thread);
        }
    }
}

/// Calls arrive with a transport-bound scope; no tool accepts parent_thread_id.
pub struct McpCall {
    pub scope: String,
    pub message: Value,
    pub reply: mpsc::Sender<Value>,
}

pub struct Bridge {
    directory: PathBuf,
    pub socket: PathBuf,
    cancel: CancellationToken,
}

impl Bridge {
    pub fn bind(scope: String, calls: mpsc::Sender<McpCall>) -> Result<Self> {
        // Match the existing ACP bridge's short socket path on macOS.
        let socket_id = uuid::Uuid::new_v4().simple().to_string();
        let directory = std::env::temp_dir().join(format!("claudex-agents-{}", &socket_id[..12]));
        fs::create_dir(&directory)?;
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
        let socket = directory.join("m.sock");
        let listener = tokio::net::UnixListener::bind(&socket)?;
        let cancel = CancellationToken::new();
        let serving = cancel.clone();
        let tools = Arc::new(catalog());
        tokio::spawn(async move {
            loop {
                let stream = tokio::select! {
                    _ = serving.cancelled() => break,
                    stream = listener.accept() => match stream { Ok((stream,_)) => stream, Err(_) => break }
                };
                tokio::spawn(serve(
                    stream,
                    scope.clone(),
                    tools.clone(),
                    calls.clone(),
                    serving.clone(),
                ));
            }
        });
        Ok(Self {
            directory,
            socket,
            cancel,
        })
    }
    pub fn config(&self) -> Result<Value> {
        Ok(
            json!({"command":std::env::current_exe()?,"args":["mcp-bridge","--socket",self.socket],
            "tool_timeout_sec":120,"enabled":true}),
        )
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.cancel.cancel();
        let _ = fs::remove_file(&self.socket);
        let _ = fs::remove_dir(&self.directory);
    }
}

async fn serve(
    stream: tokio::net::UnixStream,
    scope: String,
    tools: Arc<Vec<Value>>,
    calls: mpsc::Sender<McpCall>,
    cancel: CancellationToken,
) {
    let (read, mut write) = stream.into_split();
    let (reply, mut replies) = mpsc::channel::<Value>(32);
    let writer = tokio::spawn(async move {
        while let Some(message) = replies.recv().await {
            if write
                .write_all(format!("{message}\n").as_bytes())
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let mut lines = FramedRead::new(read, LinesCodec::new_with_max_length(1024 * 1024));
    loop {
        let line = tokio::select! {
            _ = cancel.cancelled() => break,
            line = lines.next() => match line { Some(Ok(line)) => line, _ => break }
        };
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            break;
        };
        match crate::backend::mcp_local(&message, &tools) {
            crate::backend::McpLocal::Reply(message) => {
                if reply.send(message).await.is_err() {
                    break;
                }
            }
            crate::backend::McpLocal::Notification => {}
            crate::backend::McpLocal::Call => {
                if calls
                    .try_send(McpCall {
                        scope: scope.clone(),
                        message,
                        reply: reply.clone(),
                    })
                    .is_err()
                {
                    break;
                }
            }
        }
    }
    // Outstanding callers must not keep a disconnected helper's writer alive.
    writer.abort();
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot() -> Value {
        json!({"cwd":"/tmp","approvalPolicy":"never","approvalsReviewer":"auto_review",
            "sandbox":{"type":"workspaceWrite","writableRoots":["/tmp"],"networkAccess":false},
            "runtimeWorkspaceRoots":["/tmp"],"thread":{"ephemeral":false}})
    }
    #[test]
    fn inherited_permissions_are_explicit_in_both_harnesses() {
        let source = snapshot();
        let claude = child_params(&source, Harness::Claude, "opus", Some("high")).unwrap();
        assert_eq!(claude["sandboxPolicy"], source["sandbox"]);
        assert_eq!(claude["approvalPolicy"], "never");
        assert_eq!(claude["approvalsReviewer"], "auto_review");
        let codex = child_params(&source, Harness::Codex, "gpt-fake", None).unwrap();
        assert_eq!(codex["sandbox"], "workspace-write");
        assert_eq!(
            codex["config"]["sandbox_workspace_write"]["network_access"],
            false
        );
        assert_eq!(
            codex["config"]["sandbox_workspace_write"]["writable_roots"],
            json!(["/tmp"])
        );
    }
    #[test]
    fn native_local_environment_is_supported_but_remote_targets_are_not_substituted() {
        let mut source = snapshot();
        source["thread"]["environments"] =
            json!([{"environmentId":"local","cwd":"/tmp","runtimeWorkspaceRoots":["/tmp"]}]);
        assert!(child_params(&source, Harness::Claude, "opus", None).is_ok());
        source["thread"]["environments"][0]["environmentId"] = json!("ssh:another-host");
        assert!(child_params(&source, Harness::Claude, "opus", None).is_err());
        assert!(child_params(&source, Harness::Codex, "gpt-fake", None).is_err());
    }

    #[test]
    fn read_only_delegation_is_supported_by_codex_but_not_claude() {
        for ephemeral in [false, true] {
            let mut source = snapshot();
            source["sandbox"] = json!({"type":"readOnly","networkAccess":false});
            source["activePermissionProfile"] = json!({"id":":read-only"});
            source["thread"]["ephemeral"] = json!(ephemeral);
            let error = child_params(&source, Harness::Claude, "opus", None).unwrap_err();
            assert!(
                error.to_string().contains("not delegated agents"),
                "{error}"
            );
            let codex = child_params(&source, Harness::Codex, "gpt-fake", None).unwrap();
            assert_eq!(codex["sandbox"], "read-only");
            assert_eq!(codex["ephemeral"], ephemeral);
        }
    }

    #[test]
    fn unsupported_and_unknown_parent_permissions_fail_closed() {
        for sandbox in [
            json!({"type":"readOnly"}),
            json!({"type":"externalSandbox"}),
            json!({"type":"workspaceWrite","excludeSlashTmp":true}),
        ] {
            let mut source = snapshot();
            source["sandbox"] = sandbox;
            assert!(child_params(&source, Harness::Claude, "opus", None).is_err());
        }
        let mut source = snapshot();
        source.as_object_mut().unwrap().remove("approvalPolicy");
        assert!(child_params(&source, Harness::Codex, "gpt-fake", None).is_err());
        source = snapshot();
        source["activePermissionProfile"] = json!({"id":"restricted"});
        assert!(child_params(&source, Harness::Claude, "opus", None).is_err());
        assert!(validate("spawn_agent",&json!({"harness":"claude","model":"opus","message":"task","sandbox":"danger-full-access"})).is_err());
    }
}
