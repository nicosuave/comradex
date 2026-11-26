//! Opt-in ACP models served alongside Claude by the local login service.
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::acp::AcpAgent;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalAcp {
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub provider_id: String,
    pub provider_name: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include_models: Vec<String>,
    pub initialize_timeout_seconds: u64,
}

impl LocalAcp {
    pub fn new(agent: AcpAgent, executable: PathBuf, include_models: Vec<String>) -> Result<Self> {
        let config = Self {
            executable,
            args: agent.args,
            provider_id: agent.provider_id,
            provider_name: agent.provider_name,
            model: agent
                .model
                .context("Select the local ACP model with --model")?,
            include_models,
            initialize_timeout_seconds: agent.initialize_timeout_seconds,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn models(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.model.as_str()).chain(self.include_models.iter().map(String::as_str))
    }

    fn validate(&self) -> Result<()> {
        if !self.executable.is_absolute() {
            bail!("Local ACP executable must be an absolute path");
        }
        if matches!(self.provider_id.as_str(), "anthropic" | "openai")
            || self.provider_id.is_empty()
            || !self
                .provider_id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
        {
            bail!("Local ACP requires a distinct provider id");
        }
        let mut seen = HashSet::new();
        for model in self.models() {
            if model.trim().is_empty()
                || model == crate::acp::DEFAULT_MODEL
                || crate::router::native_claude_alias(model)
            {
                bail!("Select a distinct, concrete local ACP model: {model:?}");
            }
            if !seen.insert(model) {
                bail!("Duplicate local ACP model: {model}");
            }
        }
        Ok(())
    }

    pub fn config_path(root: &Path) -> PathBuf {
        root.join("local-acp.json")
    }

    pub fn socket(root: &Path) -> PathBuf {
        root.join("sockets/local-acp.sock")
    }

    pub fn state(&self, root: &Path) -> PathBuf {
        root.join("state/local-acp").join(&self.provider_id)
    }

    pub fn read(root: &Path) -> Result<Option<Self>> {
        let path = Self::config_path(root);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).with_context(|| format!("Read {}", path.display())),
        };
        let config: Self =
            serde_json::from_slice(&bytes).with_context(|| format!("Parse {}", path.display()))?;
        config.validate()?;
        Ok(Some(config))
    }

    /// The existing generated launcher passes this standard socket layout.
    pub fn root_for_socket(socket: &Path) -> Option<&Path> {
        let directory = socket.parent()?;
        if socket.file_name()? == "app-server-control.sock" && directory.file_name()? == "sockets" {
            return directory.parent();
        }
        None
    }

    pub async fn supervise(self, root: PathBuf, shutdown: CancellationToken) {
        let Ok(binary) = std::env::current_exe() else {
            eprintln!("Cannot resolve the executable for the local ACP worker");
            return;
        };
        let mut command = Command::new(binary);
        command
            .arg("app-server")
            .arg("--listen")
            .arg(format!("unix://{}", Self::socket(&root).display()))
            .arg("--state-dir")
            .arg(self.state(&root))
            .arg("--acp-agent")
            .arg(&self.executable)
            .args(["--provider-id", &self.provider_id])
            .args(["--provider-name", &self.provider_name])
            .args([
                "--initialize-timeout-seconds",
                &self.initialize_timeout_seconds.to_string(),
            ])
            .args(self.args.iter().map(|arg| format!("--acp-arg={arg}")))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .kill_on_drop(true);
        while !shutdown.is_cancelled() {
            match command.spawn() {
                Ok(mut child) => {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            if let Some(id) = child.id() {
                                // SAFETY: the live child PID belongs to this unreaped process.
                                unsafe { libc::kill(id as i32, libc::SIGTERM) };
                            }
                            if tokio::time::timeout(Duration::from_secs(5), child.wait()).await.is_err() {
                                let _ = child.kill().await;
                            }
                            return;
                        }
                        result = child.wait() => eprintln!("Local ACP worker stopped: {result:?}"),
                    }
                }
                Err(error) => eprintln!("Cannot start the local ACP worker: {error}"),
            }
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
        }
    }
}
