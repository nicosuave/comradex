//! Translate workspace permissions to Claude's OS-enforced command sandbox.
//!
//! Native file tools keep Claude's scoped Edit permissions; MCP servers and hooks
//! are trusted integrations, not subprocesses inside this command sandbox.
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Host-owned defaults advertised to the desktop. Explicit per-turn policies
/// remain authoritative; project Claude settings cannot widen these roots.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkspaceDefaults {
    pub writable_roots: Vec<PathBuf>,
    pub network_access: bool,
}

impl WorkspaceDefaults {
    pub fn load(state_dir: &Path) -> Result<Self> {
        let path = state_dir.join("workspace-defaults.json");
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => return Err(error).context("reading workspace defaults"),
        };
        let defaults: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid workspace defaults in {}", path.display()))?;
        defaults.policy().validate()?;
        Ok(defaults)
    }

    pub fn policy(&self) -> Policy {
        Policy::WorkspaceWrite {
            writable_roots: self.writable_roots.clone(),
            network_access: self.network_access,
            exclude_slash_tmp: false,
            exclude_tmpdir_env_var: false,
        }
    }

    pub fn desktop_config(&self) -> Value {
        json!({
            "writable_roots": self.writable_roots,
            "network_access": self.network_access,
            "exclude_slash_tmp": false,
            "exclude_tmpdir_env_var": false
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum Policy {
    #[default]
    DangerFullAccess,
    /// Tool-free ephemeral desktop helpers, not a general filesystem sandbox.
    ReadOnly {
        #[serde(default, rename = "networkAccess")]
        network_access: bool,
    },
    WorkspaceWrite {
        #[serde(default, rename = "writableRoots")]
        writable_roots: Vec<PathBuf>,
        #[serde(default, rename = "networkAccess")]
        network_access: bool,
        #[serde(default, rename = "excludeSlashTmp")]
        exclude_slash_tmp: bool,
        #[serde(default, rename = "excludeTmpdirEnvVar")]
        exclude_tmpdir_env_var: bool,
    },
}

impl Policy {
    pub fn workspace() -> Self {
        Self::WorkspaceWrite {
            writable_roots: vec![],
            network_access: false,
            exclude_slash_tmp: false,
            exclude_tmpdir_env_var: false,
        }
    }

    pub fn is_workspace(&self) -> bool {
        matches!(self, Self::WorkspaceWrite { .. })
    }

    pub fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnly { .. })
    }

    /// The policy Claude enforces, in one canonical form. Clients spell the same
    /// policy differently from turn to turn: the desktop repeats roots, may name
    /// the working directory, and lists the temporary directories only on a
    /// chat's first turn. A different spelling would relaunch Claude and change
    /// its tool descriptions, invalidating the prompt cache. Roots that cannot
    /// be resolved are kept as given, so launching still reports them.
    pub fn effective(&self, cwd: &Path) -> Self {
        let Self::WorkspaceWrite {
            writable_roots,
            network_access,
            exclude_slash_tmp,
            exclude_tmpdir_env_var,
        } = self
        else {
            return self.clone();
        };
        let canonical = |root: &Path| root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
        let mut roots: Vec<PathBuf> = writable_roots.iter().map(|root| canonical(root)).collect();
        // As in Codex, workspace-write keeps both temporary directories
        // writable unless the policy excludes them.
        if !exclude_slash_tmp && let Ok(tmp) = Path::new("/tmp").canonicalize() {
            roots.push(tmp);
        }
        if !exclude_tmpdir_env_var && let Ok(tmp) = std::env::temp_dir().canonicalize() {
            roots.push(tmp);
        }
        // The working directory is always writable; the native policy adds it.
        let cwd = canonical(cwd);
        roots.retain(|root| *root != cwd);
        roots.sort();
        roots.dedup();
        Self::WorkspaceWrite {
            writable_roots: roots,
            network_access: *network_access,
            exclude_slash_tmp: *exclude_slash_tmp,
            exclude_tmpdir_env_var: *exclude_tmpdir_env_var,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if matches!(
            self,
            Self::ReadOnly {
                network_access: true
            }
        ) {
            bail!("Read-only Claude helpers cannot enable network tools");
        }
        if let Self::WorkspaceWrite {
            writable_roots,
            exclude_slash_tmp,
            exclude_tmpdir_env_var,
            ..
        } = self
        {
            if !cfg!(any(target_os = "macos", target_os = "linux")) {
                bail!("Claude workspace sandbox requires macOS or Linux");
            }
            if *exclude_slash_tmp || *exclude_tmpdir_env_var {
                bail!(
                    "Claude's command sandbox requires its private temporary directory; temporary-directory exclusion profiles are unsupported"
                );
            }
            for root in writable_roots {
                if !root.is_absolute() || !root.is_dir() {
                    bail!("sandbox writable roots must be existing absolute directories");
                }
            }
        }
        Ok(())
    }

    pub fn apply_native(
        &self,
        settings: &mut Value,
        cwd: &Path,
        permission_mode: &str,
    ) -> Result<()> {
        let Self::WorkspaceWrite {
            writable_roots,
            network_access,
            ..
        } = self
        else {
            return Ok(());
        };
        self.validate()?;
        let mut roots = vec![cwd.canonicalize()?];
        for root in writable_roots {
            let root = root.canonicalize()?;
            if !roots.contains(&root) {
                roots.push(root);
            }
        }
        let settings = settings
            .as_object_mut()
            .context("Claude settings must be an object")?;
        let permissions = settings
            .entry("permissions")
            .or_insert(json!({}))
            .as_object_mut()
            .context("Claude permissions settings must be an object")?;
        // Native allow lists merge across sources. Remove preapprovals that can
        // widen filesystem/network access or approve an unsandboxed retry; keep
        // deny and ask rules, which still take precedence in the native engine.
        if let Some(allow) = permissions.get_mut("allow") {
            let allow = allow
                .as_array_mut()
                .context("permissions.allow must be an array")?;
            allow.retain(|rule| rule.as_str().is_some_and(|rule| !widens_access(rule)));
        }
        permissions.insert("additionalDirectories".into(), json!(roots));
        let ask = permissions
            .entry("ask")
            .or_insert(json!([]))
            .as_array_mut()
            .context("permissions.ask must be an array")?;
        // Manual review must expose sandbox escapes to the desktop. Auto mode
        // must instead let Claude's classifier review them: an injected ask rule
        // forces every escape back to the human, defeating Approve for me.
        // Preserve any explicit user/managed ask rules loaded above.
        if permission_mode != "auto"
            && !ask.contains(&json!("Bash(dangerouslyDisableSandbox:true)"))
        {
            ask.push(json!("Bash(dangerouslyDisableSandbox:true)"));
        }
        let mut protected = Vec::new();
        for root in &roots {
            for name in [".git", ".codex", ".agents"] {
                let path = root.join(name);
                // Rule paths beginning // are absolute in Claude permission syntax.
                ask.push(json!(format!("Edit(/{}/**)", escape_rule_path(&path)?)));
                protected.push(path);
            }
        }
        let sandbox = settings
            .entry("sandbox")
            .or_insert(json!({}))
            .as_object_mut()
            .context("Claude sandbox settings must be an object")?;
        sandbox.insert("enabled".into(), json!(true));
        sandbox.insert("failIfUnavailable".into(), json!(true));
        sandbox.insert("autoAllowBashIfSandboxed".into(), json!(true));
        // Keep an existing strict policy; otherwise native permission handling
        // reviews requests to retry a blocked command without the sandbox.
        sandbox
            .entry("allowUnsandboxedCommands")
            .or_insert(json!(true));
        sandbox.insert("excludedCommands".into(), json!([]));
        let filesystem = sandbox
            .entry("filesystem")
            .or_insert(json!({}))
            .as_object_mut()
            .context("sandbox.filesystem must be an object")?;
        filesystem.insert("disabled".into(), json!(false));
        filesystem.insert("allowWrite".into(), json!(roots));
        let denied = filesystem
            .entry("denyWrite")
            .or_insert(json!([]))
            .as_array_mut()
            .context("sandbox.filesystem.denyWrite must be an array")?;
        denied.extend(protected.into_iter().map(|path| json!(path)));
        let network = sandbox
            .entry("network")
            .or_insert(json!({}))
            .as_object_mut()
            .context("sandbox.network must be an object")?;
        network.insert(
            "allowedDomains".into(),
            if *network_access {
                json!(["*"])
            } else {
                json!([])
            },
        );
        network.insert("allowUnixSockets".into(), json!([]));
        network.insert("allowAllUnixSockets".into(), json!(false));
        // Local development servers are opt-in native settings, and only apply
        // to profiles that already grant network access.
        if !network_access {
            network.insert("allowLocalBinding".into(), json!(false));
        }
        // Use the native allowlist-checking proxy rather than an external proxy
        // whose egress policy is not described by this workspace profile.
        network.remove("httpProxyPort");
        network.remove("socksProxyPort");
        Ok(())
    }

    /// Inspect the native runtime's resolved state before accepting user input.
    /// Managed settings stay native, and a managed widening or unavailable OS
    /// sandbox fails here rather than silently contradicting the desktop profile.
    pub fn verify_native(&self, settings: &Value, status: &Value, cwd: &Path) -> Result<()> {
        let Self::WorkspaceWrite {
            writable_roots,
            network_access,
            ..
        } = self
        else {
            return Ok(());
        };
        if status["supported"] != true
            || status["enabled"] != true
            || status["enabled_in_settings"] != true
        {
            bail!(
                "Claude's OS sandbox is unavailable or disabled; refusing to run a workspace-write turn"
            );
        }
        let effective = &settings["effective"];
        if effective["permissions"]["allow"]
            .as_array()
            .is_some_and(|rules| {
                rules
                    .iter()
                    .any(|rule| rule.as_str().is_none_or(widens_access))
            })
        {
            bail!("Native managed permission rules preapprove access beyond the workspace policy");
        }
        if effective["sandbox"]["enabled"] != true
            || effective["sandbox"]["failIfUnavailable"] != true
            || effective["sandbox"]["filesystem"]["disabled"] == true
        {
            bail!("Native settings do not enforce the requested filesystem sandbox");
        }
        let mut roots = vec![cwd.canonicalize()?];
        for root in writable_roots {
            roots.push(root.canonicalize()?);
        }
        for value in [
            &status["restrictions"]["fs_allow_write"],
            &effective["permissions"]["additionalDirectories"],
        ] {
            let paths = value
                .as_array()
                .context("native sandbox did not report its write roots")?;
            for path in paths {
                let path = path.as_str().context("invalid native sandbox write root")?;
                let resolved = Path::new(path)
                    .canonicalize()
                    .with_context(|| format!("cannot verify native sandbox root {path}"))?;
                if !roots.iter().any(|root| resolved.starts_with(root)) {
                    bail!(
                        "Native settings grant writes outside the selected workspace roots: {path}"
                    );
                }
            }
        }
        for (label, value) in [
            ("excluded commands", &status["excluded_commands"]),
            ("Unix sockets", &status["restrictions"]["unix_sockets"]),
        ] {
            if !value.as_array().is_some_and(Vec::is_empty) {
                bail!("Native sandbox has unrequested {label}");
            }
        }
        if status["restrictions"]["allow_all_unix_sockets"] != false {
            bail!("Native sandbox does not restrict Unix sockets");
        }
        if !network_access
            && !status["restrictions"]["network_allowed_domains"]
                .as_array()
                .is_some_and(Vec::is_empty)
        {
            bail!("Native settings grant network access beyond the requested workspace policy");
        }
        if (!network_access && effective["sandbox"]["network"]["allowLocalBinding"] == true)
            || !effective["sandbox"]["network"]["httpProxyPort"].is_null()
            || !effective["sandbox"]["network"]["socksProxyPort"].is_null()
        {
            bail!("Native settings replace the workspace network boundary");
        }
        Ok(())
    }

    /// Parent policy is merged by Claude beneath genuine enterprise policy.
    /// Native project/local loading stays enabled; managed-only rules and an
    /// admin-required shell sandbox keep repository grants non-authoritative.
    pub fn managed_settings(
        &self,
        settings: &Value,
        cwd: &Path,
        permission_mode: &str,
        profile: &crate::native_profile::NativeProfile,
    ) -> Result<Value> {
        if !self.is_workspace() {
            return Ok(json!({}));
        }
        let mut policy = json!({});
        if let Some(permissions) = settings.get("permissions") {
            policy["permissions"] = permissions.clone();
        }
        if let Some(sandbox) = settings.get("sandbox") {
            policy["sandbox"] = sandbox.clone();
        }
        self.apply_native(&mut policy, cwd, permission_mode)?;
        policy["allowManagedPermissionRulesOnly"] = json!(true);
        policy["sandbox"]["network"]["allowManagedDomainsOnly"] = json!(true);
        let roots = policy["sandbox"]["filesystem"]["allowWrite"]
            .as_array()
            .unwrap()
            .clone();
        let permissions = policy["permissions"].as_object_mut().unwrap();
        let allow = permissions
            .entry("allow")
            .or_insert(json!([]))
            .as_array_mut()
            .context("permissions.allow must be an array")?;
        for root in roots {
            let path = Path::new(root.as_str().context("invalid native write root")?);
            push_unique(
                allow,
                json!(format!("Edit(/{}/**)", escape_rule_path(path)?)),
            );
        }
        for path in &profile.readable_paths {
            let path = escape_rule_path(path)?;
            push_unique(allow, json!(format!("Read(/{path})")));
            push_unique(allow, json!(format!("Read(/{path}/**)")));
        }
        let deny = permissions
            .entry("deny")
            .or_insert(json!([]))
            .as_array_mut()
            .context("permissions.deny must be an array")?;
        for path in &profile.protected_paths {
            let path = escape_rule_path(path)?;
            push_unique(deny, json!(format!("Edit(/{path})")));
            push_unique(deny, json!(format!("Edit(/{path}/**)")));
        }
        let denied = policy["sandbox"]["filesystem"]["denyWrite"]
            .as_array_mut()
            .unwrap();
        for path in &profile.protected_paths {
            push_unique(denied, json!(path));
        }
        Ok(policy)
    }

    /// Unlike merged settings, this endpoint describes permission rules that
    /// are actually in force. Lower-tier grants remain visible in get_settings
    /// even when managed-only policy ignores them.
    pub fn verify_native_with_rules(
        &self,
        settings: &Value,
        status: &Value,
        rules: &Value,
        expected_managed: &Value,
        cwd: &Path,
    ) -> Result<()> {
        if !self.is_workspace() {
            return Ok(());
        }
        let rules = &rules["state"];
        if rules["managedOnly"] != true
            || status["locked"] != true
            || status["overrides_locked"] != true
            || status["restrictions"]["network_managed"] != true
            || settings["effective"]["sandbox"]["network"]["allowManagedDomainsOnly"] != true
        {
            bail!("Native managed workspace policy is not active");
        }
        let active: Vec<_> = rules["rules"]
            .as_array()
            .context("native runtime did not report active permission rules")?
            .iter()
            .filter(|rule| rule["notInEffect"] != true)
            .collect();
        let expected_allow = expected_managed["permissions"]["allow"]
            .as_array()
            .context("host policy is missing scoped permission rules")?;
        for rule in &active {
            let text = rule["rule"]
                .as_str()
                .context("invalid native permission rule")?;
            if rule["behavior"] == "allow"
                && widens_access(text)
                && !expected_allow.contains(&json!(text))
            {
                bail!("Native active permission rules exceed the workspace policy");
            }
        }
        // Missing restrictive rules must fail closed. Required scoped file
        // grants also have to survive enterprise merging before we advertise
        // automatic workspace writes (particularly in dontAsk mode).
        for behavior in ["allow", "deny", "ask"] {
            if let Some(expected) = expected_managed["permissions"][behavior].as_array() {
                for rule in expected {
                    if !active
                        .iter()
                        .any(|actual| actual["behavior"] == behavior && actual["rule"] == *rule)
                    {
                        bail!("Native policy did not retain a required {behavior} rule");
                    }
                }
            }
        }
        let denied = status["restrictions"]["fs_deny_write"]
            .as_array()
            .context("native sandbox did not report protected paths")?;
        for path in expected_managed["sandbox"]["filesystem"]["denyWrite"]
            .as_array()
            .context("host policy is missing protected paths")?
        {
            if !denied.contains(path) {
                bail!("Native sandbox did not retain a protected path");
            }
        }
        if expected_managed["sandbox"]["allowUnsandboxedCommands"] == false
            && status["no_sandbox_allowed"] != false
        {
            bail!("Native sandbox did not retain the disabled escape policy");
        }
        // Reuse OS/network validation without treating inactive raw grants as
        // executable authority. FileGuard separately bounds directory fast paths.
        settings["effective"]["permissions"]
            .as_object()
            .context("native runtime did not report effective permission settings")?;
        let mut runtime = settings.clone();
        runtime["effective"]["permissions"]["allow"] = json!([]);
        runtime["effective"]["permissions"]["additionalDirectories"] =
            status["restrictions"]["fs_allow_write"].clone();
        self.verify_native(&runtime, status, cwd)
    }
}

fn push_unique(values: &mut Vec<Value>, value: Value) {
    if !values.contains(&value) {
        values.push(value);
    }
}

pub(crate) fn widens_access(rule: &str) -> bool {
    [
        "*",
        "Edit",
        "Write",
        "NotebookEdit",
        "MultiEdit",
        "Bash",
        "PowerShell",
        "Monitor",
        "WebFetch",
    ]
    .iter()
    .any(|tool| rule == *tool || rule.starts_with(&format!("{tool}(")))
}

fn escape_rule_path(path: &Path) -> Result<String> {
    let path = path.to_str().context("sandbox paths must be UTF-8")?;
    if path.contains(['\n', '\r', ')']) {
        bail!("sandbox root cannot contain newline or ')' in a native permission rule");
    }
    Ok(path
        .chars()
        .flat_map(|c| {
            if "\\*?[]!".contains(c) {
                vec!['\\', c]
            } else {
                vec![c]
            }
        })
        .collect())
}

/// Fold documented user/project/local sources into one session overlay, so a
/// lower-priority native allowWrite array cannot widen the advertised roots.
/// This reads settings only; authentication stays with the native CLI.
pub fn load_settings(cwd: &Path, sources: &[&str]) -> Result<Value> {
    let user_dir = std::env::var_os("CLAUDE_CONFIG_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".claude")))
        .context("cannot locate native Claude settings directory")?;
    let project = cwd
        .ancestors()
        .find(|path| path.join(".git").exists())
        .unwrap_or(cwd);
    load_settings_from(cwd, &user_dir, project, sources)
}

pub fn load_settings_from(
    cwd: &Path,
    user_dir: &Path,
    project: &Path,
    sources: &[&str],
) -> Result<Value> {
    let mut settings = json!({});
    for (source, path, anchor) in [
        ("user", user_dir.join("settings.json"), user_dir),
        ("project", project.join(".claude/settings.json"), cwd),
        ("local", project.join(".claude/settings.local.json"), cwd),
    ] {
        if !sources.contains(&source) {
            continue;
        }
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("reading Claude settings {}", path.display()));
            }
        };
        let mut value: Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid Claude settings {}", path.display()))?;
        rebase_rules(&mut value, anchor)?;
        rebase_sandbox_paths(
            &mut value,
            if source == "user" { user_dir } else { project },
        )?;
        merge(&mut settings, value);
    }
    Ok(settings)
}

pub fn rebase_rules(settings: &mut Value, anchor: &Path) -> Result<()> {
    if !settings.is_object() {
        bail!("Claude settings must contain an object");
    }
    for key in ["allow", "deny", "ask"] {
        if let Some(rules) = settings.get_mut("permissions").and_then(|p| p.get_mut(key)) {
            for rule in rules
                .as_array_mut()
                .context("Claude permission rules must be arrays")?
            {
                let Some(text) = rule.as_str() else {
                    bail!("Claude permission rules must be strings")
                };
                if let Some((tool, pattern)) = text.split_once('(')
                    && [
                        "Read",
                        "Edit",
                        "Write",
                        "NotebookEdit",
                        "MultiEdit",
                        "Glob",
                        "Cd",
                    ]
                    .contains(&tool)
                    && pattern.starts_with('/')
                    && !pattern.starts_with("//")
                {
                    *rule = json!(format!("{tool}(/{}{}", escape_rule_path(anchor)?, pattern));
                }
            }
        }
    }
    Ok(())
}

pub fn rebase_sandbox_paths(settings: &mut Value, anchor: &Path) -> Result<()> {
    for key in ["denyWrite", "denyRead", "allowRead"] {
        if let Some(paths) = settings
            .get_mut("sandbox")
            .and_then(|s| s.get_mut("filesystem"))
            .and_then(|f| f.get_mut(key))
        {
            for path in paths
                .as_array_mut()
                .context("sandbox filesystem paths must be arrays")?
            {
                let text = path
                    .as_str()
                    .context("sandbox filesystem paths must be strings")?;
                if !text.starts_with('/') && !text.starts_with("~/") {
                    *path = json!(anchor.join(text));
                }
            }
        }
    }
    Ok(())
}

pub fn merge(target: &mut Value, overlay: Value) {
    match (target, overlay) {
        (Value::Object(target), Value::Object(overlay)) => {
            for (key, value) in overlay {
                if key == "allowUnsandboxedCommands"
                    && target.get(&key) == Some(&Value::Bool(false))
                {
                    continue;
                }
                if ["blockReadsOutsideWorkingDirectories", "strictAllowlist"]
                    .contains(&key.as_str())
                    && target.get(&key) == Some(&Value::Bool(true))
                {
                    continue;
                }
                // These native list settings use highest-precedence replacement,
                // unlike permission lists and most other native arrays.
                if ["fallbackModel", "modelPicker"].contains(&key.as_str()) {
                    target.insert(key, value);
                    continue;
                }
                merge(target.entry(key).or_insert(Value::Null), value);
            }
        }
        (Value::Array(target), Value::Array(overlay)) => {
            for value in overlay {
                if !target.contains(&value) {
                    target.push(value);
                }
            }
        }
        (target, overlay) => *target = overlay,
    }
}
