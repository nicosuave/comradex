//! Host-owned boundary for native file tools. Bash retains native sandbox/review.
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, RwLock};

pub(crate) const CALLBACK_ID: &str = "claudex-workspace-files";

/// Envelope fields only: tool inputs in other control requests are arbitrary.
pub(crate) fn is_callback(frame: &Value) -> bool {
    [&frame["request"], frame].iter().any(|request| {
        request["subtype"] == "hook_callback" || request.get("callback_id").is_some()
    })
}

#[derive(Clone)]
pub(crate) struct FileGuard {
    roots: Vec<PathBuf>,
    protected: Vec<PathBuf>,
    /// Native transcript namespace, protected except for the session's memory.
    transcripts: Option<PathBuf>,
    /// Native auto-memory directory, as reported by its init message.
    memory: Arc<RwLock<Option<PathBuf>>>,
}

impl FileGuard {
    pub(crate) fn new(
        cwd: &Path,
        policy: &crate::sandbox::Policy,
        protected: &[PathBuf],
        transcripts: Option<&Path>,
    ) -> Result<Option<Self>> {
        let crate::sandbox::Policy::WorkspaceWrite { writable_roots, .. } = policy else {
            return Ok(None);
        };
        policy.validate()?;
        let mut roots = vec![cwd.canonicalize()?];
        for root in writable_roots {
            let root = root.canonicalize()?;
            if !roots.contains(&root) {
                roots.push(root);
            }
        }
        let mut protected = protected.to_vec();
        for root in &roots {
            for name in [".git", ".codex", ".agents"] {
                protected.push(root.join(name));
            }
        }
        // Keep both spellings: a protected directory may itself be a symlink.
        // A dangling one has no physical spelling; writes through it fail
        // resolution and are denied anyway.
        for path in protected.clone() {
            if let Ok(physical) = destination(&path) {
                protected.push(physical);
            }
        }
        if let Some(transcripts) = transcripts {
            protected.push(transcripts.to_owned());
        }
        Ok(Some(Self {
            roots,
            protected,
            transcripts: transcripts.map(Path::to_owned),
            memory: Arc::default(),
        }))
    }

    /// Learn the auto-memory directory from native's init message. Only a
    /// directory of the default `projects/<project>/memory` shape is accepted:
    /// a repository-configured `autoMemoryDirectory` must not open other paths.
    pub(crate) fn observe(&self, frame: &Value) {
        if frame["type"] != "system" || frame["subtype"] != "init" {
            return;
        }
        let Some(transcripts) = &self.transcripts else {
            return;
        };
        let memory = frame["memory_paths"]["auto"]
            .as_str()
            .map(|raw| normalize(Path::new(raw)))
            .filter(|path| path.is_absolute())
            .and_then(|path| destination(&path).ok())
            .filter(|path| {
                path.file_name().is_some_and(|name| name == "memory")
                    && path
                        .parent()
                        .and_then(Path::parent)
                        .is_some_and(|parent| parent == transcripts)
            });
        *self.memory.write().unwrap() = memory;
    }

    pub(crate) fn hooks(&self) -> Value {
        json!({"PreToolUse":[{"matcher":"Write|Edit|NotebookEdit",
            "hookCallbackIds":[CALLBACK_ID],"timeout":60}]})
    }

    /// Envelope errors terminate the backend. Input/resolver errors return a
    /// successful protocol reply containing an explicit denial: native Claude
    /// treats hook protocol errors as permission to continue.
    pub(crate) fn response(&self, frame: &Value) -> Result<Value> {
        if frame["type"] != "control_request" || frame["request"]["subtype"] != "hook_callback" {
            bail!("invalid native file callback envelope");
        }
        let id = frame["request_id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .context("file callback has no request ID")?;
        let request = &frame["request"];
        if request["callback_id"] != CALLBACK_ID
            || request["input"]["hook_event_name"] != "PreToolUse"
        {
            bail!("unexpected native file callback identity or event");
        }
        let decision = match self.check_input(&request["input"]) {
            Ok(()) => json!({}),
            Err(_) => json!({"hookSpecificOutput":{
                "hookEventName":"PreToolUse","permissionDecision":"deny",
                "permissionDecisionReason":"Host workspace boundary: direct file edits must stay within authorized, unprotected roots. Request a reviewed Bash escape for outside or protected changes."
            }}),
        };
        Ok(json!({"type":"control_response","response":{
            "subtype":"success","request_id":id,"response":decision
        }}))
    }

    fn check_input(&self, input: &Value) -> Result<()> {
        let key = match input["tool_name"].as_str() {
            Some("Write" | "Edit") => "file_path",
            Some("NotebookEdit") => "notebook_path",
            _ => bail!("unknown native file tool"),
        };
        let cwd = Path::new(input["cwd"].as_str().context("missing callback cwd")?);
        let raw = input["tool_input"][key]
            .as_str()
            .filter(|p| !p.is_empty())
            .context("missing file target")?;
        // Tilde expansion is tool-specific; never mistake it for a cwd child.
        if !cwd.is_absolute() || raw.starts_with('~') || raw.contains('\0') {
            bail!("invalid file target or cwd");
        }
        let path = normalize(&cwd.join(raw));
        let physical = destination(&path)?;
        if let Some(memory) = self.memory.read().unwrap().as_ref()
            && physical.starts_with(memory)
            && physical != *memory
        {
            return Ok(());
        }
        if !self.roots.iter().any(|root| physical.starts_with(root))
            || self
                .protected
                .iter()
                .any(|p| protected_prefix(&path, p) || protected_prefix(&physical, p))
        {
            bail!("target outside host file boundary");
        }
        Ok(())
    }
}

fn protected_prefix(path: &Path, prefix: &Path) -> bool {
    let mut components = path.components();
    prefix.components().all(|expected| {
        components.next().is_some_and(|actual| {
            if actual == expected {
                return true;
            }
            // New suffixes cannot be canonicalized yet. On platforms commonly
            // using case-insensitive volumes, conservatively protect ASCII aliases
            // too (notably .GIT/.CODEX/.AGENTS), including on case-sensitive volumes.
            cfg!(any(target_os = "macos", target_os = "windows"))
                && actual
                    .as_os_str()
                    .to_str()
                    .zip(expected.as_os_str().to_str())
                    .is_some_and(|(a, b)| a.eq_ignore_ascii_case(b))
        })
    })
}

// Native path.resolve removes dot segments before filesystem traversal.
fn normalize(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                result.pop();
            }
            component => result.push(component.as_os_str()),
        }
    }
    result
}

fn destination(path: &Path) -> Result<PathBuf> {
    let mut ancestor = path;
    let mut suffix = Vec::new();
    loop {
        match ancestor.canonicalize() {
            Ok(mut resolved) => {
                for component in suffix.iter().rev() {
                    resolved.push(component);
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // A dangling symlink is not a new plain path component.
                match ancestor.symlink_metadata() {
                    Ok(_) => bail!("unresolvable existing target"),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                suffix.push(
                    ancestor
                        .file_name()
                        .context("target has no existing ancestor")?,
                );
                ancestor = ancestor.parent().context("target has no parent")?;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(cwd: &Path, tool: &str, target: &Path) -> Value {
        let key = if tool == "NotebookEdit" {
            "notebook_path"
        } else {
            "file_path"
        };
        json!({"type":"control_request","request_id":"hook-1","request":{
            "subtype":"hook_callback","callback_id":CALLBACK_ID,"input":{
                "hook_event_name":"PreToolUse","cwd":cwd,"tool_name":tool,
                "tool_input":{key:target}
            }
        }})
    }

    #[test]
    fn authorized_roots_and_new_paths_are_neutral_outside_and_protected_are_denied() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("work");
        let extra = dir.path().join("extra");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&extra).unwrap();
        let policy = crate::sandbox::Policy::WorkspaceWrite {
            writable_roots: vec![extra.clone()],
            network_access: false,
            exclude_slash_tmp: false,
            exclude_tmpdir_env_var: false,
        };
        let guard = FileGuard::new(&root, &policy, &[], None).unwrap().unwrap();
        for tool in ["Write", "Edit", "NotebookEdit"] {
            for path in [
                root.join("new/nested/file"),
                extra.join("file"),
                PathBuf::from("relative"),
            ] {
                let reply = guard.response(&request(&root, tool, &path)).unwrap();
                assert_eq!(reply["response"]["response"], json!({}));
            }
            for path in [
                dir.path().join("outside"),
                root.join("../outside"),
                root.join(".git/config"),
                root.join(".agents/rules"),
                root.join(".codex/settings"),
            ] {
                let reply = guard.response(&request(&root, tool, &path)).unwrap();
                assert_eq!(reply["response"]["subtype"], "success");
                assert_eq!(
                    reply["response"]["response"]["hookSpecificOutput"]["permissionDecision"],
                    "deny"
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_and_dangling_symlinks_do_not_bypass_roots() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("work");
        std::fs::create_dir(&root).unwrap();
        symlink(dir.path(), root.join("escape")).unwrap();
        symlink(dir.path().join("missing"), root.join("dangling")).unwrap();
        symlink(root.join("cycle"), root.join("cycle")).unwrap();
        let guard = FileGuard::new(&root, &crate::sandbox::Policy::workspace(), &[], None)
            .unwrap()
            .unwrap();
        for path in ["escape/new/file", "dangling/file", "cycle/file"] {
            let reply = guard
                .response(&request(&root, "Write", Path::new(path)))
                .unwrap();
            assert_eq!(
                reply["response"]["response"]["hookSpecificOutput"]["permissionDecision"],
                "deny"
            );
        }
    }

    #[test]
    fn malformed_inputs_deny_but_unrouteable_callbacks_are_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let guard = FileGuard::new(dir.path(), &crate::sandbox::Policy::workspace(), &[], None)
            .unwrap()
            .unwrap();
        let valid = request(dir.path(), "Write", &dir.path().join("file"));
        for field in ["tool_name", "tool_input", "cwd"] {
            let mut frame = valid.clone();
            frame["request"]["input"][field] = Value::Null;
            let reply = guard.response(&frame).unwrap();
            assert_eq!(
                reply["response"]["response"]["hookSpecificOutput"]["permissionDecision"],
                "deny"
            );
        }
        let mut frame = valid.clone();
        frame["request_id"] = Value::Null;
        assert!(guard.response(&frame).is_err());
        let mut frame = valid;
        frame["request"]["callback_id"] = json!("unknown");
        assert!(guard.response(&frame).is_err());
    }

    #[test]
    fn tool_inputs_naming_hook_fields_are_not_callbacks() {
        let permission = json!({"type":"control_request","request_id":"p","request":{
            "subtype":"can_use_tool","tool_name":"mcp__x__y",
            "input":{"hook_event_name":"PreToolUse","callback_id":"x"}
        }});
        assert!(!is_callback(&permission));
        assert!(is_callback(
            &json!({"type":"control_request","request":{"subtype":"hook_callback"}})
        ));
        assert!(is_callback(
            &json!({"type":"invalid","request":{"callback_id":CALLBACK_ID}})
        ));
    }

    #[cfg(unix)]
    #[test]
    fn only_the_reported_default_memory_directory_is_writable_in_transcripts() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path().canonicalize().unwrap();
        let root = dir.join("work");
        let transcripts = dir.join("claude/projects");
        let profile = dir.join("profile");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir_all(transcripts.join("-work")).unwrap();
        std::fs::create_dir(&profile).unwrap();
        // Native reports the path through the profile's transcript link.
        symlink(&transcripts, profile.join("projects")).unwrap();
        let guard = FileGuard::new(
            &root,
            &crate::sandbox::Policy::workspace(),
            &[],
            Some(&transcripts),
        )
        .unwrap()
        .unwrap();
        let decision = |path: PathBuf| {
            guard.response(&request(&root, "Write", &path)).unwrap()["response"]["response"].clone()
        };
        let memory = transcripts.join("-work/memory/note.md");
        assert_eq!(
            decision(memory.clone())["hookSpecificOutput"]["permissionDecision"],
            "deny"
        );

        let init = |path: &Path| {
            json!({"type":"system","subtype":"init",
            "memory_paths":{"auto":format!("{}/", path.display())}})
        };
        guard.observe(&init(&profile.join("projects/-work/memory")));
        assert_eq!(decision(memory), json!({}));
        assert_eq!(decision(root.join("file")), json!({}));
        for path in [
            transcripts.join("-work/session.jsonl"),
            transcripts.join("-other/memory/note.md"),
            transcripts.join("-work/memory"),
        ] {
            assert_eq!(
                decision(path)["hookSpecificOutput"]["permissionDecision"],
                "deny"
            );
        }

        // A configured directory elsewhere is not exempted.
        let elsewhere = dir.join("elsewhere/memory");
        guard.observe(&init(&elsewhere));
        assert_eq!(
            decision(elsewhere.join("note.md"))["hookSpecificOutput"]["permissionDecision"],
            "deny"
        );
        assert_eq!(
            decision(transcripts.join("-work/memory/note.md"))["hookSpecificOutput"]["permissionDecision"],
            "deny"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn absent_protected_directories_cannot_be_created_through_case_aliases() {
        let dir = tempfile::tempdir().unwrap();
        let guard = FileGuard::new(dir.path(), &crate::sandbox::Policy::workspace(), &[], None)
            .unwrap()
            .unwrap();
        for target in [".AGENTS/rules", ".GIT/config", ".CODEX/settings"] {
            let reply = guard
                .response(&request(dir.path(), "Write", Path::new(target)))
                .unwrap();
            assert_eq!(
                reply["response"]["response"]["hookSpecificOutput"]["permissionDecision"],
                "deny"
            );
        }
    }
}
