//! A native Claude user namespace with host-owned permission settings.
//! Feature content and transcripts remain native; credentials are never copied.
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use fs2::FileExt;
use serde_json::{Value, json};

// User-tier features that native discovers under its config directory.
const LINKED_FEATURES: [&str; 7] = [
    "skills",
    "commands",
    "agents",
    "rules",
    "output-styles",
    "workflows",
    "CLAUDE.md",
];
// Protected even before they exist, so a later native write cannot appear unguarded.
const PROTECTED_CONFIG: [&str; 6] = [
    "settings.json",
    "settings.local.json",
    ".credentials.json",
    "hooks",
    "plugins",
    "keybindings.json",
];
// Profiles are rebuilt on every launch, so an idle one is safe to remove.
const STALE_PROFILE: std::time::Duration = std::time::Duration::from_secs(30 * 24 * 60 * 60);

#[derive(Clone, Debug)]
pub struct NativeProfile {
    pub config_dir: PathBuf,
    pub environment: BTreeMap<OsString, OsString>,
    pub user_settings: Value,
    pub protected_paths: Vec<PathBuf>,
    pub readable_paths: Vec<PathBuf>,
    /// Native transcript namespace. Only the session's auto-memory directory
    /// inside it is writable through file tools.
    pub transcripts: PathBuf,
}

impl NativeProfile {
    pub fn prepare(state_dir: &Path, cwd: &Path, session_id: &str) -> Result<Self> {
        let home = PathBuf::from(std::env::var_os("HOME").context("HOME is not set")?);
        let configured = std::env::var_os("CLAUDE_CONFIG_DIR").filter(|s| !s.is_empty());
        let original = configured
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".claude"));
        let metadata = if configured.is_some() {
            original.join(".claude.json")
        } else {
            home.join(".claude.json")
        };
        let secure_storage = std::env::var_os("CLAUDE_SECURESTORAGE_CONFIG_DIR")
            .unwrap_or_else(|| configured.unwrap_or_default());
        let plugins = std::env::var_os("CLAUDE_CODE_PLUGIN_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| original.join("plugins"));
        Self::prepare_from(
            state_dir,
            cwd,
            session_id,
            &original,
            &metadata,
            secure_storage,
            &plugins,
        )
    }

    /// Explicit source paths keep configuration tests independent of process env.
    pub fn prepare_from(
        state_dir: &Path,
        cwd: &Path,
        session_id: &str,
        original: &Path,
        metadata: &Path,
        secure_storage: OsString,
        plugins: &Path,
    ) -> Result<Self> {
        let session_id = uuid::Uuid::parse_str(session_id).context("invalid native session ID")?;
        let cwd = cwd.canonicalize()?;
        let original = absolute(original)?;
        let metadata = absolute(metadata)?;
        let plugins = absolute(plugins)?;
        // Stable across process restarts, unlike DefaultHasher. The recorded exact
        // source below detects a collision instead of sharing unrelated profiles.
        let identity = original
            .to_str()
            .context("Claude config path must be UTF-8")?;
        let hash = identity.bytes().fold(0xcbf29ce484222325_u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x100000001b3)
        });
        let namespace = state_dir.join("native-claude");
        let config_dir = namespace
            .join(format!("{hash:016x}"))
            .join(session_id.to_string());
        private_directory(&config_dir)?;
        let config_dir = config_dir.canonicalize()?;
        let lock = private_file(&config_dir.join("profile.lock"))?;
        lock.lock_exclusive().context("locking native profile")?;
        let source = config_dir.join("source.json");
        let expected_source = json!({"config":original,"metadata":metadata});
        if source.exists() && read_object(&source)? != expected_source {
            bail!("native profile source identity changed");
        }
        write_json(&source, &expected_source)?;

        let mut user_settings = read_object(&original.join("settings.json"))?;
        crate::sandbox::rebase_rules(&mut user_settings, &original)?;
        crate::sandbox::rebase_sandbox_paths(&mut user_settings, &original)?;
        // The user tier is not subject to native's admin-required repository
        // filtering. Remove its write grants before native can combine sources.
        if let Some(permissions) = user_settings
            .get_mut("permissions")
            .and_then(Value::as_object_mut)
        {
            permissions.remove("additionalDirectories");
            if let Some(allow) = permissions.get_mut("allow") {
                allow
                    .as_array_mut()
                    .context("permissions.allow must be an array")?
                    .retain(|r| {
                        r.as_str()
                            .is_some_and(|r| !crate::sandbox::widens_access(r))
                    });
            }
        }
        if let Some(sandbox) = user_settings
            .get_mut("sandbox")
            .and_then(Value::as_object_mut)
        {
            sandbox.remove("excludedCommands");
            if let Some(filesystem) = sandbox.get_mut("filesystem").and_then(Value::as_object_mut) {
                filesystem.remove("allowWrite");
            }
        }
        write_json(&config_dir.join("settings.json"), &user_settings)?;

        // Protect the native configuration except its transcript namespace:
        // native auto-memory lives there, and a wholesale deny rule would
        // override native's memory write exception.
        let mut protected_paths = vec![metadata.clone(), plugins.clone()];
        for name in PROTECTED_CONFIG {
            protected_paths.push(original.join(name));
        }
        protected_paths.extend(entries_except(&original, "projects")?);
        let mut readable_paths = Vec::new();
        for name in LINKED_FEATURES {
            let target = original.join(name);
            let link = config_dir.join(name);
            ensure_link(&link, &target)?;
            readable_paths.push(link);
            readable_paths.push(target.clone());
            protected_paths.push(target.clone());
            if let Ok(real) = target.canonicalize() {
                readable_paths.push(real.clone());
                protected_paths.push(real);
            }
        }
        // Native resumes/forks use the existing transcript namespace. The SDK
        // session IDs are unique, so independent profiles don't share settings.
        fs::create_dir_all(original.join("projects"))
            .context("creating native transcript directory")?;
        ensure_link(&config_dir.join("projects"), &original.join("projects"))?;
        let transcripts = original.join("projects").canonicalize()?;
        let sessions = config_dir.parent().context("profile has no namespace")?;
        remove_stale_profiles(sessions, &config_dir);
        // Native addresses memory through this profile's projects link, so the
        // profile namespace is protected entry by entry around that link.
        let namespace = namespace.canonicalize()?;
        protected_paths.extend(entries_except(&namespace, sessions.file_name().unwrap())?);
        protected_paths.extend(entries_except(sessions, config_dir.file_name().unwrap())?);
        protected_paths.extend(entries_except(&config_dir, "projects")?);
        for name in [
            "settings.json",
            ".claude.json",
            "source.json",
            "profile.lock",
        ] {
            protected_paths.push(config_dir.join(name));
        }
        // The native plugin root override owns registry/cache lookup; preserve
        // its exact namespace instead of rebuilding the plugin catalog.
        let mut environment = BTreeMap::new();
        environment.insert(
            "CLAUDE_CONFIG_DIR".into(),
            config_dir.as_os_str().to_owned(),
        );
        environment.insert("CLAUDE_SECURESTORAGE_CONFIG_DIR".into(), secure_storage);
        environment.insert(
            "CLAUDE_CODE_PLUGIN_CACHE_DIR".into(),
            plugins.as_os_str().to_owned(),
        );

        let original_state = read_object(&metadata)?;
        let state_path = config_dir.join(".claude.json");
        let mut state = read_object(&state_path)?;
        // Copy only native integration settings, never auth, account identity,
        // tokens, or the original global config wholesale. Definitions may have
        // configured MCP env values, so this file is strictly owner-readable.
        copy_fields(&mut state, &original_state, &["mcpServers"])?;
        let project_key = cwd.to_str().context("native project path must be UTF-8")?;
        let projects = state
            .as_object_mut()
            .unwrap()
            .entry("projects")
            .or_insert(json!({}))
            .as_object_mut()
            .context("native projects state must be an object")?;
        let project = projects.entry(project_key).or_insert(json!({}));
        copy_fields(
            project,
            &original_state["projects"][project_key],
            &[
                "hasTrustDialogAccepted",
                "mcpServers",
                "enabledMcpjsonServers",
                "disabledMcpjsonServers",
                "enableAllProjectMcpServers",
            ],
        )?;
        write_json(&state_path, &state)?;
        protected_paths.sort();
        protected_paths.dedup();
        readable_paths.sort();
        readable_paths.dedup();
        Ok(Self {
            config_dir,
            environment,
            user_settings,
            protected_paths,
            readable_paths,
            transcripts,
        })
    }
}

/// Rule-expressible entries of `dir` other than `except`. Names that cannot be
/// written in a permission rule stay covered by the file guard's root check.
fn entries_except(dir: &Path, except: impl AsRef<std::ffi::OsStr>) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("listing {}", dir.display()));
        }
    };
    let mut paths = Vec::new();
    for entry in entries {
        let name = entry?.file_name();
        if name != except.as_ref()
            && name
                .to_str()
                .is_some_and(|n| !n.contains(['\n', '\r', ')']))
        {
            paths.push(dir.join(name));
        }
    }
    Ok(paths)
}

/// Best effort: profiles are recreated on demand, so failures only leave clutter.
fn remove_stale_profiles(sessions: &Path, current: &Path) {
    let Ok(entries) = fs::read_dir(sessions) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let stale = fs::metadata(path.join("source.json"))
            .and_then(|m| m.modified())
            .is_ok_and(|modified| modified.elapsed().is_ok_and(|age| age > STALE_PROFILE));
        if path == current || !stale {
            continue;
        }
        // A concurrent launch holds this lock while refreshing the profile.
        let Ok(lock) = fs::OpenOptions::new()
            .write(true)
            .open(path.join("profile.lock"))
        else {
            continue;
        };
        if lock.try_lock_exclusive().is_ok() {
            let _ = fs::remove_dir_all(&path);
        }
    }
}

fn absolute(path: &Path) -> Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    match path.canonicalize() {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(path),
        Err(error) => Err(error).context("resolving native configuration path"),
    }
}

fn read_object(path: &Path) -> Result<Value> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(json!({})),
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };
    let value: Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("invalid JSON in {}", path.display()))?;
    if !value.is_object() {
        bail!("native configuration must be an object: {}", path.display());
    }
    Ok(value)
}

fn copy_fields(target: &mut Value, source: &Value, names: &[&str]) -> Result<()> {
    let target = target
        .as_object_mut()
        .context("native integration state must be an object")?;
    for name in names {
        match source.get(*name) {
            Some(value) => {
                target.insert((*name).to_owned(), value.clone());
            }
            None => {
                target.remove(*name);
            }
        }
    }
    Ok(())
}

fn private_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)?;
        if fs::symlink_metadata(path)?.file_type().is_symlink() {
            bail!("native profile directory is a symlink");
        }
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    fs::create_dir_all(path)?;
    Ok(())
}

fn private_file(path: &Path) -> Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    Ok(options.open(path)?)
}

fn write_json(path: &Path, value: &Value) -> Result<()> {
    let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut file = private_file(&temporary)?;
        serde_json::to_writer(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

fn ensure_link(link: &Path, target: &Path) -> Result<()> {
    match fs::symlink_metadata(link) {
        Ok(metadata) if metadata.file_type().is_symlink() && fs::read_link(link)? == target => {
            return Ok(());
        }
        Ok(_) => bail!(
            "unexpected content at native profile link {}",
            link.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink(target, link)?;
    #[cfg(not(unix))]
    bail!("native profiles require Unix symbolic links");
    Ok(())
}
