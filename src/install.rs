use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use toml_edit::{DocumentMut, Item, value};

const DESKTOP_ENV: &str = "CODEX_API_BASE_URL";

#[derive(Serialize, Deserialize)]
struct ClaudeInstallRecord {
    destination: PathBuf,
    installed_url: String,
    previous: Option<serde_json::Value>,
}

fn claude_settings(path: &Path) -> Result<serde_json::Value> {
    let doc = match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).context("parse Claude settings.json")?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => serde_json::json!({}),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(
        doc.is_object() && doc.get("env").is_none_or(serde_json::Value::is_object),
        "Claude settings and env must be objects"
    );
    Ok(doc)
}

pub fn install_claude(settings: &Path, record_path: &Path, url: &str) -> Result<()> {
    let destination = resolve_destination(&std::path::absolute(settings)?)?;
    let mut doc = claude_settings(&destination)?;
    let previous = doc
        .get("env")
        .and_then(|env| env.get("ANTHROPIC_BASE_URL"))
        .cloned();
    let record = if record_path.exists() {
        let record: ClaudeInstallRecord = serde_json::from_slice(&fs::read(record_path)?)?;
        anyhow::ensure!(
            record.destination == destination && record.installed_url == url,
            "uninstall the existing Claude gateway before changing its URL or settings path"
        );
        anyhow::ensure!(
            previous.as_ref().and_then(|v| v.as_str()) == Some(url) || previous == record.previous,
            "ANTHROPIC_BASE_URL changed since install"
        );
        record
    } else {
        ClaudeInstallRecord {
            destination: destination.clone(),
            installed_url: url.into(),
            previous,
        }
    };
    if doc.get("env").is_none() {
        doc["env"] = serde_json::json!({});
    }
    doc["env"]["ANTHROPIC_BASE_URL"] = url.into();
    atomic_write(record_path, &serde_json::to_vec_pretty(&record)?)?;
    atomic_write(&destination, &serde_json::to_vec_pretty(&doc)?)
}

pub fn uninstall_claude(record_path: &Path) -> Result<()> {
    if !record_path.exists() {
        return Ok(());
    }
    let record: ClaudeInstallRecord = serde_json::from_slice(&fs::read(record_path)?)?;
    let mut doc = claude_settings(&record.destination)?;
    let current = doc.get("env").and_then(|v| v.get("ANTHROPIC_BASE_URL"));
    anyhow::ensure!(
        current.and_then(|v| v.as_str()) == Some(&record.installed_url)
            || current == record.previous.as_ref(),
        "ANTHROPIC_BASE_URL changed since install; refusing to overwrite it"
    );
    if let Some(previous) = record.previous {
        doc["env"]["ANTHROPIC_BASE_URL"] = previous;
    } else if let Some(env) = doc.get_mut("env").and_then(|v| v.as_object_mut()) {
        env.remove("ANTHROPIC_BASE_URL");
    }
    atomic_write(&record.destination, &serde_json::to_vec_pretty(&doc)?)?;
    fs::remove_file(record_path)?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct DesktopInstallRecord {
    installed_url: String,
    // None and Some("") are deliberately distinct launchd environment states.
    previous_url: Option<String>,
}

trait DesktopEnvironment {
    fn get(&self) -> Result<Option<String>>;
    fn set(&self, value: Option<&str>) -> Result<()>;
}

struct LaunchctlEnvironment;

impl DesktopEnvironment for LaunchctlEnvironment {
    fn get(&self) -> Result<Option<String>> {
        if !cfg!(target_os = "macos") {
            bail!("Desktop environment installation requires macOS")
        }
        let output = std::process::Command::new("/bin/launchctl")
            .args(["getenv", DESKTOP_ENV])
            .output()
            .context("read Desktop launch environment")?;
        if !output.status.success() {
            bail!("launchctl getenv failed; Desktop environment was not changed")
        }
        decode_launchctl_environment(output.stdout)
    }

    fn set(&self, value: Option<&str>) -> Result<()> {
        if !cfg!(target_os = "macos") {
            bail!("Desktop environment installation requires macOS")
        }
        let mut command = std::process::Command::new("/bin/launchctl");
        match value {
            Some(value) => {
                command.args(["setenv", DESKTOP_ENV, value]);
            }
            None => {
                command.args(["unsetenv", DESKTOP_ENV]);
            }
        }
        let output = command
            .output()
            .context("update Desktop launch environment")?;
        if !output.status.success() {
            bail!("launchctl failed to update Desktop environment; recovery record retained")
        }
        Ok(())
    }
}

fn decode_launchctl_environment(stdout: Vec<u8>) -> Result<Option<String>> {
    // launchctl succeeds without output for an absent key; an empty value emits a newline.
    if stdout.is_empty() {
        return Ok(None);
    }
    let mut value = String::from_utf8(stdout).context("Desktop environment is not UTF-8")?;
    // launchctl appends one newline; do not trim user-owned whitespace.
    if value.ends_with('\n') {
        value.pop();
    }
    Ok(Some(value))
}

pub fn install_desktop(record_path: &Path, url: &str) -> Result<()> {
    install_desktop_with(&LaunchctlEnvironment, record_path, url)
}

pub fn check_desktop_install(record_path: &Path, url: &str) -> Result<()> {
    prepare_desktop_install(&LaunchctlEnvironment, record_path, url).map(|_| ())
}

pub fn uninstall_desktop(record_path: &Path) -> Result<()> {
    uninstall_desktop_with(&LaunchctlEnvironment, record_path)
}

fn desktop_record(path: &Path) -> Result<Option<DesktopInstallRecord>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(
            serde_json::from_slice(&bytes).context("parse Desktop recovery record")?,
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).context("read Desktop recovery record"),
    }
}

fn prepare_desktop_install(
    environment: &impl DesktopEnvironment,
    path: &Path,
    url: &str,
) -> Result<(DesktopInstallRecord, Option<String>)> {
    let uri: hyper::Uri = url.parse().context("invalid Desktop backend URL")?;
    let secret = uri
        .path()
        .strip_suffix("/backend-api")
        .and_then(|path| path.strip_prefix('/'));
    if uri.scheme_str() != Some("http")
        || uri.host() != Some("localhost")
        || uri.port_u16().is_none_or(|port| port == 0)
        || uri
            .authority()
            .is_some_and(|authority| authority.as_str().contains('@'))
        || uri.query().is_some()
        || secret.is_none_or(|secret| secret.len() < 16 || secret.contains('/'))
    {
        bail!("Desktop backend URL must be an authenticated localhost backend path")
    }
    let current = environment.get()?;
    let record = match desktop_record(path)? {
        Some(record) => {
            if record.installed_url != url {
                bail!("uninstall the existing Desktop integration before changing its URL")
            }
            if current.as_deref() != Some(&record.installed_url) && current != record.previous_url {
                bail!("CODEX_API_BASE_URL changed since install; refusing to overwrite it")
            }
            record
        }
        None => DesktopInstallRecord {
            installed_url: url.to_owned(),
            previous_url: current.clone(),
        },
    };
    Ok((record, current))
}

fn install_desktop_with(
    environment: &impl DesktopEnvironment,
    path: &Path,
    url: &str,
) -> Result<()> {
    let (record, current) = prepare_desktop_install(environment, path, url)?;
    // Persist recovery first so a crash or failed launchctl never loses the original value.
    atomic_write(path, &serde_json::to_vec_pretty(&record)?)?;
    if current.as_deref() != Some(url) {
        environment.set(Some(url))?;
    }
    if environment.get()?.as_deref() != Some(url) {
        bail!("Desktop environment changed during install; recovery record retained")
    }
    Ok(())
}

fn uninstall_desktop_with(environment: &impl DesktopEnvironment, path: &Path) -> Result<()> {
    let Some(record) = desktop_record(path)? else {
        return Ok(());
    };
    let current = environment.get()?;
    if current != record.previous_url {
        if current.as_deref() != Some(&record.installed_url) {
            bail!("CODEX_API_BASE_URL changed since install; refusing to overwrite it")
        }
        environment.set(record.previous_url.as_deref())?;
    }
    if environment.get()? != record.previous_url {
        bail!("Desktop environment changed during restore; recovery record retained")
    }
    fs::remove_file(path).context("remove Desktop recovery record")?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InstallRecord {
    pub codex_config: PathBuf,
    pub installed_url: String,
    pub previous_url: Option<String>,
}

/// The install record belongs to a Codex configuration, if any.
pub fn installed_record(record_path: &Path) -> Option<InstallRecord> {
    read_install_record(record_path).ok().flatten()
}

/// The other downstream URL shape for the same listener: Comradex serves both
/// `.../<secret>/v1` (older clients) and `.../<secret>/backend-api/codex`
/// (recommended for Codex 0.153+ with `context_management.experimental_mode`),
/// so either value can be swapped in by hand without reinstalling.
pub fn alternate_url(installed_url: &str) -> Option<String> {
    if let Some(base) = installed_url.strip_suffix("/backend-api/codex") {
        Some(format!("{base}/v1"))
    } else {
        installed_url
            .strip_suffix("/v1")
            .map(|base| format!("{base}/backend-api/codex"))
    }
}

/// The `http://<listener>/<secret>` prefix shared by both downstream shapes;
/// `None` when the URL is not a Comradex shape we own.
fn owned_base(url: &str) -> Option<&str> {
    url.strip_suffix("/backend-api/codex")
        .or_else(|| url.strip_suffix("/v1"))
}

/// Whether `current` is the recorded Comradex URL or its hand-switched
/// alternate shape (`.../<secret>/v1` <-> `.../<secret>/backend-api/codex`,
/// same listener + secret). Anything else (different host/port/secret or a
/// user-customized URL) is foreign and must still refuse.
fn is_owned_url(current: &str, recorded: &str) -> bool {
    if current == recorded {
        return true;
    }
    match (owned_base(current), owned_base(recorded)) {
        (Some(current_base), Some(recorded_base)) => current_base == recorded_base,
        _ => false,
    }
}

pub fn install(codex_config: &Path, record_path: &Path, url: &str) -> Result<String> {
    let destination = resolve_destination(codex_config)?;
    let original = match fs::read_to_string(&destination) {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e).with_context(|| format!("read {}", destination.display())),
    };
    let mut doc = original
        .parse::<DocumentMut>()
        .context("parse Codex config.toml")?;
    let installed_url = if context_management_experimental_mode(&doc) {
        let base = url
            .strip_suffix("/v1")
            .context("Comradex install URL must end in /v1")?;
        format!("{base}/backend-api/codex")
    } else {
        url.to_owned()
    };
    let current_url = doc
        .get("openai_base_url")
        .and_then(Item::as_str)
        .map(str::to_owned);
    let previous_url = match read_install_record(record_path)? {
        Some(existing) => {
            if existing.codex_config != destination {
                bail!(
                    "install record belongs to {}; uninstall it before installing into {}",
                    existing.codex_config.display(),
                    destination.display()
                )
            }
            let owned = match current_url.as_deref() {
                Some(current) => is_owned_url(current, &existing.installed_url),
                None => false,
            };
            if !owned {
                bail!(
                    "openai_base_url changed since the previous Comradex install; refusing to replace its recovery record"
                )
            }
            existing.previous_url
        }
        None => current_url,
    };
    doc["openai_base_url"] = value(&installed_url);
    atomic_write(&destination, doc.to_string().as_bytes())?;
    let record = InstallRecord {
        codex_config: destination.clone(),
        installed_url: installed_url.clone(),
        previous_url,
    };
    if let Err(error) = atomic_write(record_path, &serde_json::to_vec_pretty(&record)?) {
        atomic_write(&destination, original.as_bytes())
            .context("roll back Codex config after install-record failure")?;
        return Err(error).context("write install record");
    }
    Ok(installed_url)
}

fn context_management_experimental_mode(doc: &DocumentMut) -> bool {
    doc.get("features")
        .and_then(Item::as_table_like)
        .and_then(|features| features.get("context_management"))
        .and_then(Item::as_table_like)
        .and_then(|context_management| context_management.get("experimental_mode"))
        .and_then(Item::as_bool)
        == Some(true)
}

fn read_install_record(path: &Path) -> Result<Option<InstallRecord>> {
    match fs::read(path) {
        Ok(bytes) => {
            Ok(Some(serde_json::from_slice(&bytes).with_context(|| {
                format!("parse install record {}", path.display())
            })?))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
    }
}

pub fn uninstall(record_path: &Path) -> Result<()> {
    let Some(record) = read_install_record(record_path)? else {
        return Ok(());
    };
    let destination = recorded_destination(&record.codex_config)?;
    let original = fs::read_to_string(&destination)?;
    let mut doc = original
        .parse::<DocumentMut>()
        .context("parse Codex config.toml")?;
    let current = doc.get("openai_base_url").and_then(Item::as_str);
    if current == record.previous_url.as_deref() {
        fs::remove_file(record_path)?;
        return Ok(());
    }
    let owned = match current {
        Some(current) => is_owned_url(current, &record.installed_url),
        None => false,
    };
    if !owned {
        bail!("openai_base_url changed since install; refusing to overwrite user configuration")
    }
    match record.previous_url {
        Some(v) => doc["openai_base_url"] = value(v),
        None => {
            doc.remove("openai_base_url");
        }
    }
    atomic_write(&destination, doc.to_string().as_bytes())?;
    fs::remove_file(record_path)?;
    Ok(())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let write = || -> Result<()> {
        let parent = path.parent().unwrap_or(Path::new("."));
        fs::create_dir_all(parent)?;
        let mut temp = tempfile::NamedTempFile::new_in(parent)?;
        use std::io::Write;
        temp.write_all(bytes)?;
        if let Ok(metadata) = fs::metadata(path) {
            temp.as_file().set_permissions(metadata.permissions())?;
        }
        temp.as_file().sync_all()?;
        temp.persist(path).map_err(|e| e.error)?;
        Ok(())
    };
    write().with_context(|| format!("write {}", path.display()))
}

fn resolve_destination(path: &Path) -> Result<PathBuf> {
    Ok(match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            fs::canonicalize(path).with_context(|| format!("resolve symlink {}", path.display()))?
        }
        Ok(_) => fs::canonicalize(path).with_context(|| format!("resolve {}", path.display()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let absolute = if path.is_absolute() {
                path.to_owned()
            } else {
                std::env::current_dir()?.join(path)
            };
            let parent = absolute.parent().unwrap_or(Path::new("."));
            match fs::canonicalize(parent) {
                Ok(parent) => parent.join(
                    absolute
                        .file_name()
                        .context("configuration destination has no file name")?,
                ),
                Err(parent_error) if parent_error.kind() == std::io::ErrorKind::NotFound => {
                    absolute
                }
                Err(parent_error) => {
                    return Err(parent_error)
                        .with_context(|| format!("resolve parent {}", parent.display()));
                }
            }
        }
        Err(error) => return Err(error).with_context(|| format!("inspect {}", path.display())),
    })
}

fn recorded_destination(path: &Path) -> Result<PathBuf> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!(
                "recorded Codex configuration target {} became a symlink; refusing to follow a new target",
                path.display()
            )
        }
        Ok(_) => Ok(path.to_owned()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(path.to_owned()),
        Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn claude_install_restores_only_its_url_and_preserves_other_edits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.json");
        let record = dir.path().join("claude-install.json");
        for previous in [None, Some("https://previous.example")] {
            let mut original =
                serde_json::json!({"permissions":{"allow":["Read"]},"env":{"KEEP":"yes"}});
            if let Some(url) = previous {
                original["env"]["ANTHROPIC_BASE_URL"] = url.into();
            }
            fs::write(&path, serde_json::to_vec(&original).unwrap()).unwrap();
            install_claude(&path, &record, "http://127.0.0.1:10101/secret").unwrap();
            install_claude(&path, &record, "http://127.0.0.1:10101/secret").unwrap();
            let mut installed = claude_settings(&path).unwrap();
            assert_eq!(installed["permissions"], original["permissions"]);
            assert_eq!(installed["env"].as_object().unwrap().len(), 2);
            installed["env"]["LATER"] = "kept".into();
            fs::write(&path, serde_json::to_vec(&installed).unwrap()).unwrap();
            uninstall_claude(&record).unwrap();
            original["env"]["LATER"] = "kept".into();
            assert_eq!(claude_settings(&path).unwrap(), original);
            assert!(!record.exists());
        }
    }

    #[test]
    fn claude_install_rejects_invalid_settings_and_uninstall_respects_drift() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.json");
        let record = dir.path().join("record.json");
        fs::write(&path, b"{\"env\":[]}").unwrap();
        assert!(install_claude(&path, &record, "http://127.0.0.1:10101/secret").is_err());
        assert!(!record.exists());
        fs::write(&path, b"{}").unwrap();
        install_claude(&path, &record, "http://127.0.0.1:10101/secret").unwrap();
        let other = br#"{"env":{"ANTHROPIC_BASE_URL":"https://new.example"}}"#;
        fs::write(&path, other).unwrap();
        assert!(uninstall_claude(&record).is_err());
        assert_eq!(fs::read(&path).unwrap(), other);
        assert!(record.exists());
    }
    #[test]
    fn desktop_launchctl_output_preserves_absent_empty_and_trailing_newlines() {
        assert_eq!(decode_launchctl_environment(vec![]).unwrap(), None);
        assert_eq!(
            decode_launchctl_environment(b"\n".to_vec()).unwrap(),
            Some(String::new())
        );
        assert_eq!(
            decode_launchctl_environment(b" value \n\n".to_vec()).unwrap(),
            Some(" value \n".into())
        );
    }
    struct MockDesktopEnvironment {
        value: std::cell::RefCell<Option<String>>,
        fail_set: std::cell::Cell<bool>,
    }

    impl DesktopEnvironment for MockDesktopEnvironment {
        fn get(&self) -> Result<Option<String>> {
            Ok(self.value.borrow().clone())
        }
        fn set(&self, value: Option<&str>) -> Result<()> {
            if self.fail_set.get() {
                bail!("mock launchctl failure");
            }
            *self.value.borrow_mut() = value.map(str::to_owned);
            Ok(())
        }
    }

    #[test]
    fn desktop_install_restores_exact_unset_empty_and_whitespace_values() {
        for original in [
            None,
            Some(String::new()),
            Some("  https://old.example/backend-api\n ".into()),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let record = dir.path().join("desktop.json");
            let environment = MockDesktopEnvironment {
                value: std::cell::RefCell::new(original.clone()),
                fail_set: std::cell::Cell::new(false),
            };
            for _ in 0..2 {
                install_desktop_with(
                    &environment,
                    &record,
                    "http://localhost:8000/0123456789abcdef/backend-api",
                )
                .unwrap();
            }
            uninstall_desktop_with(&environment, &record).unwrap();
            assert_eq!(environment.get().unwrap(), original);
            assert!(!record.exists());
            uninstall_desktop_with(&environment, &record).unwrap();
        }
    }

    #[test]
    fn desktop_foreign_changes_preserve_environment_and_recovery_record() {
        let dir = tempfile::tempdir().unwrap();
        let record = dir.path().join("desktop.json");
        let environment = MockDesktopEnvironment {
            value: std::cell::RefCell::new(None),
            fail_set: std::cell::Cell::new(false),
        };
        install_desktop_with(
            &environment,
            &record,
            "http://localhost:8000/0123456789abcdef/backend-api",
        )
        .unwrap();
        environment.set(Some("https://foreign.example")).unwrap();
        assert!(
            install_desktop_with(
                &environment,
                &record,
                "http://localhost:8000/0123456789abcdef/backend-api"
            )
            .is_err()
        );
        assert!(uninstall_desktop_with(&environment, &record).is_err());
        assert_eq!(
            environment.get().unwrap().as_deref(),
            Some("https://foreign.example")
        );
        assert!(record.exists());
    }

    #[test]
    fn desktop_failed_environment_write_keeps_recoverable_original() {
        let dir = tempfile::tempdir().unwrap();
        let record = dir.path().join("desktop.json");
        let environment = MockDesktopEnvironment {
            value: std::cell::RefCell::new(Some(String::new())),
            fail_set: std::cell::Cell::new(true),
        };
        assert!(
            install_desktop_with(
                &environment,
                &record,
                "http://localhost:8000/0123456789abcdef/backend-api"
            )
            .is_err()
        );
        assert!(record.exists());
        environment.fail_set.set(false);
        install_desktop_with(
            &environment,
            &record,
            "http://localhost:8000/0123456789abcdef/backend-api",
        )
        .unwrap();
        environment.fail_set.set(true);
        assert!(uninstall_desktop_with(&environment, &record).is_err());
        assert!(record.exists());
        environment.fail_set.set(false);
        uninstall_desktop_with(&environment, &record).unwrap();
        assert_eq!(environment.get().unwrap(), Some(String::new()));
    }
    #[test]
    fn install_round_trip_preserves_everything_else() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        let record = dir.path().join("install.json");
        fs::write(&config, "model = \"gpt-test\"\n[features]\nfoo = true\n").unwrap();
        install(&config, &record, "http://127.0.0.1:1/s/v1").unwrap();
        assert!(
            fs::read_to_string(&config)
                .unwrap()
                .contains("openai_base_url")
        );
        uninstall(&record).unwrap();
        let restored = fs::read_to_string(config).unwrap();
        assert!(restored.contains("model = \"gpt-test\""));
        assert!(restored.contains("foo = true"));
        assert!(!restored.contains("openai_base_url"));
    }

    #[test]
    fn install_uses_backend_api_codex_when_context_management_is_enabled() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        let record = dir.path().join("install.json");
        fs::write(
            &config,
            "[features.context_management]\nexperimental_mode = true\n",
        )
        .unwrap();

        let installed = install(&config, &record, "http://127.0.0.1:10100/secret/v1").unwrap();

        assert_eq!(installed, "http://127.0.0.1:10100/secret/backend-api/codex");
        assert!(
            fs::read_to_string(&config)
                .unwrap()
                .contains("http://127.0.0.1:10100/secret/backend-api/codex")
        );
    }

    #[test]
    fn install_uses_v1_when_context_management_is_disabled_or_absent() {
        for config_text in [
            "[features.context_management]\nexperimental_mode = false\n",
            "model = \"gpt-test\"\n",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let config = dir.path().join("config.toml");
            let record = dir.path().join("install.json");
            fs::write(&config, config_text).unwrap();

            let installed = install(&config, &record, "http://127.0.0.1:10100/secret/v1").unwrap();

            assert_eq!(installed, "http://127.0.0.1:10100/secret/v1");
        }
    }

    #[test]
    fn alternate_url_swaps_the_two_supported_downstream_shapes() {
        assert_eq!(
            alternate_url("http://127.0.0.1:10100/secret/v1").as_deref(),
            Some("http://127.0.0.1:10100/secret/backend-api/codex")
        );
        assert_eq!(
            alternate_url("http://127.0.0.1:10100/secret/backend-api/codex").as_deref(),
            Some("http://127.0.0.1:10100/secret/v1")
        );
        assert_eq!(alternate_url("http://127.0.0.1:10100/secret/v2"), None);
    }

    #[test]
    fn repeated_install_preserves_original_pre_comradex_url() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        let record = dir.path().join("install.json");
        fs::write(
            &config,
            "model = \"gpt-test\"\nopenai_base_url = \"http://127.0.0.1:10100/v1\"\n[features.context_management]\nexperimental_mode = true\n",
        )
        .unwrap();

        let first = install(&config, &record, "http://127.0.0.1:10100/secret-a/v1").unwrap();
        let second = install(&config, &record, "http://127.0.0.1:10100/secret-b/v1").unwrap();
        uninstall(&record).unwrap();

        assert!(first.ends_with("/secret-a/backend-api/codex"));
        assert!(second.ends_with("/secret-b/backend-api/codex"));
        let restored = fs::read_to_string(&config).unwrap();
        assert!(restored.contains("http://127.0.0.1:10100/v1"));
        assert!(!restored.contains("secret-a"));
        assert!(!restored.contains("secret-b"));
    }

    #[test]
    fn uninstall_is_idempotent_after_restore_before_record_deletion() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        let record = dir.path().join("install.json");
        fs::write(
            &config,
            "openai_base_url = \"http://127.0.0.1:10100/original/v1\"\n",
        )
        .unwrap();
        install(&config, &record, "http://127.0.0.1:10100/comradex/v1").unwrap();
        fs::write(
            &config,
            "openai_base_url = \"http://127.0.0.1:10100/original/v1\"\n",
        )
        .unwrap();

        uninstall(&record).unwrap();
        assert!(!record.exists());
        uninstall(&record).unwrap();
    }

    #[test]
    fn reinstall_refuses_to_overwrite_recovery_after_external_url_change() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        let record = dir.path().join("install.json");
        fs::write(&config, "model = \"gpt-test\"\n").unwrap();
        install(&config, &record, "http://127.0.0.1:10100/secret-a/v1").unwrap();
        fs::write(
            &config,
            "model = \"gpt-test\"\nopenai_base_url = \"http://127.0.0.1:9999/manual/v1\"\n",
        )
        .unwrap();

        let error = install(&config, &record, "http://127.0.0.1:10100/secret-b/v1").unwrap_err();

        assert!(error.to_string().contains("changed since"));
        assert!(fs::read_to_string(&config).unwrap().contains("9999/manual"));
    }

    #[test]
    fn hand_switched_alternate_shape_reinstall_and_uninstall_succeed() {
        // /v1 install, hand-switched to the backend shape.
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        let record = dir.path().join("install.json");
        fs::write(
            &config,
            "model = \"gpt-test\"\nopenai_base_url = \"http://127.0.0.1:10100/original/v1\"\n",
        )
        .unwrap();
        let installed = install(&config, &record, "http://127.0.0.1:10100/secret/v1").unwrap();
        assert_eq!(installed, "http://127.0.0.1:10100/secret/v1");
        let alternate = alternate_url(&installed).unwrap();
        fs::write(
            &config,
            fs::read_to_string(&config)
                .unwrap()
                .replace(&installed, &alternate),
        )
        .unwrap();

        // Reinstall from the alternate shape works and keeps the original recovery value.
        let reinstalled = install(&config, &record, "http://127.0.0.1:10100/secret/v1").unwrap();
        assert_eq!(reinstalled, "http://127.0.0.1:10100/secret/v1");
        let stored: InstallRecord = serde_json::from_slice(&fs::read(&record).unwrap()).unwrap();
        assert_eq!(stored.installed_url, reinstalled);
        assert_eq!(
            stored.previous_url.as_deref(),
            Some("http://127.0.0.1:10100/original/v1")
        );

        // Uninstall from the alternate shape restores the original value.
        fs::write(
            &config,
            fs::read_to_string(&config)
                .unwrap()
                .replace(&reinstalled, &alternate),
        )
        .unwrap();
        uninstall(&record).unwrap();
        let restored = fs::read_to_string(&config).unwrap();
        assert!(restored.contains("http://127.0.0.1:10100/original/v1"));
        assert!(!restored.contains("secret"));
        assert!(!record.exists());

        // Backend-shape install, hand-switched to /v1.
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        let record = dir.path().join("install.json");
        fs::write(
            &config,
            "model = \"gpt-test\"\nopenai_base_url = \"http://127.0.0.1:10100/original/v1\"\n[features.context_management]\nexperimental_mode = true\n",
        )
        .unwrap();
        let installed = install(&config, &record, "http://127.0.0.1:10100/secret/v1").unwrap();
        assert_eq!(installed, "http://127.0.0.1:10100/secret/backend-api/codex");
        let alternate = alternate_url(&installed).unwrap();
        assert_eq!(alternate, "http://127.0.0.1:10100/secret/v1");
        fs::write(
            &config,
            fs::read_to_string(&config)
                .unwrap()
                .replace(&installed, &alternate),
        )
        .unwrap();

        let reinstalled = install(&config, &record, "http://127.0.0.1:10100/secret/v1").unwrap();
        assert_eq!(
            reinstalled,
            "http://127.0.0.1:10100/secret/backend-api/codex"
        );
        fs::write(
            &config,
            fs::read_to_string(&config)
                .unwrap()
                .replace(&reinstalled, &alternate),
        )
        .unwrap();
        uninstall(&record).unwrap();
        let restored = fs::read_to_string(&config).unwrap();
        assert!(restored.contains("http://127.0.0.1:10100/original/v1"));
        assert!(!restored.contains("secret"));
        assert!(!record.exists());
    }

    #[test]
    fn uninstall_from_hand_switched_shape_without_reinstall_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        let record = dir.path().join("install.json");
        fs::write(&config, "model = \"gpt-test\"\n").unwrap();
        let installed = install(&config, &record, "http://127.0.0.1:10100/secret/v1").unwrap();
        let alternate = alternate_url(&installed).unwrap();
        fs::write(
            &config,
            fs::read_to_string(&config)
                .unwrap()
                .replace(&installed, &alternate),
        )
        .unwrap();

        uninstall(&record).unwrap();

        let restored = fs::read_to_string(&config).unwrap();
        assert!(restored.contains("model = \"gpt-test\""));
        assert!(!restored.contains("openai_base_url"));
        assert!(!record.exists());
    }

    #[test]
    fn foreign_url_still_refuses_reinstall_and_uninstall() {
        for foreign in [
            "http://127.0.0.1:9999/manual/v1",
            "http://127.0.0.1:10100/other-secret/v1",
            "http://127.0.0.1:10100/other-secret/backend-api/codex",
            "http://127.0.0.1:10100/secret/v2",
            "https://api.openai.com/v1",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let config = dir.path().join("config.toml");
            let record = dir.path().join("install.json");
            fs::write(&config, "model = \"gpt-test\"\n").unwrap();
            let installed = install(&config, &record, "http://127.0.0.1:10100/secret/v1").unwrap();
            fs::write(
                &config,
                format!("model = \"gpt-test\"\nopenai_base_url = \"{foreign}\"\n"),
            )
            .unwrap();

            let reinstall_error =
                install(&config, &record, "http://127.0.0.1:10100/secret/v1").unwrap_err();
            assert!(
                reinstall_error.to_string().contains("changed since"),
                "reinstall should refuse {foreign}: {reinstall_error:#}"
            );
            assert!(
                fs::read_to_string(&config).unwrap().contains(foreign),
                "failed reinstall must leave {foreign} in place"
            );

            let uninstall_error = uninstall(&record).unwrap_err();
            assert!(
                uninstall_error.to_string().contains("changed since"),
                "uninstall should refuse {foreign}: {uninstall_error:#}"
            );
            assert!(
                fs::read_to_string(&config).unwrap().contains(foreign),
                "failed uninstall must leave {foreign} in place"
            );
            assert!(
                record.exists(),
                "failed uninstall must keep {foreign} record"
            );
            assert_ne!(foreign, installed);
        }
    }

    #[cfg(unix)]
    #[test]
    fn install_preserves_codex_config_symlink() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("dotfiles-config.toml");
        let config = dir.path().join("config.toml");
        let record = dir.path().join("install.json");
        fs::write(&target, "model = \"gpt-test\"\n").unwrap();
        symlink(&target, &config).unwrap();

        install(&config, &record, "http://127.0.0.1:1/s/v1").unwrap();
        assert!(
            fs::symlink_metadata(&config)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(
            fs::read_to_string(&target)
                .unwrap()
                .contains("openai_base_url")
        );

        uninstall(&record).unwrap();
        assert!(
            fs::symlink_metadata(&config)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(
            !fs::read_to_string(&target)
                .unwrap()
                .contains("openai_base_url")
        );
    }

    #[cfg(unix)]
    #[test]
    fn uninstall_uses_recorded_target_after_symlink_is_retargeted() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let first_target = dir.path().join("first.toml");
        let second_target = dir.path().join("second.toml");
        let config = dir.path().join("config.toml");
        let record = dir.path().join("install.json");
        fs::write(&first_target, "model = \"first\"\n").unwrap();
        fs::write(&second_target, "model = \"second\"\n").unwrap();
        symlink(&first_target, &config).unwrap();

        install(&config, &record, "http://127.0.0.1:1/s/v1").unwrap();
        fs::remove_file(&config).unwrap();
        symlink(&second_target, &config).unwrap();
        uninstall(&record).unwrap();

        assert_eq!(
            fs::read_to_string(&first_target).unwrap(),
            "model = \"first\"\n"
        );
        assert_eq!(
            fs::read_to_string(&second_target).unwrap(),
            "model = \"second\"\n"
        );
        assert!(
            fs::symlink_metadata(&config)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[cfg(unix)]
    #[test]
    fn missing_config_uses_stable_symlinked_parent() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let target_dir = dir.path().join("dotfiles");
        let linked_dir = dir.path().join("linked-dotfiles");
        fs::create_dir_all(&target_dir).unwrap();
        symlink(&target_dir, &linked_dir).unwrap();
        let config = linked_dir.join("config.toml");
        let record = dir.path().join("install.json");

        install(&config, &record, "http://127.0.0.1:1/s/v1").unwrap();
        let stored: InstallRecord = serde_json::from_slice(&fs::read(&record).unwrap()).unwrap();

        assert_eq!(
            stored.codex_config,
            fs::canonicalize(&target_dir).unwrap().join("config.toml")
        );
        assert!(target_dir.join("config.toml").exists());
    }

    #[cfg(unix)]
    #[test]
    fn uninstall_refuses_if_recorded_target_becomes_a_symlink() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.toml");
        let replacement = dir.path().join("replacement.toml");
        let config = dir.path().join("config.toml");
        let record = dir.path().join("install.json");
        fs::write(&target, "model = \"original\"\n").unwrap();
        fs::write(&replacement, "model = \"replacement\"\n").unwrap();
        symlink(&target, &config).unwrap();
        install(&config, &record, "http://127.0.0.1:1/s/v1").unwrap();

        fs::remove_file(&target).unwrap();
        symlink(&replacement, &target).unwrap();
        let error = uninstall(&record).unwrap_err();

        assert!(error.to_string().contains("became a symlink"));
        assert_eq!(
            fs::read_to_string(&replacement).unwrap(),
            "model = \"replacement\"\n"
        );
    }
}
