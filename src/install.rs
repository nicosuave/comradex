use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use toml_edit::{DocumentMut, Item, value};

#[derive(Debug, Serialize, Deserialize)]
pub struct InstallRecord {
    pub codex_config: PathBuf,
    pub installed_url: String,
    pub previous_url: Option<String>,
}

/// The Codex configuration Comradex is currently installed into, if any.
pub fn installed_record(record_path: &Path) -> Option<InstallRecord> {
    read_install_record(record_path).ok().flatten()
}

pub fn install(codex_config: &Path, record_path: &Path, url: &str) -> Result<()> {
    let destination = resolve_destination(codex_config)?;
    let original = match fs::read_to_string(&destination) {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e).with_context(|| format!("read {}", destination.display())),
    };
    let mut doc = original
        .parse::<DocumentMut>()
        .context("parse Codex config.toml")?;
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
            if current_url.as_deref() != Some(existing.installed_url.as_str()) {
                bail!(
                    "openai_base_url changed since the previous Comradex install; refusing to replace its recovery record"
                )
            }
            existing.previous_url
        }
        None => current_url,
    };
    doc["openai_base_url"] = value(url);
    atomic_write(&destination, doc.to_string().as_bytes())?;
    let record = InstallRecord {
        codex_config: destination.clone(),
        installed_url: url.to_owned(),
        previous_url,
    };
    if let Err(error) = atomic_write(record_path, &serde_json::to_vec_pretty(&record)?) {
        atomic_write(&destination, original.as_bytes())
            .context("roll back Codex config after install-record failure")?;
        return Err(error).context("write install record");
    }
    Ok(())
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
    if current != Some(record.installed_url.as_str()) {
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
    fn repeated_install_preserves_original_pre_comradex_url() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        let record = dir.path().join("install.json");
        fs::write(
            &config,
            "model = \"gpt-test\"\nopenai_base_url = \"http://127.0.0.1:10100/v1\"\n",
        )
        .unwrap();

        install(&config, &record, "http://127.0.0.1:10100/secret-a/v1").unwrap();
        install(&config, &record, "http://127.0.0.1:10100/secret-b/v1").unwrap();
        uninstall(&record).unwrap();

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
