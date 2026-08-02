use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use toml_edit::{DocumentMut, Item, value};

#[derive(Debug, Serialize, Deserialize)]
struct InstallRecord {
    codex_config: PathBuf,
    installed_url: String,
    previous_url: Option<String>,
}

pub fn install(codex_config: &Path, record_path: &Path, url: &str) -> Result<()> {
    let original = match fs::read_to_string(codex_config) {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e).with_context(|| format!("read {}", codex_config.display())),
    };
    let mut doc = original
        .parse::<DocumentMut>()
        .context("parse Codex config.toml")?;
    let previous_url = doc
        .get("openai_base_url")
        .and_then(Item::as_str)
        .map(str::to_owned);
    doc["openai_base_url"] = value(url);
    atomic_write(codex_config, doc.to_string().as_bytes())?;
    let record = InstallRecord {
        codex_config: codex_config.to_owned(),
        installed_url: url.to_owned(),
        previous_url,
    };
    if let Err(error) = atomic_write(record_path, &serde_json::to_vec_pretty(&record)?) {
        atomic_write(codex_config, original.as_bytes())
            .context("roll back Codex config after install-record failure")?;
        return Err(error).context("write install record");
    }
    Ok(())
}

pub fn uninstall(record_path: &Path) -> Result<()> {
    let record: InstallRecord = serde_json::from_slice(
        &fs::read(record_path).with_context(|| format!("read {}", record_path.display()))?,
    )?;
    let original = fs::read_to_string(&record.codex_config)?;
    let mut doc = original
        .parse::<DocumentMut>()
        .context("parse Codex config.toml")?;
    let current = doc.get("openai_base_url").and_then(Item::as_str);
    if current != Some(record.installed_url.as_str()) {
        bail!("openai_base_url changed since install; refusing to overwrite user configuration")
    }
    match record.previous_url {
        Some(v) => doc["openai_base_url"] = value(v),
        None => {
            doc.remove("openai_base_url");
        }
    }
    atomic_write(&record.codex_config, doc.to_string().as_bytes())?;
    fs::remove_file(record_path)?;
    Ok(())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    use std::io::Write;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|e| e.error)?;
    Ok(())
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
}
