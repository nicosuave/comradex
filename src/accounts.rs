//! Config-file surgery for `comradex account` commands.
//!
//! These functions transform the comradex.toml text with toml_edit so user
//! formatting and comments survive. Callers are responsible for validating the
//! result through `Config::load` before persisting it.

use anyhow::{Context, Result, bail};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};
use toml_edit::{DocumentMut, Item, value};

/// Locate the login used by the requesting user's Codex CLI.
pub fn default_codex_home() -> Result<PathBuf> {
    default_codex_home_from(std::env::var_os("CODEX_HOME"), std::env::var_os("HOME"))
}

fn default_codex_home_from(
    codex_home: Option<OsString>,
    home: Option<OsString>,
) -> Result<PathBuf> {
    let path = match codex_home.filter(|value| !value.is_empty()) {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(
            home.filter(|value| !value.is_empty())
                .context("HOME is not set")?,
        )
        .join(".codex"),
    };
    let absolute = std::path::absolute(path).context("resolve Codex home")?;
    crate::config::normalize_codex_home(&absolute)
}

/// Discover a supported existing login without refreshing or changing credentials.
pub fn existing_codex_home() -> Result<PathBuf> {
    let home = default_codex_home()?;
    crate::auth::validate_existing_login(&home)?;
    Ok(home)
}

/// Link an inbound account to an existing login, keeping its pool membership and preferences.
/// The caller must validate the result with Config::load to reject duplicate homes.
pub fn connect_existing_account(text: &str, name: &str, home: &Path) -> Result<String> {
    validate_name(name)?;
    let mut doc: DocumentMut = text.parse().context("parse comradex.toml")?;
    let account = doc
        .get_mut("accounts")
        .and_then(Item::as_table_like_mut)
        .and_then(|accounts| accounts.get_mut(name))
        .with_context(|| format!("unknown account {name}"))?;
    if account.get("kind").and_then(Item::as_str) != Some("inbound") {
        bail!(
            "account {name} already has its own login; only inbound accounts can connect an existing login"
        )
    }
    let home = crate::config::normalize_codex_home(&std::path::absolute(home)?)?;
    crate::auth::validate_existing_login(&home)?;
    let path = home
        .to_str()
        .context("Codex home path is not valid UTF-8")?;
    // Preserve inline comments attached to the original kind value.
    let decor = account["kind"]
        .as_value()
        .map(|value| value.decor().clone());
    account["kind"] = value("codex_home");
    if let Some(decor) = decor {
        *account["kind"]
            .as_value_mut()
            .expect("kind is a value")
            .decor_mut() = decor;
    }
    account["path"] = value(path);
    Ok(doc.to_string())
}

/// Purging is restricted to the isolated home allocated for this account.
pub fn validate_purge_home(config_path: &Path, name: &str, home: &Path) -> Result<()> {
    validate_name(name)?;
    let config_path = std::path::absolute(config_path)?;
    let parent = config_path
        .parent()
        .context("configuration has no parent directory")?;
    let expected = parent.join("accounts").join(name);
    // Do not follow a symlink at the isolated account directory to an external login.
    if std::fs::symlink_metadata(&expected).is_ok_and(|meta| meta.file_type().is_symlink()) {
        bail!("cannot purge a linked Codex home; remove the account without --purge")
    }
    let accounts = crate::config::normalize_codex_home(&parent.join("accounts"))?;
    let home = crate::config::normalize_codex_home(&std::path::absolute(home)?)?;
    if home != accounts.join(name) {
        bail!("cannot purge an external Codex login; remove the account without --purge")
    }
    Ok(())
}

/// Account names become directory names under the config directory, so keep
/// them to a safe character set.
pub fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 256 {
        bail!("account name must be 1..=256 characters")
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        bail!("account name may only contain letters, digits, '-', and '_'")
    }
    Ok(())
}

/// Add a managed `codex_home` account and append it to a pool's members.
pub fn add_account(text: &str, name: &str, pool: &str) -> Result<String> {
    validate_name(name)?;
    let mut doc: DocumentMut = text.parse().context("parse comradex.toml")?;
    if doc
        .get("accounts")
        .and_then(|accounts| accounts.get(name))
        .is_some()
    {
        bail!("account {name} already exists")
    }
    let members = doc
        .get_mut("pools")
        .and_then(Item::as_table_like_mut)
        .and_then(|pools| pools.get_mut(pool))
        .with_context(|| format!("unknown pool {pool}"))?
        .get_mut("members")
        .and_then(Item::as_array_mut)
        .with_context(|| format!("pool {pool} has no members array"))?;
    members.push(name);
    members.fmt();
    let mut table = toml_edit::Table::new();
    table["kind"] = value("codex_home");
    table["path"] = value(format!("accounts/{name}"));
    doc["accounts"][name] = Item::Table(table);
    Ok(doc.to_string())
}

/// Remove an account from the accounts table and every pool's members.
/// Returns the new text and the removed account's `path` value, if any.
pub fn remove_account(text: &str, name: &str) -> Result<(String, Option<String>)> {
    let mut doc: DocumentMut = text.parse().context("parse comradex.toml")?;
    let accounts = doc
        .get_mut("accounts")
        .and_then(Item::as_table_like_mut)
        .context("configuration has no accounts table")?;
    let removed = accounts
        .remove(name)
        .with_context(|| format!("unknown account {name}"))?;
    let home = removed
        .get("path")
        .and_then(Item::as_str)
        .map(str::to_owned);
    if let Some(pools) = doc.get_mut("pools").and_then(Item::as_table_like_mut) {
        for (_, pool) in pools.iter_mut() {
            if let Some(members) = pool.get_mut("members").and_then(Item::as_array_mut) {
                members.retain(|member| member.as_str() != Some(name));
                members.fmt();
            }
            for field in ["preferred", "preserved"] {
                if pool.get(field).and_then(Item::as_str) == Some(name) {
                    pool.as_table_like_mut()
                        .expect("pool was already accessed as a table")
                        .remove(field);
                }
            }
        }
    }
    Ok((doc.to_string(), home))
}

/// Set or clear the preferred account for a pool while preserving surrounding formatting.
pub fn set_preferred_account(text: &str, pool_name: &str, account: Option<&str>) -> Result<String> {
    set_account_order(text, pool_name, account, "preferred", "preserved")
}

/// Set or clear the account reserved for last use in a pool.
pub fn set_preserved_account(text: &str, pool_name: &str, account: Option<&str>) -> Result<String> {
    set_account_order(text, pool_name, account, "preserved", "preferred")
}

fn set_account_order(
    text: &str,
    pool_name: &str,
    account: Option<&str>,
    field: &str,
    opposite: &str,
) -> Result<String> {
    if let Some(account) = account {
        validate_name(account)?;
    }
    let mut doc: DocumentMut = text.parse().context("parse comradex.toml")?;
    let pool = doc
        .get_mut("pools")
        .and_then(Item::as_table_like_mut)
        .and_then(|pools| pools.get_mut(pool_name))
        .with_context(|| format!("unknown pool {pool_name}"))?
        .as_table_like_mut()
        .with_context(|| format!("pool {pool_name} is not a table"))?;
    match account {
        Some(account) => {
            let is_member = pool
                .get("members")
                .and_then(Item::as_array)
                .is_some_and(|members| {
                    members
                        .iter()
                        .any(|member| member.as_str() == Some(account))
                });
            if !is_member {
                bail!("account {account} is not a member of pool {pool_name}")
            }
            if pool.get(opposite).and_then(Item::as_str) == Some(account) {
                bail!(
                    "pool {pool_name} cannot prefer and preserve the same account; clear {opposite} first"
                )
            }
            pool.insert(field, value(account));
        }
        None => {
            pool.remove(field);
        }
    }
    Ok(doc.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEMPLATE: &str = r#"[proxy]
installation_secret = "0123456789abcdef"
affinity_key = "0123456789abcdef0123456789abcdef"

# my listener
[listeners.default]
address = "127.0.0.1:10100"
pool = "default"

[pools.default]
members = ["caller"]

[accounts.caller]
kind = "inbound"
"#;

    fn login_home() -> tempfile::TempDir {
        let home = tempfile::tempdir().unwrap();
        std::fs::write(
            home.path().join("auth.json"),
            r#"{"tokens":{"access_token":"test-access-token"}}"#,
        )
        .unwrap();
        home
    }

    #[test]
    fn discover_home_honors_custom_home_and_empty_fallback() {
        let directory = tempfile::tempdir().unwrap();
        let custom = directory.path().join("custom");
        let user = directory.path().join("user");
        assert_eq!(
            default_codex_home_from(
                Some(custom.clone().into_os_string()),
                Some(user.clone().into_os_string())
            )
            .unwrap(),
            crate::config::normalize_codex_home(&custom).unwrap()
        );
        assert_eq!(
            default_codex_home_from(Some(OsString::new()), Some(user.clone().into_os_string()))
                .unwrap(),
            crate::config::normalize_codex_home(&user.join(".codex")).unwrap()
        );
        assert!(default_codex_home_from(None, None).is_err());
    }

    #[test]
    fn connect_preserves_preferences_members_comments_and_credentials() {
        let home = login_home();
        let auth_before = std::fs::read(home.path().join("auth.json")).unwrap();
        let original = set_preferred_account(TEMPLATE, "default", Some("caller"))
            .unwrap()
            .replace(
                "kind = \"inbound\"",
                "kind = \"inbound\" # existing comment",
            );
        let updated =
            connect_existing_account(&original, "caller", &home.path().join(".")).unwrap();
        let config: crate::config::Config = toml::from_str(&updated).unwrap();
        assert_eq!(config.pools["default"].preferred.as_deref(), Some("caller"));
        assert_eq!(config.pools["default"].members, vec!["caller"]);
        assert!(updated.contains("# my listener"));
        assert!(updated.contains("# existing comment"));
        let crate::config::AccountConfig::CodexHome { path } = &config.accounts["caller"] else {
            panic!("expected linked login")
        };
        assert_eq!(path, &std::fs::canonicalize(home.path()).unwrap());
        assert_eq!(
            std::fs::read(home.path().join("auth.json")).unwrap(),
            auth_before
        );
        assert!(connect_existing_account(&updated, "caller", home.path()).is_err());
        assert!(connect_existing_account(TEMPLATE, "missing", home.path()).is_err());
    }

    #[test]
    fn connect_rejects_missing_malformed_and_unsupported_logins_without_exposing_contents() {
        let home = tempfile::tempdir().unwrap();
        assert!(connect_existing_account(TEMPLATE, "caller", home.path()).is_err());
        for auth in [
            "secret-malformed-json",
            r#"{"OPENAI_API_KEY":"secret-key"}"#,
            r#"{"tokens":{"access_token":""}}"#,
        ] {
            std::fs::write(home.path().join("auth.json"), auth).unwrap();
            let error = connect_existing_account(TEMPLATE, "caller", home.path()).unwrap_err();
            assert!(!format!("{error:#}").contains("secret"));
        }
    }

    #[test]
    fn caller_validation_rejects_duplicate_linked_home() {
        let home = login_home();
        let added = add_account(TEMPLATE, "other", "default").unwrap();
        let added = added.replace(
            "path = \"accounts/other\"",
            &format!("path = {:?}", home.path().to_str().unwrap()),
        );
        let updated = connect_existing_account(&added, "caller", home.path()).unwrap();
        let config_directory = tempfile::tempdir().unwrap();
        let config_path = config_directory.path().join("comradex.toml");
        std::fs::write(&config_path, &updated).unwrap();
        assert!(crate::config::Config::load(&config_path).is_err());
    }

    #[test]
    fn purge_accepts_only_the_accounts_own_isolated_home() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("comradex.toml");
        assert!(validate_purge_home(&config, "app", &dir.path().join("accounts/app")).is_ok());
        for path in [
            dir.path().join(".codex"),
            dir.path().join("accounts"),
            dir.path().join("accounts/other"),
        ] {
            assert!(validate_purge_home(&config, "app", &path).is_err());
        }
        #[cfg(unix)]
        {
            std::fs::create_dir_all(dir.path().join("accounts")).unwrap();
            let external = tempfile::tempdir().unwrap();
            std::os::unix::fs::symlink(external.path(), dir.path().join("accounts/app")).unwrap();
            assert!(validate_purge_home(&config, "app", external.path()).is_err());
        }
    }

    #[test]
    fn add_appends_account_and_pool_member_preserving_comments() {
        let text = add_account(TEMPLATE, "work2", "default").unwrap();
        assert!(text.contains("members = [\"caller\", \"work2\"]"));
        assert!(text.contains("[accounts.work2]"));
        assert!(text.contains("path = \"accounts/work2\""));
        assert!(text.contains("# my listener"));
        let config: crate::config::Config = toml::from_str(&text).unwrap();
        assert_eq!(config.accounts.len(), 2);
        assert_eq!(config.pools["default"].members, vec!["caller", "work2"]);
    }

    #[test]
    fn add_rejects_duplicates_unknown_pools_and_unsafe_names() {
        assert!(
            add_account(TEMPLATE, "caller", "default")
                .unwrap_err()
                .to_string()
                .contains("already exists")
        );
        assert!(
            add_account(TEMPLATE, "work2", "missing")
                .unwrap_err()
                .to_string()
                .contains("unknown pool")
        );
        for name in ["", "a/b", "../up", "a b", &"x".repeat(257)] {
            assert!(add_account(TEMPLATE, name, "default").is_err(), "{name}");
        }
    }

    #[test]
    fn remove_round_trips_and_reports_the_home_path() {
        let added = add_account(TEMPLATE, "work2", "default").unwrap();
        let (removed, home) = remove_account(&added, "work2").unwrap();
        assert_eq!(home.as_deref(), Some("accounts/work2"));
        assert!(!removed.contains("work2"));
        assert!(removed.contains("members = [\"caller\"]"));
        assert!(
            remove_account(TEMPLATE, "ghost")
                .unwrap_err()
                .to_string()
                .contains("unknown account")
        );
    }

    #[test]
    fn preferred_account_is_set_cleared_and_removed_with_the_account() {
        let added = add_account(TEMPLATE, "work2", "default").unwrap();
        let preferred = set_preferred_account(&added, "default", Some("work2")).unwrap();
        assert!(preferred.contains("preferred = \"work2\""));

        let cleared = set_preferred_account(&preferred, "default", None).unwrap();
        assert!(!cleared.contains("preferred"));

        let (removed, _) = remove_account(&preferred, "work2").unwrap();
        assert!(!removed.contains("preferred"));
    }

    #[test]
    fn preserved_account_is_validated_cleared_and_removed() {
        let added = add_account(TEMPLATE, "work2", "default").unwrap();
        let saved = set_preserved_account(&added, "default", Some("work2")).unwrap();
        assert!(saved.contains("preserved = \"work2\""));
        assert!(
            !set_preserved_account(&saved, "default", None)
                .unwrap()
                .contains("preserved")
        );
        assert!(
            !remove_account(&saved, "work2")
                .unwrap()
                .0
                .contains("preserved")
        );
        assert!(set_preserved_account(&added, "default", Some("missing")).is_err());
        assert!(set_preserved_account(&added, "missing", Some("work2")).is_err());
        assert!(set_preferred_account(&saved, "default", Some("work2")).is_err());
        let preferred = set_preferred_account(&added, "default", Some("work2")).unwrap();
        assert!(set_preserved_account(&preferred, "default", Some("work2")).is_err());
        assert!(set_preferred_account(&saved, "default", Some("caller")).is_ok());
    }

    #[test]
    fn preferred_account_must_belong_to_the_pool() {
        let error = set_preferred_account(TEMPLATE, "default", Some("missing")).unwrap_err();
        assert!(error.to_string().contains("not a member"));
    }
}
