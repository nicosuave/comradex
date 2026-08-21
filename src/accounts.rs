//! Config-file surgery for `comradex account` commands.
//!
//! These functions transform the comradex.toml text with toml_edit so user
//! formatting and comments survive. Callers are responsible for validating the
//! result through `Config::load` before persisting it.

use anyhow::{Context, Result, bail};
use toml_edit::{DocumentMut, Item, value};

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
            if pool.get("preferred").and_then(Item::as_str) == Some(name) {
                pool.as_table_like_mut()
                    .expect("pool was already accessed as a table")
                    .remove("preferred");
            }
        }
    }
    Ok((doc.to_string(), home))
}

/// Set or clear the preferred account for a pool while preserving surrounding formatting.
pub fn set_preferred_account(text: &str, pool_name: &str, account: Option<&str>) -> Result<String> {
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
            pool.insert("preferred", value(account));
        }
        None => {
            pool.remove("preferred");
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
    fn preferred_account_must_belong_to_the_pool() {
        let error = set_preferred_account(TEMPLATE, "default", Some("missing")).unwrap_err();
        assert!(error.to_string().contains("not a member"));
    }
}
