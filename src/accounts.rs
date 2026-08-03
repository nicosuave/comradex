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
        }
    }
    Ok((doc.to_string(), home))
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
}
