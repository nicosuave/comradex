//! Desktop worktree setup is owned by its Git worker. This module translates
//! the environment overlay sent when the desktop starts/resumes that workspace.
use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::protocol::{RpcError, RpcResult};

/// Extract the native-compatible additive environment overlay. `None` means no
/// policy was supplied; `Some(empty)` explicitly replaces a previous overlay.
/// Restrictive Codex policies apply to tool subprocesses, and must not be
/// approximated by filtering the Claude CLI's own authentication environment.
pub fn shell_environment_overrides(
    config: &Map<String, Value>,
) -> RpcResult<Option<BTreeMap<String, String>>> {
    let mut policy = match config
        .get("shell_environment_policy")
        .filter(|v| !v.is_null())
    {
        Some(Value::Object(value)) => value.clone(),
        Some(_) => {
            return Err(RpcError::invalid(
                "shell_environment_policy must be an object",
            ));
        }
        None => Map::new(),
    };
    let mut supplied = config
        .get("shell_environment_policy")
        .is_some_and(|v| !v.is_null());
    for (key, value) in config {
        if let Some(field) = key.strip_prefix("shell_environment_policy.") {
            if value.is_null() {
                continue;
            }
            supplied = true;
            if !field.starts_with("set.") {
                policy.insert(field.into(), value.clone());
            }
        }
    }
    if !supplied {
        return Ok(None);
    }
    let mut overrides: BTreeMap<String, String> =
        match policy.remove("set").filter(|v| !v.is_null()) {
            Some(value) => serde_json::from_value(value).map_err(|_| {
                RpcError::invalid("shell_environment_policy.set must map names to strings")
            })?,
            None => BTreeMap::new(),
        };
    for (key, value) in config {
        if let Some(name) = key.strip_prefix("shell_environment_policy.set.") {
            if value.is_null() {
                continue;
            }
            let value = value
                .as_str()
                .ok_or_else(|| RpcError::invalid("shell environment values must be strings"))?;
            overrides.insert(name.into(), value.into());
        }
    }
    for (field, value) in policy {
        if value.is_null() {
            continue;
        }
        let compatible = match field.as_str() {
            "inherit" => value == "all",
            "ignore_default_excludes" => value == true,
            "experimental_use_profile" => value == false,
            "exclude" | "include_only" => value.as_array().is_some_and(Vec::is_empty),
            "filters" => value.as_object().is_some_and(Map::is_empty),
            _ => false,
        };
        if !compatible {
            return Err(RpcError::invalid(format!(
                "Claude supports additive worktree environment overrides only; shell_environment_policy.{field} cannot be reproduced by its native environment settings"
            )));
        }
    }
    for (name, value) in &overrides {
        if name.is_empty() || name.contains(['=', '\0']) || value.contains('\0') {
            return Err(RpcError::invalid(
                "shell environment names must be nonempty without '=' or NUL, and values cannot contain NUL",
            ));
        }
    }
    Ok(Some(overrides))
}

pub fn is_shell_environment_key(key: &str) -> bool {
    key == "shell_environment_policy" || key.starts_with("shell_environment_policy.")
}
