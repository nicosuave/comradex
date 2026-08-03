//! Detect and optionally terminate long-lived Codex app-server processes.
//!
//! `install`/`uninstall` rewrite `openai_base_url` on disk, but a running
//! `codex app-server` (spawned by the Codex desktop app or a CLI host) read the
//! config at startup and keeps routing to the old URL until it restarts. The
//! desktop app respawns its app-server after termination.
//!
//! Matching is intentionally narrow (same contract as OpenCodex): the process
//! must be a Codex binary whose subcommand is `app-server`, or a
//! `codex-code-mode-host` entrypoint. Never a broad `*codex*` pattern that
//! would hit unrelated tools. Only current-user processes are listed, only
//! SIGTERM is sent (never SIGKILL), and PIDs are re-resolved with a
//! pid+command-line identity check immediately before signaling so a recycled
//! PID is never killed.

use std::time::Duration;

use anyhow::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexProcess {
    pub pid: i32,
    pub command_line: String,
}

impl CodexProcess {
    /// Stable identity for PID-reuse checks: pid + whitespace-normalized command line.
    fn identity(&self) -> String {
        format!(
            "{}\0{}",
            self.pid,
            self.command_line
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        )
    }
}

#[derive(Debug, Default)]
pub struct RestartOutcome {
    pub stopped: Vec<i32>,
    pub surviving: Vec<i32>,
    pub failed: Vec<(i32, String)>,
}

pub trait ProcessControl {
    fn list(&self) -> Result<Vec<CodexProcess>>;
    fn send_sigterm(&self, pid: i32) -> std::io::Result<()>;
    fn is_alive(&self, pid: i32) -> bool;
    fn sleep(&self, duration: Duration);
}

pub struct SystemProcesses;

impl ProcessControl for SystemProcesses {
    fn list(&self) -> Result<Vec<CodexProcess>> {
        list_current_user_snapshots().map(|snapshots| {
            snapshots
                .into_iter()
                .filter(|(_, command_line)| is_codex_app_server_command_line(command_line))
                .map(|(pid, command_line)| CodexProcess { pid, command_line })
                .collect()
        })
    }

    #[cfg(unix)]
    fn send_sigterm(&self, pid: i32) -> std::io::Result<()> {
        if unsafe { libc::kill(pid, libc::SIGTERM) } == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    #[cfg(not(unix))]
    fn send_sigterm(&self, _pid: i32) -> std::io::Result<()> {
        Err(std::io::Error::other(
            "restarting Codex processes is only supported on Unix",
        ))
    }

    #[cfg(unix)]
    fn is_alive(&self, pid: i32) -> bool {
        if unsafe { libc::kill(pid, 0) } == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }

    #[cfg(not(unix))]
    fn is_alive(&self, _pid: i32) -> bool {
        false
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

#[cfg(unix)]
fn list_current_user_snapshots() -> Result<Vec<(i32, String)>> {
    use anyhow::Context;
    let uid = unsafe { libc::getuid() };
    let output = std::process::Command::new("ps")
        .args(["-u", &uid.to_string(), "-o", "pid=,command="])
        .output()
        .context("list processes with ps")?;
    let mut snapshots = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let line = line.trim();
        let Some((pid, command_line)) = line.split_once(char::is_whitespace) else {
            continue;
        };
        let Ok(pid) = pid.parse::<i32>() else {
            continue;
        };
        let command_line = command_line.trim();
        if pid <= 1 || command_line.is_empty() {
            continue;
        }
        snapshots.push((pid, command_line.to_owned()));
    }
    Ok(snapshots)
}

#[cfg(not(unix))]
fn list_current_user_snapshots() -> Result<Vec<(i32, String)>> {
    anyhow::bail!("listing Codex processes is only supported on Unix")
}

/// Send SIGTERM to matched processes and wait briefly; never escalates to SIGKILL.
pub fn restart(processes: &[CodexProcess], control: &dyn ProcessControl) -> RestartOutcome {
    let mut outcome = RestartOutcome::default();
    // Re-resolve immediately before signaling: a PID must still belong to the
    // same command line as the original match, otherwise it is skipped.
    let live: std::collections::BTreeMap<i32, CodexProcess> = control
        .list()
        .unwrap_or_default()
        .into_iter()
        .map(|process| (process.pid, process))
        .collect();
    let mut signaled = Vec::new();
    for process in processes {
        match live.get(&process.pid) {
            Some(current) if current.identity() == process.identity() => {
                match control.send_sigterm(process.pid) {
                    Ok(()) => signaled.push(process.pid),
                    Err(error) if control.is_alive(process.pid) => {
                        outcome.failed.push((process.pid, error.to_string()));
                        outcome.surviving.push(process.pid);
                    }
                    Err(_) => outcome.stopped.push(process.pid),
                }
            }
            _ => {
                // Original target exited (or the PID was recycled); never
                // signal a replacement process.
                if !control.is_alive(process.pid) {
                    outcome.stopped.push(process.pid);
                }
            }
        }
    }
    // Shared ~2s budget so N survivors wait ~2s total, not N x 2s.
    let mut polls_remaining = 40u32;
    let poll = Duration::from_millis(50);
    for pid in signaled {
        loop {
            if !control.is_alive(pid) {
                outcome.stopped.push(pid);
                break;
            }
            if polls_remaining == 0 {
                outcome.surviving.push(pid);
                break;
            }
            polls_remaining -= 1;
            control.sleep(poll);
        }
    }
    outcome
}

/// Codex global options that take a following value when written without `=`.
/// Kept explicit so unknown flags stay boolean and matching stays narrow.
const OPTIONS_WITH_VALUE: &[&str] = &[
    "--enable",
    "--disable",
    "--config",
    "-c",
    "--profile",
    "-p",
    "--model",
    "-m",
    "--sandbox",
    "-s",
    "--ask-for-approval",
    "-a",
    "--local-provider",
    "--add-dir",
    "--cd",
    "-C",
    "--color",
    "--image",
    "-i",
    "--output-schema",
    "--output-last-message",
    "-o",
];

/// True when the command line is a Codex app-server (or code-mode host) worth restarting.
pub fn is_codex_app_server_command_line(command_line: &str) -> bool {
    let tokens = tokenize(command_line.trim());
    if tokens.is_empty() {
        return false;
    }
    if is_code_mode_host_process(&tokens) {
        return true;
    }
    // Require Codex as argv0 so later-argument occurrences stay unmatched
    // (e.g. `node worker.js codex app-server`).
    if !is_codex_executable_token(&tokens[0]) {
        return false;
    }
    let mut index = 1;
    while index < tokens.len() {
        if tokens[index].starts_with('-') {
            index = advance_past_global_option(&tokens, index);
            continue;
        }
        // First non-option token after globals is the Codex subcommand.
        return tokens[index].eq_ignore_ascii_case("app-server");
    }
    false
}

/// Split a process command line into argv-like tokens (handles simple quotes).
fn tokenize(command_line: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    for ch in command_line.chars() {
        if let Some(open) = quote {
            if ch == open {
                quote = None;
            } else {
                current.push(ch);
            }
            continue;
        }
        if ch == '"' || ch == '\'' {
            quote = Some(ch);
            continue;
        }
        if ch.is_whitespace() {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            continue;
        }
        current.push(ch);
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn token_basename(token: &str) -> String {
    token
        .to_ascii_lowercase()
        .replace('\\', "/")
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_owned()
}

/// Basename of an official Codex release binary: `codex`, `codex.exe`,
/// `codex.cmd`, or a target-triple build such as `codex-aarch64-apple-darwin`
/// (arch-vendor-os with an optional env segment) — not a broad `codex-*` match.
fn is_codex_executable_token(token: &str) -> bool {
    let base = token_basename(token);
    if matches!(base.as_str(), "codex" | "codex.exe" | "codex.cmd") {
        return true;
    }
    let Some(triple) = base.strip_prefix("codex-") else {
        return false;
    };
    let triple = triple
        .strip_suffix(".exe")
        .or_else(|| triple.strip_suffix(".cmd"))
        .unwrap_or(triple);
    let segments: Vec<&str> = triple.split('-').collect();
    (3..=4).contains(&segments.len())
        && segments.iter().all(|segment| {
            !segment.is_empty()
                && segment
                    .chars()
                    .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
        })
}

fn is_code_mode_host_token(token: &str) -> bool {
    matches!(
        token_basename(token).as_str(),
        "codex-code-mode-host" | "codex-code-mode-host.exe"
    )
}

fn is_interpreter_token(token: &str) -> bool {
    matches!(
        token_basename(token).as_str(),
        "node" | "node.exe" | "bun" | "bun.exe" | "deno" | "deno.exe"
    )
}

/// True when code-mode-host is the executable or interpreter entrypoint, not a later arg.
fn is_code_mode_host_process(tokens: &[String]) -> bool {
    match tokens {
        [] => false,
        [first, rest @ ..] => {
            is_code_mode_host_token(first)
                || (is_interpreter_token(first)
                    && rest
                        .first()
                        .is_some_and(|token| is_code_mode_host_token(token)))
        }
    }
}

/// Advance past one argv token, consuming a value for known Codex global options.
fn advance_past_global_option(tokens: &[String], index: usize) -> usize {
    let token = &tokens[index];
    if token == "-" || token == "--" {
        return index + 1;
    }
    // `--opt=value` carries its value inline; preserve short-option case so
    // `-c` (config) and `-C` (cd) stay distinct.
    let name = token.split('=').next().unwrap_or(token);
    let name = if token.starts_with("--") {
        name.to_ascii_lowercase()
    } else {
        name.to_owned()
    };
    let has_inline_value = token.contains('=');
    let next = index + 1;
    if !has_inline_value
        && OPTIONS_WITH_VALUE.contains(&name.as_str())
        && next < tokens.len()
        && !tokens[next].starts_with('-')
    {
        return next + 1;
    }
    next
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn matches_only_real_app_server_command_lines() {
        for matching in [
            "codex app-server",
            "/usr/local/bin/codex app-server",
            "codex -c model=o3 app-server",
            "codex --config model=o3 app-server",
            "codex --profile work APP-SERVER",
            "codex-aarch64-apple-darwin app-server",
            "codex-x86_64-unknown-linux-musl app-server",
            "codex-code-mode-host --port 4000",
            "node /opt/codex-code-mode-host serve",
        ] {
            assert!(is_codex_app_server_command_line(matching), "{matching}");
        }
        for other in [
            "",
            "codex",
            "codex exec --json",
            "codex resume app-server",
            "node worker.js codex app-server",
            "hermes-codex-bridge-mcp app-server",
            "opencodex app-server",
            "codex-wrapper app-server",
            "grep codex app-server",
        ] {
            assert!(!is_codex_app_server_command_line(other), "{other}");
        }
    }

    #[test]
    fn global_option_values_are_not_mistaken_for_subcommands() {
        assert!(is_codex_app_server_command_line(
            "codex -m gpt-test --sandbox workspace-write app-server"
        ));
        // `app-server` here is consumed as the value of `--model`, not a subcommand.
        assert!(!is_codex_app_server_command_line(
            "codex --model app-server"
        ));
    }

    struct FakeControl {
        live: RefCell<Vec<CodexProcess>>,
        term_error: Option<i32>,
    }

    impl ProcessControl for FakeControl {
        fn list(&self) -> Result<Vec<CodexProcess>> {
            Ok(self.live.borrow().clone())
        }
        fn send_sigterm(&self, pid: i32) -> std::io::Result<()> {
            if self.term_error == Some(pid) {
                return Err(std::io::Error::other("operation not permitted"));
            }
            self.live.borrow_mut().retain(|process| process.pid != pid);
            Ok(())
        }
        fn is_alive(&self, pid: i32) -> bool {
            self.live.borrow().iter().any(|process| process.pid == pid)
        }
        fn sleep(&self, _duration: Duration) {}
    }

    fn process(pid: i32, command_line: &str) -> CodexProcess {
        CodexProcess {
            pid,
            command_line: command_line.to_owned(),
        }
    }

    #[test]
    fn restart_stops_matched_processes_and_reports_failures() {
        let control = FakeControl {
            live: RefCell::new(vec![
                process(100, "codex app-server"),
                process(200, "codex app-server"),
            ]),
            term_error: Some(200),
        };
        let targets = [
            process(100, "codex app-server"),
            process(200, "codex app-server"),
        ];
        let outcome = restart(&targets, &control);
        assert_eq!(outcome.stopped, vec![100]);
        assert_eq!(outcome.surviving, vec![200]);
        assert_eq!(outcome.failed.len(), 1);
    }

    #[test]
    fn restart_never_signals_a_recycled_pid() {
        let control = FakeControl {
            live: RefCell::new(vec![process(100, "codex --profile other app-server")]),
            term_error: None,
        };
        let outcome = restart(&[process(100, "codex app-server")], &control);
        assert!(outcome.stopped.is_empty());
        assert!(outcome.surviving.is_empty());
        assert!(outcome.failed.is_empty());
        assert!(control.is_alive(100));
    }
}
