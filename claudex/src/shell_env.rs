//! A LaunchAgent starts with a fixed minimal PATH. Claude sessions, desktop
//! commands and plugin servers should see the same PATH as the user's terminal.
use std::{
    ffi::OsString,
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};

const MARKER: &str = "__CLAUDE_CODEX_LOGIN_PATH__";
const DEADLINE: Duration = Duration::from_secs(10);

/// The user's login shell, from `SHELL` or the account database.
pub fn login_shell() -> Option<PathBuf> {
    if let Some(shell) = std::env::var_os("SHELL").filter(|s| !s.is_empty()) {
        return Some(shell.into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        // SAFETY: getpwuid returns a pointer into static storage that is read
        // immediately, before any other account database call on this thread.
        let entry = unsafe { libc::getpwuid(libc::getuid()) };
        if !entry.is_null() {
            let shell = unsafe { std::ffi::CStr::from_ptr((*entry).pw_shell) };
            if !shell.to_bytes().is_empty() {
                return Some(PathBuf::from(std::ffi::OsStr::from_bytes(shell.to_bytes())));
            }
        }
    }
    None
}

/// PATH as an interactive login shell configures it (zshenv, zprofile, zshrc).
/// Only PATH is imported; other profile exports, including secrets, are not.
pub fn login_path(shell: &Path) -> Result<OsString> {
    let mut child = Command::new(shell)
        .args([
            "-l",
            "-i",
            "-c",
            &format!("printf '{MARKER}%s{MARKER}' \"$PATH\""),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("Cannot start login shell {}", shell.display()))?;
    let mut stdout = child.stdout.take().unwrap();
    // Profiles may print banners; read concurrently so a full pipe cannot stall them.
    let reader = std::thread::spawn(move || {
        let mut output = Vec::new();
        let _ = stdout.read_to_end(&mut output);
        output
    });
    let started = Instant::now();
    loop {
        if child.try_wait()?.is_some() {
            break;
        }
        if started.elapsed() > DEADLINE {
            let _ = child.kill();
            let _ = child.wait();
            bail!("Login shell did not report PATH within {DEADLINE:?}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let output = reader.join().unwrap_or_default();
    parse(&String::from_utf8_lossy(&output))
}

fn parse(output: &str) -> Result<OsString> {
    let path = output
        .split_once(MARKER)
        .and_then(|(_, rest)| rest.split_once(MARKER))
        .map(|(path, _)| path)
        .context("Login shell output did not include PATH")?;
    if !std::env::split_paths(path).any(|dir| dir.is_absolute()) {
        bail!("Login shell reported an unusable PATH");
    }
    Ok(path.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_noise_around_the_markers_is_ignored() {
        let output = format!("welcome\n{MARKER}/a/bin:/usr/bin{MARKER}bye");
        assert_eq!(parse(&output).unwrap(), "/a/bin:/usr/bin");
        assert!(parse("no markers here").is_err());
        assert!(parse(&format!("{MARKER}{MARKER}")).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn login_shell_profile_path_is_captured() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let shell = dir.path().join("shell");
        // A stand-in shell: prints a banner, applies a "profile", runs -c.
        std::fs::write(
            &shell,
            "#!/bin/sh\necho banner\nPATH=\"/profile/bin:$PATH\"\nshift 3\neval \"$1\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&shell, std::fs::Permissions::from_mode(0o755)).unwrap();
        let path = login_path(&shell).unwrap();
        assert!(path.to_string_lossy().starts_with("/profile/bin:"));
    }
}
