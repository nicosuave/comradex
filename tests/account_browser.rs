#![cfg(unix)]

use comradex::{browser, config::Config};
use std::{
    fs,
    io::{Read, Write},
    os::{fd::FromRawFd, unix::fs::PermissionsExt},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::{Duration, Instant},
};

struct Fixture {
    dir: tempfile::TempDir,
    config: PathBuf,
    bin: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("comradex.toml");
        let bin = dir.path().join("bin with spaces");
        fs::create_dir(&bin).unwrap();
        fs::write(
            &config,
            r#"[proxy]
installation_secret = "0123456789abcdef"
affinity_key = "0123456789abcdef0123456789abcdef"
[listeners.default]
address = "127.0.0.1:0"
pool = "default"
[pools.default]
members = ["grace"]
[accounts.grace]
kind = "codex_home"
path = "accounts/grace"
"#,
        )
        .unwrap();
        script(
            &bin.join("chrome"),
            r#"#!/bin/sh
if [ "$1" = '--version' ]; then
  echo 'Google Chrome 140.0.1.2'
  exit 0
fi
printf '%s\n' "$@" >> "$TEST_BROWSER_LOG"
"#,
        );
        script(
            &bin.join("codex"),
            r#"#!/bin/sh
printf '%s\n' "$@" > "$TEST_LOGIN_LOG"
mkdir -p "$CODEX_HOME"
printf '%s' '{"tokens":{"access_token":"test-access-token"}}' > "$CODEX_HOME/auth.json"
"#,
        );
        // Deliberately fail after opening the real BROWSER launcher: no fake
        // keychain credential imports or interaction with the user's grants.
        script(
            &bin.join("claude"),
            r#"#!/bin/sh
printf '%s' "$BROWSER" > "$TEST_LOGIN_LOG"
"$BROWSER" 'https://claude.ai/oauth/authorize?test=one&other=two' || exit 8
exit 9
"#,
        );
        Self { dir, config, bin }
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_comradex"));
        cmd.arg("--config")
            .arg(&self.config)
            .args(args)
            // Service lifecycle uses HOME, not --config. Never let process
            // tests discover the user's installed LaunchAgent.
            .env("HOME", self.dir.path())
            .env("PATH", format!("{}:/usr/bin:/bin", self.bin.display()))
            .env("DISPLAY", ":test")
            .env("COMRADEX_BROWSER", self.bin.join("chrome"))
            .env("CLAUDE_EXECUTABLE", self.bin.join("claude"))
            .env("TEST_BROWSER_LOG", self.dir.path().join("browser.log"))
            .env("TEST_LOGIN_LOG", self.dir.path().join("login.log"));
        cmd
    }

    fn run(&self, args: &[&str]) -> Output {
        self.command(args).output().unwrap()
    }
    fn browser_log(&self) -> String {
        fs::read_to_string(self.dir.path().join("browser.log")).unwrap_or_default()
    }
}

fn script(path: &Path, content: &str) {
    fs::write(path, content).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

fn success(output: Output) {
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn codex_login_opts_in_then_reuses_profile_and_can_opt_out() {
    let f = Fixture::new();
    success(f.run(&["account", "login", "grace"]));
    assert!(f.browser_log().is_empty());
    success(f.run(&["account", "login", "grace", "--browser"]));
    let profile = browser::profile_path(&f.config, "grace").unwrap();
    let first = f.browser_log();
    assert!(first.contains(&format!("--user-data-dir={}", profile.display())));
    assert!(first.contains(browser::CODEX_DEVICE_URL));
    assert_eq!(
        fs::read_to_string(f.dir.path().join("login.log")).unwrap(),
        "-c\ncli_auth_credentials_store=\"file\"\nlogin\n--device-auth\n"
    );
    fs::write(profile.join("retained-cookie-fixture"), "retained").unwrap();
    success(f.run(&["account", "login", "grace"]));
    assert_eq!(f.browser_log(), first.repeat(2));
    assert!(profile.join("retained-cookie-fixture").exists());
    success(f.run(&["account", "login", "grace", "--no-browser"]));
    assert_eq!(f.browser_log(), first.repeat(2));
}

#[test]
fn add_and_browser_use_separate_profiles_and_purge_removes_only_selected_profile() {
    let f = Fixture::new();
    success(f.run(&["account", "browser", "grace"]));
    success(f.run(&["account", "add", "ada", "--browser"]));
    let grace = browser::profile_path(&f.config, "grace").unwrap();
    let ada = browser::profile_path(&f.config, "ada").unwrap();
    assert!(
        f.browser_log()
            .contains(&format!("--user-data-dir={}", ada.display()))
    );
    assert!(
        f.browser_log()
            .contains(&format!("--user-data-dir={}", grace.display()))
    );
    assert!(
        Config::load(&f.config)
            .unwrap()
            .accounts
            .contains_key("ada")
    );
    fs::write(ada.join("SingletonLock"), "active-browser").unwrap();
    let before = fs::read(&f.config).unwrap();
    assert!(
        !f.run(&["account", "remove", "ada", "--purge"])
            .status
            .success()
    );
    assert_eq!(fs::read(&f.config).unwrap(), before);
    fs::remove_file(ada.join("SingletonLock")).unwrap();
    success(f.run(&["account", "remove", "ada", "--purge"]));
    assert!(!ada.exists());
    assert!(grace.exists());
    success(f.run(&["account", "add", "anna", "--no-login"]));
    success(f.run(&["account", "remove", "grace"]));
    assert!(grace.exists());
}

#[test]
fn claude_authorization_uses_profile_launcher_and_reports_provider_failure() {
    let f = Fixture::new();
    success(f.run(&["account", "add", "ada", "--claude", "--no-login"]));
    let output = f.run(&["account", "login", "ada", "--browser"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Claude login did not complete"));
    assert!(
        f.browser_log()
            .contains("https://claude.ai/oauth/authorize?test=one&other=two")
    );
    assert!(
        f.browser_log().contains(
            &browser::profile_path(&f.config, "ada")
                .unwrap()
                .display()
                .to_string()
        )
    );
    let launcher = fs::read_to_string(f.dir.path().join("login.log")).unwrap();
    assert!(
        !Path::new(&launcher).exists(),
        "temporary launcher should be removed after login"
    );
}

#[test]
fn invalid_requests_do_not_launch_browser_or_change_configuration() {
    let f = Fixture::new();
    let original = fs::read(&f.config).unwrap();
    for args in [
        vec!["account", "new", "ada", "--codex"], // Needs a terminal.
        vec!["account", "add", "grace", "--browser"],
        vec!["account", "add", "../escape", "--browser"],
        vec!["account", "add", "ada", "--browser", "--no-login"],
        vec!["account", "login", "missing", "--browser"],
        vec!["account", "login", "grace", "--browser", "--no-browser"],
        vec!["account", "browser", "missing"],
    ] {
        assert!(!f.run(&args).status.success(), "{args:?}");
        assert_eq!(fs::read(&f.config).unwrap(), original);
        assert!(f.browser_log().is_empty());
    }
    let output = f
        .command(&["account", "add", "ada", "--browser"])
        .env("COMRADEX_BROWSER", "/missing/chrome")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(fs::read(&f.config).unwrap(), original);
}

#[test]
fn new_account_waits_for_signup_and_preserves_config_edits_made_during_signup() {
    let f = Fixture::new();
    let (mut master_fd, mut slave_fd) = (0, 0);
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        },
        0
    );
    let mut master = unsafe { fs::File::from_raw_fd(master_fd) };
    let slave = unsafe { fs::File::from_raw_fd(slave_fd) };
    // Nonblocking reads keep a failing wizard from hanging the test suite.
    unsafe {
        libc::fcntl(master_fd, libc::F_SETFL, libc::O_NONBLOCK);
    }
    let mut child = f
        .command(&["account", "new"])
        .stdin(Stdio::from(slave.try_clone().unwrap()))
        .stdout(Stdio::from(slave.try_clone().unwrap()))
        .stderr(Stdio::from(slave))
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut transcript = String::new();
    let mut stage = 0;
    loop {
        let mut bytes = [0; 4096];
        if let Ok(n) = master.read(&mut bytes) {
            transcript.push_str(&String::from_utf8_lossy(&bytes[..n]));
        }
        if stage == 0 && transcript.contains("Account name:") {
            master.write_all(b"ada\n").unwrap();
            stage = 1;
        }
        if stage == 1 && transcript.contains("Provider [codex/claude]:") {
            master.write_all(b"codex\n").unwrap();
            stage = 2;
        }
        if stage == 2 && transcript.contains("When the account is ready") {
            assert!(
                !Config::load(&f.config)
                    .unwrap()
                    .accounts
                    .contains_key("ada")
            );
            assert!(f.browser_log().contains(browser::provider_url(false)));
            let text = format!(
                "{}\n# edit during signup\n",
                fs::read_to_string(&f.config).unwrap()
            );
            fs::write(&f.config, text).unwrap();
            master.write_all(b"\n").unwrap();
            stage = 3;
        }
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "{transcript}");
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("wizard timed out: {transcript}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(stage, 3, "{transcript}");
    assert!(
        Config::load(&f.config)
            .unwrap()
            .accounts
            .contains_key("ada")
    );
    assert!(
        fs::read_to_string(&f.config)
            .unwrap()
            .contains("# edit during signup")
    );
    let log = f.browser_log();
    assert!(log.contains(browser::CODEX_DEVICE_URL));
    assert_eq!(log.matches("--user-data-dir=").count(), 2);
}
