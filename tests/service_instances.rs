#![cfg(target_os = "macos")]

use std::{fs, net::TcpListener, path::PathBuf, process::Command};

struct Instance {
    config: PathBuf,
    home: PathBuf,
    label: Option<String>,
}

impl Instance {
    fn command(&self, arguments: &[&str]) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_comradex"))
            .env("HOME", &self.home)
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    self.home.join("bin").display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            )
            .arg("--config")
            .arg(&self.config)
            .args(arguments)
            .output()
            .unwrap()
    }

    fn run(&self, arguments: &[&str]) -> String {
        let output = self.command(arguments);
        assert!(
            output.status.success(),
            "{arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }

    fn install(root: &std::path::Path, name: &str) -> Self {
        let directory = root.join(name);
        let home = root.join("home");
        fs::create_dir_all(&directory).unwrap();
        fs::create_dir_all(&home).unwrap();
        let socket = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = socket.local_addr().unwrap();
        let codex_socket = TcpListener::bind("127.0.0.1:0").unwrap();
        let codex_address = codex_socket.local_addr().unwrap();
        let config = directory.join("comradex.toml");
        fs::write(
            &config,
            format!(
                r#"
[proxy]
installation_secret="service-instance-test-secret"
affinity_key="0123456789abcdef0123456789abcdef"
state_dir="{}/state"
[listeners.claude]
address="{address}"
pool="claude"
[listeners.default]
address="{codex_address}"
pool="default"
[pools.claude]
members=["caller"]
[pools.default]
members=["code"]
[accounts.caller]
kind="claude_inbound"
[accounts.code]
kind="codex_home"
path="{}/code"
"#,
                directory.display(),
                directory.display()
            ),
        )
        .unwrap();
        let mut instance = Self {
            config,
            home,
            label: None,
        };
        drop((socket, codex_socket));
        let output = instance.run(&["service", "install"]);
        let plist = output
            .trim()
            .strip_prefix("installed and started ")
            .unwrap();
        instance.label = Some(
            std::path::Path::new(plist)
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .into(),
        );
        assert_ne!(instance.label.as_deref(), Some("com.nicosuave.comradex"));
        assert!(
            instance
                .run(&["service", "status"])
                .contains("running and ready")
        );
        instance
    }

    fn pid(&self) -> Option<u32> {
        pid(self.label.as_ref().unwrap())
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        // Cleanup checks launchd's registered plist path rather than trusting the
        // service-selection code under test, including when installation fails.
        let Ok(entries) = fs::read_dir(self.home.join("Library/LaunchAgents")) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(plist) = fs::read_to_string(&path) else {
                continue;
            };
            if !plist.contains(self.config.to_str().unwrap()) {
                continue;
            }
            let Some(label) = path.file_stem().and_then(|name| name.to_str()) else {
                continue;
            };
            let output = Command::new("launchctl")
                .args(["print", &target(label)])
                .output()
                .unwrap();
            if String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line.trim() == format!("path = {}", path.display()))
            {
                let _ = Command::new("launchctl")
                    .args(["bootout", &target(label)])
                    .output();
            }
            let _ = fs::remove_file(path);
        }
    }
}

fn target(label: &str) -> String {
    format!("gui/{}/{label}", unsafe { libc::geteuid() })
}

fn pid(label: &str) -> Option<u32> {
    let output = Command::new("launchctl")
        .args(["print", &target(label)])
        .output()
        .unwrap();
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| line.trim().strip_prefix("pid = ")?.parse().ok())
}

#[test]
#[ignore = "creates isolated LaunchAgents in the current GUI session"]
fn service_and_account_commands_only_manage_the_selected_configuration() {
    // Short paths also fit macOS's Unix-domain socket path limit.
    let root = tempfile::tempdir_in("/private/tmp").unwrap();
    let production_pid = pid("com.nicosuave.comradex");
    let a = Instance::install(root.path(), "a");
    let b = Instance::install(root.path(), "b");
    assert_ne!(a.label, b.label);
    let b_pid = b.pid();
    let a_pid = a.pid();
    assert!(a_pid.is_some() && b_pid.is_some());

    a.run(&["service", "start"]);
    assert_eq!(a.pid(), a_pid);
    a.run(&["service", "restart"]);
    assert_ne!(a.pid(), a_pid);
    assert_eq!(b.pid(), b_pid);

    let alias_path = root.path().join("alias.toml");
    std::os::unix::fs::symlink(&a.config, &alias_path).unwrap();
    let alias = Instance {
        config: alias_path.clone(),
        home: a.home.clone(),
        label: a.label.clone(),
    };
    let a_pid = a.pid();
    alias.run(&["account", "add", "alias-probe", "--claude", "--no-login"]);
    assert!(
        alias_path.is_symlink(),
        "account maintenance replaced the config symlink"
    );
    assert!(
        fs::read_to_string(&a.config)
            .unwrap()
            .contains("alias-probe")
    );
    assert_ne!(a.pid(), a_pid);
    assert_eq!(b.pid(), b_pid);

    let valid = fs::read_to_string(&a.config).unwrap();
    let a_pid = a.pid();
    fs::write(&a.config, "broken = [").unwrap();
    assert!(!a.command(&["service", "restart"]).status.success());
    assert_eq!(a.pid(), a_pid);
    fs::write(&a.config, valid).unwrap();

    assert!(
        Command::new("launchctl")
            .args(["kill", "SIGKILL", &target(a.label.as_ref().unwrap())])
            .status()
            .unwrap()
            .success()
    );
    a.run(&["service", "start"]);
    assert_ne!(a.pid(), a_pid);
    let a_pid = a.pid();
    a.run(&["account", "add", "unlogged-probe", "--claude", "--no-login"]);
    assert_ne!(a.pid(), a_pid);
    assert_eq!(b.pid(), b_pid);

    let binaries = a.home.join("bin");
    fs::create_dir(&binaries).unwrap();
    let fake_codex = binaries.join("codex");
    fs::write(&fake_codex, format!(
        "#!/bin/sh\nif /bin/launchctl print '{}' >/dev/null 2>&1; then exit 91; fi\nkill -0 {} || exit 92\nprintf isolated > \"$CODEX_HOME/login.marker\"\nexit 23\n",
        target(a.label.as_ref().unwrap()), b_pid.unwrap(),
    )).unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();
    let a_pid = a.pid();
    let login = alias.command(&["account", "login", "code"]);
    assert!(!login.status.success());
    assert!(String::from_utf8_lossy(&login.stderr).contains("23"));
    assert_eq!(
        fs::read_to_string(a.config.parent().unwrap().join("code/login.marker")).unwrap(),
        "isolated"
    );
    assert!(a.pid().is_some());
    assert_ne!(a.pid(), a_pid);
    assert_eq!(b.pid(), b_pid);

    let legacy = a
        .home
        .join("Library/LaunchAgents/com.nicosuave.comradex.plist");
    fs::write(&legacy, format!(
        "<plist><dict><key>ProgramArguments</key><array><string>{}</string><string>--config</string><string>{}</string><string>serve</string></array></dict></plist>",
        env!("CARGO_BIN_EXE_comradex"), a.config.display(),
    )).unwrap();
    let a_pid = a.pid();
    let edit = alias.command(&[
        "account",
        "add",
        "diagnostic-probe",
        "--claude",
        "--no-login",
    ]);
    let error = String::from_utf8_lossy(&edit.stderr);
    assert!(!edit.status.success());
    assert!(error.contains("configuration saved"));
    assert!(error.contains("two service labels"));
    assert!(
        fs::read_to_string(&a.config)
            .unwrap()
            .contains("diagnostic-probe")
    );
    assert_eq!(a.pid(), a_pid);
    assert_eq!(b.pid(), b_pid);
    fs::remove_file(legacy).unwrap();
    a.run(&["service", "uninstall"]);
    assert_eq!(a.pid(), None);
    assert_eq!(b.pid(), b_pid);
    assert_eq!(pid("com.nicosuave.comradex"), production_pid);
}

#[test]
#[ignore = "creates isolated LaunchAgents in the current GUI session"]
fn default_service_cannot_take_over_a_legacy_custom_service() {
    let root = tempfile::tempdir_in("/private/tmp").unwrap();
    let production_pid = pid("com.nicosuave.comradex");
    let custom = Instance::install(root.path(), "custom");
    let default_dir = custom.home.join(".config/comradex");
    fs::create_dir_all(&default_dir).unwrap();
    let config = default_dir.join("comradex.toml");
    let mut contents: toml::Value =
        toml::from_str(&fs::read_to_string(&custom.config).unwrap()).unwrap();
    let mut sockets = Vec::new();
    for (_, listener) in contents["listeners"].as_table_mut().unwrap().iter_mut() {
        let socket = TcpListener::bind("127.0.0.1:0").unwrap();
        listener["address"] = socket.local_addr().unwrap().to_string().into();
        sockets.push(socket);
    }
    contents["proxy"]["state_dir"] = default_dir.join("state").to_str().unwrap().into();
    contents["accounts"]["code"]["path"] = default_dir.join("code").to_str().unwrap().into();
    fs::write(&config, toml::to_string(&contents).unwrap()).unwrap();
    let mut default = Instance {
        config,
        home: custom.home.clone(),
        label: None,
    };
    let legacy = custom
        .home
        .join("Library/LaunchAgents/com.nicosuave.comradex.plist");
    // Only ownership metadata is simulated; never register or change the real singleton.
    fs::write(&legacy, format!(
        "<plist><dict><key>ProgramArguments</key><array><string>{}</string><string>--config</string><string>{}</string><string>serve</string></array></dict></plist>",
        env!("CARGO_BIN_EXE_comradex"), custom.config.display(),
    )).unwrap();
    assert!(
        default
            .run(&["service", "status"])
            .contains("not installed")
    );
    drop(sockets);
    let output = default.run(&["service", "install"]);
    let plist = output
        .trim()
        .strip_prefix("installed and started ")
        .unwrap();
    default.label = Some(
        std::path::Path::new(plist)
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .into(),
    );
    assert_ne!(default.label.as_deref(), Some("com.nicosuave.comradex"));
    let default_pid = default.pid();
    assert!(default_pid.is_some());
    fs::remove_file(legacy).unwrap();
    assert!(
        default
            .run(&["service", "status"])
            .contains("running and ready")
    );
    default.run(&["service", "start"]);
    assert_eq!(default.pid(), default_pid);
    assert_eq!(pid("com.nicosuave.comradex"), production_pid);
}
