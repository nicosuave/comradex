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
        drop(socket);
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
[pools.claude]
members=["caller"]
[accounts.caller]
kind="claude_inbound"
"#,
                directory.display()
            ),
        )
        .unwrap();
        let mut instance = Self {
            config,
            home,
            label: None,
        };
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
    a.run(&["service", "uninstall"]);
    assert_eq!(a.pid(), None);
    assert_eq!(b.pid(), b_pid);
    assert_eq!(pid("com.nicosuave.comradex"), production_pid);
}
