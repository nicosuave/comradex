use std::{
    fs,
    path::Path,
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use comradex::{config::Config, control};

const SECRET: &str = "0123456789abcdef";

fn cli(config: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_comradex"))
        .arg("--config")
        .arg(config)
        .args(args)
        .output()
        .unwrap()
}

fn preserve(config: &Path, args: &[&str]) {
    let output = cli(config, &[&["account", "preserve"], args].concat());
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start(config: &Path, state: &Path) -> Daemon {
    let mut daemon = Daemon(
        Command::new(env!("CARGO_BIN_EXE_comradex"))
            .arg("--config")
            .arg(config)
            .arg("serve")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(daemon.0.try_wait().unwrap().is_none(), "test daemon exited");
        if control::routing_status(state, SECRET).is_ok() {
            return daemon;
        }
        assert!(
            Instant::now() < deadline,
            "test daemon did not become ready"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn preserve_cli_persists_updates_live_and_keeps_pools_independent() {
    // A short path also fits macOS's Unix socket path limit.
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    let path = dir.path().join("comradex.toml");
    let state = dir.path().join("state");
    fs::write(
        &path,
        format!(
            r#"[proxy]
installation_secret = "{SECRET}"
affinity_key = "0123456789abcdef0123456789abcdef"
state_dir = "state"

[listeners.default]
address = "127.0.0.1:0"
pool = "default"

[pools.default]
members = ["grace", "ada"]
preferred = "ada"

[pools.research]
members = ["grace", "ada"]

[accounts.grace]
kind = "inbound"
[accounts.ada]
kind = "inbound"
"#
        ),
    )
    .unwrap();

    preserve(&path, &["grace"]);
    preserve(&path, &["ada", "--pool", "research"]);
    let saved = Config::load(&path).unwrap();
    assert_eq!(saved.pools["default"].preserved.as_deref(), Some("grace"));
    assert_eq!(saved.pools["research"].preserved.as_deref(), Some("ada"));
    preserve(&path, &["--clear"]);
    assert!(
        Config::load(&path).unwrap().pools["default"]
            .preserved
            .is_none()
    );
    assert!(cli(&path, &["check"]).status.success());

    let mut daemon = start(&path, &state);
    preserve(&path, &["grace"]);
    let routing = control::routing_status(&state, SECRET).unwrap();
    assert_eq!(routing.preserved_accounts["default"], "grace");
    assert_eq!(routing.preserved_accounts["research"], "ada");
    for args in [
        vec!["account", "preserve", "ada"],
        vec!["account", "preserve", "missing"],
        vec!["account", "preserve", "grace", "--pool", "missing"],
        vec!["account", "prefer", "grace"],
    ] {
        let before = fs::read(&path).unwrap();
        assert!(!cli(&path, &args).status.success());
        assert_eq!(fs::read(&path).unwrap(), before);
        assert_eq!(
            control::routing_status(&state, SECRET)
                .unwrap()
                .preserved_accounts,
            routing.preserved_accounts
        );
    }
    preserve(&path, &["--clear", "--pool", "research"]);
    let routing = control::routing_status(&state, SECRET).unwrap();
    assert_eq!(routing.preserved_accounts.len(), 1);
    assert_eq!(routing.preserved_accounts["default"], "grace");
    assert!(daemon.0.try_wait().unwrap().is_none());
    drop(daemon);

    let _restarted = start(&path, &state);
    assert_eq!(
        control::routing_status(&state, SECRET)
            .unwrap()
            .preserved_accounts,
        routing.preserved_accounts
    );
    preserve(&path, &["--clear"]);
    assert!(
        control::routing_status(&state, SECRET)
            .unwrap()
            .preserved_accounts
            .is_empty()
    );
    assert!(
        Config::load(&path).unwrap().pools["default"]
            .preserved
            .is_none()
    );
}
