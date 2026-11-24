#![cfg(unix)]

use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixListener,
    process::Command,
    thread,
};

use comradex::{config::Config, control};

const SECRET: &str = "0123456789abcdef";

#[test]
fn account_list_reports_control_error_and_keeps_file_fallback_output() {
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    let config_path = directory.path().join("comradex.toml");
    let account_home = directory.path().join("accounts/work");
    fs::create_dir_all(&account_home).unwrap();
    fs::write(
        account_home.join("auth.json"),
        r#"{"tokens":{"access_token":"dummy"}}"#,
    )
    .unwrap();
    fs::write(
        &config_path,
        format!(
            r#"[proxy]
installation_secret = "{SECRET}"
affinity_key = "0123456789abcdef0123456789abcdef"
state_dir = "state"

[listeners.default]
address = "127.0.0.1:0"
pool = "default"

[pools.default]
members = ["work"]

[accounts.work]
kind = "codex_home"
path = "accounts/work"
"#
        ),
    )
    .unwrap();

    let config = Config::load(&config_path).unwrap();
    let state_dir = config.proxy.state_dir.as_ref().unwrap();
    fs::create_dir_all(state_dir).unwrap();
    let listener = UnixListener::bind(control::socket_path(state_dir)).unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request_line = String::new();
        BufReader::new(&stream)
            .read_line(&mut request_line)
            .unwrap();
        stream
            .write_all(b"{\"ok\":false,\"error\":\"unauthorized\"}\n")
            .unwrap();
    });

    let output = Command::new(env!("CARGO_BIN_EXE_comradex"))
        .arg("--config")
        .arg(&config_path)
        .args(["account", "list"])
        .output()
        .unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "work  signed in  pool default\n(could not read daemon status: unauthorized; sign-in state reflects credential files only)\n"
    );
    assert!(output.stderr.is_empty());
    server.join().unwrap();
}
