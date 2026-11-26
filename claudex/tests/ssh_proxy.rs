#![cfg(unix)]
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};

use claude_codex_server::ssh_proxy;
use serde_json::json;

fn helper(port: u16) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"));
    command
        .args(["sandbox-proxy-connect", "github.com", "22"])
        .env("CLOUDSDK_PROXY_ADDRESS", "localhost")
        .env("CLOUDSDK_PROXY_PORT", port.to_string())
        .env("CLOUDSDK_PROXY_USERNAME", "fixture")
        .env("CLOUDSDK_PROXY_PASSWORD", "secret")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

#[test]
fn authenticated_connect_relays_binary_without_waiting_for_newlines_or_stdin_eof() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut child = helper(listener.local_addr().unwrap().port())
        .spawn()
        .unwrap();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream);
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            headers.push_str(&line);
            if line == "\r\n" {
                break;
            }
        }
        assert!(headers.starts_with("CONNECT github.com:22 HTTP/1.1\r\n"));
        assert!(headers.contains("Proxy-Authorization: Basic Zml4dHVyZTpzZWNyZXQ=\r\n"));
        // Coalesce initial binary bytes with headers to catch buffered data loss.
        reader
            .get_mut()
            .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n\0\xff\x01")
            .unwrap();
        let mut reply = [0; 3];
        reader.read_exact(&mut reply).unwrap();
        assert_eq!(reply, [2, 0, 255]);
    });
    let mut output = [0; 3];
    child
        .stdout
        .as_mut()
        .unwrap()
        .read_exact(&mut output)
        .unwrap();
    assert_eq!(output, [0, 255, 1]);
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(&[2, 0, 255])
        .unwrap();
    server.join().unwrap();
    // Keep stdin open: remote EOF must still end the helper.
    assert!(child.wait().unwrap().success());
}

#[test]
fn proxy_refusal_and_missing_auth_fail_without_direct_fallback_or_secret_output() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream);
        // Drain the complete request before closing, avoiding a TCP reset when
        // the headers arrive in multiple packets.
        loop {
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            if line == "\r\n" {
                break;
            }
        }
        reader
            .get_mut()
            .write_all(b"HTTP/1.1 403 Forbidden\r\n\r\nsecret response body")
            .unwrap();
    });
    let result = helper(port).output().unwrap();
    server.join().unwrap();
    assert!(!result.status.success());
    assert!(result.stdout.is_empty());
    let error = String::from_utf8(result.stderr).unwrap();
    assert!(error.contains("proxy refused"), "{error}");
    assert!(!error.contains("secret"));
    let result = helper(port)
        .env_remove("CLOUDSDK_PROXY_PASSWORD")
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(
        String::from_utf8(result.stderr)
            .unwrap()
            .contains("authentication missing")
    );
}

#[test]
fn native_env_hook_preserves_custom_ssh_and_unsandboxed_commands() {
    let dir = tempfile::tempdir().unwrap();
    let env_file = dir.path().join("shell.env");
    let mut settings = json!({"hooks":{"SessionStart":[{"hooks":[]}]}});
    ssh_proxy::add_hook(&mut settings, Path::new("/tmp/a user's facade")).unwrap();
    assert_eq!(
        settings["hooks"]["SessionStart"].as_array().unwrap().len(),
        2
    );
    let hook = settings["hooks"]["SessionStart"][1]["hooks"][0]["command"]
        .as_str()
        .unwrap();
    assert!(
        Command::new("sh")
            .args(["-c", hook])
            .env("CLAUDE_ENV_FILE", &env_file)
            .status()
            .unwrap()
            .success()
    );
    let broken = "ssh -o ControlMaster=no -o ControlPath=none -o ProxyCommand='nc -X 5 -x localhost:4321 %h %p'";
    for (sandbox, original, replaced) in [
        ("1", broken, true),
        ("", broken, false),
        ("1", "custom ssh", false),
    ] {
        let result = Command::new("sh")
            .args(["-c", ". \"$1\"; printf %s \"$GIT_SSH_COMMAND\"", "sh"])
            .arg(&env_file)
            .env("SANDBOX_RUNTIME", sandbox)
            .env("GIT_SSH_COMMAND", original)
            .output()
            .unwrap();
        assert!(result.status.success());
        let command = String::from_utf8(result.stdout).unwrap();
        if replaced {
            assert!(command.contains("sandbox-proxy-connect"));
        } else {
            assert_eq!(command, original);
        }
    }
}
