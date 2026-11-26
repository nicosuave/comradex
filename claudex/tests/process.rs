//! Real-process coverage of the installed desktop's newer host-process protocol.
use base64::{Engine, engine::general_purpose::STANDARD};
use claude_codex_server::{commands::CommandManager, protocol::RpcResult};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use tokio::sync::mpsc;

async fn call(
    manager: &CommandManager,
    tx: &mpsc::Sender<Value>,
    owner: u64,
    id: u64,
    method: &str,
    params: Value,
) -> RpcResult<Option<Value>> {
    manager
        .dispatch(
            owner,
            tx.clone(),
            json!(id),
            method,
            params,
            Path::new("/tmp"),
        )
        .await
}
async fn recv(rx: &mut mpsc::Receiver<Value>) -> Value {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("process response timed out")
        .expect("connection closed")
}
async fn exited(rx: &mut mpsc::Receiver<Value>, handle: &str) -> (Value, Vec<Value>) {
    let mut messages = Vec::new();
    loop {
        let message = recv(rx).await;
        if message["method"] == "process/exited" && message["params"]["processHandle"] == handle {
            return (message["params"].clone(), messages);
        }
        messages.push(message);
    }
}
fn output(messages: &[Value], stream: &str) -> Vec<u8> {
    messages
        .iter()
        .filter(|m| m["method"] == "process/outputDelta" && m["params"]["stream"] == stream)
        .flat_map(|m| {
            STANDARD
                .decode(m["params"]["deltaBase64"].as_str().unwrap())
                .unwrap()
        })
        .collect()
}
fn assert_exit_shape(result: &Value) {
    assert!(result["processHandle"].is_string());
    assert!(result["exitCode"].is_i64());
    assert!(result["stdout"].is_string());
    assert!(result["stderr"].is_string());
    assert!(result["stdoutCapReached"].is_boolean());
    assert!(result["stderrCapReached"].is_boolean());
}

#[tokio::test]
async fn installed_host_read_streams_binary_file_without_default_cap() {
    let manager = CommandManager::default();
    let (tx, mut rx) = mpsc::channel(32);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("plugin-icon.bin");
    let expected: Vec<u8> = (0..1_100_000).map(|n| (n % 256) as u8).collect();
    std::fs::write(&path, &expected).unwrap();
    // bootstrap's host read uses cat and yA enables both streaming directions.
    assert!(
        call(
            &manager,
            &tx,
            1,
            1,
            "process/spawn",
            json!({
                "command":["/bin/cat",path],"cwd":"/","processHandle":"icon",
                "streamStdoutStderr":true,"streamStdin":true,"timeoutMs":null,"outputBytesCap":null
            })
        )
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(recv(&mut rx).await, json!({"id":1,"result":{}}));
    let (result, messages) = exited(&mut rx, "icon").await;
    assert_exit_shape(&result);
    assert_eq!(result["exitCode"], 0);
    assert_eq!(result["stdout"], "");
    assert_eq!(result["stdoutCapReached"], false);
    assert_eq!(output(&messages, "stdout"), expected);
    assert!(
        messages
            .iter()
            .all(|m| m["method"] == "process/outputDelta")
    );
}

#[tokio::test]
async fn installed_canonical_path_lookup_and_missing_file_exit() {
    let manager = CommandManager::default();
    let (tx, mut rx) = mpsc::channel(32);
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real");
    let link = dir.path().join("link");
    std::fs::create_dir(&real).unwrap();
    std::os::unix::fs::symlink(&real, &link).unwrap();
    call(
        &manager,
        &tx,
        1,
        1,
        "process/spawn",
        json!({
            "command":["/bin/pwd","-P"],"cwd":link,"processHandle":"path",
            "streamStdoutStderr":true,"streamStdin":true,"timeoutMs":5000,"outputBytesCap":16384
        }),
    )
    .await
    .unwrap();
    assert_eq!(recv(&mut rx).await["result"], json!({}));
    let (result, messages) = exited(&mut rx, "path").await;
    assert_eq!(result["exitCode"], 0);
    assert_eq!(
        String::from_utf8(output(&messages, "stdout"))
            .unwrap()
            .trim(),
        real.canonicalize().unwrap().to_str().unwrap()
    );
    // The handle can be reused immediately after the exit notification.
    call(
        &manager,
        &tx,
        1,
        2,
        "process/spawn",
        json!({"command":["/bin/cat",dir.path().join("absent")],"cwd":"/","processHandle":"path"}),
    )
    .await
    .unwrap();
    assert_eq!(recv(&mut rx).await["id"], 2);
    let (result, _) = exited(&mut rx, "path").await;
    assert_ne!(result["exitCode"], 0);
    assert!(!result["stderr"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn spawn_ack_is_immediate_stdin_streams_and_handles_are_owned() {
    let manager = CommandManager::default();
    let (tx, mut rx) = mpsc::channel(32);
    call(&manager, &tx, 1, 1, "process/spawn", json!({"command":["/bin/cat"],"cwd":"/","processHandle":"cat","streamStdin":true,"streamStdoutStderr":true,"timeoutMs":null})).await.unwrap();
    assert_eq!(recv(&mut rx).await, json!({"id":1,"result":{}}));
    assert!(
        call(
            &manager,
            &tx,
            1,
            2,
            "process/spawn",
            json!({"command":["/bin/true"],"cwd":"/","processHandle":"cat"})
        )
        .await
        .is_err()
    );
    assert!(
        call(
            &manager,
            &tx,
            2,
            3,
            "process/kill",
            json!({"processHandle":"cat"})
        )
        .await
        .is_err()
    );
    assert!(
        call(
            &manager,
            &tx,
            1,
            4,
            "command/exec/terminate",
            json!({"processId":"cat"})
        )
        .await
        .is_err()
    );
    assert!(
        call(
            &manager,
            &tx,
            1,
            5,
            "process/writeStdin",
            json!({"processHandle":"cat"})
        )
        .await
        .is_err()
    );
    call(&manager, &tx, 1, 6, "process/writeStdin", json!({"processHandle":"cat","deltaBase64":STANDARD.encode(b"stdin bytes\0\n"),"closeStdin":true})).await.unwrap();
    let (result, messages) = exited(&mut rx, "cat").await;
    assert_eq!(result["exitCode"], 0);
    assert_eq!(output(&messages, "stdout"), b"stdin bytes\0\n");
}

#[tokio::test]
async fn caps_defaults_explicit_null_and_timeout_follow_process_contract() {
    let manager = CommandManager::default();
    let (tx, mut rx) = mpsc::channel(32);
    for (id, options, expected, capped) in [
        (1, json!({}), 1024 * 1024, true),
        (2, json!({"outputBytesCap":null}), 1024 * 1024 + 8192, false),
        (3, json!({"outputBytesCap":7}), 7, true),
    ] {
        let mut params = json!({"command":["/bin/sh","-c","dd if=/dev/zero bs=8192 count=129 2>/dev/null"],"cwd":"/","processHandle":"cap"});
        params
            .as_object_mut()
            .unwrap()
            .extend(options.as_object().unwrap().clone());
        call(&manager, &tx, 1, id, "process/spawn", params)
            .await
            .unwrap();
        assert_eq!(recv(&mut rx).await["id"], id);
        let (result, _) = exited(&mut rx, "cap").await;
        assert_exit_shape(&result);
        assert_eq!(result["exitCode"], 0);
        assert_eq!(result["stdout"].as_str().unwrap().len(), expected);
        assert_eq!(result["stdoutCapReached"], capped);
        assert_eq!(result["stderrCapReached"], false);
    }
    call(
        &manager,
        &tx,
        1,
        4,
        "process/spawn",
        json!({"command":["/bin/sleep","30"],"cwd":"/","processHandle":"timeout","timeoutMs":20}),
    )
    .await
    .unwrap();
    assert_eq!(recv(&mut rx).await["id"], 4);
    assert_eq!(exited(&mut rx, "timeout").await.0["exitCode"], 124);
}

#[tokio::test]
async fn kill_and_disconnect_reap_pty_and_pipe_processes() {
    let manager = CommandManager::default();
    let (tx, mut rx) = mpsc::channel(32);
    call(&manager, &tx, 1, 1, "process/spawn", json!({"command":["/bin/sh","-c","stty -echo; stty size; read line; stty size; printf ready; exec /bin/sleep 30"],"cwd":"/","processHandle":"pty","tty":true,"size":{"rows":24,"cols":80},"timeoutMs":null})).await.unwrap();
    assert_eq!(recv(&mut rx).await["id"], 1);
    let first = recv(&mut rx).await;
    assert_eq!(
        String::from_utf8(output(&[first], "stdout"))
            .unwrap()
            .trim(),
        "24 80"
    );
    assert_eq!(
        call(
            &manager,
            &tx,
            1,
            2,
            "process/resizePty",
            json!({"processHandle":"pty","size":{"rows":40,"cols":100}})
        )
        .await
        .unwrap(),
        Some(json!({}))
    );
    call(
        &manager,
        &tx,
        1,
        3,
        "process/writeStdin",
        json!({"processHandle":"pty","deltaBase64":STANDARD.encode("go\n")}),
    )
    .await
    .unwrap();
    let mut messages = Vec::new();
    loop {
        messages.push(recv(&mut rx).await);
        if String::from_utf8_lossy(&output(&messages, "stdout")).contains("ready") {
            break;
        }
    }
    assert!(String::from_utf8_lossy(&output(&messages, "stdout")).contains("40 100"));
    assert_eq!(
        call(
            &manager,
            &tx,
            1,
            4,
            "process/kill",
            json!({"processHandle":"pty"})
        )
        .await
        .unwrap(),
        Some(json!({}))
    );
    assert_ne!(exited(&mut rx, "pty").await.0["exitCode"], 0);
    call(&manager, &tx, 2, 5, "process/spawn", json!({"command":["/bin/sh","-c","printf '%s\\n' $$; exec /bin/sleep 30"],"cwd":"/","processHandle":"pipe","streamStdoutStderr":true,"timeoutMs":null})).await.unwrap();
    assert_eq!(recv(&mut rx).await["id"], 5);
    let message = recv(&mut rx).await;
    let pid: i32 = String::from_utf8(output(&[message], "stdout"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    manager.disconnect(2).await;
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
}

#[tokio::test]
async fn process_spawn_rejects_relative_cwd_and_command_only_settings() {
    let manager = CommandManager::default();
    let (tx, _rx) = mpsc::channel(32);
    for options in [
        json!({"cwd":"relative"}),
        json!({"processHandle":""}),
        json!({"disableTimeout":true}),
        json!({"sandboxPolicy":{"type":"dangerFullAccess"}}),
        json!({"timeoutMs":-1}),
    ] {
        let mut params = json!({"command":["/bin/true"],"cwd":"/","processHandle":"bad"});
        params
            .as_object_mut()
            .unwrap()
            .extend(options.as_object().unwrap().clone());
        assert_eq!(
            call(&manager, &tx, 1, 1, "process/spawn", params)
                .await
                .unwrap_err()
                .code,
            -32602
        );
    }
}
