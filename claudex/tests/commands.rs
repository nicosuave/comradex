use base64::{Engine, engine::general_purpose::STANDARD};
use claude_codex_server::{commands::CommandManager, protocol};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use tokio::sync::mpsc;

async fn call(
    manager: &CommandManager,
    output: &mpsc::Sender<Value>,
    owner: u64,
    id: u64,
    method: &str,
    params: Value,
) -> protocol::RpcResult<Option<Value>> {
    manager
        .dispatch(
            owner,
            output.clone(),
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
        .expect("response timeout")
        .expect("channel closed")
}
async fn final_response(rx: &mut mpsc::Receiver<Value>, id: u64) -> (Value, Vec<Value>) {
    let mut messages = Vec::new();
    loop {
        let message = recv(rx).await;
        if message["id"] == id {
            return (message, messages);
        }
        messages.push(message);
    }
}

#[tokio::test]
async fn buffered_argv_cwd_environment_stdin_eof_and_caps() {
    let manager = CommandManager::default();
    let (tx, mut rx) = mpsc::channel(32);
    let dir = tempfile::tempdir().unwrap();
    manager.dispatch(1, tx.clone(), json!(1), "command/exec", json!({
        "command":["/bin/sh","-c","read ignored; printf '%s|%s|%s' \"$PWD\" \"$COMMAND_TEST\" \"${HOME-unset}\"; printf error >&2"],
        "cwd":dir.path(), "env":{"COMMAND_TEST":"literal $value", "HOME":null}
    }), dir.path()).await.unwrap();
    let (result, notifications) = final_response(&mut rx, 1).await;
    assert_eq!(result["result"]["exitCode"], 0);
    assert!(
        result["result"]["stdout"]
            .as_str()
            .unwrap()
            .ends_with("|literal $value|unset")
    );
    assert!(
        result["result"]["stdout"]
            .as_str()
            .unwrap()
            .contains(dir.path().file_name().unwrap().to_str().unwrap())
    );
    assert_eq!(result["result"]["stderr"], "error");
    assert!(notifications.is_empty());
    call(&manager, &tx, 1, 2, "command/exec", json!({"command":["/bin/sh","-c","printf 123456789; printf abcdef >&2"],"outputBytesCap":4})).await.unwrap();
    let (result, _) = final_response(&mut rx, 2).await;
    assert_eq!(result["result"]["stdout"], "1234");
    assert_eq!(result["result"]["stderr"], "abcd");
    call(
        &manager,
        &tx,
        1,
        3,
        "command/exec",
        json!({"command":["/bin/sh","-c","printf 123456789"],"disableOutputCap":true}),
    )
    .await
    .unwrap();
    assert_eq!(
        final_response(&mut rx, 3).await.0["result"]["stdout"],
        "123456789"
    );
}

#[tokio::test]
async fn streaming_stdin_close_and_notifications_precede_final() {
    let manager = CommandManager::default();
    let (tx, mut rx) = mpsc::channel(32);
    call(&manager, &tx, 1, 1, "command/exec", json!({"command":["/bin/cat"],"processId":"cat","streamStdin":true,"streamStdoutStderr":true})).await.unwrap();
    call(&manager, &tx, 1, 2, "command/exec/write", json!({"processId":"cat","deltaBase64":STANDARD.encode(b"hello\0world\n"),"closeStdin":true})).await.unwrap();
    let (result, messages) = final_response(&mut rx, 1).await;
    assert_eq!(
        result["result"],
        json!({"exitCode":0,"stdout":"","stderr":""})
    );
    let bytes: Vec<u8> = messages
        .iter()
        .filter(|m| m["method"] == "command/exec/outputDelta")
        .flat_map(|m| {
            STANDARD
                .decode(m["params"]["deltaBase64"].as_str().unwrap())
                .unwrap()
        })
        .collect();
    assert_eq!(bytes, b"hello\0world\n");
}

#[tokio::test]
async fn concurrent_execution_ids_are_connection_scoped_and_terminate_reaps() {
    let manager = CommandManager::default();
    let (tx, mut rx) = mpsc::channel(32);
    call(&manager, &tx, 1, 1, "command/exec", json!({"command":["/bin/sh","-c","printf '%s\\n' $$; exec /bin/sleep 30"],"processId":"same","streamStdoutStderr":true,"disableTimeout":true})).await.unwrap();
    let first = recv(&mut rx).await;
    let bytes = STANDARD
        .decode(first["params"]["deltaBase64"].as_str().unwrap())
        .unwrap();
    let pid: i32 = String::from_utf8(bytes).unwrap().trim().parse().unwrap();
    assert!(
        call(
            &manager,
            &tx,
            2,
            2,
            "command/exec/terminate",
            json!({"processId":"same"})
        )
        .await
        .is_err()
    );
    assert!(
        call(
            &manager,
            &tx,
            1,
            3,
            "command/exec",
            json!({"command":["/bin/true"],"processId":"same"})
        )
        .await
        .is_err()
    );
    call(
        &manager,
        &tx,
        2,
        4,
        "command/exec",
        json!({"command":["/bin/sh","-c","printf quick"],"processId":"same"}),
    )
    .await
    .unwrap();
    assert_eq!(
        final_response(&mut rx, 4).await.0["result"]["stdout"],
        "quick"
    );
    call(
        &manager,
        &tx,
        1,
        5,
        "command/exec/terminate",
        json!({"processId":"same"}),
    )
    .await
    .unwrap();
    let (result, _) = final_response(&mut rx, 1).await;
    assert_ne!(result["result"]["exitCode"], 0);
    assert_eq!(
        unsafe { libc::kill(pid, 0) },
        -1,
        "child must be reaped before final response"
    );
}

#[tokio::test]
async fn timeout_and_disconnect_kill_real_process_groups() {
    let manager = CommandManager::default();
    let (tx, mut rx) = mpsc::channel(32);
    call(
        &manager,
        &tx,
        1,
        1,
        "command/exec",
        json!({"command":["/bin/sleep","30"],"timeoutMs":30}),
    )
    .await
    .unwrap();
    assert_eq!(
        final_response(&mut rx, 1).await.0["result"]["exitCode"],
        124
    );
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("orphan-marker");
    call(&manager, &tx, 2, 2, "command/exec", json!({"command":["/bin/sh","-c","(sleep 1; printf orphan > \"$1\") & printf '%s\\n' $$; wait","sh",marker],"processId":"tree","streamStdoutStderr":true,"disableTimeout":true})).await.unwrap();
    let message = recv(&mut rx).await;
    let bytes = STANDARD
        .decode(message["params"]["deltaBase64"].as_str().unwrap())
        .unwrap();
    let pid: i32 = String::from_utf8(bytes).unwrap().trim().parse().unwrap();
    manager.disconnect(2).await;
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(!marker.exists(), "descendant escaped disconnect cleanup");
}

#[tokio::test]
async fn pty_initial_size_resize_and_interactive_input() {
    let manager = CommandManager::default();
    let (tx, mut rx) = mpsc::channel(32);
    call(&manager, &tx, 1, 1, "command/exec", json!({"command":["/bin/sh","-c","stty -echo; stty size; read line; stty size; printf 'received:%s' \"$line\""],"processId":"pty","tty":true,"size":{"rows":31,"cols":91}})).await.unwrap();
    let first = recv(&mut rx).await;
    let first_bytes = STANDARD
        .decode(first["params"]["deltaBase64"].as_str().unwrap())
        .unwrap();
    assert!(String::from_utf8_lossy(&first_bytes).contains("31 91"));
    call(
        &manager,
        &tx,
        1,
        2,
        "command/exec/resize",
        json!({"processId":"pty","size":{"rows":42,"cols":102}}),
    )
    .await
    .unwrap();
    call(
        &manager,
        &tx,
        1,
        3,
        "command/exec/write",
        json!({"processId":"pty","deltaBase64":STANDARD.encode("hello\n")}),
    )
    .await
    .unwrap();
    let (result, messages) = final_response(&mut rx, 1).await;
    assert_eq!(result["result"]["exitCode"], 0);
    let bytes: Vec<u8> = messages
        .iter()
        .filter(|m| m["method"] == "command/exec/outputDelta")
        .flat_map(|m| {
            STANDARD
                .decode(m["params"]["deltaBase64"].as_str().unwrap())
                .unwrap()
        })
        .collect();
    let text = String::from_utf8_lossy(&bytes);
    assert!(text.contains("42 102"), "{text}");
    assert!(text.contains("received:hello"), "{text}");
    assert_eq!(result["result"]["stdout"], "");
}

#[tokio::test]
async fn capped_stream_drains_large_output_and_marks_the_last_chunk() {
    let manager = CommandManager::default();
    let (tx, mut rx) = mpsc::channel(32);
    call(&manager, &tx, 1, 1, "command/exec", json!({"command":["/bin/sh","-c","dd if=/dev/zero bs=8192 count=64 2>/dev/null; printf complete >&2"],"processId":"capped","streamStdoutStderr":true,"outputBytesCap":17})).await.unwrap();
    let (result, messages) = final_response(&mut rx, 1).await;
    assert_eq!(result["result"]["exitCode"], 0);
    let stdout: Vec<_> = messages
        .iter()
        .filter(|m| m["params"]["stream"] == "stdout")
        .collect();
    let bytes: Vec<u8> = stdout
        .iter()
        .flat_map(|m| {
            STANDARD
                .decode(m["params"]["deltaBase64"].as_str().unwrap())
                .unwrap()
        })
        .collect();
    assert_eq!(bytes, vec![0; 17]);
    assert_eq!(stdout.last().unwrap()["params"]["capReached"], true);
    assert!(messages.iter().any(|m| m["params"]["stream"] == "stderr"));
}

#[tokio::test]
async fn blocked_stdin_does_not_block_termination_and_writes_remain_ordered() {
    let manager = CommandManager::default();
    let (tx, mut rx) = mpsc::channel(64);
    call(
        &manager,
        &tx,
        1,
        1,
        "command/exec",
        json!({"command":["/bin/cat"],"processId":"ordered","streamStdin":true}),
    )
    .await
    .unwrap();
    for n in 0..10 {
        call(&manager, &tx, 1, 10+n, "command/exec/write", json!({"processId":"ordered","deltaBase64":STANDARD.encode(n.to_string()),"closeStdin":n==9})).await.unwrap();
    }
    assert_eq!(
        final_response(&mut rx, 1).await.0["result"]["stdout"],
        "0123456789"
    );
    call(&manager, &tx, 2, 100, "command/exec", json!({"command":["/bin/sleep","30"],"processId":"blocked","streamStdin":true,"disableTimeout":true})).await.unwrap();
    call(
        &manager,
        &tx,
        2,
        101,
        "command/exec/write",
        json!({"processId":"blocked","deltaBase64":STANDARD.encode(vec![0; 1024*1024])}),
    )
    .await
    .unwrap();
    call(
        &manager,
        &tx,
        2,
        102,
        "command/exec/terminate",
        json!({"processId":"blocked"}),
    )
    .await
    .unwrap();
    assert_ne!(
        final_response(&mut rx, 100).await.0["result"]["exitCode"],
        0
    );
}

#[tokio::test]
async fn unsupported_permissions_and_conflicting_options_fail_before_execution() {
    let manager = CommandManager::default();
    let (tx, _rx) = mpsc::channel(32);
    for options in [
        json!({"permissionProfile":"restricted"}),
        json!({"sandboxPolicy":{"type":"readOnly"}}),
        json!({"streamStdin":true}),
        json!({"timeoutMs":-1}),
        json!({"timeoutMs":10,"disableTimeout":true}),
        json!({"outputBytesCap":10,"disableOutputCap":true}),
        json!({"size":{"rows":1,"cols":1}}),
    ] {
        let mut params = json!({"command":["/bin/true"]});
        params
            .as_object_mut()
            .unwrap()
            .extend(options.as_object().unwrap().clone());
        assert_eq!(
            call(&manager, &tx, 1, 1, "command/exec", params)
                .await
                .unwrap_err()
                .code,
            -32602
        );
    }
}
