#![cfg(feature = "test-backend")]

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

async fn start(state: &Path, endpoint: &str) -> (Child, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
        .args([
            "--listen",
            endpoint,
            "--claude",
            env!("CARGO_BIN_EXE_fake-claude"),
        ])
        .arg("--state-dir")
        .arg(state)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
    let line = tokio::time::timeout(Duration::from_secs(15), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(line.starts_with("Listening on "), "{line}");
    (child, line.trim_start_matches("Listening on ").to_owned())
}

fn initialize() -> Value {
    json!({"id":"hello","method":"initialize","params":{"clientInfo":{"name":"transport-tests","version":"1"}}})
}

#[tokio::test]
async fn large_saved_histories_do_not_delay_service_readiness() {
    use claude_codex_server::store::{Record, Settings, Store};

    let temp = tempfile::tempdir().unwrap();
    let mut expected = Vec::new();
    {
        let store = Store::open(temp.path()).unwrap();
        // Unbuffered JSON reads issue a syscall per byte. A realistic history
        // size must still fit inside the service manager's 20-second deadline.
        for _ in 0..4 {
            let mut record = Record::new(
                Settings::new(temp.path().to_path_buf(), "sonnet".into()),
                store.threads_dir(),
                &json!({}),
                "startup-test",
            );
            record.turns = vec![json!({
                "id": uuid::Uuid::new_v4().to_string(), "status":"completed",
                "items":[{"id":uuid::Uuid::new_v4().to_string(), "type":"agentMessage",
                    "text":"history ".repeat(1024 * 1024)}]
            })];
            store.save(&record).unwrap();
            expected.push(record.id().to_owned());
        }
    }
    // start() waits at most 15s for the listener, leaving room for the health
    // handshake before the installed manager's readiness deadline.
    let (mut child, address) = start(temp.path(), "ws://127.0.0.1:0").await;
    let (mut ws, _) = connect_async(&address).await.unwrap();
    ws.send(Message::Text(initialize().to_string().into()))
        .await
        .unwrap();
    let hello = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let hello: Value = serde_json::from_str(hello.to_text().unwrap()).unwrap();
    assert!(hello["result"]["userAgent"].is_string());
    for id in expected {
        ws.send(Message::Text(
            json!({"id":id,"method":"thread/read","params":{"threadId":id,"includeTurns":true}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let reply: Value = serde_json::from_str(reply.to_text().unwrap()).unwrap();
        assert_eq!(reply["result"]["thread"]["id"], id);
        assert_eq!(
            reply["result"]["thread"]["turns"][0]["items"][0]["text"],
            "history ".repeat(1024 * 1024)
        );
    }
    ws.close(None).await.unwrap();
    child.kill().await.unwrap();
}

#[tokio::test]
async fn websocket_supports_native_clients_and_rejects_all_browser_origins() {
    let temp = tempfile::tempdir().unwrap();
    let (mut child, address) = start(temp.path(), "ws://127.0.0.1:0").await;
    for origin in [
        "https://untrusted.example",
        "http://localhost:4511",
        "http://127.0.0.1:4511",
        "null",
    ] {
        let mut request = address.clone().into_client_request().unwrap();
        request
            .headers_mut()
            .insert("origin", origin.parse().unwrap());
        let error = connect_async(request).await.unwrap_err();
        assert!(error.to_string().contains("403"), "{origin}: {error}");
    }
    let (mut a, _) = connect_async(&address).await.unwrap();
    let (mut b, _) = connect_async(&address).await.unwrap();
    a.send(Message::Text(initialize().to_string().into()))
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(5), a.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let value: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    assert_eq!(value["id"], "hello");
    assert!(value["result"]["userAgent"].is_string());
    b.send(Message::Text(
        json!({"id":2,"method":"model/list","params":{}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(5), b.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let value: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    assert_eq!(value["error"]["code"], -32000);
    a.close(None).await.unwrap();
    b.close(None).await.unwrap();
    child.kill().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn service_recovers_its_socket_after_an_unclean_exit() {
    let temp = tempfile::Builder::new()
        .prefix("facade-crash-")
        .tempdir_in("/tmp")
        .unwrap();
    let path = temp.path().join("rpc.sock");
    let endpoint = format!("unix-lines://{}", path.display());
    let (mut first, _) = start(temp.path(), &endpoint).await;
    first.kill().await.unwrap();
    assert!(path.exists(), "SIGKILL should leave the socket behind");
    let (mut replacement, _) = start(temp.path(), &endpoint).await;
    let stream = tokio::net::UnixStream::connect(&path).await.unwrap();
    let (read, mut write) = stream.into_split();
    write
        .write_all(format!("{}\n", initialize()).as_bytes())
        .await
        .unwrap();
    let response = BufReader::new(read)
        .lines()
        .next_line()
        .await
        .unwrap()
        .unwrap();
    let response: Value = serde_json::from_str(&response).unwrap();
    assert_eq!(response["id"], "hello");
    replacement.kill().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn existing_files_symlinks_and_live_sockets_are_not_replaced() {
    use claude_codex_server::{server::Event, transport};
    let temp = tempfile::Builder::new()
        .prefix("facade-path-")
        .tempdir_in("/tmp")
        .unwrap();
    let regular = temp.path().join("keep.txt");
    std::fs::write(&regular, "user content").unwrap();
    let link = temp.path().join("link");
    std::os::unix::fs::symlink(&regular, &link).unwrap();
    let live = temp.path().join("live.sock");
    let listener = tokio::net::UnixListener::bind(&live).unwrap();
    for path in [&regular, &link, &live] {
        let (sender, _receiver) = tokio::sync::mpsc::channel::<Event>(1);
        assert!(
            transport::serve(
                &format!("unix://{}", path.display()),
                sender,
                tokio_util::sync::CancellationToken::new()
            )
            .await
            .is_err()
        );
        assert!(path.symlink_metadata().is_ok());
    }
    assert_eq!(std::fs::read_to_string(regular).unwrap(), "user content");
    assert!(tokio::net::UnixStream::connect(&live).await.is_ok());
    drop(listener);
}

#[cfg(unix)]
#[tokio::test]
async fn unix_socket_is_private_framed_and_removed_on_graceful_shutdown() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::Builder::new()
        .prefix("facade-")
        .tempdir_in("/tmp")
        .unwrap();
    let path = temp.path().join("rpc.sock");
    let (mut child, _) = start(temp.path(), &format!("unix-lines://{}", path.display())).await;
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let stream = tokio::net::UnixStream::connect(&path).await.unwrap();
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read).lines();
    // Split a single frame across writes; the server must wait for its newline.
    let input = initialize().to_string();
    write.write_all(&input.as_bytes()[..15]).await.unwrap();
    write.write_all(&input.as_bytes()[15..]).await.unwrap();
    write.write_all(b"\n").await.unwrap();
    let line = tokio::time::timeout(Duration::from_secs(5), read.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let value: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(value["id"], "hello");
    unsafe {
        libc::kill(child.id().unwrap() as i32, libc::SIGTERM);
    }
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success(), "facade exit status: {status}");
    assert!(!path.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn shutdown_signals_are_handled_as_soon_as_the_listener_is_ready() {
    // Exercise the readiness boundary repeatedly because either process can run first.
    for signal in [libc::SIGTERM, libc::SIGINT] {
        for _ in 0..8 {
            let temp = tempfile::Builder::new()
                .prefix("facade-signal-")
                .tempdir_in("/tmp")
                .unwrap();
            let path = temp.path().join("rpc.sock");
            let (mut child, _) =
                start(temp.path(), &format!("unix-lines://{}", path.display())).await;
            assert_eq!(unsafe { libc::kill(child.id().unwrap() as i32, signal) }, 0);
            let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
                .await
                .unwrap()
                .unwrap();
            assert!(
                status.success(),
                "signal {signal}, facade exit status: {status}"
            );
            assert!(!path.exists());
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn desktop_bootstrap_and_proxy_carry_websocket_chat_and_approval() {
    let temp = tempfile::Builder::new()
        .prefix("facade-desktop-")
        .tempdir_in("/tmp")
        .unwrap();
    let sockets = temp.path().join("sockets");
    let binary = env!("CARGO_BIN_EXE_claude-codex-server");
    let mut server = Command::new(binary)
        .args([
            "-c",
            "features.code_mode_host=true",
            "app-server",
            "--listen",
            "unix://",
            "--claude",
            env!("CARGO_BIN_EXE_fake-claude"),
        ])
        .arg("--state-dir")
        .arg(temp.path().join("state"))
        .env("CLAUDE_CODEX_SOCKET_DIR", &sockets)
        .env("CODEX_HOME", temp.path().join("real-codex-do-not-touch"))
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut log = BufReader::new(server.stderr.take().unwrap()).lines();
    let ready = tokio::time::timeout(Duration::from_secs(15), log.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(ready.starts_with("Listening on unix://"), "{ready}");
    assert!(!temp.path().join("real-codex-do-not-touch").exists());
    let mut proxy = Command::new(binary)
        .args(["app-server", "proxy", "--sock"])
        .arg(sockets.join("app-server-control.sock"))
        .env_remove("CLAUDE_CODEX_SOCKET_DIR")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let stream = tokio::io::join(proxy.stdout.take().unwrap(), proxy.stdin.take().unwrap());
    let (mut ws, _) = tokio_tungstenite::client_async("ws://codex-app-server/rpc", stream)
        .await
        .unwrap();
    ws.send(Message::Text(initialize().to_string().into()))
        .await
        .unwrap();
    let hello = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let hello: Value = serde_json::from_str(hello.to_text().unwrap()).unwrap();
    assert!(hello["result"]["userAgent"].is_string(), "{hello}");
    ws.send(Message::Text(
        json!({"method":"initialized"}).to_string().into(),
    ))
    .await
    .unwrap();
    ws.send(Message::Text(
        json!({"id":2,"method":"thread/start","params":{"sandbox":"danger-full-access"}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let thread = loop {
        let value = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let value: Value = serde_json::from_str(value.to_text().unwrap()).unwrap();
        if value["id"] == 2 {
            assert!(value.get("error").is_none(), "{value}");
            break value["result"]["thread"]["id"].clone();
        }
    };
    ws.send(Message::Text(json!({"id":3,"method":"turn/start","params":{"threadId":thread,"input":[{"type":"text","text":"tool","text_elements":[]}]}}).to_string().into())).await.unwrap();
    let mut approved = false;
    loop {
        let value = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let value: Value = serde_json::from_str(value.to_text().unwrap()).unwrap();
        assert!(value.get("error").is_none(), "{value}");
        if value["method"] == "item/commandExecution/requestApproval" {
            assert_eq!(value["params"]["command"], "printf fake");
            ws.send(Message::Text(
                json!({"id":value["id"],"result":{"decision":"accept"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            approved = true;
        }
        if value["method"] == "turn/completed" {
            assert!(approved);
            assert_eq!(value["params"]["turn"]["status"], "completed");
            assert!(
                value["params"]["turn"]["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|item| item["aggregatedOutput"] == "fake tool output")
            );
            break;
        }
    }
    ws.close(None).await.unwrap();
    // Finish the close handshake before dropping the proxy's stdout reader.
    // Otherwise its pending close-frame write can correctly fail with EPIPE.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = ws.next().await.unwrap().unwrap();
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    })
    .await
    .unwrap();
    drop(ws);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), proxy.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    unsafe {
        libc::kill(server.id().unwrap() as i32, libc::SIGTERM);
    }
    assert!(
        tokio::time::timeout(Duration::from_secs(5), server.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(!sockets.join("app-server-control.sock").exists());
}
