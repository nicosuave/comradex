use super::*;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

// Behave like a compression-capable upstream at handshake time. No extension
// may be negotiated when Comradex terminates and parses the WebSocket frames.
async fn exercise_extensions(mode: ResponsesWebsocketMode, path: &str, raw: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (handshakes, mut observed) = mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        for turn in 0..if raw { 1 } else { 2 } {
            let (stream, _) = listener.accept().await.unwrap();
            let handshakes = handshakes.clone();
            let mut socket = tokio_tungstenite::accept_hdr_async(
                stream,
                move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                      mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                    let offered = request.headers().contains_key(SEC_WEBSOCKET_EXTENSIONS);
                    handshakes.send(offered).unwrap();
                    if offered {
                        response.headers_mut().insert(
                            SEC_WEBSOCKET_EXTENSIONS,
                            "permessage-deflate".parse().unwrap(),
                        );
                    }
                    Ok(response)
                },
            )
            .await
            .unwrap();
            let create = socket.next().await.unwrap().unwrap();
            assert!(create.is_text());
            for (event, status) in [
                ("response.created", "in_progress"),
                ("response.completed", "completed"),
            ] {
                socket.send(Message::Text(serde_json::json!({
                    "type": event,
                    "response": {"id": format!("resp_extensions_{turn}"), "status": status, "output": []}
                }).to_string().into())).await.unwrap();
            }
            if turn == 0 && !raw {
                socket.close(None).await.unwrap();
            } else {
                // Keep the final upstream alive until the test client is done.
                let _ = socket.next().await;
            }
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let (proxy, proxy_task) = tests::start_caller_proxy(
        dir.path(),
        format!("http://{address}/backend-api/codex"),
        mode,
    )
    .await;
    let mut request = format!("ws://{proxy}/0123456789abcdef{path}")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert(AUTHORIZATION, "Bearer caller-token".parse().unwrap());
    request.headers_mut().insert(
        SEC_WEBSOCKET_EXTENSIONS,
        "permessage-deflate; client_max_window_bits"
            .parse()
            .unwrap(),
    );
    let (mut socket, response) =
        tokio_tungstenite::client_async(request, TcpStream::connect(proxy).await.unwrap())
            .await
            .unwrap();
    assert_eq!(
        response.headers().contains_key(SEC_WEBSOCKET_EXTENSIONS),
        raw
    );
    for _ in 0..if raw { 1 } else { 2 } {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), observed.recv())
                .await
                .unwrap(),
            Some(raw),
            "initial and replacement upstream sockets must agree on extension support"
        );
        socket
            .send(Message::Text(
                serde_json::json!({
                    "type": "response.create", "model": "test", "input": []
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        for expected in ["response.created", "response.completed"] {
            let event = tokio::time::timeout(Duration::from_secs(3), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let value: serde_json::Value = serde_json::from_str(event.to_text().unwrap()).unwrap();
            assert_eq!(value["type"], expected);
        }
    }
    drop(socket);
    proxy_task.abort();
    server.abort();
}

#[tokio::test]
async fn direct_websocket_declines_extensions_on_initial_and_replacement_sockets() {
    exercise_extensions(ResponsesWebsocketMode::Direct, "/v1/responses", false).await;
}

#[tokio::test]
async fn backend_websocket_declines_extensions_even_with_raw_config() {
    exercise_extensions(
        ResponsesWebsocketMode::Raw,
        "/backend-api/codex/responses",
        false,
    )
    .await;
}

#[tokio::test]
async fn raw_websocket_preserves_extension_negotiation() {
    exercise_extensions(ResponsesWebsocketMode::Raw, "/v1/responses", true).await;
}
