async fn direct_capacity_proxy(
    dir: &std::path::Path,
    max_upgrades: usize,
) -> (
    std::net::SocketAddr,
    Arc<App>,
    tokio::task::JoinHandle<Result<()>>,
    tokio::task::JoinHandle<()>,
) {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let server = tokio::spawn(async move {
        loop {
            let (stream, _) = upstream.accept().await.unwrap();
            tokio::spawn(async move {
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                while let Some(Ok(message)) = socket.next().await {
                    if message.is_close() {
                        break;
                    }
                    if !message.is_text() {
                        continue;
                    }
                    let create: serde_json::Value =
                        serde_json::from_str(message.to_text().unwrap()).unwrap();
                    let held = create["model"] == "hold";
                    socket.send(Message::Text(serde_json::json!({
                        "type":"response.created", "response":{"id":"resp_capacity_test","status":"in_progress"}
                    }).to_string().into())).await.unwrap();
                    if !held {
                        socket.send(Message::Text(serde_json::json!({
                            "type":"response.completed", "response":{"id":"resp_capacity_test","status":"completed","output":[]}
                        }).to_string().into())).await.unwrap();
                    }
                }
            });
        }
    });
    let (app, mut listener, _, _) = direct_test_app_with_upgrade_limit(
        dir,
        format!("http://{upstream_address}/backend-api/codex"),
        max_upgrades,
    );
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    listener.address = address;
    let proxy = tokio::spawn(app.clone().serve_tcp("default".into(), listener, tcp));
    (address, app, proxy, server)
}

async fn direct_capacity_event(socket: &mut WebSocketStream<TcpStream>) -> serde_json::Value {
    let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}

#[tokio::test]
async fn direct_capacity_reclaims_completed_socket_and_reconnects_repeatedly() {
    let dir = tempfile::tempdir().unwrap();
    let (address, app, proxy, server) = direct_capacity_proxy(dir.path(), 1).await;
    let mut first = connect_test_websocket(address).await;
    for _ in 0..4 {
        first
            .send(Message::Text(
                serde_json::json!({
                    "type":"response.create", "model":"complete", "input":[]
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        assert_eq!(
            direct_capacity_event(&mut first).await["type"],
            "response.created"
        );
        assert_eq!(
            direct_capacity_event(&mut first).await["type"],
            "response.completed"
        );
        assert!(app.acquire_upgrade_slot(false).await.is_none());
        let second = connect_test_websocket(address).await;
        let message = tokio::time::timeout(Duration::from_secs(3), first.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Message::Close(Some(close)) = message else {
            panic!("expected idle close: {message:?}");
        };
        assert_eq!(
            close.code,
            tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Away
        );
        assert_eq!(app.stats.open_upgrades.load(Ordering::Relaxed), 1);
        first = second;
    }
    drop(first);
    app.shutdown_connections().await;
    assert_eq!(app.upgrade_slots.available_permits(), 1);
    assert_eq!(app.stats.open_upgrades.load(Ordering::Relaxed), 0);
    proxy.abort();
    server.abort();
}

#[tokio::test]
async fn direct_capacity_preserves_active_turn_and_native_children_after_terminal() {
    for native_children in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (address, app, proxy, server) = direct_capacity_proxy(dir.path(), 1).await;
        let mut first = connect_test_websocket(address).await;
        first.send(Message::Text(serde_json::json!({
            "type":"response.create", "model":if native_children { "complete" } else { "hold" },
            "input":[], "multi_agent":{"enabled":native_children}
        }).to_string().into())).await.unwrap();
        assert_eq!(
            direct_capacity_event(&mut first).await["type"],
            "response.created"
        );
        if native_children {
            assert_eq!(
                direct_capacity_event(&mut first).await["type"],
                "response.completed"
            );
        }
        let mut request = format!("ws://{address}/0123456789abcdef/v1/responses")
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert(AUTHORIZATION, "Bearer caller-token".parse().unwrap());
        let error =
            tokio_tungstenite::client_async(request, TcpStream::connect(address).await.unwrap())
                .await
                .unwrap_err();
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
            panic!("unexpected error: {error}");
        };
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "5");
        assert_eq!(app.stats.open_upgrades.load(Ordering::Relaxed), 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(30), first.next())
                .await
                .is_err()
        );
        app.shutdown_connections().await;
        proxy.abort();
        server.abort();
    }
}

#[tokio::test]
async fn direct_capacity_selects_oldest_idle_and_excludes_busy_sessions() {
    let registry = Arc::new(direct_sessions::DirectSessions::default());
    let mut oldest = registry.register();
    oldest.set_idle(true);
    let mut newer = registry.register();
    newer.set_idle(true);
    assert!(registry.evict_oldest_idle());
    oldest.evicted().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(10), newer.evicted())
            .await
            .is_err()
    );
    newer.set_idle(false);
    assert!(!registry.evict_oldest_idle());
    drop(oldest);
    drop(newer);
}
