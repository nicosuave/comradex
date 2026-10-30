#[tokio::test]
async fn session_move_and_reset_apply_on_a_reused_codex_direct_socket() {
    use crate::routing::sessions::{SessionAction, manage_session};
    let dir = tempfile::tempdir().unwrap();
    let (address, app, proxy, server) = direct_capacity_proxy(dir.path(), 8).await;
    let mut socket = connect_test_websocket(address).await;
    for (action, expected) in [
        (
            SessionAction::Move {
                account: "a".into(),
            },
            "a",
        ),
        (
            SessionAction::Move {
                account: "b".into(),
            },
            "b",
        ),
        (SessionAction::Reset, "a"),
    ] {
        app.router.set_preferred("default", Some("a".into())).await;
        manage_session(
            &app.config,
            &app.router,
            "direct-session",
            Some("default"),
            action,
        )
        .await
        .unwrap();
        socket
            .send(Message::Text(
                serde_json::json!({
                    "type":"response.create", "model":"complete", "input":[],
                    "client_metadata":{"session_id":"direct-session"}
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        assert_eq!(
            direct_capacity_event(&mut socket).await["type"],
            "response.created"
        );
        assert_eq!(
            direct_capacity_event(&mut socket).await["type"],
            "response.completed"
        );
        let status = manage_session(
            &app.config,
            &app.router,
            "direct-session",
            None,
            SessionAction::Show,
        )
        .await
        .unwrap();
        assert_eq!(status.account.as_deref(), Some(expected));
        assert_eq!(
            app.router.wired_account("default").await.as_deref(),
            Some(expected)
        );
    }
    app.shutdown_connections().await;
    proxy.abort();
    server.abort();
}

#[tokio::test]
async fn session_direct_ownership_and_thread_scope_remain_enforced() {
    use crate::routing::sessions::{SessionAction, manage_session};
    let dir = tempfile::tempdir().unwrap();
    let (app, listener, router, stats) = direct_test_app(dir.path());
    router
        .bind(router.affinity.key("previous-response:owned-a"), "a")
        .await;
    manage_session(
        &app.config,
        &router,
        "child",
        Some("default"),
        SessionAction::Move {
            account: "b".into(),
        },
    )
    .await
    .unwrap();
    let replay = |body: &'static [u8]| {
        ReplayBody::from_bytes(
            Bytes::from_static(body),
            app.config.proxy.max_request_bytes,
            app.config.proxy.max_spool_bytes,
            stats.clone(),
        )
        .unwrap()
    };
    let owned = replay(
        br#"{"client_metadata":{"thread_id":"child"},"previous_response_id":"owned-a","input":[]}"#,
    );
    let error = app
        .route_websocket_frame(&listener, &hyper::HeaderMap::new(), &owned, None)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("requires a"));
    let portable = replay(br#"{"client_metadata":{"thread_id":"child"},"input":[]}"#);
    let route = app
        .route_websocket_frame(&listener, &hyper::HeaderMap::new(), &portable, Some("a"))
        .await
        .unwrap();
    assert_eq!(route.account_id, "b");
    assert!(
        route.non_previous_hard_owner,
        "manual assignment forbids replay on another account"
    );
    let sibling = replay(br#"{"client_metadata":{"thread_id":"sibling"},"input":[]}"#);
    router.set_preferred("default", Some("a".into())).await;
    let route = app
        .route_websocket_frame(&listener, &hyper::HeaderMap::new(), &sibling, None)
        .await
        .unwrap();
    assert_eq!(route.account_id, "a");
}

#[tokio::test]
async fn session_direct_manual_move_allows_unanchored_encrypted_history() {
    use crate::routing::sessions::{SessionAction, manage_session};
    for legacy in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (address, app, proxy, server) = direct_capacity_proxy(dir.path(), 8).await;
        let id = "opaque-session";
        if legacy {
            app.router
                .bind(app.router.affinity.key(&format!("thread:{id}")), "a")
                .await;
        } else {
            let mut first = connect_test_websocket_with_headers(
                address,
                &[("authorization", "Bearer caller-token"), ("thread-id", id)],
            )
            .await;
            first
                .send(Message::Text(
                    r#"{"type":"response.create","input":[]}"#.into(),
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
            first.close(None).await.unwrap();
        }
        manage_session(
            &app.config,
            &app.router,
            id,
            Some("default"),
            SessionAction::Move {
                account: "b".into(),
            },
        )
        .await
        .unwrap();
        let mut socket = connect_test_websocket_with_headers(
            address,
            &[("authorization", "Bearer caller-token"), ("thread-id", id)],
        )
        .await;
        socket.send(Message::Text(r#"{"type":"response.create","input":[{"type":"reasoning","encrypted_content":"from-a"}]}"#.into())).await.unwrap();
        assert_eq!(
            direct_capacity_event(&mut socket).await["type"],
            "response.created"
        );
        assert_eq!(
            direct_capacity_event(&mut socket).await["type"],
            "response.completed"
        );
        assert_eq!(
            app.router.wired_account("default").await.as_deref(),
            Some("b")
        );
        // A helper on B does not change subsequent manual routing decisions.
        socket
            .send(Message::Text(
                r#"{"type":"response.create","input":[]}"#.into(),
            ))
            .await
            .unwrap();
        assert_eq!(
            direct_capacity_event(&mut socket).await["type"],
            "response.created"
        );
        assert_eq!(
            direct_capacity_event(&mut socket).await["type"],
            "response.completed"
        );
        socket.send(Message::Text(r#"{"type":"response.create","input":[{"type":"reasoning","encrypted_content":"from-a"}]}"#.into())).await.unwrap();
        assert_eq!(
            direct_capacity_event(&mut socket).await["type"],
            "response.created"
        );
        assert_eq!(
            direct_capacity_event(&mut socket).await["type"],
            "response.completed"
        );
        app.shutdown_connections().await;
        proxy.abort();
        server.abort();
    }
}

#[tokio::test]
async fn session_raw_websocket_migration_requires_a_transport_that_can_inspect_frames() {
    use crate::routing::sessions::{SessionAction, manage_session};
    let dir = tempfile::tempdir().unwrap();
    let (original, mut listener, _, _) = direct_test_app(dir.path());
    let mut config = (*original.config).clone();
    config.proxy.responses_websocket_mode = ResponsesWebsocketMode::Raw;
    let affinity = original.router.affinity.clone();
    drop(original);
    let config = Arc::new(config);
    let router = Arc::new(Router::new(&config, affinity));
    let app = App::new_unvalidated(config, router.clone(), Arc::new(Stats::default())).unwrap();
    router
        .bind(router.affinity.key("thread:raw-session"), "a")
        .await;
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    listener.address = address;
    let proxy = tokio::spawn(app.clone().serve_tcp("default".into(), listener, tcp));
    for action in [
        SessionAction::Move {
            account: "b".into(),
        },
        SessionAction::Reset,
    ] {
        manage_session(&app.config, &router, "raw-session", Some("default"), action)
            .await
            .unwrap();
        let request = format!("ws://{address}/0123456789abcdef/v1/responses")
            .into_client_request()
            .unwrap();
        let mut request = request;
        request
            .headers_mut()
            .insert("thread-id", "raw-session".parse().unwrap());
        let error = tokio::time::timeout(
            Duration::from_secs(3),
            tokio_tungstenite::client_async(request, TcpStream::connect(address).await.unwrap()),
        )
        .await
        .unwrap()
        .unwrap_err();
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
            panic!("{error}")
        };
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(
            String::from_utf8_lossy(response.body().as_ref().unwrap())
                .contains("session_transport_unsupported")
        );
    }
    app.shutdown_connections().await;
    proxy.abort();
}
