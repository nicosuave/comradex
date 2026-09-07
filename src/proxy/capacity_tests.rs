async fn capacity_http_upstream(
    status: StatusCode,
    payload: Bytes,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let payload = payload.clone();
            tokio::spawn(async move {
                let service = service_fn(move |_req: Request<Incoming>| {
                    let payload = payload.clone();
                    async move {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(status)
                                .header(CONTENT_TYPE, "application/json")
                                .header("retry-after", "17")
                                .header("x-upstream-marker", "unchanged")
                                .body(Full::new(payload))
                                .unwrap(),
                        )
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    (address, task)
}

#[tokio::test]
async fn capacity_http_status_and_late_terminals_preserve_body_and_owner_health() {
    let json = Bytes::from_static(br#"{"id":"resp_capacity","status":"failed","error":{"code":"server_is_overloaded","message":"Please try again later"}}"#);
    let sse = Bytes::from_static(b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_capacity\"}}\n\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_capacity\",\"error\":{\"code\":\"model_at_capacity\"}}}\n\n");
    for (status, payload) in [
        (StatusCode::TOO_MANY_REQUESTS, json.clone()),
        (StatusCode::SERVICE_UNAVAILABLE, json.clone()),
        (StatusCode::OK, json),
        (StatusCode::OK, sse),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (upstream, server) = capacity_http_upstream(status, payload.clone()).await;
        let (address, proxy, router) = start_caller_proxy_with_router(
            dir.path(),
            format!("http://{upstream}/backend-api/codex"),
            ResponsesWebsocketMode::Raw,
        )
        .await;
        let owner = router.affinity.key("previous-response:existing_owner");
        router.bind(owner.clone(), "caller").await;
        let client: TestClient<HttpConnector, Full<Bytes>> =
            TestClient::builder(TokioExecutor::new()).build(HttpConnector::new());
        for attempt in 0..3 {
            let response = client
                .request(
                    Request::builder()
                        .method(Method::POST)
                        .uri(format!("http://{address}/0123456789abcdef/v1/responses"))
                        .header(AUTHORIZATION, "Bearer caller-token")
                        .body(Full::new(Bytes::from_static(
                            br#"{"input":[],"previous_response_id":"existing_owner"}"#,
                        )))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), status);
            assert_eq!(response.headers()["retry-after"], "17");
            assert_eq!(response.headers()["x-upstream-marker"], "unchanged");
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                payload
            );
            let snapshot = router.routing_snapshot().await;
            let account = &snapshot.account_states["caller"];
            assert!(account.available, "{status}: {account:?}");
            assert!(account.quota_windows.is_empty());
            assert_eq!(account.capacity_backoff_until_unix.is_some(), attempt == 2);
            assert!(router.affinity.get(&owner).await.is_some());
            assert!(
                router
                    .affinity
                    .get(&router.affinity.key("previous-response:resp_capacity"))
                    .await
                    .is_none()
            );
        }
        proxy.abort();
        server.abort();
    }
}

#[tokio::test]
async fn capacity_bridge_forwards_original_error_without_double_counting() {
    let payload = Bytes::from_static(b"data: {\"type\":\"error\",\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"try again\"}}\n\n");
    let dir = tempfile::tempdir().unwrap();
    let (upstream, server) = capacity_http_upstream(StatusCode::OK, payload).await;
    let (address, proxy, router) = start_caller_proxy_with_router(
        dir.path(),
        format!("http://{upstream}/backend-api/codex"),
        ResponsesWebsocketMode::HttpBridge,
    )
    .await;
    let mut websocket = connect_test_websocket(address).await;
    for attempt in 0..3 {
        websocket
            .send(Message::Text(
                serde_json::json!({"type":"response.create","input":[]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let message = tokio::time::timeout(Duration::from_secs(5), websocket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let value: serde_json::Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
        assert_eq!(
            value,
            serde_json::json!({"type":"error","error":{"code":"server_is_overloaded","message":"try again"}})
        );
        let snapshot = router.routing_snapshot().await;
        let account = &snapshot.account_states["caller"];
        assert!(account.available);
        assert_eq!(account.capacity_backoff_until_unix.is_some(), attempt == 2);
    }
    proxy.abort();
    server.abort();
}

#[tokio::test]
async fn capacity_direct_terminal_keeps_warm_socket_and_account_available() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        for index in 0..3 {
            let message = socket.next().await.unwrap().unwrap();
            assert!(message.is_text());
            for event in [
                serde_json::json!({"type":"response.created","response":{"id":format!("resp_{index}"),"status":"in_progress"}}),
                serde_json::json!({"type":"response.failed","response":{"id":format!("resp_{index}"),"status":"failed","error":{"code":"overloaded_error"}}}),
            ] {
                socket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .unwrap();
            }
        }
        // Keep the warm connection alive until the test closes it.
        while socket.next().await.is_some() {}
    });
    let (address, proxy, router) = start_caller_proxy_with_router(
        dir.path(),
        format!("http://{upstream}/backend-api/codex"),
        ResponsesWebsocketMode::Direct,
    )
    .await;
    let mut websocket = connect_test_websocket(address).await;
    for attempt in 0..3 {
        websocket
            .send(Message::Text(
                serde_json::json!({"type":"response.create","input":[]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        for expected in ["response.created", "response.failed"] {
            let message = tokio::time::timeout(Duration::from_secs(5), websocket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let event: serde_json::Value =
                serde_json::from_str(message.to_text().unwrap()).unwrap();
            assert_eq!(event["type"], expected);
        }
        let snapshot = router.routing_snapshot().await;
        let account = &snapshot.account_states["caller"];
        assert!(account.available);
        assert_eq!(account.capacity_backoff_until_unix.is_some(), attempt == 2);
    }
    proxy.abort();
    server.abort();
}

#[tokio::test]
async fn capacity_upgrade_rejection_preserves_status_and_body_without_alternate() {
    let dir = tempfile::tempdir().unwrap();
    let payload = Bytes::from_static(br#"{"error":{"code":"model_at_capacity"}}"#);
    let (upstream, server) =
        capacity_http_upstream(StatusCode::TOO_MANY_REQUESTS, payload.clone()).await;
    let (address, proxy, router) = start_caller_proxy_with_router(
        dir.path(),
        format!("http://{upstream}/backend-api/codex"),
        ResponsesWebsocketMode::Raw,
    )
    .await;
    for _ in 0..3 {
        let stream = TcpStream::connect(address).await.unwrap();
        let mut request = format!("ws://{address}/0123456789abcdef/v1/responses")
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert(AUTHORIZATION, "Bearer caller-token".parse().unwrap());
        let error = tokio_tungstenite::client_async(request, stream)
            .await
            .unwrap_err();
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
            panic!("unexpected error: {error}");
        };
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()["retry-after"], "17");
        assert_eq!(response.body().as_ref().unwrap(), &payload.to_vec());
        assert!(router.routing_snapshot().await.account_states["caller"].available);
    }
    assert!(
        router.routing_snapshot().await.account_states["caller"]
            .capacity_backoff_until_unix
            .is_some()
    );
    proxy.abort();
    server.abort();
}

#[tokio::test]
async fn capacity_body_inspection_leaves_auth_status_and_oversized_body_untouched() {
    let client: TestClient<HttpConnector, Full<Bytes>> =
        TestClient::builder(TokioExecutor::new()).build(HttpConnector::new());
    for (status, payload) in [
        (
            StatusCode::UNAUTHORIZED,
            Bytes::from_static(br#"{"error":{"code":"model_at_capacity"}}"#),
        ),
        (
            StatusCode::FORBIDDEN,
            Bytes::from_static(br#"{"error":{"code":"model_at_capacity"}}"#),
        ),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Bytes::from(vec![b'x'; FILE_CREATE_RESPONSE_LIMIT + 1024]),
        ),
    ] {
        let (address, server) = capacity_http_upstream(status, payload.clone()).await;
        let response = client
            .request(
                Request::builder()
                    .uri(format!("http://{address}/"))
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let (response, failure) = inspect_rejection_body(response).await.unwrap();
        assert_eq!(failure, None);
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["x-upstream-marker"], "unchanged");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            payload
        );
        server.abort();
    }
}

#[tokio::test]
async fn capacity_inspection_deadline_resumes_body_and_preserves_trailers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let service = service_fn(|_req: Request<Incoming>| async move {
            let stream = futures_util::stream::unfold(0, |index| async move {
                let frame = match index {
                    0 => Frame::data(Bytes::from_static(b"{\"error\":")),
                    1 => {
                        tokio::time::sleep(Duration::from_millis(1500)).await;
                        Frame::data(Bytes::from_static(b"{\"code\":\"model_at_capacity\"}}"))
                    }
                    2 => {
                        let mut trailers = hyper::HeaderMap::new();
                        trailers.insert("x-final-marker", "preserved".parse().unwrap());
                        Frame::trailers(trailers)
                    }
                    _ => return None,
                };
                Some((Ok::<_, std::io::Error>(frame), index + 1))
            });
            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .header("trailer", "x-final-marker")
                    .body(http_body_util::StreamBody::new(stream))
                    .unwrap(),
            )
        });
        let _ = hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(stream), service)
            .await;
    });
    let client: TestClient<HttpConnector, Full<Bytes>> =
        TestClient::builder(TokioExecutor::new()).build(HttpConnector::new());
    let response = client
        .request(
            Request::builder()
                .uri(format!("http://{address}/"))
                .header("te", "trailers")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (response, failure) = inspect_rejection_body(response).await.unwrap();
    assert_eq!(
        failure, None,
        "inspection must end before the delayed terminal arrives"
    );
    let collected = response.into_body().collect().await.unwrap();
    assert_eq!(collected.trailers().unwrap()["x-final-marker"], "preserved");
    assert_eq!(
        collected.to_bytes(),
        Bytes::from_static(br#"{"error":{"code":"model_at_capacity"}}"#)
    );
    server.abort();
}

#[tokio::test]
async fn capacity_bridge_auth_status_never_records_body_capacity() {
    for status in [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
        let dir = tempfile::tempdir().unwrap();
        let (upstream, server) = capacity_http_upstream(
            status,
            Bytes::from_static(br#"{"error":{"code":"model_at_capacity"}}"#),
        )
        .await;
        let (address, proxy, router) = start_caller_proxy_with_router(
            dir.path(),
            format!("http://{upstream}/backend-api/codex"),
            ResponsesWebsocketMode::HttpBridge,
        )
        .await;
        for _ in 0..3 {
            let mut websocket = connect_test_websocket(address).await;
            websocket
                .send(Message::Text(
                    serde_json::json!({"type":"response.create","input":[]})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let message = tokio::time::timeout(Duration::from_secs(5), websocket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let value: serde_json::Value =
                serde_json::from_str(message.to_text().unwrap()).unwrap();
            assert_eq!(value["status"], status.as_u16());
            assert_eq!(value["error"]["code"], "model_at_capacity");
            assert!(
                router.routing_snapshot().await.account_states["caller"]
                    .capacity_backoff_until_unix
                    .is_none()
            );
        }
        proxy.abort();
        server.abort();
    }
}
