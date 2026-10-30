use super::*;
use crate::config::{AccountConfig, ProxyConfig};
use bytes::Bytes;
use http_body_util::Full;
use std::{collections::BTreeMap, fs, path::Path};

const SECRET: &str = "0123456789abcdef";

include!("session_tests.rs");

struct PinFixture {
    address: std::net::SocketAddr,
    app: Arc<App>,
    listener: ListenerConfig,
    seen: Arc<StdMutex<Vec<(String, String)>>>,
    bodies: Arc<StdMutex<Vec<Bytes>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for PinFixture {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn fixture(dir: &Path, failure: Option<StatusCode>) -> PinFixture {
    fixture_with_pins(
        dir,
        failure,
        BTreeMap::from([
            ("pinned-model".into(), "b".into()),
            ("gpt-daybreak-blue-latest".into(), "b".into()),
            ("gpt-daybreak-red-latest".into(), "b".into()),
        ]),
    )
    .await
}

async fn fixture_with_pins(
    dir: &Path,
    failure: Option<StatusCode>,
    model_accounts: BTreeMap<String, String>,
) -> PinFixture {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let upstream_seen = seen.clone();
    let bodies = Arc::new(StdMutex::new(Vec::new()));
    let upstream_bodies = bodies.clone();
    let upstream_task = tokio::spawn(async move {
        loop {
            let (stream, _) = upstream.accept().await.unwrap();
            let seen = upstream_seen.clone();
            let bodies = upstream_bodies.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req: Request<Incoming>| {
                    let seen = seen.clone();
                    let bodies = bodies.clone();
                    async move {
                        let authorization =
                            req.headers()[AUTHORIZATION].to_str().unwrap().to_owned();
                        let path = req.uri().to_string();
                        let is_head = req.method() == Method::HEAD;
                        let is_upload = req.uri().path() == "/backend-api/codex/files";
                        let bytes = req.into_body().collect().await.unwrap().to_bytes();
                        bodies.lock().unwrap().push(bytes.clone());
                        let streaming = serde_json::from_slice::<serde_json::Value>(&bytes)
                            .ok()
                            .is_some_and(|body| body["stream"] == true);
                        seen.lock().unwrap().push((authorization.clone(), path));
                        let status = if authorization == "Bearer token-b" {
                            failure.unwrap_or(StatusCode::OK)
                        } else {
                            StatusCode::OK
                        };
                        let body = if is_head {
                            ""
                        } else if status == StatusCode::TOO_MANY_REQUESTS {
                            r#"{"error":{"code":"usage_limit_reached","message":"quota exhausted"}}"#
                        } else if status == StatusCode::SERVICE_UNAVAILABLE {
                            r#"{"error":{"code":"server_is_overloaded","message":"capacity exhausted"}}"#
                        } else if streaming {
                            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_pin\",\"status\":\"completed\",\"output\":[]}}\n\n"
                        } else if is_upload {
                            r#"{"file_id":"file_pin_fixture"}"#
                        } else {
                            r#"{"id":"resp_pin","status":"completed","output":[],"data":[]}"#
                        };
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(status)
                                .header(
                                    CONTENT_TYPE,
                                    if streaming && status.is_success() {
                                        "text/event-stream"
                                    } else {
                                        "application/json"
                                    },
                                )
                                .body(Full::new(Bytes::from_static(body.as_bytes())))
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
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = proxy.local_addr().unwrap();
    let listener = ListenerConfig {
        address,
        pool: "default".into(),
    };
    let mut accounts = BTreeMap::new();
    for alias in ["a", "b"] {
        let home = dir.join(alias);
        fs::create_dir_all(&home).unwrap();
        fs::write(
            home.join("auth.json"),
            format!(r#"{{"tokens":{{"access_token":"token-{alias}"}}}}"#),
        )
        .unwrap();
        accounts.insert(alias.into(), AccountConfig::CodexHome { path: home });
    }
    let config = Arc::new(Config {
        proxy: ProxyConfig {
            responses_websocket_mode: ResponsesWebsocketMode::HttpBridge,
            upstream: format!("http://{upstream_address}/backend-api/codex"),
            installation_secret: SECRET.into(),
            affinity_key: "0123456789abcdef0123456789abcdef".into(),
            state_dir: Some(dir.join("state")),
            ..Default::default()
        },
        listeners: BTreeMap::from([("default".into(), listener.clone())]),
        pools: BTreeMap::from([(
            "default".into(),
            PoolConfig {
                members: vec!["a".into(), "b".into()],
                preferred: Some("a".into()),
                model_accounts,
                models_account: Some("b".into()),
                ..Default::default()
            },
        )]),
        accounts,
    });
    let affinity = Arc::new(
        AffinityStore::load(
            dir.join("affinity.json"),
            &config.proxy.affinity_key,
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let router = Arc::new(Router::new(&config, affinity));
    let app = App::new_unvalidated(config, router, Arc::new(Stats::default())).unwrap();
    let serving_app = app.clone();
    let serving_listener = listener.clone();
    let proxy_task = tokio::spawn(async move {
        serving_app
            .serve_tcp("default".into(), serving_listener, proxy)
            .await
            .unwrap();
    });
    PinFixture {
        address,
        app,
        listener,
        seen,
        bodies,
        tasks: vec![proxy_task, upstream_task],
    }
}

async fn request(
    fixture: &PinFixture,
    method: Method,
    path: &str,
    body: &str,
    turn_state: Option<&str>,
) -> StatusCode {
    request_with_content_type(fixture, method, path, body, turn_state, "application/json").await
}

async fn request_with_content_type(
    fixture: &PinFixture,
    method: Method,
    path: &str,
    body: &str,
    turn_state: Option<&str>,
    content_type: &str,
) -> StatusCode {
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build(HttpConnector::new());
    let mut request = Request::builder()
        .method(method)
        .uri(format!("http://{}/{SECRET}{path}", fixture.address))
        .header(CONTENT_TYPE, content_type)
        .header("thread-id", "soft-thread");
    if let Some(turn_state) = turn_state {
        request = request.header("x-codex-turn-state", turn_state);
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        let response = client
            .request(
                request
                    .body(Full::new(Bytes::copy_from_slice(body.as_bytes())))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        response.into_body().collect().await.unwrap();
        status
    })
    .await
    .expect("pinned request timed out")
}

#[tokio::test]
async fn http_model_pin_overrides_preferred_and_soft_affinity_without_changing_other_models() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = fixture(dir.path(), None).await;
    let key = fixture.app.router.affinity.key("thread:soft-thread");
    assert!(fixture.app.router.bind(key, "a").await);
    for model in ["pinned-model-other", "unlisted", "pinned-model"] {
        assert_eq!(
            request(
                &fixture,
                Method::POST,
                "/v1/responses",
                &format!(r#"{{"model":"{model}","input":[]}}"#),
                None
            )
            .await,
            StatusCode::OK
        );
    }
    let seen = fixture.seen.lock().unwrap();
    assert_eq!(
        seen.iter()
            .map(|(auth, _)| auth.as_str())
            .collect::<Vec<_>>(),
        ["Bearer token-a", "Bearer token-a", "Bearer token-b"]
    );
}

#[tokio::test]
async fn http_access_program_pin_overrides_soft_affinity_and_keeps_standard_routing() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = fixture(dir.path(), None).await;
    let key = fixture.app.router.affinity.key("thread:soft-thread");
    assert!(fixture.app.router.bind(key, "a").await);
    for program in ["standard", "daybreak_blue", "daybreak_red"] {
        assert_eq!(
            request(
                &fixture,
                Method::POST,
                "/v1/responses",
                &format!(
                    r#"{{"model":"gpt-6-sol","access_programs":{{"cyber":"{program}"}},"input":[]}}"#
                ),
                None,
            )
            .await,
            StatusCode::OK
        );
    }
    let seen = fixture.seen.lock().unwrap();
    assert_eq!(
        seen.iter()
            .map(|(auth, _)| auth.as_str())
            .collect::<Vec<_>>(),
        ["Bearer token-a", "Bearer token-b", "Bearer token-b"]
    );
}

#[tokio::test]
async fn access_program_pin_takes_precedence_but_an_unpinned_program_keeps_the_base_model_pin() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = fixture_with_pins(
        dir.path(),
        None,
        BTreeMap::from([
            ("gpt-6-sol".into(), "b".into()),
            ("gpt-daybreak-blue-latest".into(), "a".into()),
        ]),
    )
    .await;
    for (program, account) in [("daybreak_blue", "a"), ("daybreak_red", "b")] {
        let body = format!(
            r#"{{"model":"gpt-6-sol","access_programs":{{"cyber":"{program}"}},"input":[]}}"#
        );
        assert_eq!(
            request(&fixture, Method::POST, "/v1/responses", &body, None).await,
            StatusCode::OK
        );
        assert_eq!(
            fixture.seen.lock().unwrap().last().unwrap().0,
            format!("Bearer token-{account}")
        );
        let replay =
            ReplayBody::from_bytes(Bytes::from(body), 4096, 4096, fixture.app.stats.clone())
                .unwrap();
        let route = fixture
            .app
            .route_websocket_frame(&fixture.listener, &hyper::HeaderMap::new(), &replay, None)
            .await
            .unwrap();
        assert_eq!(route.account_id, account);
    }
}

#[tokio::test]
async fn listing_pin_applies_to_get_and_head_with_query_parameters() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = fixture(dir.path(), None).await;
    for method in [Method::GET, Method::HEAD] {
        assert_eq!(
            request(
                &fixture,
                method,
                "/v1/models?client_version=1.2.3",
                "",
                None
            )
            .await,
            StatusCode::OK
        );
    }
    let seen = fixture.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    for (authorization, path) in seen.iter() {
        assert_eq!(authorization, "Bearer token-b");
        assert!(path.ends_with("/models?client_version=1.2.3"), "{path}");
    }
}

#[tokio::test]
async fn http_pin_rejects_conflicting_owner_and_unavailable_account_before_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = fixture(dir.path(), None).await;
    let key = fixture.app.router.affinity.key("turn-state:owned-by-a");
    assert!(fixture.app.router.bind(key, "a").await);
    let bodies = [
        r#"{"model":"pinned-model","input":[]}"#,
        r#"{"model":"gpt-6-sol","access_programs":{"cyber":"daybreak_blue"},"input":[]}"#,
    ];
    for body in bodies {
        assert_eq!(
            request(
                &fixture,
                Method::POST,
                "/v1/responses",
                body,
                Some("owned-by-a")
            )
            .await,
            StatusCode::CONFLICT
        );
    }
    fixture.app.router.auth_failure("b").await;
    for body in bodies {
        assert_eq!(
            request(&fixture, Method::POST, "/v1/responses", body, None).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    assert_eq!(
        request(&fixture, Method::GET, "/v1/models", "", None).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(fixture.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn http_pin_does_not_fall_back_after_quota_or_capacity_failure() {
    for status in [
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        for body in [
            r#"{"model":"pinned-model","input":[]}"#,
            r#"{"model":"gpt-6-sol","access_programs":{"cyber":"daybreak_blue"},"input":[]}"#,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let fixture = fixture(dir.path(), Some(status)).await;
            assert_eq!(
                request(&fixture, Method::POST, "/v1/responses", body, None).await,
                status
            );
            let seen = fixture.seen.lock().unwrap();
            assert_eq!(seen.len(), 1, "pin must suppress alternate-account retry");
            assert_eq!(seen[0].0, "Bearer token-b");
        }
    }
}

#[tokio::test]
async fn direct_frame_pin_overrides_socket_and_soft_preference_and_is_a_hard_owner() {
    for body in [
        br#"{"type":"response.create","model":"pinned-model","input":[]}"#.as_slice(),
        br#"{"type":"response.create","model":"gpt-6-sol","access_programs":{"cyber":"daybreak_blue"},"input":[]}"#.as_slice(),
    ] {
    let dir = tempfile::tempdir().unwrap();
    let fixture = fixture(dir.path(), None).await;
    let key = fixture.app.router.affinity.key("thread:soft-thread");
    assert!(fixture.app.router.bind(key, "a").await);
    let mut headers = hyper::HeaderMap::new();
    headers.insert("thread-id", "soft-thread".parse().unwrap());
    let replay = ReplayBody::from_bytes(
        Bytes::copy_from_slice(body),
        fixture.app.config.proxy.max_request_bytes,
        fixture.app.config.proxy.max_spool_bytes,
        fixture.app.stats.clone(),
    )
    .unwrap();
    for preferred in [None, Some("a"), Some("b")] {
        let route = fixture
            .app
            .route_websocket_frame(&fixture.listener, &headers, &replay, preferred)
            .await
            .unwrap();
        assert_eq!(route.account_id, "b");
        assert!(route.hard_owner);
        assert!(route.non_previous_hard_owner);
    }
    let key = fixture.app.router.affinity.key("turn-state:owned-by-a");
    assert!(fixture.app.router.bind(key, "a").await);
    headers.insert("x-codex-turn-state", "owned-by-a".parse().unwrap());
    let error = fixture
        .app
        .route_websocket_frame(&fixture.listener, &headers, &replay, None)
        .await
        .err()
        .expect("conflicting hard owner must fail");
    assert!(error.to_string().contains("conflict"), "{error:#}");
    headers.remove("x-codex-turn-state");
    fixture.app.router.auth_failure("b").await;
    let error = fixture
        .app
        .route_websocket_frame(&fixture.listener, &headers, &replay, Some("a"))
        .await
        .err()
        .expect("unavailable pinned account must fail");
    assert!(error.to_string().contains("unavailable"), "{error:#}");
    }
}

#[tokio::test]
async fn http_bridge_websocket_honors_model_pin() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    for (body, account) in [
        (
            r#"{"type":"response.create","model":"pinned-model","input":[]}"#,
            "b",
        ),
        (
            r#"{"type":"response.create","model":"gpt-6-sol","access_programs":{"cyber":"daybreak_blue"},"input":[]}"#,
            "b",
        ),
        (
            r#"{"type":"response.create","model":"gpt-6-sol","access_programs":{"cyber":"daybreak_red"},"input":[]}"#,
            "b",
        ),
        (
            r#"{"type":"response.create","model":"gpt-6-sol","access_programs":{"cyber":"standard"},"input":[]}"#,
            "a",
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let fixture = fixture(dir.path(), None).await;
        let stream = tokio::net::TcpStream::connect(fixture.address)
            .await
            .unwrap();
        let request = format!(
            "ws://{}/{SECRET}/backend-api/codex/responses",
            fixture.address
        )
        .into_client_request()
        .unwrap();
        let (mut websocket, _) = tokio_tungstenite::client_async(request, stream)
            .await
            .unwrap();
        websocket.send(Message::Text(body.into())).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let message = websocket.next().await.unwrap().unwrap();
                let Message::Text(text) = message else {
                    continue;
                };
                let event: serde_json::Value = serde_json::from_str(text.as_str()).unwrap();
                assert_ne!(event["type"], "error", "{event}");
                assert_ne!(event["type"], "response.failed", "{event}");
                if event["type"] == "response.completed" {
                    break;
                }
            }
        })
        .await
        .expect("bridge did not complete pinned response");
        let seen = fixture.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, format!("Bearer token-{account}"));
    }
}

#[tokio::test]
async fn incomplete_model_scan_rejects_http_and_direct_before_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = fixture(dir.path(), None).await;
    let nested = format!("{}null{}", "[".repeat(129), "]".repeat(129));
    let body = format!(r#"{{"input":{nested},"model":"pinned-model"}}"#);
    assert_eq!(
        request(&fixture, Method::POST, "/v1/responses", &body, None).await,
        StatusCode::BAD_REQUEST
    );
    let replay = ReplayBody::from_bytes(
        Bytes::from(body),
        fixture.app.config.proxy.max_request_bytes,
        fixture.app.config.proxy.max_spool_bytes,
        fixture.app.stats.clone(),
    )
    .unwrap();
    assert!(!replay.model_scan_complete());
    let error = fixture
        .app
        .route_websocket_frame(
            &fixture.listener,
            &hyper::HeaderMap::new(),
            &replay,
            Some("a"),
        )
        .await
        .err()
        .expect("incomplete model scan must fail closed");
    assert!(error.to_string().contains("model"), "{error:#}");
    assert!(fixture.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn uploads_ignore_embedded_model_metadata_but_responses_pin_with_non_json_content_type() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = fixture(dir.path(), None).await;
    let multipart = "--upload\r\nContent-Disposition: form-data; name=\"file\"; filename=\"input.txt\"\r\nContent-Type: text/plain\r\n\r\n{\"model\":\"pinned-model\"}\nunterminated \"\r\n--upload--\r\n";
    for (content_type, body) in [
        ("multipart/form-data; boundary=upload", multipart),
        ("application/octet-stream", r#"{"model":"pinned-model"}"#),
        ("text/plain", r#"{"model":"pinned-model","unterminated":""#),
    ] {
        assert_eq!(
            request_with_content_type(
                &fixture,
                Method::POST,
                "/v1/files",
                body,
                None,
                content_type
            )
            .await,
            StatusCode::OK
        );
    }
    assert_eq!(
        request_with_content_type(
            &fixture,
            Method::POST,
            "/v1/responses",
            r#"{"model":"pinned-model","input":[]}"#,
            None,
            "text/plain"
        )
        .await,
        StatusCode::OK
    );
    let seen = fixture.seen.lock().unwrap();
    assert_eq!(
        seen.iter()
            .map(|(authorization, _)| authorization.as_str())
            .collect::<Vec<_>>(),
        [
            "Bearer token-a",
            "Bearer token-a",
            "Bearer token-a",
            "Bearer token-b"
        ]
    );
}
