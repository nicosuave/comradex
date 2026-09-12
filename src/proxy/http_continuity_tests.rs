use super::*;
use crate::config::{AccountConfig, ProxyConfig};
use bytes::Bytes;
use http_body_util::{Full, StreamBody};
use std::{collections::BTreeMap, fs, path::Path};

const SECRET: &str = "0123456789abcdef";
const TURN_STATE: &str = "synthetic-returned-turn-state";

struct ContinuityFixture {
    address: std::net::SocketAddr,
    app: Arc<App>,
    seen: Arc<StdMutex<Vec<String>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for ContinuityFixture {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn fixture(dir: &Path, first_event: Bytes, keep_open: bool) -> ContinuityFixture {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let upstream_seen = seen.clone();
    let upstream_task = tokio::spawn(async move {
        loop {
            let (stream, _) = upstream.accept().await.unwrap();
            let seen = upstream_seen.clone();
            let first_event = first_event.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req: Request<Incoming>| {
                    let seen = seen.clone();
                    let first_event = first_event.clone();
                    async move {
                        let authorization =
                            req.headers()[AUTHORIZATION].to_str().unwrap().to_owned();
                        req.into_body().collect().await.unwrap();
                        let first = {
                            let mut seen = seen.lock().unwrap();
                            let first = seen.is_empty();
                            seen.push(authorization);
                            first
                        };
                        let bytes = if first {
                            first_event
                        } else {
                            Bytes::from_static(b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_followup\",\"status\":\"completed\"}}\n\n")
                        };
                        // Pending after one data frame deliberately prevents HTTP EOF,
                        // matching clients that close once they see an SSE terminal.
                        let frames = futures_util::stream::unfold(
                            (Some(bytes), first && keep_open),
                            |(next, hold)| async move {
                                if let Some(bytes) = next {
                                    Some((Ok::<_, Infallible>(Frame::data(bytes)), (None, hold)))
                                } else if hold {
                                    std::future::pending().await
                                } else {
                                    None
                                }
                            },
                        );
                        Ok::<_, Infallible>(
                            Response::builder()
                                .header(CONTENT_TYPE, "text/event-stream")
                                .header("x-codex-turn-state", TURN_STATE)
                                .body(StreamBody::new(frames))
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
            upstream: format!("http://{upstream_address}/backend-api/codex"),
            installation_secret: SECRET.into(),
            affinity_key: "0123456789abcdef0123456789abcdef".into(),
            state_dir: Some(dir.join("state")),
            ..ProxyConfig::default()
        },
        listeners: BTreeMap::from([("default".into(), listener.clone())]),
        pools: BTreeMap::from([(
            "default".into(),
            PoolConfig {
                members: vec!["a".into(), "b".into()],
                preferred: Some("a".into()),
                preserved: None,
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
    let proxy_task = tokio::spawn(async move {
        serving_app
            .serve_tcp("default".into(), listener, proxy)
            .await
            .unwrap();
    });
    ContinuityFixture {
        address,
        app,
        seen,
        tasks: vec![proxy_task, upstream_task],
    }
}

async fn post(fixture: &ContinuityFixture, token: Option<&str>) -> Response<Incoming> {
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build(HttpConnector::new());
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{}/{SECRET}/v1/responses", fixture.address))
        .header(
            "thread-id",
            if token.is_some() {
                "followup-thread"
            } else {
                "first-thread"
            },
        )
        .header("session-id", "first-session")
        .header(CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        request = request.header("x-codex-turn-state", token);
    }
    tokio::time::timeout(
        Duration::from_secs(5),
        client.request(
            request
                .body(Full::new(Bytes::from_static(
                    br#"{"input":"hello","stream":true}"#,
                )))
                .unwrap(),
        ),
    )
    .await
    .expect("response headers timed out")
    .unwrap()
}

async fn assert_owner(fixture: &ContinuityFixture) {
    let key = fixture
        .app
        .router
        .affinity
        .key(&format!("turn-state:{TURN_STATE}"));
    let binding = fixture
        .app
        .router
        .affinity
        .get(&key)
        .await
        .expect("returned turn-state must already have an owner");
    assert_eq!(binding.account_id, "a");
}

async fn assert_no_success_affinity(fixture: &ContinuityFixture) {
    // Router selection already binds the primary thread key before dispatch.
    // Secondary session aliases and returned response IDs remain success-gated.
    for identity in ["session:first-session", "previous-response:resp_first"] {
        let key = fixture.app.router.affinity.key(identity);
        assert!(
            fixture.app.router.affinity.get(&key).await.is_none(),
            "unexpected success affinity for {identity}"
        );
    }
}

#[tokio::test]
async fn returned_turn_state_routes_continuation_after_client_closes_completed_sse_without_eof() {
    let dir = tempfile::tempdir().unwrap();
    let event = Bytes::from_static(b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_first\",\"status\":\"completed\"}}\n\n");
    let fixture = fixture(dir.path(), event.clone(), true).await;
    let mut response = post(&fixture, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-codex-turn-state"], TURN_STATE);
    assert_owner(&fixture).await;
    let frame = tokio::time::timeout(Duration::from_secs(5), response.body_mut().frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(frame.into_data().unwrap(), event);
    assert!(!response.body().is_end_stream());
    drop(response);
    fixture
        .app
        .router
        .set_preferred("default", Some("b".into()))
        .await;
    let followup = post(&fixture, Some(TURN_STATE)).await;
    assert_eq!(followup.status(), StatusCode::OK);
    tokio::time::timeout(Duration::from_secs(5), followup.into_body().collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        *fixture.seen.lock().unwrap(),
        ["Bearer token-a", "Bearer token-a"]
    );
    assert_owner(&fixture).await;
}

#[tokio::test]
async fn returned_turn_state_survives_aborted_and_truncated_response_without_success_affinity() {
    for keep_open in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let event = Bytes::from_static(b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_first\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"unfinished");
        let fixture = fixture(dir.path(), event, keep_open).await;
        let mut response = post(&fixture, None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_owner(&fixture).await;
        if keep_open {
            tokio::time::timeout(Duration::from_secs(5), response.body_mut().frame())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            drop(response);
        } else {
            tokio::time::timeout(Duration::from_secs(5), response.into_body().collect())
                .await
                .unwrap()
                .unwrap();
        }
        assert_owner(&fixture).await;
        assert_no_success_affinity(&fixture).await;
    }
}

#[tokio::test]
async fn returned_turn_state_survives_quota_and_capacity_terminal_without_success_affinity() {
    for code in ["usage_limit_reached", "server_is_overloaded"] {
        let dir = tempfile::tempdir().unwrap();
        let event = format!(
            "data: {{\"type\":\"response.failed\",\"response\":{{\"id\":\"resp_first\",\"status\":\"failed\",\"error\":{{\"code\":\"{code}\"}}}}}}\n\n"
        );
        let fixture = fixture(dir.path(), event.into(), false).await;
        let response = post(&fixture, None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_owner(&fixture).await;
        tokio::time::timeout(Duration::from_secs(5), response.into_body().collect())
            .await
            .unwrap()
            .unwrap();
        assert_owner(&fixture).await;
        assert_no_success_affinity(&fixture).await;
        assert_eq!(*fixture.seen.lock().unwrap(), ["Bearer token-a"]);
    }
}
