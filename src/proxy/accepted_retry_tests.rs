use super::*;
use crate::config::{AccountConfig, ProxyConfig};
use bytes::Bytes;
use serde_json::{Value, json};
use std::{collections::BTreeMap, fs, sync::Mutex};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

#[derive(Clone)]
struct Script {
    status: StatusCode,
    events: Vec<Value>,
    close: bool,
    resume_after_created: Option<Arc<Notify>>,
}

impl Script {
    fn events(events: Vec<Value>) -> Self {
        Self {
            status: StatusCode::OK,
            events,
            close: false,
            resume_after_created: None,
        }
    }
}

#[derive(Debug)]
struct Dispatch {
    authorization: String,
    body: Value,
}

struct Fixture {
    address: std::net::SocketAddr,
    seen: Arc<Mutex<Vec<Dispatch>>>,
    router: Arc<Router>,
    file_owners: Arc<AffinityStore>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    _dir: tempfile::TempDir,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

fn take_script(
    seen: &Mutex<Vec<Dispatch>>,
    scripts: &[Script],
    authorization: &str,
    body: Value,
) -> Script {
    let mut seen = seen.lock().unwrap();
    let index = seen.len();
    seen.push(Dispatch {
        authorization: authorization.to_owned(),
        body,
    });
    // An unexpected third dispatch completes promptly so the assertion, rather than a hang,
    // identifies a retry-budget regression.
    scripts
        .get(index)
        .cloned()
        .unwrap_or_else(|| success("resp_unexpected"))
}

impl Fixture {
    async fn start(mode: ResponsesWebsocketMode, scripts: Vec<Script>) -> Self {
        Self::start_with_members(mode, scripts, &["a", "b", "c"]).await
    }

    async fn start_with_members(
        mode: ResponsesWebsocketMode,
        scripts: Vec<Script>,
        members: &[&str],
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_address = upstream.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let upstream_seen = seen.clone();
        let scripts = Arc::new(scripts);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = upstream.accept().await.unwrap();
                let seen = upstream_seen.clone();
                let scripts = scripts.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |mut req: Request<Incoming>| {
                        let seen = seen.clone();
                        let scripts = scripts.clone();
                        async move {
                            let authorization =
                                req.headers()[AUTHORIZATION].to_str().unwrap().to_owned();
                            if req.headers().contains_key(SEC_WEBSOCKET_KEY) {
                                let refusal = {
                                    let dispatches = seen.lock().unwrap();
                                    scripts
                                        .get(dispatches.len())
                                        .filter(|script| script.status != StatusCode::OK)
                                        .cloned()
                                };
                                if let Some(script) = refusal {
                                    take_script(&seen, &scripts, &authorization, Value::Null);
                                    return Ok::<_, Infallible>(
                                        Response::builder()
                                            .status(script.status)
                                            .header(CONTENT_TYPE, "application/json")
                                            .body(bytes_body(Bytes::from_static(
                                                br#"{"error":{"code":"server_is_overloaded"}}"#,
                                            )))
                                            .unwrap(),
                                    );
                                }
                                let accept =
                                    tokio_tungstenite::tungstenite::handshake::derive_accept_key(
                                        req.headers()[SEC_WEBSOCKET_KEY].as_bytes(),
                                    );
                                let upgrade = hyper::upgrade::on(&mut req);
                                tokio::spawn(async move {
                                    let Ok(upgraded) = upgrade.await else { return };
                                    let mut ws = WebSocketStream::from_raw_socket(
                                        TokioIo::new(upgraded),
                                        Role::Server,
                                        None,
                                    )
                                    .await;
                                    while let Some(Ok(Message::Text(text))) = ws.next().await {
                                        let body = serde_json::from_str(&text).unwrap();
                                        let script =
                                            take_script(&seen, &scripts, &authorization, body);
                                        for (index, event) in script.events.into_iter().enumerate()
                                        {
                                            if index == 1
                                                && let Some(gate) = &script.resume_after_created
                                            {
                                                gate.notified().await;
                                            }
                                            if ws
                                                .send(Message::Text(event.to_string().into()))
                                                .await
                                                .is_err()
                                            {
                                                return;
                                            }
                                        }
                                        if script.close {
                                            let _ = ws.close(None).await;
                                            return;
                                        }
                                    }
                                });
                                return Ok::<_, Infallible>(
                                    Response::builder()
                                        .status(StatusCode::SWITCHING_PROTOCOLS)
                                        .header(CONNECTION, "Upgrade")
                                        .header(UPGRADE, "websocket")
                                        .header(SEC_WEBSOCKET_ACCEPT, accept)
                                        .body(bytes_body(Bytes::new()))
                                        .unwrap(),
                                );
                            }
                            let bytes = req.into_body().collect().await.unwrap().to_bytes();
                            let body = serde_json::from_slice(&bytes).unwrap();
                            let script = take_script(&seen, &scripts, &authorization, body);
                            let body = if script.status == StatusCode::OK {
                                let gate = script.resume_after_created;
                                let stream = futures_util::stream::unfold(
                                    (script.events.into_iter().enumerate(), gate),
                                    |(mut events, gate)| async move {
                                        let (index, event) = events.next()?;
                                        if index == 1
                                            && let Some(gate) = &gate
                                        {
                                            gate.notified().await;
                                        }
                                        Some((
                                            Ok::<_, std::io::Error>(Frame::data(Bytes::from(
                                                format!("data: {event}\n\n"),
                                            ))),
                                            (events, gate),
                                        ))
                                    },
                                );
                                BodyExt::boxed(http_body_util::StreamBody::new(stream))
                            } else {
                                bytes_body(Bytes::from(json!({"error":{"code":"server_is_overloaded","message":"Capacity exhausted"}}).to_string()))
                            };
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(script.status)
                                    .header(
                                        CONTENT_TYPE,
                                        if script.status == StatusCode::OK {
                                            "text/event-stream"
                                        } else {
                                            "application/json"
                                        },
                                    )
                                    .body(body)
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .with_upgrades()
                        .await;
                });
            }
        });
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = tcp.local_addr().unwrap();
        let listener = ListenerConfig {
            address,
            pool: "default".into(),
        };
        let accounts = members
            .iter()
            .copied()
            .map(|name| {
                let path = dir.path().join(name);
                fs::create_dir_all(&path).unwrap();
                fs::write(
                    path.join("auth.json"),
                    json!({"tokens":{"access_token":format!("token-{name}")}}).to_string(),
                )
                .unwrap();
                (name.to_owned(), AccountConfig::CodexHome { path })
            })
            .collect();
        let config = Arc::new(Config {
            proxy: ProxyConfig {
                upstream: format!("http://{upstream_address}/backend-api/codex"),
                responses_websocket_mode: mode,
                installation_secret: "0123456789abcdef".into(),
                affinity_key: "0123456789abcdef0123456789abcdef".into(),
                state_dir: Some(dir.path().join("state")),
                ..ProxyConfig::default()
            },
            listeners: BTreeMap::from([("default".into(), listener.clone())]),
            pools: BTreeMap::from([(
                "default".into(),
                PoolConfig {
                    members: members.iter().map(|name| (*name).to_owned()).collect(),
                    preferred: Some("a".into()),
                },
            )]),
            accounts,
        });
        let affinity = Arc::new(
            AffinityStore::load(
                dir.path().join("affinity.json"),
                &config.proxy.affinity_key,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Arc::new(Router::new(&config, affinity));
        let app = App::new_unvalidated(config, router.clone(), Arc::new(Stats::default())).unwrap();
        let file_owners = app.file_owners.clone();
        let proxy = tokio::spawn(async move {
            app.serve_tcp("default".into(), listener, tcp)
                .await
                .unwrap();
        });
        Self {
            address,
            seen,
            router,
            file_owners,
            tasks: vec![server, proxy],
            _dir: dir,
        }
    }

    async fn connect(&self) -> WebSocketStream<TcpStream> {
        self.connect_with_headers(&[]).await
    }

    async fn connect_with_headers(&self, headers: &[(&str, &str)]) -> WebSocketStream<TcpStream> {
        let stream = TcpStream::connect(self.address).await.unwrap();
        let mut request = format!("ws://{}/0123456789abcdef/v1/responses", self.address)
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert(AUTHORIZATION, "Bearer caller-token".parse().unwrap());
        for (name, value) in headers {
            request.headers_mut().insert(
                hyper::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        tokio_tungstenite::client_async(request, stream)
            .await
            .unwrap()
            .0
    }
}

fn created(id: &str) -> Value {
    json!({"type":"response.created","sequence_number":0,"response":{"id":id,"status":"in_progress","output":[],"usage":null}})
}

fn capacity(id: &str) -> Script {
    Script::events(vec![
        created(id),
        json!({"type":"response.in_progress","sequence_number":1,"response":{"id":id,"status":"in_progress","output":[],"usage":null}}),
        json!({"type":"response.failed","sequence_number":2,"response":{
            "id":id,"status":"failed","output":[],
            "usage":{"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0}},
            "error":{"code":"server_is_overloaded","message":"Capacity exhausted"}
        }}),
    ])
}

fn success(id: &str) -> Script {
    Script::events(vec![
        created(id),
        json!({"type":"response.output_item.done","sequence_number":1,"response_id":id,"output_index":0,"item":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}}),
        json!({"type":"response.completed","sequence_number":2,"response":{
            "id":id,"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}],
            "usage":{"output_tokens":1,"output_tokens_details":{"reasoning_tokens":0}}
        }}),
    ])
}

async fn send_create(ws: &mut WebSocketStream<TcpStream>, previous: Option<&str>) {
    let mut request = json!({"type":"response.create","model":"gpt-5","input":[{"role":"user","content":"hello"}]});
    if let Some(id) = previous {
        request["previous_response_id"] = json!(id);
        request["input"][0]["content"] = json!("followup");
    }
    ws.send(Message::Text(request.to_string().into()))
        .await
        .unwrap();
}

async fn collect_turn(ws: &mut WebSocketStream<TcpStream>) -> Vec<Value> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut events = Vec::new();
        while let Some(Ok(message)) = ws.next().await {
            match message {
                Message::Text(text) => {
                    let event: Value = serde_json::from_str(&text).unwrap();
                    let terminal = matches!(
                        event["type"].as_str(),
                        Some(
                            "response.completed"
                                | "response.failed"
                                | "response.incomplete"
                                | "error"
                        )
                    );
                    events.push(event);
                    if terminal {
                        break;
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        events
    })
    .await
    .expect("proxy did not settle the scripted turn")
}

#[tokio::test]
async fn accepted_capacity_retry_is_invisible_and_continuation_uses_replacement() {
    for mode in [
        ResponsesWebsocketMode::Direct,
        ResponsesWebsocketMode::HttpBridge,
    ] {
        for terminal_type in ["response.failed", "response.incomplete", "error"] {
            let mut rejection = capacity("resp_a");
            rejection.events[2]["type"] = json!(terminal_type);
            if terminal_type == "response.incomplete" {
                rejection.events[2]["response"]["status"] = json!("incomplete");
            }
            let fixture = Fixture::start(
                mode,
                vec![rejection, success("resp_b"), success("resp_next")],
            )
            .await;
            let mut ws = fixture.connect().await;
            send_create(&mut ws, None).await;
            let events = collect_turn(&mut ws).await;
            assert_eq!(events, success("resp_b").events, "mode {mode:?}");
            send_create(&mut ws, Some("resp_b")).await;
            assert_eq!(collect_turn(&mut ws).await, success("resp_next").events);
            assert!(
                fixture
                    .router
                    .affinity
                    .get(&fixture.router.affinity.key("previous-response:resp_a"))
                    .await
                    .is_none()
            );
            assert_eq!(
                fixture
                    .router
                    .affinity
                    .get(&fixture.router.affinity.key("previous-response:resp_b"))
                    .await
                    .map(|binding| binding.account_id),
                Some("b".into())
            );
            let seen = fixture.seen.lock().unwrap();
            assert_eq!(seen.len(), 3, "{seen:?}");
            assert_eq!(seen[0].authorization, "Bearer token-a");
            assert_eq!(seen[1].authorization, "Bearer token-b");
            assert_eq!(seen[2].authorization, "Bearer token-b");
            if mode == ResponsesWebsocketMode::Direct {
                assert_eq!(seen[2].body["previous_response_id"], "resp_b");
            } else {
                assert!(seen[2].body.get("previous_response_id").is_none());
                let input = seen[2].body["input"].as_array().unwrap();
                assert!(
                    input.iter().any(|item| item["role"] == "assistant"),
                    "{input:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn accepted_capacity_retry_never_replays_raw_http_streams() {
    let rejection = capacity("resp_a");
    let fixture = Fixture::start(
        ResponsesWebsocketMode::HttpBridge,
        vec![rejection.clone(), success("resp_unexpected")],
    )
    .await;
    let response = reqwest::Client::new()
        .post(format!(
            "http://{}/0123456789abcdef/v1/responses",
            fixture.address
        ))
        .bearer_auth("caller-token")
        .header(CONTENT_TYPE.as_str(), "application/json")
        .body(json!({"model":"gpt-5","stream":true,"input":"hello"}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.unwrap();
    assert_eq!(
        body,
        rejection
            .events
            .iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect::<String>()
    );
    let seen = fixture.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].authorization, "Bearer token-a");
}

#[tokio::test]
async fn accepted_capacity_retry_requires_explicit_zero_output_and_capacity() {
    let base = capacity("resp_a");
    let mut cases = Vec::new();
    for event in [
        json!({"type":"response.output_text.delta","response_id":"resp_a","delta":"hello"}),
        json!({"type":"response.output_item.added","response_id":"resp_a","output_index":0,"item":{"type":"reasoning","id":"rs_1","summary":[]}}),
        json!({"type":"response.output_item.added","response_id":"resp_a","output_index":0,"item":{"type":"function_call","id":"fc_1","call_id":"call_1","name":"tool","arguments":""}}),
    ] {
        let mut script = base.clone();
        script.events.insert(2, event);
        cases.push(script);
    }
    let mut positive_output = base.clone();
    positive_output.events[2]["response"]["usage"]["output_tokens"] = json!(1);
    cases.push(positive_output);
    let mut positive_reasoning = base.clone();
    positive_reasoning.events[2]["response"]["usage"]["output_tokens_details"]["reasoning_tokens"] =
        json!(1);
    cases.push(positive_reasoning);
    let mut missing_usage = base.clone();
    missing_usage.events[2]["response"]
        .as_object_mut()
        .unwrap()
        .remove("usage");
    cases.push(missing_usage);
    let mut quota = base.clone();
    quota.events[2]["response"]["error"]["code"] = json!("insufficient_quota");
    cases.push(quota);
    for usage in [
        json!({"input_tokens":2,"total_tokens":3,"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0}}),
        json!({"output_tokens":0,"reasoning_tokens":1,"output_tokens_details":{"reasoning_tokens":0}}),
        json!({"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0},"completion_tokens_details":{"reasoning_tokens":1}}),
        json!({"output_tokens":"0","output_tokens_details":{"reasoning_tokens":0}}),
    ] {
        let mut script = base.clone();
        script.events[2]["response"]["usage"] = usage;
        cases.push(script);
    }
    let mut terminal_output = base.clone();
    terminal_output.events[2]["response"]["output"] =
        json!([{"type":"reasoning","id":"rs_terminal","summary":[]}]);
    cases.push(terminal_output);
    let mut unknown = base.clone();
    unknown.events.insert(
        2,
        json!({"type":"response.future_event","response_id":"resp_a"}),
    );
    cases.push(unknown);
    let mut duplicate_created = base.clone();
    duplicate_created.events.insert(1, created("resp_a"));
    cases.push(duplicate_created);
    for mode in [
        ResponsesWebsocketMode::Direct,
        ResponsesWebsocketMode::HttpBridge,
    ] {
        for (index, script) in cases.iter().enumerate() {
            let fixture =
                Fixture::start(mode, vec![script.clone(), success("resp_unexpected")]).await;
            let mut ws = fixture.connect().await;
            send_create(&mut ws, None).await;
            let events = collect_turn(&mut ws).await;
            let mut expected = script.events.clone();
            if mode == ResponsesWebsocketMode::HttpBridge
                && expected.last().unwrap()["response"]["error"]["code"] == "insufficient_quota"
            {
                let terminal = expected.last_mut().unwrap();
                terminal["status"] = json!(429);
                terminal["headers"] = json!({});
            }
            assert_eq!(events, expected, "mode {mode:?}, case {index}");
            let seen = fixture.seen.lock().unwrap();
            assert_eq!(seen.len(), 1, "mode {mode:?}, case {index}: {seen:?}");
            assert_eq!(seen[0].authorization, "Bearer token-a");
        }
    }
}

#[tokio::test]
async fn accepted_capacity_retry_does_not_guess_after_close_or_eof() {
    for mode in [
        ResponsesWebsocketMode::Direct,
        ResponsesWebsocketMode::HttpBridge,
    ] {
        let fixture = Fixture::start(
            mode,
            vec![
                Script {
                    status: StatusCode::OK,
                    events: vec![created("resp_a")],
                    close: true,
                    resume_after_created: None,
                },
                success("resp_unexpected"),
            ],
        )
        .await;
        let mut ws = fixture.connect().await;
        send_create(&mut ws, None).await;
        let events = collect_turn(&mut ws).await;
        assert!(
            events
                .iter()
                .all(|event| event["response"]["id"] != "resp_unexpected")
        );
        let seen = fixture.seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "mode {mode:?}: {seen:?}");
        assert_eq!(seen[0].authorization, "Bearer token-a");
    }
}

#[tokio::test]
async fn accepted_capacity_retry_has_one_shared_replacement_budget() {
    for mode in [
        ResponsesWebsocketMode::Direct,
        ResponsesWebsocketMode::HttpBridge,
    ] {
        let fixture = Fixture::start(
            mode,
            vec![
                capacity("resp_a"),
                capacity("resp_b"),
                success("resp_unexpected"),
            ],
        )
        .await;
        let mut ws = fixture.connect().await;
        send_create(&mut ws, None).await;
        let events = collect_turn(&mut ws).await;
        assert_eq!(events, capacity("resp_b").events, "mode {mode:?}");
        let seen = fixture.seen.lock().unwrap();
        assert_eq!(seen.len(), 2, "mode {mode:?}: {seen:?}");
        assert_eq!(seen[0].authorization, "Bearer token-a");
        assert_eq!(seen[1].authorization, "Bearer token-b");
    }
    let fixture = Fixture::start(
        ResponsesWebsocketMode::HttpBridge,
        vec![
            Script {
                status: StatusCode::SERVICE_UNAVAILABLE,
                events: vec![],
                close: false,
                resume_after_created: None,
            },
            capacity("resp_b"),
            success("resp_unexpected"),
        ],
    )
    .await;
    let mut ws = fixture.connect().await;
    send_create(&mut ws, None).await;
    assert_eq!(collect_turn(&mut ws).await, capacity("resp_b").events);
    let seen = fixture.seen.lock().unwrap();
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert_eq!(seen[0].authorization, "Bearer token-a");
    assert_eq!(seen[1].authorization, "Bearer token-b");
}

#[tokio::test]
async fn accepted_capacity_retry_without_alternate_preserves_original_turn() {
    for mode in [
        ResponsesWebsocketMode::Direct,
        ResponsesWebsocketMode::HttpBridge,
    ] {
        let fixture = Fixture::start_with_members(mode, vec![capacity("resp_a")], &["a"]).await;
        let mut ws = fixture.connect().await;
        send_create(&mut ws, None).await;
        assert_eq!(
            collect_turn(&mut ws).await,
            capacity("resp_a").events,
            "{mode:?}"
        );
        let seen = fixture.seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "{mode:?}: {seen:?}");
        assert_eq!(seen[0].authorization, "Bearer token-a");
    }
}

#[tokio::test]
async fn accepted_capacity_retry_replacement_refusal_preserves_original_turn() {
    for mode in [
        ResponsesWebsocketMode::Direct,
        ResponsesWebsocketMode::HttpBridge,
    ] {
        let fixture = Fixture::start(
            mode,
            vec![
                capacity("resp_a"),
                Script {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    events: vec![],
                    close: false,
                    resume_after_created: None,
                },
                success("resp_unexpected"),
            ],
        )
        .await;
        let mut ws = fixture.connect().await;
        send_create(&mut ws, None).await;
        assert_eq!(
            collect_turn(&mut ws).await,
            capacity("resp_a").events,
            "{mode:?}"
        );
        let seen = fixture.seen.lock().unwrap();
        assert_eq!(seen.len(), 2, "{mode:?}: {seen:?}");
        assert_eq!(seen[0].authorization, "Bearer token-a");
        assert_eq!(seen[1].authorization, "Bearer token-b");
    }
}

#[tokio::test]
async fn accepted_capacity_retry_direct_precreated_failover_consumes_allowance() {
    for code in ["usage_limit_reached", "server_is_overloaded"] {
        let fixture = Fixture::start(ResponsesWebsocketMode::Direct, vec![
            Script::events(vec![json!({"type":"error","error":{"type":"server_error","code":code,"message":code}})]),
            capacity("resp_b"), success("resp_unexpected"),
        ]).await;
        let mut ws = fixture.connect().await;
        send_create(&mut ws, None).await;
        assert_eq!(
            collect_turn(&mut ws).await,
            capacity("resp_b").events,
            "{code}"
        );
        let seen = fixture.seen.lock().unwrap();
        assert_eq!(seen.len(), 2, "{code}: {seen:?}");
        assert_eq!(seen[0].authorization, "Bearer token-a");
        assert_eq!(seen[1].authorization, "Bearer token-b");
    }
}

#[tokio::test]
async fn accepted_capacity_retry_flushes_lifecycle_before_waiting_indefinitely() {
    for mode in [
        ResponsesWebsocketMode::Direct,
        ResponsesWebsocketMode::HttpBridge,
    ] {
        let resume = Arc::new(Notify::new());
        let mut script = capacity("resp_a");
        script.resume_after_created = Some(resume.clone());
        let fixture = Fixture::start(mode, vec![script, success("resp_unexpected")]).await;
        let mut ws = fixture.connect().await;
        send_create(&mut ws, None).await;
        // The upstream cannot send its terminal until the client sees created. This
        // demonstrates a bounded flush without relying on a narrowly timed sleep.
        let first = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("buffered created never became visible")
            .unwrap()
            .unwrap();
        let Message::Text(text) = first else {
            panic!("expected created, got {first:?}")
        };
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap(),
            created("resp_a")
        );
        resume.notify_one();
        assert_eq!(
            collect_turn(&mut ws).await,
            capacity("resp_a").events[1..],
            "{mode:?}"
        );
        let seen = fixture.seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "{mode:?}: {seen:?}");
        assert_eq!(seen[0].authorization, "Bearer token-a");
    }
}

#[tokio::test]
async fn accepted_capacity_retry_preserves_file_hard_and_nonportable_owners() {
    for mode in [
        ResponsesWebsocketMode::Direct,
        ResponsesWebsocketMode::HttpBridge,
    ] {
        for (case, input) in [
            (
                "file",
                json!([{"role":"user","content":[{"type":"input_file","file_id":"file_owned"}]}]),
            ),
            ("hard", json!([{"role":"user","content":"hello"}])),
            (
                "nonportable",
                json!([{"type":"compaction","id":"cmp_owned","encrypted_content":"native-ciphertext"}]),
            ),
        ] {
            let fixture =
                Fixture::start(mode, vec![capacity("resp_a"), success("resp_unexpected")]).await;
            if case == "file" {
                assert!(
                    fixture
                        .file_owners
                        .put(
                            fixture.router.affinity.key("file:file_owned"),
                            "a".into(),
                            0
                        )
                        .await
                );
            }
            if case == "hard" {
                assert!(
                    fixture
                        .router
                        .bind(fixture.router.affinity.key("turn-state:owned-turn"), "a")
                        .await
                );
            }
            let headers = if case == "hard" {
                vec![("x-codex-turn-state", "owned-turn")]
            } else {
                vec![]
            };
            let mut ws = fixture.connect_with_headers(&headers).await;
            ws.send(Message::Text(
                json!({"type":"response.create","model":"gpt-5","input":input})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            assert_eq!(
                collect_turn(&mut ws).await,
                capacity("resp_a").events,
                "{mode:?}, {case}"
            );
            let seen = fixture.seen.lock().unwrap();
            assert_eq!(seen.len(), 1, "{mode:?}, {case}: {seen:?}");
            assert_eq!(seen[0].authorization, "Bearer token-a");
        }
    }
}
