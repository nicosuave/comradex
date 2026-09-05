use super::*;
use crate::config::{AccountConfig, ProxyConfig};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, server::conn::http1};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    fs,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest, protocol::Role},
};

const SESSION: &str = "123e4567-e89b-12d3-a456-426614174000";
const SECRET: &str = "0123456789abcdef";
const AFFINITY_KEY: &str = "0123456789abcdef0123456789abcdef";
const NATIVE_NOTE: &str = "native-encrypted-note-from-a";

#[derive(Clone, Debug)]
struct SeenRequest {
    transport: &'static str,
    authorization: String,
    path: String,
    body: Value,
}

struct ContextWebSocketUpstream {
    address: std::net::SocketAddr,
    seen: Arc<Mutex<Vec<SeenRequest>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ContextWebSocketUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn test_token(workspace: &str, user: &str) -> String {
    let payload = URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&json!({
            "exp": 4_102_444_800_u64,
            "sub": user,
            "https://api.openai.com/auth": {
                "chatgpt_account_id": workspace,
                "chatgpt_user_id": user
            }
        }))
        .unwrap(),
    );
    format!("e30.{payload}.sig")
}

fn write_auth(home: &Path, workspace: &str, user: &str) {
    fs::create_dir_all(home).unwrap();
    fs::write(
        home.join("auth.json"),
        serde_json::to_vec(&json!({
            "tokens": {
                "access_token": test_token(workspace, user),
                "account_id": workspace
            }
        }))
        .unwrap(),
    )
    .unwrap();
}

fn authorization(request: &Request<Incoming>) -> String {
    request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

fn response_events(id: &str) -> [Value; 2] {
    [
        json!({
            "type": "response.created",
            "sequence_number": 0,
            "response": {"id": id, "status": "in_progress"}
        }),
        json!({
            "type": "response.completed",
            "sequence_number": 1,
            "response": {"id": id, "status": "completed", "output": []}
        }),
    ]
}

fn is_continuation(body: &Value) -> bool {
    body["input"].as_array().is_some_and(|input| {
        input
            .iter()
            .any(|item| item["type"] == "function_call_output")
    })
}

async fn run_direct_connection(
    upgraded: hyper::upgrade::Upgraded,
    authorization: String,
    path: String,
    seen: Arc<Mutex<Vec<SeenRequest>>>,
) {
    let mut websocket =
        WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, None).await;
    while let Some(Ok(message)) = websocket.next().await {
        let Message::Text(text) = message else {
            if matches!(message, Message::Close(_)) {
                break;
            }
            continue;
        };
        let body: Value = serde_json::from_str(text.as_str()).unwrap();
        let continuation = is_continuation(&body);
        seen.lock().unwrap().push(SeenRequest {
            transport: "websocket",
            authorization: authorization.clone(),
            path: path.clone(),
            body,
        });
        if continuation
            && authorization == format!("Bearer {}", test_token("workspace-a", "user-a"))
        {
            websocket
                .send(Message::Text(
                    json!({
                        "type": "error",
                        "error": {
                            "type": "rate_limit_exceeded",
                            "code": "rate_limit_exceeded",
                            "message": "usage limit reached"
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            continue;
        }
        let id = if continuation {
            "resp_replayed"
        } else {
            "resp_initial"
        };
        for event in response_events(id) {
            websocket
                .send(Message::Text(event.to_string().into()))
                .await
                .unwrap();
        }
    }
}

async fn start_context_upstream() -> ContextWebSocketUpstream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let task_seen = seen.clone();
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let seen = task_seen.clone();
            tokio::spawn(async move {
                let service = service_fn(move |mut request: Request<Incoming>| {
                    let seen = seen.clone();
                    async move {
                        let path = request.uri().path_and_query().unwrap().to_string();
                        let auth = authorization(&request);
                        if request.method() == Method::GET
                            && request
                                .headers()
                                .get(UPGRADE)
                                .and_then(|value| value.to_str().ok())
                                .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
                        {
                            let accept =
                                tokio_tungstenite::tungstenite::handshake::derive_accept_key(
                                    request.headers()[SEC_WEBSOCKET_KEY].as_bytes(),
                                );
                            let upgrade = hyper::upgrade::on(&mut request);
                            tokio::spawn(async move {
                                run_direct_connection(upgrade.await.unwrap(), auth, path, seen)
                                    .await;
                            });
                            return Ok::<_, Infallible>(
                                Response::builder()
                                    .status(StatusCode::SWITCHING_PROTOCOLS)
                                    .header(CONNECTION, "Upgrade")
                                    .header(UPGRADE, "websocket")
                                    .header(SEC_WEBSOCKET_ACCEPT, accept)
                                    .body(Full::new(Bytes::new()))
                                    .unwrap(),
                            );
                        }

                        let bytes = request.into_body().collect().await.unwrap().to_bytes();
                        let body: Value = serde_json::from_slice(&bytes).unwrap();
                        let continuation = is_continuation(&body);
                        seen.lock().unwrap().push(SeenRequest {
                            transport: "http",
                            authorization: auth.clone(),
                            path: path.clone(),
                            body,
                        });
                        if path.contains("/alpha/notes/") {
                            return Ok(Response::builder()
                                .status(StatusCode::OK)
                                .header(CONTENT_TYPE, "application/json")
                                .body(Full::new(Bytes::from(
                                    serde_json::to_vec(&json!({
                                        "encrypted_output": NATIVE_NOTE
                                    }))
                                    .unwrap(),
                                )))
                                .unwrap());
                        }
                        if continuation
                            && auth == format!("Bearer {}", test_token("workspace-a", "user-a"))
                        {
                            return Ok(Response::builder()
                                .status(StatusCode::TOO_MANY_REQUESTS)
                                .header(CONTENT_TYPE, "application/json")
                                .body(Full::new(Bytes::from_static(
                                    br#"{"error":{"type":"rate_limit_exceeded","code":"rate_limit_exceeded","message":"usage limit reached"}}"#,
                                )))
                                .unwrap());
                        }
                        let id = if continuation {
                            "resp_replayed"
                        } else {
                            "resp_initial"
                        };
                        let body = response_events(id)
                            .into_iter()
                            .map(|event| format!("data: {event}\n\n"))
                            .collect::<String>();
                        Ok(Response::builder()
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, "text/event-stream")
                            .body(Full::new(Bytes::from(body)))
                            .unwrap())
                    }
                });
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .with_upgrades()
                    .await;
            });
        }
    });
    ContextWebSocketUpstream {
        address,
        seen,
        task,
    }
}

struct ContextWebSocketProxy {
    address: std::net::SocketAddr,
    app: Arc<App>,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl Drop for ContextWebSocketProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn start_context_proxy(
    dir: &Path,
    upstream: std::net::SocketAddr,
    mode: ResponsesWebsocketMode,
) -> ContextWebSocketProxy {
    let account_a = dir.join("a");
    let account_b = dir.join("b");
    write_auth(&account_a, "workspace-a", "user-a");
    write_auth(&account_b, "workspace-b", "user-b");
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let listener = ListenerConfig {
        address,
        pool: "default".into(),
    };
    let config = Arc::new(Config {
        proxy: ProxyConfig {
            upstream: format!("http://{upstream}/backend-api/codex"),
            responses_websocket_mode: mode,
            installation_secret: SECRET.into(),
            affinity_key: AFFINITY_KEY.into(),
            state_dir: Some(dir.join("state")),
            ..ProxyConfig::default()
        },
        listeners: BTreeMap::from([("default".into(), listener.clone())]),
        pools: BTreeMap::from([(
            "default".into(),
            PoolConfig {
                members: vec!["a".into(), "b".into()],
                preferred: None,
            },
        )]),
        accounts: BTreeMap::from([
            ("a".into(), AccountConfig::CodexHome { path: account_a }),
            ("b".into(), AccountConfig::CodexHome { path: account_b }),
        ]),
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
    let task = tokio::spawn(app.clone().serve_tcp("default".into(), listener, tcp));
    ContextWebSocketProxy { address, app, task }
}

async fn connect_proxy(address: std::net::SocketAddr) -> WebSocketStream<TcpStream> {
    let stream = TcpStream::connect(address).await.unwrap();
    let request = format!("ws://{address}/{SECRET}/backend-api/codex/responses")
        .into_client_request()
        .unwrap();
    tokio_tungstenite::client_async(request, stream)
        .await
        .unwrap()
        .0
}

async fn send_turn(websocket: &mut WebSocketStream<TcpStream>, body: Value) -> Vec<Value> {
    websocket
        .send(Message::Text(body.to_string().into()))
        .await
        .unwrap();
    let mut events = Vec::new();
    for _ in 0..4 {
        let message = tokio::time::timeout(Duration::from_secs(5), websocket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let event: Value = serde_json::from_str(message.into_text().unwrap().as_ref()).unwrap();
        let terminal = matches!(
            event["type"].as_str(),
            Some("response.completed" | "response.failed" | "error")
        );
        events.push(event);
        if terminal {
            return events;
        }
    }
    panic!("turn did not produce a terminal event: {events:?}");
}

async fn post_proxy(address: std::net::SocketAddr, path: &str, body: Value) -> (StatusCode, Value) {
    let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .build_http::<Full<Bytes>>();
    let response = client
        .request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("http://{address}/{SECRET}/backend-api/codex{path}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Full::new(Bytes::from(serde_json::to_vec(&body).unwrap())))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

fn initial_create() -> Value {
    json!({
        "type": "response.create",
        "model": "gpt-test",
        "client_metadata": {"session_id": SESSION},
        "reasoning": {"context": "all_turns"},
        "input": []
    })
}

fn continuation(encrypted_content: &str) -> Value {
    json!({
        "type": "response.create",
        "model": "gpt-test",
        "client_metadata": {"session_id": SESSION},
        "input": [{
            "type": "function_call_output",
            "call_id": "call_notes",
            "output": [{
                "type": "encrypted_content",
                "encrypted_content": encrypted_content
            }]
        }]
    })
}

fn is_native_note(body: &Value) -> bool {
    body["input"].as_array().is_some_and(|items| {
        items.iter().any(|item| {
            item["output"].as_array().is_some_and(|parts| {
                parts.iter().any(|part| {
                    part["type"] == "encrypted_content" && part["encrypted_content"] == NATIVE_NOTE
                })
            })
        })
    })
}

async fn exercise_trusted_context_replay(mode: ResponsesWebsocketMode) {
    let upstream = start_context_upstream().await;
    let dir = tempfile::tempdir().unwrap();
    let proxy = start_context_proxy(dir.path(), upstream.address, mode).await;
    let mut websocket = connect_proxy(proxy.address).await;

    let initial = send_turn(&mut websocket, initial_create()).await;
    assert_eq!(initial.last().unwrap()["type"], "response.completed");
    let stored = proxy
        .app
        .context_store
        .lookup("default", SESSION)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.owner.alias, "a");
    assert_eq!(stored.participants.len(), 1);

    let (status, note) = post_proxy(
        proxy.address,
        "/alpha/notes/v2/read_file",
        json!({
            "context": {"session_id": SESSION, "current_agent_name": "/root"}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let wrapper = note["encrypted_output"].as_str().unwrap();
    assert!(wrapper.starts_with("comradex-context-v1:"));

    let replayed = send_turn(&mut websocket, continuation(wrapper)).await;
    assert_eq!(replayed.last().unwrap()["type"], "response.completed");
    assert!(
        replayed
            .iter()
            .all(|event| event["response"]["id"] != "resp_quota")
    );

    let stored = proxy
        .app
        .context_store
        .lookup("default", SESSION)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.owner.alias, "a");
    assert_eq!(
        stored
            .participants
            .iter()
            .map(|participant| participant.alias.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );

    let (status, _) = post_proxy(
        proxy.address,
        "/alpha/notes/v2/read_file",
        json!({
            "context": {"session_id": SESSION, "current_agent_name": "/root"}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let seen = upstream.seen.lock().unwrap();
    let expected_transport = match mode {
        ResponsesWebsocketMode::Direct => "websocket",
        ResponsesWebsocketMode::HttpBridge => "http",
        ResponsesWebsocketMode::Raw => unreachable!(),
    };
    let inference = seen
        .iter()
        .filter(|request| request.path.ends_with("/responses"))
        .collect::<Vec<_>>();
    assert!(
        inference
            .iter()
            .all(|request| request.transport == expected_transport)
    );
    let replay_to_b = inference
        .iter()
        .find(|request| {
            request.authorization == format!("Bearer {}", test_token("workspace-b", "user-b"))
                && is_continuation(&request.body)
        })
        .expect("trusted continuation replayed to account b");
    assert!(is_native_note(&replay_to_b.body));
    assert!(
        !replay_to_b
            .body
            .to_string()
            .contains("comradex-context-v1:")
    );
    let notes = seen
        .iter()
        .filter(|request| request.path.contains("/alpha/notes/"))
        .collect::<Vec<_>>();
    assert_eq!(notes.len(), 2);
    assert!(notes.iter().all(|request| {
        request.authorization == format!("Bearer {}", test_token("workspace-a", "user-a"))
    }));
}

async fn exercise_untrusted_ciphertext_is_not_replayed(mode: ResponsesWebsocketMode) {
    let upstream = start_context_upstream().await;
    let dir = tempfile::tempdir().unwrap();
    let proxy = start_context_proxy(dir.path(), upstream.address, mode).await;
    let mut websocket = connect_proxy(proxy.address).await;
    assert_eq!(
        send_turn(&mut websocket, initial_create())
            .await
            .last()
            .unwrap()["type"],
        "response.completed"
    );

    let failed = send_turn(
        &mut websocket,
        continuation("untrusted-native-encrypted-content"),
    )
    .await;
    assert_ne!(failed.last().unwrap()["type"], "response.completed");

    let seen = upstream.seen.lock().unwrap();
    assert!(!seen.iter().any(|request| {
        request.path.ends_with("/responses")
            && request.authorization == format!("Bearer {}", test_token("workspace-b", "user-b"))
    }));
}

#[tokio::test]
async fn direct_websocket_context_replay_preserves_native_context_ownership() {
    exercise_trusted_context_replay(ResponsesWebsocketMode::Direct).await;
    exercise_untrusted_ciphertext_is_not_replayed(ResponsesWebsocketMode::Direct).await;
}

#[tokio::test]
async fn http_bridge_websocket_context_replay_preserves_native_context_ownership() {
    exercise_trusted_context_replay(ResponsesWebsocketMode::HttpBridge).await;
    exercise_untrusted_ciphertext_is_not_replayed(ResponsesWebsocketMode::HttpBridge).await;
}
