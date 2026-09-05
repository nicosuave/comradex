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
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

const SESSION: &str = "123e4567-e89b-12d3-a456-426614174000";
const SECRET: &str = "0123456789abcdef";
const AFFINITY_KEY: &str = "0123456789abcdef0123456789abcdef";

#[derive(Clone, Debug)]
struct SeenRequest {
    path: String,
    authorization: String,
    account_id: String,
    encrypted_tool_arguments: Option<String>,
    tool_output_truncation_policy: Option<String>,
    body: Bytes,
}

type Responder = dyn Fn(&SeenRequest) -> (StatusCode, Value) + Send + Sync;

struct MockUpstream {
    address: std::net::SocketAddr,
    seen: Arc<Mutex<Vec<SeenRequest>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for MockUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn start_upstream(responder: Arc<Responder>) -> MockUpstream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let task_seen = seen.clone();
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let seen = task_seen.clone();
            let responder = responder.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| {
                    let seen = seen.clone();
                    let responder = responder.clone();
                    async move {
                        let path = request.uri().path_and_query().unwrap().to_string();
                        let authorization = request
                            .headers()
                            .get(AUTHORIZATION)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_owned();
                        let account_id = request
                            .headers()
                            .get("chatgpt-account-id")
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_owned();
                        let encrypted_tool_arguments = request
                            .headers()
                            .get("x-openai-encrypted-tool-arguments")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned);
                        let tool_output_truncation_policy = request
                            .headers()
                            .get("x-openai-tool-output-truncation-policy")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned);
                        let body = request.into_body().collect().await.unwrap().to_bytes();
                        let observed = SeenRequest {
                            path,
                            authorization,
                            account_id,
                            encrypted_tool_arguments,
                            tool_output_truncation_policy,
                            body,
                        };
                        let (status, response) = responder(&observed);
                        seen.lock().unwrap().push(observed);
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(status)
                                .header(CONTENT_TYPE, "application/json")
                                .body(Full::new(Bytes::from(
                                    serde_json::to_vec(&response).unwrap(),
                                )))
                                .unwrap(),
                        )
                    }
                });
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    MockUpstream {
        address,
        seen,
        task,
    }
}

struct ContextTestApp {
    app: Arc<App>,
    listener: ListenerConfig,
    account_a: PathBuf,
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

fn context_identity(workspace: &str, user: &str) -> String {
    serde_json::to_string(&(workspace, user)).unwrap()
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

fn context_test_app(dir: &Path, upstream: std::net::SocketAddr) -> ContextTestApp {
    let account_a = dir.join("a");
    let account_b = dir.join("b");
    write_auth(&account_a, "workspace-a", "user-a");
    write_auth(&account_b, "workspace-b", "user-b");
    let listener = ListenerConfig {
        address: "127.0.0.1:0".parse().unwrap(),
        pool: "default".into(),
    };
    let config = Arc::new(Config {
        proxy: ProxyConfig {
            upstream: format!("http://{upstream}/backend-api/codex"),
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
            (
                "a".into(),
                AccountConfig::CodexHome {
                    path: account_a.clone(),
                },
            ),
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
    let app = App::new_unvalidated(config, router.clone(), Arc::new(Stats::default())).unwrap();
    ContextTestApp {
        app,
        listener,
        account_a,
    }
}

fn replay(app: &App, body: Bytes) -> ReplayBody {
    ReplayBody::from_bytes(
        body,
        app.config.proxy.max_request_bytes,
        app.config.proxy.max_spool_bytes,
        app.stats.clone(),
    )
    .unwrap()
}

async fn post(
    app: &App,
    listener: &ListenerConfig,
    path: &str,
    headers: hyper::HeaderMap,
    body: Bytes,
) -> (StatusCode, Bytes) {
    let response = app
        .handle_http_replay(
            headers,
            Method::POST,
            listener,
            path.to_owned(),
            replay(app, body),
            ServingLane::Http,
        )
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, body)
}

fn expanded_encrypted_content(body: &Value) -> Vec<&str> {
    body["input"][0]["output"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|part| part["encrypted_content"].as_str())
        .collect()
}

async fn establish_owner_and_fetch_note_wrapper(test: &ContextTestApp) -> Value {
    let inference = Bytes::from_static(
        br#"{"client_metadata":{"session_id":"123e4567-e89b-12d3-a456-426614174000"},"reasoning":{"context":"all_turns"},"input":"first"}"#,
    );
    let (status, _) = post(
        &test.app,
        &test.listener,
        "/responses",
        hyper::HeaderMap::new(),
        inference,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, wrapper) = post(
        &test.app,
        &test.listener,
        "/alpha/notes/v2/read_file",
        hyper::HeaderMap::new(),
        Bytes::from_static(
            br#"{"context":{"session_id":"123e4567-e89b-12d3-a456-426614174000","current_agent_name":"/root"}}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    serde_json::from_slice(&wrapper).unwrap()
}

fn response_with_context_wrapper(wrapper: &Value, include_unrelated_reasoning: bool) -> Bytes {
    let mut input = vec![
        json!({
            "type": "function_call",
            "call_id": "call_context",
            "name": "read_file",
            "arguments": "{}"
        }),
        json!({
            "type": "function_call_output",
            "call_id": "call_context",
            "output": [{
                "type": "encrypted_content",
                "encrypted_content": wrapper["encrypted_output"]
            }]
        }),
    ];
    if include_unrelated_reasoning {
        input.push(json!({
            "type": "reasoning",
            "id": "reasoning_from_elsewhere",
            "encrypted_content": "unrelated-native-ciphertext"
        }));
    }
    Bytes::from(
        serde_json::to_vec(&json!({
            "model": "gpt-test",
            "client_metadata": {"session_id": SESSION},
            "input": input
        }))
        .unwrap(),
    )
}

#[tokio::test]
async fn notes_stay_with_owner_and_history_fans_out_to_all_inference_participants() {
    let upstream = start_upstream(Arc::new(|request| {
        let response = if request.path.contains("/alpha/") {
            json!({"encrypted_output": format!("cipher-{}", request.account_id)})
        } else {
            json!({"ok": true})
        };
        (StatusCode::OK, response)
    }))
    .await;
    let dir = tempfile::tempdir().unwrap();
    let test = context_test_app(dir.path(), upstream.address);

    let inference = Bytes::from_static(
        br#"{"client_metadata":{"session_id":"123e4567-e89b-12d3-a456-426614174000"},"reasoning":{"context":"all_turns"},"input":"first"}"#,
    );
    let (status, _) = post(
        &test.app,
        &test.listener,
        "/responses",
        hyper::HeaderMap::new(),
        inference,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let stored = test
        .app
        .context_store
        .lookup("default", SESSION)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.owner.alias, "a");
    assert_eq!(stored.participants.len(), 1);

    test.app
        .context_store
        .record_dispatch(
            "default",
            SESSION,
            "b",
            &context_identity("workspace-b", "user-b"),
        )
        .await
        .unwrap();

    let note_body = Bytes::from_static(
        br#"{"context":{"session_id":"123e4567-e89b-12d3-a456-426614174000","current_agent_name":"/root"},"arguments":{"encrypted":"opaque"}}"#,
    );
    let mut headers = hyper::HeaderMap::new();
    headers.insert(
        "x-openai-encrypted-tool-arguments",
        "encrypted-arguments".parse().unwrap(),
    );
    headers.insert(
        "x-openai-tool-output-truncation-policy",
        "truncate-after-8192".parse().unwrap(),
    );
    let (status, _) = post(
        &test.app,
        &test.listener,
        "/alpha/notes/v2/read_file?cursor=opaque%2Bvalue",
        headers,
        note_body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let note = upstream.seen.lock().unwrap().last().unwrap().clone();
    assert_eq!(
        note.authorization,
        format!("Bearer {}", test_token("workspace-a", "user-a"))
    );
    assert_eq!(note.account_id, "workspace-a");
    assert_eq!(
        note.encrypted_tool_arguments.as_deref(),
        Some("encrypted-arguments")
    );
    assert_eq!(
        note.tool_output_truncation_policy.as_deref(),
        Some("truncate-after-8192")
    );
    assert_eq!(
        note.path,
        "/backend-api/codex/alpha/notes/v2/read_file?cursor=opaque%2Bvalue"
    );
    assert_eq!(note.body, note_body);

    let history_body = Bytes::from_static(
        br#"{"context":{"session_id":"123e4567-e89b-12d3-a456-426614174000","current_agent_name":"/root"},"query":{"encrypted":"history-query"}}"#,
    );
    let (status, packed) = post(
        &test.app,
        &test.listener,
        "/alpha/history/v2/list_items",
        hyper::HeaderMap::new(),
        history_body,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let packed: Value = serde_json::from_slice(&packed).unwrap();
    assert!(
        packed["encrypted_output"]
            .as_str()
            .unwrap()
            .starts_with("comradex-context-v1:")
    );

    let mut continuation = json!({
        "client_metadata": {"session_id": SESSION},
        "input": [{
            "type": "function_call_output",
            "output": [{
                "type": "encrypted_content",
                "encrypted_content": packed["encrypted_output"]
            }]
        }]
    });
    assert!(
        test.app
            .expand_context(&mut continuation, "default")
            .unwrap()
    );
    assert_eq!(
        expanded_encrypted_content(&continuation),
        vec!["cipher-workspace-a", "cipher-workspace-b"]
    );
}

#[tokio::test]
async fn context_wrappers_revalidate_source_credentials_before_inference() {
    for history in [false, true] {
        for state in [
            "login",
            "different-user",
            "different-workspace",
            "same-owner",
        ] {
            let upstream = start_upstream(Arc::new(|_| {
                (
                    StatusCode::OK,
                    json!({"encrypted_output": "source-ciphertext"}),
                )
            }))
            .await;
            let dir = tempfile::tempdir().unwrap();
            let test = context_test_app(dir.path(), upstream.address);
            let mut wrapper = establish_owner_and_fetch_note_wrapper(&test).await;
            let source_home = if history {
                // Validate the second participant too, not just the notes owner.
                test.app
                    .context_store
                    .record_dispatch(
                        "default",
                        SESSION,
                        "b",
                        &context_identity("workspace-b", "user-b"),
                    )
                    .await
                    .unwrap();
                let (status, body) = post(
                    &test.app,
                    &test.listener,
                    "/alpha/history/v2/list_items",
                    hyper::HeaderMap::new(),
                    Bytes::from_static(
                        br#"{"context":{"session_id":"123e4567-e89b-12d3-a456-426614174000","current_agent_name":"/root"}}"#,
                    ),
                )
                .await;
                assert_eq!(status, StatusCode::OK);
                wrapper = serde_json::from_slice(&body).unwrap();
                dir.path().join("b")
            } else {
                test.account_a.clone()
            };
            let (alias, workspace, user) = if history {
                ("b", "workspace-b", "user-b")
            } else {
                ("a", "workspace-a", "user-a")
            };
            match state {
                "login" => assert!(test.app.router.begin_login(alias).await),
                "different-user" => write_auth(&source_home, workspace, "replacement-user"),
                "different-workspace" => write_auth(&source_home, "replacement-workspace", user),
                "same-owner" => write_auth(&source_home, workspace, user),
                _ => unreachable!(),
            }
            upstream.seen.lock().unwrap().clear();

            let (status, body) = post(
                &test.app,
                &test.listener,
                "/responses",
                hyper::HeaderMap::new(),
                response_with_context_wrapper(&wrapper, false),
            )
            .await;
            if state == "same-owner" {
                assert_eq!(status, StatusCode::OK);
                assert!(!upstream.seen.lock().unwrap().is_empty());
            } else {
                assert_eq!(status, StatusCode::BAD_REQUEST, "{history}: {state}");
                let error: Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(error["error"]["type"], "context_result_invalid");
                assert!(upstream.seen.lock().unwrap().is_empty());
            }
        }
    }
}

#[tokio::test]
async fn signed_context_wrapper_can_rotate_inference_without_moving_notes_owner() {
    let upstream = start_upstream(Arc::new(|request| {
        if request.path.contains("/alpha/notes/") {
            return (
                StatusCode::OK,
                json!({"encrypted_output": "cipher-workspace-a"}),
            );
        }
        if request
            .body
            .windows(b"call_context".len())
            .any(|window| window == b"call_context")
            && request.account_id == "workspace-a"
        {
            return (StatusCode::TOO_MANY_REQUESTS, json!({"error": "quota"}));
        }
        (StatusCode::OK, json!({"ok": true}))
    }))
    .await;
    let dir = tempfile::tempdir().unwrap();
    let test = context_test_app(dir.path(), upstream.address);
    let wrapper = establish_owner_and_fetch_note_wrapper(&test).await;

    let (status, _) = post(
        &test.app,
        &test.listener,
        "/responses",
        hyper::HeaderMap::new(),
        response_with_context_wrapper(&wrapper, false),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let seen = upstream.seen.lock().unwrap().clone();
    let rotated: Vec<_> = seen
        .iter()
        .filter(|request| {
            request.path.ends_with("/responses")
                && request
                    .body
                    .windows(b"call_context".len())
                    .any(|window| window == b"call_context")
        })
        .collect();
    assert_eq!(rotated.len(), 2);
    assert_eq!(rotated[0].account_id, "workspace-a");
    assert_eq!(rotated[1].account_id, "workspace-b");
    assert!(
        rotated[1]
            .body
            .windows(b"cipher-workspace-a".len())
            .any(|window| window == b"cipher-workspace-a")
    );
    assert!(
        !rotated[1]
            .body
            .windows(b"comradex-context-v1:".len())
            .any(|window| window == b"comradex-context-v1:")
    );
    drop(seen);

    let stored = test
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
            .map(|account| account.alias.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );

    let before = upstream.seen.lock().unwrap().len();
    let (status, _) = post(
        &test.app,
        &test.listener,
        "/alpha/notes/v2/read_file",
        hyper::HeaderMap::new(),
        Bytes::from_static(
            br#"{"context":{"session_id":"123e4567-e89b-12d3-a456-426614174000","current_agent_name":"/root"}}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let seen = upstream.seen.lock().unwrap();
    assert_eq!(seen.len(), before + 1);
    assert_eq!(seen.last().unwrap().account_id, "workspace-a");
}

#[tokio::test]
async fn unrelated_encrypted_reasoning_keeps_signed_context_replay_on_first_account() {
    let upstream = start_upstream(Arc::new(|request| {
        if request.path.contains("/alpha/notes/") {
            return (
                StatusCode::OK,
                json!({"encrypted_output": "cipher-workspace-a"}),
            );
        }
        if request
            .body
            .windows(b"call_context".len())
            .any(|window| window == b"call_context")
            && request.account_id == "workspace-a"
        {
            return (StatusCode::TOO_MANY_REQUESTS, json!({"error": "quota"}));
        }
        (StatusCode::OK, json!({"ok": true}))
    }))
    .await;
    let dir = tempfile::tempdir().unwrap();
    let test = context_test_app(dir.path(), upstream.address);
    let wrapper = establish_owner_and_fetch_note_wrapper(&test).await;

    let (status, _) = post(
        &test.app,
        &test.listener,
        "/responses",
        hyper::HeaderMap::new(),
        response_with_context_wrapper(&wrapper, true),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

    let seen = upstream.seen.lock().unwrap();
    let attempted: Vec<_> = seen
        .iter()
        .filter(|request| {
            request.path.ends_with("/responses")
                && request
                    .body
                    .windows(b"call_context".len())
                    .any(|window| window == b"call_context")
        })
        .collect();
    assert_eq!(attempted.len(), 1);
    assert_eq!(attempted[0].account_id, "workspace-a");
    assert!(
        attempted[0]
            .body
            .windows(b"unrelated-native-ciphertext".len())
            .any(|window| window == b"unrelated-native-ciphertext")
    );
}

#[tokio::test]
async fn notes_owner_quota_failure_never_calls_an_alternate_account() {
    let upstream = start_upstream(Arc::new(|request| {
        if request.account_id == "workspace-a" {
            (StatusCode::TOO_MANY_REQUESTS, json!({"error": "quota"}))
        } else {
            (StatusCode::OK, json!({"encrypted_output": "wrong-owner"}))
        }
    }))
    .await;
    let dir = tempfile::tempdir().unwrap();
    let test = context_test_app(dir.path(), upstream.address);
    test.app
        .context_store
        .record_dispatch(
            "default",
            SESSION,
            "a",
            &context_identity("workspace-a", "user-a"),
        )
        .await
        .unwrap();

    let (status, _) = post(
        &test.app,
        &test.listener,
        "/alpha/notes/v2/read_file",
        hyper::HeaderMap::new(),
        Bytes::from_static(
            br#"{"context":{"session_id":"123e4567-e89b-12d3-a456-426614174000","current_agent_name":"/root"}}"#,
        ),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let seen = upstream.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].account_id, "workspace-a");
}

#[tokio::test]
async fn first_context_read_ignores_inference_quota_cooldowns() {
    let upstream = start_upstream(Arc::new(|request| {
        (
            StatusCode::OK,
            json!({"encrypted_output": format!("cipher-{}", request.account_id)}),
        )
    }))
    .await;
    let dir = tempfile::tempdir().unwrap();
    let test = context_test_app(dir.path(), upstream.address);
    let mut quota_headers = hyper::HeaderMap::new();
    quota_headers.insert("retry-after", "3600".parse().unwrap());
    test.app.router.quota_failure("a", &quota_headers).await;
    test.app.router.quota_failure("b", &quota_headers).await;

    let (status, _) = post(
        &test.app,
        &test.listener,
        "/alpha/notes/v2/read_file",
        hyper::HeaderMap::new(),
        Bytes::from_static(
            br#"{"context":{"session_id":"123e4567-e89b-12d3-a456-426614174000","current_agent_name":"/root"}}"#,
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let seen = upstream.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].account_id, "workspace-a");
}

#[tokio::test]
async fn context_owner_in_login_is_unavailable_without_upstream_fallback() {
    let upstream = start_upstream(Arc::new(|_| {
        (StatusCode::OK, json!({"encrypted_output": "must-not-run"}))
    }))
    .await;
    let dir = tempfile::tempdir().unwrap();
    let test = context_test_app(dir.path(), upstream.address);
    test.app
        .context_store
        .record_dispatch(
            "default",
            SESSION,
            "a",
            &context_identity("workspace-a", "user-a"),
        )
        .await
        .unwrap();
    assert!(test.app.router.begin_login("a").await);

    let (status, _) = post(
        &test.app,
        &test.listener,
        "/alpha/notes/v2/read_file",
        hyper::HeaderMap::new(),
        Bytes::from_static(
            br#"{"context":{"session_id":"123e4567-e89b-12d3-a456-426614174000","current_agent_name":"/root"}}"#,
        ),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(upstream.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn history_requires_every_participant_to_be_available() {
    let mode = Arc::new(Mutex::new("partial"));
    let responder_mode = mode.clone();
    let upstream = start_upstream(Arc::new(move |request| {
        let mode = *responder_mode.lock().unwrap();
        match (mode, request.account_id.as_str()) {
            ("partial", "workspace-a") => (
                StatusCode::OK,
                json!({"encrypted_output": "cipher-workspace-a"}),
            ),
            _ => (StatusCode::SERVICE_UNAVAILABLE, json!({"error": "down"})),
        }
    }))
    .await;
    let dir = tempfile::tempdir().unwrap();
    let test = context_test_app(dir.path(), upstream.address);
    test.app
        .context_store
        .record_dispatch(
            "default",
            SESSION,
            "a",
            &context_identity("workspace-a", "user-a"),
        )
        .await
        .unwrap();
    test.app
        .context_store
        .record_dispatch(
            "default",
            SESSION,
            "b",
            &context_identity("workspace-b", "user-b"),
        )
        .await
        .unwrap();
    let request_body = Bytes::from_static(
        br#"{"context":{"session_id":"123e4567-e89b-12d3-a456-426614174000","current_agent_name":"/root"}}"#,
    );

    let (status, failed) = post(
        &test.app,
        &test.listener,
        "/alpha/history/v2/list_windows",
        hyper::HeaderMap::new(),
        request_body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let failed = String::from_utf8(failed.to_vec()).unwrap();
    assert!(!failed.contains("cipher-workspace-a"));
    assert!(!failed.contains("encrypted_output"));
    assert!(!failed.contains("comradex-context-v1:"));

    *mode.lock().unwrap() = "unavailable";
    let (status, _) = post(
        &test.app,
        &test.listener,
        "/alpha/history/v2/list_windows",
        hyper::HeaderMap::new(),
        request_body,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn owner_alias_with_same_workspace_but_different_user_fails_closed() {
    let upstream = start_upstream(Arc::new(|_| {
        (StatusCode::OK, json!({"encrypted_output": "must-not-run"}))
    }))
    .await;
    let dir = tempfile::tempdir().unwrap();
    let test = context_test_app(dir.path(), upstream.address);
    test.app
        .context_store
        .record_dispatch(
            "default",
            SESSION,
            "a",
            &context_identity("workspace-a", "user-a"),
        )
        .await
        .unwrap();
    write_auth(&test.account_a, "workspace-a", "different-user");

    let (status, _) = post(
        &test.app,
        &test.listener,
        "/alpha/notes/v2/read_file",
        hyper::HeaderMap::new(),
        Bytes::from_static(
            br#"{"context":{"session_id":"123e4567-e89b-12d3-a456-426614174000","current_agent_name":"/root"}}"#,
        ),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(upstream.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn backend_alias_requires_exact_install_secret_and_path_segment() {
    let upstream = start_upstream(Arc::new(|_| (StatusCode::OK, json!({"ok": true})))).await;
    let dir = tempfile::tempdir().unwrap();
    let test = context_test_app(dir.path(), upstream.address);

    let aliased: Uri =
        format!("/{SECRET}/backend-api/codex/alpha/history/v2/list_items?cursor=opaque")
            .parse()
            .unwrap();
    assert_eq!(
        test.app.authorized_path(&aliased).as_deref(),
        Some("/alpha/history/v2/list_items?cursor=opaque")
    );
    let legacy: Uri = format!("/{SECRET}/v1/responses").parse().unwrap();
    assert_eq!(
        test.app.authorized_path(&legacy).as_deref(),
        Some("/responses")
    );

    for rejected in [
        "/wrong-secret/backend-api/codex/responses".to_owned(),
        format!("/{SECRET}/backend-api/codexevil/responses"),
        format!("/{SECRET}/backend-api/responses"),
    ] {
        assert_eq!(test.app.authorized_path(&rejected.parse().unwrap()), None);
    }
}

#[tokio::test]
async fn context_401_retries_refreshed_credentials_on_the_same_physical_owner() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("a");
    let newer_token = test_token("workspace-a", "user-a").replacen("e30.", "e31.", 1);
    let next_token = newer_token.clone();
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = requests.clone();
    let upstream = start_upstream(Arc::new(move |_| {
        if seen.fetch_add(1, Ordering::SeqCst) == 0 {
            // Model a concurrent successful refresh; force_refresh must reuse the new credential.
            fs::write(
                home.join("auth.json"),
                serde_json::to_vec(&json!({
                    "tokens": {"access_token": next_token, "account_id": "workspace-a"}
                }))
                .unwrap(),
            )
            .unwrap();
            (StatusCode::UNAUTHORIZED, json!({"error":"expired"}))
        } else {
            (StatusCode::OK, json!({"encrypted_output":"native-note"}))
        }
    }))
    .await;
    let test = context_test_app(dir.path(), upstream.address);
    let (status, _) = post(
        &test.app,
        &test.listener,
        "/alpha/notes/v2/write_file",
        hyper::HeaderMap::new(),
        serde_json::to_vec(&json!({
            "context":{"session_id":SESSION,"current_agent_name":"/root"},
            "path":"note", "text":"encrypted-argument"
        }))
        .unwrap()
        .into(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let seen = upstream.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(
        seen.iter()
            .all(|request| request.account_id == "workspace-a")
    );
    assert_eq!(seen[1].authorization, format!("Bearer {newer_token}"));
    assert_eq!(seen[0].body, seen[1].body);
}
