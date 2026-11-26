use super::*;
use crate::config::{AccountConfig, ClaudeDesktopCodexConfig, ProxyConfig};
use http_body_util::StreamBody;
use std::{collections::BTreeMap, fs};

const KEY: &str = "fixture-key-for-claude-desktop-only";
const MODEL: &str = "gpt-fixture";
const ALIAS: &str = "claude-sonnet-gpt-fixture";
const TOOL: &str =
    "mcp__workspace_tools__inspect_fixture_record_with_a_name_long_enough_to_need_a_mapping";

struct SeenRequest {
    headers: HeaderMap,
    body: Value,
}

#[tokio::test]
async fn codex_pool_retries_quota_and_keeps_a_tool_round_trip_on_the_winning_account() {
    let (seen, mut requests) = mpsc::unbounded_channel();
    let continue_stream = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let upstream_task = tokio::spawn({
        let continue_stream = continue_stream.clone();
        let cancelled = cancelled.clone();
        async move {
            loop {
                let (stream, _) = upstream.accept().await.unwrap();
                let seen = seen.clone();
                let continue_stream = continue_stream.clone();
                let cancelled = cancelled.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request| {
                        fixture(
                            request,
                            seen.clone(),
                            continue_stream.clone(),
                            cancelled.clone(),
                        )
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        }
    });
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (app, desktop) = fixture_app(directory.path(), upstream_address, address);
    let gateway_task = tokio::spawn(app.clone().serve_claude_desktop_tcp(desktop, listener));
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let catalog: Value = serde_json::from_str(
        &client
            .get(format!("http://{address}/v1/models"))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(catalog["data"][0]["id"], ALIAS);
    let send = |message: &Value| {
        client
            .post(format!("http://{address}/v1/messages?beta=true"))
            .header("x-api-key", KEY)
            .header("x-claude-code-session-id", "fixture-claude-session")
            .header("cookie", "session=fixture")
            .body(message.to_string())
            .send()
    };
    let mut message = json!({
        "model": ALIAS, "max_tokens": 64, "stream": true,
        "system": [{"type":"text","text":"Use the workspace tools."}],
        "tools": [{"name": TOOL, "input_schema":{"type":"object"}}],
        "messages": [{"role":"user", "content":"Inspect the record."}],
    });
    let mut response = send(&message).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[CONTENT_TYPE], "text/event-stream");
    let first = response.chunk().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&first).contains("message_start"));
    assert!(!String::from_utf8_lossy(&first).contains("message_stop"));
    continue_stream.notify_one();
    let rest = response.text().await.unwrap();
    assert!(rest.contains(TOOL));
    assert!(rest.contains("input_json_delta"));
    assert!(rest.contains("\"stop_reason\":\"tool_use\""));
    assert!(rest.contains("message_stop"));

    let rejected = requests.try_recv().unwrap();
    let accepted = requests.try_recv().unwrap();
    assert_eq!(rejected.headers[AUTHORIZATION], "Bearer fixture-a");
    assert_eq!(accepted.headers[AUTHORIZATION], "Bearer fixture-b");
    assert_eq!(rejected.body, accepted.body);
    assert_eq!(accepted.body["model"], MODEL);
    assert_eq!(accepted.body["instructions"], "Use the workspace tools.");
    assert_eq!(accepted.body["tools"][0]["type"], "function");
    let native_tool = accepted.body["tools"][0]["name"].as_str().unwrap();
    assert_ne!(native_tool, TOOL);
    assert!(native_tool.len() <= 64);
    assert!(!accepted.headers.contains_key("cookie"));
    assert!(!accepted.headers.contains_key("x-api-key"));
    assert!(!accepted.headers.contains_key("x-claude-code-session-id"));
    assert_ne!(accepted.headers["session_id"], "fixture-claude-session");
    assert_eq!(
        accepted.body["prompt_cache_key"],
        accepted.headers["session_id"].to_str().unwrap()
    );

    message["stream"] = json!(false);
    continue_stream.notify_one();
    let tool_reply: Value = send(&message).await.unwrap().json().await.unwrap();
    assert_eq!(tool_reply["stop_reason"], "tool_use");
    assert_eq!(
        tool_reply["content"],
        json!([{"type":"tool_use","id":"call_fixture","name":TOOL,"input":{"id":12}}])
    );
    assert_eq!(
        requests.try_recv().unwrap().headers[AUTHORIZATION],
        "Bearer fixture-b"
    );

    message["messages"].as_array_mut().unwrap().extend([
        json!({"role":"assistant","content":[{"type":"tool_use","id":"call_fixture","name":TOOL,"input":{"id":12}}]}),
        json!({"role":"system","content":[{"type":"text","text":"Reminder: use only this record."}]}),
        json!({"role":"user","content":[{"type":"tool_result","tool_use_id":"call_fixture","content":"record 12"}]}),
    ]);
    let response = send(&message).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let completed: Value = serde_json::from_str(&response.text().await.unwrap()).unwrap();
    assert_eq!(completed["model"], ALIAS);
    assert_eq!(completed["content"][0]["text"], "Found record 12.");
    assert_eq!(completed["stop_reason"], "end_turn");
    assert_eq!(completed["usage"]["input_tokens"], 90);
    assert_eq!(completed["usage"]["cache_read_input_tokens"], 10);
    let continued = requests.try_recv().unwrap();
    assert_eq!(continued.headers[AUTHORIZATION], "Bearer fixture-b");
    assert_eq!(
        continued.headers["session_id"],
        accepted.headers["session_id"]
    );
    assert_eq!(continued.body["input"][1]["name"], native_tool);
    assert_eq!(continued.body["input"][1]["call_id"], "call_fixture");
    assert_eq!(
        continued.body["input"][2],
        json!({
            "type":"message", "role":"system",
            "content":[{"type":"input_text","text":"Reminder: use only this record."}],
        })
    );
    assert_eq!(
        continued.body["input"][3],
        json!({
            "type":"function_call_output", "call_id":"call_fixture", "output":"record 12",
        })
    );

    message["messages"] = json!([{"role":"user","content":"refuse"}]);
    let refusal: Value = send(&message).await.unwrap().json().await.unwrap();
    assert_eq!(
        refusal["content"][0]["text"],
        "I can't help with that request."
    );
    assert_eq!(refusal["stop_reason"], "refusal");
    assert_eq!(
        requests.try_recv().unwrap().headers[AUTHORIZATION],
        "Bearer fixture-b"
    );

    message["stream"] = json!(true);
    let refusal = send(&message).await.unwrap().text().await.unwrap();
    assert!(refusal.contains("I can't help with that request."));
    assert!(refusal.contains("\"stop_reason\":\"refusal\""));
    assert!(refusal.contains("message_stop"));
    assert_eq!(
        requests.try_recv().unwrap().headers[AUTHORIZATION],
        "Bearer fixture-b"
    );

    message["messages"] = json!([{"role":"user","content":"truncate"}]);
    let truncated = send(&message).await.unwrap().text().await.unwrap();
    assert!(truncated.contains("Codex stream ended during a tool call"));
    assert!(truncated.contains("input_json_delta"));
    assert!(!truncated.contains("content_block_stop"));
    assert!(!truncated.contains("message_stop"));
    assert_eq!(
        requests.try_recv().unwrap().headers[AUTHORIZATION],
        "Bearer fixture-b"
    );
    message["messages"] = json!([{"role":"user","content":"interrupt"}]);
    let interrupted = send(&message).await.unwrap().text().await.unwrap();
    assert!(interrupted.contains("message_start"));
    assert!(interrupted.contains("Codex stream ended before completion"));
    assert!(!interrupted.contains("message_stop"));
    assert_eq!(
        requests.try_recv().unwrap().headers[AUTHORIZATION],
        "Bearer fixture-b"
    );
    message["messages"] = json!([{"role":"user","content":"cancel"}]);
    let mut response = send(&message).await.unwrap();
    let first = response.chunk().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&first).contains("message_start"));
    drop(response);
    tokio::time::timeout(Duration::from_secs(3), cancelled.notified())
        .await
        .expect("client disconnect should cancel the upstream request");
    assert_eq!(
        requests.try_recv().unwrap().headers[AUTHORIZATION],
        "Bearer fixture-b"
    );
    assert!(requests.try_recv().is_err());

    gateway_task.abort();
    upstream_task.abort();
    app.shutdown_connections().await;
}

fn fixture_app(
    directory: &std::path::Path,
    upstream: SocketAddr,
    address: SocketAddr,
) -> (Arc<App>, ClaudeDesktopConfig) {
    let desktop = ClaudeDesktopConfig {
        address,
        api_key: KEY.into(),
        mlx_serve: None,
        mlx_response_timeout_seconds: None,
        codex: Some(ClaudeDesktopCodexConfig {
            pool: "subscriptions".into(),
            models: BTreeMap::from([(ALIAS.into(), MODEL.into())]),
        }),
        acp: Default::default(),
    };
    let accounts = ["a", "b"]
        .into_iter()
        .map(|name| {
            let path = directory.join(name);
            fs::create_dir(&path).unwrap();
            fs::write(
                path.join("auth.json"),
                json!({"tokens":{"access_token":format!("fixture-{name}")}}).to_string(),
            )
            .unwrap();
            (name.into(), AccountConfig::CodexHome { path })
        })
        .collect();
    let config = Arc::new(Config {
        proxy: ProxyConfig {
            upstream: format!("http://{upstream}/backend-api/codex"),
            affinity_key: "fixture-affinity-key-for-claude-desktop".into(),
            installation_secret: "fixture-installation-secret".into(),
            state_dir: Some(directory.join("state")),
            claude_desktop: Some(desktop.clone()),
            ..ProxyConfig::default()
        },
        listeners: BTreeMap::new(),
        pools: BTreeMap::from([(
            "subscriptions".into(),
            PoolConfig {
                members: vec!["a".into(), "b".into()],
                preferred: Some("a".into()),
                ..PoolConfig::default()
            },
        )]),
        accounts,
    });
    let affinity = Arc::new(
        AffinityStore::load(
            directory.join("affinity.json"),
            &config.proxy.affinity_key,
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let router = Arc::new(Router::new(&config, affinity));
    let app = App::new_unvalidated(config, router, Arc::new(Stats::default())).unwrap();
    (app, desktop)
}

async fn fixture(
    request: Request<Incoming>,
    seen: mpsc::UnboundedSender<SeenRequest>,
    continue_stream: Arc<Notify>,
    cancelled: Arc<Notify>,
) -> Result<Response<ProxyBody>, Infallible> {
    assert_eq!(request.uri().path(), "/backend-api/codex/responses");
    let (parts, body) = request.into_parts();
    let body: Value = serde_json::from_slice(&body.collect().await.unwrap().to_bytes()).unwrap();
    seen.send(SeenRequest {
        headers: parts.headers.clone(),
        body: body.clone(),
    })
    .unwrap();
    if parts.headers[AUTHORIZATION] == "Bearer fixture-a" {
        return Ok(Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header(CONTENT_TYPE, "application/json")
            .header("retry-after", "60")
            .body(json_body(json!({"error":{"code":"usage_limit_reached"}})))
            .unwrap());
    }
    let mut events = vec![json!({"type":"response.created","response":{"id":"resp_fixture"}})];
    if body["input"][0]["content"][0]["text"] == "truncate" {
        let name = body["tools"][0]["name"].as_str().unwrap();
        let mut item =
            json!({"type":"function_call","call_id":"call_fixture","name":name,"arguments":""});
        events.push(json!({"type":"response.output_item.added","output_index":0,"item":item}));
        events.push(
            json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"{}"}),
        );
        item["arguments"] = json!("{}");
        item["status"] = json!("incomplete");
        events.push(json!({"type":"response.output_item.done","output_index":0,"item":item}));
        events.push(
            json!({"type":"response.incomplete","response":{"id":"resp_fixture","output":[item],
            "incomplete_details":{"reason":"max_output_tokens"}}}),
        );
        let bytes = events.iter().flat_map(sse_bytes).collect::<Vec<_>>();
        return Ok(Response::builder()
            .header(CONTENT_TYPE, "text/event-stream")
            .body(bytes_body(bytes.into()))
            .unwrap());
    }
    if body["input"][0]["content"][0]["text"] == "interrupt" {
        return Ok(Response::builder()
            .header(CONTENT_TYPE, "text/event-stream")
            .body(bytes_body(sse_bytes(&events[0])))
            .unwrap());
    }
    if body["input"][0]["content"][0]["text"] == "cancel" {
        struct NotifyOnDrop(Arc<Notify>);

        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }

        let started = sse_bytes(&events[0]);
        let watch = NotifyOnDrop(cancelled);
        let frames = futures_util::stream::once(async move { Ok(Frame::data(started)) }).chain(
            futures_util::stream::once(async move {
                let _watch = watch;
                std::future::pending().await
            }),
        );
        return Ok(Response::builder()
            .header(CONTENT_TYPE, "text/event-stream")
            .body(BodyExt::boxed(StreamBody::new(frames)))
            .unwrap());
    }
    let refusal = body["input"][0]["content"][0]["text"] == "refuse";
    let tool_turn = !refusal && body["input"].as_array().unwrap().len() == 1;
    let output = if refusal {
        json!({"type":"message","role":"assistant","content":[{"type":"refusal","refusal":"I can't help with that request."}]})
    } else if tool_turn {
        let name = body["tools"][0]["name"].as_str().unwrap();
        events.extend([
            json!({"type":"response.output_item.added","output_index":0,
                "item":{"type":"function_call","call_id":"call_fixture","name":name,"arguments":""}}),
            json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"{\"id\":"}),
            json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"12}"}),
        ]);
        json!({"type":"function_call","call_id":"call_fixture","name":name,"arguments":"{\"id\":12}"})
    } else {
        json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"Found record 12."}]})
    };
    events.push(json!({"type":"response.output_item.done","output_index":0,"item":output}));
    // Exercise both omitted and empty terminal output; the items arrived above.
    let mut completed = json!({"type":"response.completed","response":{"id":"resp_fixture",
        "usage":{"input_tokens":100,"input_tokens_details":{"cached_tokens":10},"output_tokens":9}}});
    if tool_turn {
        completed["response"]["output"] = json!([]);
    }
    events.push(completed);
    let frames = futures_util::stream::unfold(
        (events.into_iter().enumerate(), continue_stream),
        move |(mut events, gate)| async move {
            let (index, event) = events.next()?;
            if index == 1 && tool_turn {
                gate.notified().await;
            }
            Some((Ok(Frame::data(sse_bytes(&event))), (events, gate)))
        },
    );
    Ok(Response::builder()
        .header(CONTENT_TYPE, "text/event-stream")
        .body(BodyExt::boxed(StreamBody::new(frames)))
        .unwrap())
}

fn sse_bytes(event: &Value) -> Bytes {
    Bytes::from(format!("data: {event}\n\n"))
}
