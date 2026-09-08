use super::*;
use crate::config::{AccountConfig, ProxyConfig};
use async_compression::tokio::bufread::ZstdEncoder;
use bytes::Bytes;
use http_body_util::Full;
use std::{collections::BTreeMap, path::Path};
use tokio::io::AsyncReadExt;

const SECRET: &str = "0123456789abcdef";

#[derive(Debug)]
struct SeenRequest {
    path: String,
    headers: hyper::HeaderMap,
    body: Bytes,
}

struct HttpFixture {
    address: std::net::SocketAddr,
    app: Arc<App>,
    seen: Arc<StdMutex<Vec<SeenRequest>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for HttpFixture {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn fixture(dir: &Path, memory_limit: usize, request_limit: usize) -> HttpFixture {
    fixture_with_spool_limit(
        dir,
        memory_limit,
        request_limit,
        ProxyConfig::default().max_spool_bytes,
    )
    .await
}

async fn fixture_with_spool_limit(
    dir: &Path,
    memory_limit: usize,
    request_limit: usize,
    spool_limit: usize,
) -> HttpFixture {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let upstream_seen = seen.clone();
    let upstream_task = tokio::spawn(async move {
        loop {
            let (stream, _) = upstream.accept().await.unwrap();
            let seen = upstream_seen.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req: Request<Incoming>| {
                    let seen = seen.clone();
                    async move {
                        let (parts, body) = req.into_parts();
                        let body = body.collect().await.unwrap().to_bytes();
                        seen.lock().unwrap().push(SeenRequest {
                            path: parts.uri.path_and_query().unwrap().to_string(),
                            headers: parts.headers,
                            body,
                        });
                        Ok::<_, Infallible>(
                            Response::builder()
                                .header(CONTENT_TYPE, "application/json")
                                .body(Full::new(Bytes::from_static(
                                    br#"{"id":"resp_compression","object":"response","status":"completed","output":[]}"#,
                                )))
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
    let config = Arc::new(Config {
        proxy: ProxyConfig {
            upstream: format!("http://{upstream_address}/backend-api/codex"),
            installation_secret: SECRET.into(),
            affinity_key: "0123456789abcdef0123456789abcdef".into(),
            state_dir: Some(dir.join("state")),
            replay_memory_bytes: memory_limit,
            max_request_bytes: request_limit,
            max_spool_bytes: spool_limit,
            ..ProxyConfig::default()
        },
        listeners: BTreeMap::from([("default".into(), listener.clone())]),
        pools: BTreeMap::from([(
            "default".into(),
            PoolConfig {
                members: vec!["caller".into()],
                preferred: None,
            },
        )]),
        accounts: BTreeMap::from([("caller".into(), AccountConfig::Inbound)]),
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
    HttpFixture {
        address,
        app,
        seen,
        tasks: vec![proxy_task, upstream_task],
    }
}

async fn compressed(body: &[u8]) -> Bytes {
    let mut encoder = ZstdEncoder::new(body);
    let mut encoded = Vec::new();
    encoder.read_to_end(&mut encoded).await.unwrap();
    encoded.into()
}

async fn post(
    fixture: &HttpFixture,
    prefix: &str,
    encodings: &[&str],
    body: Bytes,
) -> (StatusCode, Bytes) {
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build(HttpConnector::new());
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(format!(
            "http://{}/{SECRET}/{prefix}/responses?test=compression",
            fixture.address
        ))
        .header(AUTHORIZATION, "Bearer synthetic-caller-token")
        .header(CONTENT_TYPE, "application/json")
        .header(CONTENT_LENGTH, body.len());
    for encoding in encodings {
        request = request.header("content-encoding", *encoding);
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        let response = client
            .request(request.body(Full::new(body)).unwrap())
            .await
            .unwrap();
        let status = response.status();
        (
            status,
            response.into_body().collect().await.unwrap().to_bytes(),
        )
    })
    .await
    .expect("local compressed HTTP request timed out")
}

#[tokio::test]
async fn compressed_responses_decode_before_context_inspection_on_both_http_aliases() {
    // The compaction item forces the context JSON parser that regressed on wire bytes.
    let payload = Bytes::from_static(
        br#"{ "model":"gpt-test", "input":[{"type":"compaction","id":"cmp_owner","encrypted_content":"ciphertext"}], "stream":false }"#,
    );
    let wire = compressed(&payload).await;
    for memory_limit in [1, 4096] {
        let dir = tempfile::tempdir().unwrap();
        let fixture = fixture(dir.path(), memory_limit, 4096).await;
        for prefix in ["v1", "backend-api/codex"] {
            let (status, body) = post(&fixture, prefix, &["zstd"], wire.clone()).await;
            assert_eq!(status, StatusCode::OK, "{body:?}");
        }
        let seen = fixture.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        for request in seen.iter() {
            assert_eq!(
                request.path,
                "/backend-api/codex/responses?test=compression"
            );
            assert_eq!(request.body, payload);
            assert!(!request.headers.contains_key("content-encoding"));
            if let Some(length) = request.headers.get(CONTENT_LENGTH) {
                assert_eq!(
                    length.to_str().unwrap().parse::<usize>().unwrap(),
                    payload.len()
                );
            }
        }
    }
}

#[tokio::test]
async fn plain_and_identity_http_requests_still_forward_identical_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = fixture(dir.path(), 4096, 4096).await;
    let payload = Bytes::from_static(br#"{ "input": "plain request" }"#);
    for encodings in [vec![], vec!["identity"]] {
        let (status, body) = post(&fixture, "v1", &encodings, payload.clone()).await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
    }
    let seen = fixture.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(seen.iter().all(|request| request.body == payload));
}

#[tokio::test]
async fn invalid_or_unsupported_compressed_http_requests_do_not_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = fixture(dir.path(), 4096, 4096).await;
    let wire = compressed(br#"{"input":"test"}"#).await;
    let truncated = wire.slice(..wire.len() - 1);
    let mut trailing_garbage = wire.to_vec();
    trailing_garbage.extend_from_slice(b"trailing garbage");
    for (encodings, body, expected) in [
        (
            vec!["zstd"],
            Bytes::from_static(b"not a zstd frame"),
            StatusCode::BAD_REQUEST,
        ),
        (vec!["zstd"], truncated, StatusCode::BAD_REQUEST),
        (vec!["zstd"], Bytes::new(), StatusCode::BAD_REQUEST),
        (
            vec!["zstd"],
            trailing_garbage.into(),
            StatusCode::BAD_REQUEST,
        ),
        (
            vec!["gzip"],
            wire.clone(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
        (
            vec!["zstd, identity"],
            wire.clone(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
        (
            vec!["zstd", "identity"],
            wire,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
    ] {
        let (status, response) = post(&fixture, "v1", &encodings, body).await;
        assert_eq!(status, expected, "{encodings:?}: {response:?}");
    }
    assert!(fixture.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn compressed_http_request_limit_applies_to_decoded_size() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = fixture(dir.path(), 32, 256).await;
    let payload = format!(r#"{{"input":"{}"}}"#, "x".repeat(4096));
    let wire = compressed(payload.as_bytes()).await;
    assert!(wire.len() < 256);
    let (status, body) = post(&fixture, "v1", &["zstd"], wire).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body:?}");
    assert!(fixture.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn concatenated_zstd_frames_forward_the_entire_decoded_request() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = fixture(dir.path(), 1, 4096).await;
    let payload = br#"{ "input": "split across zstd frames" }"#;
    let split = payload.len() / 2;
    let mut wire = compressed(&payload[..split]).await.to_vec();
    wire.extend_from_slice(&compressed(&payload[split..]).await);
    let (status, body) = post(&fixture, "v1", &["zstd"], wire.into()).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    let seen = fixture.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].body.as_ref(), payload);
}

#[tokio::test]
async fn valid_empty_zstd_frame_forwards_empty_body_for_upstream_validation() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = fixture(dir.path(), 4096, 4096).await;
    let wire = compressed(b"").await;
    assert!(!wire.is_empty());
    let (status, body) = post(&fixture, "v1", &["zstd"], wire).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    let seen = fixture.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert!(seen[0].body.is_empty());
    assert!(!seen[0].headers.contains_key("content-encoding"));
}

#[tokio::test]
async fn compressed_http_request_limit_also_applies_to_wire_size() {
    let dir = tempfile::tempdir().unwrap();
    let payload = b"{}";
    let wire = compressed(payload).await;
    let limit = wire.len() - 1;
    assert!(payload.len() <= limit);
    let fixture = fixture(dir.path(), limit, limit).await;
    let (status, body) = post(&fixture, "v1", &["zstd"], wire).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body:?}");
    assert!(fixture.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn compressed_http_request_respects_global_spool_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = fixture_with_spool_limit(dir.path(), 1, 4096, 32).await;
    let payload = format!(r#"{{"input":"{}"}}"#, "x".repeat(128));
    let wire = compressed(payload.as_bytes()).await;
    let (status, body) = post(&fixture, "v1", &["zstd"], wire).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body:?}");
    assert!(fixture.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn compressed_file_references_enforce_the_same_owner_checks_as_plain_requests() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = fixture(dir.path(), 1, 4096).await;
    let owner_key = fixture.app.router.affinity.key("file:file_owned");
    assert!(
        fixture
            .app
            .file_owners
            .put(owner_key, "caller".into(), 0)
            .await
    );
    // Existing policy rejects a mix of known and unknown file owners; a request
    // with only unknown references is allowed for compatibility.
    let payload = Bytes::from_static(
        br#"{"input":[{"role":"user","content":[{"type":"input_file","file_id":"file_owned"},{"type":"input_file","file_id":"file_unknown"}]}]}"#,
    );
    let (plain_status, plain_body) = post(&fixture, "v1", &[], payload.clone()).await;
    let (compressed_status, compressed_body) =
        post(&fixture, "v1", &["zstd"], compressed(&payload).await).await;
    assert_eq!(plain_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(compressed_status, plain_status);
    assert_eq!(compressed_body, plain_body);
    let error: serde_json::Value = serde_json::from_slice(&compressed_body).unwrap();
    assert_eq!(error["error"]["type"], "file_owner_unavailable");
    assert!(fixture.seen.lock().unwrap().is_empty());
}
