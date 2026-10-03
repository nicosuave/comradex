use super::*;
use crate::{
    auth::QuotaOwner,
    config::{AccountConfig, ProxyConfig},
    routing::QuotaWindowStatus,
    usage::UsageSnapshot,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fs};

const KEY: &str = "0123456789abcdef";
const CLAUDE_URL: &str = "https://api.anthropic.com/api/oauth/usage";

type WindowObservation<'a> = (&'a str, Option<u8>, Option<u64>, Option<i64>);

struct Fixture {
    app: Arc<App>,
    urls: Vec<String>,
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

impl Fixture {
    async fn new() -> Self {
        Self::with_credit_upstream(None).await
    }

    async fn with_credit_upstream(upstream: Option<&str>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut accounts = BTreeMap::new();
        for (name, claude) in [("ada", false), ("grace", true), ("anna", true)] {
            let path = dir.path().join(name);
            fs::create_dir_all(&path).unwrap();
            if claude {
                fs::write(
                    path.join("claude-auth.json"),
                    json!({
                        "access_token": "sk-ant-oat-test-secret",
                        "refresh_token": "test-refresh-secret",
                        "expires_at": 4102444800_u64,
                        "account_uuid": "11111111-1111-4111-8111-111111111111",
                        "organization_uuid": "22222222-2222-4222-8222-222222222222",
                        "device_id": "a".repeat(64),
                    })
                    .to_string(),
                )
                .unwrap();
                accounts.insert(name.into(), AccountConfig::ClaudeHome { path });
            } else {
                fs::write(
                    path.join("auth.json"),
                    json!({
                        "tokens": { "access_token": "test-codex-secret" },
                    })
                    .to_string(),
                )
                .unwrap();
                accounts.insert(name.into(), AccountConfig::CodexHome { path });
            }
        }
        accounts.insert("inbound".into(), AccountConfig::Inbound);
        accounts.insert("claude-inbound".into(), AccountConfig::ClaudeInbound);
        let config = Arc::new(Config {
            proxy: ProxyConfig {
                installation_secret: KEY.into(),
                affinity_key: "0123456789abcdef0123456789abcdef".into(),
                state_dir: Some(dir.path().join("state")),
                ..Default::default()
            },
            listeners: BTreeMap::from([(
                "default".into(),
                ListenerConfig {
                    address: "127.0.0.1:0".parse().unwrap(),
                    pool: "codex".into(),
                },
            )]),
            pools: BTreeMap::from([
                (
                    "codex".into(),
                    PoolConfig {
                        members: vec!["ada".into(), "inbound".into()],
                        ..Default::default()
                    },
                ),
                (
                    "claude".into(),
                    PoolConfig {
                        members: vec!["grace".into(), "anna".into(), "claude-inbound".into()],
                        ..Default::default()
                    },
                ),
            ]),
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
        let mut app = App::new(config, router, Arc::new(Stats::default())).unwrap();
        if let Some(upstream) = upstream {
            let app = Arc::get_mut(&mut app).unwrap();
            app.usage_url = format!("{upstream}/usage").parse().unwrap();
        }
        let mut fixture = Self {
            app,
            urls: Vec::new(),
            tasks: Vec::new(),
            _dir: dir,
        };
        for (pool, loopback) in [("codex", true), ("claude", true), ("codex", false)] {
            let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = tcp.local_addr().unwrap();
            fixture.urls.push(format!("http://{address}"));
            let listener = ListenerConfig {
                pool: pool.into(),
                address: if loopback {
                    address
                } else {
                    "0.0.0.0:10100".parse().unwrap()
                },
            };
            let app = fixture.app.clone();
            fixture.tasks.push(tokio::spawn(async move {
                app.serve_tcp(pool.into(), listener, tcp).await.unwrap();
            }));
        }
        fixture
    }

    async fn observe(&self, name: &str, windows: &[WindowObservation<'_>]) {
        let owner = match &self.app.config.accounts[name] {
            AccountConfig::ClaudeHome { path } => crate::claude::auth::read(path).unwrap().owner(),
            _ => QuotaOwner::default(),
        };
        self.app
            .router
            .observe_usage_snapshot_for_owner(
                name,
                UsageSnapshot {
                    reset_credits_available: None,
                    observed_at_unix: 1788800000,
                    windows: windows
                        .iter()
                        .map(|(name, percent, seconds, reset)| {
                            (
                                (*name).into(),
                                QuotaWindowStatus {
                                    used_percent: *percent,
                                    limit_window_seconds: *seconds,
                                    reset_at_unix: *reset,
                                },
                            )
                        })
                        .collect(),
                },
                &owner,
            )
            .await;
    }

    async fn call(&self, account: &str, method: &str, url: &str) -> reqwest::Response {
        reqwest::Client::new()
            .post(format!("{}/v0/management/api-call", self.urls[0]))
            .bearer_auth(KEY)
            .header(CONTENT_TYPE, "application/json")
            .body(json!({
                "auth_index": account, "method": method, "url": url,
                "header": { "Authorization": "Bearer $TOKEN$", "anthropic-beta": "oauth-2025-04-20" },
            }).to_string())
            .send().await.unwrap()
    }
}

async fn json_response(response: reqwest::Response) -> Value {
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    serde_json::from_slice(&response.bytes().await.unwrap()).unwrap()
}

async fn usage_body(response: reqwest::Response) -> Value {
    let envelope = json_response(response).await;
    assert_eq!(envelope["status_code"], 200);
    assert_eq!(
        envelope["header"]["X-Comradex-Usage-Updated-At"][0],
        "1788800000"
    );
    serde_json::from_str(envelope["body"].as_str().unwrap()).unwrap()
}

#[tokio::test]
async fn management_lists_both_providers_on_either_loopback_listener_without_credentials() {
    let fixture = Fixture::new().await;
    let before = fixture.app.router.routing_snapshot().await;
    for base in &fixture.urls[..2] {
        let response = reqwest::Client::new()
            .get(format!("{base}/v0/management/auth-files"))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap();
        let value = json_response(response).await;
        assert_eq!(
            value,
            json!({ "files": [
            { "id": "ada", "auth_index": "ada", "provider": "codex", "disabled": false },
            { "id": "anna", "auth_index": "anna", "provider": "claude", "disabled": false },
            { "id": "grace", "auth_index": "grace", "provider": "claude", "disabled": false },
        ] })
        );
    }
    assert_eq!(before, fixture.app.router.routing_snapshot().await);
    fixture.app.shutdown_connections().await;
}

#[tokio::test]
async fn management_projects_all_reported_windows_without_changing_routing_or_polling() {
    let fixture = Fixture::new().await;
    fixture
        .observe(
            "ada",
            &[
                ("primary", Some(25), Some(18000), Some(4102444800)),
                ("secondary", Some(90), Some(604800), Some(4103049600)),
                ("tertiary", Some(0), None, None),
            ],
        )
        .await;
    fixture
        .observe(
            "grace",
            &[
                ("5h", Some(12), Some(18000), Some(4102444800)),
                ("7d", Some(81), Some(604800), Some(4103049600)),
            ],
        )
        .await;
    fixture
        .observe("anna", &[("5h", Some(100), Some(18000), Some(4102444800))])
        .await;
    fixture
        .app
        .router
        .set_preferred("claude", Some("grace".into()))
        .await;
    fixture
        .app
        .router
        .set_preserved("claude", Some("anna".into()))
        .await;
    let before = fixture.app.router.routing_snapshot().await;
    let codex = usage_body(fixture.call("ada", "GET", usage::USAGE_URL).await).await;
    assert_eq!(
        codex,
        json!({ "rate_limit": {
            "primary_window": { "used_percent": 25, "limit_window_seconds": 18000, "reset_at": 4102444800_i64 },
            "secondary_window": { "used_percent": 90, "limit_window_seconds": 604800, "reset_at": 4103049600_i64 },
            "tertiary_window": { "used_percent": 0, "reset_at": null },
        }})
    );
    let claude = usage_body(fixture.call("grace", "GET", CLAUDE_URL).await).await;
    assert_eq!(
        claude,
        json!({
            "five_hour": { "utilization": 12, "resets_at": "2100-01-01T00:00:00Z" },
            "seven_day": { "utilization": 81, "resets_at": "2100-01-08T00:00:00Z" },
        })
    );
    let anna = usage_body(fixture.call("anna", "GET", CLAUDE_URL).await).await;
    assert_eq!(anna["five_hour"]["utilization"], 100);
    assert!(anna.get("seven_day").is_none());
    assert_eq!(before, fixture.app.router.routing_snapshot().await);
    assert_eq!(
        fixture
            .app
            .stats
            .usage_fetch_accounts_checked
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        fixture
            .app
            .stats
            .refresh_accounts_checked
            .load(Ordering::Relaxed),
        0
    );
    fixture.app.shutdown_connections().await;
}

#[tokio::test]
async fn management_rejects_missing_or_wrong_keys_nonloopback_listeners_and_invalid_requests() {
    let fixture = Fixture::new().await;
    let client = reqwest::Client::new();
    let base = &fixture.urls[0];
    for auth in [
        None,
        Some("Bearer wrong"),
        Some(KEY),
        Some("Basic 0123456789abcdef"),
    ] {
        let mut request = client.get(format!("{base}/v0/management/auth-files"));
        if let Some(auth) = auth {
            request = request.header(AUTHORIZATION, auth)
        }
        assert_eq!(
            request.send().await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        client
            .get(format!("{}/v0/management/auth-files", fixture.urls[2]))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    for (method, path, expected) in [
        (Method::POST, "auth-files", StatusCode::METHOD_NOT_ALLOWED),
        (Method::GET, "api-call", StatusCode::METHOD_NOT_ALLOWED),
        (
            Method::GET,
            "auth-files?key=ignored",
            StatusCode::BAD_REQUEST,
        ),
        (Method::POST, "unsupported-operation", StatusCode::NOT_FOUND),
    ] {
        assert_eq!(
            client
                .request(method, format!("{base}/v0/management/{path}"))
                .bearer_auth(KEY)
                .send()
                .await
                .unwrap()
                .status(),
            expected
        );
    }
    for body in [
        "{}".to_owned(),
        "not-json".into(),
        "x".repeat(MAX_USAGE_RESPONSE_BYTES + 1),
    ] {
        assert_eq!(
            client
                .post(format!("{base}/v0/management/api-call"))
                .bearer_auth(KEY)
                .body(body)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    fixture.app.shutdown_connections().await;
}

#[tokio::test]
async fn management_never_forwards_arbitrary_urls_methods_or_other_accounts() {
    let fixture = Fixture::new().await;
    for (account, method, url, expected) in [
        ("unknown", "GET", CLAUDE_URL, StatusCode::NOT_FOUND),
        ("inbound", "GET", usage::USAGE_URL, StatusCode::NOT_FOUND),
        ("claude-inbound", "GET", CLAUDE_URL, StatusCode::NOT_FOUND),
        ("ada", "GET", CLAUDE_URL, StatusCode::BAD_REQUEST),
        ("grace", "GET", usage::USAGE_URL, StatusCode::BAD_REQUEST),
        ("grace", "POST", CLAUDE_URL, StatusCode::BAD_REQUEST),
        (
            "grace",
            "GET",
            "http://127.0.0.1:1/",
            StatusCode::BAD_REQUEST,
        ),
        (
            "grace",
            "GET",
            "https://api.anthropic.com/api/oauth/usage?x=1",
            StatusCode::BAD_REQUEST,
        ),
        (
            "ada",
            "GET",
            "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits?unsupported=1",
            StatusCode::BAD_REQUEST,
        ),
    ] {
        assert_eq!(fixture.call(account, method, url).await.status(), expected);
    }
    fixture.app.shutdown_connections().await;
}

#[tokio::test]
async fn management_reports_unavailable_until_usage_exists_and_during_login() {
    let fixture = Fixture::new().await;
    for (account, url) in [("ada", usage::USAGE_URL), ("grace", CLAUDE_URL)] {
        let result = json_response(fixture.call(account, "GET", url).await).await;
        assert_eq!(result["status_code"], 503);
        assert!(serde_json::from_str::<Value>(result["body"].as_str().unwrap()).is_ok());
    }
    fixture
        .observe(
            "ada",
            &[
                ("primary", None, Some(18000), None),
                ("secondary", Some(0), Some(0), None),
            ],
        )
        .await;
    assert_eq!(
        json_response(fixture.call("ada", "GET", usage::USAGE_URL).await).await["status_code"],
        503
    );
    fixture
        .observe("ada", &[("primary", Some(0), Some(18000), None)])
        .await;
    assert_eq!(
        usage_body(fixture.call("ada", "GET", usage::USAGE_URL).await).await["rate_limit"]["primary_window"]
            ["used_percent"],
        0
    );
    fixture.app.router.auth_failure("ada").await;
    assert_eq!(
        json_response(fixture.call("ada", "GET", usage::USAGE_URL).await).await["status_code"],
        503
    );
    fixture.app.shutdown_connections().await;
}

#[tokio::test]
async fn management_fable_reporting_replaces_optional_metadata_without_affecting_shared_usage() {
    let fixture = Fixture::new().await;
    fixture
        .observe("grace", &[("5h", Some(15), Some(18000), None)])
        .await;
    fixture
        .observe("anna", &[("5h", Some(25), Some(18000), None)])
        .await;
    let home = fixture.app.config.accounts["grace"].home().unwrap();
    let owner = crate::claude::auth::read(home).unwrap().owner();
    let before = fixture.app.router.routing_snapshot().await;
    let valid = br#"{"seven_day_fable":{"utilization":99.9,"resets_at":"2100-01-01T00:00:00Z","extra":"must-not-leak"}}"#;
    fixture
        .app
        .observe_claude_reporting_usage("grace", owner.clone(), valid)
        .await;
    assert_eq!(
        usage_body(fixture.call("grace", "GET", CLAUDE_URL).await).await["seven_day_fable"],
        json!({ "utilization": 99.9, "resets_at": "2100-01-01T00:00:00Z" })
    );
    assert!(
        usage_body(fixture.call("anna", "GET", CLAUDE_URL).await)
            .await
            .get("seven_day_fable")
            .is_none()
    );
    for bytes in [
        b"{}".as_slice(),
        br#"{"seven_day_fable":null}"#,
        br#"{"seven_day_fable":{"utilization":1000}}"#,
        br#"{"seven_day_fable":{"utilization":15,"resets_at":"invalid"}}"#,
    ] {
        fixture
            .app
            .observe_claude_reporting_usage("grace", owner.clone(), valid)
            .await;
        fixture
            .app
            .observe_claude_reporting_usage("grace", owner.clone(), bytes)
            .await;
        let body = usage_body(fixture.call("grace", "GET", CLAUDE_URL).await).await;
        assert!(body.get("seven_day_fable").is_none());
        assert_eq!(body["five_hour"]["utilization"], 15);
    }
    fixture
        .app
        .observe_claude_reporting_usage("grace", owner, br#"{"seven_day_fable":{"utilization":0}}"#)
        .await;
    assert_eq!(
        usage_body(fixture.call("grace", "GET", CLAUDE_URL).await).await["seven_day_fable"],
        json!({ "utilization": 0, "resets_at": null })
    );
    assert_eq!(before, fixture.app.router.routing_snapshot().await);
    assert_eq!(
        fixture
            .app
            .stats
            .usage_fetch_accounts_checked
            .load(Ordering::Relaxed),
        0
    );
    fixture.app.shutdown_connections().await;
}

#[tokio::test]
async fn management_fable_reporting_rejects_data_from_a_replaced_account_or_organization() {
    for field in ["account_uuid", "organization_uuid"] {
        let fixture = Fixture::new().await;
        fixture
            .observe("grace", &[("5h", Some(15), Some(18000), None)])
            .await;
        let home = fixture.app.config.accounts["grace"].home().unwrap();
        let owner = crate::claude::auth::read(home).unwrap().owner();
        let bytes = br#"{"seven_day_fable":{"utilization":50,"resets_at":null}}"#;
        fixture
            .app
            .observe_claude_reporting_usage("grace", owner.clone(), bytes)
            .await;
        assert!(fixture.app.claude_reporting_usage("grace").await.is_some());
        let path = home.join("claude-auth.json");
        let mut credential: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        credential[field] = json!("33333333-3333-4333-8333-333333333333");
        fs::write(path, credential.to_string()).unwrap();
        // New shared data must not be paired with the old identity's model quota.
        fixture
            .observe("grace", &[("5h", Some(25), Some(18000), None)])
            .await;
        // A delayed old poll also cannot expose its quota for the replacement.
        fixture
            .app
            .observe_claude_reporting_usage("grace", owner, bytes)
            .await;
        let body = usage_body(fixture.call("grace", "GET", CLAUDE_URL).await).await;
        assert_eq!(body["five_hour"]["utilization"], 25);
        assert!(body.get("seven_day_fable").is_none());
        let current = crate::claude::auth::read(home).unwrap().owner();
        fixture
            .app
            .observe_claude_reporting_usage("grace", current, bytes)
            .await;
        assert_eq!(
            usage_body(fixture.call("grace", "GET", CLAUDE_URL).await).await["seven_day_fable"]["utilization"],
            50
        );
        fixture.app.shutdown_connections().await;
    }
}

#[tokio::test]
async fn management_exposes_only_matching_account_metadata_for_client_deduplication() {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let fixture = Fixture::new().await;
    let codex_home = fixture.app.config.accounts["ada"].home().unwrap();
    let claims = json!({
        "email": "ada@example.com",
        "private_claim": "must-not-leak",
        "https://api.openai.com/auth": {
            "chatgpt_account_id": "ada-account", "chatgpt_plan_type": "plus",
        },
    });
    fs::write(
        codex_home.join("auth.json"),
        json!({
            "tokens": {
                "access_token": "test-codex-secret",
                "refresh_token": "test-refresh-secret",
                "account_id": "ada-account",
                "id_token": format!("e30.{}.sig", URL_SAFE_NO_PAD.encode(claims.to_string())),
            },
        })
        .to_string(),
    )
    .unwrap();
    let claude_home = fixture.app.config.accounts["grace"].home().unwrap();
    fs::create_dir_all(claude_home.join("native-login")).unwrap();
    let mut profile = json!({
        "oauthAccount": {
            "accountUuid": "11111111-1111-4111-8111-111111111111",
            "organizationUuid": "22222222-2222-4222-8222-222222222222",
            "emailAddress": "grace@example.com",
        },
        "private_field": "must-not-leak",
    });
    let profile_path = claude_home.join("native-login/.claude.json");
    fs::write(&profile_path, profile.to_string()).unwrap();
    let read = || {
        reqwest::Client::new()
            .get(format!("{}/v0/management/auth-files", fixture.urls[0]))
            .bearer_auth(KEY)
            .send()
    };
    let value = json_response(read().await.unwrap()).await;
    assert_eq!(
        value["files"][0],
        json!({
            "id": "ada", "auth_index": "ada", "provider": "codex", "disabled": false,
            "email": "ada@example.com",
            "id_token": { "chatgpt_account_id": "ada-account", "chatgpt_plan_type": "plus" },
        })
    );
    assert_eq!(value["files"][2]["email"], "grace@example.com");
    let text = value.to_string();
    for secret in [
        "test-codex-secret",
        "test-refresh-secret",
        "must-not-leak",
        "e30.",
    ] {
        assert!(!text.contains(secret));
    }

    profile["oauthAccount"]["accountUuid"] = json!("33333333-3333-4333-8333-333333333333");
    fs::write(&profile_path, profile.to_string()).unwrap();
    fs::write(
        codex_home.join("auth.json"),
        json!({
            "tokens": {
                "account_id": "different-account",
                "id_token": format!("e30.{}.sig", URL_SAFE_NO_PAD.encode(claims.to_string())),
            },
        })
        .to_string(),
    )
    .unwrap();
    let value = json_response(read().await.unwrap()).await;
    assert!(value["files"][0].get("email").is_none());
    assert!(value["files"][0].get("id_token").is_none());
    assert!(value["files"][2].get("email").is_none());
    fixture.app.shutdown_connections().await;
}

include!("management_reset_credit_tests.rs");
