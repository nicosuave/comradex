// Deterministic scheduling: keep an old response body unpolled until a replacement
// credential has been resolved. All auth files and tokens are temporary fixtures.
async fn deferred_quota_after_credential_replacement(replacement_workspace: &str) -> bool {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("managed");
    let expiration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let token = |workspace: &str, marker: &str| {
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&serde_json::json!({
                "exp": expiration,
                "marker": marker,
                "https://api.openai.com/auth": {
                    "chatgpt_account_id": workspace,
                    "chatgpt_user_id": "test-user"
                }
            }))
            .unwrap(),
        );
        format!("e30.{payload}.sig")
    };
    crate::auth::tests::write_managed_auth(&home, &token("old-workspace", "old"));
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let (template, _, _, _) = direct_test_app_with_upstream(
        dir.path(),
        format!("http://{upstream_address}/backend-api/codex"),
    );
    let mut config = (*template.config).clone();
    config.accounts =
        BTreeMap::from([("a".into(), AccountConfig::CodexHome { path: home.clone() })]);
    config.pools.get_mut("default").unwrap().members = vec!["a".into()];
    let config = Arc::new(config);
    let router = Arc::new(Router::new(&config, template.router.affinity.clone()));
    let app =
        App::new_unvalidated(config.clone(), router.clone(), Arc::new(Stats::default())).unwrap();
    let pool = &config.pools["default"];
    let selection = router.select_exact(pool, "a").await.unwrap();
    let old = app
        .auth
        .resolve(&config.accounts["a"], &hyper::HeaderMap::new())
        .await
        .unwrap();
    router
        .validate_selection(&selection, "default", pool)
        .await
        .unwrap();

    // Exercise send_http itself: its response stamp must come from the bearer
    // actually sent to upstream, then survive mapping into the deferred body.
    let expected_authorization = old.authorization.clone();
    let (seen, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = upstream.accept().await.unwrap();
        let seen = Arc::new(Mutex::new(Some(seen)));
        hyper::server::conn::http1::Builder::new().serve_connection(
            TokioIo::new(stream), service_fn(move |request: Request<Incoming>| {
                assert_eq!(request.headers()[AUTHORIZATION], expected_authorization);
                seen.lock().unwrap().take().unwrap().send(()).unwrap();
                async move {
                    Ok::<_, Infallible>(Response::builder()
                        .header(CONTENT_TYPE, "application/json")
                        .header("retry-after", "3600")
                        .body(Full::new(Bytes::from_static(
                            br#"{"status":"incomplete","error":{"code":"usage_limit_reached"},"incomplete_details":{"reason":"usage_limit_reached"}}"#,
                        ))).unwrap())
                }
            })
        ).await.unwrap();
    });
    let dispatched = app
        .send_http(
            "a",
            &Method::GET,
            "/v1/responses",
            &hyper::HeaderMap::new(),
            old.clone(),
            empty_body(),
        )
        .await
        .unwrap();
    received.await.unwrap();
    assert_eq!(
        dispatched.extensions().get::<auth::QuotaOwner>(),
        Some(&old.quota_owner())
    );
    let response =
        map_http_response_leased(dispatched, router.clone(), &selection, true, Vec::new());

    assert!(router.begin_login("a").await);
    crate::auth::tests::write_managed_auth(&home, &token(replacement_workspace, "new"));
    router.finish_login("a", true).await;
    let replacement = app
        .auth
        .resolve(&config.accounts["a"], &hyper::HeaderMap::new())
        .await
        .unwrap();
    assert_ne!(old.authorization, replacement.authorization);
    assert_eq!(
        old.context_identity().unwrap() == replacement.context_identity().unwrap(),
        replacement_workspace == "old-workspace",
    );
    assert!(router.select_exact(pool, "a").await.is_some());
    response.into_body().collect().await.unwrap();
    let available = router.select_exact(pool, "a").await.is_some();
    server.abort();
    available
}

#[tokio::test]
async fn deferred_quota_survives_same_identity_bearer_replacement() {
    assert!(
        !deferred_quota_after_credential_replacement("old-workspace").await,
        "quota from the same physical account must survive bearer replacement",
    );
}

#[tokio::test]
async fn deferred_quota_does_not_block_replacement_physical_identity() {
    assert!(
        deferred_quota_after_credential_replacement("new-workspace").await,
        "a late quota terminal from the old physical account must not block its replacement",
    );
}

struct QuotaIdentityFixture {
    _dir: tempfile::TempDir,
    home: PathBuf,
    app: Arc<App>,
}

impl QuotaIdentityFixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("managed");
        let (template, _, _, _) = direct_test_app(dir.path());
        let mut config = (*template.config).clone();
        config.accounts =
            BTreeMap::from([("a".into(), AccountConfig::CodexHome { path: home.clone() })]);
        config.pools.get_mut("default").unwrap().members = vec!["a".into()];
        let config = Arc::new(config);
        let router = Arc::new(Router::new(&config, template.router.affinity.clone()));
        let app = App::new_unvalidated(config, router, Arc::new(Stats::default())).unwrap();
        let fixture = Self {
            _dir: dir,
            home,
            app,
        };
        fixture.replace("workspace-a", "user-a", "initial");
        fixture
    }

    fn replace(&self, workspace: &str, user: &str, marker: &str) {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&serde_json::json!({
                "exp": chrono::Utc::now().timestamp() + 3600,
                "marker": marker,
                "https://api.openai.com/auth": {
                    "chatgpt_account_id": workspace,
                    "chatgpt_user_id": user,
                }
            }))
            .unwrap(),
        );
        crate::auth::tests::write_managed_auth(&self.home, &format!("e30.{payload}.sig"));
    }

    async fn owner(&self) -> auth::QuotaOwner {
        self.app
            .auth
            .resolve(&self.app.config.accounts["a"], &hyper::HeaderMap::new())
            .await
            .unwrap()
            .quota_owner()
    }

    async fn status(&self) -> crate::routing::AccountRoutingStatus {
        self.app.router.routing_snapshot().await.account_states["a"].clone()
    }
}

fn quota_identity_headers(used: u8, reset: i64) -> hyper::HeaderMap {
    let mut headers = hyper::HeaderMap::new();
    headers.insert("retry-after", "3600".parse().unwrap());
    headers.insert(
        "x-codex-primary-used-percent",
        used.to_string().parse().unwrap(),
    );
    headers.insert(
        "x-codex-primary-reset-at",
        reset.to_string().parse().unwrap(),
    );
    headers
}

fn quota_identity_snapshot(used: u8, reset: i64) -> usage::UsageSnapshot {
    usage::UsageSnapshot {
        observed_at_unix: chrono::Utc::now().timestamp(),
        windows: BTreeMap::from([(
            "primary".into(),
            crate::routing::QuotaWindowStatus {
                used_percent: Some(used),
                reset_at_unix: Some(reset),
                limit_window_seconds: Some(18000),
            },
        )]),
    }
}

fn assert_quota_identity_deadline_eq(actual: Option<i64>, expected: Option<i64>) {
    match (actual, expected) {
        // Status reconstructs monotonic deadlines using whole wall-clock seconds.
        (Some(actual), Some(expected)) => assert!(actual.abs_diff(expected) <= 1),
        _ => assert_eq!(actual, expected),
    }
}

fn assert_quota_identity_status_eq(
    actual: &crate::routing::AccountRoutingStatus,
    expected: &crate::routing::AccountRoutingStatus,
) {
    let (mut actual, mut expected) = (actual.clone(), expected.clone());
    assert_quota_identity_deadline_eq(actual.retry_at_unix.take(), expected.retry_at_unix.take());
    assert_quota_identity_deadline_eq(
        actual.capacity_backoff_until_unix.take(),
        expected.capacity_backoff_until_unix.take(),
    );
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn quota_identity_changes_clear_old_evidence_but_same_identity_refresh_preserves_it() {
    let fixture = QuotaIdentityFixture::new();
    let router = &fixture.app.router;
    let owner = fixture.owner().await;
    let headers = quota_identity_headers(100, chrono::Utc::now().timestamp() + 3600);
    router
        .observe_headers_for_owner("a", &headers, &owner)
        .await;
    router.quota_failure_for_owner("a", &headers, &owner).await;
    router.begin("a").await;
    for _ in 0..3 {
        router.capacity_failure("a").await;
    }
    let before = fixture.status().await;
    assert!(!before.available);
    assert_eq!(before.usage_percent, Some(100));
    assert!(before.capacity_backoff_until_unix.is_some());

    fixture.replace("workspace-a", "user-a", "refreshed-bearer");
    assert_eq!(fixture.owner().await, owner);
    assert_quota_identity_status_eq(&fixture.status().await, &before);

    // A different user within the same workspace is a different quota owner too.
    // No replacement resolve is needed: selection must notice the external file edit.
    fixture.replace("workspace-a", "user-b", "replacement-user");
    assert!(
        router
            .select_exact(&fixture.app.config.pools["default"], "a")
            .await
            .is_some()
    );
    let after = fixture.status().await;
    assert!(after.available);
    assert!(after.quota_windows.is_empty());
    assert!(after.usage_windows.is_empty());
    assert_eq!(after.usage_percent, None);
    assert_eq!(after.inflight, before.inflight);
    assert_quota_identity_deadline_eq(
        after.capacity_backoff_until_unix,
        before.capacity_backoff_until_unix,
    );
}

#[tokio::test]
async fn quota_identity_stale_headers_and_snapshots_cannot_clear_replacement_cooldown() {
    let fixture = QuotaIdentityFixture::new();
    let router = &fixture.app.router;
    let old = fixture.owner().await;
    fixture.replace("workspace-b", "user-b", "replacement");
    let current = fixture.owner().await;
    let reset = chrono::Utc::now().timestamp() + 3600;
    let blocked = quota_identity_headers(100, reset);
    router
        .observe_headers_for_owner("a", &blocked, &current)
        .await;
    router
        .quota_failure_for_owner("a", &blocked, &current)
        .await;
    let before = fixture.status().await;

    // Both observations would otherwise confirm a recovered, advanced quota window.
    let recovered = quota_identity_headers(3, reset + 18000);
    router
        .observe_headers_for_owner("a", &recovered, &old)
        .await;
    assert_quota_identity_status_eq(&fixture.status().await, &before);
    router
        .observe_usage_snapshot_for_owner("a", quota_identity_snapshot(3, reset + 18000), &old)
        .await;
    assert_quota_identity_status_eq(&fixture.status().await, &before);
    router
        .quota_failure_for_owner("a", &quota_identity_headers(100, reset + 36000), &old)
        .await;
    assert_quota_identity_status_eq(&fixture.status().await, &before);

    router
        .observe_usage_snapshot_for_owner("a", quota_identity_snapshot(3, reset + 18000), &current)
        .await;
    assert!(fixture.status().await.available);
    assert_eq!(fixture.status().await.usage_percent, Some(3));
}

#[tokio::test]
async fn quota_identity_unreadable_or_unknown_replacement_preserves_evidence_and_rejects_old_updates()
 {
    let fixture = QuotaIdentityFixture::new();
    let router = &fixture.app.router;
    let old = fixture.owner().await;
    let reset = chrono::Utc::now().timestamp() + 3600;
    let blocked = quota_identity_headers(100, reset);
    router.observe_headers_for_owner("a", &blocked, &old).await;
    router.quota_failure_for_owner("a", &blocked, &old).await;
    let before = fixture.status().await;

    for state in ["missing", "malformed", "opaque"] {
        match state {
            "missing" => fs::remove_file(fixture.home.join("auth.json")).unwrap(),
            "malformed" => fs::write(fixture.home.join("auth.json"), "{").unwrap(),
            "opaque" => crate::auth::tests::write_managed_auth(&fixture.home, "opaque-replacement"),
            _ => unreachable!(),
        }
        router
            .quota_failure_for_owner("a", &quota_identity_headers(100, reset + 36000), &old)
            .await;
        router
            .observe_headers_for_owner("a", &quota_identity_headers(3, reset + 18000), &old)
            .await;
        router
            .observe_usage_snapshot_for_owner("a", quota_identity_snapshot(3, reset + 18000), &old)
            .await;
        assert_quota_identity_status_eq(&fixture.status().await, &before);
    }
}

#[tokio::test]
async fn quota_identity_opaque_bearer_replacement_preserves_legacy_quota() {
    let fixture = QuotaIdentityFixture::new();
    crate::auth::tests::write_managed_auth(&fixture.home, "opaque-original");
    let owner = fixture.owner().await;
    assert!(!owner.is_known());
    fixture
        .app
        .router
        .quota_failure_for_owner(
            "a",
            &quota_identity_headers(100, chrono::Utc::now().timestamp() + 3600),
            &owner,
        )
        .await;
    crate::auth::tests::write_managed_auth(&fixture.home, "opaque-refreshed");
    assert_eq!(fixture.owner().await, owner);
    assert!(!fixture.status().await.available);
}
