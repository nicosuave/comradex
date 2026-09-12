struct UsageActivationFixture {
    app: Arc<App>,
    posts: Arc<Mutex<Vec<(String, serde_json::Value)>>>,
    active: Arc<AtomicUsize>,
    maximum_active: Arc<AtomicUsize>,
    release: Arc<Notify>,
    used_percent: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

impl Drop for UsageActivationFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl UsageActivationFixture {
    async fn new(enabled: bool, status: StatusCode, event: &'static str, hold: bool) -> Self {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        let dir = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let posts = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum_active = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let used_percent = Arc::new(AtomicUsize::new(0));
        let server_posts = posts.clone();
        let server_active = active.clone();
        let server_maximum = maximum_active.clone();
        let server_release = release.clone();
        let server_used = used_percent.clone();
        let server = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let posts = server_posts.clone();
                let active = server_active.clone();
                let maximum = server_maximum.clone();
                let release = server_release.clone();
                let used_percent = server_used.clone();
                connections.spawn(async move {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service_fn(move |req: Request<Incoming>| {
                            let posts = posts.clone();
                            let active = active.clone();
                            let maximum = maximum.clone();
                            let release = release.clone();
                            let used_percent = used_percent.clone();
                            async move {
                                let account = req.headers()["chatgpt-account-id"].to_str().unwrap().to_owned();
                                assert!(req.headers()[AUTHORIZATION].to_str().unwrap().starts_with("Bearer e30."));
                                if req.method() == Method::GET {
                                    assert_eq!(req.uri().path(), "/usage");
                                    // Reset keeps moving and the percentage stays rounded to zero,
                                    // exercising durable deduplication rather than an artificial usage jump.
                                    let body = serde_json::json!({"rate_limit": {"primary_window": {
                                        "used_percent": used_percent.load(Ordering::SeqCst), "limit_window_seconds": 604800,
                                        "reset_at": chrono::Utc::now().timestamp() + 604800
                                    }}});
                                    return Ok::<_, Infallible>(Response::builder()
                                        .header(CONTENT_TYPE, "application/json")
                                        .body(Full::new(Bytes::from(body.to_string()))).unwrap());
                                }
                                assert_eq!(req.uri().path(), "/backend-api/codex/responses");
                                let bytes = req.into_body().collect().await.unwrap().to_bytes();
                                let value = serde_json::from_slice(&bytes).unwrap();
                                posts.lock().unwrap().push((account, value));
                                let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                                maximum.fetch_max(count, Ordering::SeqCst);
                                if hold { release.notified().await; }
                                active.fetch_sub(1, Ordering::SeqCst);
                                Ok::<_, Infallible>(Response::builder()
                                    .status(status)
                                    .header(CONTENT_TYPE, "text/event-stream")
                                    .body(Full::new(Bytes::from(event))).unwrap())
                            }
                        })).await;
                });
            }
        });
        let (template, _, _, _) = direct_test_app_with_upstream(
            dir.path(),
            format!("http://{address}/backend-api/codex"),
        );
        let mut config = (*template.config).clone();
        config.proxy.auto_activate_weekly_usage = enabled;
        config.accounts.clear();
        // aa is a second configured name for the same physical account as a.
        for (name, workspace) in [
            ("a", "workspace-a"),
            ("aa", "workspace-a"),
            ("b", "workspace-b"),
        ] {
            let home = dir.path().join(name);
            let claims = serde_json::json!({"exp": chrono::Utc::now().timestamp() + 3600,
                "https://api.openai.com/auth": {"chatgpt_account_id": workspace, "chatgpt_user_id": "user"}});
            let token = format!("e30.{}.sig", URL_SAFE_NO_PAD.encode(claims.to_string()));
            fs::create_dir_all(&home).unwrap();
            fs::write(
                home.join("auth.json"),
                serde_json::to_vec(&serde_json::json!({
                    "tokens": {"access_token": token, "account_id": workspace,
                        "refresh_token": "synthetic-refresh-token"}
                }))
                .unwrap(),
            )
            .unwrap();
            config
                .accounts
                .insert(name.into(), AccountConfig::CodexHome { path: home });
        }
        config.pools.get_mut("default").unwrap().members =
            vec!["a".into(), "aa".into(), "b".into()];
        let config = Arc::new(config);
        let router = Arc::new(Router::new(&config, template.router.affinity.clone()));
        let mut app = App::new_unvalidated(config, router, Arc::new(Stats::default())).unwrap();
        Arc::get_mut(&mut app).unwrap().usage_url =
            format!("http://{address}/usage").parse().unwrap();
        Self {
            app,
            posts,
            active,
            maximum_active,
            release,
            used_percent,
            server,
            _dir: dir,
        }
    }

    async fn poll(&self) {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.app
                .refresh_managed_usage_at(chrono::Utc::now().timestamp() as u64),
        )
        .await
        .unwrap();
    }

    fn restart(&mut self) {
        let router = Arc::new(Router::new(
            &self.app.config,
            self.app.router.affinity.clone(),
        ));
        let mut app =
            App::new_unvalidated(self.app.config.clone(), router, Arc::new(Stats::default()))
                .unwrap();
        Arc::get_mut(&mut app).unwrap().usage_url = self.app.usage_url.clone();
        self.app = app;
    }
}

const ACTIVATION_COMPLETED_SSE: &str =
    "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n";

#[tokio::test]
async fn weekly_activation_rechecks_usage_and_identity_before_dispatch() {
    let fixture =
        UsageActivationFixture::new(true, StatusCode::OK, ACTIVATION_COMPLETED_SSE, false).await;
    let account = &fixture.app.config.accounts["a"];
    let (snapshot, credentials) = fixture
        .app
        .fetch_managed_usage_account("a", account, chrono::Utc::now().timestamp() as u64)
        .await
        .unwrap();
    fixture.used_percent.store(1, Ordering::SeqCst);
    fixture
        .app
        .activate_weekly_usage("a", &snapshot, &credentials)
        .await
        .unwrap();
    assert!(fixture.posts.lock().unwrap().is_empty());

    fixture.used_percent.store(0, Ordering::SeqCst);
    let AccountConfig::CodexHome { path: home_a } = account else {
        unreachable!()
    };
    let AccountConfig::CodexHome { path: home_b } = &fixture.app.config.accounts["b"] else {
        unreachable!()
    };
    // Synthetic credentials simulate a login changing between the poll and preflight.
    fs::copy(home_b.join("auth.json"), home_a.join("auth.json")).unwrap();
    fixture
        .app
        .activate_weekly_usage("a", &snapshot, &credentials)
        .await
        .unwrap();
    assert!(fixture.posts.lock().unwrap().is_empty());
    assert!(
        !fixture
            .app
            .config
            .proxy
            .state_dir
            .as_ref()
            .unwrap()
            .join("usage-activation.json")
            .exists()
    );
}

#[tokio::test]
async fn weekly_activation_pins_accounts_and_deduplicates_aliases_and_restarts() {
    let mut fixture =
        UsageActivationFixture::new(true, StatusCode::OK, ACTIVATION_COMPLETED_SSE, false).await;
    fixture.poll().await;
    fixture.poll().await;
    fixture.restart();
    fixture.poll().await;
    let posts = fixture.posts.lock().unwrap();
    assert_eq!(
        posts
            .iter()
            .map(|(account, _)| account.as_str())
            .collect::<Vec<_>>(),
        ["workspace-a", "workspace-b"]
    );
    for (_, body) in posts.iter() {
        assert_eq!(body["model"], "gpt-5.6-luna");
        assert_eq!(body["reasoning"]["effort"], "low");
        assert_eq!(body["tool_choice"], "none");
        assert_eq!(body["tools"], serde_json::json!([]));
        assert_eq!(body["store"], false);
    }
    assert_eq!(fixture.maximum_active.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .app
            .stats
            .usage_fetch_successes
            .load(Ordering::Relaxed),
        3
    );
}

#[tokio::test]
async fn weekly_activation_disabled_and_corrupt_ledger_do_not_stop_usage_polling() {
    let mut fixture =
        UsageActivationFixture::new(false, StatusCode::OK, ACTIVATION_COMPLETED_SSE, false).await;
    fixture.poll().await;
    assert!(fixture.posts.lock().unwrap().is_empty());
    fs::write(
        fixture
            .app
            .config
            .proxy
            .state_dir
            .as_ref()
            .unwrap()
            .join("usage-activation.json"),
        b"broken",
    )
    .unwrap();
    let mut config = (*fixture.app.config).clone();
    config.proxy.auto_activate_weekly_usage = true;
    let mut app = App::new_unvalidated(
        Arc::new(config),
        fixture.app.router.clone(),
        Arc::new(Stats::default()),
    )
    .unwrap();
    Arc::get_mut(&mut app).unwrap().usage_url = fixture.app.usage_url.clone();
    fixture.app = app;
    fixture.poll().await;
    assert!(fixture.posts.lock().unwrap().is_empty());
    assert_eq!(
        fixture
            .app
            .stats
            .usage_fetch_successes
            .load(Ordering::Relaxed),
        3
    );
}

#[tokio::test]
async fn weekly_activation_failures_back_off_without_marking_success() {
    for (status, body) in [
        (StatusCode::TOO_MANY_REQUESTS, "quota"),
        (StatusCode::OK, "data: {\"type\":\"response.failed\"}\n\n"),
        (
            StatusCode::OK,
            "data: {\"type\":\"response.incomplete\"}\n\n",
        ),
        (StatusCode::OK, "data: [DONE]\n\n"),
    ] {
        let mut fixture = UsageActivationFixture::new(true, status, body, false).await;
        fixture.poll().await;
        fixture.restart();
        fixture.poll().await;
        assert_eq!(fixture.posts.lock().unwrap().len(), 2);
        let ledger: serde_json::Value = serde_json::from_slice(
            &fs::read(
                fixture
                    .app
                    .config
                    .proxy
                    .state_dir
                    .as_ref()
                    .unwrap()
                    .join("usage-activation.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert!(
            ledger
                .as_object()
                .unwrap()
                .values()
                .all(|record| record["completed_at"].is_null())
        );
    }
}

#[tokio::test]
async fn weekly_activation_is_sequential_and_cancellation_keeps_reservation() {
    let fixture =
        UsageActivationFixture::new(true, StatusCode::OK, ACTIVATION_COMPLETED_SSE, true).await;
    let app = fixture.app.clone();
    let poll = tokio::spawn(async move {
        app.refresh_managed_usage_at(chrono::Utc::now().timestamp() as u64)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.active.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(fixture.posts.lock().unwrap().len(), 1);
    assert_eq!(fixture.posts.lock().unwrap()[0].0, "workspace-a");
    poll.abort();
    let _ = poll.await;
    fixture.release.notify_one();
    let app = fixture.app.clone();
    let poll = tokio::spawn(async move {
        app.refresh_managed_usage_at(chrono::Utc::now().timestamp() as u64)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.posts.lock().unwrap().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(fixture.posts.lock().unwrap()[1].0, "workspace-b");
    fixture.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), poll)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixture.posts.lock().unwrap().len(), 2);
}
