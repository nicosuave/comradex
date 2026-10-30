mod session_controls {
    use super::*;
    use crate::routing::sessions::{SessionAction, manage_session};

    async fn edit(fixture: &PinFixture, action: SessionAction) {
        manage_session(
            &fixture.app.config,
            &fixture.app.router,
            "soft-thread",
            Some("default"),
            action,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn session_move_and_reset_route_codex_http_and_ignore_stale_completion() {
        let dir = tempfile::tempdir().unwrap();
        let fixture = fixture(dir.path(), None).await;
        let body = r#"{"model":"unlisted","input":[]}"#;
        assert_eq!(
            request(&fixture, Method::POST, "/v1/responses", body, None).await,
            StatusCode::OK
        );
        let shown = manage_session(
            &fixture.app.config,
            &fixture.app.router,
            "soft-thread",
            None,
            SessionAction::Show,
        )
        .await
        .unwrap();
        assert_eq!(shown.account.as_deref(), Some("a"));
        edit(
            &fixture,
            SessionAction::Move {
                account: "b".into(),
            },
        )
        .await;
        assert_eq!(
            request(&fixture, Method::POST, "/v1/responses", body, None).await,
            StatusCode::OK
        );
        // An answer admitted before the edit can still finish and bind old aliases.
        fixture
            .app
            .router
            .bind(fixture.app.router.affinity.key("thread:soft-thread"), "a")
            .await;
        assert_eq!(
            request(&fixture, Method::POST, "/v1/responses", body, None).await,
            StatusCode::OK
        );
        edit(&fixture, SessionAction::Reset).await;
        assert_eq!(
            request(&fixture, Method::POST, "/v1/responses", body, None).await,
            StatusCode::OK
        );
        let accounts: Vec<_> = fixture
            .seen
            .lock()
            .unwrap()
            .iter()
            .map(|seen| seen.0.clone())
            .collect();
        assert_eq!(
            accounts,
            [
                "Bearer token-a",
                "Bearer token-b",
                "Bearer token-b",
                "Bearer token-a"
            ]
        );
    }

    #[tokio::test]
    async fn session_move_preserves_codex_ownership_pins_and_strict_target_failures() {
        let dir = tempfile::tempdir().unwrap();
        let fixture = fixture(dir.path(), None).await;
        let body = r#"{"model":"unlisted","input":[]}"#;
        assert_eq!(
            request(&fixture, Method::POST, "/v1/responses", body, None).await,
            StatusCode::OK
        );
        fixture
            .app
            .router
            .bind(
                fixture.app.router.affinity.key("turn-state:owned-by-a"),
                "a",
            )
            .await;
        edit(
            &fixture,
            SessionAction::Move {
                account: "b".into(),
            },
        )
        .await;
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
        fixture
            .app
            .file_owners
            .put(
                fixture.app.router.affinity.key("file:owned-file"),
                "a".into(),
                0,
            )
            .await;
        assert_eq!(
            request(
                &fixture,
                Method::POST,
                "/v1/responses",
                r#"{"input":[{"type":"input_file","file_id":"owned-file"}]}"#,
                None
            )
            .await,
            StatusCode::CONFLICT
        );
        let opaque =
            r#"{"model":"unlisted","input":[{"type":"reasoning","encrypted_content":"owned"}]}"#;
        assert_eq!(
            request(&fixture, Method::POST, "/v1/responses", opaque, None).await,
            StatusCode::OK
        );
        assert_eq!(fixture.seen.lock().unwrap().len(), 2);
        edit(
            &fixture,
            SessionAction::Move {
                account: "a".into(),
            },
        )
        .await;
        assert_eq!(
            request(
                &fixture,
                Method::POST,
                "/v1/responses",
                r#"{"model":"pinned-model","input":[]}"#,
                None
            )
            .await,
            StatusCode::CONFLICT
        );
        edit(
            &fixture,
            SessionAction::Move {
                account: "b".into(),
            },
        )
        .await;
        fixture.app.router.auth_failure("b").await;
        assert_eq!(
            request(&fixture, Method::POST, "/v1/responses", body, None).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(fixture.seen.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn session_manual_codex_target_never_falls_back_after_quota_or_capacity_rejection() {
        for failure in [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let fixture = fixture(dir.path(), Some(failure)).await;
            edit(
                &fixture,
                SessionAction::Move {
                    account: "b".into(),
                },
            )
            .await;
            assert_eq!(
                request(
                    &fixture,
                    Method::POST,
                    "/v1/responses",
                    r#"{"input":[]}"#,
                    None
                )
                .await,
                failure
            );
            assert_eq!(fixture.seen.lock().unwrap().len(), 1);
            assert_eq!(fixture.seen.lock().unwrap()[0].0, "Bearer token-b");
        }
    }

    #[tokio::test]
    async fn session_move_routes_unanchored_native_history_unchanged_to_the_assigned_account() {
        let dir = tempfile::tempdir().unwrap();
        let fixture = fixture(dir.path(), None).await;
        let portable = r#"{"input":[]}"#;
        assert_eq!(
            request(&fixture, Method::POST, "/v1/responses", portable, None).await,
            StatusCode::OK
        );
        edit(
            &fixture,
            SessionAction::Move {
                account: "b".into(),
            },
        )
        .await;
        let history = r#"{"model":"gpt-6-astra","input":[{"role":"user","content":"continue"},{"type":"reasoning","id":"rs_from_a","encrypted_content":"from-a","summary":[]},{"type":"function_call","id":"fc_from_a","call_id":"call_1","name":"f","arguments":"{}"},{"type":"function_call_output","call_id":"call_1","output":"done"}]}"#;
        for body in [history, portable, history] {
            assert_eq!(
                request(&fixture, Method::POST, "/v1/responses", body, None).await,
                StatusCode::OK
            );
            assert_eq!(
                fixture.seen.lock().unwrap().last().unwrap().0,
                "Bearer token-b"
            );
            assert_eq!(
                fixture.bodies.lock().unwrap().last().unwrap().as_ref(),
                body.as_bytes()
            );
        }
        edit(&fixture, SessionAction::Reset).await;
        assert_eq!(
            request(&fixture, Method::POST, "/v1/responses", history, None).await,
            StatusCode::OK
        );
        assert_eq!(
            fixture.seen.lock().unwrap().last().unwrap().0,
            "Bearer token-a"
        );
        assert_eq!(
            fixture.bodies.lock().unwrap().last().unwrap().as_ref(),
            history.as_bytes()
        );
        fixture
            .app
            .router
            .bind(fixture.app.router.affinity.key("turn-state:owned-b"), "b")
            .await;
        assert_eq!(
            request(
                &fixture,
                Method::POST,
                "/v1/responses",
                history,
                Some("owned-b")
            )
            .await,
            StatusCode::OK
        );
        assert_eq!(
            fixture.seen.lock().unwrap().last().unwrap().0,
            "Bearer token-b"
        );
    }

    #[tokio::test]
    async fn session_reset_initial_dispatch_does_not_allow_automatic_nonportable_replay() {
        for failure in [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let fixture = fixture(dir.path(), Some(failure)).await;
            assert_eq!(
                request(
                    &fixture,
                    Method::POST,
                    "/v1/responses",
                    r#"{"input":[]}"#,
                    None
                )
                .await,
                StatusCode::OK
            );
            fixture
                .app
                .router
                .set_preferred("default", Some("b".into()))
                .await;
            edit(&fixture, SessionAction::Reset).await;
            let history = r#"{"input":[{"type":"reasoning","id":"rs_original","encrypted_content":"original"}]}"#;
            assert_eq!(
                request(&fixture, Method::POST, "/v1/responses", history, None).await,
                failure
            );
            let seen = fixture.seen.lock().unwrap();
            assert_eq!(
                seen.len(),
                2,
                "nonportable requests cannot retry on another account"
            );
            assert_eq!(seen[1].0, "Bearer token-b");
        }
    }

    #[tokio::test]
    async fn session_move_and_reset_apply_on_a_reused_codex_http_bridge_socket() {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let dir = tempfile::tempdir().unwrap();
        let fixture = fixture(dir.path(), None).await;
        let stream = tokio::net::TcpStream::connect(fixture.address)
            .await
            .unwrap();
        let request = format!("ws://{}/{SECRET}/v1/responses", fixture.address)
            .into_client_request()
            .unwrap();
        let (mut websocket, _) = tokio_tungstenite::client_async(request, stream)
            .await
            .unwrap();
        for action in [
            None,
            Some(SessionAction::Move {
                account: "b".into(),
            }),
            Some(SessionAction::Reset),
        ] {
            if let Some(action) = action {
                edit(&fixture, action).await;
            }
            websocket.send(Message::Text(r#"{"type":"response.create","client_metadata":{"thread_id":"soft-thread"},"input":[{"type":"reasoning","id":"rs_prior","encrypted_content":"prior"}]}"#.into())).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let message = websocket.next().await.unwrap().unwrap();
                    if let Message::Text(text) = message {
                        let event: serde_json::Value = serde_json::from_str(text.as_str()).unwrap();
                        assert_ne!(event["type"], "error", "{event}");
                        if event["type"] == "response.completed" {
                            break;
                        }
                    }
                }
            })
            .await
            .unwrap();
        }
        let accounts: Vec<_> = fixture
            .seen
            .lock()
            .unwrap()
            .iter()
            .map(|seen| seen.0.clone())
            .collect();
        assert_eq!(
            accounts,
            ["Bearer token-a", "Bearer token-b", "Bearer token-a"]
        );
        fixture.app.shutdown_connections().await;
    }
}
