mod session_controls {
    use super::*;
    use crate::routing::sessions::{SessionAction, manage_session};

    async fn edit(harness: &Harness, action: SessionAction) {
        manage_session(
            &harness.app.config,
            &harness.app.router,
            SESSION,
            None,
            action,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn session_move_and_reset_route_claude_text_thinking_and_compaction() {
        for history in [
            json!([{"role":"user","content":"Ada Lovelace says hello."}]),
            json!([{"role":"user","content":"Ada Lovelace says hello."},{"role":"assistant","content":[{"type":"thinking","thinking":"synthetic","signature":"opaque"}]},{"role":"user","content":"continue"}]),
            json!([{"role":"assistant","content":[{"type":"compaction","content":"summary","signature":"opaque"}]},{"role":"user","content":"continue"}]),
        ] {
            let harness = Harness::new(true, StatusCode::OK).await;
            let response = harness.send(request_body(), native_headers()).await;
            assert_eq!(response.status(), StatusCode::OK);
            response.bytes().await.unwrap();
            let mut body: serde_json::Value = serde_json::from_slice(&request_body()).unwrap();
            body["messages"] = history;
            let body = serde_json::to_vec(&body).unwrap();
            edit(
                &harness,
                SessionAction::Move {
                    account: "ada".into(),
                },
            )
            .await;
            for _ in 0..2 {
                let response = harness.send(body.clone(), native_headers()).await;
                assert_eq!(response.status(), StatusCode::OK);
                response.bytes().await.unwrap();
            }
            edit(&harness, SessionAction::Reset).await;
            let response = harness.send(body.clone(), native_headers()).await;
            assert_eq!(response.status(), StatusCode::OK);
            response.bytes().await.unwrap();
            assert_eq!(
                inference_accounts(&harness).await,
                ["grace", "ada", "ada", "grace"]
            );
            assert_eq!(
                harness.seen.lock().await.last().unwrap().1,
                wire::rewrite(&body, ACCOUNT, &"a".repeat(64)).unwrap()
            );
            harness.close().await;
        }
    }

    #[tokio::test]
    async fn session_move_rejects_claude_owned_context_and_keeps_active_answer_alive() {
        let harness = Harness::new(true, StatusCode::OK).await;
        let mut headers = native_headers();
        headers.insert("x-test-hold", "1".parse().unwrap());
        let active = harness.send(request_body(), headers).await;
        assert_eq!(active.status(), StatusCode::OK);
        edit(
            &harness,
            SessionAction::Move {
                account: "ada".into(),
            },
        )
        .await;
        let response = harness.send(request_body(), native_headers()).await;
        assert_eq!(
            response.status(),
            StatusCode::CONFLICT,
            "an overlapping turn stays on the active account"
        );
        harness.release_stream.notify_one();
        assert_eq!(active.bytes().await.unwrap(), STREAM);
        for owned in [
            json!({"thread":{"type":"continue","id":"server-thread"}}),
            json!({"container":"container-id"}),
            json!({"messages":[{"role":"user","content":[{"type":"document","source":{"type":"file","file_id":"file_1"}}]}]}),
        ] {
            let mut body: serde_json::Value = serde_json::from_slice(&request_body()).unwrap();
            for (key, value) in owned.as_object().unwrap() {
                body[key] = value.clone();
            }
            let response = harness
                .send(serde_json::to_vec(&body).unwrap(), native_headers())
                .await;
            assert_eq!(response.status(), StatusCode::CONFLICT);
        }
        assert_eq!(inference_accounts(&harness).await, ["grace"]);
        let response = harness.send(request_body(), native_headers()).await;
        assert_eq!(response.status(), StatusCode::OK);
        response.bytes().await.unwrap();
        assert_eq!(inference_accounts(&harness).await, ["grace", "ada"]);
        harness.close().await;
    }

    #[tokio::test]
    async fn session_claude_manual_target_does_not_fall_back_when_unavailable_or_quota_rejected() {
        let harness = Harness::new(true, StatusCode::OK).await;
        let response = harness.send(request_body(), native_headers()).await;
        response.bytes().await.unwrap();
        edit(
            &harness,
            SessionAction::Move {
                account: "grace".into(),
            },
        )
        .await;
        let mut headers = native_headers();
        headers.insert("x-test-quota", "1".parse().unwrap());
        let response = harness.send(request_body(), headers).await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        response.bytes().await.unwrap();
        let response = harness.send(request_body(), native_headers()).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(inference_accounts(&harness).await, ["grace", "grace"]);
        harness.close().await;
    }
}
