use claude_codex_server::attachments::{Attachment, dispatch};
use serde_json::{Value, json};

fn add(items: &mut Vec<Attachment>, kind: &str, key: &str, payload: Value) -> Value {
    dispatch(
        items,
        "thread/attachment/add",
        &json!({"threadId":"thread-a","attachmentType":kind,"identityKey":key,"payload":payload}),
    )
    .unwrap()
    .response
}

#[test]
fn attachment_identity_survives_persistence_and_does_not_overwrite_payload() {
    let dir = tempfile::tempdir().unwrap();
    let mut items = vec![];
    let first = add(
        &mut items,
        "pull_request",
        "repo/pr/12",
        json!({"url":"https://example.test/repo/pull/12"}),
    );
    let worktree = add(
        &mut items,
        "worktree",
        "repo/pr/12",
        json!({"root":"/tmp/example"}),
    );
    assert_ne!(first["attachment"]["id"], worktree["attachment"]["id"]);
    let path = dir.path().join("attachments.json");
    std::fs::write(&path, serde_json::to_vec(&items).unwrap()).unwrap();
    let mut restored: Vec<Attachment> =
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let duplicate = dispatch(&mut restored, "thread/attachment/add", &json!({"threadId":"thread-a","attachmentType":"pull_request","identityKey":"repo/pr/12","payload":{"url":"changed"}})).unwrap();
    assert_eq!(duplicate.response["outcome"], "existing");
    assert_eq!(duplicate.response["attachment"], first["attachment"]);
    assert!(duplicate.notification.is_none());
    assert_eq!(restored.len(), 2);
}

#[test]
fn removal_is_metadata_only_and_notifications_describe_real_changes() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("user-file.txt");
    std::fs::write(&file, "keep me").unwrap();
    let params = json!({"threadId":"thread-a","attachmentType":"file","identityKey":file,"payload":{"path":file}});
    let mut items = vec![];
    let created = dispatch(&mut items, "thread/attachment/add", &params).unwrap();
    assert_eq!(
        created.notification.as_ref().unwrap()["params"]["operation"],
        "created"
    );
    let remove = json!({"threadId":"thread-a","attachmentType":"file","identityKey":file});
    let deleted = dispatch(&mut items, "thread/attachment/remove", &remove).unwrap();
    assert_eq!(
        deleted.notification.unwrap()["params"]["attachmentId"],
        created.response["attachment"]["id"]
    );
    assert!(
        dispatch(&mut items, "thread/attachment/remove", &remove)
            .unwrap()
            .notification
            .is_none()
    );
    assert_eq!(std::fs::read_to_string(file).unwrap(), "keep me");
}

#[test]
fn pagination_is_stable_after_anchor_removal_and_cursor_is_thread_scoped() {
    let mut items = vec![];
    for key in ["one", "two", "three"] {
        add(&mut items, "worktree", key, Value::Null);
    }
    let first = dispatch(
        &mut items,
        "thread/attachment/list",
        &json!({"threadId":"thread-a","limit":1}),
    )
    .unwrap()
    .response;
    let cursor = &first["nextCursor"];
    let key = &first["data"][0]["identityKey"];
    dispatch(
        &mut items,
        "thread/attachment/remove",
        &json!({"threadId":"thread-a","attachmentType":"worktree","identityKey":key}),
    )
    .unwrap();
    let rest = dispatch(
        &mut items,
        "thread/attachment/list",
        &json!({"threadId":"thread-a","cursor":cursor}),
    )
    .unwrap()
    .response;
    assert_eq!(rest["data"].as_array().unwrap().len(), 2);
    assert!(rest["nextCursor"].is_null());
    for params in [
        json!({"threadId":"thread-b","cursor":cursor}),
        json!({"threadId":"thread-a","cursor":"bad"}),
        json!({"threadId":"thread-a","limit":-1}),
    ] {
        assert!(dispatch(&mut items, "thread/attachment/list", &params).is_err());
    }
}

#[test]
fn arbitrary_payloads_roundtrip_and_responses_match_pinned_schema() {
    let schema: Value = serde_json::from_str(claude_codex_server::protocol::SCHEMA).unwrap();
    let mut items = vec![];
    let params = json!({"threadId":"thread-a","attachmentType":"archived_worktree","identityKey":"/tmp/root","payload":{"worktree":{"root":"/tmp/root","workspaceRoot":"/tmp/root/project"},"pullRequests":[]}});
    for (method, name, params) in [
        (
            "thread/attachment/add",
            "ThreadAttachmentAddResponse",
            params,
        ),
        (
            "thread/attachment/list",
            "ThreadAttachmentListResponse",
            json!({"threadId":"thread-a"}),
        ),
        (
            "thread/attachment/remove",
            "ThreadAttachmentRemoveResponse",
            json!({"threadId":"thread-a","attachmentType":"archived_worktree","identityKey":"/tmp/root"}),
        ),
    ] {
        let result = dispatch(&mut items, method, &params).unwrap();
        let mut response_schema = schema["definitions"]["v2"][name].clone();
        response_schema["definitions"] = schema["definitions"].clone();
        jsonschema::validator_for(&response_schema)
            .unwrap()
            .validate(&result.response)
            .unwrap();
        if let Some(notification) = result.notification {
            let mut notification_schema =
                schema["definitions"]["v2"]["ThreadAttachmentUpdatedNotification"].clone();
            notification_schema["definitions"] = schema["definitions"].clone();
            jsonschema::validator_for(&notification_schema)
                .unwrap()
                .validate(&notification["params"])
                .unwrap();
        }
    }
}
