use claude_codex_server::timeline;
use serde_json::{Value, json};

#[test]
fn timeline_pages_backwards_with_chronological_entries_and_stable_boundaries() {
    let mut turns = vec![
        json!({"id":"one","status":"completed","startedAt":1,"completedAt":2,"durationMs":1000,"items":[{"id":"a","type":"agentMessage","text":"a","phase":null}]}),
        json!({"id":"two","status":"inProgress","startedAt":3,"items":[{"id":"b","type":"agentMessage","text":"b","phase":null}]}),
    ];
    let latest = timeline::list("thread", &turns, &json!({"limit":2})).unwrap();
    assert_eq!(latest["data"][0]["type"], "turnStarted");
    assert_eq!(latest["data"][0]["turnId"], "two");
    assert_eq!(latest["data"][1]["item"]["id"], "b");
    turns[1]["status"] = json!("completed");
    let older = timeline::list(
        "thread",
        &turns,
        &json!({"limit":3,"cursor":latest["nextCursor"]}),
    )
    .unwrap();
    assert_eq!(older["data"].as_array().unwrap().len(), 3);
    assert_eq!(older["data"][2]["type"], "turnCompleted");
    assert_eq!(older["data"][2]["turnId"], "one");
    assert_eq!(older["nextCursor"], Value::Null);
    assert!(timeline::list("other", &turns, &json!({"cursor":latest["nextCursor"]})).is_err());
    assert!(timeline::list("thread", &turns, &json!({"cursor":"invalid"})).is_err());
}
