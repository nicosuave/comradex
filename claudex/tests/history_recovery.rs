pub use claude_codex_server::protocol;
#[path = "../src/history_recovery.rs"]
mod history_recovery;

use history_recovery::{HistoryBoundary, NativeAnchor, plan_prefix};
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn turns() -> Vec<Value> {
    ["a", "b", "c"]
        .map(|id| json!({"id": id, "status": "completed"}))
        .to_vec()
}

fn anchor(session: &str, message: &str) -> NativeAnchor {
    NativeAnchor {
        session_id: session.into(),
        message_id: message.into(),
    }
}

#[test]
fn before_and_inclusive_boundaries_select_exact_native_context() {
    let anchors = BTreeMap::from([
        ("a".into(), anchor("original", "native-a")),
        ("b".into(), anchor("branch", "native-b")),
        ("c".into(), anchor("branch", "native-c")),
    ]);
    for boundary in [
        HistoryBoundary::BeforeTurn("c".into()),
        HistoryBoundary::ThroughTurn("b".into()),
        HistoryBoundary::Rollback(1),
    ] {
        let plan = plan_prefix(&turns(), &anchors, None, &boundary).unwrap();
        assert_eq!(plan.retained_len, 2);
        assert_eq!(plan.anchor, Some(anchor("branch", "native-b")));
    }
    let earlier = plan_prefix(
        &turns(),
        &anchors,
        None,
        &HistoryBoundary::BeforeTurn("b".into()),
    )
    .unwrap();
    assert_eq!(earlier.anchor, Some(anchor("original", "native-a")));
}

#[test]
fn clearing_history_never_resumes_removed_native_messages() {
    for boundary in [
        HistoryBoundary::BeforeTurn("a".into()),
        HistoryBoundary::Rollback(3),
        HistoryBoundary::Rollback(u32::MAX),
    ] {
        let plan = plan_prefix(
            &turns(),
            &BTreeMap::new(),
            Some(&anchor("old", "last")),
            &boundary,
        )
        .unwrap();
        assert_eq!(plan.retained_len, 0);
        assert_eq!(plan.anchor, None);
    }
    assert_eq!(
        plan_prefix(&[], &BTreeMap::new(), None, &HistoryBoundary::Full)
            .unwrap()
            .anchor,
        None
    );
}

#[test]
fn legacy_latest_anchor_cannot_fabricate_an_earlier_snapshot() {
    let latest = anchor("old", "latest");
    let plan = plan_prefix(
        &turns(),
        &BTreeMap::new(),
        Some(&latest),
        &HistoryBoundary::Full,
    )
    .unwrap();
    assert_eq!(plan.anchor, Some(latest.clone()));
    assert!(
        plan_prefix(
            &turns(),
            &BTreeMap::new(),
            Some(&latest),
            &HistoryBoundary::Rollback(1)
        )
        .is_err()
    );
}

#[test]
fn invalid_boundaries_and_unstable_anchors_are_rejected() {
    for boundary in [
        HistoryBoundary::Rollback(0),
        HistoryBoundary::BeforeTurn("absent".into()),
        HistoryBoundary::ThroughTurn("absent".into()),
    ] {
        assert!(plan_prefix(&turns(), &BTreeMap::new(), None, &boundary).is_err());
    }
    let mut active = turns();
    active[2]["status"] = json!("inProgress");
    assert!(
        plan_prefix(
            &active,
            &BTreeMap::new(),
            Some(&anchor("s", "m")),
            &HistoryBoundary::ThroughTurn("c".into())
        )
        .is_err()
    );
    assert!(
        plan_prefix(
            &turns(),
            &BTreeMap::new(),
            Some(&anchor("s", "")),
            &HistoryBoundary::Full
        )
        .is_err()
    );
}

#[test]
fn fork_parameters_preserve_inclusive_and_exclusive_contracts() {
    assert_eq!(
        HistoryBoundary::for_fork(&json!({})).unwrap(),
        HistoryBoundary::Full
    );
    assert_eq!(
        HistoryBoundary::for_fork(&json!({"beforeTurnId":"a"})).unwrap(),
        HistoryBoundary::BeforeTurn("a".into())
    );
    assert_eq!(
        HistoryBoundary::for_fork(&json!({"lastTurnId":"a", "beforeTurnId":null})).unwrap(),
        HistoryBoundary::ThroughTurn("a".into())
    );
    for params in [
        json!({"beforeTurnId":"a", "lastTurnId":"b"}),
        json!({"beforeTurnId":""}),
        json!({"lastTurnId":42}),
    ] {
        assert!(HistoryBoundary::for_fork(&params).is_err());
    }
}
