use claude_codex_server::approvals::{Decision, QuestionFlow, SessionGrant, decision};
use serde_json::json;

#[test]
fn session_grant_is_exact_and_never_policy_wins() {
    let input = json!({"command":"git status","timeout":1000});
    let grant = SessionGrant::new("Bash", &input, "/repo");
    let restored: SessionGrant =
        serde_json::from_value(serde_json::to_value(grant).unwrap()).unwrap();
    assert!(restored.permits("Bash", &input, "/repo", "on-request"));
    assert!(!restored.permits("Bash", &input, "/repo", "never"));
    assert!(!restored.permits("Bash", &input, "/other", "on-request"));
    assert!(!restored.permits("Other", &input, "/repo", "on-request"));
    assert!(!restored.permits(
        "Bash",
        &json!({"command":"git status; rm x","timeout":1000}),
        "/repo",
        "on-request"
    ));
    assert!(!restored.permits(
        "Bash",
        &json!({"command":"git status","timeout":2000}),
        "/repo",
        "on-request"
    ));
    let question = SessionGrant::new("AskUserQuestion", &input, "/repo");
    assert!(!question.permits("AskUserQuestion", &input, "/repo", "on-request"));
}

#[test]
fn approval_decisions_fail_closed_for_amendments_and_ambiguous_answers() {
    assert_eq!(
        decision(&json!({"decision":"accept"}), false),
        Decision::Once
    );
    assert_eq!(
        decision(&json!({"decision":"acceptForSession"}), false),
        Decision::Session
    );
    assert_eq!(
        decision(&json!({"decision":"cancel"}), false),
        Decision::Cancel
    );
    assert_eq!(
        decision(
            &json!({"decision":{"acceptWithExecpolicyAmendment":{"execpolicy_amendment":["bash"]}}}),
            false
        ),
        Decision::Deny
    );
    assert_eq!(
        decision(
            &json!({"answers":{"permission":{"answers":["Allow for session"]}}}),
            true
        ),
        Decision::Session
    );
    assert_eq!(
        decision(
            &json!({"answers":{"permission":{"answers":["Allow","Deny"]}}}),
            true
        ),
        Decision::Deny
    );
}

fn question_input() -> serde_json::Value {
    json!({"questions":[
        {"question":"Which features?","header":"Features","multiSelect":true,"options":[{"label":"Search","description":"Find things"},{"label":"Export","description":"Save things"}]},
        {"question":"Which theme?","header":"Theme","multiSelect":false,"options":[{"label":"Light"},{"label":"Dark"}]}
    ],"metadata":"preserved"})
}

#[test]
fn multi_select_uses_native_picker_and_collects_all_questions() {
    let mut flow = QuestionFlow::new(&question_input()).unwrap();
    let (method, params) = flow.next_request().unwrap();
    assert_eq!(method, "item/tool/requestOptionPicker");
    assert_eq!(params["allowMultiple"], true);
    assert_eq!(params["options"][0]["label"], "Search");
    assert!(flow.respond(&json!({"action":"submit","selectedOptions":["Export","Search"],"freeformAnswer":"Custom"})).unwrap().is_none());
    assert_eq!(flow.next_request().unwrap().1["allowMultiple"], false);
    let input = flow
        .respond(&json!({"action":"submit","selectedOptions":["Dark"],"freeformAnswer":null}))
        .unwrap()
        .unwrap();
    assert_eq!(
        input["answers"],
        json!({"Which features?":"Export, Search, Custom","Which theme?":"Dark"})
    );
    assert_eq!(input["metadata"], "preserved");
    assert!(flow.next_request().is_none());
    assert!(
        flow.respond(&json!({"action":"submit","selectedOptions":["Dark"]}))
            .is_err()
    );
}

#[test]
fn malformed_or_cancelled_picker_answers_do_not_advance() {
    let mut flow = QuestionFlow::new(&question_input()).unwrap();
    for reply in [
        json!({"action":"skip","selectedOptions":[]}),
        json!({"action":"dismiss","selectedOptions":[]}),
        json!({"action":"submit","selectedOptions":[]}),
        json!({"action":"submit","selectedOptions":["Unknown"]}),
        json!({"action":"submit","selectedOptions":["Search","Search"]}),
        json!({"action":"submit","selectedOptions":[1]}),
    ] {
        assert!(flow.respond(&reply).is_err());
        assert_eq!(
            flow.next_request().unwrap().1["question"],
            "Which features?"
        );
    }
}

#[test]
fn single_picker_preserves_freeform_alongside_one_selected_option() {
    let mut flow = QuestionFlow::new(&question_input()).unwrap();
    flow.respond(&json!({"action":"submit","selectedOptions":["Search"]}))
        .unwrap();
    assert!(
        flow.respond(&json!({"action":"submit","selectedOptions":["Light","Dark"]}))
            .is_err()
    );
    let result = flow.respond(&json!({"action":"submit","selectedOptions":["Dark"],"freeformAnswer":"With high contrast"})).unwrap().unwrap();
    assert_eq!(
        result["answers"]["Which theme?"],
        "Dark, With high contrast"
    );
}

#[test]
fn single_select_keeps_pinned_protocol_and_validates_cardinality() {
    let mut input = question_input();
    input["questions"][0]["multiSelect"] = json!(false);
    let mut flow = QuestionFlow::new(&input).unwrap();
    assert_eq!(flow.next_request().unwrap().0, "item/tool/requestUserInput");
    assert!(
        flow.respond(
            &json!({"answers":{"q0":{"answers":["Search","Export"]},"q1":{"answers":["Light"]}}})
        )
        .is_err()
    );
    let result = flow
        .respond(&json!({"answers":{"q0":{"answers":["Custom text"]},"q1":{"answers":["Light"]}}}))
        .unwrap()
        .unwrap();
    assert_eq!(result["answers"]["Which features?"], "Custom text");
}

#[test]
fn invalid_native_questions_fail_without_silent_loss() {
    assert!(QuestionFlow::new(&json!({"questions":[]})).is_err());
    let mut input = question_input();
    input["questions"][1]["question"] = input["questions"][0]["question"].clone();
    assert!(QuestionFlow::new(&input).is_err());
}
