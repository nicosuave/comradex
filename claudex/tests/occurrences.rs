use claude_codex_server::occurrences;
use serde_json::{Value, json};

fn search(turns: &[Value], term: &str) -> Vec<(String, String, Value)> {
    let page = occurrences::search("thread", turns, &json!({"searchTerm": term}), |turn| {
        format!("cursor:{turn}")
    })
    .unwrap();
    page["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|occurrence| {
            assert_eq!(
                occurrence["turnCursor"],
                format!("cursor:{}", occurrence["turnId"].as_str().unwrap())
            );
            (
                occurrence["itemId"].as_str().unwrap().to_owned(),
                occurrence["snippet"].as_str().unwrap().to_owned(),
                occurrence["snippetMatchRange"].clone(),
            )
        })
        .collect()
}

fn user(id: &str, text: &str) -> Value {
    json!({"type":"userMessage","id":id,"clientId":null,
        "content":[{"type":"text","text":text,"text_elements":[]}]})
}

fn agent(id: &str, text: &str, phase: Value) -> Value {
    json!({"type":"agentMessage","id":id,"text":text,"phase":phase})
}

#[test]
fn only_user_messages_and_each_final_answer_are_searched() {
    let turns = vec![
        json!({"id":"done","status":"completed","items":[
            user("question", "## My request for Codex: find the word"),
            agent("draft", "word in an earlier message", Value::Null),
            json!({"type":"reasoning","id":"thinking","summary":["word"],"content":[]}),
            agent("answer", "the   word\nhere", Value::Null),
        ]}),
        json!({"id":"phased","status":"completed","items":[
            agent("final", "final word", json!("final_answer")),
            agent("later", "later word", Value::Null),
        ]}),
        json!({"id":"running","status":"inProgress","items":[
            user("pending", "a word"),
            agent("partial", "partial word", Value::Null),
        ]}),
    ];
    assert_eq!(
        search(&turns, "WORD"),
        [
            (
                "question".to_owned(),
                "find the word".to_owned(),
                json!({"start":9,"end":13})
            ),
            (
                "answer".to_owned(),
                "the word here".to_owned(),
                json!({"start":4,"end":8})
            ),
            (
                "final".to_owned(),
                "final word".to_owned(),
                json!({"start":6,"end":10})
            ),
            (
                "pending".to_owned(),
                "a word".to_owned(),
                json!({"start":2,"end":6})
            ),
        ]
    );
    // Codex's request marker, and anything before it, is not searched.
    assert!(search(&turns, "request for").is_empty());
}

#[test]
fn matches_map_back_through_case_folding_that_changes_length() {
    // U+0130 lowercases to two characters; the range still covers the original.
    let turns = vec![json!({"id":"turn","status":"completed","items":[
        user("message", "\u{130}stanbul and istanbul")
    ]})];
    assert_eq!(
        search(&turns, "i\u{307}stanbul"),
        [(
            "message".to_owned(),
            "\u{130}stanbul and istanbul".to_owned(),
            json!({"start":0,"end":8})
        )]
    );
    assert_eq!(search(&turns, "istanbul").len(), 1);
}

#[test]
fn cursors_bind_to_their_thread_and_term() {
    let turns = vec![json!({"id":"turn","status":"completed","items":[
        user("message", "one one one")
    ]})];
    let cursor = |_: &str| String::new();
    let first = occurrences::search(
        "thread",
        &turns,
        &json!({"searchTerm":"one","limit":2}),
        cursor,
    )
    .unwrap();
    assert_eq!(first["data"].as_array().unwrap().len(), 2);
    let rest = occurrences::search(
        "thread",
        &turns,
        &json!({"searchTerm":"one","cursor":first["nextCursor"]}),
        cursor,
    )
    .unwrap();
    assert_eq!(
        rest["data"][0]["snippetMatchRange"],
        json!({"start":8,"end":11})
    );
    assert!(rest["nextCursor"].is_null());
    for (thread, term) in [("other", "one"), ("thread", "two")] {
        let params = json!({"searchTerm":term,"cursor":first["nextCursor"]});
        assert!(occurrences::search(thread, &turns, &params, cursor).is_err());
    }
}
