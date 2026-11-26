use std::path::Path;

use claude_codex_server::translate::{Translator, user_input};
use serde_json::{Value, json};

fn stream(translator: &mut Translator, event: Value) -> Vec<Value> {
    translator
        .receive(&json!({"type": "stream_event", "event": event}))
        .unwrap()
}

fn contracts(events: &[Value]) {
    let root: Value = serde_json::from_str(claude_codex_server::protocol::SCHEMA).unwrap();
    let mut schema = root["definitions"]["ServerNotification"].clone();
    schema["definitions"] = root["definitions"].clone();
    let validator = jsonschema::validator_for(&schema).unwrap();
    for event in events {
        let errors: Vec<String> = validator
            .iter_errors(event)
            .map(|e| e.to_string())
            .collect();
        assert!(errors.is_empty(), "{event}\n{}", errors.join("\n"));
    }
}

#[test]
fn background_task_outlives_launch_ack_and_tracks_native_completion() {
    let mut translator = Translator::new("thread", "turn", Path::new("/tmp"));
    let mut events = translator.tool_start(
        "launch",
        "Agent",
        &json!({"description":"Investigate","run_in_background":true}),
    );
    events.extend(translator.receive(&json!({"type":"system","subtype":"task_started","task_id":"child","tool_use_id":"launch","task_type":"local_agent","description":"Investigate"})).unwrap());
    events.extend(translator.receive(&json!({"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"launch","content":"Agent launched"}]}})).unwrap());
    let items = translator.snapshot();
    assert_eq!(
        items.iter().find(|i| i["id"] == "launch").unwrap()["status"],
        "completed"
    );
    assert_eq!(
        items
            .iter()
            .find(|i| i["id"] == "claude-task-child")
            .unwrap()["status"],
        "inProgress"
    );
    let notification = json!({"type":"system","subtype":"task_notification","task_id":"child","status":"completed","summary":"Investigation complete"});
    events.extend(translator.receive(&notification).unwrap());
    assert!(translator.receive(&notification).unwrap().is_empty());
    let item = translator
        .snapshot()
        .iter()
        .find(|i| i["id"] == "claude-task-child")
        .unwrap();
    assert_eq!(item["status"], "completed");
    assert_eq!(item["success"], true);
    assert_eq!(item["contentItems"][0]["text"], "Investigation complete");
    contracts(&events);
}

#[test]
fn compaction_item_is_in_progress_until_its_boundary() {
    let mut translator = Translator::new("thread", "turn", Path::new("/tmp"));
    let mut events = translator
        .receive(&json!({"type":"system","subtype":"status","status":"compacting"}))
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["method"], "item/started");
    let id = events[0]["params"]["item"]["id"].clone();
    // Claude clears its status before it reports the boundary.
    assert!(
        translator
            .receive(&json!({"type":"system","subtype":"status","status":null}))
            .unwrap()
            .is_empty()
    );
    let boundary = json!({"type":"system","subtype":"compact_boundary",
        "compact_metadata":{"trigger":"auto","pre_tokens":180000,"post_tokens":9000}});
    let completed = translator.receive(&boundary).unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0]["method"], "item/completed");
    assert_eq!(completed[0]["params"]["item"]["id"], id);
    events.extend(completed);
    // A boundary without a reported status is still one complete compaction.
    let alone = translator.receive(&boundary).unwrap();
    assert_eq!(
        alone
            .iter()
            .map(|e| e["method"].clone())
            .collect::<Vec<_>>(),
        ["item/started", "item/completed"]
    );
    assert_ne!(alone[0]["params"]["item"]["id"], id);
    events.extend(alone);
    contracts(&events);
}

#[test]
fn background_failure_and_interruption_do_not_report_success() {
    for status in ["failed", "killed", "stopped"] {
        let mut translator = Translator::new("thread", "turn", Path::new("/tmp"));
        let mut events = translator.receive(&json!({"type":"system","subtype":"task_started","task_id":"child","task_type":"local_workflow","description":"Workflow"})).unwrap();
        events.extend(translator.receive(&json!({"type":"system","subtype":"task_updated","task_id":"child","patch":{"status":status}})).unwrap());
        assert_eq!(translator.snapshot()[0]["status"], "failed");
        assert_eq!(translator.snapshot()[0]["success"], false);
        contracts(&events);
    }
    let mut translator = Translator::new("thread", "turn", Path::new("/tmp"));
    let mut events = translator.receive(&json!({"type":"system","subtype":"task_started","task_id":"child","task_type":"local_agent","description":"Still working"})).unwrap();
    events.extend(translator.finish());
    assert_eq!(translator.snapshot()[0]["status"], "failed");
    contracts(&events);
}

#[test]
fn native_denial_warning_preserves_reason_without_claiming_an_approval_is_pending() {
    for kind in ["classifier", "rule"] {
        let mut translator = Translator::new("thread", "turn", Path::new("/tmp"));
        let events = translator.receive(&json!({"type":"system","subtype":"permission_denied","tool_name":"mcp__fixture__save","decision_reason_type":kind,"decision_reason":"Fixture denied"})).unwrap();
        contracts(&events);
        assert_eq!(events.len(), 1);
        let message = events[0]["params"]["message"].as_str().unwrap();
        assert!(message.contains("Fixture denied") && message.contains("no pending approval"));
        assert_eq!(message.contains("Ask for approval"), kind == "classifier");
        assert_eq!(
            message.contains("explicitly authorize this specific action"),
            kind == "classifier"
        );
    }
}

#[test]
fn disabled_reasoning_omits_streamed_and_final_thinking_but_keeps_answer() {
    let mut translator = Translator::new("thread", "turn", Path::new("/tmp")).with_reasoning(false);
    let mut events = Vec::new();
    for event in [
        json!({"type":"message_start","message":{"id":"message"}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"initial"}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"more"}}),
        json!({"type":"content_block_stop","index":0}),
    ] {
        events.extend(stream(&mut translator, event));
    }
    events.extend(
        translator
            .receive(
                &json!({"type":"assistant","uuid":"final","message":{"id":"message","content":[
                    {"type":"thinking","thinking":"initialmore"},{"type":"text","text":"Answer"}
                ]}}),
            )
            .unwrap(),
    );
    events.extend(translator.finish());
    assert!(
        !events
            .iter()
            .any(|v| v["params"]["item"]["type"] == "reasoning"
                || v["method"].as_str().unwrap().contains("reasoning"))
    );
    assert!(
        events
            .iter()
            .any(|v| v["method"] == "item/completed" && v["params"]["item"]["text"] == "Answer")
    );
    contracts(&events);
}

#[test]
fn split_assistant_frames_do_not_repeat_streamed_reasoning_or_text() {
    let mut translator = Translator::new("thread", "turn", Path::new("/tmp"));
    let mut events = stream(
        &mut translator,
        json!({"type":"message_start","message":{"id":"message"}}),
    );
    for (index, kind, delta_kind, key, text) in [
        (
            0,
            "thinking",
            "thinking_delta",
            "thinking",
            "Reasoning supplied by the test peer",
        ),
        (1, "text", "text_delta", "text", "Hello λ"),
    ] {
        events.extend(stream(
            &mut translator,
            json!({"type":"content_block_start", "index":index, "content_block":{"type":kind}}),
        ));
        events.extend(stream(&mut translator, json!({"type":"content_block_delta", "index":index, "delta":{"type":delta_kind,key:text}})));
        events.extend(stream(
            &mut translator,
            json!({"type":"content_block_stop", "index":index}),
        ));
        // Claude can emit one block at a time; text's final array index is zero,
        // even though its streaming content index was one.
        let final_message = json!({"type":"assistant", "uuid":format!("final-{index}"), "message":{"id":"message","content":[{"type":kind,key:text}]}});
        assert!(translator.receive(&final_message).unwrap().is_empty());
        assert!(translator.receive(&final_message).unwrap().is_empty());
    }
    assert_eq!(
        events
            .iter()
            .filter(|v| v["method"] == "item/started")
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|v| v["method"] == "item/completed")
            .count(),
        2
    );
    assert!(translator.finish().is_empty());
    contracts(&events);
}

#[test]
fn incomplete_stream_is_completed_from_authoritative_assistant_frame() {
    let mut translator = Translator::new("thread", "turn", Path::new("/tmp"));
    let mut events = stream(
        &mut translator,
        json!({"type":"message_start","message":{"id":"m"}}),
    );
    events.extend(stream(
        &mut translator,
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
    ));
    events.extend(stream(
        &mut translator,
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"par"}}),
    ));
    events.extend(translator.receive(&json!({"type":"assistant","uuid":"a","message":{"id":"m","content":[{"type":"text","text":"partial"}]}})).unwrap());
    let text: String = events
        .iter()
        .filter(|e| e["method"] == "item/agentMessage/delta")
        .filter_map(|e| e["params"]["delta"].as_str())
        .collect();
    assert_eq!(text, "partial");
    assert_eq!(events.last().unwrap()["params"]["item"]["text"], "partial");
    contracts(&events);
}

#[test]
fn tool_arguments_results_and_file_changes_have_real_lifecycles() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("test.txt"), "old\n").unwrap();
    let mut translator = Translator::new("thread", "turn", temp.path());
    let mut events = stream(
        &mut translator,
        json!({"type":"message_start","message":{"id":"m"}}),
    );
    events.extend(stream(&mut translator, json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"bash","name":"Bash","input":{}}})));
    for fragment in ["{\"command\":", "\"printf hi\"}"] {
        events.extend(stream(&mut translator, json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":fragment}})));
    }
    events.extend(stream(
        &mut translator,
        json!({"type":"content_block_stop","index":0}),
    ));
    assert_eq!(
        events.last().unwrap()["params"]["item"]["command"],
        "printf hi"
    );
    events.extend(translator.receive(&json!({"type":"user","tool_use_result":{"exitCode":0},"message":{"content":[{"type":"tool_result","tool_use_id":"bash","content":"hi","is_error":false}]}})).unwrap());
    events.extend(translator.tool_start(
        "edit",
        "Edit",
        &json!({"file_path":"test.txt","old_string":"old", "new_string":"new"}),
    ));
    let change = &events.last().unwrap()["params"]["item"]["changes"][0];
    assert!(change["diff"].as_str().unwrap().contains("-old"));
    assert!(change["diff"].as_str().unwrap().contains("+new"));
    events.extend(translator.receive(&json!({"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"edit","content":"denied","is_error":true}]}})).unwrap());
    assert_eq!(events.last().unwrap()["params"]["item"]["status"], "failed");
    events.extend(translator.tool_start("mcp", "mcp__docs__search", &json!({"query":"abc"})));
    events.extend(translator.receive(&json!({"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"mcp","content":[{"type":"text","text":"found"}]}]}})).unwrap());
    contracts(&events);
}

#[test]
fn structured_output_is_a_final_message_and_unfinished_tools_fail() {
    let mut translator = Translator::new("thread", "turn", Path::new("/tmp"));
    let mut events = translator.tool_start("read", "Read", &json!({"file_path":"missing"}));
    events.extend(
        translator
            .receive(&json!({"type":"result","structured_output":{"ok":true}}))
            .unwrap(),
    );
    assert_eq!(
        events.last().unwrap()["params"]["item"]["text"],
        "{\"ok\":true}"
    );
    assert_eq!(
        events.last().unwrap()["params"]["item"]["phase"],
        "final_answer"
    );
    events.extend(translator.finish());
    assert_eq!(events.last().unwrap()["params"]["item"]["status"], "failed");
    contracts(&events);
}

#[tokio::test]
async fn inputs_preserve_images_and_reject_unsupported_media() {
    let (codex, claude) = user_input(&json!([
        {"type":"text","text":"describe"},
        {"type":"image","url":"data:image/png;base64,aGVsbG8="}
    ]))
    .await
    .unwrap();
    assert_eq!(codex[0]["text_elements"], json!([]));
    assert_eq!(claude[1]["source"]["data"], "aGVsbG8=");
    assert!(
        user_input(&json!([{"type":"audio","url":"https://example.com/audio.mp3"}]))
            .await
            .is_err()
    );
    assert!(
        user_input(&json!([{"type":"image","url":"file:///private/file"}]))
            .await
            .is_err()
    );
    assert!(
        user_input(&json!([{"type":"image","url":"data:image/png;base64,invalid!"}]))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn local_images_are_sniffed_and_unreadable_files_are_named() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("Screenshot.png");
    std::fs::write(&path, b"\x89PNG\r\n\x1a\nhello").unwrap();
    let (codex, claude) = user_input(&json!([{"type":"localImage","path":path}]))
        .await
        .unwrap();
    assert_eq!(codex[0]["path"], json!(path));
    assert_eq!(claude[0]["source"]["media_type"], "image/png");
    assert_eq!(claude[0]["source"]["data"], "iVBORw0KGgpoZWxsbw==");

    let missing = root.path().join("gone.png");
    let error = user_input(&json!([{"type":"localImage","path":missing}]))
        .await
        .unwrap_err();
    assert_eq!(error.code, -32602);
    assert!(
        error.message.starts_with(&format!(
            "Cannot read attached image {}:",
            missing.display()
        )),
        "{}",
        error.message
    );
    assert!(
        user_input(&json!([{"type":"localImage","path":"relative.png"}]))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn selected_skill_is_loaded_and_mentions_preserve_identity() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("SKILL.md");
    std::fs::write(&path, "---\nname: probe\n---\nCall the plugin tool.").unwrap();
    let input = json!([{"type":"skill","name":"plugin:probe","path":path},
        {"type":"mention","name":"Connected app","path":"app://connector_id"}]);
    let (saved, native) = user_input(&input).await.unwrap();
    assert_eq!(json!(saved), input);
    assert!(
        native[0]["text"]
            .as_str()
            .unwrap()
            .contains("Call the plugin tool.")
    );
    assert!(
        native[0]["text"]
            .as_str()
            .unwrap()
            .contains(path.to_str().unwrap())
    );
    assert!(
        native[1]["text"]
            .as_str()
            .unwrap()
            .contains("app://connector_id")
    );
    assert!(
        user_input(
            &json!([{"type":"skill","name":"missing","path":root.path().join("missing/SKILL.md")}])
        )
        .await
        .is_err()
    );
}

#[test]
fn visualization_reference_is_normalized_once_after_streaming() {
    let mut translator = Translator::new("thread", "turn", Path::new("/tmp"));
    let raw = "Ready\n\nvisualize{\"path\":\"/tmp/proof.html\"}";
    stream(
        &mut translator,
        json!({"type":"message_start","message":{"id":"m"}}),
    );
    stream(
        &mut translator,
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text"}}),
    );
    for chunk in ["Ready\n\nvisual", "ize{\"path\":\"/tmp/proof.html\"}"] {
        stream(
            &mut translator,
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":chunk}}),
        );
    }
    let events = stream(
        &mut translator,
        json!({"type":"content_block_stop","index":0}),
    );
    assert_eq!(
        events[0]["params"]["item"]["text"],
        "Ready\n\n\u{e200}visualize\u{e202}{\"path\":\"/tmp/proof.html\"}\u{e201}"
    );
    assert!(translator.receive(&json!({"type":"assistant","uuid":"a","message":{"id":"m","content":[{"type":"text","text":raw}]}})).unwrap().is_empty());
    assert_eq!(translator.snapshot().len(), 1);
    contracts(&events);
    for text in [
        "```text\nvisualize{\"path\":\"/tmp/proof.html\"}\n```",
        "visualize{\"path\":\"relative.html\"}",
        "ordinary visualize prose",
    ] {
        let mut translator = Translator::new("thread", "turn", Path::new("/tmp"));
        translator.receive(&json!({"type":"assistant","uuid":"a","message":{"content":[{"type":"text","text":text}]}})).unwrap();
        assert_eq!(translator.snapshot()[0]["text"], text);
    }
}

#[test]
fn subagent_results_complete_only_items_started_by_permission_requests() {
    let mut translator = Translator::new("thread", "turn", Path::new("/tmp"));
    let mut events = translator.tool_start("child-bash", "Bash", &json!({"command":"ls"}));
    events.extend(
        translator
            .receive(
                &json!({"type":"user","parent_tool_use_id":"agent","message":{"content":[
        {"type":"tool_result","tool_use_id":"child-bash","content":"listing"},
        {"type":"tool_result","tool_use_id":"child-read","content":"unrelated"}]}}),
            )
            .unwrap(),
    );
    let items = translator.snapshot();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["status"], "completed");
    assert_eq!(items[0]["aggregatedOutput"], "listing");
    assert!(translator.finish().is_empty());
    contracts(&events);
}

#[test]
fn agent_calls_start_spawn_items_and_are_reported_for_linking() {
    let mut translator = Translator::new("parent", "turn", Path::new("/tmp"));
    let input = json!({"description":"Inspect","prompt":"Look around","model":"haiku"});
    let mut events = translator.tool_start("call", "Agent", &input);
    assert_eq!(translator.take_spawns(), [("call".to_owned(), input)]);
    assert!(translator.take_spawns().is_empty());
    let item = &translator.snapshot()[0];
    assert_eq!(item["type"], "collabAgentToolCall");
    assert_eq!(item["tool"], "spawnAgent");
    assert_eq!(item["senderThreadId"], "parent");
    assert_eq!(item["prompt"], "Look around");
    assert_eq!(item["model"], "haiku");
    events.extend(translator.end(
        "call",
        json!({"status":"completed","receiverThreadIds":["child"],
            "agentsStates":{"child":{"status":"running","message":null}}}),
    ));
    // A later tool result cannot reopen or overwrite the linked spawn.
    events.extend(
        translator
            .receive(&json!({"type":"user","message":{"content":[
        {"type":"tool_result","tool_use_id":"call","content":"report","is_error":true}]}}))
            .unwrap(),
    );
    assert_eq!(translator.snapshot()[0]["status"], "completed");
    assert_eq!(
        translator.snapshot()[0]["receiverThreadIds"],
        json!(["child"])
    );
    contracts(&events);
}
