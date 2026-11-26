use claude_codex_server::plugin_ui;
use serde_json::{Value, json};

#[test]
fn connector_ui_metadata_and_structured_results_survive_history() {
    let descriptor = json!({"annotations":{"readOnlyHint":true},"_meta":{
        "connector_id":"connector_fixture","connector_name":"Fixture","link_id":"opaque-link",
        "plugin_id":"fixture@marketplace","ui":{"resourceUri":"ui://fixture/view","preferredModelDisplayMode":"fullscreen"}}});
    let mut item = plugin_ui::item(
        "call",
        "codex_apps",
        "fixture.show",
        json!({"selected":3}),
        &descriptor,
    );
    let result = json!({"content":[{"type":"text","text":"ok"}],"structuredContent":{"selected":3},"_meta":{"ui":{"resourceUri":"ui://fallback"},"privateState":"retained"}});
    plugin_ui::complete(&mut item, &result, 42);
    assert_eq!(item["appContext"]["connectorId"], "connector_fixture");
    assert_eq!(item["appContext"]["actionName"], "fixture.show");
    assert_eq!(
        item["mcpAppUi"],
        json!({"resourceUri":"ui://fixture/view","preferredModelDisplayMode":"fullscreen"})
    );
    assert_eq!(item["result"]["_meta"], result["_meta"]);
    assert_eq!(
        item["result"]["structuredContent"],
        result["structuredContent"]
    );
    assert_eq!(item["readOnlyHint"], true);
    let root: Value = serde_json::from_str(claude_codex_server::protocol::SCHEMA).unwrap();
    let mut schema = root["definitions"]["v2"]["ThreadItem"].clone();
    schema["definitions"] = root["definitions"].clone();
    let validator = jsonschema::validator_for(&schema).unwrap();
    assert!(
        validator.is_valid(&item),
        "{:?}",
        validator
            .iter_errors(&item)
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
    );
}

#[test]
fn result_template_fallback_and_error_content_are_retained() {
    let mut item = plugin_ui::item("call", "fixture", "show", json!({}), &json!({}));
    let result = json!({"isError":true,"content":[{"type":"text","text":"visible error"}],"structuredContent":false,"_meta":{"openai/outputTemplate":"ui://error"}});
    plugin_ui::complete(&mut item, &result, 1);
    assert_eq!(item["status"], "failed");
    assert_eq!(item["result"]["structuredContent"], false);
    assert_eq!(item["mcpAppUi"]["resourceUri"], "ui://error");
}
