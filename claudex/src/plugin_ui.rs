//! Preserve MCP app descriptors on timeline items so desktop history can render
//! native plugin results after the turn's private transport has shut down.
use serde_json::{Value, json};

fn resource_uri(meta: &Value) -> Option<&str> {
    meta["ui"]["resourceUri"]
        .as_str()
        .or_else(|| meta["ui/resourceUri"].as_str())
        .or_else(|| meta["openai/outputTemplate"].as_str())
}

pub fn item(id: &str, server: &str, tool: &str, arguments: Value, descriptor: &Value) -> Value {
    let meta = &descriptor["_meta"];
    let resource = resource_uri(meta);
    let app_context = meta["connector_id"].as_str().map(|connector| {
        json!({
            "connectorId":connector,"appName":meta["connector_name"],"actionName":tool,
            "linkId":meta["link_id"],"resourceUri":resource,
        })
    });
    let ui = resource.map(|uri| {
        json!({"resourceUri":uri,"preferredModelDisplayMode":
        if meta["ui"]["preferredModelDisplayMode"] == "fullscreen" {"fullscreen"} else {"inline"}})
    });
    json!({"type":"mcpToolCall","id":id,"server":server,"tool":tool,"arguments":arguments,
        "status":"inProgress","appContext":app_context,"mcpAppUi":ui,
        "pluginId":meta["plugin_id"],"readOnlyHint":descriptor["annotations"]["readOnlyHint"],
        "result":null,"error":null,"durationMs":null})
}

pub fn complete(item: &mut Value, result: &Value, duration: u64) {
    item["status"] = json!(if result["isError"] == true {
        "failed"
    } else {
        "completed"
    });
    item["durationMs"] = json!(duration);
    item["result"] = json!({"content":result["content"],"structuredContent":result["structuredContent"],"_meta":result["_meta"]});
    if result["isError"] == true {
        item["error"] = json!({"message":"Codex plugin tool returned an error"});
    }
    if item["mcpAppUi"].is_null()
        && let Some(uri) = resource_uri(&result["_meta"])
    {
        item["mcpAppUi"] = json!({"resourceUri":uri,"preferredModelDisplayMode":"inline"});
        if item["appContext"].is_object() {
            item["appContext"]["resourceUri"] = json!(uri);
        }
    }
}
