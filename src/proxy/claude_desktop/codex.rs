//! Translate Claude's inference requests while leaving the tool loop in Claude.
use super::*;
use crate::proxy::replay_body::ReplayLimit;
use hyper::header::RETRY_AFTER;
use std::collections::BTreeMap;

mod output;

impl App {
    pub(super) async fn forward_codex_messages(
        &self,
        listener: &ListenerConfig,
        alias: &str,
        model: &str,
        inbound: &HeaderMap,
        message: &Value,
    ) -> Response<ProxyBody> {
        let mut turn = match responses_request(message, model) {
            Ok(turn) => turn,
            Err(error) => {
                return messages_error(StatusCode::BAD_REQUEST, "invalid_request_error", error);
            }
        };
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        headers.insert(ACCEPT, "text/event-stream".parse().unwrap());
        if let Some(session) = session_key(inbound, message, &listener.pool) {
            headers.insert("session_id", session.parse().expect("hex session key"));
            turn.body["prompt_cache_key"] = json!(session);
        }
        let replay = match ReplayBody::from_bytes(
            Bytes::from(turn.body.to_string()),
            self.config.proxy.max_request_bytes,
            self.config.proxy.max_spool_bytes,
            self.stats.clone(),
        ) {
            Ok(replay) => replay,
            Err(error) => {
                return if matches!(
                    error.downcast_ref::<ReplayLimit>(),
                    Some(ReplayLimit::Request)
                ) {
                    messages_error(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "invalid_request_error",
                        "translated request is too large",
                    )
                } else {
                    messages_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "overloaded_error",
                        "gateway request capacity reached",
                    )
                };
            }
        };
        let response = self
            .handle_http_replay(
                headers,
                Method::POST,
                listener,
                "/responses".into(),
                replay,
                ServingLane::Http,
            )
            .await;
        let Ok(response) = response else {
            return messages_error(
                StatusCode::BAD_GATEWAY,
                "api_error",
                "Codex inference request failed",
            );
        };
        if !response.status().is_success() {
            return output::http_error(response).await;
        }
        output::translate(
            response.into_body(),
            alias.to_owned(),
            turn.tool_names,
            message["stream"] == true,
        )
        .await
    }
}

fn session_key(headers: &HeaderMap, message: &Value, pool: &str) -> Option<String> {
    let metadata = &message["metadata"]["user_id"];
    let parsed_metadata = metadata
        .as_str()
        .and_then(|value| serde_json::from_str(value).ok());
    let session = headers
        .get("x-claude-code-session-id")
        .and_then(|value| value.to_str().ok())
        .or_else(|| message["metadata"]["session_id"].as_str())
        .or_else(|| {
            parsed_metadata
                .as_ref()
                .and_then(|value: &Value| value["session_id"].as_str())
        })
        .filter(|value| !value.is_empty() && value.len() <= 256)?;
    let mut hash = blake3::Hasher::new_derive_key("comradex claude desktop codex session");
    hash.update(&(pool.len() as u64).to_le_bytes());
    hash.update(pool.as_bytes());
    hash.update(session.as_bytes());
    Some(hash.finalize().to_hex().to_string())
}

type Translation<T> = std::result::Result<T, &'static str>;

struct Turn {
    body: Value,
    tool_names: BTreeMap<String, String>,
}

fn responses_request(message: &Value, model: &str) -> Translation<Turn> {
    let messages = message["messages"]
        .as_array()
        .ok_or("messages must be an array")?;
    let mut input = Vec::new();
    for message in messages {
        append_message(&mut input, message)?;
    }
    let instructions = match &message["system"] {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        Value::Array(blocks) => blocks
            .iter()
            .map(|part| {
                if part["type"] != "text" {
                    return Err("system supports only text blocks");
                }
                text(part, "text")
            })
            .collect::<Translation<Vec<_>>>()?
            .join("\n\n"),
        _ => return Err("invalid system prompt"),
    };
    let mut tools = Vec::new();
    let mut tool_names = BTreeMap::new();
    if !message["tools"].is_null() {
        for tool in message["tools"]
            .as_array()
            .ok_or("tools must be an array")?
        {
            let name = text(tool, "name")?;
            let native = tool_name(name);
            if !tool["input_schema"].is_object() || tool["defer_loading"] == true {
                return Err("Codex inference requires a local tool with an inline input schema");
            }
            if tool_names.insert(native.clone(), name.to_owned()).is_some() {
                return Err("tool names must be unique");
            }
            tools.push(json!({
                "type":"function", "name":native, "description":tool["description"].as_str().unwrap_or_default(),
                "parameters":tool["input_schema"], "strict":false,
            }));
        }
    }
    let choice = &message["tool_choice"];
    let tool_choice = match choice["type"].as_str() {
        None if choice.is_null() => "auto",
        Some("auto") => "auto",
        Some("none") => "none",
        Some("any") if !tools.is_empty() => "required",
        Some("tool") => {
            let name = tool_name(text(choice, "name")?);
            tools.retain(|tool| tool["name"] == name);
            if tools.is_empty() {
                return Err("tool_choice names a tool that is not defined");
            }
            "required"
        }
        _ => return Err("invalid tool_choice"),
    };
    if !message["mcp_servers"].is_null()
        && message["mcp_servers"]
            .as_array()
            .is_none_or(|servers| !servers.is_empty())
    {
        return Err("Codex inference supports tools executed by the client");
    }
    let mut body = json!({
        "model":model, "instructions":instructions, "input":input,
        "tools":tools, "tool_choice":tool_choice, "parallel_tool_calls":false,
        "store":false, "stream":true,
    });
    let format = &message["output_config"]["format"];
    if !format.is_null() {
        if format["type"] != "json_schema" || !format["schema"].is_object() {
            return Err("unsupported output format");
        }
        body["text"] = json!({"format":{
            "type":"json_schema","name":"claude_output","strict":false,"schema":format["schema"]
        }});
    }
    Ok(Turn { body, tool_names })
}

fn text<'a>(value: &'a Value, key: &str) -> Translation<&'a str> {
    value[key]
        .as_str()
        .ok_or("missing or invalid string in Messages request")
}

fn tool_name(name: &str) -> String {
    const HASHED_PREFIX: &str = "claude_tool_";
    if !name.is_empty()
        && name.len() <= 64
        && !name.starts_with(HASHED_PREFIX)
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        name.to_owned()
    } else {
        let digest = blake3::hash(name.as_bytes()).to_hex();
        format!("{HASHED_PREFIX}{}", &digest[..40])
    }
}

fn append_message(input: &mut Vec<Value>, message: &Value) -> Translation<()> {
    let role = text(message, "role")?;
    let text_type = match role {
        "assistant" => "output_text",
        "user" | "system" => "input_text",
        _ => return Err("unsupported message role"),
    };
    let mut content = Vec::new();
    let flush = |input: &mut Vec<Value>, content: &mut Vec<Value>| {
        if !content.is_empty() {
            input.push(json!({"type":"message", "role":role, "content":std::mem::take(content)}));
        }
    };
    if let Some(value) = message["content"].as_str() {
        content.push(json!({"type":text_type, "text":value}));
    } else {
        for block in message["content"]
            .as_array()
            .ok_or("message content must be text or blocks")?
        {
            match block["type"].as_str() {
                Some("text") => {
                    content.push(json!({"type":text_type, "text":text(block, "text")?}))
                }
                Some("image") if role == "user" => content.push(image(block)?),
                Some("thinking" | "redacted_thinking") if role == "assistant" => (),
                Some("tool_use") if role == "assistant" && block["input"].is_object() => {
                    flush(input, &mut content);
                    input.push(json!({
                        "type":"function_call", "call_id":text(block, "id")?,
                        "name":tool_name(text(block, "name")?), "arguments":block["input"].to_string(),
                    }));
                }
                Some("tool_result") if role == "user" => {
                    flush(input, &mut content);
                    input.push(json!({
                        "type":"function_call_output", "call_id":text(block, "tool_use_id")?,
                        "output":tool_result(&block["content"], block["is_error"] == true)?,
                    }));
                }
                _ => return Err("unsupported Messages content block for Codex inference"),
            }
        }
    }
    flush(input, &mut content);
    Ok(())
}

fn image(block: &Value) -> Translation<Value> {
    let source = &block["source"];
    let url = match source["type"].as_str() {
        Some("base64") => {
            let mime = text(source, "media_type")?;
            if !matches!(
                mime,
                "image/jpeg" | "image/png" | "image/gif" | "image/webp"
            ) {
                return Err("unsupported image media type");
            }
            format!("data:{mime};base64,{}", text(source, "data")?)
        }
        Some("url") => text(source, "url")?.to_owned(),
        _ => return Err("unsupported image source"),
    };
    Ok(json!({"type":"input_image", "image_url":url}))
}

fn tool_result(result: &Value, failed: bool) -> Translation<Value> {
    let prefix = if failed {
        "Tool returned an error.\n"
    } else {
        ""
    };
    match result {
        Value::Null => Ok(json!(prefix)),
        Value::String(value) => Ok(json!(format!("{prefix}{value}"))),
        Value::Array(blocks) => {
            let mut translated = Vec::new();
            if failed {
                translated.push(json!({"type":"input_text", "text":prefix}));
            }
            for block in blocks {
                translated.push(match block["type"].as_str() {
                    Some("text") => json!({"type":"input_text", "text":text(block, "text")?}),
                    Some("image") => image(block)?,
                    _ => return Err("unsupported tool result content"),
                });
            }
            Ok(json!(translated))
        }
        _ => Err("invalid tool result content"),
    }
}
