//! A Claude Desktop picker entry that runs a local ACP agent for a Messages turn.
use super::*;
use crate::config::ClaudeDesktopAcpConfig;
use claude_codex_server::{
    acp::{AcpBackend, DEFAULT_MODEL},
    backend::{BackendConfig, BackendEvent, BackendProtocol, SessionOptions},
    protocol,
};
use http_body_util::StreamBody;
use tokio::time::Instant as TokioInstant;
use tokio_util::sync::CancellationToken;

const TURN_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
const MAX_REPLY_BYTES: usize = 16 * 1024 * 1024;

impl App {
    pub(super) async fn forward_acp_messages(
        &self,
        agent: &ClaudeDesktopAcpConfig,
        alias: &str,
        message: &Value,
    ) -> Response<ProxyBody> {
        let prompt = match prompt(message) {
            Ok(prompt) => prompt,
            Err(error) => {
                return messages_error(StatusCode::BAD_REQUEST, "invalid_request_error", error);
            }
        };
        let config = BackendConfig {
            executable: agent.executable.clone(),
            extra_args: agent.args.clone(),
            initialize_timeout: Duration::from_secs(60),
            protocol: BackendProtocol::Acp,
        };
        let id = protocol::id();
        let options = SessionOptions {
            cwd: agent.cwd.clone(),
            state_dir: self
                .config
                .proxy
                .state_dir
                .clone()
                .unwrap_or_else(|| ".".into()),
            session_id: id.clone(),
            resume: false,
            fork_from: None,
            resume_at: None,
            model: agent.model.as_deref().unwrap_or(DEFAULT_MODEL).to_owned(),
            permission_mode: "default".into(),
            system_prompt: None,
            append_system_prompt: None,
            effort: None,
            output_schema: None,
            ephemeral: true,
            dynamic_tools: Vec::new(),
            native_settings: agent
                .path
                .as_ref()
                .map_or(Value::Null, |path| json!({"env":{"PATH":path}})),
            sandbox: Default::default(),
        };
        let Ok((backend, events)) =
            AcpBackend::spawn_cancellable(&config, &options, CancellationToken::new()).await
        else {
            return acp_error("could not start the configured ACP agent");
        };
        if backend
            .send(json!({
                "type":"user","uuid":id,"message":{"role":"user","content":prompt},
            }))
            .await
            .is_err()
        {
            return acp_error("could not send a prompt to the configured ACP agent");
        }
        let turn = Turn {
            backend,
            events,
            deadline: TokioInstant::now() + TURN_TIMEOUT,
            output_bytes: 0,
            permission_denied: false,
            message: json!({
                "id":format!("msg_acp_{id}"),"type":"message","role":"assistant","model":alias,
                "content":[],"stop_reason":null,"stop_sequence":null,"usage":zero_usage(),
            }),
        };
        if message["stream"] == true {
            turn.stream()
        } else {
            turn.complete().await
        }
    }
}

type Conversion<T> = std::result::Result<T, &'static str>;

fn prompt(request: &Value) -> Conversion<Vec<Value>> {
    if matches!(
        request["tool_choice"]["type"].as_str(),
        Some("any" | "tool")
    ) {
        return Err("ACP agents use their own tools and cannot force a Claude tool");
    }
    let instructions = match &request["system"] {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        Value::Array(parts) => Some(
            parts
                .iter()
                .map(|part| {
                    part["text"]
                        .as_str()
                        .ok_or("ACP system prompts must contain text")
                })
                .collect::<Conversion<Vec<_>>>()?
                .join("\n\n"),
        ),
        _ => return Err("invalid system prompt"),
    };
    let messages = request["messages"]
        .as_array()
        .ok_or("messages must be an array")?;
    let last_user = messages
        .iter()
        .rposition(|message| message["role"] == "user")
        .ok_or("ACP requires a user message")?;
    let mut content = Vec::new();
    if let Some(instructions) = instructions.filter(|text| !text.trim().is_empty()) {
        content.push(json!({"type":"text","text":format!("[Claude Desktop system instructions]\n{instructions}")}));
    }
    if last_user > 0 {
        content.push(json!({"type":"text","text":"Conversation history follows. Answer the latest user message."}));
    }
    for (index, message) in messages.iter().enumerate() {
        let label = match message["role"].as_str() {
            Some("user") if index == last_user => "Latest user message",
            Some("user") => "Earlier user message",
            Some("assistant") if index < last_user => "Earlier assistant response",
            Some("system") => "System reminder",
            _ => return Err("unsupported message role or assistant prefill for ACP"),
        };
        if messages.len() > 1 {
            content.push(json!({"type":"text","text":format!("[{label}]")}));
        }
        match &message["content"] {
            Value::String(text) => content.push(json!({"type":"text","text":text})),
            Value::Array(parts) => {
                for part in parts {
                    match part["type"].as_str() {
                        Some("text") if part["text"].is_string() => {
                            content.push(json!({"type":"text","text":part["text"]}));
                        }
                        Some("image") if message["role"] == "user" => content.push(part.clone()),
                        Some("tool_use") if message["role"] == "assistant" => {
                            let call = json!({"id":part["id"], "name":part["name"], "input":part["input"]});
                            content.push(
                                json!({"type":"text","text":format!("[Earlier tool call] {call}")}),
                            );
                        }
                        Some("tool_result") if message["role"] == "user" => {
                            append_tool_result(&mut content, part)?;
                        }
                        Some("thinking" | "redacted_thinking")
                            if message["role"] == "assistant" => {}
                        _ => return Err("unsupported Messages content block for ACP"),
                    }
                }
            }
            _ => return Err("ACP message content must be text or content blocks"),
        }
    }
    Ok(content)
}

fn append_tool_result(prompt: &mut Vec<Value>, result: &Value) -> Conversion<()> {
    let id = result["tool_use_id"]
        .as_str()
        .ok_or("ACP transcript tool result has no call ID")?;
    let outcome = if result["is_error"] == true {
        "error"
    } else {
        "success"
    };
    prompt.push(json!({"type":"text","text":format!("[Earlier tool result for {id}: {outcome}]")}));
    match &result["content"] {
        Value::Null => (),
        Value::String(text) => prompt.push(json!({"type":"text","text":text})),
        Value::Array(parts) => {
            for part in parts {
                match part["type"].as_str() {
                    Some("text") if part["text"].is_string() => {
                        prompt.push(json!({"type":"text","text":part["text"]}))
                    }
                    Some("image") => prompt.push(part.clone()),
                    _ => return Err("unsupported tool result content for ACP"),
                }
            }
        }
        _ => return Err("invalid tool result content for ACP"),
    }
    Ok(())
}

enum Reply {
    Text(String),
    End { reason: &'static str, usage: Value },
    Ping,
}

struct Turn {
    backend: AcpBackend,
    events: mpsc::Receiver<BackendEvent>,
    deadline: TokioInstant,
    output_bytes: usize,
    permission_denied: bool,
    message: Value,
}

impl Turn {
    async fn next(&mut self, keepalive: bool) -> Conversion<Reply> {
        let wait_until = if keepalive {
            self.deadline.min(TokioInstant::now() + KEEPALIVE_INTERVAL)
        } else {
            self.deadline
        };
        loop {
            let event = match tokio::time::timeout_at(wait_until, self.events.recv()).await {
                Ok(event) => event,
                Err(_) if wait_until < self.deadline => return Ok(Reply::Ping),
                Err(_) => return Err("ACP agent turn timed out"),
            };
            let Some(BackendEvent::Message(mut frame)) = event else {
                return Err("ACP agent exited before completing the turn");
            };
            match frame["type"].as_str() {
                Some("acp")
                    if frame["update"]["sessionUpdate"] == "agent_message_chunk"
                        && frame["update"]["content"]["type"] == "text" =>
                {
                    let Some(Value::String(text)) = frame["update"]["content"]
                        .as_object_mut()
                        .and_then(|content| content.remove("text"))
                    else {
                        continue;
                    };
                    self.output_bytes = self.output_bytes.saturating_add(text.len());
                    if self.output_bytes > MAX_REPLY_BYTES {
                        return Err("ACP agent reply is too large");
                    }
                    if !text.is_empty() {
                        return Ok(Reply::Text(text));
                    }
                }
                Some("control_request") if frame["request"]["subtype"] == "can_use_tool" => {
                    self.permission_denied = true;
                    self.backend.send(json!({
                        "type":"control_response",
                        "response":{"subtype":"success","request_id":frame["request_id"],
                            "response":{"behavior":"deny","message":"Claude Desktop cannot approve ACP actions through the Messages gateway."}},
                    })).await.map_err(|_| "could not deny the ACP permission request")?;
                }
                Some("result") => {
                    if self.permission_denied {
                        return Err(
                            "ACP agent requested a permission that this gateway cannot grant",
                        );
                    }
                    let reason = match frame["acp_stop_reason"].as_str() {
                        Some("end_turn") => "end_turn",
                        Some("max_tokens") => "max_tokens",
                        Some("refusal") => "refusal",
                        _ => return Err("ACP agent could not complete the turn"),
                    };
                    let usage = frame
                        .get_mut("usage")
                        .map(Value::take)
                        .filter(Value::is_object)
                        .unwrap_or_else(zero_usage);
                    return Ok(Reply::End { reason, usage });
                }
                _ => (),
            }
        }
    }

    async fn complete(mut self) -> Response<ProxyBody> {
        let mut text = String::new();
        let result = loop {
            match self.next(false).await {
                Ok(Reply::Text(delta)) => text.push_str(&delta),
                result => break result,
            }
        };
        let _ = self.backend.terminate().await;
        match result {
            Ok(Reply::End { reason, usage }) => {
                self.message["content"] = json!([{"type":"text","text":text}]);
                self.message["stop_reason"] = json!(reason);
                self.message["usage"] = usage;
                Response::builder()
                    .header(CONTENT_TYPE, "application/json")
                    .body(json_body(self.message))
                    .expect("static Messages headers")
            }
            Err(error) => acp_error(error),
            Ok(Reply::Text(_) | Reply::Ping) => unreachable!("only completion ends collection"),
        }
    }

    fn stream(self) -> Response<ProxyBody> {
        let mut start = String::new();
        emit(
            &mut start,
            json!({"type":"message_start","message":self.message}),
        );
        emit(
            &mut start,
            json!({"type":"content_block_start","index":0,
            "content_block":{"type":"text","text":""}}),
        );
        let updates = futures_util::stream::unfold(Some(self), |turn| async move {
            let mut turn = turn?;
            let mut out = String::new();
            let finished = match turn.next(true).await {
                Ok(Reply::Text(text)) => {
                    emit(
                        &mut out,
                        json!({"type":"content_block_delta","index":0,
                        "delta":{"type":"text_delta","text":text}}),
                    );
                    false
                }
                Ok(Reply::End { reason, usage }) => {
                    emit(&mut out, json!({"type":"content_block_stop","index":0}));
                    emit(
                        &mut out,
                        json!({"type":"message_delta",
                        "delta":{"stop_reason":reason,"stop_sequence":null},"usage":usage}),
                    );
                    emit(&mut out, json!({"type":"message_stop"}));
                    true
                }
                Err(error) => {
                    emit(&mut out, error_event("api_error", error));
                    true
                }
                Ok(Reply::Ping) => {
                    emit(&mut out, json!({"type":"ping"}));
                    false
                }
            };
            if finished {
                let _ = turn.backend.terminate().await;
            }
            Some((
                Ok(Frame::data(Bytes::from(out))),
                (!finished).then_some(turn),
            ))
        });
        let stream = futures_util::stream::once(async move { Ok(Frame::data(Bytes::from(start))) })
            .chain(updates);
        Response::builder()
            .header(CONTENT_TYPE, "text/event-stream")
            .header("cache-control", "no-cache")
            .body(BodyExt::boxed(StreamBody::new(stream)))
            .expect("static Messages stream headers")
    }
}

fn acp_error(message: &str) -> Response<ProxyBody> {
    messages_error(StatusCode::BAD_GATEWAY, "api_error", message)
}

fn zero_usage() -> Value {
    json!({"input_tokens":0,"output_tokens":0,"cache_read_input_tokens":0,"cache_creation_input_tokens":0})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn followup_prompt_includes_the_host_transcript_and_keeps_the_latest_request() {
        let request = json!({
            "system":[{"type":"text","text":"Work on the fixture."}],
            "messages":[
                {"role":"user","content":"The record is 12."},
                {"role":"assistant","content":[{"type":"text","text":"I found it."}]},
                {"role":"system","content":[{"type":"text","text":"Use the prior context."}]},
                {"role":"user","content":"Read the record again."},
            ],
        });
        let prompt = prompt(&request).unwrap();
        let text: Vec<_> = prompt
            .iter()
            .map(|block| block["text"].as_str().unwrap())
            .collect();
        assert_eq!(
            text,
            [
                "[Claude Desktop system instructions]\nWork on the fixture.",
                "Conversation history follows. Answer the latest user message.",
                "[Earlier user message]",
                "The record is 12.",
                "[Earlier assistant response]",
                "I found it.",
                "[System reminder]",
                "Use the prior context.",
                "[Latest user message]",
                "Read the record again.",
            ]
        );
    }

    #[test]
    fn prior_claude_tool_calls_are_sent_as_context() {
        let request = json!({"messages":[
            {"role":"user","content":"Open the record."},
            {"role":"assistant","content":[
                {"type":"tool_use","id":"toolu_fixture","name":"read","input":{"id":12}}
            ]},
            {"role":"user","content":[
                {"type":"tool_result","tool_use_id":"toolu_fixture","content":"Record 12 is open."},
                {"type":"text","text":"Describe it."}
            ]},
        ]});
        let prompt = prompt(&request).unwrap();
        assert!(prompt.iter().all(|block| block["type"] == "text"));
        assert!(prompt.iter().any(|block| {
            block["text"]
                .as_str()
                .unwrap()
                .starts_with("[Earlier tool call]")
        }));
        assert!(
            prompt
                .iter()
                .any(|block| block["text"] == "Record 12 is open.")
        );
        assert_eq!(prompt.last().unwrap()["text"], "Describe it.");
    }
}
