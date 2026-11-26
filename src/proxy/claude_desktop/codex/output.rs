use super::*;
use crate::proxy::{
    sse::{DEFAULT_MAX_EVENT_BYTES, TerminalStatus},
    websocket_protocol::classify_failure,
};
use http_body_util::StreamBody;

struct Wire {
    body: ProxyBody,
    decoder: SseDecoder,
}

impl Wire {
    async fn next(&mut self) -> Result<Vec<ProtocolEvent>> {
        while let Some(frame) = self.body.frame().await {
            if let Ok(data) = frame?.into_data() {
                let events = self.decoder.push(&data)?;
                if !events.is_empty() {
                    return Ok(events);
                }
            }
        }
        Ok(self.decoder.finish()?)
    }
}

pub(super) async fn translate(
    body: ProxyBody,
    alias: String,
    tools: BTreeMap<String, String>,
    stream: bool,
) -> Response<ProxyBody> {
    let mut wire = Wire {
        body,
        decoder: SseDecoder::default(),
    };
    if !stream {
        let mut finished_items = Vec::new();
        let mut finished_bytes = 0usize;
        let result = 'response: loop {
            let Ok(events) = wire.next().await else {
                break Err("Codex stream ended before completion");
            };
            if events.is_empty() {
                break Err("Codex stream ended before completion");
            }
            for mut event in events {
                if event.terminal == Some(TerminalStatus::Failed) {
                    let (status, kind, message) =
                        inference_error(&event.value, StatusCode::BAD_GATEWAY);
                    return messages_error(status, kind, message);
                }
                if event.terminal.is_some() {
                    break 'response complete_message(
                        &alias,
                        &event.value["response"],
                        &finished_items,
                        &tools,
                    );
                }
                // Codex can send the completed items only in item events.
                if event.event_type == "response.output_item.done"
                    && matches!(
                        event.value["item"]["type"].as_str(),
                        Some("message" | "function_call")
                    )
                {
                    finished_bytes += event.payload.len();
                    if finished_bytes > DEFAULT_MAX_EVENT_BYTES {
                        break 'response Err("Codex reply is too large");
                    }
                    finished_items.push(event.value["item"].take());
                }
            }
        };
        return match result {
            Ok(message) => Response::builder()
                .header(CONTENT_TYPE, "application/json")
                .body(json_body(message))
                .expect("static Messages response headers"),
            Err(error) => messages_error(StatusCode::BAD_GATEWAY, "api_error", error),
        };
    }

    let frames = futures_util::stream::unfold(
        (wire, MessagesStream::new(alias, tools), false),
        |(mut wire, mut stream, finished)| async move {
            if finished {
                return None;
            }
            let mut out = String::new();
            let done = loop {
                match tokio::time::timeout(Duration::from_secs(15), wire.next()).await {
                    Ok(Ok(events)) if !events.is_empty() => {
                        let result = events
                            .iter()
                            .try_for_each(|event| stream.translate(event, &mut out));
                        if let Err(message) = result {
                            emit(&mut out, error_event("api_error", message));
                            break true;
                        }
                        if wire.decoder.is_terminal() || !out.is_empty() {
                            break wire.decoder.is_terminal();
                        }
                    }
                    Ok(_) => {
                        emit(
                            &mut out,
                            error_event("api_error", "Codex stream ended before completion"),
                        );
                        break true;
                    }
                    Err(_) => {
                        emit(&mut out, json!({"type":"ping"}));
                        break false;
                    }
                }
            };
            Some((Ok(Frame::data(Bytes::from(out))), (wire, stream, done)))
        },
    );
    Response::builder()
        .header(CONTENT_TYPE, "text/event-stream")
        .header("cache-control", "no-cache")
        .body(BodyExt::boxed(StreamBody::new(frames)))
        .expect("static Messages stream headers")
}

pub(super) async fn http_error(upstream: Response<ProxyBody>) -> Response<ProxyBody> {
    let (parts, body) = upstream.into_parts();
    let value = match tokio::time::timeout(
        Duration::from_secs(5),
        Limited::new(body, 64 * 1024).collect(),
    )
    .await
    {
        Ok(Ok(bytes)) => serde_json::from_slice(&bytes.to_bytes()).unwrap_or(Value::Null),
        _ => Value::Null,
    };
    let (status, kind, message) = inference_error(&value, parts.status);
    let mut response = messages_error(status, kind, message);
    if let Some(value) = parts.headers.get(RETRY_AFTER) {
        response.headers_mut().insert(RETRY_AFTER, value.clone());
    }
    response
}

fn inference_error(value: &Value, status: StatusCode) -> (StatusCode, &'static str, &'static str) {
    let failure = classify_failure(value);
    match (status, failure.kind, failure.code.as_deref()) {
        (_, _, Some("context_length_exceeded" | "context_window_exceeded")) => (
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "prompt is too long",
        ),
        (StatusCode::TOO_MANY_REQUESTS, _, _)
        | (_, FailureKind::Quota | FailureKind::ScopedQuota, _) => (
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "Codex inference rate limit reached",
        ),
        (StatusCode::SERVICE_UNAVAILABLE, _, _) | (_, FailureKind::Capacity, _) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "overloaded_error",
            "Codex inference is unavailable",
        ),
        (
            StatusCode::BAD_REQUEST
            | StatusCode::UNPROCESSABLE_ENTITY
            | StatusCode::PAYLOAD_TOO_LARGE,
            _,
            _,
        ) => (
            status,
            "invalid_request_error",
            "Codex rejected the inference request",
        ),
        (StatusCode::NOT_FOUND, _, _) => (
            status,
            "not_found_error",
            "configured Codex model is unavailable",
        ),
        _ => (
            StatusCode::BAD_GATEWAY,
            "api_error",
            "Codex inference request failed",
        ),
    }
}

fn usage(response: &Value) -> Value {
    let usage = &response["usage"];
    let input = usage["input_tokens"].as_u64().unwrap_or_default();
    let cached = usage["input_tokens_details"]["cached_tokens"]
        .as_u64()
        .unwrap_or_default();
    json!({
        "input_tokens": input.saturating_sub(cached),
        "cache_read_input_tokens":cached,
        "cache_creation_input_tokens":0,
        "output_tokens":usage["output_tokens"].as_u64().unwrap_or_default(),
    })
}

fn message_shell(alias: &str, response: &Value) -> Translation<Value> {
    let id = response["id"]
        .as_str()
        .ok_or("Codex response has no message ID")?;
    let mut usage = usage(response);
    usage["output_tokens"] = json!(0);
    Ok(json!({
        "id":id, "type":"message", "role":"assistant", "model":alias,
        "content":[], "stop_reason":null, "stop_sequence":null, "usage":usage,
    }))
}

fn stop_reason(response: &Value, has_tool: bool, has_refusal: bool) -> &'static str {
    match response["incomplete_details"]["reason"].as_str() {
        Some("max_output_tokens") => "max_tokens",
        Some("content_filter") => "refusal",
        _ if has_refusal => "refusal",
        _ if has_tool => "tool_use",
        _ => "end_turn",
    }
}

fn complete_message(
    alias: &str,
    response: &Value,
    finished_items: &[Value],
    tools: &BTreeMap<String, String>,
) -> Translation<Value> {
    let mut message = message_shell(alias, response)?;
    let mut content = Vec::new();
    let mut has_tool = false;
    let output = response["output"]
        .as_array()
        .filter(|output| !output.is_empty())
        .map_or(finished_items, Vec::as_slice);
    for item in output {
        if incomplete_tool(item) {
            continue;
        }
        for (_, mut block, value) in item_blocks(item, tools)? {
            if block["type"] == "tool_use" {
                block["input"] = parse_arguments(value)?;
                has_tool = true;
            } else {
                block["text"] = json!(value);
            }
            content.push(block);
        }
    }
    message["content"] = json!(content);
    message["stop_reason"] = json!(stop_reason(
        response,
        has_tool,
        output.iter().any(item_has_refusal)
    ));
    message["usage"] = usage(response);
    Ok(message)
}

fn incomplete_tool(item: &Value) -> bool {
    item["type"] == "function_call"
        && matches!(item["status"].as_str(), Some("incomplete" | "in_progress"))
}

fn tool_block(item: &Value, tools: &BTreeMap<String, String>) -> Translation<Value> {
    let name = item["name"]
        .as_str()
        .and_then(|name| tools.get(name))
        .ok_or("Codex returned an undefined tool")?;
    let id = item["call_id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or("Codex tool call is missing its ID")?;
    Ok(json!({"type":"tool_use","id":id,"name":name,"input":{}}))
}

fn text_block() -> Value {
    json!({"type":"text","text":""})
}

// A function call is an output item, while an output message may contain
// multiple text parts. Keep their keys distinct even for the same item index.
type BlockKey = (u64, Option<u64>);

fn item_blocks<'a>(
    item: &'a Value,
    tools: &BTreeMap<String, String>,
) -> Translation<Vec<(Option<u64>, Value, &'a str)>> {
    Ok(match item["type"].as_str() {
        Some("function_call") => vec![(
            None,
            tool_block(item, tools)?,
            item["arguments"].as_str().unwrap_or(""),
        )],
        Some("message") => item["content"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
            .filter_map(|(index, part)| Some((Some(index as u64), text_block(), part_text(part)?)))
            .collect(),
        _ => Vec::new(),
    })
}

fn part_text(part: &Value) -> Option<&str> {
    match part["type"].as_str()? {
        "output_text" => part["text"].as_str(),
        "refusal" => part["refusal"].as_str(),
        _ => None,
    }
}

fn item_has_refusal(item: &Value) -> bool {
    item["type"] == "message"
        && item["content"]
            .as_array()
            .is_some_and(|parts| parts.iter().any(|part| part["type"] == "refusal"))
}

fn parse_arguments(value: &str) -> Translation<Value> {
    serde_json::from_str::<Value>(value)
        .ok()
        .filter(Value::is_object)
        .ok_or("Codex returned invalid tool arguments")
}

struct MessagesStream {
    alias: String,
    tools: BTreeMap<String, String>,
    started: bool,
    has_tool: bool,
    has_refusal: bool,
    blocks: BTreeMap<BlockKey, BlockProgress>,
}

struct BlockProgress {
    index: usize,
    bytes_sent: usize,
    stopped: bool,
}

impl MessagesStream {
    fn new(alias: String, tools: BTreeMap<String, String>) -> Self {
        Self {
            alias,
            tools,
            started: false,
            has_tool: false,
            has_refusal: false,
            blocks: BTreeMap::new(),
        }
    }

    fn start(&mut self, response: &Value, out: &mut String) -> Translation<()> {
        if !self.started {
            emit(
                out,
                json!({"type":"message_start","message":message_shell(&self.alias, response)?}),
            );
            self.started = true;
        }
        Ok(())
    }

    fn start_block(&mut self, key: BlockKey, block: Value, out: &mut String) -> Translation<()> {
        if self.blocks.contains_key(&key) {
            return Ok(());
        }
        if !self.started {
            return Err("Codex output arrived before the message started");
        }
        let index = self.blocks.len();
        self.has_tool |= key.1.is_none();
        self.blocks.insert(
            key,
            BlockProgress {
                index,
                bytes_sent: 0,
                stopped: false,
            },
        );
        emit(
            out,
            json!({"type":"content_block_start","index":index,"content_block":block}),
        );
        Ok(())
    }

    fn delta(&mut self, key: BlockKey, text: &str, out: &mut String) -> Translation<()> {
        let block = self
            .blocks
            .get_mut(&key)
            .ok_or("Codex delta arrived before its block started")?;
        if !block.stopped && !text.is_empty() {
            let delta = if key.1.is_some() {
                json!({"type":"text_delta","text":text})
            } else {
                json!({"type":"input_json_delta","partial_json":text})
            };
            emit(
                out,
                json!({"type":"content_block_delta","index":block.index,"delta":delta}),
            );
            block.bytes_sent += text.len();
        }
        Ok(())
    }

    fn finish_block(
        &mut self,
        key: BlockKey,
        block: Value,
        full: &str,
        out: &mut String,
    ) -> Translation<()> {
        self.start_block(key, block, out)?;
        let progress = &self.blocks[&key];
        if progress.stopped {
            return Ok(());
        }
        if key.1.is_none() {
            parse_arguments(full)?;
        }
        let remaining = full
            .get(progress.bytes_sent..)
            .ok_or("Codex output did not match its stream")?;
        self.delta(key, remaining, out)?;
        self.stop_block(key, out);
        Ok(())
    }

    fn stop_block(&mut self, key: BlockKey, out: &mut String) {
        let block = self.blocks.get_mut(&key).expect("block was started");
        if !block.stopped {
            emit(
                out,
                json!({"type":"content_block_stop","index":block.index}),
            );
            block.stopped = true;
        }
    }

    fn finish_item(&mut self, index: u64, item: &Value, out: &mut String) -> Translation<()> {
        if incomplete_tool(item) {
            return Err("Codex stream ended during a tool call");
        }
        self.has_refusal |= item_has_refusal(item);
        for (part, block, full) in item_blocks(item, &self.tools)? {
            self.finish_block((index, part), block, full, out)?;
        }
        Ok(())
    }

    fn translate(&mut self, event: &ProtocolEvent, out: &mut String) -> Translation<()> {
        let value = &event.value;
        let index = || {
            value["output_index"]
                .as_u64()
                .ok_or("Codex output has no item index")
        };
        let part = || {
            value["content_index"]
                .as_u64()
                .ok_or("Codex output has no content index")
        };
        let delta = || value["delta"].as_str().ok_or("Codex output has no delta");

        if event.terminal == Some(TerminalStatus::Failed) {
            let (_, kind, message) = inference_error(value, StatusCode::BAD_GATEWAY);
            emit(out, error_event(kind, message));
        } else if event.terminal.is_some() {
            let response = &value["response"];
            self.start(response, out)?;
            if let Some(items) = response["output"].as_array() {
                for (index, item) in items.iter().enumerate() {
                    // An incomplete function may end mid-JSON. Deliver the stop reason
                    // without promoting the partial arguments into a completed tool call.
                    if event.terminal == Some(TerminalStatus::Incomplete) && incomplete_tool(item) {
                        continue;
                    }
                    self.finish_item(index as u64, item, out)?;
                }
            }
            if self
                .blocks
                .iter()
                .any(|(key, progress)| key.1.is_none() && !progress.stopped)
            {
                return Err("Codex stream ended during a tool call");
            }
            let open: Vec<_> = self.blocks.keys().copied().collect();
            for key in open {
                self.stop_block(key, out);
            }
            emit(
                out,
                json!({
                    "type":"message_delta",
                    "delta":{"stop_reason":stop_reason(response, self.has_tool, self.has_refusal),"stop_sequence":null},
                    "usage":usage(response),
                }),
            );
            emit(out, json!({"type":"message_stop"}));
        } else {
            match event.event_type.as_str() {
                "response.created" => self.start(&value["response"], out)?,
                "response.content_part.added" if part_text(&value["part"]).is_some() => {
                    self.has_refusal |= value["part"]["type"] == "refusal";
                    let key = (index()?, Some(part()?));
                    self.start_block(key, text_block(), out)?;
                    self.delta(key, part_text(&value["part"]).unwrap(), out)?;
                }
                "response.output_text.delta" | "response.refusal.delta" => {
                    self.has_refusal |= event.event_type == "response.refusal.delta";
                    let key = (index()?, Some(part()?));
                    self.start_block(key, text_block(), out)?;
                    self.delta(key, delta()?, out)?;
                }
                "response.content_part.done" if part_text(&value["part"]).is_some() => {
                    self.has_refusal |= value["part"]["type"] == "refusal";
                    self.finish_block(
                        (index()?, Some(part()?)),
                        text_block(),
                        part_text(&value["part"]).unwrap(),
                        out,
                    )?;
                }
                "response.output_item.added" if value["item"]["type"] == "function_call" => {
                    let key = (index()?, None);
                    let item = &value["item"];
                    self.start_block(key, tool_block(item, &self.tools)?, out)?;
                    self.delta(key, item["arguments"].as_str().unwrap_or(""), out)?;
                }
                "response.function_call_arguments.delta" => {
                    self.delta((index()?, None), delta()?, out)?
                }
                "response.output_item.done" => self.finish_item(index()?, &value["item"], out)?,
                _ => (),
            }
        }
        Ok(())
    }
}
