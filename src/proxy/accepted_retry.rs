//! The only accepted work that may move accounts is an explicit capacity
//! rejection after empty lifecycle metadata, with affirmative zero-output usage.
//! Keeping these exact frames private avoids translating response IDs or sequence
//! numbers across upstream attempts.

use serde_json::Value;

use super::websocket_protocol::{FailureKind, classify_terminal_event};

const MAX_LIFECYCLE_EVENTS: usize = 16;
const MAX_LIFECYCLE_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub(super) struct BufferedEvent {
    pub payload: String,
    pub value: Value,
}

#[derive(Debug)]
pub(super) struct LifecycleBuffer {
    enabled: bool,
    response_id: Option<String>,
    bytes: usize,
    events: Vec<BufferedEvent>,
}

impl LifecycleBuffer {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            response_id: None,
            bytes: 0,
            events: Vec::new(),
        }
    }

    /// Returns true only when this exact event was retained. Any other event
    /// must release the buffer before it is delivered, and closes the retry gate.
    pub fn retain(&mut self, event: &Value, payload: &str) -> bool {
        if !self.enabled {
            return false;
        }
        let kind = event.get("type").and_then(Value::as_str);
        let response = event.get("response");
        let id = response
            .and_then(|response| response.get("id"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty());
        let lifecycle = match kind {
            Some("response.created") => self.response_id.is_none() && id.is_some(),
            Some("response.in_progress") => self.response_id.is_some() && id.is_some(),
            _ => false,
        };
        let same_response = self
            .response_id
            .as_deref()
            .is_none_or(|previous| Some(previous) == id);
        let empty = response.is_some_and(|response| {
            response
                .get("output")
                .is_none_or(|output| output.as_array().is_some_and(Vec::is_empty))
                && response
                    .get("usage")
                    .filter(|usage| !usage.is_null())
                    .is_none_or(zero_output_usage)
        });
        if !lifecycle
            || !same_response
            || !empty
            || self.events.len() >= MAX_LIFECYCLE_EVENTS
            || payload.len() > MAX_LIFECYCLE_BYTES.saturating_sub(self.bytes)
        {
            self.enabled = false;
            return false;
        }
        self.response_id = id.map(str::to_owned);
        self.bytes += payload.len();
        self.events.push(BufferedEvent {
            payload: payload.to_owned(),
            value: event.clone(),
        });
        true
    }

    pub fn permits_capacity_retry(&self, terminal: &Value) -> bool {
        self.enabled
            && !self.events.is_empty()
            && classify_terminal_event(terminal).kind == FailureKind::Capacity
            && terminal.get("response").is_some_and(|response| {
                response.get("id").and_then(Value::as_str) == self.response_id.as_deref()
                    && response
                        .get("output")
                        .and_then(Value::as_array)
                        .is_some_and(Vec::is_empty)
                    && response.get("usage").is_some_and(zero_output_usage)
            })
    }

    /// Release preserves the original payloads, including sequence numbers and
    /// unknown fields. Once anything is released, this attempt cannot be hidden.
    pub fn release(&mut self) -> Vec<BufferedEvent> {
        self.enabled = false;
        self.bytes = 0;
        std::mem::take(&mut self.events)
    }
}

fn zero_output_usage(usage: &Value) -> bool {
    usage.get("output_tokens").and_then(Value::as_u64) == Some(0)
        && ["output_tokens_details", "completion_tokens_details"]
            .iter()
            .all(|key| {
                usage.get(key).is_none_or(|details| {
                    details.is_null()
                        || details.as_object().is_some_and(|details| {
                            details.values().all(|value| value.as_u64() == Some(0))
                        })
                })
            })
        && ["completion_tokens", "reasoning_tokens"]
            .iter()
            .all(|key| usage.get(key).is_none_or(|value| value.as_u64() == Some(0)))
        && match (usage.get("input_tokens"), usage.get("total_tokens")) {
            (Some(input), Some(total)) => {
                input.as_u64().is_some() && input.as_u64() == total.as_u64()
            }
            _ => true,
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn created() -> Value {
        json!({"type":"response.created","sequence_number":0,
            "response":{"id":"resp_a","output":[],"usage":null}})
    }

    fn capacity() -> Value {
        json!({"type":"response.failed","sequence_number":2,"response":{
            "id":"resp_a","output":[],"usage":{"output_tokens":0,
                "output_tokens_details":{"reasoning_tokens":0}},
            "error":{"code":"server_is_overloaded"}}})
    }

    #[test]
    fn requires_affirmative_zero_output_usage_and_matching_response() {
        let mut buffer = LifecycleBuffer::new(true);
        let created = created();
        assert!(buffer.retain(&created, &created.to_string()));
        assert!(buffer.permits_capacity_retry(&capacity()));
        for usage in [
            Value::Null,
            json!({}),
            json!({"output_tokens":1}),
            json!({"output_tokens":"0"}),
            json!({"output_tokens":0,"output_tokens_details":{"reasoning_tokens":1}}),
            json!({"output_tokens":0,"output_tokens_details":{"reasoning_tokens":null}}),
            json!({"output_tokens":0,"completion_tokens":1}),
            json!({"output_tokens":0,"reasoning_tokens":1}),
            json!({"output_tokens":0,"reasoning_tokens":"0"}),
            json!({"output_tokens":0,"completion_tokens_details":{"reasoning_tokens":1}}),
            json!({"output_tokens":0,"input_tokens":2,"total_tokens":3}),
        ] {
            let mut terminal = capacity();
            terminal["response"]["usage"] = usage;
            assert!(!buffer.permits_capacity_retry(&terminal), "{terminal}");
        }
        let mut terminal = capacity();
        terminal["response"]["id"] = json!("resp_other");
        assert!(!buffer.permits_capacity_retry(&terminal));
        terminal = capacity();
        terminal["response"]["output"] = json!([{"type":"reasoning"}]);
        assert!(!buffer.permits_capacity_retry(&terminal));
    }

    #[test]
    fn first_other_event_including_empty_output_shell_permanently_closes_gate() {
        for kind in [
            "response.output_item.added",
            "response.output_item.done",
            "response.output_text.delta",
            "response.reasoning_text.delta",
            "response.reasoning_summary_text.delta",
            "response.function_call_arguments.delta",
            "response.custom_tool_call_input.delta",
            "response.content_part.added",
            "response.audio.delta",
            "response.metadata",
            "unknown",
        ] {
            let mut buffer = LifecycleBuffer::new(true);
            let created = created();
            assert!(buffer.retain(&created, &created.to_string()));
            let output = json!({"type":kind,"delta":""});
            assert!(!buffer.retain(&output, &output.to_string()));
            assert!(!buffer.permits_capacity_retry(&capacity()), "{kind}");
            assert_eq!(buffer.release()[0].value, created);
        }
    }

    #[test]
    fn preserves_exact_payloads_and_bounds_count_and_bytes() {
        let mut buffer = LifecycleBuffer::new(true);
        let created = created();
        let payload = format!("  {}  ", created);
        assert!(buffer.retain(&created, &payload));
        let mut progress = created.clone();
        progress["type"] = json!("response.in_progress");
        let progress_payload = format!("  {}  ", progress);
        for _ in 1..MAX_LIFECYCLE_EVENTS {
            assert!(buffer.retain(&progress, &progress_payload));
        }
        assert!(!buffer.retain(&progress, &progress_payload));
        assert!(!buffer.permits_capacity_retry(&capacity()));
        let events = buffer.release();
        assert_eq!(events[0].payload, payload);
        assert!(
            events[1..]
                .iter()
                .all(|event| event.payload == progress_payload)
        );
        let mut buffer = LifecycleBuffer::new(true);
        assert!(!buffer.retain(&created, &" ".repeat(MAX_LIFECYCLE_BYTES + 1)));
        assert!(buffer.release().is_empty());
    }
}
