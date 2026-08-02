use std::{error::Error, fmt};

use serde_json::{Map, Value, json};

pub const DEFAULT_MAX_EVENT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalStatus {
    Completed,
    Failed,
    Incomplete,
}

impl TerminalStatus {
    pub fn event_type(self) -> &'static str {
        match self {
            Self::Completed => "response.completed",
            Self::Failed => "response.failed",
            Self::Incomplete => "response.incomplete",
        }
    }

    fn from_event_type(event_type: &str) -> Option<Self> {
        match event_type {
            "response.completed" => Some(Self::Completed),
            "response.failed" => Some(Self::Failed),
            "response.incomplete" => Some(Self::Incomplete),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolEvent {
    /// The exact JSON text to place in a Responses WebSocket text frame.
    pub payload: String,
    /// Parsed payload for routing, logging, and response-id observation.
    pub value: Value,
    pub event_type: String,
    pub terminal: Option<TerminalStatus>,
}

impl ProtocolEvent {
    pub fn from_value(value: Value) -> Result<Self, DecodeError> {
        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .ok_or(DecodeError::MissingEventType)?
            .to_owned();
        let terminal = TerminalStatus::from_event_type(&event_type);
        let payload = serde_json::to_string(&value)
            .map_err(|error| DecodeError::InvalidJson(error.to_string()))?;
        Ok(Self {
            payload,
            value,
            event_type,
            terminal,
        })
    }

    fn from_payload(payload: String) -> Result<Self, DecodeError> {
        let value: Value = serde_json::from_str(&payload)
            .map_err(|error| DecodeError::InvalidJson(error.to_string()))?;
        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .ok_or(DecodeError::MissingEventType)?
            .to_owned();
        let terminal = TerminalStatus::from_event_type(&event_type);
        Ok(Self {
            payload,
            value,
            event_type,
            terminal,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    EventTooLarge { limit: usize },
    InvalidUtf8,
    InvalidJson(String),
    MissingEventType,
    InvalidResponsesJson(&'static str),
    PrematureEof,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EventTooLarge { limit } => {
                write!(formatter, "upstream SSE event exceeds {limit} byte limit")
            }
            Self::InvalidUtf8 => formatter.write_str("upstream SSE data is not valid UTF-8"),
            Self::InvalidJson(message) => {
                write!(
                    formatter,
                    "invalid JSON payload in upstream SSE event: {message}"
                )
            }
            Self::MissingEventType => {
                formatter.write_str("upstream Responses event has no string type")
            }
            Self::InvalidResponsesJson(message) => {
                write!(formatter, "invalid successful Responses JSON: {message}")
            }
            Self::PrematureEof => {
                formatter.write_str("upstream stream ended before response terminal event")
            }
        }
    }
}

impl Error for DecodeError {}

/// Incrementally converts an HTTP Responses SSE body into validated WebSocket events.
///
/// Comments and empty SSE blocks are ignored. A standalone `[DONE]` data payload is
/// ignored, but does not count as a terminal Responses event; EOF afterward is an
/// error. Once the first terminal event is returned, trailing input is discarded.
pub struct SseDecoder {
    buffer: Vec<u8>,
    max_event_bytes: usize,
    terminal: Option<TerminalStatus>,
    previous_lf: Option<usize>,
}

impl Default for SseDecoder {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_EVENT_BYTES)
    }
}

impl SseDecoder {
    pub fn new(max_event_bytes: usize) -> Self {
        Self {
            buffer: Vec::new(),
            max_event_bytes,
            terminal: None,
            previous_lf: None,
        }
    }

    pub fn terminal_status(&self) -> Option<TerminalStatus> {
        self.terminal
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal.is_some()
    }

    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<ProtocolEvent>, DecodeError> {
        if self.is_terminal() {
            return Ok(Vec::new());
        }

        let mut events = Vec::new();
        let mut offset = 0;
        while offset < chunk.len() && !self.is_terminal() {
            let remaining = &chunk[offset..];
            let Some(relative_lf) = remaining.iter().position(|byte| *byte == b'\n') else {
                self.extend_bounded(remaining)?;
                break;
            };
            let end = offset + relative_lf + 1;
            let through_lf = &chunk[offset..end];
            let before_lf = &through_lf[..through_lf.len() - 1];

            if self.completes_boundary(before_lf) {
                let previous_lf = self.previous_lf.expect("boundary requires previous LF");
                let block_end = if previous_lf > 0 && self.buffer[previous_lf - 1] == b'\r' {
                    previous_lf - 1
                } else {
                    previous_lf
                };
                if block_end > self.max_event_bytes {
                    return Err(DecodeError::EventTooLarge {
                        limit: self.max_event_bytes,
                    });
                }
                let event = decode_block(&self.buffer[..block_end])?;
                self.buffer.clear();
                self.previous_lf = None;
                if let Some(event) = event {
                    let terminal = event.terminal;
                    events.push(event);
                    if let Some(status) = terminal {
                        self.terminal = Some(status);
                    }
                }
            } else {
                self.extend_bounded(through_lf)?;
                self.previous_lf = Some(self.buffer.len() - 1);
            }
            offset = end;
        }
        Ok(events)
    }

    /// Completes decoding at HTTP-body EOF.
    ///
    /// An unterminated final SSE block is parsed, matching browser/EventSource
    /// behavior. Completion is successful only if a Responses terminal event has
    /// been observed.
    pub fn finish(&mut self) -> Result<Vec<ProtocolEvent>, DecodeError> {
        if self.is_terminal() {
            self.buffer.clear();
            self.previous_lf = None;
            return Ok(Vec::new());
        }

        let mut events = Vec::new();
        if !self.buffer.is_empty() {
            if self.buffer.len() > self.max_event_bytes {
                return Err(DecodeError::EventTooLarge {
                    limit: self.max_event_bytes,
                });
            }
            let block = std::mem::take(&mut self.buffer);
            self.previous_lf = None;
            if let Some(event) = decode_block(&block)? {
                let terminal = event.terminal;
                events.push(event);
                if let Some(status) = terminal {
                    self.terminal = Some(status);
                }
            }
        }

        if self.is_terminal() {
            Ok(events)
        } else {
            Err(DecodeError::PrematureEof)
        }
    }

    fn extend_bounded(&mut self, bytes: &[u8]) -> Result<(), DecodeError> {
        // At most three bytes beyond the event limit can be a split prefix of
        // `\r?\n\r?\n`. Reject before copying anything that cannot fit.
        let max_buffer = self.max_event_bytes.saturating_add(3);
        if bytes.len() > max_buffer.saturating_sub(self.buffer.len()) {
            return Err(DecodeError::EventTooLarge {
                limit: self.max_event_bytes,
            });
        }
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    fn completes_boundary(&self, before_lf: &[u8]) -> bool {
        let Some(previous_lf) = self.previous_lf else {
            return false;
        };
        let buffered_between = &self.buffer[previous_lf + 1..];
        (buffered_between.is_empty() && before_lf.is_empty())
            || (buffered_between == b"\r" && before_lf.is_empty())
            || (buffered_between.is_empty() && before_lf == b"\r")
    }
}

fn decode_block(block: &[u8]) -> Result<Option<ProtocolEvent>, DecodeError> {
    let mut data = Vec::new();
    for line in block.split(|byte| *byte == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(value) = line.strip_prefix(b"data:") else {
            continue;
        };
        if !data.is_empty() {
            data.push(b'\n');
        }
        data.extend_from_slice(value.strip_prefix(b" ").unwrap_or(value));
    }

    if data.is_empty() {
        return Ok(None);
    }
    let payload = String::from_utf8(data).map_err(|_| DecodeError::InvalidUtf8)?;
    if payload == "[DONE]" {
        return Ok(None);
    }
    ProtocolEvent::from_payload(payload).map(Some)
}

/// Converts a successful, non-streaming Responses JSON object into the event
/// lifecycle expected by the Responses WebSocket transport.
pub fn responses_json_events(response: Value) -> Result<Vec<ProtocolEvent>, DecodeError> {
    let object = response
        .as_object()
        .ok_or(DecodeError::InvalidResponsesJson(
            "top-level value must be an object",
        ))?;
    let output = object
        .get("output")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut created_response = object.clone();
    created_response.insert("status".to_owned(), Value::String("in_progress".to_owned()));
    created_response.insert("output".to_owned(), Value::Array(Vec::new()));

    let mut events = Vec::with_capacity(output.len().saturating_add(2));
    events.push(ProtocolEvent::from_value(json!({
        "type": "response.created",
        "response": Value::Object(created_response),
    }))?);

    for (output_index, item) in output.into_iter().enumerate() {
        events.push(ProtocolEvent::from_value(json!({
            "type": "response.output_item.done",
            "output_index": output_index,
            "item": item,
        }))?);
    }

    let final_status = match object.get("status").and_then(Value::as_str) {
        Some("failed") => TerminalStatus::Failed,
        Some("incomplete") => TerminalStatus::Incomplete,
        _ => TerminalStatus::Completed,
    };
    let mut terminal_response: Map<String, Value> = object.clone();
    terminal_response.insert(
        "status".to_owned(),
        Value::String(
            match final_status {
                TerminalStatus::Completed => "completed",
                TerminalStatus::Failed => "failed",
                TerminalStatus::Incomplete => "incomplete",
            }
            .to_owned(),
        ),
    );
    events.push(ProtocolEvent::from_value(json!({
        "type": final_status.event_type(),
        "response": Value::Object(terminal_response),
    }))?);
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(event_type: &str) -> String {
        json!({"type": event_type}).to_string()
    }

    #[test]
    fn chooses_earliest_boundary_across_line_endings() {
        let mut decoder = SseDecoder::default();
        let input = format!(
            "data: {}\n\ndata: {}\r\n\r\n",
            event("response.created"),
            event("response.completed")
        );
        let events = decoder.push(input.as_bytes()).unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.event_type.as_str())
                .collect::<Vec<_>>(),
            ["response.created", "response.completed"]
        );
        assert_eq!(decoder.terminal_status(), Some(TerminalStatus::Completed));
    }

    #[test]
    fn accepts_mixed_and_chunked_boundaries() {
        let mut decoder = SseDecoder::default();
        assert!(
            decoder
                .push(format!("data: {}\r\n", event("response.created")).as_bytes())
                .unwrap()
                .is_empty()
        );
        let events = decoder
            .push(b"\ndata: {\"type\":\"response.failed\"}\n\r\n")
            .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].terminal, Some(TerminalStatus::Failed));
    }

    #[test]
    fn joins_multiple_data_lines() {
        let mut decoder = SseDecoder::default();
        let events = decoder
            .push(b"data: {\"type\":\ndata: \"response.completed\"}\n\n")
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].terminal, Some(TerminalStatus::Completed));
    }

    #[test]
    fn ignores_comments_empty_blocks_and_done_without_treating_done_as_terminal() {
        let mut decoder = SseDecoder::default();
        let events = decoder
            .push(b": heartbeat\n\ndata: [DONE]\n\n\n\ndata: {\"type\":\"response.completed\"}\n\n")
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].terminal, Some(TerminalStatus::Completed));
    }

    #[test]
    fn rejects_done_followed_by_eof_without_terminal_event() {
        let mut decoder = SseDecoder::default();
        assert!(decoder.push(b"data: [DONE]\n\n").unwrap().is_empty());
        assert_eq!(decoder.finish(), Err(DecodeError::PrematureEof));
    }

    #[test]
    fn rejects_invalid_json_and_missing_string_type() {
        let mut decoder = SseDecoder::default();
        assert!(matches!(
            decoder.push(b"data: not-json\n\n"),
            Err(DecodeError::InvalidJson(_))
        ));

        let mut decoder = SseDecoder::default();
        assert_eq!(
            decoder.push(b"data: {\"type\":42}\n\n"),
            Err(DecodeError::MissingEventType)
        );
    }

    #[test]
    fn rejects_premature_eof_but_accepts_unterminated_terminal_block() {
        let mut decoder = SseDecoder::default();
        decoder
            .push(b"data: {\"type\":\"response.created\"}\n\n")
            .unwrap();
        assert_eq!(decoder.finish(), Err(DecodeError::PrematureEof));

        let mut decoder = SseDecoder::default();
        decoder
            .push(b"data: {\"type\":\"response.created\"}\n\n")
            .unwrap();
        decoder
            .push(b"data: {\"type\":\"response.incomplete\"}")
            .unwrap();
        let events = decoder.finish().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].terminal, Some(TerminalStatus::Incomplete));
    }

    #[test]
    fn stops_at_first_terminal_and_discards_trailing_data() {
        let mut decoder = SseDecoder::default();
        let events = decoder
            .push(b"data: {\"type\":\"response.completed\"}\n\ndata: not-json\n\n")
            .unwrap();
        assert_eq!(events.len(), 1);
        assert!(decoder.is_terminal());
        assert!(decoder.push(b"data: also-not-json\n\n").unwrap().is_empty());
        assert!(decoder.finish().unwrap().is_empty());
    }

    #[test]
    fn enforces_event_limit_across_chunks() {
        let mut decoder = SseDecoder::new(8);
        assert!(decoder.push(b"data: 12").unwrap().is_empty());
        assert_eq!(
            decoder.push(b"3456"),
            Err(DecodeError::EventTooLarge { limit: 8 })
        );

        let mut decoder = SseDecoder::new(35);
        let block = b"data: {\"type\":\"response.completed\"}";
        assert_eq!(block.len(), 35);
        assert!(decoder.push(block).unwrap().is_empty());
        let events = decoder.push(b"\r\n\r\n").unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn rejects_a_huge_unterminated_chunk_before_copying_it() {
        let mut decoder = SseDecoder::new(64);
        decoder.push(b"data: ").unwrap();
        let before = decoder.buffer.clone();
        let huge = vec![b'x'; 1024 * 1024];

        assert_eq!(
            decoder.push(&huge),
            Err(DecodeError::EventTooLarge { limit: 64 })
        );
        assert_eq!(decoder.buffer, before);
        assert!(decoder.buffer.capacity() <= 67);
    }

    #[test]
    fn stops_scanning_a_huge_chunk_immediately_after_terminal() {
        let mut decoder = SseDecoder::new(64);
        let mut chunk = b"data: {\"type\":\"response.completed\"}\n\n".to_vec();
        chunk.extend(std::iter::repeat_n(b'x', 1024 * 1024));

        let events = decoder.push(&chunk).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].terminal, Some(TerminalStatus::Completed));
        assert!(decoder.buffer.is_empty());
        assert!(decoder.buffer.capacity() <= 67);
    }

    #[test]
    fn incrementally_accepts_many_bounded_events_in_one_large_chunk() {
        let mut decoder = SseDecoder::new(64);
        let mut chunk = Vec::new();
        for _ in 0..1024 {
            chunk.extend_from_slice(b"data: {\"type\":\"response.created\"}\n\n");
        }
        chunk.extend_from_slice(b"data: {\"type\":\"response.completed\"}\n\n");
        assert!(chunk.len() > decoder.max_event_bytes);

        let events = decoder.push(&chunk).unwrap();
        assert_eq!(events.len(), 1025);
        assert_eq!(
            events.last().unwrap().terminal,
            Some(TerminalStatus::Completed)
        );
        assert!(decoder.buffer.is_empty());
        assert!(decoder.buffer.capacity() <= decoder.max_event_bytes.saturating_mul(2));
    }

    #[test]
    fn synthesizes_non_streaming_response_lifecycle() {
        let response = json!({
            "id": "resp_123",
            "object": "response",
            "status": "completed",
            "output": [
                {"type": "message", "id": "msg_1"},
                {"type": "reasoning", "id": "rs_1"}
            ]
        });
        let events = responses_json_events(response).unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.event_type.as_str())
                .collect::<Vec<_>>(),
            [
                "response.created",
                "response.output_item.done",
                "response.output_item.done",
                "response.completed"
            ]
        );
        assert_eq!(events[1].value["output_index"], 0);
        assert_eq!(events[2].value["output_index"], 1);
        assert_eq!(events[0].value["response"]["output"], json!([]));
        assert_eq!(events[3].terminal, Some(TerminalStatus::Completed));
    }

    #[test]
    fn preserves_failed_and_incomplete_non_streaming_statuses() {
        let failed = responses_json_events(json!({"status":"failed","output":[]})).unwrap();
        assert_eq!(
            failed.last().unwrap().terminal,
            Some(TerminalStatus::Failed)
        );
        let incomplete = responses_json_events(json!({"status":"incomplete","output":[]})).unwrap();
        assert_eq!(
            incomplete.last().unwrap().terminal,
            Some(TerminalStatus::Incomplete)
        );
    }

    #[test]
    fn rejects_non_object_response_and_treats_non_array_output_as_empty() {
        assert!(matches!(
            responses_json_events(json!([])),
            Err(DecodeError::InvalidResponsesJson(_))
        ));
        let events = responses_json_events(json!({"output": {}})).unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.event_type.as_str())
                .collect::<Vec<_>>(),
            ["response.created", "response.completed"]
        );
    }
}
