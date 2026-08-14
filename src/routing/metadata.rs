use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use hyper::HeaderMap;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AffinityKind {
    TurnState,
    ThreadHeader,
    Session,
    ParentThread,
    TurnMetadata,
    BodyThread,
    PreviousResponse,
    PromptCache,
    File,
}

impl AffinityKind {
    pub fn is_hard_continuity(self) -> bool {
        matches!(self, Self::TurnState | Self::PreviousResponse | Self::File)
    }

    /// Lower values are stronger soft routing identities. A conversation/thread
    /// identifier carried by the request itself must win over connection-level
    /// session and cache hints that can survive across conversations.
    pub fn soft_routing_priority(self) -> Option<u8> {
        match self {
            Self::ThreadHeader | Self::BodyThread => Some(0),
            Self::ParentThread | Self::TurnMetadata => Some(1),
            Self::Session => Some(2),
            Self::PromptCache => Some(3),
            Self::TurnState | Self::PreviousResponse | Self::File => None,
        }
    }

    fn prefix(self) -> &'static str {
        match self {
            Self::TurnState => "turn-state",
            Self::Session => "session",
            Self::ThreadHeader | Self::ParentThread | Self::TurnMetadata | Self::BodyThread => {
                "thread"
            }
            Self::PreviousResponse => "previous-response",
            Self::PromptCache => "prompt-cache",
            Self::File => "file",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffinityValue {
    pub kind: AffinityKind,
    pub value: String,
}

impl AffinityValue {
    pub fn namespaced(&self) -> String {
        format!("{}:{}", self.kind.prefix(), self.value)
    }
}

pub fn thread_id(headers: &HeaderMap, body: Option<&[u8]>) -> Option<String> {
    header(headers, "thread-id")
        .or_else(|| header(headers, "x-codex-parent-thread-id"))
        .or_else(|| header_json(headers, "x-codex-turn-metadata", "thread_id"))
        .or_else(|| body.and_then(body_thread_id))
}

pub fn affinity_values(
    headers: &HeaderMap,
    body_thread_id: Option<&str>,
    previous_response_id: Option<&str>,
    prompt_cache_key: Option<&str>,
    file_ids: &[String],
) -> Vec<AffinityValue> {
    let mut values = Vec::new();
    push_header(
        &mut values,
        headers,
        "x-codex-turn-state",
        AffinityKind::TurnState,
    );
    for name in [
        "session_id",
        "session-id",
        "x-codex-session-id",
        "x-codex-conversation-id",
    ] {
        if push_header(&mut values, headers, name, AffinityKind::Session) {
            break;
        }
    }
    push_header(
        &mut values,
        headers,
        "thread-id",
        AffinityKind::ThreadHeader,
    );
    push_header(
        &mut values,
        headers,
        "x-codex-parent-thread-id",
        AffinityKind::ParentThread,
    );
    if let Some(value) = header_json(headers, "x-codex-turn-metadata", "thread_id") {
        push(&mut values, AffinityKind::TurnMetadata, value);
    }
    if let Some(value) = body_thread_id {
        push(&mut values, AffinityKind::BodyThread, value.to_owned());
    }
    if let Some(value) = previous_response_id {
        push(
            &mut values,
            AffinityKind::PreviousResponse,
            value.to_owned(),
        );
    }
    if let Some(value) = prompt_cache_key {
        push(&mut values, AffinityKind::PromptCache, value.to_owned());
    }
    for file_id in file_ids {
        push(&mut values, AffinityKind::File, file_id.clone());
    }
    values
}

fn push_header(
    values: &mut Vec<AffinityValue>,
    headers: &HeaderMap,
    name: &str,
    kind: AffinityKind,
) -> bool {
    let Some(value) = header(headers, name) else {
        return false;
    };
    push(values, kind, value);
    true
}

fn push(values: &mut Vec<AffinityValue>, kind: AffinityKind, value: String) {
    if value.is_empty()
        || values
            .iter()
            .any(|existing| existing.kind == kind && existing.value == value)
    {
        return;
    }
    values.push(AffinityValue { kind, value });
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)?
        .to_str()
        .ok()
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
}

fn header_json(headers: &HeaderMap, name: &str, field: &str) -> Option<String> {
    let raw = headers.get(name)?.to_str().ok()?;
    parse_field(raw.as_bytes(), field)
        .or_else(|| {
            URL_SAFE_NO_PAD
                .decode(raw)
                .ok()
                .and_then(|v| parse_field(&v, field))
        })
        .or_else(|| {
            STANDARD
                .decode(raw)
                .ok()
                .and_then(|v| parse_field(&v, field))
        })
}

fn body_thread_id(bytes: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(bytes).ok()?;
    value
        .get("client_metadata")?
        .get("thread_id")?
        .as_str()
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
}

fn parse_field(bytes: &[u8], field: &str) -> Option<String> {
    serde_json::from_slice::<Value>(bytes)
        .ok()?
        .get(field)?
        .as_str()
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::HeaderValue;

    #[test]
    fn precedence_and_fallbacks() {
        let mut h = HeaderMap::new();
        h.insert(
            "x-codex-turn-metadata",
            HeaderValue::from_static(r#"{"thread_id":"root"}"#),
        );
        assert_eq!(thread_id(&h, None).as_deref(), Some("root"));
        h.insert("thread-id", HeaderValue::from_static("task"));
        assert_eq!(thread_id(&h, None).as_deref(), Some("task"));
        h.insert(
            "x-codex-parent-thread-id",
            HeaderValue::from_static("parent"),
        );
        assert_eq!(
            thread_id(&h, Some(br#"{"client_metadata":{"thread_id":"body"}}"#)).as_deref(),
            Some("task")
        );

        h.insert("x-codex-turn-state", HeaderValue::from_static("opaque"));
        let values = affinity_values(
            &h,
            Some("body"),
            Some("resp_1"),
            Some("cache_1"),
            &["file_1".into()],
        );
        assert_eq!(values[0].kind, AffinityKind::TurnState);
        assert!(
            values
                .iter()
                .any(|v| v.kind == AffinityKind::PreviousResponse)
        );
        assert!(values.iter().any(|v| v.kind == AffinityKind::File));
        assert!(!AffinityKind::Session.is_hard_continuity());
        assert_eq!(AffinityKind::BodyThread.soft_routing_priority(), Some(0));
        assert!(
            AffinityKind::BodyThread.soft_routing_priority()
                < AffinityKind::Session.soft_routing_priority()
        );
        assert!(
            AffinityKind::Session.soft_routing_priority()
                < AffinityKind::PromptCache.soft_routing_priority()
        );
    }

    #[test]
    fn keeps_process_session_and_task_thread_as_distinct_affinity_keys() {
        let mut headers = HeaderMap::new();
        headers.insert("session-id", HeaderValue::from_static("agent-tree"));
        headers.insert("thread-id", HeaderValue::from_static("child-task"));

        let values = affinity_values(&headers, None, None, Some("agent-tree"), &[]);

        assert!(values.iter().any(|value| {
            value.kind == AffinityKind::Session && value.namespaced() == "session:agent-tree"
        }));
        assert!(values.iter().any(|value| {
            value.kind == AffinityKind::ThreadHeader && value.namespaced() == "thread:child-task"
        }));
        assert_eq!(
            values
                .iter()
                .filter_map(|value| {
                    value
                        .kind
                        .soft_routing_priority()
                        .map(|priority| (priority, value.namespaced()))
                })
                .min_by_key(|(priority, _)| *priority)
                .map(|(_, value)| value),
            Some("thread:child-task".to_owned())
        );
    }
}
