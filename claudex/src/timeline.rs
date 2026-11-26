//! Paginated ordinary history in the desktop's canonical timeline order.
use crate::protocol::{RpcError, RpcResult};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};

pub fn list(thread_id: &str, turns: &[Value], params: &Value) -> RpcResult<Value> {
    let mut entries = Vec::new();
    for turn in turns {
        // The pinned export uses snake_case on boundaries; the installed desktop
        // reads camelCase. Supply both spellings with identical values.
        entries.push(json!({"type":"turnStarted", "position":entries.len(),
            "turn_id":turn["id"],"turnId":turn["id"],
            "started_at":turn["startedAt"],"startedAt":turn["startedAt"]}));
        for item in turn["items"].as_array().into_iter().flatten() {
            entries.push(
                json!({"type":"item","position":entries.len(),"turnId":turn["id"],"item":item}),
            );
        }
        if turn["status"] != "inProgress" {
            entries.push(json!({"type":"turnCompleted","position":entries.len(),
                "turn_id":turn["id"],"turnId":turn["id"],"status":turn["status"],"error":turn["error"],
                "started_at":turn["startedAt"],"startedAt":turn["startedAt"],
                "completed_at":turn["completedAt"],"completedAt":turn["completedAt"],
                "duration_ms":turn["durationMs"],"durationMs":turn["durationMs"]}));
        }
    }
    let end = match params["cursor"].as_str() {
        None => entries.len(),
        Some(cursor) => {
            let cursor: Value = URL_SAFE_NO_PAD
                .decode(cursor)
                .ok()
                .and_then(|bytes| serde_json::from_slice(&bytes).ok())
                .ok_or_else(|| RpcError::invalid("Invalid timeline cursor"))?;
            if cursor["thread"] != thread_id || cursor["kind"] != "timeline" {
                return Err(RpcError::invalid("Timeline cursor belongs to another list"));
            }
            cursor["before"]
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .filter(|n| *n <= entries.len())
                .ok_or_else(|| RpcError::invalid("Timeline cursor is outside saved history"))?
        }
    };
    let limit = params["limit"].as_u64().unwrap_or(500).clamp(1, 1000) as usize;
    let start = end.saturating_sub(limit);
    let next = (start > 0).then(|| {
        URL_SAFE_NO_PAD
            .encode(json!({"kind":"timeline","thread":thread_id,"before":start}).to_string())
    });
    Ok(
        json!({"data": &entries[start..end],"nextCursor":next,"activeRealtimeSessionAtPageStart":null}),
    )
}
