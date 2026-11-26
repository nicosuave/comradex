//! Persisted, thread-local attachment metadata. Files and Git checkouts are owned
//! by the desktop; deleting an attachment never deletes its target.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::protocol::{self, RpcError, RpcResult, required_str, supported_fields};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Attachment {
    pub id: String,
    pub attachment_type: String,
    pub identity_key: String,
    pub payload: Value,
    pub created_at: u64,
}

pub struct Change {
    pub response: Value,
    /// Emit only after the enclosing record has been saved successfully.
    pub notification: Option<Value>,
}

/// The caller must resolve `threadId`, hold that record's mutation lock, save
/// changed metadata, and only then publish the returned notification.
pub fn dispatch(
    attachments: &mut Vec<Attachment>,
    method: &str,
    params: &Value,
) -> RpcResult<Change> {
    let thread_id = required_str(params, "threadId")?;
    match method {
        "thread/attachment/add" => {
            supported_fields(
                params,
                &["threadId", "attachmentType", "identityKey", "payload"],
            )?;
            let attachment_type = required_str(params, "attachmentType")?;
            let identity_key = required_str(params, "identityKey")?;
            let payload = params
                .get("payload")
                .ok_or_else(|| RpcError::invalid("payload is required"))?;
            if let Some(existing) = attachments
                .iter()
                .find(|a| a.attachment_type == attachment_type && a.identity_key == identity_key)
            {
                return Ok(Change {
                    response: json!({"attachment": existing, "outcome": "existing"}),
                    notification: None,
                });
            }
            let attachment = Attachment {
                id: protocol::id(),
                attachment_type: attachment_type.into(),
                identity_key: identity_key.into(),
                payload: payload.clone(),
                created_at: protocol::now(),
            };
            let notification = updated(thread_id, &attachment, "created");
            let response = json!({"attachment": attachment, "outcome": "created"});
            attachments.push(attachment);
            Ok(Change {
                response,
                notification: Some(notification),
            })
        }
        "thread/attachment/remove" => {
            supported_fields(params, &["threadId", "attachmentType", "identityKey"])?;
            let attachment_type = required_str(params, "attachmentType")?;
            let identity_key = required_str(params, "identityKey")?;
            let notification = attachments
                .iter()
                .position(|a| {
                    a.attachment_type == attachment_type && a.identity_key == identity_key
                })
                .map(|index| updated(thread_id, &attachments.remove(index), "deleted"));
            Ok(Change {
                response: json!({}),
                notification,
            })
        }
        "thread/attachment/list" => {
            supported_fields(params, &["threadId", "cursor", "limit"])?;
            let limit = match params.get("limit").filter(|v| !v.is_null()) {
                None => 100,
                Some(v) => v
                    .as_u64()
                    .filter(|n| *n <= u32::MAX as u64)
                    .ok_or_else(|| RpcError::invalid("limit must be an unsigned 32-bit integer"))?
                    .clamp(1, 100) as usize,
            };
            let cursor: Option<(String, u64, String)> = params
                .get("cursor")
                .filter(|v| !v.is_null())
                .map(|v| {
                    let bytes = v
                        .as_str()
                        .and_then(|s| URL_SAFE_NO_PAD.decode(s).ok())
                        .ok_or_else(|| RpcError::invalid("invalid attachment cursor"))?;
                    serde_json::from_slice(&bytes)
                        .map_err(|_| RpcError::invalid("invalid attachment cursor"))
                })
                .transpose()?;
            if cursor
                .as_ref()
                .is_some_and(|(owner, _, _)| owner != thread_id)
            {
                return Err(RpcError::invalid(
                    "attachment cursor belongs to another thread",
                ));
            }
            let mut items: Vec<_> = attachments
                .iter()
                .filter(|a| {
                    cursor
                        .as_ref()
                        .is_none_or(|(_, time, id)| (a.created_at, &a.id) > (*time, id))
                })
                .collect();
            items.sort_by(|a, b| (a.created_at, &a.id).cmp(&(b.created_at, &b.id)));
            let more = items.len() > limit;
            items.truncate(limit);
            let next_cursor = if more {
                items.last().map(|a| {
                    URL_SAFE_NO_PAD.encode(
                        serde_json::to_vec(&(thread_id, a.created_at, &a.id))
                            .expect("cursor serializes"),
                    )
                })
            } else {
                None
            };
            Ok(Change {
                response: json!({"data": items, "nextCursor": next_cursor}),
                notification: None,
            })
        }
        _ => Err(RpcError::unsupported(method)),
    }
}

fn updated(thread_id: &str, attachment: &Attachment, operation: &str) -> Value {
    protocol::notification(
        "thread/attachment/updated",
        json!({
            "threadId": thread_id, "attachmentId": attachment.id,
            "attachmentType": attachment.attachment_type, "identityKey": attachment.identity_key,
            "operation": operation,
        }),
    )
}
