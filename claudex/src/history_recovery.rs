//! Resolve facade history boundaries to immutable native Claude transcript anchors.
//!
//! A nonempty retained prefix must have an exact native anchor. Truncating only
//! displayed turns would leave removed messages in the model's next context.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::protocol::{RpcError, RpcResult};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeAnchor {
    pub session_id: String,
    pub message_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HistoryBoundary {
    Full,
    BeforeTurn(String),
    ThroughTurn(String),
    Rollback(u32),
}

impl HistoryBoundary {
    pub fn for_fork(params: &Value) -> RpcResult<Self> {
        let before = optional_turn_id(params, "beforeTurnId")?;
        let through = optional_turn_id(params, "lastTurnId")?;
        match (before, through) {
            (Some(_), Some(_)) => Err(RpcError::invalid(
                "beforeTurnId and lastTurnId cannot be combined",
            )),
            (Some(id), None) => Ok(Self::BeforeTurn(id)),
            (None, Some(id)) => Ok(Self::ThroughTurn(id)),
            (None, None) => Ok(Self::Full),
        }
    }
}

fn optional_turn_id(params: &Value, name: &str) -> RpcResult<Option<String>> {
    match params.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(id)) if !id.is_empty() => Ok(Some(id.clone())),
        _ => Err(RpcError::invalid(format!(
            "{name} must be a nonempty string"
        ))),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryPlan {
    pub retained_len: usize,
    /// None means start a fresh native session, with no resumed conversation.
    /// Otherwise fork this session at this inclusive message UUID.
    pub anchor: Option<NativeAnchor>,
}

/// Recover an idle thread or initialize a fork from a completed parent prefix.
/// The caller must apply the facade prefix and native fork atomically to its record.
/// `latest_anchor` allows full snapshots of records written before per-turn
/// anchors existed; it must never stand in for a missing earlier boundary.
pub fn plan_prefix(
    turns: &[Value],
    turn_anchors: &BTreeMap<String, NativeAnchor>,
    latest_anchor: Option<&NativeAnchor>,
    boundary: &HistoryBoundary,
) -> RpcResult<RecoveryPlan> {
    let retained_len = match boundary {
        HistoryBoundary::Full => turns.len(),
        HistoryBoundary::BeforeTurn(id) => turn_index(turns, id)?,
        HistoryBoundary::ThroughTurn(id) => {
            let index = turn_index(turns, id)?;
            if turns[index]["status"] == "inProgress" {
                return Err(RpcError::invalid("Cannot fork through an in-progress turn"));
            }
            index + 1
        }
        HistoryBoundary::Rollback(0) => {
            return Err(RpcError::invalid("numTurns must be >= 1"));
        }
        HistoryBoundary::Rollback(count) => turns.len().saturating_sub(*count as usize),
    };
    if retained_len == 0 {
        return Ok(RecoveryPlan {
            retained_len,
            anchor: None,
        });
    }
    let last_turn = &turns[retained_len - 1];
    if last_turn["status"] == "inProgress" {
        return Err(RpcError::invalid("Cannot retain an in-progress turn"));
    }
    let anchor = last_turn["id"]
        .as_str()
        .and_then(|id| turn_anchors.get(id))
        .or_else(|| (retained_len == turns.len()).then_some(latest_anchor).flatten())
        .filter(|anchor| !anchor.session_id.is_empty() && !anchor.message_id.is_empty())
        .ok_or_else(|| RpcError::invalid(
            "No exact Claude transcript anchor exists for the retained turn; cannot safely recover this history",
        ))?;
    Ok(RecoveryPlan {
        retained_len,
        anchor: Some(anchor.clone()),
    })
}

fn turn_index(turns: &[Value], id: &str) -> RpcResult<usize> {
    turns
        .iter()
        .position(|turn| turn["id"].as_str() == Some(id))
        .ok_or_else(|| RpcError::invalid(format!("Turn not found in thread: {id}")))
}
