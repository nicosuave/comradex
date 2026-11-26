//! `thread/searchOccurrences` over a thread's saved turns, following Codex's
//! search of its paginated history: case-insensitive literal matches in user
//! messages and in each turn's final assistant message, in chronological order.
use std::ops::Range;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};

use crate::protocol::{RpcError, RpcResult};

const DEFAULT_LIMIT: u64 = 50;
const MAX_LIMIT: u64 = 250;
const SNIPPET_CONTEXT_BEFORE_CHARS: usize = 48;
const SNIPPET_CONTEXT_AFTER_CHARS: usize = 96;
/// Codex strips this model-context prefix before showing a user message.
const USER_MESSAGE_BEGIN: &str = "## My request for Codex:";

/// One page of occurrences. `turn_cursor` gives the `thread/turns/list`
/// cursor that opens a match's turn.
pub fn search(
    thread_id: &str,
    turns: &[Value],
    params: &Value,
    turn_cursor: impl Fn(&str) -> String,
) -> RpcResult<Value> {
    let term = params["searchTerm"].as_str().unwrap_or("");
    if term.trim().is_empty() {
        return Err(RpcError::invalid(
            "thread/searchOccurrences requires a non-empty searchTerm",
        ));
    }
    let limit = params["limit"]
        .as_u64()
        .unwrap_or(DEFAULT_LIMIT)
        .clamp(1, MAX_LIMIT) as usize;
    // Cursors resume at an item's next occurrence, so turns added since the
    // previous page do not shift it.
    let resume = params["cursor"]
        .as_str()
        .map(|cursor| parse_cursor(cursor, thread_id, term))
        .transpose()?;
    let needle = term.to_lowercase();
    let mut skipping = resume.is_some();
    let mut data = Vec::new();
    for turn in turns {
        let turn_id = turn["id"].as_str().unwrap_or("");
        for item in candidates(turn) {
            let item_id = item["id"].as_str().unwrap_or("");
            let mut first = 0;
            if let Some((anchor, resume_at)) = &resume
                && skipping
            {
                if item_id != anchor.as_str() {
                    continue;
                }
                skipping = false;
                first = *resume_at;
            }
            let Some(text) = searchable_text(item) else {
                continue;
            };
            for (index, matched) in find_ranges(&text, &needle)
                .into_iter()
                .enumerate()
                .skip(first)
            {
                if data.len() == limit {
                    let next = encode_cursor(thread_id, term, item_id, index);
                    return Ok(json!({"data": data, "nextCursor": next}));
                }
                data.push(occurrence(
                    turn_id,
                    item_id,
                    &text,
                    matched,
                    turn_cursor(turn_id),
                ));
            }
        }
    }
    if skipping {
        return Err(RpcError::invalid(
            "Search cursor anchor is no longer in this thread",
        ));
    }
    Ok(json!({"data": data, "nextCursor": null}))
}

/// User messages and the turn's final assistant message, in item order.
fn candidates(turn: &Value) -> impl Iterator<Item = &Value> {
    let final_answer = final_agent_message(turn);
    turn["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(move |item| {
            item["type"] == "userMessage"
                || (final_answer.is_some() && item["id"].as_str() == final_answer)
        })
}

/// Codex's final assistant message of a turn: its last `final_answer`, or,
/// once the turn has ended, its last message without a phase.
fn final_agent_message(turn: &Value) -> Option<&str> {
    let messages: Vec<&Value> = turn["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| item["type"] == "agentMessage")
        .collect();
    let last = |phase: Option<&str>| {
        messages
            .iter()
            .rev()
            .copied()
            .find(|item| item["phase"].as_str() == phase)
    };
    let ended = turn["status"] != "inProgress";
    last(Some("final_answer"))
        .or_else(|| if ended { last(None) } else { None })
        .and_then(|item| item["id"].as_str())
}

/// The visible text Codex searches. Codex also renders assistant Markdown to
/// plain text first; without a Markdown parser, only whitespace is normalized.
fn searchable_text(item: &Value) -> Option<String> {
    if item["type"] == "userMessage" {
        let text: String = item["content"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|part| part["type"] == "text")
            .filter_map(|part| part["text"].as_str())
            .map(strip_user_message_prefix)
            .collect();
        return (!text.is_empty()).then_some(text);
    }
    let text = item["text"]
        .as_str()?
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    (!text.is_empty()).then_some(text)
}

fn strip_user_message_prefix(text: &str) -> &str {
    match text.find(USER_MESSAGE_BEGIN) {
        Some(index) => text[index + USER_MESSAGE_BEGIN.len()..].trim(),
        None => text.trim(),
    }
}

/// Byte ranges in `text` of non-overlapping matches of the lowercased
/// `needle`. Lowercasing can change a character's encoded length, so matches
/// in the lowercased text map back through each character's span.
fn find_ranges(text: &str, needle: &str) -> Vec<Range<usize>> {
    let lowercase = text.to_lowercase();
    // Each character's end offset in the lowercased text, and its own range.
    let mut spans = Vec::new();
    let mut lowercase_end = 0;
    for (start, character) in text.char_indices() {
        lowercase_end += character.to_lowercase().map(char::len_utf8).sum::<usize>();
        spans.push((lowercase_end, start..start + character.len_utf8()));
    }
    let mut first = 0;
    let mut last = 0;
    lowercase
        .match_indices(needle)
        .filter_map(|(start, matched)| {
            let end = start + matched.len();
            while spans.get(first)?.0 <= start {
                first += 1;
            }
            while spans.get(last)?.0 < end {
                last += 1;
            }
            Some(spans[first].1.start..spans[last].1.end)
        })
        .collect()
}

fn occurrence(
    turn_id: &str,
    item_id: &str,
    text: &str,
    matched: Range<usize>,
    turn_cursor: String,
) -> Value {
    let start = char_start_before(text, matched.start, SNIPPET_CONTEXT_BEFORE_CHARS);
    let end = char_end_after(text, matched.end, SNIPPET_CONTEXT_AFTER_CHARS);
    let leading_ellipsis = start > 0;
    let mut snippet = String::new();
    if leading_ellipsis {
        snippet.push_str("... ");
    }
    snippet.push_str(&text[start..end]);
    if end < text.len() {
        snippet.push_str(" ...");
    }
    // The match range counts UTF-16 code units, as the desktop indexes strings.
    let ellipsis = if leading_ellipsis { "... ".len() } else { 0 };
    let match_start = ellipsis + utf16_len(&text[start..matched.start]);
    let match_end = match_start + utf16_len(&text[matched]);
    json!({"turnId": turn_id, "itemId": item_id, "snippet": snippet,
        "snippetMatchRange": {"start": match_start, "end": match_end},
        "turnCursor": turn_cursor})
}

fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

fn char_start_before(text: &str, index: usize, chars: usize) -> usize {
    text[..index]
        .char_indices()
        .rev()
        .nth(chars)
        .map_or(0, |(start, _)| start)
}

fn char_end_after(text: &str, index: usize, chars: usize) -> usize {
    text[index..]
        .char_indices()
        .nth(chars)
        .map_or(text.len(), |(offset, _)| index + offset)
}

fn encode_cursor(thread_id: &str, term: &str, item_id: &str, occurrence: usize) -> String {
    URL_SAFE_NO_PAD.encode(
        json!({"threadId": thread_id, "searchTerm": term, "itemId": item_id,
            "occurrence": occurrence})
        .to_string(),
    )
}

fn parse_cursor(cursor: &str, thread_id: &str, term: &str) -> RpcResult<(String, usize)> {
    let decoded: Value = URL_SAFE_NO_PAD
        .decode(cursor)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default();
    if decoded["threadId"] != thread_id || decoded["searchTerm"] != term {
        return Err(RpcError::invalid(
            "Search cursor belongs to a different thread or search term",
        ));
    }
    let item = decoded["itemId"].as_str();
    let occurrence = decoded["occurrence"]
        .as_u64()
        .and_then(|n| usize::try_from(n).ok());
    match (item, occurrence) {
        (Some(item), Some(occurrence)) => Ok((item.to_owned(), occurrence)),
        _ => Err(RpcError::invalid("Invalid search cursor")),
    }
}
