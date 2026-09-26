use anyhow::{Context, Result, bail, ensure};
use hyper::HeaderMap;
use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, Visitor},
};
use serde_json::{Value, value::RawValue};
use std::collections::BTreeSet;

#[derive(Clone)]
pub struct NativeRequest {
    pub session: String,
    pub account: String,
    pub model: String,
    pub nonportable: bool,
    pub conversation: String,
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

pub fn uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
}

pub fn native_headers(headers: &HeaderMap) -> bool {
    let ua = header(headers, "user-agent");
    let Some(rest) = ua.strip_prefix("claude-cli/") else {
        return false;
    };
    let Some((version, profile)) = rest.split_once(" (") else {
        return false;
    };
    let Some(profile) = profile.strip_suffix(')') else {
        return false;
    };
    // The native Agent SDK adds fields after the entry point, for example
    // `(external, sdk-ts, agent-sdk/0.3.276)`. Match the field, not the suffix.
    let mut fields = profile.split(", ");
    let Some(_environment) = fields.next().filter(|v| !v.is_empty()) else {
        return false;
    };
    let Some(entrypoint) = fields.next() else {
        return false;
    };
    version.split('.').count() == 3
        && version
            .split('.')
            .all(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
        && matches!(
            entrypoint,
            "cli" | "sdk-cli" | "sdk-ts" | "sdk-py" | "claude-vscode"
        )
}

pub fn inspect(headers: &HeaderMap, body: &[u8], count_tokens: bool) -> Result<NativeRequest> {
    ensure!(
        native_headers(headers) && header(headers, "x-app") == "cli",
        "native Claude Code headers required"
    );
    ensure!(
        header(headers, "authorization").starts_with("Bearer sk-ant-oat"),
        "native subscription OAuth required"
    );
    ensure!(
        !headers.contains_key("x-api-key"),
        "API-key harnesses are not supported"
    );
    let betas: Vec<&str> = headers
        .get_all("anthropic-beta")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(',').map(str::trim))
        .collect();
    ensure!(betas.contains(&"oauth-2025-04-20"), "OAuth beta required");
    let raw: &RawValue = serde_json::from_slice(body).context("invalid Claude request JSON")?;
    let fields = object(raw)?;
    let model = string(member(&fields, "model")?.context("model required")?)?;
    ensure!(!model.is_empty() && model.len() <= 512, "invalid model");
    let mut session = header(headers, "x-claude-code-session-id").to_owned();
    let mut account = String::new();
    if let Some(metadata) = member(&fields, "metadata")? {
        let metadata = object(metadata)?;
        let identity = string(member(&metadata, "user_id")?.context("native user_id required")?)?;
        let raw: &RawValue =
            serde_json::from_str(&identity).context("native JSON user_id required")?;
        let identity_fields = object(raw)?;
        account =
            string(member(&identity_fields, "account_uuid")?.context("account_uuid required")?)?;
        let device = string(member(&identity_fields, "device_id")?.context("device_id required")?)?;
        let body_session =
            string(member(&identity_fields, "session_id")?.context("session_id required")?)?;
        ensure!(
            session.is_empty() || session == body_session,
            "conflicting session identities"
        );
        session = body_session;
        // Claude Code sends an empty account UUID while it holds a token but no cached
        // account profile, for example after a failed refresh and a later re-login.
        ensure!(
            account.is_empty() || uuid(&account),
            "native account UUID required"
        );
        ensure!(
            device.len() == 64 && device.bytes().all(|b| b.is_ascii_hexdigit()),
            "native device ID required"
        );
    } else {
        ensure!(count_tokens, "native metadata required");
    }
    ensure!(uuid(&session), "native session UUID required");
    // Native helper requests can omit the main Claude Code beta. Their SDK, identity,
    // and model signals must still agree; unknown callers never get a fabricated profile.
    if !betas.contains(&"claude-code-20250219") {
        ensure!(
            !count_tokens
                && model.starts_with("claude-haiku-")
                && header(headers, "x-stainless-lang") == "js"
                && header(headers, "x-stainless-runtime") == "node"
                && header(headers, "x-stainless-async") == "async",
            "unrecognized native helper profile"
        );
    }
    let value: Value = serde_json::from_slice(body)?;
    Ok(NativeRequest {
        session,
        account,
        model,
        conversation: conversation(&value),
        nonportable: value.get("messages").is_some_and(nonportable)
            || [
                "container",
                "container_id",
                "fallback_credit_token",
                "cc_prev_req",
            ]
            .iter()
            .any(|key| value.get(key).is_some_and(|v| !v.is_null()))
            || value
                .get("diagnostics")
                .and_then(|v| v.get("previous_message_id"))
                .is_some_and(|v| !v.is_null()),
    })
}

/// Helper and subagent requests share their parent session but open with their own first
/// message. Cache breakpoints move between turns and can turn text content into blocks.
fn conversation(value: &Value) -> String {
    fn normalize(value: &mut Value) {
        match value {
            Value::Object(map) => {
                map.remove("cache_control");
                map.values_mut().for_each(normalize);
                if let Some(Value::Array(blocks)) = map.get("content")
                    && let Some(text) = blocks
                        .iter()
                        .map(|block| {
                            (block.get("type")?.as_str()? == "text"
                                && block.as_object()?.len() == 2)
                                .then(|| block.get("text")?.as_str())
                                .flatten()
                        })
                        .collect::<Option<String>>()
                {
                    map.insert("content".into(), text.into());
                }
            }
            Value::Array(values) => values.iter_mut().for_each(normalize),
            _ => {}
        }
    }
    let mut first = value
        .get("messages")
        .and_then(|messages| messages.get(0))
        .cloned()
        .unwrap_or_default();
    normalize(&mut first);
    blake3::hash(first.to_string().as_bytes())
        .to_hex()
        .to_string()
}

fn nonportable(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(key, value)| {
            if key == "input" {
                return false;
            }
            ([
                "file_id",
                "container",
                "container_id",
                "fallback_credit_token",
                "signature",
                "encrypted_content",
            ]
            .contains(&key.as_str())
                && !value.is_null())
                || (key == "type"
                    && value.as_str().is_some_and(|kind| {
                        matches!(kind, "compaction" | "redacted_thinking" | "server_tool_use")
                    }))
                || nonportable(value)
        }),
        Value::Array(values) => values.iter().any(nonportable),
        _ => false,
    }
}

// RawValue keeps the client's serialization intact. Only the credential identity string
// and an already-present checksum can change; no unknown fields are dropped.
pub fn rewrite(body: &[u8], account: &str, device: &str) -> Result<Vec<u8>> {
    let raw: &RawValue = serde_json::from_slice(body)?;
    let fields = object(raw)?;
    let Some(metadata) = member(&fields, "metadata")? else {
        return Ok(body.to_vec());
    };
    let meta = object(metadata)?;
    let identity_raw = member(&meta, "user_id")?.context("missing user_id")?;
    let identity = string(identity_raw)?;
    let identity_doc: &RawValue = serde_json::from_str(&identity)?;
    let members = object(identity_doc)?;
    let mut edits = Vec::new();
    for (key, value) in [("account_uuid", account), ("device_id", device)] {
        let old = member(&members, key)?.context("missing credential identity")?;
        if string(old)? != value {
            let start = encoded_offset(
                identity_raw.get().as_bytes(),
                offset(identity.as_bytes(), old),
            )?;
            let end = encoded_offset(
                identity_raw.get().as_bytes(),
                offset(identity.as_bytes(), old) + old.get().len(),
            )?;
            let encoded = serde_json::to_vec(&serde_json::to_string(value)?)?;
            edits.push((
                offset(body, identity_raw) + start,
                end - start,
                encoded[1..encoded.len() - 1].to_vec(),
            ));
        }
    }
    if edits.is_empty() {
        return Ok(body.to_vec());
    }
    let mut body = edit(body, edits);
    sign_existing(&mut body)?;
    Ok(body)
}

// Locate a decoded byte boundary inside the original JSON string token. This lets
// identity edits retain even unusual escapes in unrelated identity members.
fn encoded_offset(encoded: &[u8], target: usize) -> Result<usize> {
    let (mut source, mut decoded) = (1, 0);
    while decoded < target {
        if encoded.get(source) == Some(&b'\\') {
            if encoded.get(source + 1) == Some(&b'u') {
                let hex = encoded
                    .get(source + 2..source + 6)
                    .context("invalid identity escape")?;
                let code = u16::from_str_radix(std::str::from_utf8(hex)?, 16)?;
                if (0xd800..=0xdbff).contains(&code) {
                    source += 12;
                    decoded += 4;
                } else {
                    source += 6;
                    decoded += char::from_u32(u32::from(code))
                        .context("invalid identity unicode")?
                        .len_utf8();
                }
            } else {
                source += 2;
                decoded += 1;
            }
        } else {
            ensure!(source < encoded.len() - 1, "identity offset out of bounds");
            source += 1;
            decoded += 1;
        }
    }
    ensure!(
        decoded == target && source < encoded.len(),
        "invalid identity boundary"
    );
    Ok(source)
}

fn string(raw: &RawValue) -> Result<String> {
    Ok(serde_json::from_str(raw.get())?)
}
fn offset(bytes: &[u8], raw: &RawValue) -> usize {
    raw.get().as_ptr() as usize - bytes.as_ptr() as usize
}
pub(crate) fn edit(bytes: &[u8], mut edits: Vec<(usize, usize, Vec<u8>)>) -> Vec<u8> {
    edits.sort_by_key(|(start, _, _)| *start);
    let mut result = Vec::with_capacity(bytes.len());
    let mut pos = 0;
    for (start, len, replacement) in edits {
        result.extend_from_slice(&bytes[pos..start]);
        result.extend(replacement);
        pos = start + len;
    }
    result.extend_from_slice(&bytes[pos..]);
    result
}

struct Members<'a>(Vec<(String, &'a RawValue)>);
impl<'de> Deserialize<'de> for Members<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Members<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an object")
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                let mut keys = BTreeSet::new();
                while let Some((key, value)) = map.next_entry::<String, &'de RawValue>()? {
                    if !keys.insert(key.clone()) {
                        return Err(serde::de::Error::custom("duplicate JSON key"));
                    }
                    values.push((key, value));
                }
                Ok(Members(values))
            }
        }
        d.deserialize_map(V)
    }
}
fn object(raw: &RawValue) -> Result<Vec<(String, &RawValue)>> {
    Ok(serde_json::from_str::<Members>(raw.get())?.0)
}
fn member<'a>(members: &[(String, &'a RawValue)], key: &str) -> Result<Option<&'a RawValue>> {
    Ok(members.iter().find(|(k, _)| k == key).map(|(_, v)| *v))
}

fn sign_existing(body: &mut [u8]) -> Result<()> {
    let raw: &RawValue = serde_json::from_slice(body)?;
    let root = object(raw)?;
    let Some(system) = member(&root, "system")? else {
        return Ok(());
    };
    let blocks: Vec<&RawValue> = match serde_json::from_str(system.get()) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };
    let Some(first) = blocks.first() else {
        return Ok(());
    };
    let first = object(first)?;
    let Some(text) = member(&first, "text")? else {
        return Ok(());
    };
    let decoded = string(text)?;
    if !decoded.starts_with("x-anthropic-billing-header:") || !decoded.contains("cch=") {
        return Ok(());
    }
    let patch = decoded
        .split("cc_version=2.1.")
        .nth(1)
        .and_then(|s| s.split(['.', ';']).next())
        .and_then(|s| s.parse::<u32>().ok());
    ensure!(
        patch.is_some_and(|v| (220..=281).contains(&v)) && decoded.matches("cch=").count() == 1,
        "unverified Claude checksum version"
    );
    let Some(pos) = text.get().find("cch=") else {
        bail!("unsupported escaped CCH")
    };
    let at = offset(body, text) + pos + 4;
    ensure!(
        body.get(at + 5) == Some(&b';')
            && body
                .get(at..at + 5)
                .is_some_and(|v| v.iter().all(u8::is_ascii_hexdigit)),
        "unsupported CCH format"
    );
    body[at..at + 5].copy_from_slice(b"00000");
    let raw: &RawValue = serde_json::from_slice(body)?;
    let mut edits = Vec::new();
    normalize(body, raw, &mut edits, 0)?;
    let hash = xxhash_rust::xxh64::xxh64(&edit(body, edits), 0x4D659218E32A3268);
    body[at..at + 5].copy_from_slice(format!("{:05x}", hash & 0xfffff).as_bytes());
    Ok(())
}

fn normalize(
    base: &[u8],
    raw: &RawValue,
    edits: &mut Vec<(usize, usize, Vec<u8>)>,
    depth: usize,
) -> Result<()> {
    ensure!(depth < 128, "JSON nesting exceeds supported depth");
    match raw.get().as_bytes()[0] {
        b'{' => {
            let members = object(raw)?;
            let start = offset(base, raw);
            let mut pos = start + 1;
            let mut spans = Vec::new();
            for (key, value) in &members {
                while base[pos].is_ascii_whitespace() {
                    pos += 1;
                }
                let field_start = pos;
                let end = offset(base, value) + value.get().len();
                pos = end;
                while base[pos].is_ascii_whitespace() {
                    pos += 1;
                }
                let comma = (base[pos] == b',').then_some(pos);
                if comma.is_some() {
                    pos += 1;
                }
                let literal_key = base[field_start..].starts_with(format!("\"{key}\"").as_bytes());
                let excluded = literal_key
                    && ["max_tokens", "fallbacks", "fallback_credit_token"].contains(&key.as_str());
                spans.push((field_start, end, comma, excluded));
                if excluded {
                    continue;
                }
                if literal_key && key == "model" && value.get().starts_with('"') {
                    edits.push((offset(base, value) + 1, value.get().len() - 2, vec![]));
                } else {
                    normalize(base, value, edits, depth + 1)?;
                }
            }
            let mut i = 0;
            while i < spans.len() {
                if !spans[i].3 {
                    i += 1;
                    continue;
                }
                let first = i;
                while i + 1 < spans.len() && spans[i + 1].3 {
                    i += 1;
                }
                let (from, to) = if i + 1 < spans.len() {
                    (spans[first].0, spans[i].2.unwrap() + 1)
                } else if first > 0 && first == i {
                    (spans[first - 1].2.unwrap(), spans[i].1)
                } else {
                    (spans[first].0, spans[i].1)
                };
                edits.push((from, to - from, vec![]));
                i += 1;
            }
        }
        b'[' => {
            for value in serde_json::from_str::<Vec<&RawValue>>(raw.get())? {
                normalize(base, value, edits, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_user_agent_recognizes_sdk_entrypoints_and_optional_attribution_fields() {
        let mut headers = headers();
        for ua in [
            "claude-cli/2.1.281 (external, sdk-ts, agent-sdk/0.3.276)",
            "claude-cli/2.1.281 (external, sdk-py, agent-sdk/0.1.0)",
            "claude-cli/2.1.281 (external, sdk-cli)",
            "claude-cli/2.1.281 (external, cli, client-app/example)",
            "claude-cli/2.1.281 (external, claude-vscode)",
        ] {
            headers.insert("user-agent", ua.parse().unwrap());
            assert!(native_headers(&headers), "{ua}");
            assert!(inspect(&headers, &body(), false).is_ok(), "{ua}");
        }
        for ua in [
            "claude-cli/2.1.281 (external, foreign, sdk-cli)",
            "claude-cli/2.1.281 (external, sdk-ts-foreign, agent-sdk/0.3.276)",
            "claude-cli/2.1.281 (external, sdk-ts, agent-sdk/0.3.276",
            "claude-cli/2.1 (external, sdk-ts)",
            "claude-cli/2.1.281 (external)",
            "anthropic-typescript/0.3.276 (external, sdk-ts)",
        ] {
            headers.insert("user-agent", ua.parse().unwrap());
            assert!(!native_headers(&headers), "{ua}");
        }
    }
    pub const ACCOUNT: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    pub const SESSION: &str = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
    pub fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("user-agent", "claude-cli/2.1.281 (external, sdk-cli)"),
            ("x-app", "cli"),
            ("authorization", "Bearer sk-ant-oat01-synthetic"),
            (
                "anthropic-beta",
                "claude-code-20250219,oauth-2025-04-20,new-future-beta",
            ),
            ("x-claude-code-session-id", SESSION),
        ] {
            headers.insert(name, value.parse().unwrap());
        }
        headers
    }
    pub fn body() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "model":"claude-sonnet-5","max_tokens":64,"stream":true,
            "system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.281.127; cc_entrypoint=sdk-cli;"}],
            "metadata":{"user_id":serde_json::json!({"device_id":"a".repeat(64),"account_uuid":ACCOUNT,"session_id":SESSION,"future":"kept"}).to_string(),"unknown":7},
            "messages":[{"role":"user","content":"Ada Lovelace says hello."}],"future_field":{"literal":"cch=00000;"}
        })).unwrap()
    }
    #[test]
    fn native_gate_rejects_foreign_and_inconsistent_requests() {
        let bytes = body();
        let good = headers();
        assert_eq!(inspect(&good, &bytes, false).unwrap().session, SESSION);
        for name in ["user-agent", "x-app", "authorization", "anthropic-beta"] {
            let mut bad = good.clone();
            bad.remove(name);
            assert!(inspect(&bad, &bytes, false).is_err(), "{name}");
        }
        let mut bad = good.clone();
        bad.insert("user-agent", "OpenCode/1.0".parse().unwrap());
        assert!(inspect(&bad, &bytes, false).is_err());
        let mut bad = good.clone();
        bad.insert("x-api-key", "synthetic".parse().unwrap());
        assert!(inspect(&bad, &bytes, false).is_err());
        let mut bad = good.clone();
        bad.insert("x-claude-code-session-id", ACCOUNT.parse().unwrap());
        assert!(inspect(&bad, &bytes, false).is_err());
        let mut duplicate = bytes.clone();
        duplicate.splice(1..1, b"\"metadata\":{},".iter().copied());
        assert!(inspect(&good, &duplicate, false).is_err());
        assert!(inspect(&good, b"[]", false).is_err());
    }
    #[test]
    fn identity_edits_preserve_all_other_serialized_bytes() {
        let bytes = body();
        assert_eq!(rewrite(&bytes, ACCOUNT, &"a".repeat(64)).unwrap(), bytes);
        let edited = rewrite(
            &bytes,
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            &"b".repeat(64),
        )
        .unwrap();
        let expected = String::from_utf8(bytes.clone())
            .unwrap()
            .replace(ACCOUNT, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
            .replace(&"a".repeat(64), &"b".repeat(64));
        assert_eq!(edited, expected.as_bytes());
        assert!(!String::from_utf8(edited).unwrap().contains(" cch="));
    }
    #[test]
    fn native_request_without_cached_account_profile_is_accepted_and_gains_identity() {
        // Captured from Claude Code 2.1.281 (sdk-ts) holding an OAuth token without a
        // cached account profile: it sends the key with an empty value.
        let bytes = String::from_utf8(body())
            .unwrap()
            .replace(ACCOUNT, "")
            .into_bytes();
        let native = inspect(&headers(), &bytes, false).unwrap();
        assert_eq!(
            (native.account.as_str(), native.session.as_str()),
            ("", SESSION)
        );
        let edited = rewrite(&bytes, ACCOUNT, &"a".repeat(64)).unwrap();
        assert_eq!(edited, body());
        for account in ["not-a-uuid", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa"] {
            let bytes = String::from_utf8(body())
                .unwrap()
                .replace(ACCOUNT, account)
                .into_bytes();
            assert!(inspect(&headers(), &bytes, false).is_err(), "{account}");
        }
    }
    #[test]
    fn signed_and_server_owned_context_is_not_replayed_cross_account() {
        for field in [
            serde_json::json!({"signature":"opaque"}),
            serde_json::json!({"type":"redacted_thinking","data":"opaque"}),
            serde_json::json!({"type":"server_tool_use","id":"owned"}),
            serde_json::json!({"file_id":"file_1"}),
            serde_json::json!({"type":"compaction","content":"opaque"}),
            serde_json::json!({"container":"owned"}),
        ] {
            let mut value: Value = serde_json::from_slice(&body()).unwrap();
            value["messages"][0]["content"] = serde_json::json!([field]);
            assert!(
                inspect(&headers(), &serde_json::to_vec(&value).unwrap(), false)
                    .unwrap()
                    .nonportable
            );
        }
    }
    #[test]
    fn conversation_follows_the_first_message_across_cache_breakpoints() {
        let conversation = |messages: Value| {
            let mut value: Value = serde_json::from_slice(&body()).unwrap();
            value["messages"] = messages;
            inspect(&headers(), &serde_json::to_vec(&value).unwrap(), false)
                .unwrap()
                .conversation
        };
        let first = conversation(serde_json::json!([
            {"role":"user","content":[{"type":"text","text":"Grace Hopper asks","cache_control":{"type":"ephemeral"}}]}
        ]));
        let later = conversation(serde_json::json!([
            {"role":"user","content":"Grace Hopper asks"},
            {"role":"assistant","content":[{"type":"thinking","thinking":"synthetic","signature":"opaque"}]},
            {"role":"user","content":[{"type":"text","text":"continue","cache_control":{"type":"ephemeral"}}]}
        ]));
        let helper = conversation(serde_json::json!([{"role":"user","content":"Write a title"}]));
        assert_eq!(first, later);
        assert_ne!(first, helper);
    }
    #[test]
    fn count_tokens_does_not_require_or_gain_generation_metadata() {
        let bytes = br#"{"model":"claude-sonnet-5","messages":[{"role":"user","content":"hi"}]}"#;
        assert!(inspect(&headers(), bytes, true).is_ok());
        assert_eq!(rewrite(bytes, ACCOUNT, &"a".repeat(64)).unwrap(), bytes);
        assert!(inspect(&headers(), bytes, false).is_err());
    }
    #[test]
    fn checksum_matches_reference_vector_without_changing_other_bytes() {
        // Known-vector behavior documented by CPA's Claude Code 2.1.220 tests.
        let mut bytes=br#"{"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.220.test; cc_entrypoint=sdk-cli; cch=00000;"}]}"#.to_vec();
        let original = bytes.clone();
        sign_existing(&mut bytes).unwrap();
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            String::from_utf8(original)
                .unwrap()
                .replace("cch=00000;", "cch=f2edb;")
        );
    }
    #[test]
    fn checksum_normalization_preserves_native_trailing_comma_rule() {
        let bytes=br#"{"model":"unused","metadata":{"max_tokens":1,"model":"ignored","x":"keep","fallbacks":[],"fallback_credit_token":"x"}}"#;
        let raw: &RawValue = serde_json::from_slice(bytes).unwrap();
        let mut edits = Vec::new();
        normalize(bytes, raw, &mut edits, 0).unwrap();
        assert_eq!(
            edit(bytes, edits),
            br#"{"model":"","metadata":{"model":"","x":"keep",}}"#
        );
    }
    #[test]
    fn escaped_identity_and_unknown_fields_survive_roundtrip() {
        let bytes = body();
        let bytes = String::from_utf8(bytes)
            .unwrap()
            .replace("Ada Lovelace", "\\u0041da Lovelace")
            .into_bytes();
        let output = rewrite(&bytes, ACCOUNT, &"b".repeat(64)).unwrap();
        assert_eq!(
            output,
            String::from_utf8(bytes)
                .unwrap()
                .replace(&"a".repeat(64), &"b".repeat(64))
                .as_bytes()
        );
    }

    #[test]
    fn identity_span_edit_keeps_outer_unicode_escapes_and_rejects_unknown_checksum_versions() {
        let original = String::from_utf8(body())
            .unwrap()
            .replace("kept", r"\u006b\u00e9\ud83d\udc69")
            .replace(
                r#"{\"account_uuid"#,
                r#"{\"note\":\"\u00e9\ud83d\udc69\",\"account_uuid"#,
            );
        let output = rewrite(original.as_bytes(), ACCOUNT, &"b".repeat(64)).unwrap();
        assert_eq!(
            output,
            original
                .replace(&"a".repeat(64), &"b".repeat(64))
                .as_bytes()
        );
        let mut unknown =
            br#"{"system":[{"text":"x-anthropic-billing-header: cc_version=3.0.0; cch=00000;"}]}"#
                .to_vec();
        assert!(sign_existing(&mut unknown).is_err());
    }
}
