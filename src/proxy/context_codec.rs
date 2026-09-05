use anyhow::{Result, ensure};
use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

const PREFIX: &str = "comradex-context-v1:";
const MAX_ENVELOPE_BYTES: usize = 2_000_000;
const MAX_RESULTS: usize = 32;
const MAX_EXPANDED_BYTES: usize = 128_000_000;
const NONCE_BYTES: usize = 12;
const TAG_BYTES: usize = 16;
const KEY_DERIVATION_CONTEXT: &str = "comradex context codec v1 AES-256-GCM key";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextEnvelope {
    scope: String,
    session: String,
    history: bool,
    results: Vec<Value>,
}

/// Carries native Codex context results through a single function result.
pub struct ContextCodec {
    key_bytes: [u8; 32],
}

impl ContextCodec {
    pub fn new(key_text: &str) -> Self {
        Self {
            key_bytes: blake3::derive_key(KEY_DERIVATION_CONTEXT, key_text.as_bytes()),
        }
    }

    pub fn pack(
        &self,
        scope: &str,
        session: &str,
        results: Vec<Value>,
        history: bool,
    ) -> Result<Value> {
        ensure!(
            (1..=MAX_RESULTS).contains(&results.len()),
            "invalid Comradex context result count"
        );
        let mut plaintext = serde_json::to_vec(&ContextEnvelope {
            scope: scope.to_owned(),
            session: session.to_owned(),
            history,
            results,
        })?;
        ensure!(
            plaintext.len() <= MAX_ENVELOPE_BYTES,
            "Comradex context envelope exceeds safety limit"
        );

        let mut nonce_bytes = [0_u8; NONCE_BYTES];
        rand::fill(&mut nonce_bytes);
        self.key()?
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(PREFIX.as_bytes()),
                &mut plaintext,
            )
            .map_err(|_| anyhow::anyhow!("failed to seal Comradex context envelope"))?;

        let mut token_bytes = Vec::with_capacity(NONCE_BYTES + plaintext.len());
        token_bytes.extend_from_slice(&nonce_bytes);
        token_bytes.extend_from_slice(&plaintext);
        Ok(json!({
            "encrypted_output": format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(token_bytes))
        }))
    }

    /// Expands Comradex context tokens in native function-call outputs.
    ///
    /// Returns whether any token was expanded. On error, `body` is unchanged.
    pub fn expand(&self, body: &mut Value, scope: &str, session: &str) -> Result<bool> {
        let Some(input) = body.get("input").and_then(Value::as_array) else {
            return Ok(false);
        };

        let mut expanded_input = input.clone();
        let mut changed = false;
        let mut expanded_bytes = 0_usize;

        for item in &mut expanded_input {
            let Some(item_object) = item.as_object_mut() else {
                continue;
            };
            if item_object.get("type").and_then(Value::as_str) != Some("function_call_output") {
                continue;
            }
            let Some(output) = item_object.get("output").and_then(Value::as_array) else {
                continue;
            };

            let mut expanded_output = Vec::with_capacity(output.len());
            let mut output_changed = false;
            for part in output {
                let Some(token) = context_token(part) else {
                    expanded_output.push(part.clone());
                    continue;
                };

                let envelope = self.open_verified(token, scope, session)?;
                let parts = expanded_parts(envelope.results, envelope.history)?;
                let part_bytes = serde_json::to_vec(&parts)?.len();
                expanded_bytes = expanded_bytes.checked_add(part_bytes).ok_or_else(|| {
                    anyhow::anyhow!("expanded Comradex context exceeds safety limit")
                })?;
                ensure!(
                    expanded_bytes <= MAX_EXPANDED_BYTES,
                    "expanded Comradex context exceeds safety limit"
                );
                expanded_output.extend(parts);
                output_changed = true;
            }

            if output_changed {
                item_object.insert("output".to_owned(), Value::Array(expanded_output));
                changed = true;
            }
        }

        if changed {
            body.as_object_mut()
                .expect("a value with an input field must be an object")
                .insert("input".to_owned(), Value::Array(expanded_input));
        }
        Ok(changed)
    }

    /// Produces a request clone suitable for routing and full-resend analysis.
    ///
    /// Authenticated Comradex context wrappers are replaced with a stable marker. Native
    /// ciphertext and every other request value remain untouched, and the returned view must
    /// never be sent upstream.
    pub fn routing_view(&self, body: &Value, scope: &str, session: &str) -> Result<Value> {
        let mut view = body.clone();
        let Some(input) = view.get_mut("input").and_then(Value::as_array_mut) else {
            return Ok(view);
        };

        for item in input {
            let Some(item) = item.as_object_mut() else {
                continue;
            };
            if item.get("type").and_then(Value::as_str) != Some("function_call_output") {
                continue;
            }
            let Some(output) = item.get_mut("output").and_then(Value::as_array_mut) else {
                continue;
            };
            for part in output {
                let Some(token) = context_token(part) else {
                    continue;
                };
                self.open_verified(token, scope, session)?;
                *part = json!({
                    "type": "input_text",
                    "text": "Verified context tool result"
                });
            }
        }
        Ok(view)
    }

    /// Returns the authenticated source ordinals represented by context wrappers in a request.
    pub fn source_partitions(
        &self,
        body: &Value,
        scope: &str,
        session: &str,
    ) -> Result<Vec<usize>> {
        let Some(input) = body.get("input").and_then(Value::as_array) else {
            return Ok(Vec::new());
        };
        let mut present = [false; MAX_RESULTS];

        for item in input {
            let Some(item) = item.as_object() else {
                continue;
            };
            if item.get("type").and_then(Value::as_str) != Some("function_call_output") {
                continue;
            }
            let Some(output) = item.get("output").and_then(Value::as_array) else {
                continue;
            };
            for part in output {
                let Some(token) = context_token(part) else {
                    continue;
                };
                let envelope = self.open_verified(token, scope, session)?;
                if envelope.history {
                    for partition in 1..=envelope.results.len() {
                        present[partition - 1] = true;
                    }
                } else {
                    present[0] = true;
                }
            }
        }

        Ok(present
            .into_iter()
            .enumerate()
            .filter_map(|(index, is_present)| is_present.then_some(index + 1))
            .collect())
    }

    fn key(&self) -> Result<LessSafeKey> {
        let key = UnboundKey::new(&AES_256_GCM, &self.key_bytes)
            .map_err(|_| anyhow::anyhow!("failed to initialize Comradex context codec"))?;
        Ok(LessSafeKey::new(key))
    }

    fn open(&self, token: &str) -> Result<ContextEnvelope> {
        let encoded = token
            .strip_prefix(PREFIX)
            .ok_or_else(|| anyhow::anyhow!("invalid Comradex context envelope"))?;
        let max_raw_bytes = MAX_ENVELOPE_BYTES + NONCE_BYTES + TAG_BYTES;
        let max_encoded_bytes = max_raw_bytes.div_ceil(3) * 4;
        ensure!(
            encoded.len() <= max_encoded_bytes,
            "invalid Comradex context envelope"
        );

        let mut token_bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| anyhow::anyhow!("invalid Comradex context envelope"))?;
        ensure!(
            (NONCE_BYTES + TAG_BYTES..=max_raw_bytes).contains(&token_bytes.len()),
            "invalid Comradex context envelope"
        );
        let ciphertext = token_bytes.split_off(NONCE_BYTES);
        let nonce_bytes: [u8; NONCE_BYTES] = token_bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid Comradex context envelope"))?;
        let mut ciphertext = ciphertext;
        let plaintext = self
            .key()?
            .open_in_place(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(PREFIX.as_bytes()),
                &mut ciphertext,
            )
            .map_err(|_| anyhow::anyhow!("invalid Comradex context envelope"))?;
        ensure!(
            plaintext.len() <= MAX_ENVELOPE_BYTES,
            "invalid Comradex context envelope"
        );
        serde_json::from_slice(plaintext)
            .map_err(|_| anyhow::anyhow!("invalid Comradex context envelope"))
    }

    fn open_verified(&self, token: &str, scope: &str, session: &str) -> Result<ContextEnvelope> {
        let envelope = self.open(token)?;
        ensure!(
            envelope.scope == scope && envelope.session == session,
            "Comradex context scope mismatch"
        );
        ensure!(
            (1..=MAX_RESULTS).contains(&envelope.results.len()),
            "invalid Comradex context envelope"
        );
        Ok(envelope)
    }
}

fn context_token(part: &Value) -> Option<&str> {
    let part = part.as_object()?;
    if part.get("type")?.as_str()? != "encrypted_content" {
        return None;
    }
    part.get("encrypted_content")
        .and_then(Value::as_str)
        .filter(|token| token.starts_with(PREFIX))
}

fn expanded_parts(results: Vec<Value>, history: bool) -> Result<Vec<Value>> {
    let multiple_history_partitions = history && results.len() > 1;
    let mut parts = Vec::new();
    if multiple_history_partitions {
        parts.push(json!({
            "type": "input_text",
            "text": "The following are independent history partitions for the same task. Combine their results, deduplicate matching item and window IDs, and apply the requested ordering and limit across all partitions."
        }));
    }

    for (index, result) in results.into_iter().enumerate() {
        if multiple_history_partitions {
            parts.push(json!({
                "type": "input_text",
                "text": format!("History partition {}:", index + 1)
            }));
        }
        parts.extend(result_parts(result)?);
    }
    Ok(parts)
}

fn result_parts(mut result: Value) -> Result<Vec<Value>> {
    let images = if let Some(object) = result.as_object_mut() {
        match object.remove("images") {
            Some(Value::Array(images)) => images,
            Some(_) => return Err(anyhow::anyhow!("invalid Comradex context image")),
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };

    let encrypted_output = result
        .as_object()
        .and_then(|object| object.get("encrypted_output"))
        .and_then(Value::as_str);
    let mut parts = Vec::with_capacity(1 + images.len());
    if let Some(encrypted_output) = encrypted_output {
        parts.push(json!({
            "type": "encrypted_content",
            "encrypted_content": encrypted_output
        }));
    } else {
        parts.push(json!({
            "type": "input_text",
            "text": serde_json::to_string(&result).expect("JSON values always serialize")
        }));
    }
    for image in images {
        parts.push(native_image(&image)?);
    }
    Ok(parts)
}

fn native_image(image: &Value) -> Result<Value> {
    let image = image
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("invalid Comradex context image"))?;
    let data = image
        .get("data")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("invalid Comradex context image"))?;
    let mime_type = image
        .get("mime_type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("invalid Comradex context image"))?;
    let detail = image.get("detail").cloned().unwrap_or(Value::Null);
    let mut part = Map::new();
    part.insert("type".to_owned(), Value::String("input_image".to_owned()));
    part.insert(
        "image_url".to_owned(),
        Value::String(format!("data:{mime_type};base64,{data}")),
    );
    part.insert("detail".to_owned(), detail);
    Ok(Value::Object(part))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wrapped(token: Value) -> Value {
        json!({
            "model": "gpt-test",
            "input": [{
                "type": "function_call_output",
                "call_id": "call_1",
                "output": [{
                    "type": "encrypted_content",
                    "encrypted_content": token["encrypted_output"]
                }]
            }]
        })
    }

    #[test]
    fn notes_round_trip_native_and_json_results() {
        let codec = ContextCodec::new("test-key");
        let token = codec
            .pack(
                "scope-a",
                "session-a",
                vec![
                    json!({"encrypted_output":"native-ciphertext"}),
                    json!({"items":[{"id":"note-1"}],"next":null}),
                ],
                false,
            )
            .unwrap();
        let mut body = wrapped(token);

        assert!(codec.expand(&mut body, "scope-a", "session-a").unwrap());
        assert_eq!(
            body["input"][0]["output"],
            json!([
                {"type":"encrypted_content","encrypted_content":"native-ciphertext"},
                {"type":"input_text","text":"{\"items\":[{\"id\":\"note-1\"}],\"next\":null}"}
            ])
        );
    }

    #[test]
    fn history_round_trip_labels_independent_partitions() {
        let codec = ContextCodec::new("test-key");
        let token = codec
            .pack(
                "scope-a",
                "session-a",
                vec![json!({"items":[1]}), json!({"items":[2]})],
                true,
            )
            .unwrap();
        let mut body = wrapped(token);

        codec.expand(&mut body, "scope-a", "session-a").unwrap();
        let output = body["input"][0]["output"].as_array().unwrap();
        assert_eq!(output.len(), 5);
        assert!(
            output[0]["text"]
                .as_str()
                .unwrap()
                .contains("independent history partitions")
        );
        assert_eq!(output[1]["text"], "History partition 1:");
        assert_eq!(output[2]["text"], "{\"items\":[1]}");
        assert_eq!(output[3]["text"], "History partition 2:");
        assert_eq!(output[4]["text"], "{\"items\":[2]}");
    }

    #[test]
    fn native_images_become_input_images() {
        let codec = ContextCodec::new("test-key");
        let token = codec
            .pack(
                "scope-a",
                "session-a",
                vec![json!({
                    "encrypted_output":"native-ciphertext",
                    "images":[{"data":"aGVsbG8=","mime_type":"image/png","detail":"high"}]
                })],
                false,
            )
            .unwrap();
        let mut body = wrapped(token);

        codec.expand(&mut body, "scope-a", "session-a").unwrap();
        assert_eq!(
            body["input"][0]["output"][1],
            json!({
                "type":"input_image",
                "image_url":"data:image/png;base64,aGVsbG8=",
                "detail":"high"
            })
        );
    }

    #[test]
    fn native_ciphertext_is_used_when_metadata_is_present() {
        let codec = ContextCodec::new("test-key");
        let token = codec
            .pack(
                "scope-a",
                "session-a",
                vec![json!({
                    "encrypted_output":"native-ciphertext",
                    "request_id":"request-metadata",
                    "next_cursor":null
                })],
                false,
            )
            .unwrap();
        let mut body = wrapped(token);

        codec.expand(&mut body, "scope-a", "session-a").unwrap();
        assert_eq!(
            body["input"][0]["output"],
            json!([{"type":"encrypted_content","encrypted_content":"native-ciphertext"}])
        );
    }

    #[test]
    fn images_are_attached_to_plaintext_results_and_detail_defaults_to_null() {
        let codec = ContextCodec::new("test-key");
        let token = codec
            .pack(
                "scope-a",
                "session-a",
                vec![json!({
                    "items":[{"id":"note-1"}],
                    "images":[{"data":"aGVsbG8=","mime_type":"image/png"}]
                })],
                false,
            )
            .unwrap();
        let mut body = wrapped(token);

        codec.expand(&mut body, "scope-a", "session-a").unwrap();
        assert_eq!(
            body["input"][0]["output"],
            json!([
                {"type":"input_text","text":"{\"items\":[{\"id\":\"note-1\"}]}"},
                {
                    "type":"input_image",
                    "image_url":"data:image/png;base64,aGVsbG8=",
                    "detail":null
                }
            ])
        );
    }

    #[test]
    fn malformed_images_fail_without_exposing_or_mutating_results() {
        let codec = ContextCodec::new("test-key");
        let token = codec
            .pack(
                "scope-a",
                "session-a",
                vec![json!({
                    "items":[],
                    "images":[{"data":7,"mime_type":"image/png"}]
                })],
                false,
            )
            .unwrap();
        let mut body = wrapped(token);
        let original = body.clone();

        assert_eq!(
            codec
                .expand(&mut body, "scope-a", "session-a")
                .unwrap_err()
                .to_string(),
            "invalid Comradex context image"
        );
        assert_eq!(body, original);
    }

    #[test]
    fn tampering_and_scope_or_session_changes_are_rejected_without_mutation() {
        let codec = ContextCodec::new("test-key");
        let token = codec
            .pack("scope-a", "session-a", vec![json!({"items":[]})], false)
            .unwrap();

        for (scope, session) in [("scope-b", "session-a"), ("scope-a", "session-b")] {
            let mut body = wrapped(token.clone());
            let original = body.clone();
            let error = codec.expand(&mut body, scope, session).unwrap_err();
            assert_eq!(error.to_string(), "Comradex context scope mismatch");
            assert_eq!(body, original);
        }

        let mut tampered = token;
        let value = tampered["encrypted_output"].as_str().unwrap();
        let replacement = if value.ends_with('A') { 'B' } else { 'A' };
        let mut changed = value[..value.len() - 1].to_owned();
        changed.push(replacement);
        tampered["encrypted_output"] = Value::String(changed);
        let mut body = wrapped(tampered);
        let original = body.clone();
        let error = codec.expand(&mut body, "scope-a", "session-a").unwrap_err();
        assert_eq!(error.to_string(), "invalid Comradex context envelope");
        assert_eq!(body, original);
    }

    #[test]
    fn oversized_envelopes_are_rejected() {
        let codec = ContextCodec::new("test-key");
        let error = codec
            .pack(
                "scope-a",
                "session-a",
                vec![Value::String("x".repeat(MAX_ENVELOPE_BYTES))],
                false,
            )
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Comradex context envelope exceeds safety limit"
        );

        let error = codec
            .pack(
                "scope-a",
                "session-a",
                vec![Value::Null; MAX_RESULTS + 1],
                false,
            )
            .unwrap_err();
        assert_eq!(error.to_string(), "invalid Comradex context result count");
    }

    #[test]
    fn arbitrary_ciphertext_and_unknown_values_are_unchanged() {
        let codec = ContextCodec::new("test-key");
        let mut body = json!({
            "input": [
                {
                    "type":"function_call_output",
                    "output":[
                        {"type":"encrypted_content","encrypted_content":"native-ciphertext"},
                        {"type":"future_part","value":7}
                    ]
                },
                {"type":"future_item","value":8}
            ],
            "future_field": true
        });
        let original = body.clone();

        assert!(!codec.expand(&mut body, "scope-a", "session-a").unwrap());
        assert_eq!(body, original);
    }

    #[test]
    fn routing_view_redacts_only_authenticated_context_wrappers() {
        let codec = ContextCodec::new("test-key");
        let token = codec
            .pack(
                "scope-a",
                "session-a",
                vec![json!({"encrypted_output":"native-context"})],
                false,
            )
            .unwrap();
        let body = json!({
            "input": [
                {
                    "type": "reasoning",
                    "encrypted_content": "arbitrary-reasoning-ciphertext"
                },
                {
                    "type": "function_call_output",
                    "output": [
                        {
                            "type": "encrypted_content",
                            "encrypted_content": "arbitrary-output-ciphertext"
                        },
                        {
                            "type": "encrypted_content",
                            "encrypted_content": token["encrypted_output"]
                        }
                    ]
                }
            ]
        });
        let original = body.clone();

        let view = codec.routing_view(&body, "scope-a", "session-a").unwrap();

        assert_eq!(body, original);
        assert_eq!(
            view["input"][0]["encrypted_content"],
            "arbitrary-reasoning-ciphertext"
        );
        assert_eq!(
            view["input"][1]["output"][0],
            json!({
                "type": "encrypted_content",
                "encrypted_content": "arbitrary-output-ciphertext"
            })
        );
        assert_eq!(
            view["input"][1]["output"][1],
            json!({"type":"input_text","text":"Verified context tool result"})
        );
    }

    #[test]
    fn routing_view_rejects_wrong_scope() {
        let codec = ContextCodec::new("test-key");
        let token = codec
            .pack("scope-a", "session-a", vec![json!({"items":[]})], false)
            .unwrap();
        let body = wrapped(token);

        assert_eq!(
            codec
                .routing_view(&body, "scope-b", "session-a")
                .unwrap_err()
                .to_string(),
            "Comradex context scope mismatch"
        );
    }

    #[test]
    fn source_partitions_returns_authenticated_complete_ordinals() {
        let codec = ContextCodec::new("test-key");
        let history = codec
            .pack(
                "scope-a",
                "session-a",
                vec![json!({"items":[1]}), json!({"items":[2]})],
                true,
            )
            .unwrap();
        let notes = codec
            .pack("scope-a", "session-a", vec![json!({"items":[]})], false)
            .unwrap();
        let body = json!({
            "input":[{
                "type":"function_call_output",
                "output":[
                    {"type":"encrypted_content","encrypted_content":history["encrypted_output"]},
                    {"type":"encrypted_content","encrypted_content":notes["encrypted_output"]},
                    {"type":"encrypted_content","encrypted_content":"arbitrary-ciphertext"}
                ]
            }]
        });

        assert_eq!(
            codec
                .source_partitions(&body, "scope-a", "session-a")
                .unwrap(),
            vec![1, 2]
        );
    }
}
