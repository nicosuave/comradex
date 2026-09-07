//! Opt-in qualification against the configured Codex backend. No credentials, account
//! identities, response text, or encrypted payloads are logged or written to disk.
//! Run with COMRADEX_PROBE_MODEL set to a model supported by the configured backend:
//! mbx test --test native_reasoning_live -- --ignored --nocapture
use std::{path::PathBuf, time::Duration};

use comradex::{
    auth::{Credentials, Resolver},
    config::{AccountConfig, Config},
};
use serde_json::{Value, json};

async fn response(
    client: &reqwest::Client,
    endpoint: &str,
    credentials: &Credentials,
    body: &Value,
    stage: &str,
) -> Option<Value> {
    let mut request = client
        .post(endpoint)
        .header("authorization", &credentials.authorization)
        .header("content-type", "application/json")
        .header("accept", "text/event-stream")
        .header("originator", "codex_cli_rs")
        .body(serde_json::to_vec(body).expect("serialize synthetic request"));
    if let Some(account_id) = &credentials.account_id {
        request = request.header("chatgpt-account-id", account_id);
    }
    let Ok(response) = request.send().await else {
        println!("stage={stage} transport_failure=true");
        return None;
    };
    let status = response.status();
    let Ok(text) = response.text().await else {
        println!("stage={stage} http={} read_failure=true", status.as_u16());
        return None;
    };
    let mut completed = None;
    let mut output_items = std::collections::BTreeMap::new();
    let mut code = "none";
    if !status.is_success() {
        let lower = text.to_ascii_lowercase();
        println!(
            "stage={stage} unsupported={} mentions_model={} mentions_tool_choice={} mentions_required={} mentions_instructions={} mentions_reasoning={} mentions_encrypted={} html={}",
            lower.contains("unsupported") || lower.contains("not supported"),
            lower.contains("model"),
            lower.contains("tool_choice"),
            lower.contains("required"),
            lower.contains("instructions"),
            lower.contains("reasoning"),
            lower.contains("encrypted"),
            lower.contains("<html")
        );
    }
    let values = serde_json::from_str::<Value>(&text).into_iter().chain(
        text.lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter_map(|data| serde_json::from_str::<Value>(data).ok()),
    );
    for event in values {
        if event["type"] == "response.output_item.done"
            && let (Some(index), Some(item)) = (event["output_index"].as_u64(), event.get("item"))
        {
            output_items.insert(index, item.clone());
        }
        let candidate = event
            .pointer("/error/code")
            .or_else(|| event.pointer("/response/error/code"))
            .and_then(Value::as_str);
        code = match candidate {
            Some("invalid_encrypted_content") => "invalid_encrypted_content",
            Some("usage_limit_reached") => "usage_limit_reached",
            Some("insufficient_quota") => "insufficient_quota",
            Some("invalid_api_key") => "invalid_api_key",
            Some("model_not_found") => "model_not_found",
            Some("invalid_request_error") => "invalid_request_error",
            Some(_) => "other",
            None => code,
        };
        if event["type"] == "response.completed" {
            completed = event.get("response").cloned();
        }
    }
    // Codex can leave response.completed.output empty; output_item.done owns the
    // complete native item in that stream. Preserve each item without rewriting it.
    if let Some(result) = completed.as_mut()
        && result["output"].as_array().is_some_and(Vec::is_empty)
    {
        result["output"] = Value::Array(output_items.into_values().collect());
    }
    println!(
        "stage={stage} http={} completed={} error_code={code}",
        status.as_u16(),
        completed.is_some()
    );
    completed
}

#[tokio::test]
#[ignore = "uses configured managed accounts and live backend; explicitly authorize before running"]
async fn native_reasoning_same_and_cross_account_tool_continuation() {
    let home = PathBuf::from(std::env::var_os("HOME").expect("HOME required"));
    let path = std::env::var_os("COMRADEX_PROBE_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".config/comradex/comradex.toml"));
    let config = Config::load(&path)
        .unwrap_or_else(|_| panic!("configured Comradex config could not be loaded"));
    assert!(
        config.proxy.upstream == comradex::config::CANONICAL_UPSTREAM,
        "live qualification requires the canonical Codex upstream"
    );
    let resolver = Resolver::new(&config);
    let mut credentials: Vec<(String, Credentials)> = Vec::new();
    for account in config
        .accounts
        .values()
        .filter(|account| matches!(account, AccountConfig::CodexHome { .. }))
    {
        if let Ok(credential) = resolver.resolve(account, &hyper::HeaderMap::new()).await
            && let Ok(identity) = credential.context_identity()
            && credential.account_id.is_some()
            && !credentials.iter().any(|(existing, selected)| {
                existing == &identity || selected.account_id == credential.account_id
            })
        {
            credentials.push((identity, credential));
            if credentials.len() == 2 {
                break;
            }
        }
    }
    assert!(
        credentials.len() == 2,
        "two distinct configured managed identities required"
    );
    assert!(
        credentials[0].1.account_id.is_some()
            && credentials[1].1.account_id.is_some()
            && credentials[0].1.account_id != credentials[1].1.account_id,
        "two distinct configured workspace accounts required"
    );
    println!(
        "distinct_configured_identities=true distinct_workspace_accounts=true upstream_canonical=true"
    );
    let model = std::env::var("COMRADEX_PROBE_MODEL")
        .expect("set COMRADEX_PROBE_MODEL to the configured client model");
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(120))
        .build()
        .expect("HTTP client");
    let endpoint = format!("{}/responses", config.proxy.upstream);
    let prompt = json!({"role":"user", "content":"Four people must cross a bridge at night with one torch. Their crossing times are 1, 2, 5, and 10 minutes. At most two can cross together, a pair takes the slower person's time, and the torch must be carried on every crossing. Work out the minimum total crossing time. Then use the add tool with that minimum as a and 25 as b. Once the tool returns, answer only with the integer it returned."});
    let mut request = json!({
        "model": model, "instructions": "Perform the harmless arithmetic task.",
        "store": false, "stream": true, "reasoning": {"effort":"high"},
        "include": ["reasoning.encrypted_content"],
        "input": [prompt],
        "tools": [{"type":"function", "name":"add", "description":"Add two integers.", "parameters":{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"],"additionalProperties":false},"strict":true}],
        "tool_choice": "required", "parallel_tool_calls": false
    });
    let output = response(
        &client,
        &endpoint,
        &credentials[0].1,
        &request,
        "generate_a",
    )
    .await
    .expect("account A generation failed; see safe stage metadata");
    let items = output["output"]
        .as_array()
        .expect("generation output array");
    println!(
        "generation_items={} reasoning_items={} encrypted_reasoning_items={} function_calls={}",
        items.len(),
        items
            .iter()
            .filter(|item| item["type"] == "reasoning")
            .count(),
        items
            .iter()
            .filter(|item| item["type"] == "reasoning"
                && item["encrypted_content"]
                    .as_str()
                    .is_some_and(|s| !s.is_empty()))
            .count(),
        items
            .iter()
            .filter(|item| item["type"] == "function_call")
            .count()
    );
    assert!(
        items.iter().any(|item| item["type"] == "reasoning"
            && item["encrypted_content"]
                .as_str()
                .is_some_and(|s| !s.is_empty())),
        "generation must contain native encrypted reasoning"
    );
    let calls: Vec<_> = items
        .iter()
        .filter(|item| item["type"] == "function_call")
        .collect();
    assert!(
        calls.len() == 1 && calls[0]["name"] == "add",
        "expected one synthetic tool call"
    );
    let arguments: Value =
        serde_json::from_str(calls[0]["arguments"].as_str().expect("tool arguments"))
            .expect("tool argument JSON");
    assert!(
        arguments["a"] == 17 && arguments["b"] == 25,
        "expected synthetic tool arguments"
    );
    let mut input = vec![prompt];
    input.extend(items.iter().cloned());
    input.push(json!({"type":"function_call_output","call_id":calls[0]["call_id"],"output":"42"}));
    request["input"] = Value::Array(input);
    request["tool_choice"] = json!("auto");
    let bytes_before = serde_json::to_vec(&request).unwrap();
    let control = response(
        &client,
        &endpoint,
        &credentials[0].1,
        &request,
        "same_account_tool_continuation",
    )
    .await;
    let cross = response(
        &client,
        &endpoint,
        &credentials[1].1,
        &request,
        "cross_account_tool_continuation",
    )
    .await;
    assert!(
        bytes_before == serde_json::to_vec(&request).unwrap(),
        "continuation request was mutated"
    );
    println!("exact_native_items_preserved=true same_request_bytes=true");
    assert!(
        control.is_some(),
        "same-account control failed; qualification inconclusive"
    );
    assert!(
        cross.is_some(),
        "cross-account qualification failed; native portability is not qualified"
    );
    for result in [control.unwrap(), cross.unwrap()] {
        assert!(
            result["output"]
                .as_array()
                .is_some_and(|items| items.iter().any(|item| item["content"]
                    .as_array()
                    .is_some_and(|content| content.iter().any(|part| part["text"]
                        .as_str()
                        .is_some_and(|text| text.trim() == "42"))))),
            "continuation must complete the synthetic tool task"
        );
    }
    println!("native_reasoning_portability_qualified=true tool_continuations_correct=true");
}
